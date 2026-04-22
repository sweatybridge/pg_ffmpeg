use ffmpeg_next::format::Pixel;
use ffmpeg_next::media::Type;
use ffmpeg_next::software::scaling::{context::Context as ScaleContext, flag::Flags};
use ffmpeg_next::util::frame::video::Video;
use ffmpeg_next::Rational;
use pgrx::prelude::*;

use crate::frame_encoder;
use crate::mem_io::MemInput;

#[pg_extern]
fn extract_frames(
    data: Vec<u8>,
    interval: default!(f64, 1.0),
    format: default!(String, "'png'"),
    keyframes_only: default!(bool, false),
    max_frames: default!(i32, 1000),
) -> TableIterator<'static, (name!(timestamp, f64), name!(frame, Vec<u8>))> {
    ffmpeg_next::init().unwrap();
    validate_extract_frames_args(interval, &format, max_frames);

    if keyframes_only && (interval - 1.0).abs() > f64::EPSILON {
        pgrx::warning!(
            "pg_ffmpeg: extract_frames(keyframes_only=true) ignores interval; emitting keyframes only"
        );
    }

    let rows = collect_frame_rows(
        &data,
        interval,
        &format,
        keyframes_only,
        max_frames as usize,
    );
    TableIterator::new(rows)
}

fn validate_extract_frames_args(interval: f64, format: &str, max_frames: i32) {
    if interval <= 0.0 {
        error!("pg_ffmpeg: interval must be > 0");
    }
    if max_frames <= 0 {
        error!("pg_ffmpeg: max_frames must be > 0");
    }
    match format {
        "png" | "jpeg" | "jpg" => {}
        _ => error!("pg_ffmpeg: format must be png, jpeg, or jpg"),
    }
}

fn collect_frame_rows(
    data: &[u8],
    interval: f64,
    format: &str,
    keyframes_only: bool,
    max_frames: usize,
) -> Vec<(f64, Vec<u8>)> {
    let mut ictx = MemInput::open(data);
    let input = ictx
        .streams()
        .best(Type::Video)
        .unwrap_or_else(|| error!("no video stream found"));
    let video_stream_index = input.index();
    let stream_time_base = input.time_base();

    let context_decoder = ffmpeg_next::codec::context::Context::from_parameters(input.parameters())
        .unwrap_or_else(|e| error!("failed to create decoder context: {e}"));
    let mut decoder = context_decoder
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open video decoder: {e}"));
    decoder.set_packet_time_base(stream_time_base);
    let time_base = decoder.time_base();

    let mut scaler = ScaleContext::get(
        decoder.format(),
        decoder.width(),
        decoder.height(),
        Pixel::RGB24,
        decoder.width(),
        decoder.height(),
        Flags::BILINEAR,
    )
    .unwrap_or_else(|e| error!("failed to create scaler: {e}"));

    let mut rows = Vec::new();
    let mut next_threshold = 0.0f64;

    for (stream, packet) in ictx.packets() {
        if stream.index() != video_stream_index {
            continue;
        }
        if keyframes_only && !packet.is_key() {
            continue;
        }

        decoder
            .send_packet(&packet)
            .unwrap_or_else(|e| error!("decode error: {e}"));
        drain_decoded_frames(
            &mut decoder,
            &mut scaler,
            time_base,
            format,
            keyframes_only,
            interval,
            &mut next_threshold,
            max_frames,
            &mut rows,
        );
    }

    decoder
        .send_eof()
        .unwrap_or_else(|e| error!("decode flush error: {e}"));
    drain_decoded_frames(
        &mut decoder,
        &mut scaler,
        time_base,
        format,
        keyframes_only,
        interval,
        &mut next_threshold,
        max_frames,
        &mut rows,
    );

    rows
}

#[allow(clippy::too_many_arguments)]
fn drain_decoded_frames(
    decoder: &mut ffmpeg_next::decoder::Video,
    scaler: &mut ScaleContext,
    time_base: Rational,
    format: &str,
    keyframes_only: bool,
    interval: f64,
    next_threshold: &mut f64,
    max_frames: usize,
    rows: &mut Vec<(f64, Vec<u8>)>,
) {
    let mut decoded = Video::empty();
    while decoder.receive_frame(&mut decoded).is_ok() {
        let timestamp = frame_timestamp_seconds(&decoded, time_base);
        if !should_emit_frame(timestamp, keyframes_only, *next_threshold) {
            continue;
        }

        if rows.len() == max_frames {
            error!(
                "pg_ffmpeg: extract_frames would emit more than max_frames ({max_frames}) rows; increase max_frames or use a larger interval"
            );
        }

        let mut rgb_frame = Video::empty();
        scaler
            .run(&decoded, &mut rgb_frame)
            .unwrap_or_else(|e| error!("scaling error: {e}"));
        rows.push((timestamp, frame_encoder::encode_frame(&rgb_frame, format)));

        if !keyframes_only {
            advance_threshold_past_timestamp(next_threshold, interval, timestamp);
        }
    }
}

fn should_emit_frame(timestamp: f64, keyframes_only: bool, next_threshold: f64) -> bool {
    keyframes_only || timestamp + f64::EPSILON >= next_threshold
}

fn frame_timestamp_seconds(frame: &Video, time_base: Rational) -> f64 {
    let pts = frame
        .pts()
        .or_else(|| frame.timestamp())
        .unwrap_or_else(|| error!("pg_ffmpeg: decoded frame is missing PTS"));
    timestamp_seconds(pts, time_base)
}

fn advance_threshold_past_timestamp(next_threshold: &mut f64, interval: f64, timestamp: f64) {
    while *next_threshold <= timestamp {
        *next_threshold += interval;
    }
}

fn timestamp_seconds(timestamp: i64, time_base: Rational) -> f64 {
    f64::from(time_base.numerator()) * timestamp as f64 / f64::from(time_base.denominator())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::generate_test_video_bytes;

    const PNG_MAGIC: &[u8; 8] = b"\x89PNG\r\n\x1a\n";

    #[pg_test]
    fn test_extract_frames_keyframes_only() {
        let data = generate_test_video_bytes(64, 64, 10, 2);
        let rows = collect_frame_rows(&data, 1.0, "png", true, 1000);

        assert_eq!(
            rows.len(),
            20,
            "all-intra fixture should expose every frame as a keyframe"
        );
        assert!(
            rows.windows(2).all(|pair| pair[0].0 <= pair[1].0),
            "timestamps should be monotonic in decode order"
        );
        assert_eq!(&rows[0].1[..8], PNG_MAGIC, "frame output should be PNG");
    }

    #[pg_test]
    fn test_extract_frames_interval() {
        let mut next_threshold = 0.0;
        let mut emitted = Vec::new();

        for timestamp in [0.0, 0.9, 1.033, 2.0, 2.1, 3.05] {
            if should_emit_frame(timestamp, false, next_threshold) {
                emitted.push(timestamp);
                advance_threshold_past_timestamp(&mut next_threshold, 1.0, timestamp);
            }
        }

        assert_eq!(
            emitted,
            vec![0.0, 1.033, 2.0, 3.05],
            "interval mode should snap to the first decoded frame at or after each fixed threshold"
        );
        assert!(
            (next_threshold - 4.0).abs() < 1e-9,
            "thresholds should advance on the fixed N * interval schedule without drift"
        );
    }

    #[pg_test]
    fn test_extract_frames_max_frames_limit() {
        let data = generate_test_video_bytes(64, 64, 10, 4);
        let result = std::panic::catch_unwind(|| collect_frame_rows(&data, 1.0, "png", true, 3));

        assert!(
            result.is_err(),
            "extract_frames should error instead of silently truncating"
        );
    }

    #[pg_test]
    fn test_extract_frames_invalid_interval_errors() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        let result =
            std::panic::catch_unwind(|| extract_frames(data, 0.0, "png".to_string(), false, 1000));

        assert!(result.is_err(), "non-positive interval should error");
    }

    #[pg_test]
    fn test_frame_timestamp_seconds_uses_frame_pts_directly() {
        let mut frame = Video::empty();
        unsafe {
            (*frame.as_mut_ptr()).pts = 1;
            (*frame.as_mut_ptr()).best_effort_timestamp = 2;
        }

        let timestamp = frame_timestamp_seconds(&frame, Rational(1, 90000));
        assert!(
            (timestamp - (1.0 / 90000.0)).abs() < 1e-12,
            "frame timestamps should use frame PTS directly without packet-duration scaling"
        );
    }

    #[pg_test]
    fn test_extract_frames_keyframes_only_ignores_interval() {
        let data = generate_test_video_bytes(64, 64, 10, 2);
        let default_rows = collect_frame_rows(&data, 1.0, "png", true, 1000);
        let ignored_interval_rows = collect_frame_rows(&data, 99.0, "png", true, 1000);

        assert_eq!(
            ignored_interval_rows.len(),
            default_rows.len(),
            "keyframes_only should ignore interval when choosing rows"
        );
        assert_eq!(
            ignored_interval_rows
                .iter()
                .map(|(timestamp, _)| *timestamp)
                .collect::<Vec<_>>(),
            default_rows
                .iter()
                .map(|(timestamp, _)| *timestamp)
                .collect::<Vec<_>>(),
        );
    }
}
