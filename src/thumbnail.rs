use pgrx::prelude::*;

use ffmpeg_next::format::Pixel;
use ffmpeg_next::media::Type;
use ffmpeg_next::software::scaling::{context::Context, flag::Flags};
use ffmpeg_next::util::frame::video::Video;

use crate::frame_encoder::encode_frame;
use crate::mem_io::MemInput;

#[pg_extern]
fn thumbnail(
    data: Vec<u8>,
    seconds: default!(f64, 0.0),
    format: default!(String, "'png'"),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    let mut ictx = MemInput::open(&data);

    let input = ictx
        .streams()
        .best(Type::Video)
        .unwrap_or_else(|| error!("no video stream found"));
    let video_stream_index = input.index();
    let time_base = input.time_base();

    let context_decoder = ffmpeg_next::codec::context::Context::from_parameters(input.parameters())
        .unwrap_or_else(|e| error!("failed to create decoder context: {e}"));
    let mut decoder = context_decoder
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open video decoder: {e}"));

    // Seek to the target time if > 0
    if seconds > 0.0 {
        let target_ts = (seconds * f64::from(ffmpeg_next::ffi::AV_TIME_BASE)) as i64;
        let _ = ictx.seek(target_ts, ..target_ts + 1);
    }

    let mut scaler = Context::get(
        decoder.format(),
        decoder.width(),
        decoder.height(),
        Pixel::RGB24,
        decoder.width(),
        decoder.height(),
        Flags::BILINEAR,
    )
    .unwrap_or_else(|e| error!("failed to create scaler: {e}"));

    let target_pts = if time_base.denominator() != 0 {
        Some(
            (seconds * f64::from(time_base.denominator()) / f64::from(time_base.numerator()))
                as i64,
        )
    } else {
        None
    };

    let mut result_frame: Option<Video> = None;

    for (stream, packet) in ictx.packets() {
        if stream.index() == video_stream_index {
            decoder
                .send_packet(&packet)
                .unwrap_or_else(|e| error!("decode error: {e}"));

            let mut decoded = Video::empty();
            while decoder.receive_frame(&mut decoded).is_ok() {
                // Take the first frame at or after target time
                if let Some(target) = target_pts {
                    if let Some(pts) = decoded.timestamp() {
                        if pts < target {
                            continue;
                        }
                    }
                }
                let mut rgb_frame = Video::empty();
                scaler
                    .run(&decoded, &mut rgb_frame)
                    .unwrap_or_else(|e| error!("scaling error: {e}"));
                result_frame = Some(rgb_frame);
                break;
            }
            if result_frame.is_some() {
                break;
            }
        }
    }

    // If we haven't found a frame yet, flush the decoder
    if result_frame.is_none() {
        let _ = decoder.send_eof();
        let mut decoded = Video::empty();
        if decoder.receive_frame(&mut decoded).is_ok() {
            let mut rgb_frame = Video::empty();
            scaler
                .run(&decoded, &mut rgb_frame)
                .unwrap_or_else(|e| error!("scaling error: {e}"));
            result_frame = Some(rgb_frame);
        }
    }

    let frame = result_frame.unwrap_or_else(|| error!("no video frame could be decoded"));

    encode_frame(&frame, &format)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::generate_test_video_bytes;

    #[pg_test]
    fn test_thumbnail_at_one_second() {
        // 5-second video gives the seek+decode path room to land well before EOF.
        let data = generate_test_video_bytes(64, 64, 25, 5);
        let png = thumbnail(data, 1.0, "png".to_string());
        assert!(!png.is_empty(), "thumbnail bytes should be non-empty");
        assert_eq!(&png[..8], b"\x89PNG\r\n\x1a\n", "output should be a PNG");
    }
}

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use crate::bench_common::{generate_sample_video, sample_video_bytes};
    use pgrx::pg_bench;
    use pgrx_bench::{black_box, Bencher};

    #[pg_bench(setup = generate_sample_video)]
    fn bench_thumbnail(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || black_box(super::thumbnail(data.clone(), 0.0, "png".to_string())));
    }
}
