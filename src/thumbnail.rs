use pgrx::prelude::*;

use ffmpeg_next::format::Pixel;
use ffmpeg_next::media::Type;
use ffmpeg_next::software::scaling::{context::Context, flag::Flags};
use ffmpeg_next::util::frame::video::Video;

use crate::write_to_tempfile;

#[pg_extern]
fn thumbnail(data: Vec<u8>, seconds: default!(f64, 0.0)) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    let tmp = write_to_tempfile(&data, ".video")
        .unwrap_or_else(|e| error!("failed to write temp file: {e}"));

    let mut ictx = ffmpeg_next::format::input(tmp.path())
        .unwrap_or_else(|e| error!("failed to open media: {e}"));

    let input = ictx
        .streams()
        .best(Type::Video)
        .unwrap_or_else(|| error!("no video stream found"));
    let video_stream_index = input.index();
    let time_base = input.time_base();

    let context_decoder =
        ffmpeg_next::codec::context::Context::from_parameters(input.parameters())
            .unwrap_or_else(|e| error!("failed to create decoder context: {e}"));
    let mut decoder = context_decoder
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open video decoder: {e}"));

    // Seek to the target time if > 0
    if seconds > 0.0 {
        let target_ts =
            (seconds * f64::from(ffmpeg_next::ffi::AV_TIME_BASE)) as i64;
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
        Some((seconds * f64::from(time_base.denominator()) / f64::from(time_base.numerator())) as i64)
    } else {
        None
    };

    let mut result_frame: Option<Video> = None;

    for (stream, packet) in ictx.packets() {
        if stream.index() == video_stream_index {
            decoder.send_packet(&packet).unwrap_or_else(|e| error!("decode error: {e}"));

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
        while decoder.receive_frame(&mut decoded).is_ok() {
            let mut rgb_frame = Video::empty();
            scaler
                .run(&decoded, &mut rgb_frame)
                .unwrap_or_else(|e| error!("scaling error: {e}"));
            result_frame = Some(rgb_frame);
            break;
        }
    }

    let frame = result_frame.unwrap_or_else(|| error!("no video frame could be decoded"));

    // Encode as PPM (simple uncompressed format that's easy to produce)
    // PPM format: "P6\n{width} {height}\n255\n" + raw RGB data
    let header = format!("P6\n{} {}\n255\n", frame.width(), frame.height());
    let mut output = Vec::with_capacity(header.len() + (frame.width() * frame.height() * 3) as usize);
    output.extend_from_slice(header.as_bytes());
    // RGB24 data may have padding per row (stride != width*3)
    let stride = frame.stride(0);
    let width_bytes = frame.width() as usize * 3;
    let data = frame.data(0);
    for y in 0..frame.height() as usize {
        let row_start = y * stride;
        output.extend_from_slice(&data[row_start..row_start + width_bytes]);
    }
    output
}
