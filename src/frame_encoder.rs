//! Shared single-frame image encoder (Task F1 extraction).
//!
//! `thumbnail` and `extract_frames` both need to turn an RGB24 `Video`
//! frame into a PNG / JPEG / PPM byte buffer. Until Milestone 1, this
//! helper lived inside `thumbnail.rs` and `extract_frames` reached
//! across module boundaries to call it. The PLAN calls this out as
//! exactly the kind of cross-function coupling F1 is meant to remove,
//! so the helper now lives here and both call sites import it.

use ffmpeg_next::format::Pixel;
use ffmpeg_next::software::scaling::{context::Context, flag::Flags};
use ffmpeg_next::util::frame::video::Video;
use pgrx::prelude::*;

use crate::codec_lookup::{self, CodecKind};
use crate::limits;
use crate::mem_io::MemOutput;

/// Encode a single RGB24 video frame into the requested image format.
///
/// Supported formats are `"png"`, `"jpeg"` / `"jpg"`, and `"ppm"`. Any
/// other format string triggers an ERROR.
pub fn encode_frame(frame: &Video, format: &str) -> Vec<u8> {
    let (codec_name, pixel_fmt) = match format {
        "ppm" => return encode_ppm(frame),
        "png" => ("png", Pixel::RGB24),
        "jpeg" | "jpg" => ("mjpeg", Pixel::YUVJ420P),
        _ => error!("unsupported thumbnail format: {format}"),
    };

    let mut converted = Video::empty();
    let enc_frame = if pixel_fmt != Pixel::RGB24 {
        let mut conv = Context::get(
            Pixel::RGB24,
            frame.width(),
            frame.height(),
            pixel_fmt,
            frame.width(),
            frame.height(),
            Flags::BILINEAR,
        )
        .unwrap_or_else(|e| error!("failed to create pixel format converter: {e}"));
        conv.run(frame, &mut converted)
            .unwrap_or_else(|e| error!("pixel format conversion error: {e}"));
        &converted
    } else {
        frame
    };

    let codec =
        codec_lookup::find_encoder(codec_name, CodecKind::Video).unwrap_or_else(|e| error!("{e}"));
    let ctx = ffmpeg_next::codec::context::Context::new_with_codec(codec);
    let mut encoder = ctx
        .encoder()
        .video()
        .unwrap_or_else(|e| error!("failed to create encoder: {e}"));
    encoder.set_width(enc_frame.width());
    encoder.set_height(enc_frame.height());
    encoder.set_format(pixel_fmt);
    encoder.set_time_base(ffmpeg_next::Rational::new(1, 25));
    let mut encoder = encoder
        .open_as(codec)
        .unwrap_or_else(|e| error!("failed to open encoder: {e}"));
    let mut octx = MemOutput::open("image2pipe");
    let out_time_base = {
        let mut stream = octx
            .add_stream(codec)
            .unwrap_or_else(|e| error!("failed to add thumbnail stream: {e}"));
        stream.set_time_base((1, 25));
        stream.set_parameters(&encoder);
        stream.time_base()
    };

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write thumbnail header: {e}"));

    encoder
        .send_frame(enc_frame)
        .unwrap_or_else(|e| error!("encode send_frame error: {e}"));
    encoder
        .send_eof()
        .unwrap_or_else(|e| error!("encode send_eof error: {e}"));

    let mut packet = ffmpeg_next::Packet::empty();
    while encoder.receive_packet(&mut packet).is_ok() {
        packet.set_stream(0);
        packet.rescale_ts((1, 25), out_time_base);
        packet
            .write_interleaved(&mut octx)
            .unwrap_or_else(|e| error!("encode write_interleaved error: {e}"));
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write thumbnail trailer: {e}"));
    octx.into_data()
}

fn encode_ppm(frame: &Video) -> Vec<u8> {
    let header = format!("P6\n{} {}\n255\n", frame.width(), frame.height());
    let total_len = header.len() + (frame.width() * frame.height() * 3) as usize;
    limits::check_output_size(total_len).unwrap_or_else(|e| error!("{e}"));
    let mut output = Vec::with_capacity(total_len);
    output.extend_from_slice(header.as_bytes());
    let stride = frame.stride(0);
    let width_bytes = frame.width() as usize * 3;
    let data = frame.data(0);
    for y in 0..frame.height() as usize {
        let row_start = y * stride;
        output.extend_from_slice(&data[row_start..row_start + width_bytes]);
    }
    output
}
