//! Shared test fixtures and utilities (Task F6).
//!
//! Every module's tests need a reliable way to materialize a small,
//! well-formed piece of media in-memory without touching the filesystem.
//! Before this module, each file grew its own copy of
//! `generate_test_video_bytes()`; those copies drifted and produced
//! subtly different outputs. Consolidating here keeps the test corpus
//! identical across modules and keeps the behavior of
//! `CountingAllocator` consistent for zero-copy boundary tests.
//!
//! Gated behind `#[cfg(any(test, feature = "pg_test"))]` so it never
//! ships in a release build.

#![cfg(any(test, feature = "pg_test"))]
// `generate_test_image_bytes` is only called from Milestone 1 / 2
// tests. Milestone F ships the helper so those tasks have a stable
// target.
#![allow(dead_code)]

use ffmpeg_next::codec;
use ffmpeg_next::format::Pixel;
use ffmpeg_next::util::frame::video::Video;

use crate::mem_io::MemOutput;

/// Generate a minimal MPEG-TS video (video stream only) in memory.
///
/// All-intra encoding: every frame is a keyframe so seeks land exactly
/// on the requested timestamp. This matches the old per-file helper's
/// behavior in `thumbnail.rs` (the stricter of the two existing
/// flavors).
pub fn generate_test_video_bytes(width: u32, height: u32, fps: i32, duration_secs: i32) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    let total_frames = fps * duration_secs;
    let enc_codec =
        ffmpeg_next::encoder::find(codec::Id::MPEG2VIDEO).expect("MPEG2VIDEO encoder not found");

    let mut octx = MemOutput::open("mpegts");

    let mut stream = octx.add_stream(enc_codec).expect("failed to add stream");
    stream.set_time_base((1, fps));

    let ctx = codec::context::Context::new_with_codec(enc_codec);
    let mut encoder = ctx.encoder().video().expect("failed to create encoder");
    encoder.set_width(width);
    encoder.set_height(height);
    encoder.set_format(Pixel::YUV420P);
    encoder.set_bit_rate(400_000);
    encoder.set_gop(1);
    encoder.set_max_b_frames(0);
    encoder.set_frame_rate(Some((fps, 1)));
    encoder.set_time_base((1, fps));

    let mut encoder = encoder.open().expect("failed to open encoder");
    stream.set_parameters(&encoder);
    let out_time_base = stream.time_base();
    drop(stream);

    octx.write_header().expect("failed to write header");

    let mut packet = ffmpeg_next::Packet::empty();
    for i in 0..total_frames {
        let mut frame = Video::new(Pixel::YUV420P, width, height);
        let y_data = frame.data_mut(0);
        for (j, byte) in y_data.iter_mut().enumerate() {
            *byte = ((i as usize * 3 + j) % 256) as u8;
        }
        for plane in 1..=2 {
            for byte in frame.data_mut(plane).iter_mut() {
                *byte = 128;
            }
        }
        frame.set_pts(Some(i as i64));

        encoder.send_frame(&frame).expect("failed to send frame");
        while encoder.receive_packet(&mut packet).is_ok() {
            packet.set_stream(0);
            packet.rescale_ts((1, fps), out_time_base);
            packet
                .write_interleaved(&mut *octx)
                .expect("failed to write packet");
        }
    }

    encoder.send_eof().expect("failed to send eof");
    while encoder.receive_packet(&mut packet).is_ok() {
        packet.set_stream(0);
        packet.rescale_ts((1, fps), out_time_base);
        packet
            .write_interleaved(&mut *octx)
            .expect("failed to write packet");
    }

    octx.write_trailer().expect("failed to write trailer");
    octx.into_data()
}

/// Generate a single-frame PNG or JPEG in memory.
///
/// Useful for tests that need to feed an image into `transcode` /
/// `encode` without pulling in a real fixture file.
pub fn generate_test_image_bytes(format: &str, width: u32, height: u32) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    let (codec_id, pix_fmt, muxer) = match format {
        "png" => (codec::Id::PNG, Pixel::RGB24, "image2pipe"),
        "jpeg" | "jpg" => (codec::Id::MJPEG, Pixel::YUVJ420P, "image2pipe"),
        _ => panic!("unsupported test image format: {}", format),
    };

    let enc_codec =
        ffmpeg_next::encoder::find(codec_id).expect("image encoder not found in FFmpeg build");

    let mut octx = MemOutput::open(muxer);
    let mut stream = octx.add_stream(enc_codec).expect("failed to add stream");
    stream.set_time_base((1, 1));

    let ctx = codec::context::Context::new_with_codec(enc_codec);
    let mut encoder = ctx.encoder().video().expect("failed to create encoder");
    encoder.set_width(width);
    encoder.set_height(height);
    encoder.set_format(pix_fmt);
    encoder.set_time_base((1, 1));

    let mut encoder = encoder.open().expect("failed to open image encoder");
    stream.set_parameters(&encoder);
    let out_time_base = stream.time_base();
    drop(stream);

    octx.write_header().expect("failed to write header");

    let mut frame = Video::new(pix_fmt, width, height);
    // Fill with a recognizable pattern so tests can eyeball round-trips.
    for plane in 0..frame.planes() {
        for byte in frame.data_mut(plane).iter_mut() {
            *byte = 128;
        }
    }
    frame.set_pts(Some(0));

    encoder.send_frame(&frame).expect("failed to send frame");
    encoder.send_eof().expect("failed to send eof");

    let mut packet = ffmpeg_next::Packet::empty();
    while encoder.receive_packet(&mut packet).is_ok() {
        packet.set_stream(0);
        packet.rescale_ts((1, 1), out_time_base);
        packet
            .write_interleaved(&mut *octx)
            .expect("failed to write packet");
    }

    octx.write_trailer().expect("failed to write trailer");
    octx.into_data()
}
