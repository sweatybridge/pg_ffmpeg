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
use ffmpeg_next::format::{sample::Type as SampleType, Pixel, Sample};
use ffmpeg_next::util::frame::{audio::Audio as AudioFrame, video::Video};

use crate::mem_io::MemOutput;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

pub const LARGE_ALLOC_THRESHOLD: usize = 256 * 1024;

thread_local! {
    static COUNTING_DEPTH: Cell<u32> = const { Cell::new(0) };
    static LARGE_ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
}

pub struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn note_large_alloc(size: usize) {
    if size < LARGE_ALLOC_THRESHOLD {
        return;
    }
    COUNTING_DEPTH.with(|depth| {
        if depth.get() > 0 {
            LARGE_ALLOC_COUNT.with(|count| count.set(count.get().saturating_add(1)));
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_large_alloc(layout.size());
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_large_alloc(layout.size());
        System.alloc_zeroed(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_large_alloc(new_size);
        System.realloc(ptr, layout, new_size)
    }
}

pub fn count_large_allocs<T>(f: impl FnOnce() -> T) -> (usize, T) {
    struct ScopeGuard;

    impl Drop for ScopeGuard {
        fn drop(&mut self) {
            COUNTING_DEPTH.with(|depth| {
                let current = depth.get();
                if current == 0 {
                    return;
                }
                depth.set(current - 1);
                if current == 1 {
                    LARGE_ALLOC_COUNT.with(|count| count.set(0));
                }
            });
        }
    }

    COUNTING_DEPTH.with(|depth| {
        if depth.get() == 0 {
            LARGE_ALLOC_COUNT.with(|count| count.set(0));
        }
        depth.set(depth.get() + 1);
    });
    let guard = ScopeGuard;

    let result = match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(panic) => {
            drop(guard);
            resume_unwind(panic);
        }
    };

    let count = LARGE_ALLOC_COUNT.with(|tracked| tracked.get());
    drop(guard);
    (count, result)
}

pub fn assert_large_allocs_at_most<T>(n: usize, f: impl FnOnce() -> T) -> T {
    let (count, result) = count_large_allocs(f);
    assert!(
        count <= n,
        "expected at most {n} allocations >= {LARGE_ALLOC_THRESHOLD} bytes, saw {count}"
    );
    result
}

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

/// Generate a minimal MPEG-TS asset with one video stream and one AAC
/// audio stream.
pub fn generate_test_video_with_audio_bytes(
    width: u32,
    height: u32,
    fps: i32,
    duration_secs: i32,
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    let total_frames = fps * duration_secs;
    let video_codec =
        ffmpeg_next::encoder::find(codec::Id::MPEG2VIDEO).expect("MPEG2VIDEO encoder not found");
    let audio_codec = ffmpeg_next::encoder::find(codec::Id::AAC).expect("AAC encoder not found");

    let mut octx = MemOutput::open("mpegts");
    let global_header = octx
        .format()
        .flags()
        .contains(ffmpeg_next::format::Flags::GLOBAL_HEADER);

    let mut video_stream = octx.add_stream(video_codec).expect("failed to add video stream");
    video_stream.set_time_base((1, fps));

    let video_ctx = codec::context::Context::new_with_codec(video_codec);
    let mut video_encoder = video_ctx
        .encoder()
        .video()
        .expect("failed to create video encoder");
    video_encoder.set_width(width);
    video_encoder.set_height(height);
    video_encoder.set_format(Pixel::YUV420P);
    video_encoder.set_bit_rate(400_000);
    video_encoder.set_gop(1);
    video_encoder.set_max_b_frames(0);
    video_encoder.set_frame_rate(Some((fps, 1)));
    video_encoder.set_time_base((1, fps));
    if global_header {
        video_encoder.set_flags(codec::Flags::GLOBAL_HEADER);
    }

    let mut video_encoder = video_encoder
        .open_as(video_codec)
        .expect("failed to open video encoder");
    video_stream.set_parameters(&video_encoder);
    let video_out_time_base = video_stream.time_base();
    drop(video_stream);

    let mut audio_stream = octx.add_stream(audio_codec).expect("failed to add audio stream");
    let audio_props = audio_codec.audio().expect("AAC codec is not an audio encoder");
    let sample_rate = audio_props
        .rates()
        .and_then(|mut rates| rates.next())
        .unwrap_or(48_000);
    let channel_layout = audio_props
        .channel_layouts()
        .map(|layouts| layouts.best(2))
        .unwrap_or(ffmpeg_next::ChannelLayout::STEREO);
    let sample_format = audio_props
        .formats()
        .and_then(|mut formats| formats.next())
        .unwrap_or(Sample::I16(SampleType::Packed));

    audio_stream.set_time_base((1, sample_rate));

    let audio_ctx = codec::context::Context::new_with_codec(audio_codec);
    let mut audio_encoder = audio_ctx
        .encoder()
        .audio()
        .expect("failed to create audio encoder");
    audio_encoder.set_rate(sample_rate);
    audio_encoder.set_channel_layout(channel_layout);
    audio_encoder.set_format(sample_format);
    audio_encoder.set_bit_rate(128_000);
    audio_encoder.set_time_base((1, sample_rate));
    if global_header {
        audio_encoder.set_flags(codec::Flags::GLOBAL_HEADER);
    }

    let mut audio_encoder = audio_encoder
        .open_as(audio_codec)
        .expect("failed to open audio encoder");
    audio_stream.set_parameters(&audio_encoder);
    let audio_out_time_base = audio_stream.time_base();
    drop(audio_stream);

    octx.write_header().expect("failed to write header");

    let mut video_packet = ffmpeg_next::Packet::empty();
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

        video_encoder
            .send_frame(&frame)
            .expect("failed to send video frame");
        while video_encoder.receive_packet(&mut video_packet).is_ok() {
            video_packet.set_stream(0);
            video_packet.rescale_ts((1, fps), video_out_time_base);
            video_packet
                .write_interleaved(&mut *octx)
                .expect("failed to write video packet");
        }
    }

    video_encoder.send_eof().expect("failed to send video eof");
    while video_encoder.receive_packet(&mut video_packet).is_ok() {
        video_packet.set_stream(0);
        video_packet.rescale_ts((1, fps), video_out_time_base);
        video_packet
            .write_interleaved(&mut *octx)
            .expect("failed to flush video packet");
    }

    let mut audio_packet = ffmpeg_next::Packet::empty();
    let mut samples_per_frame = audio_encoder.frame_size() as usize;
    if samples_per_frame == 0 {
        samples_per_frame = 1024;
    }
    let total_audio_samples =
        (duration_secs as usize).saturating_mul(sample_rate as usize) / samples_per_frame
            * samples_per_frame;
    let total_audio_samples = total_audio_samples.max(samples_per_frame);

    let mut next_audio_pts = 0usize;
    while next_audio_pts < total_audio_samples {
        let mut frame = AudioFrame::new(sample_format, samples_per_frame, channel_layout);
        frame.set_rate(sample_rate as u32);
        frame.set_pts(Some(next_audio_pts as i64));
        for plane in 0..frame.planes() {
            for byte in frame.data_mut(plane).iter_mut() {
                *byte = 0;
            }
        }

        audio_encoder
            .send_frame(&frame)
            .expect("failed to send audio frame");
        while audio_encoder.receive_packet(&mut audio_packet).is_ok() {
            audio_packet.set_stream(1);
            audio_packet.rescale_ts((1, sample_rate), audio_out_time_base);
            audio_packet
                .write_interleaved(&mut *octx)
                .expect("failed to write audio packet");
        }
        next_audio_pts += samples_per_frame;
    }

    audio_encoder.send_eof().expect("failed to send audio eof");
    while audio_encoder.receive_packet(&mut audio_packet).is_ok() {
        audio_packet.set_stream(1);
        audio_packet.rescale_ts((1, sample_rate), audio_out_time_base);
        audio_packet
            .write_interleaved(&mut *octx)
            .expect("failed to flush audio packet");
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
