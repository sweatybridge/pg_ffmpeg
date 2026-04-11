//! `generate_gif` — build an animated image from a video (Task 2A).
//!
//! Converts a bytea video into an animated GIF, APNG, or animated
//! WebP using a built-in filter graph. The filter spec is assembled
//! internally from the numeric parameters, so it is intentionally NOT
//! routed through `filter_safety::validate_filter_spec` — there is no
//! user-supplied filter string to validate (per Task F3 scope).
//!
//! The implementation reuses `pipeline::build_video_filter_graph` for
//! the `buffer → (fps / scale / palettegen) → buffersink` wiring and
//! then manages its own decoder / encoder loop. It cannot use
//! `VideoPipeline::new` from Task F1 directly: that helper reuses the
//! input codec as the output codec, whereas here the output codec is
//! chosen from the requested container (GIF / APNG / WebP).

use ffmpeg_next::codec;
use ffmpeg_next::media::Type;
use ffmpeg_next::util::frame::video::Video;
use ffmpeg_next::{filter, picture, Packet, Rational};
use pgrx::prelude::*;

use crate::codec_lookup::{self, CodecKind};
use crate::mem_io::{MemInput, MemOutput};
use crate::pipeline;

#[pg_extern]
fn generate_gif(
    data: Vec<u8>,
    start_time: default!(f64, 0.0),
    duration: default!(f64, 5.0),
    width: default!(Option<i32>, "NULL"),
    fps: default!(i32, 10),
    format: default!(&str, "'gif'"),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    if fps <= 0 {
        error!("pg_ffmpeg: fps must be > 0");
    }
    if duration <= 0.0 {
        error!("pg_ffmpeg: duration must be > 0");
    }
    if start_time < 0.0 {
        error!("pg_ffmpeg: start_time must be >= 0");
    }
    if let Some(w) = width {
        if w <= 0 {
            error!("pg_ffmpeg: width must be > 0");
        }
    }

    let (encoder_name, muxer_name, filter_spec) = gif_params(format, fps, width);
    let encoder_codec = codec_lookup::find_encoder(encoder_name, CodecKind::Video)
        .unwrap_or_else(|e| error!("{e}"));

    let mut ictx = MemInput::open(&data);
    let mut octx = MemOutput::open(muxer_name);

    let input_stream = ictx
        .streams()
        .best(Type::Video)
        .unwrap_or_else(|| error!("pg_ffmpeg: no video stream in input"));
    let ist_index = input_stream.index();
    let ist_time_base = input_stream.time_base();

    let dec_ctx = codec::context::Context::from_parameters(input_stream.parameters())
        .unwrap_or_else(|e| error!("failed to create decoder context: {e}"));
    let mut decoder = dec_ctx
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open video decoder: {e}"));
    decoder.set_time_base(ist_time_base);

    // Seek to start_time if requested. MemInput::seek takes AV_TIME_BASE units.
    if start_time > 0.0 {
        let target_ts = (start_time * f64::from(ffmpeg_next::ffi::AV_TIME_BASE)) as i64;
        let _ = ictx.seek(target_ts, ..target_ts + 1);
    }

    let mut graph = pipeline::build_video_filter_graph(&decoder, &filter_spec);

    // Read resolved output dimensions / pix_fmt / time-base from the
    // validated buffersink. palettegen+paletteuse leaves the sink
    // configured with PAL8; plain fps/scale + `format=` leaves it with
    // whatever the trailing `format` filter selected.
    let (out_width, out_height, out_pix_fmt, filter_tb) = unsafe {
        let sink_ptr = graph.get("out").unwrap().as_ptr();
        let w = ffmpeg_next::sys::av_buffersink_get_w(sink_ptr) as u32;
        let h = ffmpeg_next::sys::av_buffersink_get_h(sink_ptr) as u32;
        let fmt = ffmpeg_next::sys::av_buffersink_get_format(sink_ptr);
        let tb = ffmpeg_next::sys::av_buffersink_get_time_base(sink_ptr);
        (
            w,
            h,
            ffmpeg_next::format::Pixel::from(std::mem::transmute::<
                i32,
                ffmpeg_next::sys::AVPixelFormat,
            >(fmt)),
            Rational(tb.num, tb.den),
        )
    };

    let enc_ctx = codec::context::Context::new_with_codec(encoder_codec);
    let mut enc_builder = enc_ctx
        .encoder()
        .video()
        .unwrap_or_else(|e| error!("failed to create video encoder: {e}"));
    enc_builder.set_width(out_width);
    enc_builder.set_height(out_height);
    enc_builder.set_format(out_pix_fmt);
    enc_builder.set_frame_rate(Some(Rational(fps, 1)));
    enc_builder.set_time_base(if filter_tb.denominator() != 0 {
        filter_tb
    } else {
        Rational(1, fps)
    });
    if octx
        .format()
        .flags()
        .contains(ffmpeg_next::format::Flags::GLOBAL_HEADER)
    {
        enc_builder.set_flags(codec::Flags::GLOBAL_HEADER);
    }

    let mut encoder = enc_builder
        .open_as(encoder_codec)
        .unwrap_or_else(|e| error!("failed to open encoder: {e}"));

    let ost_index = {
        let mut ost = octx
            .add_stream(encoder_codec)
            .unwrap_or_else(|e| error!("failed to add output stream: {e}"));
        ost.set_parameters(&encoder);
        ost.index()
    };

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write header: {e}"));
    let ost_time_base = octx.stream(ost_index).unwrap().time_base();

    // [start_pts, end_pts) in the stream's own time base.
    let (start_pts, end_pts) = pts_range(start_time, duration, ist_time_base);

    // The ctx block keeps the mutable borrows of decoder/graph/encoder/octx
    // scoped so that `octx.write_trailer()` below can reacquire its own
    // mutable borrow after ctx is dropped.
    {
        let mut ctx = PipelineCtx {
            decoder: &mut decoder,
            graph: &mut graph,
            encoder: &mut encoder,
            octx: &mut octx,
            ost_index,
            filter_tb,
            ost_time_base,
            start_pts,
            end_pts,
        };

        'outer: for (stream, packet) in ictx.packets() {
            if stream.index() != ist_index {
                continue;
            }
            ctx.decoder
                .send_packet(&packet)
                .unwrap_or_else(|e| error!("decode error: {e}"));
            if pump_decoder(&mut ctx) {
                break 'outer;
            }
        }

        // Flush decoder → filter.
        let _ = ctx.decoder.send_eof();
        let _ = pump_decoder(&mut ctx);

        // Flush the filter graph. palettegen only emits its palette
        // once the source reports EOF, so paletteuse cannot produce any
        // output frames until this point.
        let _ = ctx.graph.get("in").unwrap().source().flush();
        drain_filter(&mut ctx);

        // Flush encoder.
        let _ = ctx.encoder.send_eof();
        drain_encoder(&mut ctx);
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));

    octx.into_data()
}

/// Translate the [start, end) seconds range into stream-time-base PTS
/// values. Returns `(i64::MIN, i64::MAX)` when the stream's time base
/// is degenerate so the caller falls back to "process everything".
fn pts_range(start_time: f64, duration: f64, tb: Rational) -> (i64, i64) {
    if tb.denominator() == 0 || tb.numerator() == 0 {
        return (i64::MIN, i64::MAX);
    }
    let to_pts = |secs: f64| -> i64 {
        (secs * f64::from(tb.denominator()) / f64::from(tb.numerator())) as i64
    };
    let start = if start_time > 0.0 {
        to_pts(start_time)
    } else {
        i64::MIN
    };
    let end = to_pts(start_time + duration);
    (start, end)
}

struct PipelineCtx<'a> {
    decoder: &'a mut ffmpeg_next::decoder::Video,
    graph: &'a mut filter::Graph,
    encoder: &'a mut ffmpeg_next::encoder::Video,
    octx: &'a mut ffmpeg_next::format::context::Output,
    ost_index: usize,
    filter_tb: Rational,
    ost_time_base: Rational,
    start_pts: i64,
    end_pts: i64,
}

/// Drain decoded frames from the decoder, push each one through the
/// filter graph source, and opportunistically drain the filter sink.
///
/// Returns `true` once a decoded frame at or past `end_pts` is seen —
/// the caller should stop feeding more packets into the decoder.
fn pump_decoder(ctx: &mut PipelineCtx) -> bool {
    let mut decoded = Video::empty();
    while ctx.decoder.receive_frame(&mut decoded).is_ok() {
        if let Some(pts) = decoded.timestamp() {
            if pts >= ctx.end_pts {
                return true;
            }
            if pts < ctx.start_pts {
                continue;
            }
        }
        let ts = decoded.timestamp();
        decoded.set_pts(ts);
        decoded.set_kind(picture::Type::None);
        ctx.graph
            .get("in")
            .unwrap()
            .source()
            .add(&decoded)
            .unwrap_or_else(|e| error!("filter source error: {e}"));
        drain_filter(ctx);
    }
    false
}

/// Pull any ready frames out of the filter sink and hand them to the
/// encoder, then drain the encoder.
fn drain_filter(ctx: &mut PipelineCtx) {
    let mut filtered = Video::empty();
    while ctx
        .graph
        .get("out")
        .unwrap()
        .sink()
        .frame(&mut filtered)
        .is_ok()
    {
        ctx.encoder
            .send_frame(&filtered)
            .unwrap_or_else(|e| error!("encode error: {e}"));
        drain_encoder(ctx);
    }
}

/// Pull any ready packets out of the encoder and write them to the
/// output muxer.
fn drain_encoder(ctx: &mut PipelineCtx) {
    let mut packet = Packet::empty();
    while ctx.encoder.receive_packet(&mut packet).is_ok() {
        packet.set_stream(ctx.ost_index);
        packet.rescale_ts(ctx.filter_tb, ctx.ost_time_base);
        packet.set_position(-1);
        packet
            .write_interleaved(&mut *ctx.octx)
            .unwrap_or_else(|e| error!("failed to write packet: {e}"));
    }
}

/// Resolve (encoder_name, muxer_name, filter_spec) for the requested
/// output format. Filter specs here are built from numeric parameters
/// only — there is no user-supplied filter string to validate.
fn gif_params(format: &str, fps: i32, width: Option<i32>) -> (&'static str, &'static str, String) {
    let scale_prefix = match width {
        Some(w) => format!("scale={}:-1:flags=lanczos,", w),
        None => String::new(),
    };
    match format {
        "gif" => {
            let spec = format!(
                "fps={fps},{scale_prefix}split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse",
            );
            ("gif", "gif", spec)
        }
        "apng" => {
            // APNG encoder wants rgb24/rgba; the source is usually
            // YUV420P so add an explicit format conversion.
            let spec = format!("fps={fps},{scale_prefix}format=rgb24");
            ("apng", "apng", spec)
        }
        "webp" => {
            // libwebp accepts yuv420p/yuva420p; pin to yuv420p for
            // the widest compatibility.
            let spec = format!("fps={fps},{scale_prefix}format=yuv420p");
            ("libwebp", "webp", spec)
        }
        other => error!(
            "pg_ffmpeg: unsupported generate_gif format '{}', expected 'gif' | 'apng' | 'webp'",
            other
        ),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::generate_test_video_bytes;

    #[pg_test]
    fn test_generate_gif() {
        // 5-second all-intra test video gives us enough frames for a
        // meaningful palette.
        let data = generate_test_video_bytes(64, 64, 25, 5);
        let gif = generate_gif(data, 0.0, 5.0, None, 10, "gif");
        assert!(!gif.is_empty(), "gif bytes should be non-empty");
        assert!(
            gif.starts_with(b"GIF87a") || gif.starts_with(b"GIF89a"),
            "expected GIF header, got {:?}",
            &gif[..gif.len().min(6)]
        );
    }

    #[pg_test]
    fn test_generate_gif_with_width() {
        let data = generate_test_video_bytes(64, 64, 25, 5);
        let gif = generate_gif(data, 0.0, 2.0, Some(32), 10, "gif");
        assert!(
            gif.starts_with(b"GIF87a") || gif.starts_with(b"GIF89a"),
            "expected GIF header when width is specified"
        );
        // Logical screen descriptor width at offset 6..8 (little-endian u16).
        let width = u16::from_le_bytes([gif[6], gif[7]]);
        assert_eq!(width, 32, "expected GIF canvas width 32, got {width}");
    }

    #[pg_test]
    fn test_generate_apng() {
        let data = generate_test_video_bytes(64, 64, 25, 5);
        let apng = generate_gif(data, 0.0, 2.0, Some(32), 10, "apng");
        assert!(!apng.is_empty(), "apng bytes should be non-empty");
        assert_eq!(&apng[..8], b"\x89PNG\r\n\x1a\n", "expected PNG signature");
        // APNG adds an `acTL` animation control chunk near the start
        // of the file. Searching the first 128 bytes is plenty: the
        // chunk always appears right after IHDR.
        let head = &apng[..apng.len().min(128)];
        assert!(
            head.windows(4).any(|w| w == b"acTL"),
            "expected APNG acTL chunk in early bytes"
        );
    }

    #[pg_test]
    fn test_generate_gif_rejects_invalid_fps() {
        let data = generate_test_video_bytes(64, 64, 25, 1);
        let result = std::panic::catch_unwind(|| generate_gif(data, 0.0, 1.0, None, 0, "gif"));
        assert!(result.is_err(), "fps = 0 should error");
    }

    #[pg_test]
    fn test_generate_gif_rejects_unknown_format() {
        let data = generate_test_video_bytes(64, 64, 25, 1);
        let result = std::panic::catch_unwind(|| generate_gif(data, 0.0, 1.0, None, 10, "mp4"));
        assert!(result.is_err(), "unknown format should error");
    }
}
