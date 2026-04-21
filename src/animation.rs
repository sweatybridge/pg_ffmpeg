use pgrx::prelude::*;

use ffmpeg_next::codec;
use ffmpeg_next::format::Pixel;
use ffmpeg_next::media::Type;
use ffmpeg_next::software::scaling::{context::Context as ScaleContext, flag::Flags};
use ffmpeg_next::{Packet, Rational};

use crate::codec_lookup::{self, CodecKind};
use crate::mem_io::{MemInput, MemOutput};

#[pg_extern]
fn generate_gif(
    data: Vec<u8>,
    start_time: default!(f64, 0.0),
    duration: default!(f64, 5.0),
    width: default!(Option<i32>, "NULL"),
    fps: default!(i32, 10),
    format: default!(String, "'gif'"),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();
    validate_args(start_time, duration, width, fps, &format);

    let frame = decode_frame_at(&data, start_time);
    encode_animation_frame(frame, width, fps, duration, &format)
}

fn decode_frame_at(data: &[u8], start_time: f64) -> ffmpeg_next::frame::Video {
    let mut ictx = MemInput::open(data);
    let input = ictx
        .streams()
        .best(Type::Video)
        .unwrap_or_else(|| error!("pg_ffmpeg: no video stream found"));
    let stream_index = input.index();
    let time_base = input.time_base();
    let decoder_ctx = codec::context::Context::from_parameters(input.parameters())
        .unwrap_or_else(|e| error!("failed to create video decoder context: {e}"));
    let mut decoder = decoder_ctx
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open video decoder: {e}"));

    if start_time > 0.0 {
        let target_ts = (start_time * f64::from(ffmpeg_next::ffi::AV_TIME_BASE)) as i64;
        let _ = ictx.seek(target_ts, ..target_ts + 1);
    }

    let target_pts = if time_base.denominator() != 0 {
        Some(
            (start_time * f64::from(time_base.denominator()) / f64::from(time_base.numerator()))
                as i64,
        )
    } else {
        None
    };

    let mut decoded = ffmpeg_next::frame::Video::empty();
    for (stream, packet) in ictx.packets() {
        if stream.index() != stream_index {
            continue;
        }
        decoder
            .send_packet(&packet)
            .unwrap_or_else(|e| error!("video decode error: {e}"));
        while decoder.receive_frame(&mut decoded).is_ok() {
            if let Some(target) = target_pts {
                if decoded.timestamp().is_some_and(|pts| pts < target) {
                    continue;
                }
            }
            decoded.set_pts(None);
            return decoded;
        }
    }

    let _ = decoder.send_eof();
    if decoder.receive_frame(&mut decoded).is_ok() {
        decoded.set_pts(None);
        return decoded;
    }

    error!("pg_ffmpeg: no video frame could be decoded");
}

fn encode_animation_frame(
    frame: ffmpeg_next::frame::Video,
    width: Option<i32>,
    fps: i32,
    duration: f64,
    format: &str,
) -> Vec<u8> {
    let (codec_name, muxer) = match format {
        "gif" => ("gif", "gif"),
        "apng" => ("apng", "apng"),
        "webp" => ("libwebp_anim", "webp"),
        _ => unreachable!(),
    };

    let selected_codec = codec_lookup::find_encoder(codec_name, CodecKind::Video)
        .or_else(|_| codec_lookup::find_encoder("libwebp", CodecKind::Video))
        .unwrap_or_else(|e| error!("{e}"));
    let pix_fmt = choose_animation_pixel_format(selected_codec, format, frame.format());
    let frame = scale_animation_frame(frame, width, pix_fmt);

    let mut octx = MemOutput::open(muxer);
    let time_base = Rational::new(1, fps);
    let ctx = codec::context::Context::new_with_codec(selected_codec);
    let mut encoder = ctx
        .encoder()
        .video()
        .unwrap_or_else(|e| error!("failed to create animation encoder: {e}"));
    encoder.set_width(frame.width());
    encoder.set_height(frame.height());
    encoder.set_format(pix_fmt);
    encoder.set_time_base(time_base);
    encoder.set_frame_rate(Some(Rational::new(fps, 1)));
    if octx
        .format()
        .flags()
        .contains(ffmpeg_next::format::Flags::GLOBAL_HEADER)
    {
        encoder.set_flags(codec::Flags::GLOBAL_HEADER);
    }
    let mut encoder = encoder.open_as(selected_codec).unwrap_or_else(|e| {
        error!(
            "{}",
            codec_lookup::open_failed(codec_name, CodecKind::Video, e)
        )
    });

    let out_time_base = {
        let mut stream = octx
            .add_stream(selected_codec)
            .unwrap_or_else(|e| error!("failed to add animation stream: {e}"));
        stream.set_time_base(time_base);
        stream.set_parameters(&encoder);
        unsafe {
            (*stream.parameters().as_mut_ptr()).codec_tag = 0;
        }
        stream.time_base()
    };

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write animation header: {e}"));

    let mut packet = Packet::empty();
    let frame_count = ((duration * f64::from(fps)).ceil() as i64).max(1);
    for pts in 0..frame_count {
        let mut frame = frame.clone();
        frame.set_pts(Some(pts));
        encoder
            .send_frame(&frame)
            .unwrap_or_else(|e| error!("animation send_frame error: {e}"));
        receive_animation_packets(
            &mut encoder,
            &mut packet,
            &mut octx,
            time_base,
            out_time_base,
        );
    }
    encoder
        .send_eof()
        .unwrap_or_else(|e| error!("animation send_eof error: {e}"));

    receive_animation_packets(
        &mut encoder,
        &mut packet,
        &mut octx,
        time_base,
        out_time_base,
    );

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write animation trailer: {e}"));
    octx.into_data()
}

fn receive_animation_packets(
    encoder: &mut ffmpeg_next::encoder::Video,
    packet: &mut Packet,
    octx: &mut MemOutput,
    time_base: Rational,
    out_time_base: Rational,
) {
    while encoder.receive_packet(packet).is_ok() {
        packet.set_stream(0);
        packet.rescale_ts(time_base, out_time_base);
        packet.set_position(-1);
        packet
            .write_interleaved(octx)
            .unwrap_or_else(|e| error!("failed to write animation packet: {e}"));
    }
}

fn choose_animation_pixel_format(
    codec: ffmpeg_next::Codec,
    format: &str,
    input_pix_fmt: Pixel,
) -> Pixel {
    let supported = codec
        .video()
        .ok()
        .and_then(|video| video.formats().map(|formats| formats.collect::<Vec<_>>()))
        .unwrap_or_default();

    let preferences: &[Pixel] = match format {
        "gif" => &[Pixel::RGB8, Pixel::RGB24, Pixel::PAL8],
        "apng" => &[Pixel::RGBA, Pixel::RGB24],
        "webp" => &[Pixel::YUV420P, Pixel::RGBA, Pixel::RGB24],
        _ => &[input_pix_fmt],
    };

    if supported.contains(&input_pix_fmt) {
        return input_pix_fmt;
    }
    for pix_fmt in preferences {
        if supported.contains(pix_fmt) {
            return *pix_fmt;
        }
    }
    supported.first().copied().unwrap_or(input_pix_fmt)
}

fn scale_animation_frame(
    frame: ffmpeg_next::frame::Video,
    width: Option<i32>,
    pix_fmt: Pixel,
) -> ffmpeg_next::frame::Video {
    let out_width = width.map(|w| w as u32).unwrap_or_else(|| frame.width());
    let out_height = if out_width == frame.width() {
        frame.height()
    } else {
        ((u64::from(frame.height()) * u64::from(out_width)) / u64::from(frame.width())).max(1)
            as u32
    };

    if frame.format() == pix_fmt && frame.width() == out_width && frame.height() == out_height {
        return frame;
    }

    let mut scaled = ffmpeg_next::frame::Video::empty();
    let mut scaler = ScaleContext::get(
        frame.format(),
        frame.width(),
        frame.height(),
        pix_fmt,
        out_width,
        out_height,
        Flags::BILINEAR,
    )
    .unwrap_or_else(|e| error!("failed to create animation scaler: {e}"));
    scaler
        .run(&frame, &mut scaled)
        .unwrap_or_else(|e| error!("animation scale error: {e}"));
    scaled
}

fn validate_args(start_time: f64, duration: f64, width: Option<i32>, fps: i32, format: &str) {
    if start_time < 0.0 {
        error!("pg_ffmpeg: start_time must be >= 0");
    }
    if duration <= 0.0 {
        error!("pg_ffmpeg: duration must be > 0");
    }
    if let Some(width) = width {
        if width <= 0 {
            error!("pg_ffmpeg: width must be > 0");
        }
    }
    if fps <= 0 {
        error!("pg_ffmpeg: fps must be > 0");
    }
    match format {
        "gif" | "apng" | "webp" => {}
        _ => error!("pg_ffmpeg: format must be gif, apng, or webp"),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::generate_test_video_bytes;

    #[pg_test]
    fn test_generate_gif() {
        let data = generate_test_video_bytes(64, 64, 10, 2);
        let gif = generate_gif(data, 0.0, 1.0, Some(64), 10, "gif".to_string());
        assert!(gif.starts_with(b"GIF89a"));
    }

    #[pg_test]
    fn test_generate_apng() {
        let data = generate_test_video_bytes(64, 64, 10, 2);
        let apng = generate_gif(data, 0.0, 1.0, Some(64), 10, "apng".to_string());
        assert!(apng.starts_with(b"\x89PNG\r\n\x1a\n"));
    }
}
