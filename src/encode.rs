//! Encode a video from an in-memory image sequence.

use ffmpeg_next::codec;
use ffmpeg_next::format;
use ffmpeg_next::format::Pixel;
use ffmpeg_next::software::scaling::{context::Context as ScaleContext, flag::Flags};
use ffmpeg_next::{Dictionary, Packet, Rational};
use pgrx::error;
use pgrx::prelude::*;

use crate::codec_lookup::{self, CodecKind};
use crate::hwaccel;
use crate::limits;
use crate::mem_io::{MemInput, MemOutput};

#[pg_extern]
fn encode(
    frames: Array<'_, &[u8]>,
    fps: default!(i32, 24),
    codec: default!(String, "'libx264'"),
    format: default!(String, "'mp4'"),
    crf: default!(i32, 23),
    hwaccel: default!(bool, false),
) -> Vec<u8> {
    limits::check_array_size(frames.len()).unwrap_or_else(|e| error!("{e}"));
    if frames.is_empty() {
        error!("pg_ffmpeg: encode requires at least one frame");
    }
    if fps <= 0 {
        error!("pg_ffmpeg: encode fps must be positive");
    }

    let mut borrowed = Vec::with_capacity(frames.len());
    for (frame_index, maybe_data) in frames.iter().enumerate() {
        let data = maybe_data
            .unwrap_or_else(|| error!("pg_ffmpeg: encode frame {} is NULL", frame_index + 1));
        limits::check_input_size(data.len()).unwrap_or_else(|e| error!("{e}"));
        borrowed.push(data);
    }

    encode_slices(borrowed, fps, &codec, &format, crf, hwaccel)
}

fn encode_slices<'a, I>(
    frames: I,
    fps: i32,
    codec: &str,
    format: &str,
    crf: i32,
    hwaccel: bool,
) -> Vec<u8>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    if fps <= 0 {
        error!("pg_ffmpeg: encode fps must be positive");
    }

    let mut iter = frames.into_iter().enumerate();
    let (first_index, first_data) = iter
        .next()
        .unwrap_or_else(|| error!("pg_ffmpeg: encode requires at least one frame"));

    limits::check_input_size(first_data.len()).unwrap_or_else(|e| error!("{e}"));
    let first_frame = decode_image_frame(first_data, first_index);
    let width = first_frame.width();
    let height = first_frame.height();

    let software_codec =
        codec_lookup::find_encoder(codec, CodecKind::Video).unwrap_or_else(|e| error!("{e}"));
    let mut octx = MemOutput::open(format);
    let encoder_time_base = Rational::new(1, fps);
    let (selected_codec, mut encoder) = open_video_encoder_with_fallback(
        &octx,
        codec,
        software_codec,
        width,
        height,
        first_frame.format(),
        encoder_time_base,
        fps,
        crf,
        hwaccel,
    );
    let encoder_pix_fmt = encoder.format();

    let out_time_base = {
        let mut stream = octx
            .add_stream(selected_codec)
            .unwrap_or_else(|e| error!("failed to add encode output stream: {e}"));
        stream.set_time_base(encoder_time_base);
        stream.set_parameters(&encoder);
        unsafe {
            (*stream.parameters().as_mut_ptr()).codec_tag = 0;
        }
        stream.time_base()
    };

    write_encode_header(&mut octx, format);

    let mut next_pts = 0i64;
    send_frame(
        first_frame,
        next_pts,
        encoder_pix_fmt,
        &mut encoder,
        &mut octx,
        encoder_time_base,
        out_time_base,
    );
    next_pts += 1;

    for (frame_index, data) in iter {
        limits::check_input_size(data.len()).unwrap_or_else(|e| error!("{e}"));
        let frame = decode_image_frame(data, frame_index);
        if frame.width() != width || frame.height() != height {
            error!(
                "pg_ffmpeg: encode frame {} dimensions {}x{} do not match first frame dimensions {}x{}",
                frame_index + 1,
                frame.width(),
                frame.height(),
                width,
                height
            );
        }
        send_frame(
            frame,
            next_pts,
            encoder_pix_fmt,
            &mut encoder,
            &mut octx,
            encoder_time_base,
            out_time_base,
        );
        next_pts += 1;
    }

    encoder
        .send_eof()
        .unwrap_or_else(|e| error!("encode send_eof error: {e}"));
    receive_packets(&mut encoder, &mut octx, encoder_time_base, out_time_base);

    if let Err(e) = octx.write_trailer() {
        if !matches!(format, "mp4" | "mov") {
            error!("failed to write encode trailer: {e}");
        }
    }
    octx.into_data()
}

fn decode_image_frame(data: &[u8], frame_index: usize) -> ffmpeg_next::frame::Video {
    let mut ictx = MemInput::open(data);
    let stream = ictx
        .streams()
        .best(ffmpeg_next::media::Type::Video)
        .unwrap_or_else(|| {
            error!(
                "pg_ffmpeg: encode frame {} does not contain a decodable image stream",
                frame_index + 1
            )
        });
    let stream_index = stream.index();
    let decoder_ctx = codec::context::Context::from_parameters(stream.parameters())
        .unwrap_or_else(|e| error!("failed to create image decoder context: {e}"));
    let mut decoder = decoder_ctx
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open image decoder: {e}"));

    let mut decoded = ffmpeg_next::frame::Video::empty();
    for (packet_stream, packet) in ictx.packets() {
        if packet_stream.index() != stream_index {
            continue;
        }
        decoder
            .send_packet(&packet)
            .unwrap_or_else(|e| error!("image decode error: {e}"));
        if decoder.receive_frame(&mut decoded).is_ok() {
            decoded.set_pts(None);
            return decoded;
        }
    }

    let _ = decoder.send_eof();
    if decoder.receive_frame(&mut decoded).is_ok() {
        decoded.set_pts(None);
        return decoded;
    }

    error!(
        "pg_ffmpeg: encode frame {} did not decode to a video frame",
        frame_index + 1
    );
}

#[allow(clippy::too_many_arguments)]
fn open_video_encoder_with_fallback(
    octx: &ffmpeg_next::format::context::Output,
    codec_name: &str,
    software_codec: ffmpeg_next::Codec,
    width: u32,
    height: u32,
    input_pix_fmt: Pixel,
    time_base: Rational,
    fps: i32,
    crf: i32,
    use_hwaccel: bool,
) -> (ffmpeg_next::Codec, ffmpeg_next::encoder::Video) {
    if use_hwaccel {
        if let Some(hw_codec) = hwaccel::hw_encoder(codec_name) {
            if let Some(device) = hwaccel::hw_device_for(&hw_codec) {
                let hw_pix_fmt = choose_encoder_pixel_format(hw_codec, input_pix_fmt);
                if let Ok(encoder) = open_video_encoder(
                    octx,
                    hw_codec,
                    width,
                    height,
                    hw_pix_fmt,
                    time_base,
                    fps,
                    crf,
                    Some(&device),
                ) {
                    return (hw_codec, encoder);
                }
            }
            hwaccel::warn_hw_fallback(codec_name);
        } else {
            hwaccel::warn_hw_fallback(codec_name);
        }
    }

    let software_pix_fmt = choose_encoder_pixel_format(software_codec, input_pix_fmt);
    let encoder = open_video_encoder(
        octx,
        software_codec,
        width,
        height,
        software_pix_fmt,
        time_base,
        fps,
        crf,
        None,
    )
    .unwrap_or_else(|e| {
        error!(
            "{}",
            codec_lookup::open_failed(codec_name, CodecKind::Video, e)
        )
    });
    (software_codec, encoder)
}

#[allow(clippy::too_many_arguments)]
fn open_video_encoder(
    octx: &ffmpeg_next::format::context::Output,
    selected_codec: ffmpeg_next::Codec,
    width: u32,
    height: u32,
    pix_fmt: Pixel,
    time_base: Rational,
    fps: i32,
    crf: i32,
    hw_device: Option<&hwaccel::HwDeviceRef>,
) -> Result<ffmpeg_next::encoder::Video, ffmpeg_next::Error> {
    let ctx = codec::context::Context::new_with_codec(selected_codec);
    let mut encoder = ctx.encoder().video()?;
    encoder.set_width(width);
    encoder.set_height(height);
    encoder.set_format(pix_fmt);
    encoder.set_time_base(time_base);
    encoder.set_frame_rate(Some(Rational::new(fps, 1)));
    encoder.set_gop(fps.max(1) as u32);

    unsafe {
        if let Some(device) = hw_device {
            (*encoder.as_mut_ptr()).hw_device_ctx =
                ffmpeg_next::sys::av_buffer_ref(device.as_ptr());
        }
    }

    if octx.format().flags().contains(format::Flags::GLOBAL_HEADER) {
        encoder.set_flags(codec::Flags::GLOBAL_HEADER);
    }

    let mut options = Dictionary::new();
    if crf >= 0 && codec_supports_crf(selected_codec.name()) {
        options.set("crf", &crf.to_string());
    }

    encoder.open_with(options)
}

fn choose_encoder_pixel_format(codec: ffmpeg_next::Codec, input_pix_fmt: Pixel) -> Pixel {
    let Some(video_codec) = codec.video().ok() else {
        return input_pix_fmt;
    };
    let Some(mut formats) = video_codec.formats() else {
        return input_pix_fmt;
    };
    let supported = formats.by_ref().collect::<Vec<_>>();
    if supported.contains(&input_pix_fmt) {
        return input_pix_fmt;
    }
    if supported.contains(&Pixel::YUV420P) {
        return Pixel::YUV420P;
    }
    if supported.contains(&Pixel::NV12) {
        return Pixel::NV12;
    }
    supported.first().copied().unwrap_or(input_pix_fmt)
}

fn codec_supports_crf(codec_name: &str) -> bool {
    matches!(
        codec_name,
        "libx264" | "libx265" | "libvpx-vp9" | "libaom-av1" | "libsvtav1"
    )
}

fn write_encode_header(octx: &mut MemOutput, format: &str) {
    if matches!(format, "mp4" | "mov") {
        let mut muxer_options = Dictionary::new();
        muxer_options.set("movflags", "frag_keyframe+empty_moov+default_base_moof");
        octx.write_header_with(muxer_options)
            .unwrap_or_else(|e| error!("failed to write encode header: {e}"));
    } else {
        octx.write_header()
            .unwrap_or_else(|e| error!("failed to write encode header: {e}"));
    }
}

#[allow(clippy::too_many_arguments)]
fn send_frame(
    frame: ffmpeg_next::frame::Video,
    pts: i64,
    encoder_pix_fmt: Pixel,
    encoder: &mut ffmpeg_next::encoder::Video,
    octx: &mut MemOutput,
    encoder_time_base: Rational,
    out_time_base: Rational,
) {
    let mut frame = convert_frame_if_needed(frame, encoder_pix_fmt);
    frame.set_pts(Some(pts));
    encoder
        .send_frame(&frame)
        .unwrap_or_else(|e| error!("encode send_frame error: {e}"));
    receive_packets(encoder, octx, encoder_time_base, out_time_base);
}

fn convert_frame_if_needed(
    frame: ffmpeg_next::frame::Video,
    encoder_pix_fmt: Pixel,
) -> ffmpeg_next::frame::Video {
    if frame.format() == encoder_pix_fmt {
        return frame;
    }

    let mut converted = ffmpeg_next::frame::Video::empty();
    let mut scaler = ScaleContext::get(
        frame.format(),
        frame.width(),
        frame.height(),
        encoder_pix_fmt,
        frame.width(),
        frame.height(),
        Flags::BILINEAR,
    )
    .unwrap_or_else(|e| error!("failed to create image scaler: {e}"));
    scaler
        .run(&frame, &mut converted)
        .unwrap_or_else(|e| error!("image scale error: {e}"));
    converted
}

fn receive_packets(
    encoder: &mut ffmpeg_next::encoder::Video,
    octx: &mut MemOutput,
    encoder_time_base: Rational,
    out_time_base: Rational,
) {
    let mut packet = Packet::empty();
    while encoder.receive_packet(&mut packet).is_ok() {
        packet.set_stream(0);
        packet.rescale_ts(encoder_time_base, out_time_base);
        packet.set_position(-1);
        packet
            .write_interleaved(octx)
            .unwrap_or_else(|e| error!("failed to write encoded packet: {e}"));
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::generate_test_image_bytes;

    #[pg_test]
    fn test_encode_from_frames() {
        let first = generate_test_image_bytes("png", 64, 64);
        let second = generate_test_image_bytes("png", 64, 64);
        let result = encode_slices(
            [first.as_slice(), second.as_slice()],
            10,
            "libx264",
            "mp4",
            23,
            false,
        );
        assert!(!result.is_empty());
        assert!(decoded_video_frame_count(&result) >= 2);
    }

    #[pg_test]
    fn test_encode_hwaccel_fallback() {
        let frame = generate_test_image_bytes("png", 64, 64);
        let result = encode_slices([frame.as_slice()], 10, "libx264", "mp4", 23, true);
        assert!(!result.is_empty());
    }

    #[pg_test]
    #[should_panic(expected = "dimensions")]
    fn test_encode_mismatched_dimensions_errors() {
        let first = generate_test_image_bytes("png", 64, 64);
        let second = generate_test_image_bytes("png", 96, 64);
        let _ = encode_slices(
            [first.as_slice(), second.as_slice()],
            10,
            "libx264",
            "mp4",
            23,
            false,
        );
    }

    fn decoded_video_frame_count(data: &[u8]) -> usize {
        let mut input = MemInput::open(data);
        let stream = input
            .streams()
            .best(ffmpeg_next::media::Type::Video)
            .expect("output has no video stream");
        let stream_index = stream.index();
        let decoder_ctx =
            ffmpeg_next::codec::context::Context::from_parameters(stream.parameters())
                .expect("failed to create decoder context");
        let mut decoder = decoder_ctx
            .decoder()
            .video()
            .expect("failed to open decoder");
        let mut frame = ffmpeg_next::frame::Video::empty();
        let mut count = 0;

        for (packet_stream, packet) in input.packets() {
            if packet_stream.index() != stream_index {
                continue;
            }
            decoder.send_packet(&packet).expect("failed to send packet");
            while decoder.receive_frame(&mut frame).is_ok() {
                count += 1;
            }
        }
        decoder.send_eof().expect("failed to send eof");
        while decoder.receive_frame(&mut frame).is_ok() {
            count += 1;
        }
        count
    }
}
