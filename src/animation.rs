use pgrx::prelude::*;

use ffmpeg_next::codec;
use ffmpeg_next::media::Type;
use ffmpeg_next::{Packet, Rational, Rescale};

use crate::codec_lookup::{self, CodecKind};
use crate::mem_io::{MemInput, MemOutput};
use crate::pipeline;

#[pg_extern]
fn generate_gif(
    data: &[u8],
    start_time: default!(f64, 0.0),
    duration: default!(f64, 5.0),
    width: default!(Option<i32>, "NULL"),
    fps: default!(i32, 10),
    format: default!(String, "'gif'"),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();
    validate_args(start_time, duration, width, fps, &format);

    encode_animation(data, start_time, duration, width, fps, &format)
}

fn encode_animation(
    data: &[u8],
    start_time: f64,
    duration: f64,
    width: Option<i32>,
    fps: i32,
    format: &str,
) -> Vec<u8> {
    let mut ictx = MemInput::open(data);
    let input = ictx
        .streams()
        .best(Type::Video)
        .unwrap_or_else(|| error!("pg_ffmpeg: no video stream found"));
    let stream_index = input.index();
    let decoder_ctx = codec::context::Context::from_parameters(input.parameters())
        .unwrap_or_else(|e| error!("failed to create video decoder context: {e}"));
    let mut decoder = decoder_ctx
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open video decoder: {e}"));
    decoder.set_time_base(input.time_base());

    let filter_spec = animation_filter_spec(start_time, duration, width, fps, format);
    let mut graph = pipeline::build_video_filter_graph(&decoder, &filter_spec);
    let (out_width, out_height, out_pix_fmt, filter_time_base) =
        resolved_animation_output(&mut graph);

    let (codec_name, muxer) = match format {
        "gif" => ("gif", "gif"),
        "apng" => ("apng", "apng"),
        "webp" => ("libwebp_anim", "webp"),
        _ => unreachable!(),
    };

    let selected_codec = codec_lookup::find_encoder(codec_name, CodecKind::Video)
        .or_else(|_| codec_lookup::find_encoder("libwebp", CodecKind::Video))
        .unwrap_or_else(|e| error!("{e}"));

    let mut octx = MemOutput::open(muxer);
    let encoder_time_base = Rational::new(1, fps);
    let ctx = codec::context::Context::new_with_codec(selected_codec);
    let mut encoder = ctx
        .encoder()
        .video()
        .unwrap_or_else(|e| error!("failed to create animation encoder: {e}"));
    encoder.set_width(out_width);
    encoder.set_height(out_height);
    encoder.set_format(out_pix_fmt);
    encoder.set_time_base(encoder_time_base);
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
        stream.set_time_base(encoder_time_base);
        stream.set_parameters(&encoder);
        unsafe {
            (*stream.parameters().as_mut_ptr()).codec_tag = 0;
        }
        stream.time_base()
    };

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write animation header: {e}"));

    let mut packet = Packet::empty();
    let mut decoded = ffmpeg_next::frame::Video::empty();
    for (stream, packet_in) in ictx.packets() {
        if stream.index() != stream_index {
            continue;
        }
        decoder
            .send_packet(&packet_in)
            .unwrap_or_else(|e| error!("video decode error: {e}"));
        while decoder.receive_frame(&mut decoded).is_ok() {
            if !push_animation_frame(&mut graph, &decoded) {
                continue;
            }
            drain_animation_filter(
                &mut graph,
                &mut encoder,
                &mut packet,
                &mut octx,
                filter_time_base,
                encoder_time_base,
                out_time_base,
            );
        }
    }

    let _ = decoder.send_eof();
    while decoder.receive_frame(&mut decoded).is_ok() {
        if !push_animation_frame(&mut graph, &decoded) {
            continue;
        }
        drain_animation_filter(
            &mut graph,
            &mut encoder,
            &mut packet,
            &mut octx,
            filter_time_base,
            encoder_time_base,
            out_time_base,
        );
    }
    let _ = graph.get("in").unwrap().source().flush();
    drain_animation_filter(
        &mut graph,
        &mut encoder,
        &mut packet,
        &mut octx,
        filter_time_base,
        encoder_time_base,
        out_time_base,
    );

    encoder
        .send_eof()
        .unwrap_or_else(|e| error!("animation send_eof error: {e}"));
    receive_animation_packets(
        &mut encoder,
        &mut packet,
        &mut octx,
        encoder_time_base,
        out_time_base,
    );

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write animation trailer: {e}"));
    octx.into_data()
}

fn animation_filter_spec(
    start_time: f64,
    duration: f64,
    width: Option<i32>,
    fps: i32,
    format: &str,
) -> String {
    let end_time = start_time + duration;
    let width_expr = width
        .map(|width| width.to_string())
        .unwrap_or_else(|| "iw".to_owned());
    let prefix = format!(
        "trim=start={start_time:.6}:end={end_time:.6},setpts=PTS-STARTPTS,fps={fps},scale={width_expr}:-1:flags=lanczos"
    );
    match format {
        "gif" => format!("{prefix},split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse"),
        "apng" => format!("{prefix},format=rgba"),
        "webp" => prefix,
        _ => unreachable!(),
    }
}

fn push_animation_frame(
    graph: &mut ffmpeg_next::filter::Graph,
    frame: &ffmpeg_next::frame::Video,
) -> bool {
    match graph.get("in").unwrap().source().add(frame) {
        Ok(()) => true,
        Err(ffmpeg_next::Error::Eof) => false,
        Err(e) => error!("animation filter source error: {e}"),
    }
}

fn resolved_animation_output(
    graph: &mut ffmpeg_next::filter::Graph,
) -> (u32, u32, ffmpeg_next::format::Pixel, Rational) {
    unsafe {
        let sink_ptr = graph.get("out").unwrap().as_ptr();
        let width = ffmpeg_next::sys::av_buffersink_get_w(sink_ptr) as u32;
        let height = ffmpeg_next::sys::av_buffersink_get_h(sink_ptr) as u32;
        let pix_fmt = ffmpeg_next::sys::av_buffersink_get_format(sink_ptr);
        let time_base = ffmpeg_next::sys::av_buffersink_get_time_base(sink_ptr);
        (
            width,
            height,
            ffmpeg_next::format::Pixel::from(std::mem::transmute::<
                i32,
                ffmpeg_next::sys::AVPixelFormat,
            >(pix_fmt)),
            Rational(time_base.num, time_base.den),
        )
    }
}

fn drain_animation_filter(
    graph: &mut ffmpeg_next::filter::Graph,
    encoder: &mut ffmpeg_next::encoder::Video,
    packet: &mut Packet,
    octx: &mut MemOutput,
    filter_time_base: Rational,
    encoder_time_base: Rational,
    out_time_base: Rational,
) {
    let mut filtered = ffmpeg_next::frame::Video::empty();
    while graph
        .get("out")
        .unwrap()
        .sink()
        .frame(&mut filtered)
        .is_ok()
    {
        if let Some(pts) = filtered.timestamp() {
            filtered.set_pts(Some(pts.rescale(filter_time_base, encoder_time_base)));
        }
        encoder
            .send_frame(&filtered)
            .unwrap_or_else(|e| error!("animation send_frame error: {e}"));
        receive_animation_packets(encoder, packet, octx, encoder_time_base, out_time_base);
    }
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
    use crate::mem_io::MemInput;
    use crate::test_utils::generate_test_video_bytes;

    #[pg_test]
    fn test_generate_gif() {
        let data = generate_test_video_bytes(64, 64, 10, 2);
        let gif = generate_gif(&data, 0.0, 1.0, Some(64), 10, "gif".to_string());
        assert!(gif.starts_with(b"GIF89a"));
        assert!(decoded_video_frame_count(&gif) > 1);
    }

    #[pg_test]
    fn test_generate_apng() {
        let data = generate_test_video_bytes(64, 64, 10, 2);
        let apng = generate_gif(&data, 0.0, 1.0, Some(64), 10, "apng".to_string());
        assert!(apng.starts_with(b"\x89PNG\r\n\x1a\n"));
    }

    fn decoded_video_frame_count(data: &[u8]) -> usize {
        let mut input = MemInput::open(data);
        let stream = input
            .streams()
            .best(Type::Video)
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
