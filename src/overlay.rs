#![allow(dead_code)]

use pgrx::prelude::*;

use crate::codec_lookup::{self, CodecKind};
use crate::limits;
use crate::mem_io::{MemInput, MemOutput};
use crate::pipeline;

use ffmpeg_next::codec;
use ffmpeg_next::format::{self, Pixel};
use ffmpeg_next::media::Type;
use ffmpeg_next::{frame, picture, Error as FfmpegError, Packet, Rational, Rescale};

#[pg_extern]
fn overlay(
    background: Vec<u8>,
    foreground: Vec<u8>,
    x: default!(i32, 0),
    y: default!(i32, 0),
    start_time: default!(f64, 0.0),
    end_time: default!(Option<f64>, "NULL"),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();
    limits::check_input_size(background.len()).unwrap_or_else(|e| error!("{e}"));
    limits::check_input_size(foreground.len()).unwrap_or_else(|e| error!("{e}"));

    overlay_slices(&background, &foreground, x, y, start_time, end_time)
}

pub(crate) fn overlay_slices(
    background: &[u8],
    foreground: &[u8],
    x: i32,
    y: i32,
    start_time: f64,
    end_time: Option<f64>,
) -> Vec<u8> {
    if start_time < 0.0 {
        error!("pg_ffmpeg: start_time must be >= 0");
    }
    if let Some(end) = end_time {
        if end <= start_time {
            error!("pg_ffmpeg: end_time must be greater than start_time");
        }
    }

    let mut bg = MemInput::open(background);
    let mut fg = MemInput::open(foreground);
    let output_format = default_output_format(bg.format().name());

    let (bg_video_stream, mut bg_decoder) = open_video_decoder(&bg, "background");
    let (fg_video_stream, mut fg_decoder) = open_video_decoder(&fg, "foreground");
    let bg_audio_stream = bg.streams().best(Type::Audio).map(|stream| stream.index());

    let filter_spec = overlay_filter_spec(x, y, start_time, end_time);
    let mut graph = {
        let mut builder = pipeline::MultiInputGraph::builder();
        builder.add_video_input(
            "bg",
            bg_decoder.width(),
            bg_decoder.height(),
            bg_decoder.format(),
            bg_decoder.time_base(),
            bg_decoder.aspect_ratio(),
        );
        builder.add_video_input(
            "fg",
            fg_decoder.width(),
            fg_decoder.height(),
            fg_decoder.format(),
            fg_decoder.time_base(),
            fg_decoder.aspect_ratio(),
        );
        builder.add_video_output("vout");
        builder.build(&filter_spec)
    };

    let mut octx = MemOutput::open(&output_format);
    let mut video = OverlayVideoOutput::new(&mut graph, &mut octx, &bg_decoder);
    let audio_out_index = if bg_audio_stream.is_some() {
        Some(copy_background_audio_stream(&bg, &mut octx))
    } else {
        None
    };

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write header: {e}"));
    let output_time_bases: Vec<Rational> =
        octx.streams().map(|stream| stream.time_base()).collect();

    let mut background_active = true;
    let mut foreground_active = true;
    while background_active || foreground_active {
        if background_active {
            background_active = process_next_background_packet(
                &mut bg,
                bg_video_stream,
                &mut bg_decoder,
                bg_audio_stream,
                audio_out_index,
                &mut graph,
                &mut video,
                &mut octx,
                &output_time_bases,
            );
        }
        if foreground_active {
            foreground_active = process_next_foreground_packet(
                &mut fg,
                fg_video_stream,
                &mut fg_decoder,
                &mut graph,
                &mut video,
                &mut octx,
                &output_time_bases,
            );
        }
    }

    let video_time_base = output_time_bases[video.ost_index];
    drain_overlay_output(&mut graph, &mut video, &mut octx, video_time_base);
    video.send_eof();
    video.receive_packets(&mut octx, output_time_bases[video.ost_index]);

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));
    octx.into_data()
}

fn overlay_filter_spec(x: i32, y: i32, start_time: f64, end_time: Option<f64>) -> String {
    match end_time {
        Some(end) => format!(
            "[bg][fg]overlay={x}:{y}:enable='between(t,{start_time:.6},{end:.6})':eof_action=pass[vout]"
        ),
        None => {
            format!("[bg][fg]overlay={x}:{y}:enable='gte(t,{start_time:.6})':eof_action=pass[vout]")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn process_next_background_packet(
    bg: &mut MemInput<'_>,
    video_stream: usize,
    decoder: &mut ffmpeg_next::decoder::Video,
    audio_stream: Option<usize>,
    audio_out_index: Option<usize>,
    graph: &mut pipeline::MultiInputGraph,
    video: &mut OverlayVideoOutput,
    octx: &mut ffmpeg_next::format::context::Output,
    output_time_bases: &[Rational],
) -> bool {
    loop {
        let mut packet = Packet::empty();
        match packet.read(bg) {
            Ok(()) => {
                let stream_index = packet.stream();
                let stream_time_base = bg
                    .stream(stream_index)
                    .unwrap_or_else(|| error!("pg_ffmpeg: packet references missing stream"))
                    .time_base();
                if stream_index == video_stream {
                    packet.rescale_ts(stream_time_base, decoder.time_base());
                    decoder
                        .send_packet(&packet)
                        .unwrap_or_else(|e| error!("background decode error: {e}"));
                    receive_and_push_video(decoder, graph, 0);
                    let video_time_base = output_time_bases[video.ost_index];
                    drain_overlay_output(graph, video, octx, video_time_base);
                    return true;
                }
                if Some(stream_index) == audio_stream {
                    if let Some(ost) = audio_out_index {
                        packet.set_stream(ost);
                        packet.rescale_ts(stream_time_base, output_time_bases[ost]);
                        packet.set_position(-1);
                        packet.write_interleaved(octx).unwrap_or_else(|e| {
                            error!("failed to write background audio packet: {e}")
                        });
                    }
                    return true;
                }
            }
            Err(FfmpegError::Eof) => {
                let _ = decoder.send_eof();
                receive_and_push_video(decoder, graph, 0);
                graph.flush_input(0);
                let video_time_base = output_time_bases[video.ost_index];
                drain_overlay_output(graph, video, octx, video_time_base);
                return false;
            }
            Err(_) => {}
        }
    }
}

fn process_next_foreground_packet(
    fg: &mut MemInput<'_>,
    video_stream: usize,
    decoder: &mut ffmpeg_next::decoder::Video,
    graph: &mut pipeline::MultiInputGraph,
    video: &mut OverlayVideoOutput,
    octx: &mut ffmpeg_next::format::context::Output,
    output_time_bases: &[Rational],
) -> bool {
    loop {
        let mut packet = Packet::empty();
        match packet.read(fg) {
            Ok(()) => {
                let stream_index = packet.stream();
                if stream_index != video_stream {
                    continue;
                }
                let stream_time_base = fg
                    .stream(stream_index)
                    .unwrap_or_else(|| error!("pg_ffmpeg: packet references missing stream"))
                    .time_base();
                packet.rescale_ts(stream_time_base, decoder.time_base());
                decoder
                    .send_packet(&packet)
                    .unwrap_or_else(|e| error!("foreground decode error: {e}"));
                receive_and_push_video(decoder, graph, 1);
                let video_time_base = output_time_bases[video.ost_index];
                drain_overlay_output(graph, video, octx, video_time_base);
                return true;
            }
            Err(FfmpegError::Eof) => {
                let _ = decoder.send_eof();
                receive_and_push_video(decoder, graph, 1);
                graph.flush_input(1);
                let video_time_base = output_time_bases[video.ost_index];
                drain_overlay_output(graph, video, octx, video_time_base);
                return false;
            }
            Err(_) => {}
        }
    }
}

fn open_video_decoder(ictx: &MemInput<'_>, label: &str) -> (usize, ffmpeg_next::decoder::Video) {
    let stream = ictx
        .streams()
        .best(Type::Video)
        .unwrap_or_else(|| error!("pg_ffmpeg: {label} input has no video stream"));
    let stream_index = stream.index();
    let decoder_ctx = codec::context::Context::from_parameters(stream.parameters())
        .unwrap_or_else(|e| error!("failed to create {label} decoder context: {e}"));
    let mut decoder = decoder_ctx
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open {label} decoder: {e}"));
    decoder.set_time_base(stream.time_base());
    if let Some(rate) = preferred_video_frame_rate(&stream, &decoder) {
        decoder.set_frame_rate(Some(rate));
    }
    (stream_index, decoder)
}

fn copy_background_audio_stream(
    bg: &MemInput<'_>,
    octx: &mut ffmpeg_next::format::context::Output,
) -> usize {
    let audio = bg
        .streams()
        .best(Type::Audio)
        .unwrap_or_else(|| error!("pg_ffmpeg: no background audio stream found"));
    let mut out = octx
        .add_stream(codec::Id::None)
        .unwrap_or_else(|e| error!("failed to add audio output stream: {e}"));
    out.set_parameters(audio.parameters());
    unsafe {
        (*out.parameters().as_mut_ptr()).codec_tag = 0;
    }
    out.index()
}

struct OverlayVideoOutput {
    ost_index: usize,
    encoder: ffmpeg_next::encoder::Video,
    encoder_time_base: Rational,
    filter_time_base: Rational,
    next_pts: i64,
}

impl OverlayVideoOutput {
    fn new(
        graph: &mut pipeline::MultiInputGraph,
        octx: &mut ffmpeg_next::format::context::Output,
        decoder: &ffmpeg_next::decoder::Video,
    ) -> Self {
        let (width, height, pix_fmt, filter_time_base) = resolved_video_output(graph);
        let selected_codec = codec_lookup::find_encoder_by_id(decoder.id(), CodecKind::Video)
            .unwrap_or_else(|e| error!("{e}"));
        let codec_label = selected_codec.name().to_owned();
        let encoder_time_base = if let Some(frame_rate) = decoder.frame_rate() {
            Rational(frame_rate.denominator(), frame_rate.numerator())
        } else if filter_time_base.numerator() > 0 && filter_time_base.denominator() > 0 {
            filter_time_base
        } else {
            decoder.time_base()
        };
        let encoder = open_video_encoder(
            octx,
            selected_codec,
            &codec_label,
            decoder,
            width,
            height,
            pix_fmt,
            encoder_time_base,
        );

        let mut stream = octx
            .add_stream(selected_codec)
            .unwrap_or_else(|e| error!("failed to add video output stream: {e}"));
        stream.set_time_base(encoder_time_base);
        stream.set_parameters(&encoder);
        unsafe {
            (*stream.parameters().as_mut_ptr()).codec_tag = 0;
        }

        Self {
            ost_index: stream.index(),
            encoder,
            encoder_time_base,
            filter_time_base,
            next_pts: 0,
        }
    }

    fn send_frame(&mut self, frame: &frame::Video) {
        let mut frame = frame.clone();
        let pts = frame
            .timestamp()
            .map(|pts| pts.rescale(self.filter_time_base, self.encoder_time_base))
            .unwrap_or(self.next_pts)
            .max(self.next_pts);
        frame.set_pts(Some(pts));
        self.next_pts = pts.saturating_add(1);
        self.encoder
            .send_frame(&frame)
            .unwrap_or_else(|e| error!("video encode error: {e}"));
    }

    fn receive_packets(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut packet = Packet::empty();
        while self.encoder.receive_packet(&mut packet).is_ok() {
            packet.set_stream(self.ost_index);
            packet.rescale_ts(self.encoder_time_base, ost_time_base);
            packet.set_position(-1);
            packet
                .write_interleaved(octx)
                .unwrap_or_else(|e| error!("failed to write video packet: {e}"));
        }
    }

    fn send_eof(&mut self) {
        let _ = self.encoder.send_eof();
    }
}

fn receive_and_push_video(
    decoder: &mut ffmpeg_next::decoder::Video,
    graph: &mut pipeline::MultiInputGraph,
    input_index: usize,
) {
    let mut decoded = frame::Video::empty();
    while decoder.receive_frame(&mut decoded).is_ok() {
        let timestamp = decoded.timestamp();
        decoded.set_pts(timestamp);
        decoded.set_kind(picture::Type::None);
        graph.push_video_frame(input_index, &decoded);
    }
}

fn drain_overlay_output(
    graph: &mut pipeline::MultiInputGraph,
    video: &mut OverlayVideoOutput,
    octx: &mut ffmpeg_next::format::context::Output,
    ost_time_base: Rational,
) {
    let mut filtered = frame::Video::empty();
    while graph.try_recv_video(0, &mut filtered) {
        video.send_frame(&filtered);
        video.receive_packets(octx, ost_time_base);
    }
}

fn resolved_video_output(graph: &mut pipeline::MultiInputGraph) -> (u32, u32, Pixel, Rational) {
    unsafe {
        let sink_ptr = graph.graph().get("vout").unwrap().as_ptr();
        let width = ffmpeg_next::sys::av_buffersink_get_w(sink_ptr) as u32;
        let height = ffmpeg_next::sys::av_buffersink_get_h(sink_ptr) as u32;
        let pix_fmt = ffmpeg_next::sys::av_buffersink_get_format(sink_ptr);
        let time_base = ffmpeg_next::sys::av_buffersink_get_time_base(sink_ptr);
        (
            width,
            height,
            Pixel::from(std::mem::transmute::<i32, ffmpeg_next::sys::AVPixelFormat>(
                pix_fmt,
            )),
            Rational(time_base.num, time_base.den),
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn open_video_encoder(
    octx: &ffmpeg_next::format::context::Output,
    selected_codec: ffmpeg_next::Codec,
    codec_label: &str,
    decoder: &ffmpeg_next::decoder::Video,
    width: u32,
    height: u32,
    pix_fmt: Pixel,
    time_base: Rational,
) -> ffmpeg_next::encoder::Video {
    let ctx = codec::context::Context::new_with_codec(selected_codec);
    let mut encoder = ctx
        .encoder()
        .video()
        .unwrap_or_else(|e| error!("failed to create video encoder: {e}"));
    encoder.set_width(width);
    encoder.set_height(height);
    encoder.set_format(pix_fmt);
    encoder.set_time_base(time_base);
    if let Some(rate) = decoder.frame_rate() {
        encoder.set_frame_rate(Some(rate));
    }
    encoder.set_bit_rate(decoder.bit_rate().max(400_000));
    unsafe {
        let dec_ptr = decoder.as_ptr();
        let enc_ptr = encoder.as_mut_ptr();
        (*enc_ptr).gop_size = (*dec_ptr).gop_size;
        (*enc_ptr).max_b_frames = (*dec_ptr).max_b_frames;
    }
    if octx.format().flags().contains(format::Flags::GLOBAL_HEADER) {
        encoder.set_flags(codec::Flags::GLOBAL_HEADER);
    }
    encoder.open_as(selected_codec).unwrap_or_else(|e| {
        error!(
            "{} (width={} height={} pix_fmt={:?} time_base={}/{})",
            codec_lookup::open_failed(codec_label, CodecKind::Video, e),
            width,
            height,
            pix_fmt,
            time_base.numerator(),
            time_base.denominator(),
        )
    })
}

fn preferred_video_frame_rate(
    stream: &ffmpeg_next::format::stream::Stream,
    decoder: &ffmpeg_next::decoder::Video,
) -> Option<Rational> {
    if let Some(rate) = decoder.frame_rate() {
        if rate.numerator() > 0 && rate.denominator() > 0 {
            return Some(rate);
        }
    }
    let rate = stream.avg_frame_rate();
    if rate.numerator() > 0 && rate.denominator() > 0 {
        Some(rate)
    } else {
        None
    }
}

fn default_output_format(input_format: &str) -> String {
    match input_format {
        "matroska,webm" => "matroska".to_owned(),
        "mov,mp4,m4a,3gp,3g2,mj2" => "mp4".to_owned(),
        _ => input_format
            .split(',')
            .next()
            .unwrap_or("matroska")
            .to_owned(),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::mem_io::MemInput;
    use crate::test_utils::{generate_test_video_bytes, generate_test_video_with_audio_bytes};

    #[pg_test]
    fn test_overlay_basic() {
        let bg = generate_test_video_bytes(64, 64, 5, 1);
        let fg = generate_test_video_bytes(32, 32, 5, 1);
        let result = overlay_slices(&bg, &fg, 8, 8, 0.0, None);
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let stream = probe
            .streams()
            .best(Type::Video)
            .expect("expected video output");
        let decoder_ctx = codec::context::Context::from_parameters(stream.parameters()).unwrap();
        let decoder = decoder_ctx.decoder().video().unwrap();
        assert_eq!(decoder.width(), 64);
        assert_eq!(decoder.height(), 64);
    }

    #[pg_test]
    fn test_overlay_preserves_background_audio() {
        let bg = generate_test_video_with_audio_bytes(64, 64, 5, 1);
        let fg = generate_test_video_bytes(32, 32, 5, 1);
        let result = overlay_slices(&bg, &fg, 0, 0, 0.0, Some(0.5));
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        assert!(probe.streams().best(Type::Video).is_some());
        assert!(probe.streams().best(Type::Audio).is_some());
    }
}
