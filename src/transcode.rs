use pgrx::prelude::*;

use crate::codec_lookup::{self, CodecKind};
use crate::filter_safety;
use crate::hwaccel;
use crate::mem_io::{MemInput, MemOutput};
use crate::pipeline;

use ffmpeg_next::codec;
use ffmpeg_next::filter;
use ffmpeg_next::format::{self, Pixel, Sample};
use ffmpeg_next::media::Type;
use ffmpeg_next::{frame, ChannelLayout, Dictionary, Packet, Rational};

use std::collections::HashMap;

struct VideoTranscodeConfig<'a> {
    filter_spec: &'a str,
    codec_name: Option<&'a str>,
    preset: Option<&'a str>,
    crf: Option<i32>,
    bitrate: Option<i32>,
    hwaccel: bool,
}

struct AudioTranscodeConfig<'a> {
    filter_spec: &'a str,
    codec_name: Option<&'a str>,
    bitrate: Option<i32>,
}

struct VideoTranscodePipeline {
    ost_index: usize,
    decoder: ffmpeg_next::decoder::Video,
    encoder: ffmpeg_next::encoder::Video,
    encoder_time_base: Rational,
    graph: filter::Graph,
}

struct AudioTranscodePipeline {
    ost_index: usize,
    decoder: ffmpeg_next::decoder::Audio,
    encoder: ffmpeg_next::encoder::Audio,
    encoder_time_base: Rational,
    graph: filter::Graph,
}

#[allow(clippy::too_many_arguments)]
#[pg_extern]
fn transcode(
    data: Vec<u8>,
    format: default!(Option<&str>, "NULL"),
    filter: default!(Option<&str>, "NULL"),
    codec: default!(Option<&str>, "NULL"),
    preset: default!(Option<&str>, "NULL"),
    crf: default!(Option<i32>, "NULL"),
    bitrate: default!(Option<i32>, "NULL"),
    audio_codec: default!(Option<&str>, "NULL"),
    audio_filter: default!(Option<&str>, "NULL"),
    audio_bitrate: default!(Option<i32>, "NULL"),
    hwaccel: default!(bool, false),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    if let Some(value) = bitrate {
        if value <= 0 {
            error!("pg_ffmpeg: bitrate must be > 0");
        }
    }
    if let Some(value) = audio_bitrate {
        if value <= 0 {
            error!("pg_ffmpeg: audio_bitrate must be > 0");
        }
    }

    if let Some(spec) = filter {
        filter_safety::validate_filter_spec(spec).unwrap_or_else(|e| error!("{e}"));
    }
    if let Some(spec) = audio_filter {
        filter_safety::validate_filter_spec(spec).unwrap_or_else(|e| error!("{e}"));
    }

    let mut ictx = MemInput::open(&data);
    let input_format = ictx.format().name().to_owned();
    let default_format = default_output_format(&input_format);
    let out_format = format.unwrap_or(&default_format);

    let video_passthrough = filter.is_none()
        && codec.is_none()
        && preset.is_none()
        && crf.is_none()
        && bitrate.is_none()
        && !hwaccel;
    let audio_passthrough =
        audio_codec.is_none() && audio_filter.is_none() && audio_bitrate.is_none();

    if video_passthrough && audio_passthrough {
        return remux_all_streams(&mut ictx, out_format);
    }

    let mut octx = MemOutput::open(out_format);
    let mut stream_mapping: Vec<isize> = vec![-1; ictx.streams().count()];
    let mut ist_time_bases = vec![Rational(0, 0); ictx.streams().count()];
    let mut video_pipelines: HashMap<usize, VideoTranscodePipeline> = HashMap::new();
    let mut audio_pipelines: HashMap<usize, AudioTranscodePipeline> = HashMap::new();
    let mut ost_index: usize = 0;
    let mut saw_video = false;
    let mut saw_audio = false;

    let video_config = VideoTranscodeConfig {
        filter_spec: filter.unwrap_or("null"),
        codec_name: codec,
        preset,
        crf,
        bitrate,
        hwaccel,
    };
    let audio_config = AudioTranscodeConfig {
        filter_spec: audio_filter.unwrap_or("anull"),
        codec_name: audio_codec,
        bitrate: audio_bitrate,
    };

    for (ist_index, ist) in ictx.streams().enumerate() {
        let medium = ist.parameters().medium();
        if medium != Type::Audio && medium != Type::Video && medium != Type::Subtitle {
            continue;
        }
        ist_time_bases[ist_index] = ist.time_base();

        match medium {
            Type::Video => {
                saw_video = true;
                if video_passthrough {
                    stream_mapping[ist_index] = pipeline::copy_stream(&ist, &mut octx) as isize;
                } else {
                    let pipe =
                        VideoTranscodePipeline::new(&ist, &mut octx, ost_index, &video_config);
                    stream_mapping[ist_index] = ost_index as isize;
                    video_pipelines.insert(ist_index, pipe);
                    ost_index += 1;
                    continue;
                }
            }
            Type::Audio => {
                saw_audio = true;
                if audio_passthrough {
                    stream_mapping[ist_index] = pipeline::copy_stream(&ist, &mut octx) as isize;
                } else {
                    let pipe =
                        AudioTranscodePipeline::new(&ist, &mut octx, ost_index, &audio_config);
                    stream_mapping[ist_index] = ost_index as isize;
                    audio_pipelines.insert(ist_index, pipe);
                    ost_index += 1;
                    continue;
                }
            }
            Type::Subtitle => {
                stream_mapping[ist_index] = pipeline::copy_stream(&ist, &mut octx) as isize;
            }
            _ => {}
        }

        ost_index += 1;
    }

    if !video_passthrough && !saw_video {
        error!("pg_ffmpeg: no video stream found for video transcoding");
    }
    if !audio_passthrough && !saw_audio {
        error!("pg_ffmpeg: no audio stream found for audio transcoding");
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write header: {e}"));

    let mut ost_time_bases = vec![Rational(0, 0); octx.streams().count()];
    for (i, _) in octx.streams().enumerate() {
        ost_time_bases[i] = octx.stream(i).unwrap().time_base();
    }

    for (stream, mut packet) in ictx.packets() {
        let ist_index = stream.index();
        let ost_index = stream_mapping[ist_index];
        if ost_index < 0 {
            continue;
        }
        let ost_time_base = ost_time_bases[ost_index as usize];

        if let Some(pipe) = video_pipelines.get_mut(&ist_index) {
            packet.rescale_ts(stream.time_base(), pipe.decoder_time_base());
            pipe.send_packet_to_decoder(&packet);
            pipe.receive_and_process_decoded_frames(&mut octx, ost_time_base);
            continue;
        }

        if let Some(pipe) = audio_pipelines.get_mut(&ist_index) {
            packet.rescale_ts(stream.time_base(), pipe.decoder_time_base());
            pipe.send_packet_to_decoder(&packet);
            pipe.receive_and_process_decoded_frames(&mut octx, ost_time_base);
            continue;
        }

        packet.rescale_ts(ist_time_bases[ist_index], ost_time_base);
        packet.set_position(-1);
        packet.set_stream(ost_index as usize);
        packet
            .write_interleaved(&mut octx)
            .unwrap_or_else(|e| error!("failed to write packet: {e}"));
    }

    for (ist_index, pipe) in video_pipelines.iter_mut() {
        let ost_time_base = ost_time_bases[stream_mapping[*ist_index] as usize];
        pipe.send_eof_to_decoder();
        pipe.receive_and_process_decoded_frames(&mut octx, ost_time_base);
        pipe.flush_filter();
        pipe.receive_and_process_filtered_frames(&mut octx, ost_time_base);
        pipe.send_eof_to_encoder();
        pipe.receive_and_process_encoded_packets(&mut octx, ost_time_base);
    }

    for (ist_index, pipe) in audio_pipelines.iter_mut() {
        let ost_time_base = ost_time_bases[stream_mapping[*ist_index] as usize];
        pipe.send_eof_to_decoder();
        pipe.receive_and_process_decoded_frames(&mut octx, ost_time_base);
        pipe.flush_filter();
        pipe.receive_and_process_filtered_frames(&mut octx, ost_time_base);
        pipe.send_eof_to_encoder();
        pipe.receive_and_process_encoded_packets(&mut octx, ost_time_base);
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));

    octx.into_data()
}

fn default_output_format(input_format: &str) -> String {
    match input_format {
        "png_pipe" | "ppm_pipe" => "image2pipe".to_owned(),
        _ => input_format.to_owned(),
    }
}

fn remux_all_streams(ictx: &mut MemInput<'_>, out_format: &str) -> Vec<u8> {
    let mut octx = MemOutput::open(out_format);

    let mut stream_map = vec![None::<usize>; ictx.streams().count()];
    for input_stream in ictx.streams() {
        let out_idx = pipeline::copy_stream(&input_stream, &mut octx);
        stream_map[input_stream.index()] = Some(out_idx);
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write header: {e}"));

    for (stream, mut packet) in ictx.packets() {
        if let Some(Some(oi)) = stream_map.get(stream.index()) {
            let in_tb = stream.time_base();
            let out_tb = octx.stream(*oi).unwrap().time_base();
            packet.set_stream(*oi);
            packet.rescale_ts(in_tb, out_tb);
            packet.set_position(-1);
            packet
                .write_interleaved(&mut octx)
                .unwrap_or_else(|e| error!("failed to write packet: {e}"));
        }
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));

    octx.into_data()
}

impl VideoTranscodePipeline {
    fn new(
        ist: &ffmpeg_next::format::stream::Stream,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_index: usize,
        config: &VideoTranscodeConfig<'_>,
    ) -> Self {
        let decoder_ctx = codec::context::Context::from_parameters(ist.parameters())
            .unwrap_or_else(|e| error!("failed to create decoder context: {e}"));
        let mut decoder = decoder_ctx
            .decoder()
            .video()
            .unwrap_or_else(|e| error!("failed to open decoder: {e}"));
        decoder.set_time_base(ist.time_base());

        let mut graph = pipeline::build_video_filter_graph(&decoder, config.filter_spec);
        let (out_width, out_height, out_pix_fmt, filter_tb) = resolved_video_output(&mut graph);

        let (software_codec, codec_label) = resolve_video_encoder(config.codec_name, decoder.id());
        let encoder_time_base = resolved_video_time_base(&decoder, filter_tb);
        let (opened_codec, encoder) = open_video_encoder_with_fallback(
            octx,
            &decoder,
            software_codec,
            &codec_label,
            out_width,
            out_height,
            out_pix_fmt,
            encoder_time_base,
            config,
        );

        let mut ost = octx
            .add_stream(opened_codec)
            .unwrap_or_else(|e| error!("failed to add video output stream: {e}"));
        ost.set_time_base(encoder_time_base);
        ost.set_parameters(&encoder);
        unsafe {
            (*ost.parameters().as_mut_ptr()).codec_tag = 0;
        }

        Self {
            ost_index,
            decoder,
            encoder,
            encoder_time_base,
            graph,
        }
    }

    fn decoder_time_base(&self) -> Rational {
        self.decoder.time_base()
    }

    fn send_packet_to_decoder(&mut self, packet: &Packet) {
        self.decoder
            .send_packet(packet)
            .unwrap_or_else(|e| error!("decode error: {e}"));
    }

    fn send_eof_to_decoder(&mut self) {
        let _ = self.decoder.send_eof();
    }

    fn receive_and_process_decoded_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut decoded = frame::Video::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            let timestamp = decoded.timestamp();
            decoded.set_pts(timestamp);
            self.graph
                .get("in")
                .unwrap()
                .source()
                .add(&decoded)
                .unwrap_or_else(|e| error!("filter source error: {e}"));
            self.receive_and_process_filtered_frames(octx, ost_time_base);
        }
    }

    fn receive_and_process_filtered_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut filtered = frame::Video::empty();
        while self
            .graph
            .get("out")
            .unwrap()
            .sink()
            .frame(&mut filtered)
            .is_ok()
        {
            self.encoder
                .send_frame(&filtered)
                .unwrap_or_else(|e| error!("encode error: {e}"));
            self.receive_and_process_encoded_packets(octx, ost_time_base);
        }
    }

    fn flush_filter(&mut self) {
        let _ = self.graph.get("in").unwrap().source().flush();
    }

    fn send_eof_to_encoder(&mut self) {
        let _ = self.encoder.send_eof();
    }

    fn receive_and_process_encoded_packets(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(self.ost_index);
            encoded.rescale_ts(self.encoder_time_base, ost_time_base);
            encoded.set_position(-1);
            encoded
                .write_interleaved(octx)
                .unwrap_or_else(|e| error!("failed to write video packet: {e}"));
        }
    }
}

impl AudioTranscodePipeline {
    fn new(
        ist: &ffmpeg_next::format::stream::Stream,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_index: usize,
        config: &AudioTranscodeConfig<'_>,
    ) -> Self {
        let decoder_ctx = codec::context::Context::from_parameters(ist.parameters())
            .unwrap_or_else(|e| error!("failed to create audio decoder context: {e}"));
        let mut decoder = decoder_ctx
            .decoder()
            .audio()
            .unwrap_or_else(|e| error!("failed to open audio decoder: {e}"));
        decoder
            .set_parameters(ist.parameters())
            .unwrap_or_else(|e| error!("failed to set audio decoder parameters: {e}"));

        let (selected_codec, codec_label) = resolve_audio_encoder(config.codec_name, decoder.id());
        let audio_props = selected_codec
            .audio()
            .unwrap_or_else(|_| error!("selected encoder is not audio-capable"));
        let sample_rate = resolve_audio_sample_rate(&decoder, &audio_props);
        let channel_layout = resolve_audio_channel_layout(&decoder, &audio_props);
        let sample_format = resolve_audio_sample_format(&decoder, &audio_props);

        let encoder = open_audio_encoder(
            octx,
            &decoder,
            selected_codec,
            &codec_label,
            sample_rate,
            channel_layout,
            sample_format,
            Rational::new(1, sample_rate as i32),
            config,
        );
        let encoder_time_base = encoder.time_base();
        let graph = build_audio_filter_graph(&decoder, &encoder, config.filter_spec);

        let mut ost = octx
            .add_stream(selected_codec)
            .unwrap_or_else(|e| error!("failed to add audio output stream: {e}"));
        ost.set_time_base(encoder_time_base);
        ost.set_parameters(&encoder);
        unsafe {
            (*ost.parameters().as_mut_ptr()).codec_tag = 0;
        }

        Self {
            ost_index,
            decoder,
            encoder,
            encoder_time_base,
            graph,
        }
    }

    fn decoder_time_base(&self) -> Rational {
        self.decoder.time_base()
    }

    fn send_packet_to_decoder(&mut self, packet: &Packet) {
        self.decoder
            .send_packet(packet)
            .unwrap_or_else(|e| error!("audio decode error: {e}"));
    }

    fn send_eof_to_decoder(&mut self) {
        let _ = self.decoder.send_eof();
    }

    fn receive_and_process_decoded_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut decoded = frame::Audio::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            let timestamp = decoded.timestamp();
            decoded.set_pts(timestamp);
            self.graph
                .get("in")
                .unwrap()
                .source()
                .add(&decoded)
                .unwrap_or_else(|e| error!("audio filter source error: {e}"));
            self.receive_and_process_filtered_frames(octx, ost_time_base);
        }
    }

    fn receive_and_process_filtered_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut filtered = frame::Audio::empty();
        while self
            .graph
            .get("out")
            .unwrap()
            .sink()
            .frame(&mut filtered)
            .is_ok()
        {
            self.encoder
                .send_frame(&filtered)
                .unwrap_or_else(|e| error!("audio encode error: {e}"));
            self.receive_and_process_encoded_packets(octx, ost_time_base);
        }
    }

    fn flush_filter(&mut self) {
        let _ = self.graph.get("in").unwrap().source().flush();
    }

    fn send_eof_to_encoder(&mut self) {
        let _ = self.encoder.send_eof();
    }

    fn receive_and_process_encoded_packets(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(self.ost_index);
            encoded.rescale_ts(self.encoder_time_base, ost_time_base);
            encoded.set_position(-1);
            encoded
                .write_interleaved(octx)
                .unwrap_or_else(|e| error!("failed to write audio packet: {e}"));
        }
    }
}

fn resolved_video_output(graph: &mut filter::Graph) -> (u32, u32, Pixel, Rational) {
    unsafe {
        let sink_ptr = graph.get("out").unwrap().as_ptr();
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

fn resolved_video_time_base(
    decoder: &ffmpeg_next::decoder::Video,
    filter_tb: Rational,
) -> Rational {
    if let Some(frame_rate) = decoder.frame_rate() {
        Rational(frame_rate.denominator(), frame_rate.numerator())
    } else if filter_tb.denominator() != 0 {
        filter_tb
    } else {
        decoder.time_base()
    }
}

fn resolve_video_encoder(
    requested: Option<&str>,
    source_id: codec::Id,
) -> (ffmpeg_next::Codec, String) {
    if let Some(name) = requested {
        let codec =
            codec_lookup::find_encoder(name, CodecKind::Video).unwrap_or_else(|e| error!("{e}"));
        (codec, name.to_owned())
    } else {
        let codec = codec::encoder::find(source_id).unwrap_or_else(|| {
            error!(
                "pg_ffmpeg: no video encoder found for source codec {:?}",
                source_id
            )
        });
        (codec, codec.name().to_owned())
    }
}

fn resolve_audio_encoder(
    requested: Option<&str>,
    source_id: codec::Id,
) -> (ffmpeg_next::Codec, String) {
    if let Some(name) = requested {
        let codec =
            codec_lookup::find_encoder(name, CodecKind::Audio).unwrap_or_else(|e| error!("{e}"));
        (codec, name.to_owned())
    } else {
        let codec = codec::encoder::find(source_id).unwrap_or_else(|| {
            error!(
                "pg_ffmpeg: no audio encoder found for source codec {:?}",
                source_id
            )
        });
        (codec, codec.name().to_owned())
    }
}

#[allow(clippy::too_many_arguments)]
fn open_video_encoder_with_fallback(
    octx: &ffmpeg_next::format::context::Output,
    decoder: &ffmpeg_next::decoder::Video,
    software_codec: ffmpeg_next::Codec,
    codec_label: &str,
    out_width: u32,
    out_height: u32,
    out_pix_fmt: Pixel,
    encoder_time_base: Rational,
    config: &VideoTranscodeConfig<'_>,
) -> (ffmpeg_next::Codec, ffmpeg_next::encoder::Video) {
    if config.hwaccel {
        if let Some(hw_codec) = hwaccel::hw_encoder(codec_label) {
            if let Some(device) = hwaccel::hw_device_for(&hw_codec) {
                if let Ok(encoder) = open_video_encoder(
                    octx,
                    decoder,
                    hw_codec,
                    out_width,
                    out_height,
                    out_pix_fmt,
                    encoder_time_base,
                    config,
                    Some(&device),
                ) {
                    return (hw_codec, encoder);
                }
            }
            hwaccel::warn_hw_fallback(codec_label);
        } else {
            hwaccel::warn_hw_fallback(codec_label);
        }
    }

    let encoder = open_video_encoder(
        octx,
        decoder,
        software_codec,
        out_width,
        out_height,
        out_pix_fmt,
        encoder_time_base,
        config,
        None,
    )
    .unwrap_or_else(|e| {
        error!(
            "{}",
            codec_lookup::open_failed(codec_label, CodecKind::Video, e)
        )
    });
    (software_codec, encoder)
}

#[allow(clippy::too_many_arguments)]
fn open_video_encoder(
    octx: &ffmpeg_next::format::context::Output,
    decoder: &ffmpeg_next::decoder::Video,
    selected_codec: ffmpeg_next::Codec,
    out_width: u32,
    out_height: u32,
    out_pix_fmt: Pixel,
    encoder_time_base: Rational,
    config: &VideoTranscodeConfig<'_>,
    hw_device: Option<&hwaccel::HwDeviceRef>,
) -> Result<ffmpeg_next::encoder::Video, ffmpeg_next::Error> {
    let ctx = codec::context::Context::new_with_codec(selected_codec);
    let mut encoder = ctx.encoder().video()?;
    encoder.set_width(out_width);
    encoder.set_height(out_height);
    encoder.set_format(out_pix_fmt);
    if let Some(frame_rate) = decoder.frame_rate() {
        encoder.set_frame_rate(Some(frame_rate));
    }
    encoder.set_time_base(encoder_time_base);

    let target_bitrate = config
        .bitrate
        .map(|value| value as usize)
        .filter(|value| *value > 0)
        .unwrap_or_else(|| decoder.bit_rate());
    if target_bitrate > 0 {
        encoder.set_bit_rate(target_bitrate);
    }

    unsafe {
        let dec_ptr = decoder.as_ptr();
        let enc_ptr = encoder.as_mut_ptr();
        (*enc_ptr).gop_size = (*dec_ptr).gop_size;
        (*enc_ptr).max_b_frames = (*dec_ptr).max_b_frames;
        if let Some(device) = hw_device {
            (*enc_ptr).hw_device_ctx = ffmpeg_next::sys::av_buffer_ref(device.as_ptr());
        }
    }

    if octx.format().flags().contains(format::Flags::GLOBAL_HEADER) {
        encoder.set_flags(codec::Flags::GLOBAL_HEADER);
    }

    let mut options = Dictionary::new();
    if let Some(preset) = config.preset {
        options.set("preset", preset);
    }
    if let Some(crf) = config.crf {
        options.set("crf", &crf.to_string());
    }

    encoder.open_with(options)
}

#[allow(clippy::too_many_arguments)]
fn open_audio_encoder(
    octx: &ffmpeg_next::format::context::Output,
    decoder: &ffmpeg_next::decoder::Audio,
    selected_codec: ffmpeg_next::Codec,
    codec_label: &str,
    sample_rate: u32,
    channel_layout: ChannelLayout,
    sample_format: Sample,
    encoder_time_base: Rational,
    config: &AudioTranscodeConfig<'_>,
) -> ffmpeg_next::encoder::Audio {
    let ctx = codec::context::Context::new_with_codec(selected_codec);
    let mut encoder = ctx
        .encoder()
        .audio()
        .unwrap_or_else(|e| error!("failed to create audio encoder: {e}"));
    encoder.set_rate(sample_rate as i32);
    encoder.set_channel_layout(channel_layout);
    encoder.set_channels(channel_layout.channels());
    encoder.set_format(sample_format);
    encoder.set_time_base(encoder_time_base);

    let target_bitrate = config
        .bitrate
        .map(|value| value as usize)
        .filter(|value| *value > 0)
        .unwrap_or_else(|| decoder.bit_rate());
    if target_bitrate > 0 {
        encoder.set_bit_rate(target_bitrate);
    }

    if octx.format().flags().contains(format::Flags::GLOBAL_HEADER) {
        encoder.set_flags(codec::Flags::GLOBAL_HEADER);
    }

    encoder.open_as(selected_codec).unwrap_or_else(|e| {
        error!(
            "{}",
            codec_lookup::open_failed(codec_label, CodecKind::Audio, e)
        )
    })
}

fn build_audio_filter_graph(
    decoder: &ffmpeg_next::decoder::Audio,
    encoder: &ffmpeg_next::encoder::Audio,
    spec: &str,
) -> filter::Graph {
    let mut graph = filter::Graph::new();
    let decoder_layout = decoder_channel_layout(decoder);
    let decoder_time_base = audio_frame_time_base(decoder);
    let sample_fmt = Into::<ffmpeg_next::sys::AVSampleFormat>::into(decoder.format()) as i32;
    let args = format!(
        "time_base={}/{}:sample_rate={}:sample_fmt={}:channel_layout=0x{:x}",
        decoder_time_base.numerator(),
        decoder_time_base.denominator(),
        decoder.rate(),
        sample_fmt,
        decoder_layout.bits(),
    );
    graph
        .add(&filter::find("abuffer").unwrap(), "in", &args)
        .unwrap_or_else(|e| error!("failed to add abuffer source: {e}"));
    graph
        .add(&filter::find("abuffersink").unwrap(), "out", "")
        .unwrap_or_else(|e| error!("failed to add abuffer sink: {e}"));
    {
        let mut out = graph.get("out").unwrap();
        out.set_sample_rate(encoder.rate());
        out.set_channel_layout(encoder.channel_layout());
        out.set_sample_format(encoder.format());
    }
    graph
        .output("in", 0)
        .unwrap()
        .input("out", 0)
        .unwrap()
        .parse(spec)
        .unwrap_or_else(|e| error!("failed to parse audio filter '{}': {e}", spec));
    graph
        .validate()
        .unwrap_or_else(|e| error!("failed to validate audio filter graph: {e}"));
    if encoder.frame_size() > 0 {
        graph
            .get("out")
            .unwrap()
            .sink()
            .set_frame_size(encoder.frame_size());
    }
    graph
}

fn resolve_audio_sample_rate(
    decoder: &ffmpeg_next::decoder::Audio,
    codec: &ffmpeg_next::codec::audio::Audio,
) -> u32 {
    if let Some(rates) = codec.rates() {
        let decoder_rate = decoder.rate();
        let mut fallback = None;
        for rate in rates {
            fallback.get_or_insert(rate as u32);
            if rate as u32 == decoder_rate {
                return decoder_rate;
            }
        }
        fallback.unwrap_or(decoder_rate)
    } else {
        decoder.rate()
    }
}

fn resolve_audio_channel_layout(
    decoder: &ffmpeg_next::decoder::Audio,
    codec: &ffmpeg_next::codec::audio::Audio,
) -> ChannelLayout {
    let decoder_layout = decoder_channel_layout(decoder);
    let decoder_channels = decoder.channels() as i32;
    codec
        .channel_layouts()
        .map(|layouts| layouts.best(decoder_channels))
        .unwrap_or(decoder_layout)
}

fn decoder_channel_layout(decoder: &ffmpeg_next::decoder::Audio) -> ChannelLayout {
    if decoder.channel_layout().bits() != 0 {
        decoder.channel_layout()
    } else if decoder.channels() > 0 {
        ChannelLayout::default(decoder.channels() as i32)
    } else {
        ChannelLayout::STEREO
    }
}

fn audio_frame_time_base(decoder: &ffmpeg_next::decoder::Audio) -> Rational {
    if decoder.rate() > 0 {
        Rational::new(1, decoder.rate() as i32)
    } else {
        decoder.time_base()
    }
}

fn resolve_audio_sample_format(
    decoder: &ffmpeg_next::decoder::Audio,
    codec: &ffmpeg_next::codec::audio::Audio,
) -> Sample {
    if let Some(formats) = codec.formats() {
        let decoder_format = decoder.format();
        let mut fallback = None;
        for format in formats {
            fallback.get_or_insert(format);
            if format == decoder_format {
                return decoder_format;
            }
        }
        fallback.unwrap_or(decoder_format)
    } else {
        decoder.format()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::mem_io::MemInput;
    use crate::test_utils::{generate_test_video_bytes, generate_test_video_with_audio_bytes};

    #[pg_test]
    fn test_transcode_default_format() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        let result = transcode(
            data.clone(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let fmt = probe.format().name().to_owned();
        assert!(fmt.contains("mpegts"), "expected mpegts, got {fmt}");
    }

    #[pg_test]
    fn test_transcode_to_different_format() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        let result = transcode(
            data,
            Some("matroska"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let fmt = probe.format().name().to_owned();
        assert!(fmt.contains("matroska"), "expected matroska, got {fmt}");
    }

    #[pg_test]
    fn test_transcode_image() {
        use ffmpeg_next::util::frame::video::Video;

        ffmpeg_next::init().unwrap();

        let enc_codec = ffmpeg_next::encoder::find(codec::Id::PNG).expect("PNG encoder not found");
        let mut octx = MemOutput::open("image2pipe");
        let mut stream = octx.add_stream(enc_codec).expect("failed to add stream");
        stream.set_time_base((1, 1));

        let ctx = codec::context::Context::new_with_codec(enc_codec);
        let mut encoder = ctx.encoder().video().expect("failed to create encoder");
        encoder.set_width(64);
        encoder.set_height(64);
        encoder.set_format(Pixel::RGB24);
        encoder.set_time_base((1, 1));

        let mut encoder = encoder.open().expect("failed to open encoder");
        let out_time_base = {
            stream.set_parameters(&encoder);
            stream.time_base()
        };

        octx.write_header().expect("failed to write header");

        let mut frame = Video::new(Pixel::RGB24, 64, 64);
        for (i, byte) in frame.data_mut(0).iter_mut().enumerate() {
            *byte = (i % 256) as u8;
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
        let png_data = octx.into_data();
        assert!(!png_data.is_empty());

        let result = transcode(
            png_data,
            None,
            Some("crop=32:32:0:0"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let video = probe
            .streams()
            .best(ffmpeg_next::media::Type::Video)
            .expect("no video stream in output");
        let params = video.parameters();
        let ctx = ffmpeg_next::codec::context::Context::from_parameters(params).unwrap();
        let dec = ctx.decoder().video().unwrap();
        assert_eq!(dec.width(), 32);
        assert_eq!(dec.height(), 32);
    }

    #[pg_test]
    fn test_transcode_with_scale_filter() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        let result = transcode(
            data,
            None,
            Some("scale=32:32"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let video = probe
            .streams()
            .best(ffmpeg_next::media::Type::Video)
            .expect("no video stream in output");
        let params = video.parameters();
        let ctx = ffmpeg_next::codec::context::Context::from_parameters(params).unwrap();
        let dec = ctx.decoder().video().unwrap();
        assert_eq!(dec.width(), 32);
        assert_eq!(dec.height(), 32);
    }

    #[pg_test]
    fn test_transcode_with_codec_selection() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        let result = transcode(
            data,
            Some("matroska"),
            None,
            Some("libx264"),
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let video = probe
            .streams()
            .best(ffmpeg_next::media::Type::Video)
            .expect("no video stream in output");
        let params = video.parameters();
        let ctx = ffmpeg_next::codec::context::Context::from_parameters(params).unwrap();
        let dec = ctx.decoder().video().unwrap();
        assert_eq!(dec.id(), codec::Id::H264);
    }

    #[pg_test]
    fn test_transcode_with_crf_and_preset() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        let result = transcode(
            data,
            Some("matroska"),
            None,
            Some("libx264"),
            Some("ultrafast"),
            Some(30),
            None,
            None,
            None,
            None,
            false,
        );
        assert!(!result.is_empty());
    }

    #[pg_test]
    fn test_transcode_audio_filter_volume() {
        let data = generate_test_video_with_audio_bytes(64, 64, 10, 1);
        let result = transcode(
            data,
            Some("matroska"),
            None,
            None,
            None,
            None,
            None,
            None,
            Some("volume=0.5"),
            None,
            false,
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        assert!(probe.streams().best(Type::Audio).is_some());
    }

    #[pg_test]
    fn test_transcode_audio_codec_change() {
        let data = generate_test_video_with_audio_bytes(64, 64, 10, 1);
        let result = transcode(
            data,
            Some("matroska"),
            None,
            None,
            None,
            None,
            None,
            Some("aac"),
            None,
            Some(96_000),
            false,
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let audio = probe
            .streams()
            .best(Type::Audio)
            .expect("no audio stream in output");
        assert_eq!(audio.parameters().id(), codec::Id::AAC);
    }

    #[pg_test]
    fn test_transcode_hwaccel_fallback() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        let result = transcode(
            data,
            Some("mpegts"),
            None,
            Some("mpeg2video"),
            None,
            None,
            None,
            None,
            None,
            None,
            true,
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let video = probe
            .streams()
            .best(Type::Video)
            .expect("no video stream in output");
        assert_eq!(video.parameters().id(), codec::Id::MPEG2VIDEO);
    }

    #[pg_test]
    fn test_transcode_rejects_unsafe_filter() {
        let data = generate_test_video_bytes(64, 64, 10, 1);
        let result = std::panic::catch_unwind(|| {
            transcode(
                data,
                None,
                Some("movie=/etc/passwd"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
            )
        });
        assert!(result.is_err(), "unsafe filter should error");
    }
}

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use crate::bench_common::{generate_sample_video, sample_video_bytes};
    use pgrx::pg_bench;
    use pgrx_bench::{black_box, Bencher};

    #[pg_bench(setup = generate_sample_video)]
    fn bench_transcode_remux(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || {
            black_box(super::transcode(
                data.clone(),
                Some("matroska"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
            ))
        });
    }

    #[pg_bench(setup = generate_sample_video)]
    fn bench_transcode_filter_scale(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || {
            black_box(super::transcode(
                data.clone(),
                None,
                Some("scale=320:240"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
            ))
        });
    }
}
