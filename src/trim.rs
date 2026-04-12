use pgrx::prelude::*;

use crate::codec_lookup::{self, CodecKind};
use crate::mem_io::{MemInput, MemOutput};
use crate::pipeline;

use ffmpeg_next::codec;
use ffmpeg_next::filter;
use ffmpeg_next::format::{self, Pixel, Sample};
use ffmpeg_next::media::Type;
use ffmpeg_next::{frame, ChannelLayout, Packet, Rational, Rescale};

#[pg_extern]
fn trim(
    data: Vec<u8>,
    start_time: default!(f64, 0.0),
    end_time: default!(Option<f64>, "NULL"),
    precise: default!(bool, false),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();
    validate_trim_args(start_time, end_time);

    if precise {
        precise_trim(data, start_time, end_time)
    } else {
        fast_trim(data, start_time, end_time)
    }
}

fn validate_trim_args(start_time: f64, end_time: Option<f64>) {
    if start_time < 0.0 {
        error!("pg_ffmpeg: start_time must be >= 0");
    }
    if let Some(end_time) = end_time {
        if end_time <= start_time {
            error!("pg_ffmpeg: end_time must be > start_time");
        }
    }
}

fn fast_trim(data: Vec<u8>, start_time: f64, end_time: Option<f64>) -> Vec<u8> {
    let mut ictx = MemInput::open(&data);
    let out_format = default_output_format(ictx.format().name());

    if start_time > 0.0 {
        let seek_ts = (start_time * f64::from(ffmpeg_next::ffi::AV_TIME_BASE)) as i64;
        let _ = ictx.seek(seek_ts, ..seek_ts + 1);
    }

    let anchor_stream_index = ictx
        .streams()
        .best(Type::Video)
        .or_else(|| ictx.streams().best(Type::Audio))
        .map(|stream| stream.index());

    let mut octx = MemOutput::open(&out_format);
    let mut stream_mapping = vec![None::<usize>; ictx.streams().count()];
    let mut input_time_bases = vec![Rational(0, 0); ictx.streams().count()];
    for stream in ictx.streams() {
        let input_index = stream.index();
        input_time_bases[input_index] = stream.time_base();
        stream_mapping[input_index] = Some(pipeline::copy_stream(&stream, &mut octx));
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write trim header: {e}"));

    let mut output_time_bases = vec![Rational(0, 0); octx.streams().count()];
    for (index, _) in octx.streams().enumerate() {
        output_time_bases[index] = octx.stream(index).unwrap().time_base();
    }

    let mut anchor_start_seconds = None::<f64>;
    let mut fallback_anchor_seconds = None::<f64>;

    for (stream, mut packet) in ictx.packets() {
        let input_index = stream.index();
        let Some(output_index) = stream_mapping[input_index] else {
            continue;
        };

        let packet_seconds = packet_timestamp_seconds(&packet, stream.time_base());
        if fallback_anchor_seconds.is_none() {
            fallback_anchor_seconds = packet_seconds;
        }
        if anchor_start_seconds.is_none() && Some(input_index) == anchor_stream_index {
            anchor_start_seconds = packet_seconds;
        }

        if let Some(end_time) = end_time {
            if let Some(packet_seconds) = packet_seconds {
                if packet_seconds >= end_time {
                    continue;
                }
            }
        }

        let shift_seconds = anchor_start_seconds
            .or(fallback_anchor_seconds)
            .unwrap_or(start_time);
        let input_time_base = input_time_bases[input_index];
        let output_time_base = output_time_bases[output_index];
        let timestamp_offset = seconds_to_timestamp(shift_seconds, input_time_base);

        if let Some(pts) = packet.pts() {
            packet.set_pts(Some(pts.saturating_sub(timestamp_offset).max(0)));
        }
        if let Some(dts) = packet.dts() {
            packet.set_dts(Some(dts.saturating_sub(timestamp_offset).max(0)));
        }

        packet.set_stream(output_index);
        packet.rescale_ts(input_time_base, output_time_base);
        packet.set_position(-1);
        packet
            .write_interleaved(&mut octx)
            .unwrap_or_else(|e| error!("failed to write trimmed packet: {e}"));
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trim trailer: {e}"));
    octx.into_data()
}

fn precise_trim(data: Vec<u8>, start_time: f64, end_time: Option<f64>) -> Vec<u8> {
    let (out_format, dropped_aux_streams) = {
        let ictx = MemInput::open(&data);
        let out_format = default_output_format(ictx.format().name());
        let dropped_aux_streams = ictx
            .streams()
            .filter(|stream| matches!(stream.parameters().medium(), Type::Subtitle | Type::Data))
            .count();
        (out_format, dropped_aux_streams)
    };

    if dropped_aux_streams == 0 {
        return fast_trim(data, start_time, end_time);
    }

    let mut ictx = MemInput::open(&data);
    let mut octx = MemOutput::open(&out_format);

    let mut stream_mapping = vec![-1isize; ictx.streams().count()];
    let mut video_pipelines = std::collections::HashMap::<usize, PreciseVideoTrimPipeline>::new();
    let mut audio_pipelines = std::collections::HashMap::<usize, PreciseAudioTrimPipeline>::new();
    let mut next_output_index = 0usize;

    let video_filter = video_trim_filter(start_time, end_time);
    let audio_filter = audio_trim_filter(start_time, end_time);

    for (input_index, stream) in ictx.streams().enumerate() {
        match stream.parameters().medium() {
            Type::Video => {
                let pipeline = PreciseVideoTrimPipeline::new(
                    &stream,
                    &mut octx,
                    next_output_index,
                    &video_filter,
                );
                stream_mapping[input_index] = next_output_index as isize;
                video_pipelines.insert(input_index, pipeline);
                next_output_index += 1;
            }
            Type::Audio => {
                let pipeline = PreciseAudioTrimPipeline::new(
                    &stream,
                    &mut octx,
                    next_output_index,
                    &audio_filter,
                );
                stream_mapping[input_index] = next_output_index as isize;
                audio_pipelines.insert(input_index, pipeline);
                next_output_index += 1;
            }
            Type::Subtitle | Type::Data => {}
            _ => {}
        }
    }

    if next_output_index == 0 {
        error!("pg_ffmpeg: trim found no audio or video streams to keep");
    }
    if dropped_aux_streams > 0 {
        pgrx::warning!(
            "pg_ffmpeg: trim(precise=true) dropped {} subtitle/data stream(s); use precise=false to preserve them",
            dropped_aux_streams
        );
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write trim header: {e}"));

    let mut output_time_bases = vec![Rational(0, 0); octx.streams().count()];
    for (index, _) in octx.streams().enumerate() {
        output_time_bases[index] = octx.stream(index).unwrap().time_base();
    }

    for (stream, mut packet) in ictx.packets() {
        let input_index = stream.index();
        let output_index = stream_mapping[input_index];
        if output_index < 0 {
            continue;
        }

        if let Some(pipeline) = video_pipelines.get_mut(&input_index) {
            packet.rescale_ts(stream.time_base(), pipeline.decoder_time_base());
            pipeline.send_packet_to_decoder(&packet);
            pipeline.receive_and_process_decoded_frames(
                &mut octx,
                output_time_bases[output_index as usize],
            );
            continue;
        }

        if let Some(pipeline) = audio_pipelines.get_mut(&input_index) {
            packet.rescale_ts(stream.time_base(), pipeline.decoder_time_base());
            pipeline.send_packet_to_decoder(&packet);
            pipeline.receive_and_process_decoded_frames(
                &mut octx,
                output_time_bases[output_index as usize],
            );
        }
    }

    for (input_index, pipeline) in video_pipelines.iter_mut() {
        let output_index = stream_mapping[*input_index] as usize;
        pipeline.send_eof_to_decoder();
        pipeline.receive_and_process_decoded_frames(&mut octx, output_time_bases[output_index]);
        pipeline.flush_filter();
        pipeline.receive_and_process_filtered_frames(&mut octx, output_time_bases[output_index]);
        pipeline.send_eof_to_encoder();
        pipeline.receive_and_process_encoded_packets(&mut octx, output_time_bases[output_index]);
    }

    for (input_index, pipeline) in audio_pipelines.iter_mut() {
        let output_index = stream_mapping[*input_index] as usize;
        pipeline.send_eof_to_decoder();
        pipeline.receive_and_process_decoded_frames(&mut octx, output_time_bases[output_index]);
        pipeline.flush_filter();
        pipeline.receive_and_process_filtered_frames(&mut octx, output_time_bases[output_index]);
        pipeline.send_eof_to_encoder();
        pipeline.receive_and_process_encoded_packets(&mut octx, output_time_bases[output_index]);
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trim trailer: {e}"));
    octx.into_data()
}

struct PreciseVideoTrimPipeline {
    output_index: usize,
    decoder: ffmpeg_next::decoder::Video,
    encoder: ffmpeg_next::encoder::Video,
    encoder_time_base: Rational,
    graph: filter::Graph,
}

impl PreciseVideoTrimPipeline {
    fn new(
        input_stream: &ffmpeg_next::format::stream::Stream,
        octx: &mut ffmpeg_next::format::context::Output,
        output_index: usize,
        filter_spec: &str,
    ) -> Self {
        let decoder_ctx = codec::context::Context::from_parameters(input_stream.parameters())
            .unwrap_or_else(|e| error!("failed to create video decoder context: {e}"));
        let mut decoder = decoder_ctx
            .decoder()
            .video()
            .unwrap_or_else(|e| error!("failed to open video decoder: {e}"));
        decoder.set_time_base(input_stream.time_base());

        let mut graph = pipeline::build_video_filter_graph(&decoder, filter_spec);
        let (width, height, pix_fmt, filter_time_base) = resolved_video_output(&mut graph);
        let (selected_codec, codec_label, warned_fallback) =
            resolve_precise_video_encoder(decoder.id());
        if warned_fallback {
            pgrx::warning!(
                "pg_ffmpeg: trim(precise=true) source {} has no encoder in this FFmpeg build; re-encoding video as libx264",
                codec_name(decoder.id())
            );
        }

        let ctx = codec::context::Context::new_with_codec(selected_codec);
        let mut encoder = ctx
            .encoder()
            .video()
            .unwrap_or_else(|e| error!("failed to create video encoder: {e}"));
        encoder.set_width(width);
        encoder.set_height(height);
        encoder.set_format(pix_fmt);
        if let Some(frame_rate) = decoder.frame_rate() {
            encoder.set_frame_rate(Some(frame_rate));
        }
        let encoder_time_base = resolved_video_time_base(&decoder, filter_time_base);
        encoder.set_time_base(encoder_time_base);
        if decoder.bit_rate() > 0 {
            encoder.set_bit_rate(decoder.bit_rate());
        }
        unsafe {
            let dec_ptr = decoder.as_ptr();
            let enc_ptr = encoder.as_mut_ptr();
            (*enc_ptr).gop_size = (*dec_ptr).gop_size;
            (*enc_ptr).max_b_frames = (*dec_ptr).max_b_frames;
        }
        if octx.format().flags().contains(format::Flags::GLOBAL_HEADER) {
            encoder.set_flags(codec::Flags::GLOBAL_HEADER);
        }
        let encoder = encoder.open_as(selected_codec).unwrap_or_else(|e| {
            error!(
                "{}",
                codec_lookup::open_failed(&codec_label, CodecKind::Video, e)
            )
        });

        let mut output_stream = octx
            .add_stream(selected_codec)
            .unwrap_or_else(|e| error!("failed to add output video stream: {e}"));
        output_stream.set_time_base(encoder_time_base);
        output_stream.set_parameters(&encoder);
        unsafe {
            (*output_stream.parameters().as_mut_ptr()).codec_tag = 0;
        }

        Self {
            output_index,
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
            .unwrap_or_else(|e| error!("video decode error: {e}"));
    }

    fn send_eof_to_decoder(&mut self) {
        let _ = self.decoder.send_eof();
    }

    fn receive_and_process_decoded_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        output_time_base: Rational,
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
                .unwrap_or_else(|e| error!("video trim filter source error: {e}"));
            self.receive_and_process_filtered_frames(octx, output_time_base);
        }
    }

    fn receive_and_process_filtered_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        output_time_base: Rational,
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
                .unwrap_or_else(|e| error!("video trim encode error: {e}"));
            self.receive_and_process_encoded_packets(octx, output_time_base);
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
        output_time_base: Rational,
    ) {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(self.output_index);
            encoded.rescale_ts(self.encoder_time_base, output_time_base);
            encoded.set_position(-1);
            encoded
                .write_interleaved(octx)
                .unwrap_or_else(|e| error!("failed to write trimmed video packet: {e}"));
        }
    }
}

struct PreciseAudioTrimPipeline {
    output_index: usize,
    decoder: ffmpeg_next::decoder::Audio,
    encoder: ffmpeg_next::encoder::Audio,
    encoder_time_base: Rational,
    graph: filter::Graph,
    next_decoded_pts: i64,
    next_encoded_pts: i64,
}

impl PreciseAudioTrimPipeline {
    fn new(
        input_stream: &ffmpeg_next::format::stream::Stream,
        octx: &mut ffmpeg_next::format::context::Output,
        output_index: usize,
        filter_spec: &str,
    ) -> Self {
        let decoder_ctx = codec::context::Context::from_parameters(input_stream.parameters())
            .unwrap_or_else(|e| error!("failed to create audio decoder context: {e}"));
        let mut decoder = decoder_ctx
            .decoder()
            .audio()
            .unwrap_or_else(|e| error!("failed to open audio decoder: {e}"));
        decoder
            .set_parameters(input_stream.parameters())
            .unwrap_or_else(|e| error!("failed to set audio decoder parameters: {e}"));

        let (selected_codec, codec_label, warned_fallback) =
            resolve_precise_audio_encoder(decoder.id());
        if warned_fallback {
            pgrx::warning!(
                "pg_ffmpeg: trim(precise=true) source {} has no encoder in this FFmpeg build; re-encoding audio as aac",
                codec_name(decoder.id())
            );
        }

        let audio_props = selected_codec
            .audio()
            .unwrap_or_else(|_| error!("selected audio encoder is not audio-capable"));
        let sample_rate = resolve_audio_sample_rate(&decoder, &audio_props);
        let channel_layout = resolve_audio_channel_layout(&decoder, &audio_props);
        let sample_format = resolve_audio_sample_format(&decoder, &audio_props);
        let encoder_time_base = Rational::new(1, sample_rate as i32);

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
        if decoder.bit_rate() > 0 {
            encoder.set_bit_rate(decoder.bit_rate());
        }
        if octx.format().flags().contains(format::Flags::GLOBAL_HEADER) {
            encoder.set_flags(codec::Flags::GLOBAL_HEADER);
        }
        let encoder = encoder.open_as(selected_codec).unwrap_or_else(|e| {
            error!(
                "{}",
                codec_lookup::open_failed(&codec_label, CodecKind::Audio, e)
            )
        });
        let encoder_time_base = encoder.time_base();

        let graph = build_audio_trim_filter_graph(&decoder, &encoder, filter_spec);

        let mut output_stream = octx
            .add_stream(selected_codec)
            .unwrap_or_else(|e| error!("failed to add output audio stream: {e}"));
        output_stream.set_time_base(encoder_time_base);
        output_stream.set_parameters(&encoder);
        unsafe {
            (*output_stream.parameters().as_mut_ptr()).codec_tag = 0;
        }

        Self {
            output_index,
            decoder,
            encoder,
            encoder_time_base,
            graph,
            next_decoded_pts: 0,
            next_encoded_pts: 0,
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
        output_time_base: Rational,
    ) {
        let mut decoded = frame::Audio::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            let frame_time_base = audio_frame_time_base(&self.decoder);
            let timestamp = decoded
                .timestamp()
                .map(|pts| pts.rescale(self.decoder.time_base(), frame_time_base))
                .unwrap_or(self.next_decoded_pts);
            decoded.set_pts(Some(timestamp));
            self.next_decoded_pts = timestamp.saturating_add(decoded.samples() as i64);
            self.graph
                .get("in")
                .unwrap()
                .source()
                .add(&decoded)
                .unwrap_or_else(|e| error!("audio trim filter source error: {e}"));
            self.receive_and_process_filtered_frames(octx, output_time_base);
        }
    }

    fn receive_and_process_filtered_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        output_time_base: Rational,
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
            filtered.set_channel_layout(self.encoder.channel_layout());
            filtered.set_channels(
                u16::try_from(self.encoder.channel_layout().channels())
                    .expect("audio channel count should fit into u16"),
            );
            filtered.set_rate(self.encoder.rate());
            let timestamp = filtered.timestamp().unwrap_or(self.next_encoded_pts);
            filtered.set_pts(Some(timestamp));
            self.next_encoded_pts = timestamp.saturating_add(filtered.samples() as i64);
            self.encoder
                .send_frame(&filtered)
                .unwrap_or_else(|e| error!("audio trim encode error: {e}"));
            self.receive_and_process_encoded_packets(octx, output_time_base);
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
        output_time_base: Rational,
    ) {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(self.output_index);
            encoded.rescale_ts(self.encoder_time_base, output_time_base);
            encoded.set_position(-1);
            encoded
                .write_interleaved(octx)
                .unwrap_or_else(|e| error!("failed to write trimmed audio packet: {e}"));
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
    filter_time_base: Rational,
) -> Rational {
    if let Some(frame_rate) = decoder.frame_rate() {
        Rational(frame_rate.denominator(), frame_rate.numerator())
    } else if filter_time_base.denominator() != 0 {
        filter_time_base
    } else {
        decoder.time_base()
    }
}

fn build_audio_trim_filter_graph(
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
        .unwrap_or_else(|e| error!("failed to add trim abuffer source: {e}"));
    graph
        .add(&filter::find("abuffersink").unwrap(), "out", "")
        .unwrap_or_else(|e| error!("failed to add trim abuffersink: {e}"));
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
        .unwrap_or_else(|e| error!("failed to parse trim audio filter '{}': {e}", spec));
    graph
        .validate()
        .unwrap_or_else(|e| error!("failed to validate trim audio filter graph: {e}"));
    if encoder.frame_size() > 0 {
        graph
            .get("out")
            .unwrap()
            .sink()
            .set_frame_size(encoder.frame_size());
    }
    graph
}

fn resolve_precise_video_encoder(source_id: codec::Id) -> (ffmpeg_next::Codec, String, bool) {
    if let Some(codec) = codec::encoder::find(source_id) {
        let codec_name = codec.name().to_owned();
        (codec, codec_name, false)
    } else {
        let codec = codec_lookup::find_encoder("libx264", CodecKind::Video)
            .unwrap_or_else(|e| error!("{e}"));
        (codec, "libx264".to_owned(), true)
    }
}

fn resolve_precise_audio_encoder(source_id: codec::Id) -> (ffmpeg_next::Codec, String, bool) {
    if let Some(codec) = codec::encoder::find(source_id) {
        let codec_name = codec.name().to_owned();
        (codec, codec_name, false)
    } else {
        let codec =
            codec_lookup::find_encoder("aac", CodecKind::Audio).unwrap_or_else(|e| error!("{e}"));
        (codec, "aac".to_owned(), true)
    }
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

fn default_output_format(input_format: &str) -> String {
    match input_format {
        "png_pipe" | "ppm_pipe" => "image2pipe".to_owned(),
        _ => input_format.to_owned(),
    }
}

fn video_trim_filter(start_time: f64, end_time: Option<f64>) -> String {
    match end_time {
        Some(end_time) => format!("trim=start={start_time}:end={end_time},setpts=PTS-STARTPTS"),
        None => format!("trim=start={start_time},setpts=PTS-STARTPTS"),
    }
}

fn audio_trim_filter(start_time: f64, end_time: Option<f64>) -> String {
    match end_time {
        Some(end_time) => format!("atrim=start={start_time}:end={end_time},asetpts=PTS-STARTPTS"),
        None => format!("atrim=start={start_time},asetpts=PTS-STARTPTS"),
    }
}

fn packet_timestamp_seconds(packet: &Packet, time_base: Rational) -> Option<f64> {
    packet
        .pts()
        .or_else(|| packet.dts())
        .map(|timestamp| timestamp_to_seconds(timestamp, time_base))
}

fn timestamp_to_seconds(timestamp: i64, time_base: Rational) -> f64 {
    if time_base.denominator() == 0 {
        0.0
    } else {
        timestamp as f64 * f64::from(time_base.numerator()) / f64::from(time_base.denominator())
    }
}

fn seconds_to_timestamp(seconds: f64, time_base: Rational) -> i64 {
    if time_base.numerator() == 0 {
        0
    } else {
        (seconds * f64::from(time_base.denominator()) / f64::from(time_base.numerator())).round()
            as i64
    }
}

fn codec_name(codec_id: codec::Id) -> String {
    format!("{:?}", codec_id).to_lowercase()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::mem_io::MemInput;
    use crate::test_utils::{generate_test_video_bytes, generate_test_video_with_audio_bytes};

    #[pg_test]
    fn test_trim_fast_keyframe() {
        let data = generate_test_video_bytes(64, 64, 10, 4);
        let result = trim(data, 1.0, Some(2.5), false);
        assert!(!result.is_empty(), "trim output should not be empty");

        assert!(
            packet_count(&result, Type::Video) > 0,
            "fast trim should preserve video packets"
        );
    }

    #[pg_test]
    fn test_trim_precise_reencode() {
        let data = generate_test_video_bytes(64, 64, 10, 4);
        let result = trim(data, 1.0, Some(2.5), true);
        assert!(!result.is_empty(), "trim output should not be empty");

        let probe = MemInput::open(&result);
        assert!(probe.streams().best(Type::Video).is_some());
        assert!(
            packet_count(&result, Type::Video) > 0,
            "precise trim should preserve video packets"
        );
    }

    #[pg_test]
    fn test_trim_to_end() {
        let data = generate_test_video_bytes(64, 64, 10, 4);
        let result = trim(data, 1.5, None, true);
        assert!(!result.is_empty(), "trim output should not be empty");

        assert!(
            packet_count(&result, Type::Video) > 0,
            "trim-to-end should preserve video packets"
        );
    }

    #[pg_test]
    fn test_trim_av_sync_precise() {
        let data = generate_test_video_with_audio_bytes(64, 64, 10, 4);
        let result = trim(data, 1.0, Some(2.8), true);
        assert!(!result.is_empty(), "trim output should not be empty");

        let video_packets = packet_count(&result, Type::Video);
        let audio_packets = packet_count(&result, Type::Audio);
        assert!(
            video_packets > 0 && audio_packets > 0,
            "precise trim should keep both audio and video packets"
        );
    }

    fn packet_count(data: &[u8], medium: Type) -> usize {
        let mut input = MemInput::open(data);
        let Some(stream) = input.streams().best(medium) else {
            return 0;
        };
        let target_index = stream.index();
        let mut count = 0usize;

        for (packet_stream, packet) in input.packets() {
            if packet_stream.index() != target_index {
                continue;
            }
            if packet.pts().is_some() || packet.dts().is_some() {
                count += 1;
            }
        }
        count
    }
}

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use crate::bench_common::{generate_sample_video, sample_video_bytes};
    use pgrx::pg_bench;
    use pgrx_bench::{black_box, Bencher};

    #[pg_bench(setup = generate_sample_video)]
    fn bench_trim_fast(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || black_box(super::trim(data.clone(), 5.0, Some(15.0), false)));
    }

    #[pg_bench(setup = generate_sample_video)]
    fn bench_trim_precise(b: &mut Bencher) {
        let data = sample_video_bytes();
        b.iter(move || black_box(super::trim(data.clone(), 5.0, Some(15.0), true)));
    }
}
