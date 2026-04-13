use pgrx::prelude::*;

use crate::codec_lookup::{self, CodecKind};
use crate::filter_safety;
use crate::mem_io::{MemInput, MemOutput};

use ffmpeg_next::codec::{self, Id as CodecId};
use ffmpeg_next::filter;
use ffmpeg_next::format::Sample;
use ffmpeg_next::software;
use ffmpeg_next::{frame, ChannelLayout, Packet, Rational, Rescale};

/// Auto-pick output container format based on the source audio codec.
fn auto_format_for_codec(codec_id: CodecId) -> &'static str {
    match codec_id {
        CodecId::AAC => "adts",
        CodecId::MP3 => "mp3",
        CodecId::OPUS => "ogg",
        CodecId::VORBIS => "ogg",
        CodecId::FLAC => "flac",
        CodecId::PCM_S16LE => "wav",
        _ => error!(
            "pg_ffmpeg: cannot auto-pick container for codec {:?}; supply format and codec to re-encode",
            codec_id
        ),
    }
}

/// Check if a container format can accept the source codec for stream-copy.
fn format_accepts_codec(format: &str, codec_id: CodecId) -> bool {
    matches!(
        (format, codec_id),
        ("adts" | "aac", CodecId::AAC)
            | ("mp3", CodecId::MP3)
            | ("ogg", CodecId::OPUS | CodecId::VORBIS)
            | ("flac", CodecId::FLAC)
            | ("wav", CodecId::PCM_S16LE)
    )
}

/// Default encoder name for a container format in the re-encode path.
fn default_codec_for_format(format: &str) -> &'static str {
    match format {
        "mp3" => "libmp3lame",
        "ogg" => "libopus",
        "adts" | "aac" => "aac",
        "flac" => "flac",
        "wav" => "pcm_s16le",
        _ => error!(
            "pg_ffmpeg: cannot infer default audio codec for format '{}'; supply codec explicitly",
            format
        ),
    }
}

#[allow(clippy::too_many_arguments)]
#[pg_extern]
fn extract_audio(
    data: Vec<u8>,
    format: default!(Option<&str>, "NULL"),
    codec: default!(Option<&str>, "NULL"),
    bitrate: default!(Option<i32>, "NULL"),
    sample_rate: default!(Option<i32>, "NULL"),
    channels: default!(Option<i32>, "NULL"),
    filter: default!(Option<&str>, "NULL"),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    if let Some(value) = bitrate {
        if value <= 0 {
            error!("pg_ffmpeg: bitrate must be > 0");
        }
    }
    if let Some(value) = sample_rate {
        if value <= 0 {
            error!("pg_ffmpeg: sample_rate must be > 0");
        }
    }
    if let Some(value) = channels {
        if value <= 0 {
            error!("pg_ffmpeg: channels must be > 0");
        }
    }

    if let Some(spec) = filter {
        filter_safety::validate_filter_spec(spec).unwrap_or_else(|e| error!("{e}"));
    }

    let mut ictx = MemInput::open(&data);

    let (audio_stream_index, source_codec_id) = {
        let audio_stream = ictx
            .streams()
            .best(ffmpeg_next::media::Type::Audio)
            .unwrap_or_else(|| error!("pg_ffmpeg: no audio stream found"));
        (audio_stream.index(), audio_stream.parameters().id())
    };

    // Mode selection per PLAN.md Task 1B:
    // Stream-copy requires ALL re-encode params NULL AND format compatible.
    let reencode_params = codec.is_some()
        || bitrate.is_some()
        || sample_rate.is_some()
        || channels.is_some()
        || filter.is_some();

    let use_stream_copy = !reencode_params
        && match format {
            None => matches!(
                source_codec_id,
                CodecId::AAC
                    | CodecId::MP3
                    | CodecId::OPUS
                    | CodecId::VORBIS
                    | CodecId::FLAC
                    | CodecId::PCM_S16LE
            ),
            Some(fmt) => format_accepts_codec(fmt, source_codec_id),
        };

    let out_format = match format {
        Some(fmt) => fmt,
        None => auto_format_for_codec(source_codec_id),
    };

    if use_stream_copy {
        stream_copy_audio(&mut ictx, audio_stream_index, out_format)
    } else {
        let codec_name = match codec {
            Some(name) => name,
            None => default_codec_for_format(out_format),
        };
        reencode_audio(
            &mut ictx,
            audio_stream_index,
            out_format,
            codec_name,
            bitrate,
            sample_rate,
            channels,
            filter.unwrap_or("anull"),
        )
    }
}

fn stream_copy_audio(
    ictx: &mut MemInput<'_>,
    audio_stream_index: usize,
    out_format: &str,
) -> Vec<u8> {
    let mut octx = MemOutput::open(out_format);

    {
        let audio_stream = ictx.stream(audio_stream_index).unwrap();
        let mut new_stream = octx
            .add_stream(codec::Id::None)
            .unwrap_or_else(|e| error!("failed to add output stream: {e}"));
        new_stream.set_parameters(audio_stream.parameters());
        unsafe { (*new_stream.parameters().as_mut_ptr()).codec_tag = 0 };
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write output header: {e}"));

    let out_tb = octx.stream(0).unwrap().time_base();
    for (stream, mut packet) in ictx.packets() {
        if stream.index() == audio_stream_index {
            packet.set_stream(0);
            packet.rescale_ts(stream.time_base(), out_tb);
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

#[allow(clippy::too_many_arguments)]
fn reencode_audio(
    ictx: &mut MemInput<'_>,
    audio_stream_index: usize,
    out_format: &str,
    codec_name: &str,
    bitrate: Option<i32>,
    user_sample_rate: Option<i32>,
    user_channels: Option<i32>,
    filter_spec: &str,
) -> Vec<u8> {
    // Scope the stream borrow so ictx can be used for packets later.
    let decoder = {
        let ist = ictx.stream(audio_stream_index).unwrap();
        let decoder_ctx = codec::context::Context::from_parameters(ist.parameters())
            .unwrap_or_else(|e| error!("failed to create audio decoder context: {e}"));
        let mut decoder = decoder_ctx
            .decoder()
            .audio()
            .unwrap_or_else(|e| error!("failed to open audio decoder: {e}"));
        decoder
            .set_parameters(ist.parameters())
            .unwrap_or_else(|e| error!("failed to set audio decoder parameters: {e}"));
        decoder
    };

    let selected_codec =
        codec_lookup::find_encoder(codec_name, CodecKind::Audio).unwrap_or_else(|e| error!("{e}"));
    let codec_label = selected_codec.name().to_owned();
    let audio_props = selected_codec
        .audio()
        .unwrap_or_else(|_| error!("pg_ffmpeg: '{}' is not an audio encoder", codec_label));

    let out_sample_rate = match user_sample_rate {
        Some(rate) => rate as u32,
        None => resolve_sample_rate(&decoder, &audio_props),
    };
    let out_channel_layout = match user_channels {
        Some(ch) => ChannelLayout::default(ch),
        None => resolve_channel_layout(&decoder, &audio_props),
    };
    let out_sample_format = resolve_sample_format(&decoder, &audio_props);

    let mut octx = MemOutput::open(out_format);
    let requested_encoder_time_base = Rational::new(1, out_sample_rate as i32);

    let encoder = open_encoder(
        &octx,
        &decoder,
        selected_codec,
        &codec_label,
        out_sample_rate,
        out_channel_layout,
        out_sample_format,
        requested_encoder_time_base,
        bitrate,
    );
    let encoder_time_base = encoder.time_base();

    {
        let mut ost = octx
            .add_stream(selected_codec)
            .unwrap_or_else(|e| error!("failed to add audio output stream: {e}"));
        ost.set_time_base(encoder_time_base);
        ost.set_parameters(&encoder);
        unsafe { (*ost.parameters().as_mut_ptr()).codec_tag = 0 };
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write header: {e}"));

    let ost_time_base = octx.stream(0).unwrap().time_base();

    if filter_spec == "anull" && selected_codec.id() == CodecId::MP3 {
        let resampler = open_resampler(&decoder, &encoder);
        let mut pipe = ResamplePipeline {
            decoder,
            encoder,
            encoder_time_base,
            resampler,
            next_encoded_pts: 0,
        };

        for (stream, mut packet) in ictx.packets() {
            if stream.index() == audio_stream_index {
                normalize_audio_packet_duration(
                    &mut packet,
                    stream.time_base(),
                    pipe.decoder.time_base(),
                );
                pipe.send_packet(&packet);
                pipe.process_decoded_frames(&mut octx, ost_time_base);
            }
        }

        pipe.flush(&mut octx, ost_time_base);
        drop(pipe);
    } else {
        let graph = build_filter_graph(&decoder, &encoder, filter_spec);
        let mut pipe = ReencodePipeline {
            decoder,
            encoder,
            encoder_time_base,
            graph,
            next_decoded_pts: 0,
            next_encoded_pts: 0,
        };

        for (stream, mut packet) in ictx.packets() {
            if stream.index() == audio_stream_index {
                normalize_audio_packet_duration(
                    &mut packet,
                    stream.time_base(),
                    pipe.decoder.time_base(),
                );
                pipe.send_packet(&packet);
                pipe.process_decoded_frames(&mut octx, ost_time_base);
            }
        }

        pipe.flush(&mut octx, ost_time_base);
        drop(pipe);
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));

    octx.into_data()
}

// ---------------------------------------------------------------------------
// Re-encode pipeline
// ---------------------------------------------------------------------------

struct ReencodePipeline {
    decoder: ffmpeg_next::decoder::Audio,
    encoder: ffmpeg_next::encoder::Audio,
    encoder_time_base: Rational,
    graph: filter::Graph,
    next_decoded_pts: i64,
    next_encoded_pts: i64,
}

struct ResamplePipeline {
    decoder: ffmpeg_next::decoder::Audio,
    encoder: ffmpeg_next::encoder::Audio,
    encoder_time_base: Rational,
    resampler: software::resampling::Context,
    next_encoded_pts: i64,
}

impl ReencodePipeline {
    fn send_packet(&mut self, packet: &Packet) {
        self.decoder
            .send_packet(packet)
            .unwrap_or_else(|e| error!("audio decode error: {e}"));
    }

    fn process_decoded_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut decoded = frame::Audio::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            let frame_time_base = audio_frame_time_base(&self.decoder);
            let timestamp = decoded
                .timestamp()
                .map(|pts| pts.rescale(self.decoder.time_base(), frame_time_base))
                .unwrap_or(self.next_decoded_pts);
            let timestamp = timestamp.max(self.next_decoded_pts);
            self.next_decoded_pts = timestamp.saturating_add(decoded.samples() as i64);
            let mut filtered_input = decoded.clone();
            filtered_input.set_pts(Some(timestamp));
            self.graph
                .get("in")
                .unwrap()
                .source()
                .add(&filtered_input)
                .unwrap_or_else(|e| error!("audio filter source error: {e}"));
            self.process_filtered_frames(octx, ost_time_base);
        }
    }

    fn process_filtered_frames(
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
            let timestamp = filtered.timestamp().unwrap_or(self.next_encoded_pts);
            let timestamp = timestamp.max(self.next_encoded_pts);
            self.next_encoded_pts = timestamp.saturating_add(filtered.samples() as i64);
            let mut encoded_input = filtered.clone();
            encoded_input.set_pts(Some(timestamp));
            self.encoder
                .send_frame(&encoded_input)
                .unwrap_or_else(|e| error!("audio encode error: {e}"));
            self.process_encoded_packets(octx, ost_time_base);
        }
    }

    fn process_encoded_packets(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(0);
            encoded.rescale_ts(self.encoder_time_base, ost_time_base);
            encoded.set_position(-1);
            encoded
                .write_interleaved(octx)
                .unwrap_or_else(|e| error!("failed to write audio packet: {e}"));
        }
    }

    fn flush(&mut self, octx: &mut ffmpeg_next::format::context::Output, ost_time_base: Rational) {
        let _ = self.decoder.send_eof();
        self.process_decoded_frames(octx, ost_time_base);
        let _ = self.graph.get("in").unwrap().source().flush();
        self.process_filtered_frames(octx, ost_time_base);
        let _ = self.encoder.send_eof();
        self.process_encoded_packets(octx, ost_time_base);
    }
}

impl ResamplePipeline {
    fn send_packet(&mut self, packet: &Packet) {
        self.decoder
            .send_packet(packet)
            .unwrap_or_else(|e| error!("audio decode error: {e}"));
    }

    fn process_decoded_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut decoded = frame::Audio::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            let mut converted = alloc_audio_frame(
                self.encoder.format(),
                decoded.samples(),
                self.encoder.channel_layout(),
                self.encoder.rate(),
            );
            self.resampler
                .run(&decoded, &mut converted)
                .unwrap_or_else(|e| error!("audio resample error: {e}"));

            if converted.samples() == 0 {
                continue;
            }

            let mut encoded_input = converted.clone();
            encoded_input.set_pts(Some(self.next_encoded_pts));
            self.next_encoded_pts = self
                .next_encoded_pts
                .saturating_add(encoded_input.samples() as i64);
            self.encoder
                .send_frame(&encoded_input)
                .unwrap_or_else(|e| error!("audio encode error: {e}"));
            self.process_encoded_packets(octx, ost_time_base);
        }
    }

    fn process_encoded_packets(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(0);
            encoded.rescale_ts(self.encoder_time_base, ost_time_base);
            encoded.set_position(-1);
            encoded
                .write_interleaved(octx)
                .unwrap_or_else(|e| error!("failed to write audio packet: {e}"));
        }
    }

    fn flush(&mut self, octx: &mut ffmpeg_next::format::context::Output, ost_time_base: Rational) {
        let _ = self.decoder.send_eof();
        self.process_decoded_frames(octx, ost_time_base);

        loop {
            let frame_samples = self.encoder.frame_size().max(1) as usize;
            let mut converted = alloc_audio_frame(
                self.encoder.format(),
                frame_samples,
                self.encoder.channel_layout(),
                self.encoder.rate(),
            );
            let delay = match self.resampler.flush(&mut converted) {
                Ok(delay) => delay,
                Err(ffmpeg_next::Error::OutputChanged | ffmpeg_next::Error::InputChanged) => None,
                Err(e) => error!("audio resample flush error: {e}"),
            };
            if converted.samples() > 0 {
                let mut encoded_input = converted.clone();
                encoded_input.set_pts(Some(self.next_encoded_pts));
                self.next_encoded_pts = self
                    .next_encoded_pts
                    .saturating_add(encoded_input.samples() as i64);
                self.encoder
                    .send_frame(&encoded_input)
                    .unwrap_or_else(|e| error!("audio encode error: {e}"));
                self.process_encoded_packets(octx, ost_time_base);
            }
            if delay.is_none() {
                break;
            }
        }

        let _ = self.encoder.send_eof();
        self.process_encoded_packets(octx, ost_time_base);
    }
}

// ---------------------------------------------------------------------------
// Encoder / filter-graph / parameter-resolution helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn open_encoder(
    octx: &ffmpeg_next::format::context::Output,
    decoder: &ffmpeg_next::decoder::Audio,
    selected_codec: ffmpeg_next::Codec,
    codec_name: &str,
    sample_rate: u32,
    channel_layout: ChannelLayout,
    sample_format: Sample,
    encoder_time_base: Rational,
    bitrate: Option<i32>,
) -> ffmpeg_next::encoder::Audio {
    let ctx = codec::context::Context::new_with_codec(selected_codec);
    let mut encoder = ctx
        .encoder()
        .audio()
        .unwrap_or_else(|e| error!("failed to create audio encoder: {e}"));
    encoder.set_rate(sample_rate as i32);
    encoder.set_channel_layout(channel_layout);
    encoder.set_format(sample_format);
    encoder.set_time_base(encoder_time_base);

    let target_bitrate = bitrate.map(|v| v as usize).filter(|v| *v > 0);
    let inherited_bitrate = (selected_codec.id() != CodecId::MP3)
        .then(|| decoder.bit_rate())
        .filter(|v| *v > 0);
    if let Some(target_bitrate) = target_bitrate.or(inherited_bitrate) {
        encoder.set_bit_rate(target_bitrate);
    }

    if octx
        .format()
        .flags()
        .contains(ffmpeg_next::format::Flags::GLOBAL_HEADER)
    {
        encoder.set_flags(codec::Flags::GLOBAL_HEADER);
    }

    encoder.open_as(selected_codec).unwrap_or_else(|e| {
        error!(
            "{}",
            codec_lookup::open_failed(codec_name, CodecKind::Audio, e)
        )
    })
}

fn build_filter_graph(
    decoder: &ffmpeg_next::decoder::Audio,
    encoder: &ffmpeg_next::encoder::Audio,
    spec: &str,
) -> filter::Graph {
    let mut graph = filter::Graph::new();
    let decoder_layout = decoder_channel_layout(decoder);
    let decoder_time_base = decoder.time_base();
    let args = format!(
        "time_base={}/{}:sample_rate={}:sample_fmt={}:channel_layout=0x{:x}",
        decoder_time_base.numerator(),
        decoder_time_base.denominator(),
        decoder.rate(),
        decoder.format().name(),
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
    if let Some(codec) = encoder.codec() {
        if encoder.frame_size() > 0
            && !codec
                .capabilities()
                .contains(ffmpeg_next::codec::capabilities::Capabilities::VARIABLE_FRAME_SIZE)
        {
            graph
                .get("out")
                .unwrap()
                .sink()
                .set_frame_size(encoder.frame_size());
        }
    }
    graph
}

fn resolve_sample_rate(
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

fn resolve_channel_layout(
    decoder: &ffmpeg_next::decoder::Audio,
    codec: &ffmpeg_next::codec::audio::Audio,
) -> ChannelLayout {
    let decoder_channels = decoder.channels() as i32;
    codec
        .channel_layouts()
        .map(|layouts| layouts.best(decoder_channels))
        .unwrap_or_else(|| decoder_channel_layout(decoder))
}

fn resolve_sample_format(
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

fn open_resampler(
    decoder: &ffmpeg_next::decoder::Audio,
    encoder: &ffmpeg_next::encoder::Audio,
) -> software::resampling::Context {
    software::resampling::Context::get(
        decoder.format(),
        decoder_channel_layout(decoder),
        decoder.rate(),
        encoder.format(),
        encoder.channel_layout(),
        encoder.rate(),
    )
    .unwrap_or_else(|e| error!("failed to create audio resampler: {e}"))
}

fn normalize_audio_packet_duration(
    packet: &mut Packet,
    src_time_base: Rational,
    dst_time_base: Rational,
) {
    if packet.duration() > 0 {
        packet.set_duration(packet.duration().rescale(src_time_base, dst_time_base));
    }
}

fn alloc_audio_frame(
    format: Sample,
    samples: usize,
    channel_layout: ChannelLayout,
    rate: u32,
) -> frame::Audio {
    let mut frame = frame::Audio::new(format, samples, channel_layout);
    frame.set_rate(rate);
    for plane in 0..frame.planes() {
        frame.data_mut(plane).fill(0);
    }
    frame
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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::{generate_test_aac_adts_bytes, generate_test_video_with_audio_bytes};

    #[pg_test]
    fn test_extract_audio_copy_aac_auto_adts() {
        let data = generate_test_aac_adts_bytes(1);
        let result = extract_audio(data, None, None, None, None, None, None);
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let audio = probe
            .streams()
            .best(ffmpeg_next::media::Type::Audio)
            .expect("no audio stream in output");
        assert_eq!(audio.parameters().id(), CodecId::AAC);
    }

    #[pg_test]
    fn test_extract_audio_copy_aac_rejects_wav_container_stream_copy() {
        // AAC source with an incompatible WAV container rejects stream-copy and
        // falls through to re-encode via the default encoder for that format.
        let data = generate_test_aac_adts_bytes(1);
        let result = extract_audio(data, Some("wav"), None, None, None, None, None);
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let audio = probe
            .streams()
            .best(ffmpeg_next::media::Type::Audio)
            .expect("no audio stream in output");
        assert_eq!(audio.parameters().id(), CodecId::PCM_S16LE);
    }

    #[pg_test]
    fn test_extract_audio_reencode_to_wav_pcm() {
        // MP2 source explicitly re-encoded to PCM audio in a WAV container.
        let data = generate_test_video_with_audio_bytes(64, 64, 10, 1);
        let result = extract_audio(data, Some("wav"), Some("pcm_s16le"), None, None, None, None);
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let audio = probe
            .streams()
            .best(ffmpeg_next::media::Type::Audio)
            .expect("no audio stream in output");
        assert_eq!(audio.parameters().id(), CodecId::PCM_S16LE);
    }

    #[pg_test]
    fn test_extract_audio_with_filter() {
        let data = generate_test_aac_adts_bytes(1);
        let result = extract_audio(
            data,
            Some("adts"),
            Some("aac"),
            None,
            None,
            None,
            Some("volume=0.5"),
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        assert!(probe
            .streams()
            .best(ffmpeg_next::media::Type::Audio)
            .is_some());
    }

    #[pg_test]
    #[should_panic(expected = "always denied")]
    fn test_extract_audio_rejects_unsafe_filter() {
        let data = generate_test_aac_adts_bytes(1);
        extract_audio(data, None, None, None, None, None, Some("movie=foo.mp4"));
    }

    #[pg_test]
    #[should_panic(expected = "cannot infer default audio codec for format")]
    fn test_extract_audio_unknown_format_errors() {
        let data = generate_test_aac_adts_bytes(1);
        extract_audio(data, Some("nonexistent_fmt"), None, None, None, None, None);
    }
}
