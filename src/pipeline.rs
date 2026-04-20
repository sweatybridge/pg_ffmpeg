//! Shared decode → filter → encode pipeline primitives (Tasks F1 / F1b).
//!
//! Before this module, `transcode.rs` owned its own `Transcoder` struct
//! with a hard-coded single-video-stream decode→filter→encode loop, and
//! every future function that wanted to do the same thing would have
//! had to copy it. The plan's first milestone calls this out as the
//! cross-cutting dependency that every later feature builds on: extract
//! the primitives once, then thread them through.
//!
//! `VideoPipeline` handles the single-stream case. `AudioPipeline` is
//! its audio-stream twin (used starting in Milestone 1 by the enhanced
//! `extract_audio`/`transcode`). `MultiInputGraph` (Task F1b) is the
//! multi-input variant that `overlay`, `filter_complex`, and `encode`
//! will build on in Milestone 2 — it is intentionally a small
//! primitive that only owns the filter graph and its buffer
//! sources/sinks; callers provide their own decoders and encoders.
//!
//! None of these primitives are called from the SQL surface that ships
//! in Milestone F. The module exists so Milestone 1 and 2 tasks can
//! land without re-opening the same design.

// `build_video_filter_graph`, `build_audio_filter_graph`,
// `MultiInputGraph`, and the audio pipeline skeletons are called from
// Milestone 1 / 2 tasks. M-F only ships the primitives and the one
// `VideoPipeline` struct that `transcode.rs` already uses.
#![allow(dead_code)]

use ffmpeg_next::codec;
use ffmpeg_next::filter;
use ffmpeg_next::util::frame::video::Video;
use ffmpeg_next::{frame, picture, Packet, Rational, Rescale};

use crate::codec_lookup;

/// Decode → filter → encode pipeline for a single video stream.
///
/// Keeps the exact packet-handling shape the old `Transcoder` had so
/// the current `transcode()` test suite keeps passing. New callers can
/// use the building blocks directly (`build_video_filter_graph` + a
/// plain encoder) when they need finer-grained control.
pub struct VideoPipeline {
    ost_index: usize,
    decoder: ffmpeg_next::decoder::Video,
    input_time_base: Rational,
    encoder: ffmpeg_next::encoder::Video,
    graph: filter::Graph,
}

impl VideoPipeline {
    /// Build a pipeline from an input stream, reusing the input codec
    /// as the output codec. Matches the historical `Transcoder::new`
    /// behavior so `transcode.rs` can migrate without touching tests.
    pub fn new(
        ist: &ffmpeg_next::format::stream::Stream,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_index: usize,
        filter_spec: &str,
    ) -> Self {
        let decoder_ctx = codec::context::Context::from_parameters(ist.parameters())
            .unwrap_or_else(|e| pgrx::error!("failed to create decoder context: {e}"));
        let mut decoder = decoder_ctx
            .decoder()
            .video()
            .unwrap_or_else(|e| pgrx::error!("failed to open decoder: {e}"));
        let input_time_base = ist.time_base();
        decoder.set_time_base(input_time_base);

        let mut graph = build_video_filter_graph_inner(&decoder, filter_spec);

        // Read resolved output dimensions/format from the validated buffersink.
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

        // Resolve the encoder via the uniform codec-lookup path so that
        // decode-only codecs surface the F7 error contract.
        let codec_id = decoder.id();
        let enc_codec = codec_lookup::find_encoder_by_id(codec_id, codec_lookup::CodecKind::Video)
            .unwrap_or_else(|e| pgrx::error!("{e}"));
        let enc_ctx = codec::context::Context::new_with_codec(enc_codec);
        let mut video_enc = enc_ctx
            .encoder()
            .video()
            .unwrap_or_else(|e| pgrx::error!("failed to create video encoder: {e}"));
        video_enc.set_width(out_width);
        video_enc.set_height(out_height);
        video_enc.set_format(out_pix_fmt);

        let enc_time_base = if let Some(frame_rate) = decoder.frame_rate() {
            video_enc.set_frame_rate(Some(frame_rate));
            Rational(frame_rate.denominator(), frame_rate.numerator())
        } else if filter_tb.denominator() != 0 {
            filter_tb
        } else {
            input_time_base
        };
        video_enc.set_time_base(enc_time_base);
        video_enc.set_bit_rate(decoder.bit_rate());

        unsafe {
            let dec_ptr = decoder.as_ptr();
            let enc_ptr = video_enc.as_mut_ptr();
            (*enc_ptr).gop_size = (*dec_ptr).gop_size;
            (*enc_ptr).max_b_frames = (*dec_ptr).max_b_frames;
        }

        let global_header = octx
            .format()
            .flags()
            .contains(ffmpeg_next::format::Flags::GLOBAL_HEADER);
        if global_header {
            video_enc.set_flags(codec::Flags::GLOBAL_HEADER);
        }

        let encoder = video_enc
            .open_as(enc_codec)
            .unwrap_or_else(|e| pgrx::error!("failed to open encoder: {e}"));

        let mut ost = octx
            .add_stream(enc_codec)
            .unwrap_or_else(|e| pgrx::error!("failed to add video output stream: {e}"));
        ost.set_parameters(&encoder);

        Self {
            ost_index,
            decoder,
            input_time_base,
            encoder,
            graph,
        }
    }

    pub fn decoder_time_base(&self) -> Rational {
        self.decoder.time_base()
    }

    pub fn input_time_base(&self) -> Rational {
        self.input_time_base
    }

    pub fn send_packet_to_decoder(&mut self, packet: &Packet) {
        self.decoder
            .send_packet(packet)
            .unwrap_or_else(|e| pgrx::error!("decode error: {e}"));
    }

    pub fn send_eof_to_decoder(&mut self) {
        let _ = self.decoder.send_eof();
    }

    pub fn receive_and_process_decoded_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut decoded = Video::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            let timestamp = decoded.timestamp();
            decoded.set_pts(timestamp);
            decoded.set_kind(picture::Type::None);
            self.graph
                .get("in")
                .unwrap()
                .source()
                .add(&decoded)
                .unwrap_or_else(|e| pgrx::error!("filter source error: {e}"));
            self.receive_and_process_filtered_frames(octx, ost_time_base);
        }
    }

    pub fn receive_and_process_filtered_frames(
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
                .unwrap_or_else(|e| pgrx::error!("encode error: {e}"));
            self.receive_and_process_encoded_packets(octx, ost_time_base);
        }
    }

    pub fn flush_filter(&mut self) {
        let _ = self.graph.get("in").unwrap().source().flush();
    }

    pub fn send_eof_to_encoder(&mut self) {
        let _ = self.encoder.send_eof();
    }

    pub fn receive_and_process_encoded_packets(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(self.ost_index);
            encoded.rescale_ts(self.input_time_base, ost_time_base);
            encoded.set_position(-1);
            encoded
                .write_interleaved(octx)
                .unwrap_or_else(|e| pgrx::error!("failed to write video packet: {e}"));
        }
    }
}

/// Assemble a `buffer → spec → buffersink` filter graph for a single
/// video decoder. Returns the validated graph. Exposed as a free
/// function so that callers that don't need the full
/// `VideoPipeline` (for example, a future frame-extractor) can still
/// reuse the wiring.
pub fn build_video_filter_graph(
    decoder: &ffmpeg_next::decoder::Video,
    spec: &str,
) -> filter::Graph {
    build_video_filter_graph_inner(decoder, spec)
}

fn build_video_filter_graph_inner(
    decoder: &ffmpeg_next::decoder::Video,
    spec: &str,
) -> filter::Graph {
    let mut graph = filter::Graph::new();
    let pix_fmt = Into::<ffmpeg_next::sys::AVPixelFormat>::into(decoder.format()) as i32;
    let aspect = decoder.aspect_ratio();
    let input_time_base = video_filter_input_time_base(decoder);
    let buffer_args = format!(
        "video_size={}x{}:pix_fmt={}:time_base={}/{}:pixel_aspect={}/{}",
        decoder.width(),
        decoder.height(),
        pix_fmt,
        input_time_base.numerator(),
        input_time_base.denominator(),
        aspect.numerator().max(1),
        aspect.denominator().max(1),
    );
    graph
        .add(&filter::find("buffer").unwrap(), "in", &buffer_args)
        .unwrap_or_else(|e| pgrx::error!("failed to add buffer source: {e}"));
    graph
        .add(&filter::find("buffersink").unwrap(), "out", "")
        .unwrap_or_else(|e| pgrx::error!("failed to add buffer sink: {e}"));
    graph
        .output("in", 0)
        .unwrap()
        .input("out", 0)
        .unwrap()
        .parse(spec)
        .unwrap_or_else(|e| pgrx::error!("failed to parse filter '{}': {e}", spec));
    graph
        .validate()
        .unwrap_or_else(|e| pgrx::error!("failed to validate filter graph: {e}"));
    graph
}

fn video_filter_input_time_base(decoder: &ffmpeg_next::decoder::Video) -> Rational {
    if let Some(frame_rate) = decoder.frame_rate() {
        if frame_rate.numerator() > 0 && frame_rate.denominator() > 0 {
            return Rational(frame_rate.denominator(), frame_rate.numerator());
        }
    }
    decoder.time_base()
}

// -----------------------------------------------------------------------------
// AudioPipeline (F1): audio-stream twin of VideoPipeline.
//
// Shared between `transcode` and `extract_audio`. Keeping the pump loop
// in one place means both paths inherit the same PTS normalization
// (ffmpeg decoders can omit timestamps on the first frame) and the same
// frame-move discipline — frames are never cloned between pipeline
// stages; the same frame buffer is reused for each decode→filter→encode
// step and mutated in place.
// -----------------------------------------------------------------------------

/// Assemble an `abuffer → spec → abuffersink` filter graph wired for a
/// specific decoder and encoder pair.
///
/// The sink is constrained to the encoder's sample rate / channel
/// layout / sample format so the filter graph itself performs any
/// needed resampling; callers don't need a separate `swresample`
/// context unless they're deliberately bypassing the filter path.
///
/// When the encoder requires a fixed frame size (i.e. it does not
/// advertise `Capabilities::VARIABLE_FRAME_SIZE`) the sink is told to
/// chunk output frames to that size.
pub fn build_audio_filter_graph(
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
        .unwrap_or_else(|e| pgrx::error!("failed to add abuffer source: {e}"));
    graph
        .add(&filter::find("abuffersink").unwrap(), "out", "")
        .unwrap_or_else(|e| pgrx::error!("failed to add abuffer sink: {e}"));
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
        .unwrap_or_else(|e| pgrx::error!("failed to parse audio filter '{}': {e}", spec));
    graph
        .validate()
        .unwrap_or_else(|e| pgrx::error!("failed to validate audio filter graph: {e}"));
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

/// Decode → filter → encode pipeline for a single audio stream.
///
/// Takes ownership of a pre-built decoder and encoder, wires an
/// `abuffer → spec → abuffersink` graph constrained to the encoder's
/// output format, and exposes the same pump API as `VideoPipeline`.
///
/// Frames are never cloned: `decoded` and `filtered` are reused across
/// iterations and mutated in place so pts normalization does not cost
/// an allocation per sample block.
pub struct AudioPipeline {
    ost_index: usize,
    decoder: ffmpeg_next::decoder::Audio,
    encoder: ffmpeg_next::encoder::Audio,
    encoder_time_base: Rational,
    graph: filter::Graph,
    next_decoded_pts: i64,
    next_encoded_pts: i64,
    filter_finished: bool,
}

impl AudioPipeline {
    pub fn new(
        decoder: ffmpeg_next::decoder::Audio,
        encoder: ffmpeg_next::encoder::Audio,
        filter_spec: &str,
        ost_index: usize,
    ) -> Self {
        let encoder_time_base = encoder.time_base();
        let graph = build_audio_filter_graph(&decoder, &encoder, filter_spec);
        Self {
            ost_index,
            decoder,
            encoder,
            encoder_time_base,
            graph,
            next_decoded_pts: 0,
            next_encoded_pts: 0,
            filter_finished: false,
        }
    }

    pub fn decoder_time_base(&self) -> Rational {
        self.decoder.time_base()
    }

    pub fn encoder(&self) -> &ffmpeg_next::encoder::Audio {
        &self.encoder
    }

    pub fn encoder_time_base(&self) -> Rational {
        self.encoder_time_base
    }

    pub fn send_packet_to_decoder(&mut self, packet: &Packet) {
        if self.filter_finished {
            return;
        }
        self.decoder
            .send_packet(packet)
            .unwrap_or_else(|e| pgrx::error!("audio decode error: {e}"));
    }

    pub fn send_eof_to_decoder(&mut self) {
        if self.filter_finished {
            return;
        }
        let _ = self.decoder.send_eof();
    }

    pub fn receive_and_process_decoded_frames(
        &mut self,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_time_base: Rational,
    ) {
        let mut decoded = frame::Audio::empty();
        while self.decoder.receive_frame(&mut decoded).is_ok() {
            if self.filter_finished {
                continue;
            }
            let frame_time_base = audio_frame_time_base(&self.decoder);
            let timestamp = decoded
                .timestamp()
                .map(|pts| pts.rescale(self.decoder.time_base(), frame_time_base))
                .unwrap_or(self.next_decoded_pts)
                .max(self.next_decoded_pts);
            decoded.set_pts(Some(timestamp));
            self.next_decoded_pts = timestamp.saturating_add(decoded.samples() as i64);
            let mut source = self.graph.get("in").unwrap();
            match source.source().add(&decoded) {
                Ok(()) => {}
                Err(ffmpeg_next::Error::Eof) => {
                    self.filter_finished = true;
                    continue;
                }
                Err(e) => pgrx::error!("audio filter source error: {e}"),
            }
            self.receive_and_process_filtered_frames(octx, ost_time_base);
        }
    }

    pub fn receive_and_process_filtered_frames(
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
            let timestamp = filtered
                .timestamp()
                .unwrap_or(self.next_encoded_pts)
                .max(self.next_encoded_pts);
            filtered.set_pts(Some(timestamp));
            self.next_encoded_pts = timestamp.saturating_add(filtered.samples() as i64);
            self.encoder.send_frame(&filtered).unwrap_or_else(|e| {
                pgrx::error!(
                    "audio encode error: {e} (frame: pts={:?} samples={} rate={} channels={} layout=0x{:x} format={:?}; encoder: frame_size={} rate={} channels={} layout=0x{:x} format={:?} time_base={}/{})",
                    filtered.timestamp(),
                    filtered.samples(),
                    filtered.rate(),
                    filtered.channels(),
                    filtered.channel_layout().bits(),
                    filtered.format(),
                    self.encoder.frame_size(),
                    self.encoder.rate(),
                    self.encoder.channels(),
                    self.encoder.channel_layout().bits(),
                    self.encoder.format(),
                    self.encoder_time_base.numerator(),
                    self.encoder_time_base.denominator(),
                )
            });
            self.receive_and_process_encoded_packets(octx, ost_time_base);
        }
    }

    pub fn flush_filter(&mut self) {
        let _ = self.graph.get("in").unwrap().source().flush();
    }

    pub fn send_eof_to_encoder(&mut self) {
        let _ = self.encoder.send_eof();
    }

    pub fn receive_and_process_encoded_packets(
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
                .unwrap_or_else(|e| pgrx::error!("failed to write audio packet: {e}"));
        }
    }
}

fn decoder_channel_layout(decoder: &ffmpeg_next::decoder::Audio) -> ffmpeg_next::ChannelLayout {
    if decoder.channel_layout().bits() != 0 {
        decoder.channel_layout()
    } else if decoder.channels() > 0 {
        ffmpeg_next::ChannelLayout::default(decoder.channels() as i32)
    } else {
        ffmpeg_next::ChannelLayout::STEREO
    }
}

fn audio_frame_time_base(decoder: &ffmpeg_next::decoder::Audio) -> Rational {
    if decoder.rate() > 0 {
        Rational::new(1, decoder.rate() as i32)
    } else {
        decoder.time_base()
    }
}

/// Copy a single input stream's parameters into a new output stream.
///
/// Returns the index of the newly-added output stream. Used by
/// `transcode` and the upcoming `trim` for stream-copy paths.
pub fn copy_stream(
    ist: &ffmpeg_next::format::stream::Stream,
    octx: &mut ffmpeg_next::format::context::Output,
) -> usize {
    let mut ost = octx
        .add_stream(codec::Id::None)
        .unwrap_or_else(|e| pgrx::error!("failed to add output stream: {e}"));
    ost.set_parameters(ist.parameters());
    unsafe { (*ost.parameters().as_mut_ptr()).codec_tag = 0 };
    ost.index()
}

// -----------------------------------------------------------------------------
// MultiInputGraph (F1b): primitive used by overlay, filter_complex, and
// any other function that needs to feed multiple decoded streams into
// one filter graph. It deliberately owns only the filter graph — NOT
// decoders or encoders. Callers build those themselves and use this as
// the plumbing layer.
//
// Milestone F ships the struct and the builder shape so Milestone 2's
// multi-input tasks have a stable API to target. The actual frame-push
// / frame-recv helpers go in alongside those tasks.
// -----------------------------------------------------------------------------

/// Kind of a declared input pad on the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Video,
    Audio,
}

/// Builder for [`MultiInputGraph`].
pub struct MultiInputGraphBuilder {
    inputs: Vec<(String, InputKind, String)>,
    outputs: Vec<(String, InputKind)>,
}

impl Default for MultiInputGraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiInputGraphBuilder {
    pub fn new() -> Self {
        Self {
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    pub fn add_video_input(
        &mut self,
        label: &str,
        width: u32,
        height: u32,
        pix_fmt: ffmpeg_next::format::Pixel,
        time_base: Rational,
        sample_aspect_ratio: Rational,
    ) -> &mut Self {
        let pix_fmt_int = Into::<ffmpeg_next::sys::AVPixelFormat>::into(pix_fmt) as i32;
        let args = format!(
            "video_size={}x{}:pix_fmt={}:time_base={}/{}:pixel_aspect={}/{}",
            width,
            height,
            pix_fmt_int,
            time_base.numerator(),
            time_base.denominator(),
            sample_aspect_ratio.numerator().max(1),
            sample_aspect_ratio.denominator().max(1),
        );
        self.inputs.push((label.to_owned(), InputKind::Video, args));
        self
    }

    pub fn add_audio_input(
        &mut self,
        label: &str,
        sample_rate: u32,
        sample_fmt: ffmpeg_next::format::Sample,
        channel_layout: ffmpeg_next::ChannelLayout,
        time_base: Rational,
    ) -> &mut Self {
        let sample_fmt_int = Into::<ffmpeg_next::sys::AVSampleFormat>::into(sample_fmt) as i32;
        let args = format!(
            "time_base={}/{}:sample_rate={}:sample_fmt={}:channel_layout=0x{:x}",
            time_base.numerator(),
            time_base.denominator(),
            sample_rate,
            sample_fmt_int,
            channel_layout.bits(),
        );
        self.inputs.push((label.to_owned(), InputKind::Audio, args));
        self
    }

    pub fn add_video_output(&mut self, label: &str) -> &mut Self {
        self.outputs.push((label.to_owned(), InputKind::Video));
        self
    }

    pub fn add_audio_output(&mut self, label: &str) -> &mut Self {
        self.outputs.push((label.to_owned(), InputKind::Audio));
        self
    }

    /// Parse the user/internal filter spec and wire up the pre-declared
    /// sources and sinks. Validation is performed by FFmpeg; callers
    /// that accept user strings should first run
    /// `filter_safety::validate_filter_spec`.
    pub fn build(self, filter_spec: &str) -> MultiInputGraph {
        let mut graph = filter::Graph::new();

        for (label, kind, args) in &self.inputs {
            let src_name = match kind {
                InputKind::Video => "buffer",
                InputKind::Audio => "abuffer",
            };
            graph
                .add(&filter::find(src_name).unwrap(), label, args)
                .unwrap_or_else(|e| pgrx::error!("failed to add {src_name} '{label}': {e}"));
        }
        for (label, kind) in &self.outputs {
            let sink_name = match kind {
                InputKind::Video => "buffersink",
                InputKind::Audio => "abuffersink",
            };
            graph
                .add(&filter::find(sink_name).unwrap(), label, "")
                .unwrap_or_else(|e| pgrx::error!("failed to add {sink_name} '{label}': {e}"));
        }

        // Chain the `.output()` / `.input()` declarations so the
        // filter spec parser knows where to attach. Each Parser method
        // consumes `self` and returns a new Parser, so we thread a
        // single parser value through the loops.
        let input_labels: Vec<String> = self.inputs.iter().map(|(l, _, _)| l.clone()).collect();
        let output_labels: Vec<String> = self.outputs.iter().map(|(l, _)| l.clone()).collect();

        if !input_labels.is_empty() {
            let mut parser = graph
                .output(&input_labels[0], 0)
                .unwrap_or_else(|e| pgrx::error!("multi-input graph output: {e}"));
            for label in &input_labels[1..] {
                parser = parser
                    .output(label, 0)
                    .unwrap_or_else(|e| pgrx::error!("multi-input graph output: {e}"));
            }
            for label in &output_labels {
                parser = parser
                    .input(label, 0)
                    .unwrap_or_else(|e| pgrx::error!("multi-input graph input: {e}"));
            }
            parser
                .parse(filter_spec)
                .unwrap_or_else(|e| pgrx::error!("failed to parse multi-input filter graph: {e}"));
        } else if !output_labels.is_empty() {
            // Source-only graphs (no declared inputs) still go through
            // the parser so FFmpeg wires the sinks.
            let mut parser = graph
                .input(&output_labels[0], 0)
                .unwrap_or_else(|e| pgrx::error!("multi-input graph input: {e}"));
            for label in &output_labels[1..] {
                parser = parser
                    .input(label, 0)
                    .unwrap_or_else(|e| pgrx::error!("multi-input graph input: {e}"));
            }
            parser
                .parse(filter_spec)
                .unwrap_or_else(|e| pgrx::error!("failed to parse multi-input filter graph: {e}"));
        }

        graph
            .validate()
            .unwrap_or_else(|e| pgrx::error!("failed to validate multi-input graph: {e}"));

        MultiInputGraph {
            graph,
            input_labels,
            output_labels,
        }
    }
}

/// N-input, M-output filter graph owned independently of decoders /
/// encoders. See module-level docs for the overall design rationale.
pub struct MultiInputGraph {
    graph: filter::Graph,
    input_labels: Vec<String>,
    output_labels: Vec<String>,
}

impl MultiInputGraph {
    pub fn builder() -> MultiInputGraphBuilder {
        MultiInputGraphBuilder::new()
    }

    pub fn graph(&mut self) -> &mut filter::Graph {
        &mut self.graph
    }

    pub fn input_label(&self, index: usize) -> Option<&str> {
        self.input_labels.get(index).map(String::as_str)
    }

    pub fn output_label(&self, index: usize) -> Option<&str> {
        self.output_labels.get(index).map(String::as_str)
    }

    pub fn num_inputs(&self) -> usize {
        self.input_labels.len()
    }

    pub fn num_outputs(&self) -> usize {
        self.output_labels.len()
    }

    /// Push a decoded video frame into the N-th input. Thin wrapper so
    /// callers in Milestone 2 don't need to know the label strings.
    pub fn push_video_frame(&mut self, input_index: usize, frame: &frame::Video) {
        let label = self
            .input_labels
            .get(input_index)
            .unwrap_or_else(|| pgrx::error!("input index {input_index} out of range"))
            .clone();
        self.graph
            .get(&label)
            .unwrap()
            .source()
            .add(frame)
            .unwrap_or_else(|e| pgrx::error!("multi-input push video frame: {e}"));
    }

    pub fn push_audio_frame(&mut self, input_index: usize, frame: &frame::Audio) {
        let label = self
            .input_labels
            .get(input_index)
            .unwrap_or_else(|| pgrx::error!("input index {input_index} out of range"))
            .clone();
        self.graph
            .get(&label)
            .unwrap()
            .source()
            .add(frame)
            .unwrap_or_else(|e| pgrx::error!("multi-input push audio frame: {e}"));
    }

    pub fn flush_input(&mut self, input_index: usize) {
        let label = match self.input_labels.get(input_index) {
            Some(l) => l.clone(),
            None => return,
        };
        let _ = self.graph.get(&label).unwrap().source().flush();
    }

    pub fn try_recv_video(&mut self, output_index: usize, frame: &mut frame::Video) -> bool {
        let label = match self.output_labels.get(output_index) {
            Some(l) => l.clone(),
            None => return false,
        };
        self.graph.get(&label).unwrap().sink().frame(frame).is_ok()
    }

    pub fn try_recv_audio(&mut self, output_index: usize, frame: &mut frame::Audio) -> bool {
        let label = match self.output_labels.get(output_index) {
            Some(l) => l.clone(),
            None => return false,
        };
        self.graph.get(&label).unwrap().sink().frame(frame).is_ok()
    }
}
