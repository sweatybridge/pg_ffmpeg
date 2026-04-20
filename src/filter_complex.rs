#![allow(dead_code)]

use pgrx::prelude::*;

use crate::codec_lookup::{self, CodecKind};
use crate::filter_safety;
use crate::limits;
use crate::mem_io::{MemInput, MemOutput};
use crate::pipeline::{InputKind, MultiInputGraph};

use ffmpeg_next::codec::{self, Id as CodecId};
use ffmpeg_next::format::{self, Pixel, Sample};
use ffmpeg_next::media::Type;
use ffmpeg_next::{frame, picture, ChannelLayout, Error as FfmpegError, Packet, Rational, Rescale};

const INTERNAL_PREFIX: &str = "pgffmpeg_";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum StreamKind {
    Video,
    Audio,
}

impl StreamKind {
    fn suffix(self) -> &'static str {
        match self {
            StreamKind::Video => "v",
            StreamKind::Audio => "a",
        }
    }

    fn input_kind(self) -> InputKind {
        match self {
            StreamKind::Video => InputKind::Video,
            StreamKind::Audio => InputKind::Audio,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InputRef {
    index: usize,
    kind: StreamKind,
}

#[derive(Debug, Clone)]
pub(crate) struct RewrittenGraph {
    pub(crate) graph: String,
    refs: Vec<InputRef>,
    has_vout: bool,
    has_aout: bool,
}

#[derive(Debug)]
enum GraphRewriteError {
    EmptyGraph,
    MissingOutput,
    InternalLabelCollision { label: String },
    MalformedInputLabel { label: String },
    NegativeInputIndex { label: String },
    NonNumericInputIndex { label: String },
    InvalidStreamKind { label: String },
    InputOutOfRange { index: usize, len: usize },
    UnusedInput { index: usize },
}

impl std::fmt::Display for GraphRewriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphRewriteError::EmptyGraph => write!(f, "pg_ffmpeg: filter graph is empty"),
            GraphRewriteError::MissingOutput => write!(
                f,
                "pg_ffmpeg: filter graph must declare at least one of [vout] or [aout]"
            ),
            GraphRewriteError::InternalLabelCollision { label } => write!(
                f,
                "pg_ffmpeg: filter graph label [{label}] is reserved for internal use"
            ),
            GraphRewriteError::MalformedInputLabel { label } => write!(
                f,
                "pg_ffmpeg: input label [{label}] must use [iN:v] or [iN:a]"
            ),
            GraphRewriteError::NegativeInputIndex { label } => {
                write!(f, "pg_ffmpeg: input label [{label}] uses a negative index")
            }
            GraphRewriteError::NonNumericInputIndex { label } => write!(
                f,
                "pg_ffmpeg: input label [{label}] uses a non-numeric index"
            ),
            GraphRewriteError::InvalidStreamKind { label } => {
                write!(f, "pg_ffmpeg: input label [{label}] must end in :v or :a")
            }
            GraphRewriteError::InputOutOfRange { index, len } => write!(
                f,
                "pg_ffmpeg: input label references i{index}, but inputs has only {len} elements"
            ),
            GraphRewriteError::UnusedInput { index } => write!(
                f,
                "pg_ffmpeg: inputs[{index}] is not referenced by the filter graph"
            ),
        }
    }
}

#[pg_extern]
fn filter_complex(
    inputs: Array<'_, &[u8]>,
    filter_graph: &str,
    format: default!(Option<&str>, "NULL"),
    codec: default!(Option<&str>, "NULL"),
    audio_codec: default!(Option<&str>, "NULL"),
    hwaccel: default!(bool, false),
) -> Vec<u8> {
    if hwaccel {
        error!("pg_ffmpeg: filter_complex hwaccel is not implemented yet");
    }

    let input_count = inputs.len();
    limits::check_array_size(input_count).unwrap_or_else(|e| error!("{e}"));

    let mut borrowed = Vec::with_capacity(input_count);
    for (i, item) in inputs.iter().enumerate() {
        let bytes = item.unwrap_or_else(|| error!("pg_ffmpeg: inputs[{i}] must not be NULL"));
        limits::check_input_size(bytes.len()).unwrap_or_else(|e| error!("{e}"));
        borrowed.push(bytes);
    }

    filter_complex_slices(
        &borrowed,
        filter_graph,
        format.unwrap_or("matroska"),
        codec,
        audio_codec,
    )
}

pub(crate) fn filter_complex_slices(
    inputs: &[&[u8]],
    filter_graph: &str,
    out_format: &str,
    codec_name: Option<&str>,
    audio_codec_name: Option<&str>,
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    limits::check_array_size(inputs.len()).unwrap_or_else(|e| error!("{e}"));
    for data in inputs {
        limits::check_input_size(data.len()).unwrap_or_else(|e| error!("{e}"));
    }

    let rewritten = rewrite_filter_graph_for_inputs(filter_graph, inputs.len())
        .unwrap_or_else(|e| error!("{e}"));
    filter_safety::validate_filter_spec(&rewritten.graph).unwrap_or_else(|e| error!("{e}"));

    let mut states = open_input_states(inputs);
    let mut graph = build_multi_input_graph(&rewritten, &mut states);
    let mut octx = MemOutput::open(out_format);

    let mut video_pipeline = if rewritten.has_vout {
        Some(VideoOutputPipeline::new(
            &mut graph,
            &mut octx,
            0,
            codec_name,
            first_video_decoder(&states).unwrap_or_else(|| {
                error!(
                    "pg_ffmpeg: filter graph declares [vout] but no referenced video input exists"
                )
            }),
        ))
    } else {
        None
    };

    let audio_stream_index = usize::from(video_pipeline.is_some());
    let mut audio_pipeline = if rewritten.has_aout {
        Some(AudioOutputPipeline::new(
            &mut graph,
            &mut octx,
            audio_stream_index,
            audio_codec_name,
            first_audio_decoder(&states).unwrap_or_else(|| {
                error!(
                    "pg_ffmpeg: filter graph declares [aout] but no referenced audio input exists"
                )
            }),
        ))
    } else {
        None
    };

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write header: {e}"));
    let output_time_bases = output_time_bases(&octx);

    process_all_inputs(
        &mut states,
        &mut graph,
        &mut video_pipeline,
        &mut audio_pipeline,
        &mut octx,
        &output_time_bases,
    );

    drain_outputs(
        &mut graph,
        &mut video_pipeline,
        &mut audio_pipeline,
        &mut octx,
        &output_time_bases,
    );

    if let Some(video) = video_pipeline.as_mut() {
        video.send_eof();
        video.receive_packets(&mut octx, output_time_bases[video.ost_index]);
    }
    if let Some(audio) = audio_pipeline.as_mut() {
        audio.send_eof();
        audio.receive_packets(&mut octx, output_time_bases[audio.ost_index]);
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));
    octx.into_data()
}

fn rewrite_filter_graph_for_inputs(
    filter_graph: &str,
    input_count: usize,
) -> Result<RewrittenGraph, GraphRewriteError> {
    if filter_graph.trim().is_empty() {
        return Err(GraphRewriteError::EmptyGraph);
    }

    let bytes = filter_graph.as_bytes();
    let mut out = String::with_capacity(filter_graph.len());
    let mut refs = Vec::<InputRef>::new();
    let mut used_inputs = vec![false; input_count];
    let mut has_vout = false;
    let mut has_aout = false;
    let mut i = 0usize;
    let mut quote = None::<u8>;

    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' {
            out.push(b as char);
            if i + 1 < bytes.len() {
                out.push(bytes[i + 1] as char);
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }

        if let Some(q) = quote {
            out.push(b as char);
            if b == q {
                quote = None;
            }
            i += 1;
            continue;
        }

        if b == b'\'' || b == b'"' {
            quote = Some(b);
            out.push(b as char);
            i += 1;
            continue;
        }

        if b != b'[' {
            out.push(b as char);
            i += 1;
            continue;
        }

        let label_start = i + 1;
        let mut j = label_start;
        while j < bytes.len() {
            if bytes[j] == b'\\' {
                j = (j + 2).min(bytes.len());
                continue;
            }
            if bytes[j] == b']' {
                break;
            }
            j += 1;
        }
        if j >= bytes.len() {
            out.push(b as char);
            i += 1;
            continue;
        }

        let label = &filter_graph[label_start..j];
        if label.starts_with(INTERNAL_PREFIX) {
            return Err(GraphRewriteError::InternalLabelCollision {
                label: label.to_owned(),
            });
        }
        if label == "vout" {
            has_vout = true;
        } else if label == "aout" {
            has_aout = true;
        }

        if let Some(input_ref) = parse_input_label_if_reserved(label, input_count)? {
            if let Some(used) = used_inputs.get_mut(input_ref.index) {
                *used = true;
            }
            refs.push(input_ref);
            out.push('[');
            out.push_str(&internal_input_label(input_ref));
            out.push(']');
        } else {
            out.push('[');
            out.push_str(label);
            out.push(']');
        }
        i = j + 1;
    }

    if !has_vout && !has_aout {
        return Err(GraphRewriteError::MissingOutput);
    }
    for (index, used) in used_inputs.iter().enumerate() {
        if !used {
            return Err(GraphRewriteError::UnusedInput { index });
        }
    }

    refs.sort_unstable();
    refs.dedup();

    Ok(RewrittenGraph {
        graph: out,
        refs,
        has_vout,
        has_aout,
    })
}

fn parse_input_label_if_reserved(
    label: &str,
    input_count: usize,
) -> Result<Option<InputRef>, GraphRewriteError> {
    if !looks_like_reserved_input_label(label) {
        return Ok(None);
    }
    parse_input_label(label, input_count).map(Some)
}

fn looks_like_reserved_input_label(label: &str) -> bool {
    let Some(rest) = label.strip_prefix('i') else {
        return false;
    };
    let Some(first) = rest.bytes().next() else {
        return false;
    };
    if first.is_ascii_digit() || first == b'-' || first == b':' {
        return true;
    }
    if let Some((index_part, kind_part)) = rest.split_once(':') {
        return index_part.len() == 1 && kind_part.len() == 1;
    }
    false
}

fn parse_input_label(label: &str, input_count: usize) -> Result<InputRef, GraphRewriteError> {
    let Some((index_part, kind_part)) = label.strip_prefix('i').and_then(|s| s.split_once(':'))
    else {
        return Err(GraphRewriteError::MalformedInputLabel {
            label: label.to_owned(),
        });
    };

    if index_part.starts_with('-') {
        return Err(GraphRewriteError::NegativeInputIndex {
            label: label.to_owned(),
        });
    }
    if index_part.is_empty() || !index_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(GraphRewriteError::NonNumericInputIndex {
            label: label.to_owned(),
        });
    }

    let kind = match kind_part {
        "v" => StreamKind::Video,
        "a" => StreamKind::Audio,
        _ => {
            return Err(GraphRewriteError::InvalidStreamKind {
                label: label.to_owned(),
            })
        }
    };
    let index =
        index_part
            .parse::<usize>()
            .map_err(|_| GraphRewriteError::NonNumericInputIndex {
                label: label.to_owned(),
            })?;
    if index >= input_count {
        return Err(GraphRewriteError::InputOutOfRange {
            index,
            len: input_count,
        });
    }

    Ok(InputRef { index, kind })
}

fn internal_input_label(input_ref: InputRef) -> String {
    format!(
        "{INTERNAL_PREFIX}i{}_{}",
        input_ref.index,
        input_ref.kind.suffix()
    )
}

struct InputState<'a> {
    ictx: MemInput<'a>,
    video_stream: Option<usize>,
    audio_stream: Option<usize>,
    video_decoder: Option<ffmpeg_next::decoder::Video>,
    audio_decoder: Option<ffmpeg_next::decoder::Audio>,
    video_graph_input: Option<usize>,
    audio_graph_input: Option<usize>,
    next_decoded_audio_pts: i64,
}

fn open_input_states<'a>(inputs: &'a [&'a [u8]]) -> Vec<InputState<'a>> {
    inputs
        .iter()
        .map(|data| InputState {
            ictx: MemInput::open(data),
            video_stream: None,
            audio_stream: None,
            video_decoder: None,
            audio_decoder: None,
            video_graph_input: None,
            audio_graph_input: None,
            next_decoded_audio_pts: 0,
        })
        .collect()
}

fn build_multi_input_graph(
    rewritten: &RewrittenGraph,
    states: &mut [InputState<'_>],
) -> MultiInputGraph {
    let mut builder = MultiInputGraph::builder();

    for input_ref in &rewritten.refs {
        let state = &mut states[input_ref.index];
        match input_ref.kind {
            StreamKind::Video => {
                let (stream_index, decoder) = open_video_decoder(&state.ictx);
                let graph_index = builder_input_index(&rewritten.refs, *input_ref);
                builder.add_video_input(
                    &internal_input_label(*input_ref),
                    decoder.width(),
                    decoder.height(),
                    decoder.format(),
                    decoder.time_base(),
                    decoder.aspect_ratio(),
                );
                state.video_stream = Some(stream_index);
                state.video_decoder = Some(decoder);
                state.video_graph_input = Some(graph_index);
            }
            StreamKind::Audio => {
                let (stream_index, decoder) = open_audio_decoder(&state.ictx);
                let graph_index = builder_input_index(&rewritten.refs, *input_ref);
                builder.add_audio_input(
                    &internal_input_label(*input_ref),
                    decoder.rate(),
                    decoder.format(),
                    decoder_channel_layout(&decoder),
                    decoder.time_base(),
                );
                state.audio_stream = Some(stream_index);
                state.audio_decoder = Some(decoder);
                state.audio_graph_input = Some(graph_index);
            }
        }
    }

    if rewritten.has_vout {
        builder.add_video_output("vout");
    }
    if rewritten.has_aout {
        builder.add_audio_output("aout");
    }

    builder.build(&rewritten.graph)
}

fn builder_input_index(refs: &[InputRef], needle: InputRef) -> usize {
    refs.iter()
        .position(|input_ref| *input_ref == needle)
        .unwrap_or_else(|| error!("pg_ffmpeg: internal input mapping error"))
}

fn open_video_decoder(ictx: &MemInput<'_>) -> (usize, ffmpeg_next::decoder::Video) {
    let stream = ictx
        .streams()
        .best(Type::Video)
        .unwrap_or_else(|| error!("pg_ffmpeg: referenced video input has no video stream"));
    let stream_index = stream.index();
    let decoder_ctx = codec::context::Context::from_parameters(stream.parameters())
        .unwrap_or_else(|e| error!("failed to create video decoder context: {e}"));
    let mut decoder = decoder_ctx
        .decoder()
        .video()
        .unwrap_or_else(|e| error!("failed to open video decoder: {e}"));
    decoder.set_time_base(stream.time_base());
    if let Some(rate) = preferred_video_frame_rate(&stream, &decoder) {
        decoder.set_frame_rate(Some(rate));
    }
    (stream_index, decoder)
}

fn open_audio_decoder(ictx: &MemInput<'_>) -> (usize, ffmpeg_next::decoder::Audio) {
    let stream = ictx
        .streams()
        .best(Type::Audio)
        .unwrap_or_else(|| error!("pg_ffmpeg: referenced audio input has no audio stream"));
    let stream_index = stream.index();
    let decoder_ctx = codec::context::Context::from_parameters(stream.parameters())
        .unwrap_or_else(|e| error!("failed to create audio decoder context: {e}"));
    let mut decoder = decoder_ctx
        .decoder()
        .audio()
        .unwrap_or_else(|e| error!("failed to open audio decoder: {e}"));
    decoder
        .set_parameters(stream.parameters())
        .unwrap_or_else(|e| error!("failed to set audio decoder parameters: {e}"));
    (stream_index, decoder)
}

struct VideoOutputPipeline {
    ost_index: usize,
    encoder: ffmpeg_next::encoder::Video,
    encoder_time_base: Rational,
    filter_time_base: Rational,
    next_pts: i64,
}

impl VideoOutputPipeline {
    fn new(
        graph: &mut MultiInputGraph,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_index: usize,
        codec_name: Option<&str>,
        source_decoder: &ffmpeg_next::decoder::Video,
    ) -> Self {
        let (width, height, pix_fmt, filter_time_base) = resolved_video_output(graph);
        let (selected_codec, codec_label) = resolve_video_encoder(codec_name, source_decoder.id());
        let encoder_time_base = if let Some(frame_rate) = source_decoder.frame_rate() {
            Rational(frame_rate.denominator(), frame_rate.numerator())
        } else if filter_time_base.numerator() > 0 && filter_time_base.denominator() > 0 {
            filter_time_base
        } else {
            source_decoder.time_base()
        };
        let encoder = open_video_encoder(
            octx,
            selected_codec,
            &codec_label,
            source_decoder,
            width,
            height,
            pix_fmt,
            encoder_time_base,
        );

        let mut ost = octx
            .add_stream(selected_codec)
            .unwrap_or_else(|e| error!("failed to add video output stream: {e}"));
        ost.set_time_base(encoder_time_base);
        ost.set_parameters(&encoder);
        unsafe {
            (*ost.parameters().as_mut_ptr()).codec_tag = 0;
        }

        Self {
            ost_index,
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
            .unwrap_or(self.next_pts);
        frame.set_pts(Some(pts.max(self.next_pts)));
        self.next_pts = frame.pts().unwrap_or(self.next_pts).saturating_add(1);
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

struct AudioOutputPipeline {
    ost_index: usize,
    encoder: ffmpeg_next::encoder::Audio,
    encoder_time_base: Rational,
    next_pts: i64,
}

impl AudioOutputPipeline {
    fn new(
        graph: &mut MultiInputGraph,
        octx: &mut ffmpeg_next::format::context::Output,
        ost_index: usize,
        codec_name: Option<&str>,
        source_decoder: &ffmpeg_next::decoder::Audio,
    ) -> Self {
        let (selected_codec, codec_label) = resolve_audio_encoder(codec_name, source_decoder.id());
        let audio_props = selected_codec
            .audio()
            .unwrap_or_else(|_| error!("pg_ffmpeg: selected encoder is not audio-capable"));
        let sample_rate = resolve_audio_sample_rate(source_decoder, &audio_props);
        let channel_layout = resolve_audio_channel_layout(source_decoder, &audio_props);
        let sample_format = resolve_audio_sample_format(source_decoder, &audio_props);
        let encoder_time_base = Rational::new(1, sample_rate as i32);
        let encoder = open_audio_encoder(
            octx,
            selected_codec,
            &codec_label,
            source_decoder,
            sample_rate,
            channel_layout,
            sample_format,
            encoder_time_base,
        );

        if let Some(output_index) = graph_output_index(graph, "aout") {
            if encoder.frame_size() > 0 {
                let output_label = graph.output_label(output_index).unwrap().to_owned();
                graph
                    .graph()
                    .get(&output_label)
                    .unwrap()
                    .sink()
                    .set_frame_size(encoder.frame_size());
            }
        }

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
            encoder,
            encoder_time_base,
            next_pts: 0,
        }
    }

    fn send_frame(&mut self, frame: &frame::Audio) {
        let mut frame = frame.clone();
        let pts = frame
            .timestamp()
            .unwrap_or(self.next_pts)
            .max(self.next_pts);
        frame.set_pts(Some(pts));
        self.next_pts = pts.saturating_add(frame.samples() as i64);
        self.encoder.send_frame(&frame).unwrap_or_else(|e| {
            error!(
                "audio encode error: {e} (frame format={:?} rate={} layout=0x{:x}; encoder format={:?} rate={} layout=0x{:x})",
                frame.format(),
                frame.rate(),
                frame.channel_layout().bits(),
                self.encoder.format(),
                self.encoder.rate(),
                self.encoder.channel_layout().bits(),
            )
        });
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
                .unwrap_or_else(|e| error!("failed to write audio packet: {e}"));
        }
    }

    fn send_eof(&mut self) {
        let _ = self.encoder.send_eof();
    }
}

fn process_all_inputs(
    states: &mut [InputState<'_>],
    graph: &mut MultiInputGraph,
    video_pipeline: &mut Option<VideoOutputPipeline>,
    audio_pipeline: &mut Option<AudioOutputPipeline>,
    octx: &mut ffmpeg_next::format::context::Output,
    output_time_bases: &[Rational],
) {
    if states.is_empty() {
        return;
    }

    let mut active = vec![true; states.len()];
    let mut active_count = active.len();
    let mut index = 0usize;

    while active_count > 0 {
        if active[index]
            && !process_next_input_packet(
                &mut states[index],
                graph,
                video_pipeline,
                audio_pipeline,
                octx,
                output_time_bases,
            )
        {
            active[index] = false;
            active_count -= 1;
        }
        index = (index + 1) % states.len();
    }
}

fn process_next_input_packet(
    state: &mut InputState<'_>,
    graph: &mut MultiInputGraph,
    video_pipeline: &mut Option<VideoOutputPipeline>,
    audio_pipeline: &mut Option<AudioOutputPipeline>,
    octx: &mut ffmpeg_next::format::context::Output,
    output_time_bases: &[Rational],
) -> bool {
    loop {
        let mut packet = Packet::empty();
        match packet.read(&mut state.ictx) {
            Ok(()) => {
                let stream_index = packet.stream();
                let stream_time_base = state
                    .ictx
                    .stream(stream_index)
                    .unwrap_or_else(|| error!("pg_ffmpeg: packet references missing stream"))
                    .time_base();
                if Some(stream_index) == state.video_stream {
                    process_video_packet(state, packet, stream_time_base, graph);
                    drain_outputs(
                        graph,
                        video_pipeline,
                        audio_pipeline,
                        octx,
                        output_time_bases,
                    );
                    return true;
                }
                if Some(stream_index) == state.audio_stream {
                    process_audio_packet(state, packet, stream_time_base, graph);
                    drain_outputs(
                        graph,
                        video_pipeline,
                        audio_pipeline,
                        octx,
                        output_time_bases,
                    );
                    return true;
                }
            }
            Err(FfmpegError::Eof) => {
                flush_state(state, graph);
                drain_outputs(
                    graph,
                    video_pipeline,
                    audio_pipeline,
                    octx,
                    output_time_bases,
                );
                return false;
            }
            Err(_) => {}
        }
    }
}

fn process_video_packet(
    state: &mut InputState<'_>,
    mut packet: Packet,
    stream_time_base: Rational,
    graph: &mut MultiInputGraph,
) {
    let decoder = state
        .video_decoder
        .as_mut()
        .unwrap_or_else(|| error!("pg_ffmpeg: missing video decoder"));
    packet.rescale_ts(stream_time_base, decoder.time_base());
    decoder
        .send_packet(&packet)
        .unwrap_or_else(|e| error!("video decode error: {e}"));
    receive_video_frames(decoder, state.video_graph_input, graph);
}

fn process_audio_packet(
    state: &mut InputState<'_>,
    mut packet: Packet,
    stream_time_base: Rational,
    graph: &mut MultiInputGraph,
) {
    let decoder = state
        .audio_decoder
        .as_mut()
        .unwrap_or_else(|| error!("pg_ffmpeg: missing audio decoder"));
    normalize_audio_packet_duration(&mut packet, stream_time_base, decoder.time_base());
    decoder
        .send_packet(&packet)
        .unwrap_or_else(|e| error!("audio decode error: {e}"));
    receive_audio_frames(
        decoder,
        &mut state.next_decoded_audio_pts,
        state.audio_graph_input,
        graph,
    );
}

fn flush_state(state: &mut InputState<'_>, graph: &mut MultiInputGraph) {
    if let Some(decoder) = state.video_decoder.as_mut() {
        let _ = decoder.send_eof();
        receive_video_frames(decoder, state.video_graph_input, graph);
    }
    if let Some(input_index) = state.video_graph_input {
        graph.flush_input(input_index);
    }

    if let Some(decoder) = state.audio_decoder.as_mut() {
        let _ = decoder.send_eof();
        receive_audio_frames(
            decoder,
            &mut state.next_decoded_audio_pts,
            state.audio_graph_input,
            graph,
        );
    }
    if let Some(input_index) = state.audio_graph_input {
        graph.flush_input(input_index);
    }
}

fn receive_video_frames(
    decoder: &mut ffmpeg_next::decoder::Video,
    graph_input: Option<usize>,
    graph: &mut MultiInputGraph,
) {
    let Some(graph_input) = graph_input else {
        return;
    };
    let mut decoded = frame::Video::empty();
    while decoder.receive_frame(&mut decoded).is_ok() {
        let timestamp = decoded.timestamp();
        decoded.set_pts(timestamp);
        decoded.set_kind(picture::Type::None);
        graph.push_video_frame(graph_input, &decoded);
    }
}

fn receive_audio_frames(
    decoder: &mut ffmpeg_next::decoder::Audio,
    next_decoded_pts: &mut i64,
    graph_input: Option<usize>,
    graph: &mut MultiInputGraph,
) {
    let Some(graph_input) = graph_input else {
        return;
    };
    let mut decoded = frame::Audio::empty();
    while decoder.receive_frame(&mut decoded).is_ok() {
        let frame_time_base = audio_frame_time_base(decoder);
        let pts = decoded
            .timestamp()
            .map(|pts| pts.rescale(decoder.time_base(), frame_time_base))
            .unwrap_or(*next_decoded_pts)
            .max(*next_decoded_pts);
        decoded.set_pts(Some(pts));
        *next_decoded_pts = pts.saturating_add(decoded.samples() as i64);
        graph.push_audio_frame(graph_input, &decoded);
    }
}

fn drain_outputs(
    graph: &mut MultiInputGraph,
    video_pipeline: &mut Option<VideoOutputPipeline>,
    audio_pipeline: &mut Option<AudioOutputPipeline>,
    octx: &mut ffmpeg_next::format::context::Output,
    output_time_bases: &[Rational],
) {
    if let Some(video) = video_pipeline.as_mut() {
        let mut filtered = frame::Video::empty();
        while graph.try_recv_video(0, &mut filtered) {
            video.send_frame(&filtered);
            video.receive_packets(octx, output_time_bases[video.ost_index]);
        }
    }

    if let Some(audio) = audio_pipeline.as_mut() {
        let output_index = if video_pipeline.is_some() { 1 } else { 0 };
        let mut filtered = frame::Audio::empty();
        while graph.try_recv_audio(output_index, &mut filtered) {
            audio.send_frame(&filtered);
            audio.receive_packets(octx, output_time_bases[audio.ost_index]);
        }
    }
}

fn output_time_bases(octx: &ffmpeg_next::format::context::Output) -> Vec<Rational> {
    octx.streams().map(|stream| stream.time_base()).collect()
}

fn graph_output_index(graph: &MultiInputGraph, label: &str) -> Option<usize> {
    (0..graph.num_outputs()).find(|i| graph.output_label(*i) == Some(label))
}

fn first_video_decoder<'a>(
    states: &'a [InputState<'_>],
) -> Option<&'a ffmpeg_next::decoder::Video> {
    states.iter().find_map(|state| state.video_decoder.as_ref())
}

fn first_audio_decoder<'a>(
    states: &'a [InputState<'_>],
) -> Option<&'a ffmpeg_next::decoder::Audio> {
    states.iter().find_map(|state| state.audio_decoder.as_ref())
}

fn resolved_video_output(graph: &mut MultiInputGraph) -> (u32, u32, Pixel, Rational) {
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

fn resolve_video_encoder(
    requested: Option<&str>,
    source_id: codec::Id,
) -> (ffmpeg_next::Codec, String) {
    if let Some(name) = requested {
        let codec =
            codec_lookup::find_encoder(name, CodecKind::Video).unwrap_or_else(|e| error!("{e}"));
        (codec, codec.name().to_owned())
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
        (codec, codec.name().to_owned())
    } else {
        let codec = codec::encoder::find(source_id).unwrap_or_else(|| {
            let fallback = codec::encoder::find(CodecId::AAC)
                .unwrap_or_else(|| error!("pg_ffmpeg: no audio encoder found"));
            fallback
        });
        (codec, codec.name().to_owned())
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
    if let Some(frame_rate) = decoder.frame_rate() {
        encoder.set_frame_rate(Some(frame_rate));
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

#[allow(clippy::too_many_arguments)]
fn open_audio_encoder(
    octx: &ffmpeg_next::format::context::Output,
    selected_codec: ffmpeg_next::Codec,
    codec_label: &str,
    decoder: &ffmpeg_next::decoder::Audio,
    sample_rate: u32,
    channel_layout: ChannelLayout,
    sample_format: Sample,
    time_base: Rational,
) -> ffmpeg_next::encoder::Audio {
    let ctx = codec::context::Context::new_with_codec(selected_codec);
    let mut encoder = ctx
        .encoder()
        .audio()
        .unwrap_or_else(|e| error!("failed to create audio encoder: {e}"));
    encoder.set_rate(sample_rate as i32);
    encoder.set_channel_layout(channel_layout);
    encoder.set_format(sample_format);
    encoder.set_time_base(time_base);
    if decoder.bit_rate() > 0 {
        encoder.set_bit_rate(decoder.bit_rate());
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

fn preferred_video_frame_rate(
    ist: &ffmpeg_next::format::stream::Stream,
    decoder: &ffmpeg_next::decoder::Video,
) -> Option<Rational> {
    if let Some(rate) = decoder.frame_rate() {
        if rate.numerator() > 0 && rate.denominator() > 0 {
            return Some(rate);
        }
    }
    let rate = ist.avg_frame_rate();
    if rate.numerator() > 0 && rate.denominator() > 0 {
        Some(rate)
    } else {
        None
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
    let decoder_channels = decoder.channels() as i32;
    codec
        .channel_layouts()
        .map(|layouts| layouts.best(decoder_channels))
        .unwrap_or_else(|| decoder_channel_layout(decoder))
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

fn normalize_audio_packet_duration(packet: &mut Packet, src: Rational, dst: Rational) {
    if packet.duration() > 0 {
        packet.set_duration(packet.duration().rescale(src, dst));
    }
}

#[cfg(test)]
mod parser_tests {
    use super::*;

    fn rewrite(spec: &str, inputs: usize) -> Result<RewrittenGraph, String> {
        rewrite_filter_graph_for_inputs(spec, inputs).map_err(|e| e.to_string())
    }

    #[test]
    fn test_filter_complex_label_rewrite_escaped_brackets() {
        let graph = rewrite("[i0:v]drawtext=text='foo [i9:v] [bar]'[vout]", 1).unwrap();
        assert!(graph.graph.contains("[pgffmpeg_i0_v]"));
        assert!(graph.graph.contains("'foo [i9:v] [bar]'"));
        assert_eq!(graph.refs.len(), 1);
    }

    #[test]
    fn test_filter_complex_label_rewrite_nested_graphs() {
        let graph = rewrite(
            "[i0:v]split[a][b];[a]scale=32:32[out1];[b]scale=16:16[vout]",
            1,
        )
        .unwrap();
        assert!(graph.graph.starts_with("[pgffmpeg_i0_v]split"));
        assert_eq!(
            graph.refs,
            vec![InputRef {
                index: 0,
                kind: StreamKind::Video
            }]
        );
    }

    #[test]
    fn test_filter_complex_keeps_intermediate_labels_starting_with_i() {
        let graph = rewrite("[i0:v]split[intro][idtmp];[intro][idtmp]hstack[vout]", 1).unwrap();
        assert!(graph.graph.contains("[intro]"));
        assert!(graph.graph.contains("[idtmp]"));
        assert_eq!(
            graph.refs,
            vec![InputRef {
                index: 0,
                kind: StreamKind::Video
            }]
        );
    }

    #[test]
    fn test_filter_complex_label_out_of_range() {
        let err = rewrite("[i9:v]null[vout]", 2).unwrap_err();
        assert!(err.contains("references i9"));
    }

    #[test]
    fn test_filter_complex_label_negative_index() {
        let err = rewrite("[i-1:v]null[vout]", 2).unwrap_err();
        assert!(err.contains("negative"));
    }

    #[test]
    fn test_filter_complex_label_non_numeric() {
        let err = rewrite("[iX:v]null[vout]", 2).unwrap_err();
        assert!(err.contains("non-numeric"));
    }

    #[test]
    fn test_filter_complex_label_collision_with_internal() {
        let err = rewrite("[pgffmpeg_i0_v]null[vout]", 1).unwrap_err();
        assert!(err.contains("reserved"));
    }

    #[test]
    fn test_filter_complex_empty_filter_graph() {
        let err = rewrite("   ", 1).unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn test_filter_complex_missing_output_labels() {
        let err = rewrite("[i0:v]null[out]", 1).unwrap_err();
        assert!(err.contains("[vout] or [aout]"));
    }

    #[test]
    fn test_filter_complex_both_output_labels() {
        let graph = rewrite("[i0:v]null[vout];[i0:a]anull[aout]", 1).unwrap();
        assert!(graph.has_vout);
        assert!(graph.has_aout);
    }

    #[test]
    fn test_filter_complex_unused_input() {
        let err = rewrite("[i0:v][i1:v]hstack=inputs=2[vout]", 3).unwrap_err();
        assert!(err.contains("inputs[2]"));
    }

    #[test]
    fn test_filter_complex_rejects_movie_filter_before_ffmpeg() {
        let graph = rewrite("movie=/tmp/x[vout]", 0).unwrap();
        let err = filter_safety::validate_filter_spec(&graph.graph).unwrap_err();
        assert!(err.to_string().contains("always denied"));
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::mem_io::MemInput;
    use crate::test_utils::{generate_test_aac_adts_bytes, generate_test_video_bytes};

    #[pg_test]
    fn test_filter_complex_hstack() {
        let left = generate_test_video_bytes(32, 32, 5, 1);
        let right = generate_test_video_bytes(32, 32, 5, 1);
        let result = filter_complex_slices(
            &[left.as_slice(), right.as_slice()],
            "[i0:v][i1:v]hstack=inputs=2[vout]",
            "matroska",
            Some("mpeg2video"),
            None,
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        let stream = probe
            .streams()
            .best(Type::Video)
            .expect("expected video output");
        let decoder_ctx = codec::context::Context::from_parameters(stream.parameters()).unwrap();
        let decoder = decoder_ctx.decoder().video().unwrap();
        assert_eq!(decoder.width(), 64);
        assert_eq!(decoder.height(), 32);
    }

    #[pg_test]
    fn test_filter_complex_amix() {
        let a = generate_test_aac_adts_bytes(1);
        let b = generate_test_aac_adts_bytes(1);
        let result = filter_complex_slices(
            &[a.as_slice(), b.as_slice()],
            "[i0:a][i1:a]amix=inputs=2[aout]",
            "matroska",
            None,
            Some("aac"),
        );
        assert!(!result.is_empty());

        let probe = MemInput::open(&result);
        assert!(probe.streams().best(Type::Audio).is_some());
    }
}
