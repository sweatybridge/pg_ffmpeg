//! Concatenate compatible in-memory media segments.
//!
//! `concat(bytea[])` streams packets from each input into one output context.
//! `concat_agg(bytea ORDER BY ...)` stores chunks in aggregate state and feeds
//! them to the same muxer during finalization. The aggregate is intentionally
//! not parallel-safe because order is part of the result.

use ffmpeg_next::codec::Id as CodecId;
use ffmpeg_next::media::Type;
use ffmpeg_next::Rational;
use pgrx::prelude::*;
use pgrx::{error, pg_sys, Internal};

use crate::limits;
use crate::mem_io::{MemInput, MemOutput};
use crate::pipeline;

#[pg_extern]
fn concat(inputs: Array<'_, &[u8]>) -> Vec<u8> {
    limits::check_array_size(inputs.len()).unwrap_or_else(|e| error!("{e}"));
    if inputs.is_empty() {
        error!("pg_ffmpeg: concat requires at least one input");
    }

    let mut borrowed = Vec::with_capacity(inputs.len());
    for (input_index, maybe_data) in inputs.iter().enumerate() {
        let data = maybe_data.unwrap_or_else(|| {
            error!("pg_ffmpeg: concat input {} is NULL", input_index + 1);
        });
        limits::check_input_size(data.len()).unwrap_or_else(|e| error!("{e}"));
        borrowed.push(data);
    }

    concat_slices(borrowed)
}

fn concat_slices<'a, I>(inputs: I) -> Vec<u8>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut muxer = None::<ConcatMuxer>;
    for (input_index, data) in inputs.into_iter().enumerate() {
        let mut input = MemInput::open(data);
        match muxer.as_mut() {
            Some(muxer) => muxer.push_input(input_index, &mut input),
            None => {
                let mut new_muxer = ConcatMuxer::new(&input);
                new_muxer.push_input(input_index, &mut input);
                muxer = Some(new_muxer);
            }
        }
    }

    muxer
        .unwrap_or_else(|| error!("pg_ffmpeg: concat requires at least one input"))
        .finish()
}

#[derive(Debug, Default)]
pub(crate) struct ConcatState {
    total_bytes: usize,
    chunks: Vec<Box<[u8]>>,
}

#[derive(AggregateName)]
#[aggregate_name = "concat_agg"]
pub struct ConcatAgg;

#[pg_aggregate]
impl Aggregate<ConcatAgg> for ConcatAgg {
    type State = Internal;
    type Args = Vec<u8>;
    type Finalize = Vec<u8>;

    const PARALLEL: Option<ParallelOption> = Some(ParallelOption::Unsafe);
    const FINALIZE_MODIFY: Option<FinalizeModify> = Some(FinalizeModify::ReadWrite);

    fn state(
        mut current: Self::State,
        chunk: Self::Args,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::State {
        unsafe {
            let state = current.get_or_insert_default::<ConcatState>();
            limits::check_aggregate_state(state.total_bytes, chunk.len())
                .unwrap_or_else(|e| error!("{e}"));
            state.total_bytes = state.total_bytes.saturating_add(chunk.len());
            state.chunks.push(chunk.into_boxed_slice());
        }
        current
    }

    fn finalize(
        mut current: Self::State,
        _direct_args: Self::OrderedSetArgs,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::Finalize {
        let Some(state) = (unsafe { current.get_mut::<ConcatState>() }) else {
            error!("pg_ffmpeg: concat_agg requires at least one input");
        };
        if state.chunks.is_empty() {
            error!("pg_ffmpeg: concat_agg requires at least one input");
        }

        let chunks = std::mem::take(&mut state.chunks);
        state.total_bytes = 0;
        concat_chunks(chunks)
    }
}

fn concat_chunks(chunks: Vec<Box<[u8]>>) -> Vec<u8> {
    let mut muxer = None::<ConcatMuxer>;
    for (input_index, chunk) in chunks.into_iter().enumerate() {
        let mut input = MemInput::open(&chunk);
        match muxer.as_mut() {
            Some(muxer) => muxer.push_input(input_index, &mut input),
            None => {
                let mut new_muxer = ConcatMuxer::new(&input);
                new_muxer.push_input(input_index, &mut input);
                muxer = Some(new_muxer);
            }
        }
        drop(input);
        drop(chunk);
    }

    muxer
        .unwrap_or_else(|| error!("pg_ffmpeg: concat_agg requires at least one input"))
        .finish()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamSignature {
    medium: Type,
    codec_id: CodecId,
    width: i32,
    height: i32,
    sample_rate: i32,
    channels: i32,
    sample_format: i32,
}

impl StreamSignature {
    fn from_stream(stream: &ffmpeg_next::format::stream::Stream<'_>) -> Self {
        let parameters = stream.parameters();
        unsafe {
            let raw = parameters.as_ptr();
            Self {
                medium: parameters.medium(),
                codec_id: parameters.id(),
                width: (*raw).width,
                height: (*raw).height,
                sample_rate: (*raw).sample_rate,
                channels: (*raw).ch_layout.nb_channels,
                sample_format: (*raw).format,
            }
        }
    }
}

struct ConcatMuxer {
    signature: Vec<StreamSignature>,
    stream_mapping: Vec<usize>,
    output_time_bases: Vec<Rational>,
    next_offsets: Vec<i64>,
    octx: MemOutput,
}

impl ConcatMuxer {
    fn new(first_input: &MemInput<'_>) -> Self {
        let signature = stream_signatures(first_input, 0);
        if signature.is_empty() {
            error!("pg_ffmpeg: concat input 1 has no streams");
        }

        let out_format = default_output_format(first_input.format().name());
        let mut octx = MemOutput::open(&out_format);
        let mut stream_mapping = vec![usize::MAX; first_input.streams().count()];
        for stream in first_input.streams() {
            let output_index = pipeline::copy_stream(&stream, &mut octx);
            stream_mapping[stream.index()] = output_index;
        }

        octx.write_header()
            .unwrap_or_else(|e| error!("failed to write concat header: {e}"));

        let output_time_bases = (0..octx.streams().count())
            .map(|index| octx.stream(index).unwrap().time_base())
            .collect::<Vec<_>>();
        let next_offsets = vec![0; output_time_bases.len()];

        Self {
            signature,
            stream_mapping,
            output_time_bases,
            next_offsets,
            octx,
        }
    }

    fn push_input(&mut self, input_index: usize, input: &mut MemInput<'_>) {
        verify_compatible(input_index, &self.signature, input);

        let mut segment_starts = vec![None::<i64>; input.streams().count()];
        let mut segment_next_offsets = self.next_offsets.clone();

        for (stream, mut packet) in input.packets() {
            let input_stream_index = stream.index();
            let Some(&output_stream_index) = self.stream_mapping.get(input_stream_index) else {
                error!(
                    "pg_ffmpeg: concat input {} stream {} has no output mapping",
                    input_index + 1,
                    input_stream_index
                );
            };
            if output_stream_index == usize::MAX {
                error!(
                    "pg_ffmpeg: concat input {} stream {} has no output mapping",
                    input_index + 1,
                    input_stream_index
                );
            }

            let output_time_base = self.output_time_bases[output_stream_index];
            packet.rescale_ts(stream.time_base(), output_time_base);

            let first_ts = packet.dts().or_else(|| packet.pts()).unwrap_or(0);
            let segment_start = segment_starts[input_stream_index].get_or_insert(first_ts);
            let offset = self.next_offsets[output_stream_index];

            let adjusted_pts = packet.pts().map(|pts| pts - *segment_start + offset);
            let adjusted_dts = packet.dts().map(|dts| dts - *segment_start + offset);
            packet.set_pts(adjusted_pts);
            packet.set_dts(adjusted_dts);

            let packet_end = adjusted_pts
                .into_iter()
                .chain(adjusted_dts)
                .map(|ts| ts.saturating_add(packet.duration().max(1)))
                .max()
                .unwrap_or(offset);
            segment_next_offsets[output_stream_index] =
                segment_next_offsets[output_stream_index].max(packet_end);

            packet.set_stream(output_stream_index);
            packet.set_position(-1);
            packet
                .write_interleaved(&mut self.octx)
                .unwrap_or_else(|e| error!("failed to write concat packet: {e}"));
        }

        self.next_offsets = segment_next_offsets;
    }

    fn finish(mut self) -> Vec<u8> {
        self.octx
            .write_trailer()
            .unwrap_or_else(|e| error!("failed to write concat trailer: {e}"));
        self.octx.into_data()
    }
}

fn stream_signatures(input: &MemInput<'_>, input_index: usize) -> Vec<StreamSignature> {
    let count = input.streams().count();
    let mut signatures = vec![None::<StreamSignature>; count];
    for stream in input.streams() {
        if stream.index() >= count {
            error!(
                "pg_ffmpeg: concat input {} has non-contiguous stream index {}",
                input_index + 1,
                stream.index()
            );
        }
        signatures[stream.index()] = Some(StreamSignature::from_stream(&stream));
    }
    signatures
        .into_iter()
        .enumerate()
        .map(|(stream_index, signature)| {
            signature.unwrap_or_else(|| {
                error!(
                    "pg_ffmpeg: concat input {} is missing stream {}",
                    input_index + 1,
                    stream_index
                )
            })
        })
        .collect()
}

fn verify_compatible(input_index: usize, expected: &[StreamSignature], input: &MemInput<'_>) {
    let actual = stream_signatures(input, input_index);
    if actual.len() != expected.len() {
        error!(
            "pg_ffmpeg: concat input {} has {} streams, expected {} from input 1",
            input_index + 1,
            actual.len(),
            expected.len()
        );
    }

    for (stream_index, (expected, actual)) in expected.iter().zip(actual.iter()).enumerate() {
        if actual.medium != expected.medium {
            error!(
                "pg_ffmpeg: concat input {} stream {} type {:?} does not match input 1 type {:?}",
                input_index + 1,
                stream_index,
                actual.medium,
                expected.medium
            );
        }
        if actual.codec_id != expected.codec_id {
            error!(
                "pg_ffmpeg: concat input {} stream {} codec {:?} does not match input 1 codec {:?}",
                input_index + 1,
                stream_index,
                actual.codec_id,
                expected.codec_id
            );
        }

        match actual.medium {
            Type::Video => {
                if actual.width != expected.width || actual.height != expected.height {
                    error!(
                        "pg_ffmpeg: concat input {} stream {} dimensions {}x{} do not match input 1 dimensions {}x{}",
                        input_index + 1,
                        stream_index,
                        actual.width,
                        actual.height,
                        expected.width,
                        expected.height
                    );
                }
            }
            Type::Audio => {
                if actual.sample_rate != expected.sample_rate {
                    error!(
                        "pg_ffmpeg: concat input {} stream {} sample_rate {} does not match input 1 sample_rate {}",
                        input_index + 1,
                        stream_index,
                        actual.sample_rate,
                        expected.sample_rate
                    );
                }
                if actual.channels != expected.channels {
                    error!(
                        "pg_ffmpeg: concat input {} stream {} channels {} do not match input 1 channels {}",
                        input_index + 1,
                        stream_index,
                        actual.channels,
                        expected.channels
                    );
                }
                if actual.sample_format != expected.sample_format {
                    error!(
                        "pg_ffmpeg: concat input {} stream {} sample format {} does not match input 1 sample format {}",
                        input_index + 1,
                        stream_index,
                        actual.sample_format,
                        expected.sample_format
                    );
                }
            }
            _ => {}
        }
    }
}

fn default_output_format(input_format: &str) -> String {
    match input_format {
        "png_pipe" | "ppm_pipe" => "image2pipe".to_owned(),
        "matroska,webm" => "matroska".to_owned(),
        "mov,mp4,m4a,3gp,3g2,mj2" => "mp4".to_owned(),
        _ => input_format.to_owned(),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::generate_test_video_bytes;

    #[pg_test]
    fn test_concat_two_videos() {
        let first = generate_test_video_bytes(64, 64, 10, 1);
        let second = generate_test_video_bytes(64, 64, 10, 1);
        let result = concat_slices([first.as_slice(), second.as_slice()]);
        assert!(!result.is_empty());
        assert!(decoded_video_frame_count(&result) >= 2);
    }

    #[pg_test]
    #[should_panic(expected = "dimensions")]
    fn test_concat_incompatible_formats_errors() {
        let first = generate_test_video_bytes(64, 64, 10, 1);
        let second = generate_test_video_bytes(96, 64, 10, 1);
        let _ = concat_slices([first.as_slice(), second.as_slice()]);
    }

    #[pg_test]
    fn test_concat_agg_with_order_by() {
        let first = generate_test_video_bytes(64, 64, 10, 1);
        let second = generate_test_video_bytes(64, 64, 10, 1);
        let mut state = Internal::default();
        state = ConcatAgg::state(state, first, std::ptr::null_mut());
        state = ConcatAgg::state(state, second, std::ptr::null_mut());
        let result = ConcatAgg::finalize(state, (), std::ptr::null_mut());
        assert!(!result.is_empty());
        assert!(decoded_video_frame_count(&result) >= 2);
    }

    #[pg_test]
    #[should_panic(expected = "max_aggregate_state_bytes")]
    fn test_concat_agg_exceeds_state_limit_errors() {
        Spi::run("SET LOCAL pg_ffmpeg.max_aggregate_state_bytes = 4").unwrap();
        let state = Internal::default();
        let _ = ConcatAgg::state(state, vec![1, 2, 3, 4, 5], std::ptr::null_mut());
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
