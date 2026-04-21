use ffmpeg_next::filter;
use ffmpeg_next::format::Pixel;
use ffmpeg_next::media::Type;
use ffmpeg_next::software::scaling::{context::Context as ScaleContext, flag::Flags};
use ffmpeg_next::util::frame::{audio::Audio, video::Video};
use pgrx::prelude::*;

use crate::mem_io::MemInput;
use crate::thumbnail;

#[pg_extern]
fn waveform(
    data: Vec<u8>,
    width: default!(i32, 800),
    height: default!(i32, 200),
    format: default!(String, "'png'"),
    mode: default!(String, "'waveform'"),
) -> Vec<u8> {
    ffmpeg_next::init().unwrap();
    validate_waveform_args(width, height, &format, &mode);

    render_waveform(&data, width as u32, height as u32, &format, &mode)
}

fn validate_waveform_args(width: i32, height: i32, format: &str, mode: &str) {
    if width <= 0 {
        error!("pg_ffmpeg: waveform width must be > 0");
    }
    if height <= 0 {
        error!("pg_ffmpeg: waveform height must be > 0");
    }
    match format {
        "png" | "jpeg" | "jpg" | "ppm" => {}
        _ => error!("pg_ffmpeg: waveform format must be png, jpeg, jpg, or ppm"),
    }
    match mode {
        "waveform" | "spectrum" => {}
        _ => error!("pg_ffmpeg: waveform mode must be waveform or spectrum"),
    }
}

fn render_waveform(data: &[u8], width: u32, height: u32, format: &str, mode: &str) -> Vec<u8> {
    let mut ictx = MemInput::open(data);
    let input = ictx
        .streams()
        .best(Type::Audio)
        .unwrap_or_else(|| error!("no audio stream found"));
    let audio_stream_index = input.index();
    let stream_time_base = input.time_base();

    let context_decoder = ffmpeg_next::codec::context::Context::from_parameters(input.parameters())
        .unwrap_or_else(|e| error!("failed to create audio decoder context: {e}"));
    let mut decoder = context_decoder
        .decoder()
        .audio()
        .unwrap_or_else(|e| error!("failed to open audio decoder: {e}"));
    decoder.set_packet_time_base(stream_time_base);

    let mut graph = build_waveform_graph(&decoder, width, height, mode);
    let mut result_frame = None;

    for (stream, packet) in ictx.packets() {
        if stream.index() != audio_stream_index {
            continue;
        }

        decoder
            .send_packet(&packet)
            .unwrap_or_else(|e| error!("audio decode error: {e}"));
        drain_audio_decoder(&mut decoder, &mut graph, &mut result_frame);
    }

    decoder
        .send_eof()
        .unwrap_or_else(|e| error!("audio decode flush error: {e}"));
    drain_audio_decoder(&mut decoder, &mut graph, &mut result_frame);

    graph
        .get("in")
        .unwrap()
        .source()
        .flush()
        .unwrap_or_else(|e| error!("waveform filter flush error: {e}"));
    drain_waveform_frames(&mut graph, &mut result_frame);

    let frame = result_frame.unwrap_or_else(|| error!("no waveform frame could be generated"));
    let rgb_frame = ensure_rgb24(&frame);
    thumbnail::encode_frame(&rgb_frame, format)
}

fn build_waveform_graph(
    decoder: &ffmpeg_next::decoder::Audio,
    width: u32,
    height: u32,
    mode: &str,
) -> filter::Graph {
    let mut graph = filter::Graph::new();
    let layout_mask = if decoder.channel_layout().bits() != 0 {
        decoder.channel_layout().bits()
    } else {
        ffmpeg_next::ChannelLayout::STEREO.bits()
    };
    let sample_fmt_int = Into::<ffmpeg_next::sys::AVSampleFormat>::into(decoder.format()) as i32;
    let args = format!(
        "time_base={}/{}:sample_rate={}:sample_fmt={}:channel_layout=0x{:x}",
        decoder.time_base().numerator(),
        decoder.time_base().denominator(),
        decoder.rate(),
        sample_fmt_int,
        layout_mask,
    );

    let filter_spec = match mode {
        "waveform" => format!(
            "showwavespic=s={}x{}:colors=white,format=rgb24",
            width, height
        ),
        "spectrum" => format!("showspectrumpic=s={}x{},format=rgb24", width, height),
        _ => unreachable!("mode is validated before building waveform graph"),
    };

    graph
        .add(&filter::find("abuffer").unwrap(), "in", &args)
        .unwrap_or_else(|e| error!("failed to add waveform audio source: {e}"));
    graph
        .add(&filter::find("buffersink").unwrap(), "out", "")
        .unwrap_or_else(|e| error!("failed to add waveform video sink: {e}"));
    graph
        .output("in", 0)
        .unwrap()
        .input("out", 0)
        .unwrap()
        .parse(&filter_spec)
        .unwrap_or_else(|e| error!("failed to parse waveform filter graph: {e}"));
    graph
        .validate()
        .unwrap_or_else(|e| error!("failed to validate waveform filter graph: {e}"));
    graph
}

fn drain_audio_decoder(
    decoder: &mut ffmpeg_next::decoder::Audio,
    graph: &mut filter::Graph,
    result_frame: &mut Option<Video>,
) {
    let mut decoded = Audio::empty();
    while decoder.receive_frame(&mut decoded).is_ok() {
        graph
            .get("in")
            .unwrap()
            .source()
            .add(&decoded)
            .unwrap_or_else(|e| error!("waveform filter source error: {e}"));
        drain_waveform_frames(graph, result_frame);
    }
}

fn drain_waveform_frames(graph: &mut filter::Graph, result_frame: &mut Option<Video>) {
    let mut filtered = Video::empty();
    while graph
        .get("out")
        .unwrap()
        .sink()
        .frame(&mut filtered)
        .is_ok()
    {
        *result_frame = Some(filtered);
        filtered = Video::empty();
    }
}

fn ensure_rgb24(frame: &Video) -> Video {
    if frame.format() == Pixel::RGB24 {
        return frame.clone();
    }

    let mut rgb = Video::empty();
    let mut scaler = ScaleContext::get(
        frame.format(),
        frame.width(),
        frame.height(),
        Pixel::RGB24,
        frame.width(),
        frame.height(),
        Flags::BILINEAR,
    )
    .unwrap_or_else(|e| error!("failed to create waveform RGB converter: {e}"));
    scaler
        .run(frame, &mut rgb)
        .unwrap_or_else(|e| error!("waveform RGB conversion error: {e}"));
    rgb
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use crate::test_utils::generate_test_aac_adts_bytes;

    const PNG_MAGIC: &[u8; 8] = b"\x89PNG\r\n\x1a\n";

    #[pg_test]
    fn test_waveform_png() {
        let data = generate_test_aac_adts_bytes(1);
        let png = waveform(data, 320, 120, "png".to_string(), "waveform".to_string());

        assert!(!png.is_empty(), "waveform image should not be empty");
        assert_eq!(&png[..8], PNG_MAGIC, "waveform output should be a PNG");
    }

    #[pg_test]
    fn test_spectrum_png() {
        let data = generate_test_aac_adts_bytes(1);
        let png = waveform(data, 320, 120, "png".to_string(), "spectrum".to_string());

        assert!(!png.is_empty(), "spectrum image should not be empty");
        assert_eq!(&png[..8], PNG_MAGIC, "spectrum output should be a PNG");
    }
}
