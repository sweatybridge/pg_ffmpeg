use pgrx::prelude::*;
use std::fs;

use crate::write_to_tempfile;

#[pg_extern(schema = "ffmpeg")]
fn extract_audio(data: Vec<u8>, format: default!(&str, "'mp3'")) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    let suffix = format!(".{format}");
    let input_tmp = write_to_tempfile(&data, ".input")
        .unwrap_or_else(|e| error!("failed to write input temp file: {e}"));

    let output_tmp = tempfile::Builder::new()
        .suffix(&suffix)
        .tempfile()
        .unwrap_or_else(|e| error!("failed to create output temp file: {e}"));
    let output_path = output_tmp.path().to_path_buf();
    drop(output_tmp);

    let mut ictx = ffmpeg_next::format::input(input_tmp.path())
        .unwrap_or_else(|e| error!("failed to open input: {e}"));

    let audio_stream = ictx
        .streams()
        .best(ffmpeg_next::media::Type::Audio)
        .unwrap_or_else(|| error!("no audio stream found"));
    let audio_stream_index = audio_stream.index();
    let audio_params = audio_stream.parameters();

    let mut octx = ffmpeg_next::format::output_as(&output_path, format)
        .unwrap_or_else(|e| error!("failed to create output context for format '{format}': {e}"));

    let mut new_stream = octx
        .add_stream(ffmpeg_next::codec::Id::None)
        .unwrap_or_else(|e| error!("failed to add output stream: {e}"));
    new_stream.set_parameters(audio_params);

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write output header: {e}"));

    for (stream, mut packet) in ictx.packets() {
        if stream.index() == audio_stream_index {
            let in_tb = stream.time_base();
            let out_tb = octx.stream(0).unwrap().time_base();
            packet.set_stream(0);
            packet.rescale_ts(in_tb, out_tb);
            packet.set_position(-1);
            packet
                .write_interleaved(&mut octx)
                .unwrap_or_else(|e| error!("failed to write packet: {e}"));
        }
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));

    let result = fs::read(&output_path)
        .unwrap_or_else(|e| error!("failed to read output file: {e}"));

    let _ = fs::remove_file(&output_path);
    result
}
