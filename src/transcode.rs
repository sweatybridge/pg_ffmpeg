use pgrx::prelude::*;
use std::fs;

use crate::write_to_tempfile;

#[pg_extern(schema = "pg_ffmpeg")]
fn transcode(data: Vec<u8>, format: &str) -> Vec<u8> {
    ffmpeg_next::init().unwrap();

    let suffix = format!(".{format}");
    let input_tmp = write_to_tempfile(&data, ".input")
        .unwrap_or_else(|e| error!("failed to write input temp file: {e}"));

    let output_tmp = tempfile::Builder::new()
        .suffix(&suffix)
        .tempfile()
        .unwrap_or_else(|e| error!("failed to create output temp file: {e}"));
    let output_path = output_tmp.path().to_path_buf();
    // Close the output file so ffmpeg can write to it
    drop(output_tmp);

    let mut ictx = ffmpeg_next::format::input(input_tmp.path())
        .unwrap_or_else(|e| error!("failed to open input: {e}"));

    let mut octx = ffmpeg_next::format::output_as(&output_path, format)
        .unwrap_or_else(|e| error!("failed to create output context for format '{format}': {e}"));

    // Copy all streams (remux without re-encoding)
    let mut stream_mapping = vec![];
    let mut output_index = 0usize;
    for input_stream in ictx.streams() {
        let medium = input_stream.parameters().medium();
        if medium == ffmpeg_next::media::Type::Video
            || medium == ffmpeg_next::media::Type::Audio
            || medium == ffmpeg_next::media::Type::Subtitle
        {
            let mut new_stream = octx
                .add_stream(ffmpeg_next::codec::Id::None)
                .unwrap_or_else(|e| error!("failed to add output stream: {e}"));
            new_stream
                .set_parameters(input_stream.parameters());
            stream_mapping.push(Some(output_index));
            output_index += 1;
        } else {
            stream_mapping.push(None);
        }
    }

    octx.write_header()
        .unwrap_or_else(|e| error!("failed to write output header: {e}"));

    for (stream, mut packet) in ictx.packets() {
        let input_index = stream.index();
        if let Some(Some(out_idx)) = stream_mapping.get(input_index) {
            let in_tb = stream.time_base();
            let out_tb = octx.stream(*out_idx).unwrap().time_base();
            packet.set_stream(*out_idx);
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
