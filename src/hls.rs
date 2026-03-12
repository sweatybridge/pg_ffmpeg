use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use std::fs;

use crate::write_to_tempfile;

/// Parsed segment info from m3u8 playlist.
struct SegmentInfo {
    filename: String,
    duration: f64,
}

/// Parsed m3u8 playlist metadata.
struct PlaylistInfo {
    target_duration: i32,
    media_sequence: i32,
    segments: Vec<SegmentInfo>,
}

/// Parse an m3u8 playlist file into structured data.
fn parse_m3u8(content: &str) -> PlaylistInfo {
    let mut target_duration = 0i32;
    let mut media_sequence = 0i32;
    let mut segments = Vec::new();
    let mut pending_duration: Option<f64> = None;

    for line in content.lines() {
        let line = line.trim();
        if let Some(val) = line.strip_prefix("#EXT-X-TARGETDURATION:") {
            target_duration = val.parse().unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
            media_sequence = val.parse().unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("#EXTINF:") {
            let dur_str = val.trim_end_matches(',');
            pending_duration = dur_str.parse().ok();
        } else if !line.starts_with('#') && !line.is_empty() {
            if let Some(duration) = pending_duration.take() {
                segments.push(SegmentInfo {
                    filename: line.to_string(),
                    duration,
                });
            }
        }
    }

    PlaylistInfo {
        target_duration,
        media_sequence,
        segments,
    }
}

#[pg_extern(schema = "pg_ffmpeg")]
fn hls(data: Vec<u8>, segment_duration: default!(i32, 6)) -> i64 {
    ffmpeg_next::init().unwrap();

    let input_tmp = write_to_tempfile(&data, ".input")
        .unwrap_or_else(|e| error!("failed to write input temp file: {e}"));

    let tmp_dir = tempfile::TempDir::new()
        .unwrap_or_else(|e| error!("failed to create temp directory: {e}"));

    let playlist_path = tmp_dir.path().join("playlist.m3u8");
    let seg_pattern = tmp_dir
        .path()
        .join("seg%03d.ts")
        .to_string_lossy()
        .to_string();

    let mut ictx = ffmpeg_next::format::input(input_tmp.path())
        .unwrap_or_else(|e| error!("failed to open input: {e}"));

    let mut octx = ffmpeg_next::format::output_as(&playlist_path, "hls")
        .unwrap_or_else(|e| error!("failed to create HLS output context: {e}"));

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
            new_stream.set_parameters(input_stream.parameters());
            stream_mapping.push(Some(output_index));
            output_index += 1;
        } else {
            stream_mapping.push(None);
        }
    }

    // Configure HLS options
    let mut opts = ffmpeg_next::Dictionary::new();
    opts.set("hls_time", &segment_duration.to_string());
    opts.set("hls_segment_filename", &seg_pattern);
    opts.set("hls_list_size", "0");

    octx.write_header_with(opts)
        .unwrap_or_else(|e| error!("failed to write HLS header: {e}"));

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

    // Parse the m3u8 playlist
    let m3u8_content = fs::read_to_string(&playlist_path)
        .unwrap_or_else(|e| error!("failed to read m3u8 playlist: {e}"));
    let playlist_info = parse_m3u8(&m3u8_content);

    // Read segment files
    let mut segment_data: Vec<(f64, Vec<u8>)> = Vec::new();
    for seg in &playlist_info.segments {
        let seg_path = tmp_dir.path().join(&seg.filename);
        let data = fs::read(&seg_path)
            .unwrap_or_else(|e| error!("failed to read segment {}: {e}", seg.filename));
        segment_data.push((seg.duration, data));
    }

    // Insert into database via SPI
    Spi::connect_mut(|client| {
        let playlist_id = client
            .update(
                "INSERT INTO pg_ffmpeg.hls_playlists (target_duration, media_sequence) VALUES ($1, $2) RETURNING id",
                None,
                &[
                    DatumWithOid::from(playlist_info.target_duration),
                    DatumWithOid::from(playlist_info.media_sequence),
                ],
            )
            .unwrap_or_else(|e| error!("failed to insert playlist: {e}"))
            .first()
            .get_one::<i64>()
            .unwrap_or_else(|e| error!("failed to get playlist id: {e}"))
            .unwrap_or_else(|| error!("playlist id was null"));

        for (i, (duration, data)) in segment_data.into_iter().enumerate() {
            client
                .update(
                    "INSERT INTO pg_ffmpeg.hls_segments (playlist_id, segment_index, duration, data) VALUES ($1, $2, $3, $4)",
                    None,
                    &[
                        DatumWithOid::from(playlist_id),
                        DatumWithOid::from(i as i32),
                        DatumWithOid::from(duration),
                        DatumWithOid::from(data),
                    ],
                )
                .unwrap_or_else(|e| error!("failed to insert segment {i}: {e}"));
        }

        playlist_id
    })
}
