use pgrx::prelude::*;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;

use ffmpeg_next::sys::{
    av_free, av_malloc, avformat_alloc_output_context2, avio_alloc_context, avio_context_free,
    AVDictionary, AVFormatContext, AVIOContext,
};

const AVIO_BUF_SIZE: c_int = 4096;

/// Custom write callback: appends into a Vec<u8>.
unsafe extern "C" fn vec_write(opaque: *mut c_void, data: *const u8, size: c_int) -> c_int {
    let buf = &mut *(opaque as *mut Vec<u8>);
    buf.extend_from_slice(std::slice::from_raw_parts(data, size as usize));
    size
}

struct CollectedSegment {
    index: i32,
    data: Vec<u8>,
}

struct HlsIoState {
    segment_index: i32,
    /// Buffer for the m3u8 playlist being written.
    m3u8_buf: Vec<u8>,
    m3u8_pb: *mut AVIOContext,
    /// Buffer for the current .ts segment being written.
    seg_buf: Vec<u8>,
    segment_pb: *mut AVIOContext,
    /// Completed segments collected during muxing.
    segments: Vec<CollectedSegment>,
}

unsafe extern "C" fn hls_io_open(
    s: *mut AVFormatContext,
    pb: *mut *mut AVIOContext,
    url: *const c_char,
    _flags: c_int,
    _options: *mut *mut AVDictionary,
) -> c_int {
    let state = &mut *((*s).opaque as *mut HlsIoState);
    let url_bytes = CStr::from_ptr(url).to_bytes();
    let is_ts = url_bytes.ends_with(b".ts");

    let target_buf = if is_ts {
        state.seg_buf.clear();
        &mut state.seg_buf as *mut Vec<u8>
    } else {
        state.m3u8_buf.clear();
        &mut state.m3u8_buf as *mut Vec<u8>
    };

    let avio_buf = av_malloc(AVIO_BUF_SIZE as usize) as *mut u8;
    if avio_buf.is_null() {
        return ffmpeg_next::sys::AVERROR_EOF;
    }

    let ctx = avio_alloc_context(
        avio_buf,
        AVIO_BUF_SIZE,
        1,
        target_buf as *mut c_void,
        None,
        Some(std::mem::transmute(vec_write as *const ())),
        None,
    );
    if ctx.is_null() {
        av_free(avio_buf as *mut c_void);
        return ffmpeg_next::sys::AVERROR_EOF;
    }

    *pb = ctx;
    if is_ts {
        state.segment_pb = ctx;
    } else {
        state.m3u8_pb = ctx;
    }

    0
}

unsafe extern "C" fn hls_io_close2(
    s: *mut AVFormatContext,
    pb: *mut AVIOContext,
) -> c_int {
    let state = &mut *((*s).opaque as *mut HlsIoState);

    if pb == state.segment_pb {
        state.segment_pb = ptr::null_mut();
        if !state.seg_buf.is_empty() {
            let idx = state.segment_index;
            state.segment_index += 1;
            state.segments.push(CollectedSegment {
                index: idx,
                data: std::mem::take(&mut state.seg_buf),
            });
        }
    } else {
        ffmpeg_next::sys::avio_flush(pb);
        state.m3u8_pb = ptr::null_mut();
    }

    let mut pb_mut = pb;
    avio_context_free(&mut pb_mut);
    0
}

// --- m3u8 parsing ---

struct SegmentInfo {
    duration: f64,
}

struct PlaylistInfo {
    target_duration: i32,
    media_sequence: i32,
    segments: Vec<SegmentInfo>,
}

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
                segments.push(SegmentInfo { duration });
            }
        }
    }

    PlaylistInfo {
        target_duration,
        media_sequence,
        segments,
    }
}

// --- Main function ---

#[pg_extern]
fn hls(url: &str, segment_duration: default!(i32, 6)) -> i64 {
    ffmpeg_next::init().unwrap();
    ffmpeg_next::format::network::init();

    // Pre-allocate playlist row to get playlist_id
    let playlist_id = Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO ffmpeg.hls_playlists DEFAULT VALUES RETURNING id",
                None,
                &[],
            )
            .unwrap_or_else(|e| error!("failed to insert playlist: {e}"))
            .first()
            .get_one::<i64>()
            .unwrap_or_else(|e| error!("failed to get playlist id: {e}"))
            .unwrap_or_else(|| error!("playlist id was null"))
    });

    // Open input directly from URL — FFmpeg handles protocol decoding
    let mut ictx = ffmpeg_next::format::input(&url)
        .unwrap_or_else(|e| error!("failed to open input url: {e}"));

    // Allocate HLS output context with streaming I/O callbacks
    let mut output_state = Box::new(HlsIoState {
        segment_index: 0,
        m3u8_buf: Vec::new(),
        m3u8_pb: ptr::null_mut(),
        seg_buf: Vec::new(),
        segment_pb: ptr::null_mut(),
        segments: Vec::new(),
    });

    let mut octx = unsafe {
        let mut ps: *mut AVFormatContext = ptr::null_mut();
        let format = std::ffi::CString::new("hls").unwrap();
        let filename = std::ffi::CString::new("playlist.m3u8").unwrap();

        let ret = avformat_alloc_output_context2(
            &mut ps,
            ptr::null_mut(),
            format.as_ptr(),
            filename.as_ptr(),
        );
        if ret < 0 || ps.is_null() {
            error!("failed to allocate HLS output context");
        }

        (*ps).io_open = Some(hls_io_open);
        (*ps).io_close2 = Some(hls_io_close2);
        (*ps).opaque = &mut *output_state as *mut HlsIoState as *mut c_void;

        ffmpeg_next::format::context::Output::wrap(ps)
    };

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
    opts.set("hls_segment_filename", "seg%03d.ts");
    opts.set("hls_list_size", "0");
    opts.set("hls_playlist_type", "vod");

    octx.write_header_with(opts)
        .unwrap_or_else(|e| error!("failed to write HLS header: {e}"));

    // Remux packets — segments stream to DB as they complete
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

    // Clean up FFmpeg contexts
    drop(octx);
    drop(ictx);

    // Parse m3u8 and update playlist metadata
    let m3u8_content = String::from_utf8_lossy(&output_state.m3u8_buf);
    if m3u8_content.is_empty() {
        error!("no m3u8 playlist found in output");
    }
    let playlist_info = parse_m3u8(&m3u8_content);

    Spi::connect_mut(|client| {
        // Update playlist metadata
        client
            .update(
                "UPDATE ffmpeg.hls_playlists SET target_duration = $1, media_sequence = $2 WHERE id = $3",
                None,
                &[
                    pgrx::datum::DatumWithOid::from(playlist_info.target_duration),
                    pgrx::datum::DatumWithOid::from(playlist_info.media_sequence),
                    pgrx::datum::DatumWithOid::from(playlist_id),
                ],
            )
            .unwrap_or_else(|e| error!("failed to update playlist: {e}"));

        // Insert segments
        for seg in &output_state.segments {
            let duration = playlist_info
                .segments
                .get(seg.index as usize)
                .map(|s| s.duration);
            client
                .update(
                    "INSERT INTO ffmpeg.hls_segments (playlist_id, segment_index, duration, data) \
                     VALUES ($1, $2, $3, $4)",
                    None,
                    &[
                        pgrx::datum::DatumWithOid::from(playlist_id),
                        pgrx::datum::DatumWithOid::from(seg.index),
                        pgrx::datum::DatumWithOid::from(duration),
                        pgrx::datum::DatumWithOid::from(seg.data.clone()),
                    ],
                )
                .unwrap_or_else(|e| error!("failed to insert segment: {e}"));
        }
    });

    playlist_id
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    // --- Unit tests for parse_m3u8 (no PostgreSQL needed) ---

    #[test]
    fn test_parse_m3u8_basic() {
        let m3u8 = "\
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:5.005333,
seg000.ts
#EXTINF:4.838167,
seg001.ts
#EXTINF:2.135467,
seg002.ts
#EXT-X-ENDLIST
";
        let info = parse_m3u8(m3u8);
        assert_eq!(info.target_duration, 6);
        assert_eq!(info.media_sequence, 0);
        assert_eq!(info.segments.len(), 3);
        assert!((info.segments[0].duration - 5.005333).abs() < 1e-6);
        assert!((info.segments[1].duration - 4.838167).abs() < 1e-6);
        assert!((info.segments[2].duration - 2.135467).abs() < 1e-6);
    }

    #[test]
    fn test_parse_m3u8_nonzero_sequence() {
        let m3u8 = "\
#EXTM3U
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:42
#EXTINF:9.9,
seg042.ts
#EXT-X-ENDLIST
";
        let info = parse_m3u8(m3u8);
        assert_eq!(info.target_duration, 10);
        assert_eq!(info.media_sequence, 42);
        assert_eq!(info.segments.len(), 1);
        assert!((info.segments[0].duration - 9.9).abs() < 1e-6);
    }

    #[test]
    fn test_parse_m3u8_empty() {
        let info = parse_m3u8("#EXTM3U\n#EXT-X-ENDLIST\n");
        assert_eq!(info.target_duration, 0);
        assert_eq!(info.media_sequence, 0);
        assert!(info.segments.is_empty());
    }

    #[test]
    fn test_parse_m3u8_trailing_comma_stripped() {
        let m3u8 = "#EXTINF:3.500000,\nseg.ts\n";
        let info = parse_m3u8(m3u8);
        assert_eq!(info.segments.len(), 1);
        assert!((info.segments[0].duration - 3.5).abs() < 1e-6);
    }

    // --- Integration test requiring PostgreSQL ---

    /// Generate a minimal 3-second test video file using the ffmpeg library.
    fn generate_test_video(path: &std::path::Path) {
        use ffmpeg_next::codec;
        use ffmpeg_next::format::Pixel;
        use ffmpeg_next::util::frame::video::Video;

        ffmpeg_next::init().unwrap();

        let width = 64u32;
        let height = 64u32;
        let fps = 10;
        let duration_secs = 3;
        let total_frames = fps * duration_secs;

        let codec = ffmpeg_next::encoder::find(codec::Id::MPEG2VIDEO)
            .expect("MPEG2VIDEO encoder not found");

        let mut octx = ffmpeg_next::format::output_as(path, "mpegts")
            .expect("failed to create output context");

        let mut stream = octx.add_stream(codec).expect("failed to add stream");
        stream.set_time_base((1, fps));

        let ctx = codec::context::Context::new_with_codec(codec);
        let mut encoder = ctx.encoder().video().expect("failed to create encoder");
        encoder.set_width(width);
        encoder.set_height(height);
        encoder.set_format(Pixel::YUV420P);
        encoder.set_bit_rate(400_000);
        encoder.set_gop(10);
        encoder.set_max_b_frames(2);
        encoder.set_frame_rate(Some((fps, 1)));
        encoder.set_time_base((1, fps));

        let mut encoder = encoder.open().expect("failed to open encoder");
        stream.set_parameters(&encoder);
        let out_time_base = stream.time_base();
        drop(stream);

        octx.write_header().expect("failed to write header");

        let mut packet = ffmpeg_next::Packet::empty();

        for i in 0..total_frames {
            let mut frame = Video::new(Pixel::YUV420P, width, height);
            // Fill Y plane with a shifting pattern so frames differ
            let y_data = frame.data_mut(0);
            for (j, byte) in y_data.iter_mut().enumerate() {
                *byte = ((i as usize * 3 + j) % 256) as u8;
            }
            // Fill U and V planes with 128 (neutral chroma)
            for plane in 1..=2 {
                for byte in frame.data_mut(plane).iter_mut() {
                    *byte = 128;
                }
            }
            frame.set_pts(Some(i as i64));

            encoder.send_frame(&frame).expect("failed to send frame");
            while encoder.receive_packet(&mut packet).is_ok() {
                packet.set_stream(0);
                packet.rescale_ts((1, fps), out_time_base);
                packet
                    .write_interleaved(&mut octx)
                    .expect("failed to write packet");
            }
        }

        encoder.send_eof().expect("failed to send eof");
        while encoder.receive_packet(&mut packet).is_ok() {
            packet.set_stream(0);
            packet.rescale_ts((1, fps), out_time_base);
            packet
                .write_interleaved(&mut octx)
                .expect("failed to write packet");
        }

        octx.write_trailer().expect("failed to write trailer");
    }

    #[pg_test]
    fn test_hls_creates_playlist_and_segments() {
        // Generate a short test video
        let tmp = tempfile::Builder::new().suffix(".mp4").tempfile().unwrap();
        let video_path = tmp.path().to_path_buf();
        drop(tmp); // release the fd so ffmpeg can write
        generate_test_video(&video_path);

        let url = format!("file://{}", video_path.display());
        let playlist_id = crate::hls::hls(&url, 2);

        assert!(playlist_id > 0);

        // Verify playlist metadata was set
        let row = Spi::connect(|client| {
            client
                .select(
                    "SELECT target_duration, media_sequence FROM ffmpeg.hls_playlists WHERE id = $1",
                    None,
                    &[pgrx::datum::DatumWithOid::from(playlist_id)],
                )
                .unwrap()
                .first()
                .get_two::<i32, i32>()
                .unwrap()
        });
        let (target_dur, media_seq) = row;
        assert!(target_dur.unwrap() > 0, "target_duration should be set");
        assert_eq!(media_seq.unwrap(), 0);

        // Verify segments were created
        let seg_count = Spi::connect(|client| {
            client
                .select(
                    "SELECT count(*)::int4 FROM ffmpeg.hls_segments WHERE playlist_id = $1",
                    None,
                    &[pgrx::datum::DatumWithOid::from(playlist_id)],
                )
                .unwrap()
                .first()
                .get_one::<i32>()
                .unwrap()
                .unwrap()
        });
        assert!(seg_count > 0, "should have at least one segment");

        // Verify segments have data and sequential indices
        let rows = Spi::connect(|client| {
            client
                .select(
                    "SELECT segment_index, duration, octet_length(data) \
                     FROM ffmpeg.hls_segments WHERE playlist_id = $1 ORDER BY segment_index",
                    None,
                    &[pgrx::datum::DatumWithOid::from(playlist_id)],
                )
                .unwrap()
                .map(|row| {
                    (
                        row.get::<i32>(1).unwrap().unwrap(),
                        row.get::<f64>(2).unwrap(),
                        row.get::<i32>(3).unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>()
        });

        for (i, (seg_idx, duration, data_len)) in rows.iter().enumerate() {
            assert_eq!(*seg_idx, i as i32, "segment_index should be sequential");
            assert!(duration.is_some(), "duration should be set for segment {i}");
            assert!(duration.unwrap() > 0.0, "duration should be positive for segment {i}");
            assert!(*data_len > 0, "segment {i} should have data");
        }

        // Clean up test file
        let _ = std::fs::remove_file(&video_path);
    }

    #[pg_test]
    fn test_hls_custom_segment_duration() {
        let tmp = tempfile::Builder::new().suffix(".mp4").tempfile().unwrap();
        let video_path = tmp.path().to_path_buf();
        drop(tmp);
        generate_test_video(&video_path);

        let url = format!("file://{}", video_path.display());

        // Use 1-second segments on a 3-second video — should produce more segments
        let playlist_id_short = crate::hls::hls(&url, 1);
        let count_short = Spi::connect(|client| {
            client
                .select(
                    "SELECT count(*)::int4 FROM ffmpeg.hls_segments WHERE playlist_id = $1",
                    None,
                    &[pgrx::datum::DatumWithOid::from(playlist_id_short)],
                )
                .unwrap()
                .first()
                .get_one::<i32>()
                .unwrap()
                .unwrap()
        });

        // Use 10-second segments — should produce fewer (likely 1) segment
        let playlist_id_long = crate::hls::hls(&url, 10);
        let count_long = Spi::connect(|client| {
            client
                .select(
                    "SELECT count(*)::int4 FROM ffmpeg.hls_segments WHERE playlist_id = $1",
                    None,
                    &[pgrx::datum::DatumWithOid::from(playlist_id_long)],
                )
                .unwrap()
                .first()
                .get_one::<i32>()
                .unwrap()
                .unwrap()
        });

        assert!(
            count_short >= count_long,
            "shorter segment_duration ({count_short} segs) should produce >= segments than longer ({count_long} segs)"
        );

        let _ = std::fs::remove_file(&video_path);
    }
}
