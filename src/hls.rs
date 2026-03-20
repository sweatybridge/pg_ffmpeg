use pgrx::prelude::*;
use pgrx::PgRelation;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;

use ffmpeg_next::sys::{
    av_free, av_malloc, avformat_alloc_output_context2,
    avio_alloc_context, avio_close_dyn_buf,
    avio_context_free, avio_open_dyn_buf, AVDictionary, AVFormatContext, AVIOContext,
};

const AVIO_BUF_SIZE: c_int = 4096;
const SEGMENT_BUF_INITIAL: usize = 256 * 1024; // 256 KB

// --- Output AVIO: HLS muxer streams segments directly to DB ---

/// Growing palloc'd buffer used as write target for segment AVIO.
/// Data starts at offset VARHDRSZ so the buffer is a ready-made varlena when done.
struct SegmentBuf {
    ptr: *mut u8,
    len: usize, // bytes of payload written (not counting VARHDRSZ)
    cap: usize, // total allocated bytes
}

/// Custom write callback: appends directly into a palloc'd varlena buffer.
unsafe extern "C" fn segment_write(
    opaque: *mut c_void,
    data: *const u8,
    size: c_int,
) -> c_int {
    let seg = &mut *(opaque as *mut SegmentBuf);
    let needed = pg_sys::VARHDRSZ + seg.len + size as usize;
    if needed > seg.cap {
        let new_cap = needed.next_power_of_two();
        seg.ptr = pg_sys::repalloc(seg.ptr as *mut c_void, new_cap) as *mut u8;
        seg.cap = new_cap;
    }
    ptr::copy_nonoverlapping(
        data,
        seg.ptr.add(pg_sys::VARHDRSZ + seg.len),
        size as usize,
    );
    seg.len += size as usize;
    size
}

struct HlsIoState {
    playlist_id: i64,
    segment_index: i32,
    m3u8_data: Option<Vec<u8>>,
    /// Reusable segment buffer — reset (len=0) on each segment open, kept across segments.
    seg_buf: SegmentBuf,
    /// AVIO pointer for the currently open .ts segment (null when none open).
    segment_pb: *mut AVIOContext,
    segments_rel: pg_sys::Relation,
    segments_tupdesc: pg_sys::TupleDesc,
    slot: *mut pg_sys::TupleTableSlot,
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

    if is_ts {
        // Reuse the segment buffer — just reset the write position
        state.seg_buf.len = 0;

        let avio_buf = av_malloc(AVIO_BUF_SIZE as usize) as *mut u8;
        if avio_buf.is_null() {
            return ffmpeg_next::sys::AVERROR_EOF; // OOM
        }

        let ctx = avio_alloc_context(
            avio_buf,
            AVIO_BUF_SIZE,
            1, // write mode
            &mut state.seg_buf as *mut SegmentBuf as *mut c_void,
            None,
            // Transmute to match the write_packet signature: older FFmpeg uses
            // `*mut u8`, newer uses `*const u8`. Both are ABI-compatible.
            Some(std::mem::transmute(segment_write as *const ())),
            None,
        );
        if ctx.is_null() {
            av_free(avio_buf as *mut c_void);
            return ffmpeg_next::sys::AVERROR_EOF;
        }

        *pb = ctx;
        state.segment_pb = ctx;
    } else {
        // m3u8 and other small files: use dyn_buf as before
        let ret = avio_open_dyn_buf(pb);
        if ret < 0 {
            return ret;
        }
    }

    0
}

unsafe extern "C" fn hls_io_close2(
    s: *mut AVFormatContext,
    pb: *mut AVIOContext,
) -> c_int {
    let state = &mut *((*s).opaque as *mut HlsIoState);

    if pb == state.segment_pb {
        // .ts segment — data is in the reusable seg_buf
        state.segment_pb = ptr::null_mut();

        if state.seg_buf.len > 0 {
            // Set varlena header — buffer is already the bytea datum
            let total = pg_sys::VARHDRSZ + state.seg_buf.len;
            pgrx::set_varsize_4b(state.seg_buf.ptr as *mut pg_sys::varlena, total as i32);
            let datum = pg_sys::Datum::from(state.seg_buf.ptr as *mut pg_sys::varlena);

            let idx = state.segment_index;
            state.segment_index += 1;

            // heap_form_tuple copies the datum, so seg_buf can be reused
            insert_segment_raw(
                state.segments_rel,
                state.segments_tupdesc,
                state.slot,
                state.playlist_id,
                idx,
                datum,
            );
        }

        // Free the AVIOContext + its internal buffer (seg_buf itself is reused)
        let mut pb_mut = pb;
        avio_context_free(&mut pb_mut);
    } else {
        // .m3u8 playlist
        let mut buf: *mut u8 = ptr::null_mut();
        let size = avio_close_dyn_buf(pb, &mut buf);

        if size > 0 {
            state.m3u8_data =
                Some(std::slice::from_raw_parts(buf, size as usize).to_vec());
        }

        if !buf.is_null() {
            av_free(buf as *mut c_void);
        }
    }

    0
}

/// Insert a segment row. Reuses TupleTableSlot and stack-allocated arrays.
#[inline]
unsafe fn insert_segment_raw(
    rel: pg_sys::Relation,
    tupdesc: pg_sys::TupleDesc,
    slot: *mut pg_sys::TupleTableSlot,
    playlist_id: i64,
    segment_index: i32,
    bytea_datum: pg_sys::Datum,
) {
    let mut values: [pg_sys::Datum; 5] = [pg_sys::Datum::from(0); 5];
    let mut nulls: [bool; 5] = [true, false, false, true, false];

    values[1] = pg_sys::Datum::from(playlist_id);
    values[2] = pg_sys::Datum::from(segment_index as i64);
    values[4] = bytea_datum;

    let tuple = pg_sys::heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());

    pg_sys::ExecClearTuple(slot);
    pg_sys::ExecStoreHeapTuple(tuple, slot, true);
    pg_sys::simple_table_tuple_insert(rel, slot);
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

    // Open hls_segments relation for direct inserts
    let segments_rel = unsafe {
        let rel = PgRelation::open_with_name("ffmpeg.hls_segments")
            .unwrap_or_else(|_| error!("failed to open hls_segments"));
        let oid = rel.oid();
        drop(rel);
        pg_sys::relation_open(oid, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE)
    };
    let segments_tupdesc = unsafe { (*segments_rel).rd_att };
    let segments_slot = unsafe {
        pg_sys::MakeSingleTupleTableSlot(segments_tupdesc, &pg_sys::TTSOpsHeapTuple)
    };

    // Open input directly from URL — FFmpeg handles protocol decoding
    let mut ictx = ffmpeg_next::format::input(&url)
        .unwrap_or_else(|e| error!("failed to open input url: {e}"));

    // Allocate HLS output context with streaming I/O callbacks
    let mut output_state = unsafe {
        Box::new(HlsIoState {
            playlist_id,
            segment_index: 0,
            m3u8_data: None,
            seg_buf: SegmentBuf {
                ptr: pg_sys::palloc(SEGMENT_BUF_INITIAL) as *mut u8,
                len: 0,
                cap: SEGMENT_BUF_INITIAL,
            },
            segment_pb: ptr::null_mut(),
            segments_rel,
            segments_tupdesc,
            slot: segments_slot,
        })
    };

    let mut octx = unsafe {
        let mut ps: *mut AVFormatContext = ptr::null_mut();
        let format = std::ffi::CString::new("hls").unwrap();

        let ret = avformat_alloc_output_context2(
            &mut ps,
            ptr::null_mut(),
            format.as_ptr(),
            ptr::null(),
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

    // Free the reusable segment buffer
    unsafe {
        pg_sys::pfree(output_state.seg_buf.ptr as *mut c_void);
    }

    // Clean up slot and close relation
    unsafe {
        pg_sys::ExecDropSingleTupleTableSlot(segments_slot);
        pg_sys::relation_close(segments_rel, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
    }

    // Parse m3u8 and update playlist metadata + segment durations
    let m3u8_bytes = output_state
        .m3u8_data
        .take()
        .unwrap_or_else(|| error!("no m3u8 playlist found in output"));
    let m3u8_content = String::from_utf8_lossy(&m3u8_bytes);
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

        // Batch update all segment durations in a single query
        if !playlist_info.segments.is_empty() {
            let indices: Vec<Option<i32>> = (0..playlist_info.segments.len() as i32)
                .map(|i| Some(i))
                .collect();
            let durations: Vec<Option<f64>> = playlist_info
                .segments
                .iter()
                .map(|s| Some(s.duration))
                .collect();

            client
                .update(
                    "UPDATE ffmpeg.hls_segments s SET duration = v.duration \
                     FROM unnest($1::int4[], $2::float8[]) AS v(segment_index, duration) \
                     WHERE s.playlist_id = $3 AND s.segment_index = v.segment_index",
                    None,
                    &[
                        pgrx::datum::DatumWithOid::from(indices),
                        pgrx::datum::DatumWithOid::from(durations),
                        pgrx::datum::DatumWithOid::from(playlist_id),
                    ],
                )
                .unwrap_or_else(|e| error!("failed to update segment durations: {e}"));
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

        let codec = ffmpeg_next::encoder::find(codec::Id::MPEG4)
            .expect("MPEG4 encoder not found");

        let mut octx = ffmpeg_next::format::output(path)
            .expect("failed to create output context");

        let mut stream = octx.add_stream(codec).expect("failed to add stream");
        stream.set_time_base((1, fps));

        let ctx = codec::context::Context::new_with_codec(codec);
        let mut encoder = ctx.encoder().video().expect("failed to create encoder");
        encoder.set_width(width);
        encoder.set_height(height);
        encoder.set_format(Pixel::YUV420P);
        encoder.set_gop(12);
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
