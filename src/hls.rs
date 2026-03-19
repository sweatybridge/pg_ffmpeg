use pgrx::prelude::*;
use pgrx::PgRelation;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;

use ffmpeg_next::sys::{
    av_free, av_malloc, avformat_alloc_context, avformat_alloc_output_context2,
    avformat_find_stream_info, avformat_open_input, avio_alloc_context, avio_close_dyn_buf,
    avio_context_free, avio_open_dyn_buf, AVDictionary, AVFormatContext, AVIOContext, AVSEEK_SIZE,
};

const AVIO_BUF_SIZE: c_int = 4096;

// --- Input AVIO: read from in-memory buffer ---

struct InputIoState {
    data: Vec<u8>,
    pos: usize,
}

unsafe extern "C" fn input_read(opaque: *mut c_void, buf: *mut u8, buf_size: c_int) -> c_int {
    let state = &mut *(opaque as *mut InputIoState);
    let remaining = state.data.len() - state.pos;
    let to_read = std::cmp::min(remaining, buf_size as usize);
    if to_read == 0 {
        return ffmpeg_next::sys::AVERROR_EOF;
    }
    ptr::copy_nonoverlapping(state.data.as_ptr().add(state.pos), buf, to_read);
    state.pos += to_read;
    to_read as c_int
}

unsafe extern "C" fn input_seek(opaque: *mut c_void, offset: i64, whence: c_int) -> i64 {
    let state = &mut *(opaque as *mut InputIoState);
    if whence == AVSEEK_SIZE {
        return state.data.len() as i64;
    }
    let new_pos = match whence & 0xFF {
        0 => offset,                            // SEEK_SET
        1 => state.pos as i64 + offset,         // SEEK_CUR
        2 => state.data.len() as i64 + offset,  // SEEK_END
        _ => return -1,
    };
    if new_pos < 0 || new_pos > state.data.len() as i64 {
        return -1;
    }
    state.pos = new_pos as usize;
    new_pos
}

// --- Output AVIO: HLS muxer streams segments directly to DB ---

struct HlsIoState {
    /// Playlist ID (pre-allocated before muxing starts).
    playlist_id: i64,
    /// Counter for segment ordering.
    segment_index: i32,
    /// Final m3u8 content (small text, kept for post-mux metadata update).
    m3u8_data: Option<Vec<u8>>,
    /// Currently open AVIO contexts: ptr address -> filename.
    open_contexts: HashMap<usize, String>,
    /// Open relation for hls_segments (held for duration of muxing).
    segments_rel: pg_sys::Relation,
    /// Tuple descriptor for hls_segments.
    segments_tupdesc: pg_sys::TupleDesc,
    /// Reusable tuple table slot (created once, reused for all inserts).
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
    let filename = CStr::from_ptr(url).to_string_lossy().into_owned();

    let ret = avio_open_dyn_buf(pb);
    if ret < 0 {
        return ret;
    }

    state.open_contexts.insert(*pb as usize, filename);
    0
}

/// Called when FFmpeg closes a segment or playlist file.
/// For .ts segments: direct heap INSERT into hls_segments and free data.
/// For .m3u8: keep the small text for post-mux metadata update.
unsafe extern "C" fn hls_io_close2(
    s: *mut AVFormatContext,
    pb: *mut AVIOContext,
) -> c_int {
    let state = &mut *((*s).opaque as *mut HlsIoState);

    let mut buf: *mut u8 = ptr::null_mut();
    let size = avio_close_dyn_buf(pb, &mut buf);

    if let Some(filename) = state.open_contexts.remove(&(pb as usize)) {
        if size > 0 {
            if filename.ends_with(".ts") {
                let idx = state.segment_index;
                state.segment_index += 1;

                // Build bytea varlena directly from FFmpeg's buffer — single copy into palloc'd memory.
                // Then heap_form_tuple copies into the tuple. Total: 2 copies (was 3).
                let bytea_datum = raw_to_bytea_datum(buf, size as usize);

                insert_segment_raw(
                    state.segments_rel,
                    state.segments_tupdesc,
                    state.slot,
                    state.playlist_id,
                    idx,
                    bytea_datum,
                );
            } else if filename.ends_with(".m3u8") {
                // m3u8 is small text — copy is negligible
                state.m3u8_data =
                    Some(std::slice::from_raw_parts(buf, size as usize).to_vec());
            }
        }
    }

    if !buf.is_null() {
        av_free(buf as *mut c_void);
    }
    0
}

/// Build a bytea Datum directly from a raw pointer, avoiding intermediate Vec allocation.
/// Performs a single palloc + copy from src into a properly-headered varlena.
#[inline]
unsafe fn raw_to_bytea_datum(src: *const u8, len: usize) -> pg_sys::Datum {
    let total = len + pg_sys::VARHDRSZ;
    let varlena = pg_sys::palloc(total) as *mut pg_sys::varlena;
    pgrx::set_varsize_4b(varlena, total as i32);
    ptr::copy_nonoverlapping(
        src,
        (varlena as *mut u8).add(pg_sys::VARHDRSZ),
        len,
    );
    pg_sys::Datum::from(varlena)
}

/// Insert a segment row using pre-built datums. Reuses the TupleTableSlot.
/// Stack-allocated arrays (5 columns: id, playlist_id, segment_index, duration, data).
#[inline]
unsafe fn insert_segment_raw(
    rel: pg_sys::Relation,
    tupdesc: pg_sys::TupleDesc,
    slot: *mut pg_sys::TupleTableSlot,
    playlist_id: i64,
    segment_index: i32,
    bytea_datum: pg_sys::Datum,
) {
    // Stack arrays — no heap allocation
    let mut values: [pg_sys::Datum; 5] = [pg_sys::Datum::from(0); 5];
    let mut nulls: [bool; 5] = [true, false, false, true, false];

    // Column 0: id — NULL, serial default
    // Column 1: playlist_id (int8) — always non-null
    values[1] = pg_sys::Datum::from(playlist_id);
    // Column 2: segment_index (int4) — always non-null
    values[2] = pg_sys::Datum::from(segment_index as i64);
    // Column 3: duration — NULL, updated after muxing
    // Column 4: data (bytea) — pre-built varlena datum
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

#[pg_extern(schema = "pg_ffmpeg")]
fn hls(data: Vec<u8>, segment_duration: default!(i32, 6)) -> i64 {
    ffmpeg_next::init().unwrap();

    // Pre-allocate playlist row to get playlist_id
    let playlist_id = Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO pg_ffmpeg.hls_playlists DEFAULT VALUES RETURNING id",
                None,
                &[],
            )
            .unwrap_or_else(|e| error!("failed to insert playlist: {e}"))
            .first()
            .get_one::<i64>()
            .unwrap_or_else(|e| error!("failed to get playlist id: {e}"))
            .unwrap_or_else(|| error!("playlist id was null"))
    });

    // Open hls_segments relation for direct inserts (held during muxing)
    let segments_rel = unsafe {
        let rel = PgRelation::open_with_name("pg_ffmpeg.hls_segments")
            .unwrap_or_else(|_| error!("failed to open hls_segments"));
        // Re-open with RowExclusiveLock for writes
        let oid = rel.oid();
        drop(rel);
        pg_sys::relation_open(oid, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE)
    };
    let segments_tupdesc = unsafe { (*segments_rel).rd_att };
    let segments_slot = unsafe {
        pg_sys::MakeSingleTupleTableSlot(segments_tupdesc, &pg_sys::TTSOpsHeapTuple)
    };

    // Open input from memory via custom AVIO
    let mut input_state = Box::new(InputIoState { data, pos: 0 });
    let mut avio_ctx_ptr: *mut AVIOContext;

    let mut ictx = unsafe {
        let avio_buf = av_malloc(AVIO_BUF_SIZE as usize) as *mut u8;
        if avio_buf.is_null() {
            error!("failed to allocate AVIO buffer");
        }

        avio_ctx_ptr = avio_alloc_context(
            avio_buf,
            AVIO_BUF_SIZE,
            0,
            &mut *input_state as *mut InputIoState as *mut c_void,
            Some(input_read),
            None,
            Some(input_seek),
        );
        if avio_ctx_ptr.is_null() {
            av_free(avio_buf as *mut c_void);
            error!("failed to allocate AVIO context");
        }

        let ps = avformat_alloc_context();
        if ps.is_null() {
            avio_context_free(&mut avio_ctx_ptr);
            error!("failed to allocate format context");
        }
        (*ps).pb = avio_ctx_ptr;

        let ret =
            avformat_open_input(&mut (ps as *mut _), ptr::null(), ptr::null(), ptr::null_mut());
        if ret < 0 {
            avio_context_free(&mut avio_ctx_ptr);
            error!("failed to open input: error code {ret}");
        }

        let ret = avformat_find_stream_info(ps, ptr::null_mut());
        if ret < 0 {
            error!("failed to find stream info: error code {ret}");
        }

        ffmpeg_next::format::context::Input::wrap(ps)
    };

    // Allocate HLS output context with streaming I/O callbacks
    let mut output_state = Box::new(HlsIoState {
        playlist_id,
        segment_index: 0,
        m3u8_data: None,
        open_contexts: HashMap::new(),
        segments_rel,
        segments_tupdesc,
        slot: segments_slot,
    });

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

    // Remux packets — segments are streamed to DB as they complete
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
    unsafe {
        avio_context_free(&mut avio_ctx_ptr);
    }
    drop(input_state);

    // Clean up slot and close relation
    unsafe {
        pg_sys::ExecDropSingleTupleTableSlot(segments_slot);
        pg_sys::relation_close(segments_rel, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
    }

    // Parse m3u8 and update playlist metadata + segment durations via SPI
    let m3u8_bytes = output_state
        .m3u8_data
        .take()
        .unwrap_or_else(|| error!("no m3u8 playlist found in output"));
    let m3u8_content = String::from_utf8_lossy(&m3u8_bytes);
    let playlist_info = parse_m3u8(&m3u8_content);

    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE pg_ffmpeg.hls_playlists SET target_duration = $1, media_sequence = $2 WHERE id = $3",
                None,
                &[
                    pgrx::datum::DatumWithOid::from(playlist_info.target_duration),
                    pgrx::datum::DatumWithOid::from(playlist_info.media_sequence),
                    pgrx::datum::DatumWithOid::from(playlist_id),
                ],
            )
            .unwrap_or_else(|e| error!("failed to update playlist: {e}"));

        for (i, seg) in playlist_info.segments.iter().enumerate() {
            client
                .update(
                    "UPDATE pg_ffmpeg.hls_segments SET duration = $1 WHERE playlist_id = $2 AND segment_index = $3",
                    None,
                    &[
                        pgrx::datum::DatumWithOid::from(seg.duration),
                        pgrx::datum::DatumWithOid::from(playlist_id),
                        pgrx::datum::DatumWithOid::from(i as i32),
                    ],
                )
                .unwrap_or_else(|e| error!("failed to update segment duration {i}: {e}"));
        }
    });

    playlist_id
}
