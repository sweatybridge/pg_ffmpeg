use pgrx::prelude::*;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

use ffmpeg_next::sys::{
    av_free, av_malloc, avformat_alloc_output_context2, avio_alloc_context, avio_context_free,
    AVDictionary, AVFormatContext, AVIOContext,
};

/// PostgreSQL's TOAST_MAX_CHUNK_SIZE for standard 8KB BLCKSZ with 64-bit alignment:
/// BLCKSZ/4 - MAXALIGN(SizeofHeapTupleHeader + 3*ItemIdData) - sizeof(int32) - MAXALIGN(sizeof(varattrib_4b))
/// = 2048 - 40 - 4 - 8 = 1996
const TOAST_CHUNK_SIZE: usize = 1996;

/// Custom write callback: appends into a Vec<u8> (used for the m3u8 playlist).
unsafe extern "C" fn vec_write(opaque: *mut c_void, data: *const u8, size: c_int) -> c_int {
    let buf = &mut *(opaque as *mut Vec<u8>);
    buf.extend_from_slice(std::slice::from_raw_parts(data, size as usize));
    size
}

/// Custom write callback: streams bytes directly into TOAST chunks.
unsafe extern "C" fn toast_write(opaque: *mut c_void, data: *const u8, size: c_int) -> c_int {
    let writer = &mut *(opaque as *mut ToastWriter);
    writer.write_data(std::slice::from_raw_parts(data, size as usize));
    size
}

/// Streams bytes into TOAST chunks without buffering the full segment.
/// Chunks are staged as TupleTableSlots during write_data() and
/// batch-inserted via heap_multi_insert in end_segment().
struct ToastWriter {
    toast_rel: pg_sys::Relation,
    toast_idx: pg_sys::Relation,
    toast_idx_info: *mut pg_sys::IndexInfo,
    toast_rel_oid: pg_sys::Oid,
    // Per-segment state, reset by begin_segment().
    value_id: pg_sys::Oid,
    seq: i32,
    total_bytes: usize,
    // Staged slots accumulated for heap_multi_insert in end_segment().
    slot_ptrs: Vec<*mut pg_sys::TupleTableSlot>,
    all_values: Vec<[pg_sys::Datum; 3]>,
    all_nulls: Vec<[bool; 3]>,
}

impl ToastWriter {
    unsafe fn begin_segment(&mut self) {
        self.value_id = pg_sys::GetNewOidWithIndex(self.toast_rel, (*self.toast_idx).rd_id, 1);
        self.seq = 0;
        self.total_bytes = 0;
        self.slot_ptrs.clear();
        self.all_values.clear();
        self.all_nulls.clear();
    }

    unsafe fn write_data(&mut self, data: &[u8]) {
        self.total_bytes += data.len();
        if data.is_empty() {
            return;
        }
        let chunk_len = data.len();
        let varlena_size = pg_sys::VARHDRSZ + chunk_len;
        let cbuf = pg_sys::palloc(varlena_size) as *mut u8;
        *(cbuf as *mut u32) = (varlena_size as u32) << 2; // SET_VARSIZE_4B
        ptr::copy_nonoverlapping(data.as_ptr(), cbuf.add(pg_sys::VARHDRSZ), chunk_len);
        let value_id_u32: u32 = std::mem::transmute(self.value_id);
        self.all_values.push([
            pg_sys::Datum::from(value_id_u32 as usize),
            pg_sys::Datum::from(self.seq as usize),
            pg_sys::Datum::from(cbuf as usize),
        ]);
        self.all_nulls.push([false; 3]);
        let vals = self.all_values.last_mut().unwrap();
        let nulls = self.all_nulls.last_mut().unwrap();
        let tupdesc = (*self.toast_rel).rd_att;
        let tuple = pg_sys::heap_form_tuple(tupdesc, vals.as_mut_ptr(), nulls.as_mut_ptr());
        (*tuple).t_tableOid = (*self.toast_rel).rd_id;
        let slot = pg_sys::MakeSingleTupleTableSlot(tupdesc, &pg_sys::TTSOpsHeapTuple);
        pg_sys::ExecStoreHeapTuple(tuple, slot, true); // slot owns tuple
        self.slot_ptrs.push(slot);
        self.seq += 1;
    }

    // Flush all staged chunks via heap_multi_insert, then update the TOAST index per slot.
    unsafe fn end_segment(&mut self) -> (pg_sys::Oid, usize) {
        if !self.slot_ptrs.is_empty() {
            let bistate = pg_sys::GetBulkInsertState();
            pg_sys::heap_multi_insert(
                self.toast_rel,
                self.slot_ptrs.as_mut_ptr(),
                self.slot_ptrs.len() as c_int,
                pg_sys::GetCurrentCommandId(true),
                0,
                bistate,
            );
            pg_sys::FreeBulkInsertState(bistate);

            for (i, &slot) in self.slot_ptrs.iter().enumerate() {
                pg_sys::index_insert(
                    self.toast_idx,
                    self.all_values[i].as_mut_ptr(),
                    self.all_nulls[i].as_mut_ptr(),
                    &mut (*slot).tts_tid,
                    self.toast_rel,
                    pg_sys::IndexUniqueCheck::UNIQUE_CHECK_YES,
                    false,
                    self.toast_idx_info,
                );
                pg_sys::ExecDropSingleTupleTableSlot(slot);
            }
        }

        (self.value_id, self.total_bytes)
    }
}

struct CompletedToastSegment {
    index: i32,
    value_id: pg_sys::Oid,
    total_bytes: usize,
}

struct HlsIoState {
    segment_index: i32,
    /// Buffer for the m3u8 playlist being written.
    m3u8_buf: Vec<u8>,
    m3u8_pb: *mut AVIOContext,
    segment_pb: *mut AVIOContext,
    toast_writer: ToastWriter,
    /// Completed segments (TOAST metadata only, no buffered data).
    completed: Vec<CompletedToastSegment>,
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

    let avio_buf = av_malloc(TOAST_CHUNK_SIZE as c_int as usize) as *mut u8;
    if avio_buf.is_null() {
        return ffmpeg_next::sys::AVERROR_EOF;
    }

    let ctx = if is_ts {
        state.toast_writer.begin_segment();
        avio_alloc_context(
            avio_buf,
            TOAST_CHUNK_SIZE as c_int,
            1,
            &mut state.toast_writer as *mut ToastWriter as *mut c_void,
            None,
            Some(std::mem::transmute(toast_write as *const ())),
            None,
        )
    } else {
        state.m3u8_buf.clear();
        avio_alloc_context(
            avio_buf,
            TOAST_CHUNK_SIZE as c_int,
            1,
            &mut state.m3u8_buf as *mut Vec<u8> as *mut c_void,
            None,
            Some(std::mem::transmute(vec_write as *const ())),
            None,
        )
    };

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
        let idx = state.segment_index;
        state.segment_index += 1;
        let (value_id, total_bytes) = state.toast_writer.end_segment();
        if total_bytes > 0 {
            state.completed.push(CompletedToastSegment { index: idx, value_id, total_bytes });
        }
    } else {
        ffmpeg_next::sys::avio_flush(pb);
        state.m3u8_pb = ptr::null_mut();
    }

    let mut pb_mut = pb;
    avio_context_free(&mut pb_mut);
    0
}


/// Build an ondisk TOAST pointer in palloc'd memory. Returns a Datum.
unsafe fn build_toast_pointer(
    value_id: pg_sys::Oid,
    toast_rel_oid: pg_sys::Oid,
    rawsize: usize,
) -> pg_sys::Datum {
    let ptr_size = 2 + std::mem::size_of::<pg_sys::varatt_external>();
    let buf = pg_sys::palloc(ptr_size) as *mut u8;
    *buf = 0x01; // 1-byte external varlena header
    *buf.add(1) = pg_sys::vartag_external::VARTAG_ONDISK as u8;
    ptr::write_unaligned(
        buf.add(2) as *mut pg_sys::varatt_external,
        pg_sys::varatt_external {
            va_rawsize: (rawsize + pg_sys::VARHDRSZ) as i32,
            va_extinfo: rawsize as u32,
            va_valueid: value_id,
            va_toastrelid: toast_rel_oid,
        },
    );
    pg_sys::Datum::from(buf as usize)
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

    // Open the TOAST relation before muxing so ToastWriter can write chunks during io_close2.
    let (toast_rel, toast_idx, toast_idx_info, toast_rel_oid) = unsafe {
        let seg_oid = {
            use pgrx::PgRelation;
            let rel = PgRelation::open_with_name("ffmpeg.hls_segments")
                .unwrap_or_else(|_| error!("failed to open hls_segments"));
            rel.oid()
        };
        let srel = pg_sys::relation_open(seg_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let toast_oid = (*(*srel).rd_rel).reltoastrelid;
        pg_sys::relation_close(srel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        if toast_oid == pg_sys::Oid::INVALID {
            error!("hls_segments has no TOAST table");
        }

        let trel = pg_sys::relation_open(toast_oid, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
        let idx_list = pg_sys::RelationGetIndexList(trel);
        if idx_list.is_null() || (*idx_list).length == 0 {
            error!("TOAST relation has no index");
        }
        let tidx_oid = (*(*idx_list).elements).oid_value;
        let tidx = pg_sys::index_open(tidx_oid, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
        let tidx_info = pg_sys::BuildIndexInfo(tidx);
        (trel, tidx, tidx_info, toast_oid)
    };

    // Allocate HLS output context with streaming I/O callbacks
    let mut output_state = Box::new(HlsIoState {
        segment_index: 0,
        m3u8_buf: Vec::new(),
        m3u8_pb: ptr::null_mut(),
        segment_pb: ptr::null_mut(),
        toast_writer: ToastWriter {
            toast_rel,
            toast_idx,
            toast_idx_info,
            toast_rel_oid,
            value_id: pg_sys::Oid::INVALID,
            seq: 0,
            total_bytes: 0,

            slot_ptrs: Vec::new(),
            all_values: Vec::new(),
            all_nulls: Vec::new(),
        },
        completed: Vec::new(),
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

    // Remux packets — TOAST chunks are written directly during hls_io_close2
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

    // Prepare INSERT statement for segments — TOAST chunks already written during muxing,
    // we just need to record the TOAST pointer and duration metadata.
    let insert_sql = CString::new(
        "INSERT INTO ffmpeg.hls_segments (playlist_id, segment_index, duration, data) \
         VALUES ($1, $2, $3, $4)",
    )
    .unwrap();
    let mut insert_argtypes = [
        pg_sys::INT8OID,
        pg_sys::INT4OID,
        pg_sys::FLOAT8OID,
        pg_sys::BYTEAOID,
    ];

    let toast_rel_oid = output_state.toast_writer.toast_rel_oid;

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

        // Insert each segment row using the pre-built TOAST pointer.
        for seg in &output_state.completed {
            let duration = playlist_info.segments.get(seg.index as usize).map(|s| s.duration);
            let toast_datum = unsafe { build_toast_pointer(seg.value_id, toast_rel_oid, seg.total_bytes) };

            let dur_val = duration.unwrap_or(0.0_f64);
            let mut values = [
                pg_sys::Datum::from(playlist_id as usize),
                pg_sys::Datum::from(seg.index as usize),
                pg_sys::Datum::from(dur_val.to_bits() as usize),
                toast_datum,
            ];
            // nulls: ' ' = non-null, 'n' = null
            let nulls: [i8; 5] = [
                b' ' as i8,
                b' ' as i8,
                if duration.is_some() { b' ' as i8 } else { b'n' as i8 },
                b' ' as i8,
                0i8,
            ];

            unsafe {
                let ret = pg_sys::SPI_execute_with_args(
                    insert_sql.as_ptr(),
                    4,
                    insert_argtypes.as_mut_ptr(),
                    values.as_mut_ptr(),
                    nulls.as_ptr(),
                    false,
                    1,
                );
                if ret < 0 {
                    error!("failed to insert segment {}: SPI error {}", seg.index, ret);
                }
            }
        }
    });

    // Close TOAST index and relation
    unsafe {
        pg_sys::index_close(
            output_state.toast_writer.toast_idx,
            pg_sys::RowExclusiveLock as pg_sys::LOCKMODE,
        );
        pg_sys::relation_close(
            output_state.toast_writer.toast_rel,
            pg_sys::RowExclusiveLock as pg_sys::LOCKMODE,
        );
    }

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

    fn generate_video(
        path: &std::path::Path,
        width: u32,
        height: u32,
        fps: i32,
        duration_secs: i32,
        bitrate: usize,
    ) {
        use ffmpeg_next::codec;
        use ffmpeg_next::format::Pixel;
        use ffmpeg_next::util::frame::video::Video;

        ffmpeg_next::init().unwrap();

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
        encoder.set_bit_rate(bitrate);
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

    /// Generate a minimal 3-second test video file using the ffmpeg library.
    fn generate_test_video(path: &std::path::Path) {
        generate_video(path, 64, 64, 10, 3, 400_000);
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

    #[pg_test]
    fn bench_hls_30s_sd() {
        let tmp = tempfile::Builder::new().suffix(".mp4").tempfile().unwrap();
        let video_path = tmp.path().to_path_buf();
        drop(tmp);

        let gen_start = std::time::Instant::now();
        generate_video(&video_path, 640, 480, 25, 30, 2_000_000);
        let gen_elapsed = gen_start.elapsed();

        let url = format!("file://{}", video_path.display());

        let hls_start = std::time::Instant::now();
        let playlist_id = crate::hls::hls(&url, 6);
        let hls_elapsed = hls_start.elapsed();

        pgrx::warning!(
            "BENCH bench_hls_30s_sd: video_gen={:.3}s hls={:.3}s total={:.3}s playlist_id={}",
            gen_elapsed.as_secs_f64(),
            hls_elapsed.as_secs_f64(),
            (gen_elapsed + hls_elapsed).as_secs_f64(),
            playlist_id,
        );

        assert!(playlist_id > 0);

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
        pgrx::warning!("BENCH bench_hls_30s_sd: segments={}", seg_count);

        let _ = std::fs::remove_file(&video_path);
    }
}
