use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
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

/// State for the input read AVIO callbacks.
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
        0 => offset,                         // SEEK_SET
        1 => state.pos as i64 + offset,      // SEEK_CUR
        2 => state.data.len() as i64 + offset, // SEEK_END
        _ => return -1,
    };
    if new_pos < 0 || new_pos > state.data.len() as i64 {
        return -1;
    }
    state.pos = new_pos as usize;
    new_pos
}

// --- Output AVIO: HLS muxer writes segments to memory ---

/// Shared state for custom HLS output I/O callbacks.
struct HlsIoState {
    /// Completed files: (filename, data). Last entry wins for duplicates.
    completed: Vec<(String, Vec<u8>)>,
    /// Currently open AVIO contexts: ptr address -> filename.
    open_contexts: HashMap<usize, String>,
}

/// Custom `io_open` callback: creates an in-memory AVIO context instead of a file.
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

/// Custom `io_close2` callback: collects the in-memory buffer into `completed`.
unsafe extern "C" fn hls_io_close2(
    s: *mut AVFormatContext,
    pb: *mut AVIOContext,
) -> c_int {
    let state = &mut *((*s).opaque as *mut HlsIoState);

    let mut buf: *mut u8 = ptr::null_mut();
    let size = avio_close_dyn_buf(pb, &mut buf);

    if let Some(filename) = state.open_contexts.remove(&(pb as usize)) {
        if size > 0 {
            let data = std::slice::from_raw_parts(buf, size as usize).to_vec();
            state.completed.push((filename, data));
        }
    }

    if !buf.is_null() {
        av_free(buf as *mut c_void);
    }
    0
}

// --- m3u8 parsing ---

struct SegmentInfo {
    filename: String,
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

// --- Main function ---

#[pg_extern(schema = "pg_ffmpeg")]
fn hls(data: Vec<u8>, segment_duration: default!(i32, 6)) -> i64 {
    ffmpeg_next::init().unwrap();

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
            0, // read-only
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

        let ret = avformat_open_input(&mut (ps as *mut _), ptr::null(), ptr::null(), ptr::null_mut());
        if ret < 0 {
            // avformat_open_input frees ps on failure
            avio_context_free(&mut avio_ctx_ptr);
            error!("failed to open input: error code {ret}");
        }

        let ret = avformat_find_stream_info(ps, ptr::null_mut());
        if ret < 0 {
            error!("failed to find stream info: error code {ret}");
        }

        ffmpeg_next::format::context::Input::wrap(ps)
    };

    // Allocate HLS output context with custom in-memory I/O
    let mut output_state = Box::new(HlsIoState {
        completed: Vec::new(),
        open_contexts: HashMap::new(),
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

    // Drop contexts before consuming state
    drop(octx);
    drop(ictx);

    // Clean up input AVIO (avformat_close_input doesn't free custom IO)
    unsafe {
        avio_context_free(&mut avio_ctx_ptr);
    }
    drop(input_state);

    let HlsIoState { completed, .. } = *output_state;

    // Parse the m3u8 playlist from in-memory buffers
    let m3u8_content = completed
        .iter()
        .rev()
        .find(|(name, _)| name.ends_with(".m3u8"))
        .map(|(_, data)| String::from_utf8_lossy(data).into_owned())
        .unwrap_or_else(|| error!("no m3u8 playlist found in output"));

    let playlist_info = parse_m3u8(&m3u8_content);

    // Build lookup of segment data by filename
    let segment_files: HashMap<&str, &[u8]> = completed
        .iter()
        .filter(|(name, _)| name.ends_with(".ts"))
        .map(|(name, data)| {
            let basename = name.rsplit('/').next().unwrap_or(name);
            (basename, data.as_slice())
        })
        .collect();

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

        for (i, seg) in playlist_info.segments.iter().enumerate() {
            let seg_data = segment_files
                .get(seg.filename.as_str())
                .unwrap_or_else(|| error!("missing segment data for {}", seg.filename));

            client
                .update(
                    "INSERT INTO pg_ffmpeg.hls_segments (playlist_id, segment_index, duration, data) VALUES ($1, $2, $3, $4)",
                    None,
                    &[
                        DatumWithOid::from(playlist_id),
                        DatumWithOid::from(i as i32),
                        DatumWithOid::from(seg.duration),
                        DatumWithOid::from(seg_data.to_vec()),
                    ],
                )
                .unwrap_or_else(|e| error!("failed to insert segment {i}: {e}"));
        }

        playlist_id
    })
}
