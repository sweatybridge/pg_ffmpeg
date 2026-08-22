use pgrx::prelude::*;
use std::collections::VecDeque;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::time::{Duration, Instant};

use ffmpeg_next::sys::{
    av_dict_free, av_dict_set, av_free, av_malloc, avformat_alloc_context,
    avformat_alloc_output_context2, avformat_close_input, avformat_find_stream_info,
    avformat_open_input, avio_alloc_context, avio_closep, avio_context_free, avio_open,
    avio_read_partial, AVDictionary, AVFormatContext, AVIOContext, AVIOInterruptCB, AVIO_FLAG_READ,
};

const LIVE_PROTOCOL_WHITELIST: &str = "file,http,https,tcp,tls,udp,rtp,rtsp";
const LIVE_PROBE_BYTES: i64 = 1_000_000;
const LIVE_ANALYZE_DURATION_US: i64 = 2_000_000;

struct LiveDeadline {
    deadline: Instant,
}

unsafe extern "C" fn interrupt_live_input(opaque: *mut c_void) -> c_int {
    if opaque.is_null() {
        return 0;
    }
    let deadline = &*(opaque as *const LiveDeadline);
    if Instant::now() >= deadline.deadline {
        1
    } else {
        0
    }
}

/// FFmpeg input context whose blocking reads are interrupted after a stall timeout.
struct LiveInput {
    ctx: Option<ffmpeg_next::format::context::Input>,
    _deadline: Box<LiveDeadline>,
    stall_timeout: Duration,
}

impl LiveInput {
    fn open(url: &str, stall_timeout: f64) -> Self {
        unsafe {
            let stall_timeout = Duration::from_secs_f64(stall_timeout);
            let mut deadline = Box::new(LiveDeadline {
                deadline: Instant::now() + stall_timeout,
            });
            let mut ps = avformat_alloc_context();
            if ps.is_null() {
                error!("failed to allocate live input context");
            }
            // Endless inputs cannot rely on EOF to finish stream discovery.
            (*ps).probesize = LIVE_PROBE_BYTES;
            (*ps).max_analyze_duration = LIVE_ANALYZE_DURATION_US;
            (*ps).interrupt_callback = AVIOInterruptCB {
                callback: Some(interrupt_live_input),
                opaque: &mut *deadline as *mut LiveDeadline as *mut c_void,
            };

            let key = CString::new("protocol_whitelist").unwrap();
            let value = CString::new(LIVE_PROTOCOL_WHITELIST).unwrap();
            let timeout_key = CString::new("rw_timeout").unwrap();
            let timeout_value =
                CString::new(stall_timeout.as_micros().min(i64::MAX as u128).to_string()).unwrap();
            let mut options: *mut AVDictionary = ptr::null_mut();
            if av_dict_set(&mut options, key.as_ptr(), value.as_ptr(), 0) < 0 {
                av_dict_free(&mut options);
                avformat_close_input(&mut ps);
                error!("failed to configure live input protocols");
            }
            if av_dict_set(
                &mut options,
                timeout_key.as_ptr(),
                timeout_value.as_ptr(),
                0,
            ) < 0
            {
                av_dict_free(&mut options);
                avformat_close_input(&mut ps);
                error!("failed to configure live input timeout");
            }

            let url =
                CString::new(url).unwrap_or_else(|_| error!("live input url contains a NUL byte"));
            let open_result =
                avformat_open_input(&mut ps, url.as_ptr(), ptr::null_mut(), &mut options);
            av_dict_free(&mut options);
            if open_result < 0 {
                avformat_close_input(&mut ps);
                error!("failed to open live input url: {open_result}");
            }

            let stream_result = avformat_find_stream_info(ps, ptr::null_mut());
            if stream_result < 0 {
                avformat_close_input(&mut ps);
                error!("failed to read live input stream info: {stream_result}");
            }

            Self {
                ctx: Some(ffmpeg_next::format::context::Input::wrap(ps)),
                _deadline: deadline,
                stall_timeout,
            }
        }
    }

    fn reset_stall_deadline(&mut self) {
        self._deadline.deadline = Instant::now() + self.stall_timeout;
    }

    fn deadline_ptr(&mut self) -> *mut LiveDeadline {
        &mut *self._deadline
    }
}

impl Deref for LiveInput {
    type Target = ffmpeg_next::format::context::Input;

    fn deref(&self) -> &Self::Target {
        self.ctx.as_ref().unwrap()
    }
}

impl DerefMut for LiveInput {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ctx.as_mut().unwrap()
    }
}

impl Drop for LiveInput {
    fn drop(&mut self) {
        // Drop the FFmpeg context while its interrupt callback still points at `deadline`.
        drop(self.ctx.take());
    }
}

/// Custom write callback: appends into a Vec<u8>.
unsafe extern "C" fn vec_write(opaque: *mut c_void, data: *const u8, size: c_int) -> c_int {
    let buf = &mut *(opaque as *mut Vec<u8>);
    buf.extend_from_slice(std::slice::from_raw_parts(data, size as usize));
    size
}

struct HlsIoState {
    segment_index: i32,
    completed_segments: VecDeque<CompletedSegment>,
    /// Buffer for the m3u8 playlist being written.
    m3u8_buf: Vec<u8>,
    m3u8_pb: *mut AVIOContext,
    /// Buffer for the current .ts segment being written.
    seg_buf: Vec<u8>,
    segment_pb: *mut AVIOContext,
    /// DTS tracking for computing segment duration.
    seg_start_dts: Option<i64>,
    current_pkt_dts: Option<i64>,
    video_tb_scale: f64,
}

struct CompletedSegment {
    segment_index: i32,
    duration: Option<f64>,
    data: Vec<u8>,
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

    let avio_buf = av_malloc(pg_sys::BLCKSZ as usize) as *mut u8;
    if avio_buf.is_null() {
        return ffmpeg_next::sys::AVERROR_EOF;
    }

    let ctx = avio_alloc_context(
        avio_buf,
        pg_sys::BLCKSZ as c_int,
        1,
        target_buf as *mut c_void,
        None,
        // Transmute to satisfy newer FFmpeg signature where data is *const u8.
        #[allow(clippy::missing_transmute_annotations)]
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

unsafe extern "C" fn hls_io_close2(s: *mut AVFormatContext, pb: *mut AVIOContext) -> c_int {
    let state = &mut *((*s).opaque as *mut HlsIoState);

    if pb == state.segment_pb {
        state.segment_pb = ptr::null_mut();
        if !state.seg_buf.is_empty() {
            let idx = state.segment_index;
            state.segment_index += 1;
            let duration = match (state.seg_start_dts, state.current_pkt_dts) {
                (Some(start), Some(end)) => Some((end - start) as f64 * state.video_tb_scale),
                _ => None,
            };
            state.seg_start_dts = state.current_pkt_dts;
            let data = std::mem::take(&mut state.seg_buf);
            state.completed_segments.push_back(CompletedSegment {
                segment_index: idx,
                duration,
                data,
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
// --- Still-image detection / raw-byte fetch ---

/// Detect an input whose video stream is a single still frame. The
/// HLS/mpegts muxer can't wrap such a stream into a thumbnail-decodable
/// segment, so `hls()` bypasses muxing for these and stores the
/// original image bytes as a single segment row instead.
///
/// The image2 / png_pipe / jpeg_pipe demuxers all advertise a non-zero
/// frame rate and a non-zero duration by default, so stream metadata
/// can't reliably distinguish a still from real video. Instead, we
/// check the input format name: single-image demuxers are named
/// `image2`, `image2pipe`, or `<codec>_pipe` (e.g. `png_pipe`).
fn is_still_image_input(ictx: &ffmpeg_next::format::context::Input) -> bool {
    let fmt = ictx.format();
    let name = fmt.name();
    name.starts_with("image2") || name.ends_with("_pipe")
}

/// Slurp the raw bytes backing `url` via FFmpeg's AVIO layer so the same
/// URL protocol coverage as `format::input(&url)` (file, http, https,
/// ...) is reused. Bounded by `pg_ffmpeg.max_input_bytes` since the
/// fetched bytes are later handed to `thumbnail` -> `MemInput` which
/// enforces the same cap.
fn fetch_url_bytes(url: &str) -> Vec<u8> {
    unsafe {
        let url_c = std::ffi::CString::new(url).unwrap();
        let mut pb: *mut AVIOContext = ptr::null_mut();
        let ret = avio_open(&mut pb, url_c.as_ptr(), AVIO_FLAG_READ as c_int);
        if ret < 0 || pb.is_null() {
            error!("failed to open url for still-image fetch: {ret}");
        }

        let mut out = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            let n = avio_read_partial(pb, buf.as_mut_ptr(), buf.len() as c_int);
            if n == ffmpeg_next::sys::AVERROR_EOF || n < 0 {
                break;
            }
            out.extend_from_slice(&buf[..n as usize]);
            if let Err(e) = crate::limits::check_input_size(out.len()) {
                let _ = avio_closep(&mut pb);
                error!("{e}");
            }
        }
        let _ = avio_closep(&mut pb);
        out
    }
}

// --- Main function ---

fn create_playlist(segment_duration: i32) -> i64 {
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO ffmpeg.hls_playlists (target_duration) VALUES ($1) RETURNING id",
                None,
                &[pgrx::datum::DatumWithOid::from(segment_duration)],
            )
            .unwrap_or_else(|e| error!("failed to insert playlist: {e}"))
            .first()
            .get_one::<i64>()
            .unwrap_or_else(|e| error!("failed to get playlist id: {e}"))
            .unwrap_or_else(|| error!("playlist id was null"))
    })
}

fn store_still_image(url: &str, playlist_id: i64) {
    let data = fetch_url_bytes(url);
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO ffmpeg.hls_segments (playlist_id, segment_index, duration, data) \
                 VALUES ($1, 0, NULL, $2)",
                None,
                &[
                    pgrx::datum::DatumWithOid::from(playlist_id),
                    pgrx::datum::DatumWithOid::from(data),
                ],
            )
            .unwrap_or_else(|e| error!("failed to insert still-image segment: {e}"));
    });
}

fn store_segment(playlist_id: i64, segment: CompletedSegment) {
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO ffmpeg.hls_segments (playlist_id, segment_index, duration, data) \
                 VALUES ($1, $2, $3, $4)",
                None,
                &[
                    pgrx::datum::DatumWithOid::from(playlist_id),
                    pgrx::datum::DatumWithOid::from(segment.segment_index),
                    pgrx::datum::DatumWithOid::from(segment.duration),
                    pgrx::datum::DatumWithOid::from(segment.data),
                ],
            )
            .unwrap_or_else(|e| error!("failed to insert segment: {e}"));
    });
}

fn remux_hls<OnSegment, ShouldStop>(
    ictx: &mut ffmpeg_next::format::context::Input,
    playlist_id: i64,
    segment_duration: i32,
    first_segment_index: i32,
    live: bool,
    mut on_segment: OnSegment,
    mut should_stop: ShouldStop,
) -> (i64, bool)
where
    OnSegment: FnMut(CompletedSegment),
    ShouldStop: FnMut() -> bool,
{
    // Allocate HLS output context with streaming I/O callbacks
    let mut output_state = Box::new(HlsIoState {
        segment_index: first_segment_index,
        completed_segments: VecDeque::new(),
        m3u8_buf: Vec::new(),
        m3u8_pb: ptr::null_mut(),
        seg_buf: Vec::new(),
        segment_pb: ptr::null_mut(),
        seg_start_dts: None,
        current_pkt_dts: None,
        video_tb_scale: 0.0,
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
    let mut video_out_idx: Option<usize> = None;
    for input_stream in ictx.streams() {
        let medium = input_stream.parameters().medium();
        if medium == ffmpeg_next::media::Type::Video
            || medium == ffmpeg_next::media::Type::Audio
            || medium == ffmpeg_next::media::Type::Subtitle
        {
            let output_index = crate::pipeline::copy_stream(&input_stream, &mut octx);
            if medium == ffmpeg_next::media::Type::Video && video_out_idx.is_none() {
                video_out_idx = Some(output_index);
            }
            stream_mapping.push(Some(output_index));
        } else {
            stream_mapping.push(None);
        }
    }

    // Configure HLS options
    let mut opts = ffmpeg_next::Dictionary::new();
    opts.set("hls_time", &segment_duration.to_string());
    opts.set("hls_segment_filename", "seg%03d.ts");
    opts.set("hls_list_size", "0");
    if !live {
        opts.set("hls_playlist_type", "vod");
    }

    octx.write_header_with(opts)
        .unwrap_or_else(|e| error!("failed to write HLS header: {e}"));

    // Set video time_base scale for duration computation in close callback
    if let Some(idx) = video_out_idx {
        if let Some(stream) = octx.stream(idx) {
            let tb = stream.time_base();
            output_state.video_tb_scale = tb.0 as f64 / tb.1 as f64;
        }
    }

    // The FFmpeg callback only moves completed bytes into a Rust queue. Database
    // writes happen here, after control has returned from the C callback.
    let mut stopped = false;
    for (stream, mut packet) in ictx.packets() {
        if should_stop() {
            stopped = true;
            break;
        }
        let input_index = stream.index();
        if let Some(Some(out_idx)) = stream_mapping.get(input_index) {
            let in_tb = stream.time_base();
            let out_tb = octx.stream(*out_idx).unwrap().time_base();
            packet.set_stream(*out_idx);
            packet.rescale_ts(in_tb, out_tb);
            packet.set_position(-1);

            // Track video DTS before write (write may trigger segment close in callback)
            if Some(*out_idx) == video_out_idx {
                if let Some(dts) = packet.dts() {
                    output_state.current_pkt_dts = Some(dts);
                    if output_state.seg_start_dts.is_none() {
                        output_state.seg_start_dts = Some(dts);
                    }
                }
            }

            packet
                .write_interleaved(&mut octx)
                .unwrap_or_else(|e| error!("failed to write packet: {e}"));

            while let Some(segment) = output_state.completed_segments.pop_front() {
                on_segment(segment);
            }
        }
    }

    octx.write_trailer()
        .unwrap_or_else(|e| error!("failed to write trailer: {e}"));

    while let Some(segment) = output_state.completed_segments.pop_front() {
        on_segment(segment);
    }

    (playlist_id, stopped)
}

#[pg_extern]
fn hls(url: &str, segment_duration: default!(i32, 6)) -> i64 {
    ffmpeg_next::init().unwrap();
    ffmpeg_next::format::network::init();

    let playlist_id = create_playlist(segment_duration);
    let mut ictx = ffmpeg_next::format::input(&url)
        .unwrap_or_else(|e| error!("failed to open input url: {e}"));

    // Still-image inputs (single frame, no frame rate) cannot be wrapped
    // into a thumbnail-decodable mpegts segment by the HLS muxer. Bypass
    // muxing and store the original image bytes as one segment so
    // `ffmpeg.thumbnail` can decode them directly.
    if is_still_image_input(&ictx) {
        store_still_image(url, playlist_id);
        return playlist_id;
    }

    remux_hls(
        &mut ictx,
        playlist_id,
        segment_duration,
        0,
        false,
        |segment| store_segment(playlist_id, segment),
        || false,
    )
    .0
}

struct NonAtomicSpi;

impl NonAtomicSpi {
    fn connect() -> Self {
        let result = unsafe { pg_sys::SPI_connect_ext(pg_sys::SPI_OPT_NONATOMIC as c_int) };
        if result != pg_sys::SPI_OK_CONNECT as c_int {
            error!("failed to connect to non-atomic SPI: {result}");
        }
        if !unsafe { pg_sys::SPI_inside_nonatomic_context() } {
            unsafe {
                pg_sys::SPI_finish();
            }
            error!("hls_live must be called outside an explicit transaction");
        }
        Self
    }

    fn commit_and_chain(&self) {
        unsafe {
            pg_sys::SPI_commit_and_chain();
        }
    }
}

impl Drop for NonAtomicSpi {
    fn drop(&mut self) {
        unsafe {
            pg_sys::SPI_finish();
        }
    }
}

fn claim_live_playlist(url: &str, segment_duration: i32) -> (i64, i32) {
    let playlist_id = Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO ffmpeg.hls_playlists \
                    (source_url, target_duration, stop_requested, owner_pid) \
                 VALUES ($1, $2, false, pg_backend_pid()) \
                 ON CONFLICT (source_url) DO UPDATE SET \
                    target_duration = EXCLUDED.target_duration, \
                    stop_requested = false, \
                    owner_pid = pg_backend_pid(), \
                    updated_at = clock_timestamp() \
                 WHERE hls_playlists.owner_pid IS NULL \
                    OR hls_playlists.owner_pid = pg_backend_pid() \
                    OR NOT EXISTS ( \
                        SELECT 1 FROM pg_stat_activity a \
                        WHERE a.pid = hls_playlists.owner_pid AND a.state = 'active' \
                    ) \
                 RETURNING id",
                None,
                &[
                    pgrx::datum::DatumWithOid::from(url),
                    pgrx::datum::DatumWithOid::from(segment_duration),
                ],
            )
            .unwrap_or_else(|e| error!("failed to claim live playlist: {e}"))
            .first()
            .get_one::<i64>()
            .unwrap_or_else(|e| error!("failed to read live playlist id: {e}"))
            .unwrap_or_else(|| error!("hls_live is already running for this url"))
    });

    let first_segment_index = Spi::connect(|client| {
        client
            .select(
                "SELECT COALESCE(max(segment_index), -1) + 1 \
                 FROM ffmpeg.hls_segments WHERE playlist_id = $1",
                None,
                &[pgrx::datum::DatumWithOid::from(playlist_id)],
            )
            .unwrap_or_else(|e| error!("failed to read live segment sequence: {e}"))
            .first()
            .get_one::<i32>()
            .unwrap_or_else(|e| error!("failed to read live segment index: {e}"))
            .unwrap_or(0)
    });

    (playlist_id, first_segment_index)
}

fn release_live_playlist(playlist_id: i64) {
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE ffmpeg.hls_playlists SET owner_pid = NULL, updated_at = clock_timestamp() \
                 WHERE id = $1 AND owner_pid = pg_backend_pid()",
                None,
                &[pgrx::datum::DatumWithOid::from(playlist_id)],
            )
            .unwrap_or_else(|e| error!("failed to release live playlist: {e}"));
    });
}

fn live_stop_requested(url: &str) -> bool {
    Spi::connect(|client| {
        client
            .select(
                "SELECT stop_requested FROM ffmpeg.hls_playlists WHERE source_url = $1",
                None,
                &[pgrx::datum::DatumWithOid::from(url)],
            )
            .unwrap_or_else(|e| error!("failed to read live stream state: {e}"))
            .first()
            .get_one::<bool>()
            .unwrap_or_else(|e| error!("failed to read live stop request: {e}"))
            .unwrap_or(true)
    })
}

fn store_live_segment(playlist_id: i64, segment: CompletedSegment) {
    store_segment(playlist_id, segment);
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE ffmpeg.hls_playlists SET updated_at = clock_timestamp() WHERE id = $1",
                None,
                &[pgrx::datum::DatumWithOid::from(playlist_id)],
            )
            .unwrap_or_else(|e| error!("failed to update live playlist heartbeat: {e}"));
    });
}

/// Continuously remux a live URL into HLS, committing every completed segment.
///
/// This must be invoked as a top-level `CALL`, which allows the procedure to commit
/// while its FFmpeg input and output contexts remain open in the current backend.
#[pg_extern(sql = r#"
CREATE PROCEDURE ffmpeg.hls_live(
    url text,
    segment_duration integer DEFAULT 6,
    stall_timeout double precision DEFAULT 10.0
)
LANGUAGE c
AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
"#)]
fn hls_live(url: &str, segment_duration: default!(i32, 6), stall_timeout: default!(f64, 10.0)) {
    if segment_duration <= 0 {
        error!("segment_duration must be greater than 0");
    }
    if !stall_timeout.is_finite() || stall_timeout <= 0.0 {
        error!("stall_timeout must be finite and greater than 0");
    }
    // Procedure arguments are PostgreSQL Datums. Keep only Rust-owned state across commits.
    let url = url.to_owned();

    ffmpeg_next::init().unwrap();
    ffmpeg_next::format::network::init();

    let spi = NonAtomicSpi::connect();
    let (playlist_id, first_segment_index) = claim_live_playlist(&url, segment_duration);
    spi.commit_and_chain();

    let mut ictx = LiveInput::open(&url, stall_timeout);
    #[cfg(any(test, feature = "pg_test"))]
    warning!("hls_live diagnostic: input ready");
    if is_still_image_input(&ictx) {
        error!("hls_live does not support still-image inputs; use ffmpeg.hls instead");
    }

    ictx.reset_stall_deadline();
    let deadline = ictx.deadline_ptr();
    let mut next_stop_poll = Instant::now();
    #[cfg(any(test, feature = "pg_test"))]
    let mut saw_packet = false;
    let (_, stopped_in_packet_loop) = remux_hls(
        &mut ictx,
        playlist_id,
        segment_duration,
        first_segment_index,
        true,
        |segment| {
            #[cfg(any(test, feature = "pg_test"))]
            warning!("hls_live diagnostic: completed segment");
            store_live_segment(playlist_id, segment);
            spi.commit_and_chain();
        },
        || {
            #[cfg(any(test, feature = "pg_test"))]
            if !saw_packet {
                warning!("hls_live diagnostic: first packet");
                saw_packet = true;
            }
            unsafe {
                (*deadline).deadline = Instant::now() + Duration::from_secs_f64(stall_timeout);
            }
            if Instant::now() < next_stop_poll {
                return false;
            }
            next_stop_poll = Instant::now() + Duration::from_millis(500);
            live_stop_requested(&url)
        },
    );
    let stopped = stopped_in_packet_loop || live_stop_requested(&url);

    release_live_playlist(playlist_id);
    spi.commit_and_chain();

    if !stopped {
        error!("live input ended or stalled");
    }
}

#[pg_extern]
fn hls_live_stop(url: &str) -> bool {
    Spi::connect_mut(|client| {
        client
            .update(
                "UPDATE ffmpeg.hls_playlists SET stop_requested = true \
                 WHERE source_url = $1 RETURNING true",
                None,
                &[pgrx::datum::DatumWithOid::from(url)],
            )
            .unwrap_or_else(|e| error!("failed to request live stream stop: {e}"))
            .first()
            .get_one::<bool>()
            .unwrap_or_else(|e| error!("failed to read live stop result: {e}"))
            .unwrap_or(false)
    })
}

#[cfg(any(test, feature = "pg_test", feature = "pg_bench"))]
pub(crate) fn generate_video(
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

    let codec = crate::codec_lookup::find_encoder_by_id(
        codec::Id::MPEG2VIDEO,
        crate::codec_lookup::CodecKind::Video,
    )
    .expect("MPEG2VIDEO encoder not found");

    let mut octx =
        ffmpeg_next::format::output_as(path, "mpegts").expect("failed to create output context");

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
    let out_time_base = {
        stream.set_parameters(&encoder);
        stream.time_base()
    };

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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    fn psql_command(sql: &str, database: &str, user: &str, port: i32) -> std::process::Command {
        let psql = std::env::current_exe()
            .expect("failed to find postgres executable")
            .with_file_name("psql");
        let mut command = std::process::Command::new(psql);
        command.args([
            "-X",
            "-A",
            "-t",
            "-w",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-U",
            user,
            "-d",
            database,
            "-c",
            sql,
        ]);
        command
    }

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
        let target_dur = Spi::connect(|client| {
            client
                .select(
                    "SELECT target_duration FROM ffmpeg.hls_playlists WHERE id = $1",
                    None,
                    &[pgrx::datum::DatumWithOid::from(playlist_id)],
                )
                .unwrap()
                .first()
                .get_one::<i32>()
                .unwrap()
        });
        assert_eq!(
            target_dur.unwrap(),
            2,
            "target_duration should match segment_duration"
        );

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
            assert!(
                duration.unwrap() > 0.0,
                "duration should be positive for segment {i}"
            );
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
    fn test_hls_still_image_returns_single_decodable_segment() {
        // A single PNG is the path that previously produced an
        // undecodable mpegts blob. `hls()` should now bypass muxing and
        // store the original image bytes as one segment.
        let img = crate::test_utils::generate_test_image_bytes("png", 32, 32);
        let tmp = tempfile::Builder::new().suffix(".png").tempfile().unwrap();
        let image_path = tmp.path().to_path_buf();
        drop(tmp);
        std::fs::write(&image_path, &img).unwrap();

        let url = format!("file://{}", image_path.display());
        let playlist_id = crate::hls::hls(&url, 6);
        assert!(playlist_id > 0);

        let count = Spi::connect(|client| {
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
        assert_eq!(count, 1, "still-image input should produce one segment");

        let seg_data = Spi::connect(|client| {
            client
                .select(
                    "SELECT data FROM ffmpeg.hls_segments WHERE playlist_id = $1",
                    None,
                    &[pgrx::datum::DatumWithOid::from(playlist_id)],
                )
                .unwrap()
                .first()
                .get_one::<Vec<u8>>()
                .unwrap()
                .unwrap()
        });
        assert_eq!(
            seg_data, img,
            "still-image segment should store the raw original bytes"
        );

        // The whole point: thumbnail must be able to decode the segment.
        let thumb = Spi::connect(|client| {
            client
                .select(
                    "SELECT ffmpeg.thumbnail($1, 0.0, 'png')",
                    None,
                    &[pgrx::datum::DatumWithOid::from(seg_data)],
                )
                .unwrap()
                .first()
                .get_one::<Vec<u8>>()
                .unwrap()
                .unwrap()
        });
        assert!(!thumb.is_empty(), "thumbnail should produce output");
        assert_eq!(
            &thumb[..8],
            b"\x89PNG\r\n\x1a\n",
            "thumbnail output should be PNG"
        );

        let _ = std::fs::remove_file(&image_path);
    }

    #[pg_test]
    #[should_panic(expected = "segment_duration must be greater than 0")]
    fn test_hls_live_rejects_non_positive_segment_duration() {
        hls_live("udp://127.0.0.1:5001", 0, 10.0);
    }

    #[pg_test]
    #[should_panic(expected = "stall_timeout must be finite and greater than 0")]
    fn test_hls_live_rejects_non_positive_stall_timeout() {
        hls_live("udp://127.0.0.1:5001", 1, 0.0);
    }

    #[pg_test]
    fn test_hls_live_stop_uses_url_as_stream_key() {
        let url = "udp://127.0.0.1:5001?stream=stable-test-key";
        Spi::connect_mut(|client| {
            client
                .update(
                    "INSERT INTO ffmpeg.hls_playlists (source_url) VALUES ($1)",
                    None,
                    &[pgrx::datum::DatumWithOid::from(url)],
                )
                .unwrap();
        });

        assert!(hls_live_stop(url));

        let requested = Spi::connect(|client| {
            client
                .select(
                    "SELECT stop_requested FROM ffmpeg.hls_playlists WHERE source_url = $1",
                    None,
                    &[pgrx::datum::DatumWithOid::from(url)],
                )
                .unwrap()
                .first()
                .get_one::<bool>()
                .unwrap()
        });
        assert_eq!(requested, Some(true));
    }

    #[pg_test]
    fn test_hls_live_commits_segments_before_call_returns() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::process::Stdio;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;

        let tmp = tempfile::Builder::new().suffix(".ts").tempfile().unwrap();
        let video_path = tmp.path().to_path_buf();
        drop(tmp);
        generate_video(&video_path, 64, 64, 10, 30, 400_000);
        let video = std::fs::read(&video_path).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let stop_sender = Arc::new(AtomicBool::new(false));
        let source_accepted = Arc::new(AtomicBool::new(false));
        let source_sent = Arc::new(AtomicBool::new(false));
        let sender_stop = Arc::clone(&stop_sender);
        let sender_accepted = Arc::clone(&source_accepted);
        let sender_sent = Arc::clone(&source_sent);
        let sender = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            sender_accepted.store(true, Ordering::Relaxed);
            let mut request = [0_u8; 4096];
            let request_len = stream.read(&mut request).unwrap();
            assert!(request_len > 0, "live source received an empty request");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            if stream.write_all(&video).is_ok() {
                sender_sent.store(true, Ordering::Relaxed);
                while !sender_stop.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        });

        let database = Spi::get_one::<String>("SELECT current_database()::text")
            .unwrap()
            .unwrap();
        let user = Spi::get_one::<String>("SELECT current_user::text")
            .unwrap()
            .unwrap();
        let postgres_port = unsafe { pg_sys::PostPortNumber };
        let url = format!("http://127.0.0.1:{port}/live.ts");
        let quoted_url = url.replace('\'', "''");

        let call_sql = format!("CALL ffmpeg.hls_live('{quoted_url}', 1, 3.0)");
        let mut call = psql_command(&call_sql, &database, &user, postgres_port)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let readiness_sql = format!(
            "SELECT count(*) FROM ffmpeg.hls_playlists \
             WHERE source_url = '{quoted_url}' AND owner_pid IS NOT NULL"
        );
        let readiness_deadline = Instant::now() + Duration::from_secs(10);
        let mut call_ready = false;
        while Instant::now() < readiness_deadline {
            if call.try_wait().unwrap().is_some() {
                break;
            }
            let observed = psql_command(&readiness_sql, &database, &user, postgres_port)
                .output()
                .unwrap();
            if observed.status.success()
                && String::from_utf8_lossy(&observed.stdout)
                    .trim()
                    .parse::<i64>()
                    .unwrap_or(0)
                    > 0
            {
                call_ready = true;
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        if !call_ready {
            if call.try_wait().unwrap().is_none() {
                call.kill().unwrap();
            }
            let call_output = call.wait_with_output().unwrap();
            let _ = std::fs::remove_file(&video_path);
            panic!(
                "live CALL did not commit its playlist claim: {}",
                String::from_utf8_lossy(&call_output.stderr)
            );
        }
        let count_sql = format!(
            "SELECT count(*) FROM ffmpeg.hls_segments s \
             JOIN ffmpeg.hls_playlists p ON p.id = s.playlist_id \
             WHERE p.source_url = '{quoted_url}'"
        );
        let visibility_deadline = Instant::now() + Duration::from_secs(10);
        let mut visible_while_running = false;
        while Instant::now() < visibility_deadline {
            thread::sleep(Duration::from_millis(100));
            if call.try_wait().unwrap().is_some() {
                break;
            }
            let observed = psql_command(&count_sql, &database, &user, postgres_port)
                .output()
                .unwrap();
            if observed.status.success()
                && String::from_utf8_lossy(&observed.stdout)
                    .trim()
                    .parse::<i64>()
                    .unwrap_or(0)
                    > 0
            {
                visible_while_running = true;
                break;
            }
        }

        let backend_state_sql = format!(
            "SELECT concat_ws('/', state, COALESCE(wait_event_type, 'CPU'), \
                    COALESCE(wait_event, 'running')) \
             FROM pg_stat_activity \
             WHERE pid = (SELECT owner_pid FROM ffmpeg.hls_playlists \
                          WHERE source_url = '{quoted_url}')"
        );
        let backend_state = psql_command(&backend_state_sql, &database, &user, postgres_port)
            .output()
            .unwrap();

        let stop_sql = format!("SELECT ffmpeg.hls_live_stop('{quoted_url}')");
        let stop_output = psql_command(&stop_sql, &database, &user, postgres_port)
            .output()
            .unwrap();

        let exit_deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < exit_deadline && call.try_wait().unwrap().is_none() {
            thread::sleep(Duration::from_millis(100));
        }
        if call.try_wait().unwrap().is_none() {
            call.kill().unwrap();
        }
        let call_output = call.wait_with_output().unwrap();

        stop_sender.store(true, Ordering::Relaxed);
        sender.join().unwrap();
        let _ = std::fs::remove_file(&video_path);

        assert!(
            visible_while_running,
            "another session did not see a segment before CALL returned \
             (source accepted: {}, source sent: {}, backend: {}): {}",
            source_accepted.load(Ordering::Relaxed),
            source_sent.load(Ordering::Relaxed),
            String::from_utf8_lossy(&backend_state.stdout).trim(),
            String::from_utf8_lossy(&call_output.stderr)
        );
        assert!(
            stop_output.status.success(),
            "failed to request live stop: {}",
            String::from_utf8_lossy(&stop_output.stderr)
        );
        assert!(
            call_output.status.success(),
            "live CALL did not stop cleanly: {}",
            String::from_utf8_lossy(&call_output.stderr)
        );
    }
}

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use crate::bench_common::{generate_sample_video, sample_video_path};
    use pgrx::pg_bench;
    use pgrx_bench::{black_box, Bencher};

    #[pg_bench(setup = generate_sample_video)]
    fn bench_hls_30s_sd(b: &mut Bencher) {
        let url = format!("file://{}", sample_video_path().display());
        b.iter(move || {
            black_box(crate::hls::hls(&url, 6));
        });
    }
}
