use std::ffi::{c_int, c_void, CString};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr;

use ffmpeg_next::sys::{
    av_malloc, avformat_alloc_context, avformat_alloc_output_context2, avformat_find_stream_info,
    avformat_open_input, avio_alloc_context, avio_context_free, avio_flush, AVFormatContext,
    AVIOContext, AVERROR,
};
use pgrx::error;

use crate::limits;

const BUF_SIZE: usize = 32768;

// -----------------------------------------------------------------------------
// Input side (Task F1): MemInput borrows a `&[u8]` slice directly.
//
// Rationale: the old signature `MemInput::open(data: Vec<u8>)` forced a
// copy of every bytea argument into an owned Vec before FFmpeg could
// read it. Postgres gives us a borrowed slice from the argument's
// `bytea` header, so we should be able to hand that same slice to
// FFmpeg's AVIO callbacks without a detour. `MemInput<'a>` carries the
// borrow lifetime in its type, so the borrow checker guarantees the
// slice outlives any FFmpeg reads.
// -----------------------------------------------------------------------------

// Safety: the opaque pointer is always a `Cursor<&'static [u8]>`-shaped
// cell — it's really a `Cursor<&'a [u8]>` but Rust's extern "C"
// functions can't carry lifetime parameters, so we cast through the
// 'static view inside the callback. The borrow-checker guarantee comes
// from `MemInput<'a>` owning the cursor and keeping the slice alive.
unsafe extern "C" fn read_cb(opaque: *mut c_void, buf: *mut u8, buf_size: c_int) -> c_int {
    let cursor = &mut *(opaque as *mut Cursor<&'static [u8]>);
    let slice = std::slice::from_raw_parts_mut(buf, buf_size as usize);
    match cursor.read(slice) {
        Ok(0) => ffmpeg_next::sys::AVERROR_EOF,
        Ok(n) => n as c_int,
        Err(_) => ffmpeg_next::sys::AVERROR_EOF,
    }
}

unsafe extern "C" fn seek_cb(opaque: *mut c_void, offset: i64, whence: c_int) -> i64 {
    let cursor = &mut *(opaque as *mut Cursor<&'static [u8]>);
    const AVSEEK_SIZE: c_int = 0x10000;
    if whence == AVSEEK_SIZE {
        return cursor.get_ref().len() as i64;
    }
    let pos = match whence & 0xff {
        0 => SeekFrom::Start(offset as u64),
        1 => SeekFrom::Current(offset),
        2 => SeekFrom::End(offset),
        _ => return -1,
    };
    match cursor.seek(pos) {
        Ok(p) => p as i64,
        Err(_) => -1,
    }
}

/// FFmpeg input context backed by in-memory data the caller owns.
///
/// Borrows the input slice for the lifetime `'a` — callers pass the
/// bytea directly (`MemInput::open(&data)`) and the borrow checker
/// ensures the slice outlives the FFmpeg reads.
pub struct MemInput<'a> {
    ctx: Option<ffmpeg_next::format::context::Input>,
    avio_ctx: *mut AVIOContext,
    _cursor: Box<Cursor<&'a [u8]>>,
    _marker: PhantomData<&'a [u8]>,
}

impl<'a> MemInput<'a> {
    /// Open an input context over a borrowed slice. The caller's `data`
    /// must outlive the returned `MemInput<'a>`.
    ///
    /// Errors via Postgres `error!` if the slice exceeds
    /// `pg_ffmpeg.max_input_bytes` (F4). This centralizes the check so
    /// every function gets the same treatment without duplicating it at
    /// every callsite.
    pub fn open(data: &'a [u8]) -> Self {
        if let Err(e) = limits::check_input_size(data.len()) {
            error!("{e}");
        }
        unsafe {
            let mut cursor = Box::new(Cursor::new(data));
            let avio_buf = av_malloc(BUF_SIZE) as *mut u8;
            if avio_buf.is_null() {
                error!("failed to allocate AVIO buffer");
            }

            let avio_ctx = avio_alloc_context(
                avio_buf,
                BUF_SIZE as c_int,
                0,
                &mut *cursor as *mut Cursor<&'a [u8]> as *mut c_void,
                Some(read_cb),
                None,
                Some(seek_cb),
            );
            if avio_ctx.is_null() {
                error!("failed to allocate AVIO context");
            }

            let mut ps = avformat_alloc_context();
            if ps.is_null() {
                error!("failed to allocate format context");
            }
            (*ps).pb = avio_ctx;

            let ret = avformat_open_input(&mut ps, ptr::null(), ptr::null_mut(), ptr::null_mut());
            if ret < 0 {
                error!("failed to open input from memory");
            }

            avformat_find_stream_info(ps, ptr::null_mut());

            MemInput {
                ctx: Some(ffmpeg_next::format::context::Input::wrap(ps)),
                avio_ctx,
                _cursor: cursor,
                _marker: PhantomData,
            }
        }
    }
}

impl<'a> Deref for MemInput<'a> {
    type Target = ffmpeg_next::format::context::Input;
    fn deref(&self) -> &Self::Target {
        self.ctx.as_ref().unwrap()
    }
}

impl<'a> DerefMut for MemInput<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ctx.as_mut().unwrap()
    }
}

impl<'a> Drop for MemInput<'a> {
    fn drop(&mut self) {
        if let Some(mut ctx) = self.ctx.take() {
            unsafe { (*ctx.as_mut_ptr()).pb = ptr::null_mut() };
            drop(ctx);
        }
        unsafe {
            if !self.avio_ctx.is_null() {
                avio_context_free(&mut self.avio_ctx);
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Output side (Task F4 hook): MemOutput tracks cumulative bytes written
// and aborts the FFmpeg callback when the running total exceeds
// `pg_ffmpeg.max_output_bytes`. The callback returns AVERROR(ENOMEM) so
// the muxer unwinds cleanly; the Rust side then raises `error!` with
// the limit message.
// -----------------------------------------------------------------------------

/// Opaque state for the AVIO write callback. Stores the accumulating
/// buffer and the cumulative-byte counter that F4's
/// `check_output_size` consults on every write.
pub(crate) struct OutputSink {
    buf: Vec<u8>,
    written: usize,
    /// Set to `true` once a write was rejected by the output-size cap.
    /// We surface this at `into_data()` time so the caller can turn it
    /// into a proper Postgres ERROR with the limit message.
    over_limit: bool,
}

impl OutputSink {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            written: 0,
            over_limit: false,
        }
    }
}

unsafe extern "C" fn write_cb(opaque: *mut c_void, data: *const u8, size: c_int) -> c_int {
    let sink = &mut *(opaque as *mut OutputSink);
    let new_total = sink.written.saturating_add(size as usize);
    if limits::check_output_size(new_total).is_err() {
        sink.over_limit = true;
        // Return -ENOMEM (ENOMEM = 12 on Linux/POSIX). FFmpeg's muxers
        // treat any negative value as a write failure and unwind; we
        // surface the limit error at into_data() time.
        return AVERROR(12);
    }
    sink.buf
        .extend_from_slice(std::slice::from_raw_parts(data, size as usize));
    sink.written = new_total;
    size
}

/// FFmpeg output context that writes to an in-memory buffer.
pub struct MemOutput {
    ctx: Option<ffmpeg_next::format::context::Output>,
    avio_ctx: *mut AVIOContext,
    sink: Box<OutputSink>,
}

impl MemOutput {
    pub fn open(format: &str) -> Self {
        unsafe {
            let mut sink = Box::new(OutputSink::new());
            let avio_buf = av_malloc(BUF_SIZE) as *mut u8;
            if avio_buf.is_null() {
                error!("failed to allocate AVIO buffer");
            }

            let avio_ctx = avio_alloc_context(
                avio_buf,
                BUF_SIZE as c_int,
                1,
                &mut *sink as *mut OutputSink as *mut c_void,
                None,
                // Transmute to satisfy newer FFmpeg signature where data is *const u8.
                #[allow(clippy::missing_transmute_annotations)]
                Some(std::mem::transmute(write_cb as *const ())),
                None,
            );
            if avio_ctx.is_null() {
                error!("failed to allocate AVIO context");
            }

            let format_cstr = CString::new(format).unwrap();
            let mut ps: *mut AVFormatContext = ptr::null_mut();
            let ret = avformat_alloc_output_context2(
                &mut ps,
                ptr::null_mut(),
                format_cstr.as_ptr(),
                ptr::null(),
            );
            if ret < 0 || ps.is_null() {
                error!("failed to allocate output context for format '{format}'");
            }

            (*ps).pb = avio_ctx;

            MemOutput {
                ctx: Some(ffmpeg_next::format::context::Output::wrap(ps)),
                avio_ctx,
                sink,
            }
        }
    }

    /// Consume the output context and return the written data.
    ///
    /// If the output-size cap was hit mid-write, this raises a Postgres
    /// `error!` with the limit message instead of returning a partial
    /// buffer.
    pub fn into_data(mut self) -> Vec<u8> {
        unsafe {
            if !self.avio_ctx.is_null() {
                avio_flush(self.avio_ctx);
            }
        }
        if let Some(mut ctx) = self.ctx.take() {
            unsafe { (*ctx.as_mut_ptr()).pb = ptr::null_mut() };
            drop(ctx);
        }
        unsafe {
            if !self.avio_ctx.is_null() {
                avio_context_free(&mut self.avio_ctx);
            }
        }
        if self.sink.over_limit {
            // Synthesize a LimitError with the values we saw at the time
            // the cap tripped.
            let written = self.sink.written;
            match limits::check_output_size(written + 1) {
                Err(e) => error!("{e}"),
                Ok(()) => {
                    // Should not happen — the cap might have been raised
                    // between rejection and here. Fall through and
                    // return whatever we buffered.
                }
            }
        }
        std::mem::take(&mut self.sink.buf)
    }
}

impl Deref for MemOutput {
    type Target = ffmpeg_next::format::context::Output;
    fn deref(&self) -> &Self::Target {
        self.ctx.as_ref().unwrap()
    }
}

impl DerefMut for MemOutput {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ctx.as_mut().unwrap()
    }
}

impl Drop for MemOutput {
    fn drop(&mut self) {
        if let Some(mut ctx) = self.ctx.take() {
            unsafe { (*ctx.as_mut_ptr()).pb = ptr::null_mut() };
            drop(ctx);
        }
        unsafe {
            if !self.avio_ctx.is_null() {
                avio_context_free(&mut self.avio_ctx);
            }
        }
    }
}
