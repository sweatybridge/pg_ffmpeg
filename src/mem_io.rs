use std::ffi::{c_int, c_void, CString};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::ops::{Deref, DerefMut};
use std::ptr;

use ffmpeg_next::sys::{
    av_malloc, avformat_alloc_context, avformat_alloc_output_context2, avformat_find_stream_info,
    avformat_open_input, avio_alloc_context, avio_context_free, avio_flush, AVFormatContext,
    AVIOContext,
};
use pgrx::error;

const BUF_SIZE: usize = 32768;

unsafe extern "C" fn read_cb(opaque: *mut c_void, buf: *mut u8, buf_size: c_int) -> c_int {
    let cursor = &mut *(opaque as *mut Cursor<Vec<u8>>);
    let slice = std::slice::from_raw_parts_mut(buf, buf_size as usize);
    match cursor.read(slice) {
        Ok(0) => ffmpeg_next::sys::AVERROR_EOF,
        Ok(n) => n as c_int,
        Err(_) => ffmpeg_next::sys::AVERROR_EOF,
    }
}

unsafe extern "C" fn seek_cb(opaque: *mut c_void, offset: i64, whence: c_int) -> i64 {
    let cursor = &mut *(opaque as *mut Cursor<Vec<u8>>);
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

unsafe extern "C" fn write_cb(opaque: *mut c_void, data: *mut u8, size: c_int) -> c_int {
    let vec = &mut *(opaque as *mut Vec<u8>);
    vec.extend_from_slice(std::slice::from_raw_parts(data as *const u8, size as usize));
    size
}

/// FFmpeg input context backed by in-memory data.
pub struct MemInput {
    ctx: Option<ffmpeg_next::format::context::Input>,
    avio_ctx: *mut AVIOContext,
    _cursor: Box<Cursor<Vec<u8>>>,
}

impl MemInput {
    pub fn open(data: Vec<u8>) -> Self {
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
                &mut *cursor as *mut Cursor<Vec<u8>> as *mut c_void,
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
            }
        }
    }
}

impl Deref for MemInput {
    type Target = ffmpeg_next::format::context::Input;
    fn deref(&self) -> &Self::Target {
        self.ctx.as_ref().unwrap()
    }
}

impl DerefMut for MemInput {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ctx.as_mut().unwrap()
    }
}

impl Drop for MemInput {
    fn drop(&mut self) {
        // Drop format context first, then free AVIO context
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

/// FFmpeg output context that writes to an in-memory buffer.
pub struct MemOutput {
    ctx: Option<ffmpeg_next::format::context::Output>,
    avio_ctx: *mut AVIOContext,
    #[allow(clippy::box_collection)]
    output_buf: Box<Vec<u8>>,
}

impl MemOutput {
    pub fn open(format: &str) -> Self {
        unsafe {
            let mut output_buf = Box::new(Vec::new());
            let avio_buf = av_malloc(BUF_SIZE) as *mut u8;
            if avio_buf.is_null() {
                error!("failed to allocate AVIO buffer");
            }

            let avio_ctx = avio_alloc_context(
                avio_buf,
                BUF_SIZE as c_int,
                1,
                &mut *output_buf as *mut Vec<u8> as *mut c_void,
                None,
                Some(write_cb),
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
                output_buf,
            }
        }
    }

    /// Consume the output context and return the written data.
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
        std::mem::take(&mut *self.output_buf)
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
