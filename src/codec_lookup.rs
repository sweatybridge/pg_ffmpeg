//! Shared codec/format lookup with the uniform error contract (Task F7).
//!
//! All encoder / decoder / muxer / demuxer resolution in pg_ffmpeg MUST go
//! through this module so users see a single, greppable error message family
//! when a codec or container is missing from the linked FFmpeg build.
//!
//! The direct `ffmpeg::codec::encoder::find_by_name`,
//! `ffmpeg::codec::decoder::find`, and `ffmpeg::format::output::find` calls
//! are banned outside this file by `clippy.toml`'s `disallowed-methods` list.
//! This module applies `#![allow(clippy::disallowed_methods)]` so the bans
//! don't fire on the thin wrappers below.

#![allow(clippy::disallowed_methods)]
// Most helpers here are called only from Milestone 1 / 2 SQL entry
// points. Milestone F ships the primitives and the error contract so
// that later tasks can land without re-opening the API; the dead-code
// lint is expected to fire until the wiring happens.
#![allow(dead_code)]

use ffmpeg_next::codec::{self, Id as CodecId};
use ffmpeg_next::Codec;

/// Which kind of stream an encoder/decoder is expected to handle.
///
/// Used for the user-facing error message and to reject cross-kind requests
/// (for example, passing an audio encoder name to a video parameter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecKind {
    Video,
    Audio,
    Subtitle,
}

impl CodecKind {
    pub fn as_str(self) -> &'static str {
        match self {
            CodecKind::Video => "video",
            CodecKind::Audio => "audio",
            CodecKind::Subtitle => "subtitle",
        }
    }

    fn from_medium(medium: ffmpeg_next::media::Type) -> Option<Self> {
        match medium {
            ffmpeg_next::media::Type::Video => Some(CodecKind::Video),
            ffmpeg_next::media::Type::Audio => Some(CodecKind::Audio),
            ffmpeg_next::media::Type::Subtitle => Some(CodecKind::Subtitle),
            _ => None,
        }
    }
}

/// Uniform error type returned by all lookup helpers.
///
/// The `Display` implementation produces the exact, user-facing message
/// documented in PLAN.md (Task F7). Users are allowed to grep on these
/// strings — do not reword them without updating the plan and the
/// per-module tests.
#[derive(Debug)]
pub enum CodecError {
    /// Encoder name is not compiled into the linked FFmpeg build.
    EncoderNotFound { name: String, kind: CodecKind },
    /// Decoder for a given codec id is not compiled in.
    DecoderNotFound { codec_name: String, id: CodecId },
    /// Container format is not compiled in.
    MuxerNotFound { name: String },
    /// Input bytes couldn't be probed into a known container format.
    DemuxerProbeFailed,
    /// Encoder exists but is the wrong kind (e.g. libmp3lame requested for video).
    WrongKind {
        name: String,
        actual: CodecKind,
        expected: CodecKind,
    },
    /// Encoder opened but FFmpeg returned an error (bad options, etc).
    OpenFailed {
        name: String,
        kind: CodecKind,
        source: ffmpeg_next::Error,
    },
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::EncoderNotFound { name, kind } => write!(
                f,
                "pg_ffmpeg: {} encoder '{}' is not available in this FFmpeg build",
                kind.as_str(),
                name,
            ),
            CodecError::DecoderNotFound { codec_name, id } => write!(
                f,
                "pg_ffmpeg: decoder for codec '{}' (id {:?}) is not available in this FFmpeg build",
                codec_name, id,
            ),
            CodecError::MuxerNotFound { name } => write!(
                f,
                "pg_ffmpeg: container format '{}' is not available in this FFmpeg build",
                name,
            ),
            CodecError::DemuxerProbeFailed => {
                write!(f, "pg_ffmpeg: could not detect input container format")
            }
            CodecError::WrongKind {
                name,
                actual,
                expected,
            } => write!(
                f,
                "pg_ffmpeg: '{}' is a {} encoder, expected {}",
                name,
                actual.as_str(),
                expected.as_str(),
            ),
            CodecError::OpenFailed { name, kind, source } => write!(
                f,
                "pg_ffmpeg: failed to open {} encoder '{}': {}",
                kind.as_str(),
                name,
                source,
            ),
        }
    }
}

impl std::error::Error for CodecError {}

/// Look up an encoder by name, enforcing the expected kind.
///
/// Returns `Err(CodecError::EncoderNotFound)` if the encoder is not linked,
/// or `Err(CodecError::WrongKind)` if the encoder exists but serves a
/// different stream type.
pub fn find_encoder(name: &str, kind: CodecKind) -> Result<Codec, CodecError> {
    let Some(codec) = codec::encoder::find_by_name(name) else {
        return Err(CodecError::EncoderNotFound {
            name: name.to_owned(),
            kind,
        });
    };
    let actual = CodecKind::from_medium(codec.medium()).ok_or_else(|| CodecError::WrongKind {
        name: name.to_owned(),
        actual: kind, // unknown medium — fall back to expected so message is still meaningful
        expected: kind,
    })?;
    if actual != kind {
        return Err(CodecError::WrongKind {
            name: name.to_owned(),
            actual,
            expected: kind,
        });
    }
    Ok(codec)
}

/// Look up a decoder by codec id.
pub fn find_decoder(id: CodecId) -> Result<Codec, CodecError> {
    codec::decoder::find(id).ok_or_else(|| CodecError::DecoderNotFound {
        codec_name: format!("{:?}", id).to_lowercase(),
        id,
    })
}

/// Look up a muxer / container format by short name.
///
/// Note: `ffmpeg_next::format::output::find` exists in some versions of the
/// crate but not all; we fall back to probing via `format::list()` by name.
pub fn find_muxer(name: &str) -> Result<(), CodecError> {
    // The muxer is owned by FFmpeg and found via `avformat_alloc_output_context2`
    // at the time the MemOutput is created; this helper is used by callers that
    // want to validate the name up front. We perform a lightweight probe by
    // attempting to find the output format via the ffmpeg-next `format::list()`
    // iterator if available, otherwise accept the name and let
    // `MemOutput::open` surface the error.
    //
    // In practice every version of ffmpeg-next we support exposes
    // `av_guess_format` via the sys crate, which is what we use here.
    use ffmpeg_next::sys::av_guess_format;
    use std::ffi::CString;
    let c_name = CString::new(name).map_err(|_| CodecError::MuxerNotFound {
        name: name.to_owned(),
    })?;
    let ptr = unsafe { av_guess_format(c_name.as_ptr(), std::ptr::null(), std::ptr::null()) };
    if ptr.is_null() {
        Err(CodecError::MuxerNotFound {
            name: name.to_owned(),
        })
    } else {
        Ok(())
    }
}

/// Probe an input buffer for a recognized demuxer.
///
/// Used by functions that want to validate container detection before
/// committing to the full decode path. Note that for typical pg_ffmpeg
/// functions, `MemInput::open` will surface the same error via the
/// `avformat_open_input` call — this helper is only useful when a function
/// wants to validate probing independently of opening a context.
pub fn probe_demuxer(buf: &[u8]) -> Result<(), CodecError> {
    // Use FFmpeg's `av_probe_input_format` via a scratch AVProbeData.
    use ffmpeg_next::sys::{av_probe_input_format, AVProbeData};
    let probe = AVProbeData {
        filename: std::ptr::null(),
        buf: buf.as_ptr() as *mut u8,
        buf_size: buf.len().min(i32::MAX as usize) as i32,
        mime_type: std::ptr::null(),
    };
    let fmt = unsafe { av_probe_input_format(&probe as *const _ as *mut _, 1) };
    if fmt.is_null() {
        Err(CodecError::DemuxerProbeFailed)
    } else {
        Ok(())
    }
}

/// Tiny helper used by pipeline code: look up an encoder with context to
/// attach an FFmpeg open-time error onto the uniform type.
pub fn open_failed(name: &str, kind: CodecKind, source: ffmpeg_next::Error) -> CodecError {
    CodecError::OpenFailed {
        name: name.to_owned(),
        kind,
        source,
    }
}
