//! Filter-graph allow-list validator (Task F3).
//!
//! FFmpeg's filter DSL is extremely powerful — it can read files (`movie`,
//! `amovie`), open sockets (`zmq`), and execute external commands
//! (`sendcmd`). A user-supplied filter string that reaches FFmpeg
//! unchecked is effectively a remote-code-execution foothold inside the
//! database backend. This module first blocks explicitly-hostile filters
//! before handing the string to FFmpeg, then enforces an allow-list of
//! known-safe filters on the parsed graph.
//!
//! The allow-list is intentionally conservative. Callers that need an
//! exotic filter can set `pg_ffmpeg.unsafe_filters = true` (superuser
//! only); the plan documents this as an escape hatch for testing and
//! advanced operations.

// This module's public surface is consumed by Milestone 1 / 2 SQL
// entry points. Milestone F only ships the primitives, so dead_code is
// expected until the wiring happens.
#![allow(dead_code)]

use std::ffi::CString;
use std::ptr;

use ffmpeg_next::sys::{
    avfilter_graph_alloc, avfilter_graph_free, avfilter_graph_parse2, AVFilterGraph, AVFilterInOut,
};

use crate::limits::{DRAWTEXT_FONT_DIR, UNSAFE_FILTERS};

/// Allow-list of filter names considered safe. See PLAN.md Task F3 for the
/// rationale for each. Adding a filter here is a security decision — it
/// must be reviewed for any file/network access and for any hidden
/// sub-filter invocation it performs.
const ALLOWED_FILTERS: &[&str] = &[
    // Video geometry and color
    "scale",
    "crop",
    "pad",
    "rotate",
    "hflip",
    "vflip",
    "transpose",
    "setpts",
    "fps",
    "format",
    "null",
    "copy",
    "overlay",
    "drawtext",
    "hstack",
    "vstack",
    "palettegen",
    "paletteuse",
    // Audio / visualization
    "showwavespic",
    "showspectrumpic",
    "volume",
    "atempo",
    "aresample",
    "amerge",
    "amix",
    "anull",
    "asetpts",
    "afade",
    "equalizer",
    "loudnorm",
    // Split / concat (safe variants)
    "split",
    "asplit",
    "trim",
    "atrim",
    "concat",
    // Essential helpers automatically inserted by FFmpeg.
    // These can't be exploited on their own but may appear in parsed
    // graphs as buffer sources / sinks — including them avoids false
    // positives during validation.
    "buffer",
    "buffersink",
    "abuffer",
    "abuffersink",
];

/// Names that MUST be rejected even if the allow-list is bypassed by
/// `unsafe_filters` — these are the explicitly-hostile ones where a
/// superuser misconfiguration would still be a disaster.
const ALWAYS_DENIED: &[&str] = &["movie", "amovie", "sendcmd", "zmq", "azmq"];

#[derive(Debug)]
pub enum FilterError {
    /// FFmpeg's parser rejected the string. The graph is malformed.
    ParseFailed { message: String },
    /// A filter name appears that is not on the allow-list.
    FilterNotAllowed { name: String },
    /// A filter name appears that is on the always-denied list.
    FilterDenied { name: String },
    /// `drawtext` was used with `textfile=` or `fontfile=` but
    /// `drawtext_font_dir` is empty, so the file options are disabled.
    DrawtextFileOptionsDisabled,
    /// A `textfile=` / `fontfile=` path resolved outside the allowed
    /// `drawtext_font_dir`, or the file doesn't exist.
    DrawtextPathRejected { path: String },
}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterError::ParseFailed { message } => write!(
                f,
                "pg_ffmpeg: could not parse filter graph: {}",
                message
            ),
            FilterError::FilterNotAllowed { name } => write!(
                f,
                "pg_ffmpeg: filter '{}' is not in the allow-list; set pg_ffmpeg.unsafe_filters=on to bypass (superuser)",
                name,
            ),
            FilterError::FilterDenied { name } => write!(
                f,
                "pg_ffmpeg: filter '{}' is always denied for security reasons",
                name,
            ),
            FilterError::DrawtextFileOptionsDisabled => write!(
                f,
                "pg_ffmpeg: drawtext textfile=/fontfile= options require pg_ffmpeg.drawtext_font_dir to be set",
            ),
            FilterError::DrawtextPathRejected { path } => write!(
                f,
                "pg_ffmpeg: drawtext path '{}' resolves outside pg_ffmpeg.drawtext_font_dir or does not exist",
                path,
            ),
        }
    }
}

impl std::error::Error for FilterError {}

/// Validate a user-supplied filter spec against the allow-list.
///
/// Rejects explicitly-hostile filters before parsing so validation
/// cannot trigger file, network, or command side effects, then parses
/// the remaining spec with FFmpeg's own parser, walks the resulting
/// filter list, and rejects any filter whose name is not on
/// [`ALLOWED_FILTERS`]. The parsed graph is always freed before this
/// function returns, so no state leaks to the caller.
///
/// When `pg_ffmpeg.unsafe_filters = true`, the allow-list is skipped,
/// but the always-denied list ([`ALWAYS_DENIED`]) is still enforced.
pub fn validate_filter_spec(spec: &str) -> Result<(), FilterError> {
    if spec.trim().is_empty() {
        return Ok(());
    }

    if let Some(name) = first_denied_filter_name(spec) {
        return Err(FilterError::FilterDenied { name });
    }

    let c_spec = CString::new(spec).map_err(|_| FilterError::ParseFailed {
        message: "filter spec contains an embedded NUL byte".to_owned(),
    })?;

    unsafe {
        let graph: *mut AVFilterGraph = avfilter_graph_alloc();
        if graph.is_null() {
            return Err(FilterError::ParseFailed {
                message: "avfilter_graph_alloc returned NULL".to_owned(),
            });
        }

        let mut inputs: *mut AVFilterInOut = ptr::null_mut();
        let mut outputs: *mut AVFilterInOut = ptr::null_mut();
        let parse_ret = avfilter_graph_parse2(graph, c_spec.as_ptr(), &mut inputs, &mut outputs);

        // Free the inputs/outputs chains regardless of parse success — we
        // only care about the filter list inside `graph`.
        ffmpeg_next::sys::avfilter_inout_free(&mut inputs);
        ffmpeg_next::sys::avfilter_inout_free(&mut outputs);

        if parse_ret < 0 {
            let mut g = graph;
            avfilter_graph_free(&mut g);
            return Err(FilterError::ParseFailed {
                message: format!("avfilter_graph_parse2 returned {}", parse_ret),
            });
        }

        let result = walk_and_check(graph);

        let mut g = graph;
        avfilter_graph_free(&mut g);

        result
    }
}

fn first_denied_filter_name(spec: &str) -> Option<String> {
    let mut cursor = 0;
    while let Some(name) = next_filter_name(spec, &mut cursor) {
        if ALWAYS_DENIED.contains(&name.as_str()) {
            return Some(name);
        }
    }
    None
}

fn next_filter_name(spec: &str, cursor: &mut usize) -> Option<String> {
    let bytes = spec.as_bytes();
    let len = bytes.len();

    while *cursor < len {
        while *cursor < len && bytes[*cursor].is_ascii_whitespace() {
            *cursor += 1;
        }

        while *cursor < len && bytes[*cursor] == b'[' {
            *cursor += 1;
            while *cursor < len && bytes[*cursor] != b']' {
                if bytes[*cursor] == b'\\' && *cursor + 1 < len {
                    *cursor += 2;
                } else {
                    *cursor += 1;
                }
            }
            if *cursor < len && bytes[*cursor] == b']' {
                *cursor += 1;
            }
            while *cursor < len && bytes[*cursor].is_ascii_whitespace() {
                *cursor += 1;
            }
        }

        if *cursor >= len {
            return None;
        }

        let start = *cursor;
        while *cursor < len {
            let ch = bytes[*cursor];
            if matches!(
                ch,
                b'=' | b',' | b';' | b'[' | b']' | b'@' | b' ' | b'\t' | b'\r' | b'\n'
            ) {
                break;
            }
            *cursor += 1;
        }

        if start == *cursor {
            *cursor += 1;
            continue;
        }

        let name = spec[start..*cursor].to_ascii_lowercase();

        if *cursor < len && bytes[*cursor] == b'@' {
            *cursor += 1;
            while *cursor < len {
                let ch = bytes[*cursor];
                if matches!(
                    ch,
                    b'=' | b',' | b';' | b'[' | b']' | b' ' | b'\t' | b'\r' | b'\n'
                ) {
                    break;
                }
                *cursor += 1;
            }
        }

        let mut in_quote = false;
        let mut escaped = false;
        while *cursor < len {
            let ch = bytes[*cursor];
            if escaped {
                escaped = false;
                *cursor += 1;
                continue;
            }
            match ch {
                b'\\' => {
                    escaped = true;
                    *cursor += 1;
                }
                b'\'' => {
                    in_quote = !in_quote;
                    *cursor += 1;
                }
                b',' | b';' if !in_quote => {
                    *cursor += 1;
                    break;
                }
                _ => *cursor += 1,
            }
        }

        return Some(name);
    }

    None
}

/// Walk an already-parsed filter graph and apply the allow-list / deny-list.
///
/// Safety: `graph` must be a non-null, parsed `AVFilterGraph`. The caller
/// is responsible for freeing it afterwards.
unsafe fn walk_and_check(graph: *mut AVFilterGraph) -> Result<(), FilterError> {
    let allow_all = UNSAFE_FILTERS.get();

    let nb_filters = (*graph).nb_filters as usize;
    let filters_ptr = (*graph).filters;

    for i in 0..nb_filters {
        let ctx = *filters_ptr.add(i);
        if ctx.is_null() {
            continue;
        }
        let f = (*ctx).filter;
        if f.is_null() {
            continue;
        }
        let name_ptr = (*f).name;
        if name_ptr.is_null() {
            continue;
        }
        let name = std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned();

        if ALWAYS_DENIED.contains(&name.as_str()) {
            return Err(FilterError::FilterDenied { name });
        }

        if !allow_all && !ALLOWED_FILTERS.contains(&name.as_str()) {
            return Err(FilterError::FilterNotAllowed { name });
        }

        if name == "drawtext" {
            check_drawtext_options(ctx)?;
        }
    }

    Ok(())
}

/// Inspect a drawtext filter context's options and reject file-based
/// ones unless `drawtext_font_dir` is set and the file resolves inside it.
///
/// Safety: `ctx` must be a valid, initialized `AVFilterContext` pointer
/// for a filter named `drawtext`.
unsafe fn check_drawtext_options(
    ctx: *mut ffmpeg_next::sys::AVFilterContext,
) -> Result<(), FilterError> {
    // The drawtext filter stores its options on the filter's private
    // context. We query `textfile` and `fontfile` via
    // av_opt_get and examine the results.
    for opt_name in ["textfile", "fontfile"] {
        let c_opt = CString::new(opt_name).unwrap();
        let mut out: *mut u8 = ptr::null_mut();
        // AV_OPT_SEARCH_CHILDREN = 1
        let ret = ffmpeg_next::sys::av_opt_get(ctx as *mut _, c_opt.as_ptr(), 1, &mut out);
        if ret < 0 || out.is_null() {
            continue;
        }
        let value = std::ffi::CStr::from_ptr(out as *const _)
            .to_string_lossy()
            .into_owned();
        ffmpeg_next::sys::av_free(out as *mut _);

        if value.is_empty() {
            continue;
        }

        let dir_guc = DRAWTEXT_FONT_DIR.get();
        let allowed_dir = match dir_guc.as_ref() {
            Some(cstr) => cstr.to_string_lossy().into_owned(),
            None => return Err(FilterError::DrawtextFileOptionsDisabled),
        };
        if allowed_dir.is_empty() {
            return Err(FilterError::DrawtextFileOptionsDisabled);
        }

        let allowed_canonical =
            std::fs::canonicalize(&allowed_dir).map_err(|_| FilterError::DrawtextPathRejected {
                path: value.clone(),
            })?;

        let resolved =
            std::fs::canonicalize(&value).map_err(|_| FilterError::DrawtextPathRejected {
                path: value.clone(),
            })?;

        // Strict prefix match with a trailing separator ensures a file
        // named `/foo/barfile` does not accidentally match dir `/foo/bar`.
        let mut dir_with_sep = allowed_canonical.into_os_string();
        dir_with_sep.push(std::path::MAIN_SEPARATOR_STR);
        if !resolved
            .as_os_str()
            .to_string_lossy()
            .starts_with(&*dir_with_sep.to_string_lossy())
        {
            return Err(FilterError::DrawtextPathRejected { path: value });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::first_denied_filter_name;

    #[test]
    fn finds_denied_filter_at_graph_start() {
        assert_eq!(
            first_denied_filter_name("movie=foo.mp4"),
            Some("movie".to_owned())
        );
    }

    #[test]
    fn finds_denied_filter_after_labels_and_instance_name() {
        assert_eq!(
            first_denied_filter_name("[in]amovie@src='/tmp/a.mp3'[aout];volume=0.5"),
            Some("amovie".to_owned())
        );
    }

    #[test]
    fn ignores_denied_names_inside_quoted_arguments() {
        assert_eq!(
            first_denied_filter_name("drawtext=text='movie=foo.mp4',volume=0.5"),
            None
        );
    }
}
