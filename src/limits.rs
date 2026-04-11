//! Memory limit GUCs and enforcement helpers (Task F4).
//!
//! Every public function in pg_ffmpeg feeds user-supplied bytes into FFmpeg
//! in-memory. Without hard caps, a pathological input or an unlucky filter
//! graph can grow the backend's memory footprint until the OOM killer
//! intervenes — which in Postgres means losing unrelated sessions. This
//! module defines the size caps (as GUCs so operators can tune them) and
//! the thin helpers every function call-site uses to enforce them.
//!
//! Scope is deliberately limited to per-single-input and per-single-output
//! caps plus the `concat_agg` state cap. We do NOT add aggregate-sum or
//! row-total caps: they reject legitimate multi-segment workloads, and
//! Postgres' `work_mem` / OOM killer is the ultimate backstop.

// `check_array_size` and `check_aggregate_state` are called from
// Milestone 1 / 2 entry points; Milestone F ships the functions and
// the GUCs so those tasks have a stable target.
#![allow(dead_code)]

use std::ffi::CString;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Maximum size (bytes) of a single `bytea` input accepted by any
/// pg_ffmpeg function. Checked once per call before opening `MemInput`.
/// For `bytea[]` parameters, checked once per element.
pub static MAX_INPUT_BYTES: GucSetting<i32> = GucSetting::<i32>::new(64 * 1024 * 1024);

/// Maximum cumulative bytes any single function call may write into its
/// `MemOutput`. Enforced inside the AVIO write callback so FFmpeg aborts
/// cleanly with `AVERROR` rather than running the backend out of memory.
pub static MAX_OUTPUT_BYTES: GucSetting<i32> = GucSetting::<i32>::new(256 * 1024 * 1024);

/// Maximum number of elements in a `bytea[]` parameter. Prevents a single
/// call from fanning out the per-input cost to thousands of segments.
pub static MAX_INPUTS: GucSetting<i32> = GucSetting::<i32>::new(32);

/// Maximum bytes accumulated in the `concat_agg` aggregate state. If a
/// transition would exceed this, the aggregate errors before appending.
pub static MAX_AGGREGATE_STATE_BYTES: GucSetting<i32> = GucSetting::<i32>::new(512 * 1024 * 1024);

/// When true (superuser only), the filter allow-list in `filter_safety`
/// is bypassed. Used for testing and advanced operations — production
/// deployments should leave this as `false`.
pub static UNSAFE_FILTERS: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Directory whose contents are accessible to `drawtext`'s `fontfile=` and
/// `textfile=` options. Empty (default) disables those options entirely.
/// When non-empty, the referenced paths must canonicalize to a real file
/// strictly inside this directory.
pub static DRAWTEXT_FONT_DIR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Every limit violation surfaces through this enum. Callers propagate it
/// up and `error!` at the SQL boundary with the `Display` message.
#[derive(Debug)]
pub enum LimitError {
    InputTooLarge {
        len: usize,
        max: usize,
    },
    OutputTooLarge {
        written: usize,
        max: usize,
    },
    TooManyInputs {
        len: usize,
        max: usize,
    },
    AggregateStateTooLarge {
        current: usize,
        adding: usize,
        max: usize,
    },
}

impl std::fmt::Display for LimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LimitError::InputTooLarge { len, max } => write!(
                f,
                "pg_ffmpeg: input size {} exceeds pg_ffmpeg.max_input_bytes ({})",
                fmt_bytes(*len),
                fmt_bytes(*max),
            ),
            LimitError::OutputTooLarge { written, max } => write!(
                f,
                "pg_ffmpeg: output size {} exceeds pg_ffmpeg.max_output_bytes ({})",
                fmt_bytes(*written),
                fmt_bytes(*max),
            ),
            LimitError::TooManyInputs { len, max } => write!(
                f,
                "pg_ffmpeg: input array has {} elements, exceeds pg_ffmpeg.max_inputs ({})",
                len, max,
            ),
            LimitError::AggregateStateTooLarge { current, adding, max } => write!(
                f,
                "pg_ffmpeg: concat_agg state size {} + incoming {} exceeds pg_ffmpeg.max_aggregate_state_bytes ({})",
                fmt_bytes(*current),
                fmt_bytes(*adding),
                fmt_bytes(*max),
            ),
        }
    }
}

impl std::error::Error for LimitError {}

fn fmt_bytes(n: usize) -> String {
    const MB: usize = 1024 * 1024;
    const KB: usize = 1024;
    if n >= MB {
        format!("{} MB", n / MB)
    } else if n >= KB {
        format!("{} KB", n / KB)
    } else {
        format!("{} bytes", n)
    }
}

/// Check a single input against `max_input_bytes`. Call at the top of
/// every function before `MemInput::open`; call once per element for
/// `bytea[]` parameters.
pub fn check_input_size(len: usize) -> Result<(), LimitError> {
    let max = MAX_INPUT_BYTES.get() as usize;
    if len > max {
        Err(LimitError::InputTooLarge { len, max })
    } else {
        Ok(())
    }
}

/// Check the cumulative output size so far. This takes the running total
/// from `MemOutput::write_cb`, not the incoming chunk size — the callback
/// is responsible for tracking that total itself.
pub fn check_output_size(cumulative_written: usize) -> Result<(), LimitError> {
    let max = MAX_OUTPUT_BYTES.get() as usize;
    if cumulative_written > max {
        Err(LimitError::OutputTooLarge {
            written: cumulative_written,
            max,
        })
    } else {
        Ok(())
    }
}

/// Check a `bytea[]` parameter's length against `max_inputs`.
pub fn check_array_size(n: usize) -> Result<(), LimitError> {
    let max = MAX_INPUTS.get() as usize;
    if n > max {
        Err(LimitError::TooManyInputs { len: n, max })
    } else {
        Ok(())
    }
}

/// Check that appending `adding` bytes to a `concat_agg` state currently
/// holding `current` bytes would not exceed `max_aggregate_state_bytes`.
pub fn check_aggregate_state(current: usize, adding: usize) -> Result<(), LimitError> {
    let max = MAX_AGGREGATE_STATE_BYTES.get() as usize;
    if current.saturating_add(adding) > max {
        Err(LimitError::AggregateStateTooLarge {
            current,
            adding,
            max,
        })
    } else {
        Ok(())
    }
}

/// Register all GUCs with the Postgres GUC machinery. Called from
/// `_PG_init` via `lib.rs`.
pub fn register_gucs() {
    GucRegistry::define_int_guc(
        c"pg_ffmpeg.max_input_bytes",
        c"Maximum size in bytes for a single bytea input.",
        c"Functions ERROR when a bytea argument is larger than this. Applies per element for bytea[] arguments.",
        &MAX_INPUT_BYTES,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_ffmpeg.max_output_bytes",
        c"Maximum cumulative bytes written by a single function call.",
        c"Enforced inside the FFmpeg AVIO write callback; exceeding this aborts the call with an ERROR.",
        &MAX_OUTPUT_BYTES,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_ffmpeg.max_inputs",
        c"Maximum number of elements allowed in a bytea[] argument.",
        c"Functions that accept bytea[] ERROR when the array has more elements than this.",
        &MAX_INPUTS,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_ffmpeg.max_aggregate_state_bytes",
        c"Maximum bytes accumulated in the concat_agg aggregate state.",
        c"concat_agg ERRORs before the transition function appends a value that would push state size past this cap.",
        &MAX_AGGREGATE_STATE_BYTES,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_ffmpeg.unsafe_filters",
        c"Bypass the filter allow-list (superuser only).",
        c"When true, filter strings passed to transcode/extract_audio/filter_complex are not checked against the safe allow-list.",
        &UNSAFE_FILTERS,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_ffmpeg.drawtext_font_dir",
        c"Directory allowed as a source for drawtext textfile= / fontfile= paths.",
        c"Empty disables drawtext file options entirely. When set, referenced paths must canonicalize to a file strictly inside this directory.",
        &DRAWTEXT_FONT_DIR,
        GucContext::Sighup,
        GucFlags::default(),
    );
}
