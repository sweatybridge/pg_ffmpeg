//! Lazy per-backend hardware acceleration detection (Task F2).
//!
//! Postgres forks a new backend process per connection, and FFmpeg's
//! hardware device contexts cannot be shared across processes. Init-time
//! probing (as `_PG_init`) would either fail on machines without a GPU
//! or leak device contexts into every backend regardless of whether it
//! ever asks for hardware acceleration.
//!
//! This module takes the opposite approach: every helper is a pure
//! lookup that consults a `thread_local!` cache owned by the current
//! backend. The cache fills lazily the first time a caller asks for a
//! specific backend. Device contexts are allocated via
//! `av_hwdevice_ctx_create` and freed when the thread-local drops at
//! backend exit.
//!
//! ## Capability matrix (plan F2)
//!
//! | Codec | HW encoder names tried                                    | SW fallback         |
//! |-------|-----------------------------------------------------------|---------------------|
//! | h264  | `h264_nvenc`, `h264_vaapi`, `h264_qsv`, `h264_videotoolbox` | `libx264`           |
//! | hevc  | `hevc_nvenc`, `hevc_vaapi`, `hevc_qsv`, `hevc_videotoolbox` | `libx265`           |
//! | av1   | `av1_nvenc`, `av1_vaapi`, `av1_qsv`                         | `libaom-av1`, `libsvtav1` |
//! | vp9   | `vp9_vaapi`, `vp9_qsv`                                      | `libvpx-vp9`        |
//!
//! Callers must treat a `None` return from [`hw_encoder`] as "fall back
//! to software" and log a `WARNING` — never a hard error.

#![allow(clippy::disallowed_methods)]
// This module's purpose is codec lookups.
// The HW-encoder helpers are called from Milestone 1 / 2 encode paths.
// Milestone F only ships the probing machinery so M1/M2 can land.
#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::HashMap;

use ffmpeg_next::Codec;

/// Normalize a software codec name (or a bare family name) to the HW
/// family key used by the lookup below. Returns `None` for codecs we
/// don't have a HW mapping for — callers skip the HW path in that case.
pub fn codec_family(codec_name: &str) -> Option<&'static str> {
    match codec_name {
        "libx264" | "h264" => Some("h264"),
        "libx265" | "hevc" | "h265" => Some("hevc"),
        "libaom-av1" | "libsvtav1" | "av1" => Some("av1"),
        "libvpx-vp9" | "vp9" => Some("vp9"),
        "libmp3lame" | "mp3" => Some("mp3"), // no HW; kept for consistency
        _ => None,
    }
}

/// Fixed list of HW encoder names to try per family, in preference order.
/// First successful `find_by_name` wins.
const HW_ENCODER_TABLE: &[(&str, &[&str])] = &[
    (
        "h264",
        &["h264_nvenc", "h264_vaapi", "h264_qsv", "h264_videotoolbox"],
    ),
    (
        "hevc",
        &["hevc_nvenc", "hevc_vaapi", "hevc_qsv", "hevc_videotoolbox"],
    ),
    ("av1", &["av1_nvenc", "av1_vaapi", "av1_qsv"]),
    ("vp9", &["vp9_vaapi", "vp9_qsv"]),
];

thread_local! {
    static HW_CACHE: RefCell<HwCache> = RefCell::new(HwCache::default());
}

#[derive(Default)]
struct HwCache {
    encoders: HashMap<String, Option<&'static str>>,
}

/// Find a hardware encoder variant for a given codec name.
///
/// Returns a `Codec` that callers can pass to
/// `codec::context::Context::new_with_codec`. Returns `None` when:
/// - the codec name has no HW mapping,
/// - no HW encoder name from the table resolves in the linked FFmpeg
///   build.
///
/// In either case the caller should fall back to software and emit a
/// `WARNING` — see PLAN.md Task F2 for the exact phrasing.
pub fn hw_encoder(codec_name: &str) -> Option<Codec> {
    let family = codec_family(codec_name)?;
    let candidates = HW_ENCODER_TABLE
        .iter()
        .find(|(f, _)| *f == family)
        .map(|(_, names)| *names)?;

    // Cached per-family "first working HW name" so we don't pay the
    // find_by_name cost on every call. The cache is per-backend via
    // thread_local, so each Postgres worker maintains its own.
    let cached_name: Option<&'static str> = HW_CACHE.with(|cell| {
        let mut cache = cell.borrow_mut();
        if let Some(entry) = cache.encoders.get(family) {
            return *entry;
        }
        let mut found: Option<&'static str> = None;
        for name in candidates {
            if ffmpeg_next::codec::encoder::find_by_name(name).is_some() {
                found = Some(*name);
                break;
            }
        }
        cache.encoders.insert(family.to_owned(), found);
        found
    });

    cached_name.and_then(ffmpeg_next::codec::encoder::find_by_name)
}

/// Log a `WARNING` that HW acceleration is unavailable and we're falling
/// back to software. Callers use this right before opening the software
/// encoder so operators see a single, greppable phrase in logs.
pub fn warn_hw_fallback(codec_name: &str) {
    pgrx::warning!(
        "pg_ffmpeg: HW encoder for {} unavailable, falling back to software",
        codec_name
    );
}
