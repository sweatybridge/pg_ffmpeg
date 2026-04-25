# Complete pg_ffmpeg: Full FFmpeg Capabilities in PostgreSQL

## Context

pg_ffmpeg is a Rust/pgrx PostgreSQL extension that wraps FFmpeg for in-memory media processing. Today it has 5 functions covering metadata extraction, thumbnails, basic transcoding (with video filters), audio extraction (stream copy only), and HLS segmentation. The goal is to expand this into a comprehensive FFmpeg-in-Postgres surface — including hardware-accelerated encoding/decoding — while keeping the SQL API clean, composable, and safe.

### Design Philosophy

- Expose **a small number of powerful, composable functions** with well-chosen parameters
- Lean on FFmpeg's filter graph DSL rather than reinventing it
- Use SQL's `SELECT`/`FROM`/`JOIN`/aggregates for orchestration
- Multi-input operations use `bytea[]` arrays
- **Hardware acceleration** (VAAPI, NVENC, QSV, VideoToolbox) transparent fallback to software
- **Security by default** — reject filters that touch the filesystem or network

### Decisions

- **Scope**: Full feature set delivered across 3 milestones (Foundation → Core APIs → Advanced/Multi-input)
- **Multi-input approach**: `bytea[]` arrays (all in-memory)
- **Hardware acceleration**: Lazy per-backend detection with cache, automatic software fallback
- **Memory safety**: Hard limits on input/output sizes with explicit `ERROR` responses when exceeded

### Input Size Assumption

**Every function assumes `bytea` inputs are on the order of a few MB — roughly the size of a single HLS segment (2–10 MB).** This shapes the design throughout:

- All processing happens in-memory via `MemInput`/`MemOutput`; no spill to disk
- No streaming/chunked interfaces — one call processes one segment
- For full videos, callers pre-segment via `hls()` and process segments in parallel: `SELECT ffmpeg.transcode(data) FROM hls_segments`
- **Hard limits enforced** (configurable via GUCs, see Task F4):
  - `pg_ffmpeg.max_input_bytes` — default 64 MB per bytea input, ERROR if exceeded
  - `pg_ffmpeg.max_output_bytes` — default 256 MB per function call, ERROR if exceeded
  - `pg_ffmpeg.max_inputs` — default 32 elements in a `bytea[]` array
  - `pg_ffmpeg.max_aggregate_state_bytes` — default 512 MB for `concat_agg` state
  - Set-returning and text-returning functions (`extract_frames`, `extract_subtitles`) rely on `max_input_bytes` and function-specific caps (e.g., `extract_frames.max_frames`) rather than aggregate output GUCs — Postgres' `work_mem` / OOM killer is the ultimate backstop.
- Hardware acceleration paths inherit the same assumption — HW upload/download cost per call is amortized over a segment, not a full movie

### Zero-Copy Discipline

Inputs are small **per call**, but many concurrent backends each holding multi-MB copies compounds fast. Every module must minimise buffer copying. The target is **one copy per input byte** (pgrx bytea → AVIO `read_cb` → FFmpeg internals) and **one copy per output byte** (FFmpeg `write_cb` → return `bytea`).

Rules enforced across the codebase:

- **`MemInput` borrows, not owns**: the current `MemInput::open(data: Vec<u8>)` signature forces a copy from the pgrx bytea slice into an owned `Vec<u8>`. Task F1 rewrites it to `MemInput::open<'a>(data: &'a [u8]) -> MemInput<'a>` backed by `Cursor<&'a [u8]>`. Callers pass the pgrx bytea slice directly — no `.to_vec()` / `.to_owned()` at the call site.
- **`bytea[]` parameters borrow each element**: `filter_complex`, `overlay`, `concat`, and `encode` iterate the array and pass `&[u8]` slices to `MemInput`. Never materialise a `Vec<Vec<u8>>`.
- **`MemOutput` is already single-buffer**: it writes into one `Box<Vec<u8>>` that grows, then `into_data()` moves the `Vec` out via `std::mem::take`. Do not introduce intermediate staging buffers.
- **Frames move, not clone**: pipeline stages pass `ffmpeg::frame::Video` / `frame::Audio` by move through decode→filter→encode. No `.clone()` on frames unless there's a split filter that genuinely forks the stream.
- **`concat_agg` state owns its chunks** (pgrx aggregate state lifetime requires it), but each call to `accum` appends the new `bytea` once into a `Vec<Box<[u8]>>` (see Task 2F for the full state type), never re-allocates existing chunks, and frees chunks progressively as the muxer consumes them at finalize time.
- **Boundary test**: every new function ships with a test that asserts the number of large allocations is O(1) with respect to input size. Concretely: for each function, run the same operation on a small input (e.g., 1 MB) and a large input (e.g., 8 MB), capture the count of allocations ≥ 256 KiB via the `CountingAllocator` helper (F6), and assert the counts are **equal** (not "within N"). FFmpeg's internal buffering is input-size-independent for a given codec/container combination at this size range, so the counts should match exactly; if they don't, the regression is either a real leak or a legitimate codec-dependent buffer growth that must be explained in the test comment. The allocation size threshold (256 KiB) is tuned to ignore small bookkeeping allocations from FFmpeg's struct churn. CI runs the boundary tests as gating; if a specific codec turns out to be genuinely variable, its boundary test can be marked `#[ignore]` with a comment linking to the reason, but the default is strict equality.

This discipline applies even though per-call inputs are small — concurrency × unnecessary copies is the failure mode that motivates it.

---

## What Exists Today

| Function | File | What it does |
|---|---|---|
| `media_info(bytea) → jsonb` | `src/media_info.rs` | Format, duration, bitrate, per-stream metadata |
| `thumbnail(bytea, float8, text) → bytea` | `src/thumbnail.rs` | Extract single frame as image |
| `transcode(bytea, text, text) → bytea` | `src/transcode.rs` | Remux or decode→filter→encode (video only, same codec) |
| `extract_audio(bytea, text) → bytea` | `src/extract_audio.rs` | Stream-copy audio track (no re-encoding) |
| `hls(text, int) → bigint` | `src/hls.rs` | URL→HLS segments stored in DB |

Supporting infra: `mem_io.rs` (MemInput/MemOutput with AVIO callbacks), HLS tables.

---

## Milestone Structure

The work is organized into **3 sequential milestones** with hard gates between them. Tasks within a milestone can run in parallel, but no milestone starts until the previous one's gate passes.

```
Milestone F (Foundation)  →  Milestone 1 (Core APIs)  →  Milestone 2 (Advanced)
  shared helpers, limits,       enhanced transcode,        generate_gif, waveform,
  HW detection, filter          enhanced extract_audio,    extract_subtitles, overlay,
  allow-list, test utils,       trim, extract_frames,      filter_complex,
  CI gates                      enhanced media_info        concat, encode
```

**Why this order** (per review feedback): the original "5 parallel workstreams" claim was optimistic. The shared `Transcoder` struct, filter graph helpers, HW detection, and memory-limit enforcement are cross-cutting dependencies. Building them once in a foundation milestone prevents duplicated work and API drift.

This is a greenfield project with no external users to protect. **Breaking changes to existing SQL signatures are allowed and encouraged when they simplify the design.** No deprecation windows, no backward-compat shims, no staged feature flags beyond what's needed for real optional dependencies (e.g. GPU-only tests).

---

## Milestone F: Foundation

**Gate to advance**: All foundation modules compile, unit tests pass, CI green on PG 16/17/18.

**Parallelism**: F tasks are mostly parallelizable but have real coupling:
- **F1 must land first or merge first** — it owns the breaking `MemInput` API change that every other module imports.
- **F1b depends on F1** — `MultiInputGraph` lives in the same file (`src/pipeline.rs`) and reuses F1's frame-handling helpers.
- **F4 partially depends on F1** — the `MemOutput::write_cb` hook for `check_output_size` needs F1's refactored `MemOutput` signatures to land first so the hook site is stable.
- **F3, F5, F6, F7** are independently parallelizable against each other once F1 is merged.

In practice: one person/agent lands F1, then the rest fan out. Do not try to merge F1b, F4's `MemOutput` hook, or any F1-consuming module before F1 itself.

### Task F1: Shared transcoder/filter helpers (`src/pipeline.rs`)

Extract reusable building blocks from `src/transcode.rs` into a new module:

- `VideoPipeline` — generalized decode→filter→encode for a **single video stream**. Takes input stream, optional filter spec, encoder codec (name or ID), encoder options (CRF/preset/bitrate), and output context.
- `AudioPipeline` — same pattern for audio (uses `abuffer`/`abuffersink` sinks, handles `channel_layout`, `sample_fmt`, `sample_rate`).
- `build_video_filter_graph(spec, decoder_params)` — helper that assembles `buffer → spec → buffersink`, returns the graph plus resolved output (w, h, pix_fmt, time_base).
- `build_audio_filter_graph(spec, decoder_params)` — same for audio.
- `copy_stream(ictx, octx, index)` — helper for stream-copy path.

Current `Transcoder` in `src/transcode.rs` becomes a thin wrapper around `VideoPipeline`. Mostly a refactor, except for the one behavior change in the bullet below.

**Breaking `MemInput` API change (part of F1)**: rewrite `src/mem_io.rs::MemInput::open` from

```rust
pub fn open(data: Vec<u8>) -> Self            // current — takes ownership, forces a copy
```

to

```rust
pub fn open<'a>(data: &'a [u8]) -> MemInput<'a>   // borrow the pgrx bytea slice directly
```

The internal `Cursor<Vec<u8>>` becomes `Cursor<&'a [u8]>`. The AVIO opaque pointer's lifetime is tied to the borrowed slice via the `MemInput<'a>` lifetime parameter. All existing callers (`transcode`, `thumbnail`, `extract_audio`, `media_info`, `hls`) are updated in the same PR to pass `data.as_slice()` (or equivalent) from the pgrx bytea argument — no `.to_vec()`. This is the single biggest lever for the Zero-Copy Discipline and must land in Milestone F before any module that accepts `bytea[]` is built.

### Task F1b: Multi-input pipeline plumbing (`src/pipeline.rs`)

**Separated from F1 per review feedback.** `overlay`, `filter_complex`, and `encode` (image sequence) all need to feed **multiple input streams into a single filter graph**. The single-stream `VideoPipeline`/`AudioPipeline` APIs from F1 do not cover this.

Add to `src/pipeline.rs`:

- `MultiInputGraph` — struct that owns:
  - A `filter::Graph`
  - A `Vec<(source_label, BufferSourceRef)>` — one buffer/abuffer source per user input stream
  - A `Vec<(sink_label, BufferSinkRef)>` — one buffersink/abuffersink per output pad
- `MultiInputGraph::builder()` — fluent API:
  - `.add_video_input(label, width, height, pix_fmt, time_base, sample_aspect_ratio)`
  - `.add_audio_input(label, sample_rate, sample_fmt, channel_layout, time_base)`
  - `.add_video_output(label)` → adds `buffersink`
  - `.add_audio_output(label)` → adds `abuffersink`
  - `.parse(filter_spec)` → calls `avfilter_graph_parse_ptr` wiring pre-declared sources/sinks to the spec's pads
  - `.build()` → validates and returns `MultiInputGraph`
- `MultiInputGraph::push_video_frame(input_index, frame)` / `push_audio_frame(...)` — feed a decoded frame into the N-th source
- `MultiInputGraph::try_recv_video(output_index)` / `try_recv_audio(...)` — pull a filtered frame from the N-th sink
- `MultiInputGraph::flush(input_index)` — signal EOF on one source (overlay handles uneven input lengths this way)

This is a focused primitive — it does NOT own decoders or encoders. Callers (`overlay`, `filter_complex`) own their decoders and encoders and use `MultiInputGraph` just for the graph wiring.

`overlay` (Task 2D) and `filter_complex` (Task 2E) depend on this. `encode` (Task 2G) does NOT — an image sequence is a single video input to a single encoder, so it uses `VideoPipeline` from F1 directly with a custom input loop that synthesizes frames from decoded images.

### Task F2: Lazy hardware acceleration module (`src/hwaccel.rs`)

Per review feedback, **probe HW lazily per backend** (Postgres forks processes; init-time probing is wrong):

- `fn codec_family(codec_name: &str) -> Option<&'static str>` — normalizes SW codec names to HW family keys:
  - `libx264` → `h264`
  - `libx265` → `hevc`
  - `libaom-av1`, `libsvtav1` → `av1`
  - `libvpx-vp9` → `vp9`
  - `libmp3lame` → `mp3` (audio; no HW but used for consistent lookup)
  - Bare codec names (`h264`, `hevc`, ...) map to themselves
  - Returns `None` for unknown codecs (HW path skipped)
- `fn hw_encoder(codec_name: &str) -> Option<&'static ffmpeg::Codec>` — returns a HW encoder variant if available, else `None`. Flow:
  1. Look up `family = codec_family(codec_name)` (e.g. `libx264` → `h264`)
  2. Try encoder names in order: `{family}_nvenc`, `{family}_vaapi`, `{family}_qsv`, `{family}_videotoolbox`
  3. First successful `avcodec_find_encoder_by_name` wins
- `fn hw_decoder(codec_id: CodecId) -> Option<&'static ffmpeg::Codec>` — same pattern, keyed on codec ID since decoders are found from input stream.
- `fn hw_device_for(codec: &Codec) -> Option<HwDeviceRef>` — returns a device context, creating it on first use per backend.
- `thread_local! { static HW_CACHE: RefCell<HwCache> }` — per-backend cache. Each Postgres worker maintains its own cache; no cross-process state.
- `HwCache` stores: `probed: bool`, `available_types: Vec<AvHwDeviceType>`, `device_contexts: HashMap<AvHwDeviceType, AvBufferRef>`.
- Device contexts are created via `av_hwdevice_ctx_create` on first request and freed when the backend exits (via `ProcessExitShmemExit` or `atexit`).
- **Fallback semantics**: if HW encoder open fails (device busy, driver mismatch, unsupported profile), log `WARNING` and fall back to software encoder. Never hard-error on HW unavailability.
- **Capability matrix** (documented in module header comment):

  | Codec | HW encoder names tried | SW fallback |
  |-------|------------------------|-------------|
  | h264  | `h264_nvenc`, `h264_vaapi`, `h264_qsv`, `h264_videotoolbox` | `libx264` |
  | hevc  | `hevc_nvenc`, `hevc_vaapi`, `hevc_qsv`, `hevc_videotoolbox` | `libx265` |
  | av1   | `av1_nvenc`, `av1_vaapi`, `av1_qsv` | `libaom-av1`, `libsvtav1` |
  | vp9   | `vp9_vaapi`, `vp9_qsv` | `libvpx-vp9` |

### Task F3: Safe filter validation (`src/filter_safety.rs`)

Per review feedback, arbitrary filter graph strings are a security risk. FFmpeg filters like `movie`, `amovie`, `lavfi/concat` (with file path), and `sendcmd` can read files or open URLs. This module enforces an allow-list.

**Parser strategy — mandatory**: validation MUST use FFmpeg's own graph parser (`avfilter_graph_parse2` / `avfilter_graph_parse_ptr`) followed by walking the resulting `AVFilterGraph`'s `filters` array and checking each `filter->filter->name` against the allow-list. **Regex/string splitting is explicitly forbidden** — filter specs contain nested expressions, escaped characters, and option strings that cannot be reliably tokenized by hand, and a regex-based check has already been the source of security bugs in similar projects.

- `fn validate_filter_spec(spec: &str) -> Result<(), FilterError>` — parses the filter spec via `avfilter_graph_parse2`, walks the resulting node list, checks each filter name + options against the allow-list, then frees the graph. Returns `Err` on any deny-list hit or parse failure.
- **Allow-list**: common safe filters — `scale`, `crop`, `pad`, `rotate`, `hflip`, `vflip`, `transpose`, `setpts`, `fps`, `format`, `null`, `copy`, `overlay`, `drawtext` (with restrictions on `textfile=`), `hstack`, `vstack`, `palettegen`, `paletteuse`, `showwavespic`, `showspectrumpic`, `volume`, `atempo`, `aresample`, `amerge`, `amix`, `anull`, `afade`, `equalizer`, `loudnorm`, `split`, `asplit`, `trim`, `atrim`, `concat` (with `n=` only, no file mode).
- **Denylist (rejected outright)**: `movie`, `amovie`, `sendcmd`, `zmq`, `azmq`, any filter matching `*_lavfi` with file paths.
- **`drawtext` restrictions**: `text=` literal is always allowed. `textfile=` and `fontfile=` options are governed by the `pg_ffmpeg.drawtext_font_dir` GUC:
  - **Empty** (default): `textfile=` / `fontfile=` options cause `drawtext` to be rejected entirely. Any user relying on them must explicitly configure the font directory.
  - **Non-empty**: the referenced path is:
    1. Resolved to an absolute path via `std::fs::canonicalize` (this follows symlinks to their real target)
    2. Compared against the canonicalized `drawtext_font_dir`
    3. Rejected unless the resolved path is strictly inside the directory (prefix match with trailing path separator)
  - Symlink traversal OUT of the directory is blocked by step 1 (canonicalization dereferences the symlink before the prefix check). A symlink pointing to `/etc/passwd` would canonicalize to `/etc/passwd`, which fails the prefix check.
  - Non-existent files ERROR immediately with a clear message — we never let FFmpeg silently fail-open.
- GUC `pg_ffmpeg.unsafe_filters = false` (default) — when set to `true` by a superuser, bypasses the allow-list. Used for testing/advanced ops only.
- **Only called on user-supplied filter strings.** Specifically:
  - `transcode(filter, audio_filter)` — both user-supplied → validated
  - `extract_audio(filter)` — user-supplied → validated
  - `filter_complex(filter_graph)` — user-supplied → validated
  - `generate_gif`, `waveform`, `overlay` — filter spec is **built-in and hardcoded** from function parameters (fps, width, x, y, etc.); NOT passed through the validator because no user filter string exists. The parameter values are numeric/bounded, not filter DSL.

### Task F4: Memory limit GUCs and enforcement (`src/limits.rs`)

**Scope**: only per-single-input and per-single-output caps plus the `concat_agg` state cap. No aggregate-sum, row-total, or text-total GUCs — those reject legitimate workloads (e.g., concatenating 32 HLS segments) and Postgres `work_mem`/OOM is the ultimate backstop.

- Register GUCs via pgrx `GucRegistry`:
  - `pg_ffmpeg.max_input_bytes` (int, default `64 * 1024 * 1024`)
  - `pg_ffmpeg.max_output_bytes` (int, default `256 * 1024 * 1024`)
  - `pg_ffmpeg.max_inputs` (int, default `32`)
  - `pg_ffmpeg.max_aggregate_state_bytes` (int, default `512 * 1024 * 1024`)
  - `pg_ffmpeg.unsafe_filters` (bool, default `false`)
  - `pg_ffmpeg.drawtext_font_dir` (text, default empty)
- `fn check_input_size(len: usize) -> Result<(), LimitError>` — called at the top of every function before `MemInput::open`. For `bytea[]` parameters, called once per element.
- `fn check_output_size(cumulative_written: usize) -> Result<(), LimitError>` — takes the **cumulative total bytes written so far** (not the incoming chunk size). Hooked into `MemOutput::write_cb`, which tracks a running total in the opaque state and passes it to this helper on every callback invocation. The AVIO callback returns `AVERROR` when the check fails so FFmpeg aborts cleanly.
- `fn check_array_size(n: usize) -> Result<(), LimitError>` — enforces `max_inputs` on `bytea[]` length.
- `fn check_aggregate_state(current: usize, adding: usize) -> Result<(), LimitError>` — used by `concat_agg`.
- Exceeding a limit raises `ERROR` with a clear message: `"pg_ffmpeg: input size 128 MB exceeds pg_ffmpeg.max_input_bytes (64 MB)"`.
- All limit checks return `Result` with consistent error plumbing so every call site uses the same `?`-propagation pattern.

### Task F5: CI gates for HW-dependent tests

- Declare the `hwaccel_tests` feature in `Cargo.toml` under `[features]` (empty feature — it only acts as a compile-time flag for `#[cfg(feature = "hwaccel_tests")]`).
- Add `#[cfg(feature = "hwaccel_tests")]` gate for any test that actually requires a GPU.
- Default `cargo pgrx test` runs do NOT exercise HW encoders (only the fallback path).
- Add a separate CI job `test-hwaccel` that runs only on a self-hosted GPU runner (future — gate optional).
- Document in `README.md` how to run the HW test suite locally.

### Task F6: Shared test utilities (`src/test_utils.rs`)

- Move `generate_test_video_bytes()` from `src/transcode.rs` tests.
- Add `generate_test_video_with_audio_bytes()` — MPEG-TS with both video and AAC audio stream (needed for Tasks 1B, 2E tests).
- Add `generate_test_image_bytes(format, width, height)` — PNG/JPEG generation.
- Add `CountingAllocator` — a `#[global_allocator]` wrapper (test-only) that counts allocations at or above **256 KiB** during a scoped block (matching the Zero-Copy Discipline threshold). Exposed as `assert_large_allocs_at_most(n, || { ... })`. Used by every module's "zero-copy boundary" test to assert that processing an N-byte input triggers O(1) large allocations rather than O(N) or O(inputs * N).
- Gated behind `#[cfg(any(test, feature = "pg_test"))]`.

### Task F7: Codec/format availability error contract (`src/codec_lookup.rs`)

**Per review feedback**: every function that selects a codec or container must produce the same error format when the requested name is unavailable in the linked FFmpeg build. Without a shared helper, each function invents its own message, and users can't tell "typo" from "not compiled in" from "wrong type."

All codec and container lookups MUST go through this module. Direct calls to `codec::encoder::find_by_name` / `format::output::find` are forbidden outside of `codec_lookup.rs` (enforced by a clippy deny-list in X4).

**API**:

- `fn find_encoder(name: &str, kind: CodecKind) -> Result<&'static ffmpeg::Codec, CodecError>`
  - `CodecKind` is `Video` | `Audio` | `Subtitle`. Used for the error message and to reject a subtitle codec name passed to a video parameter.
- `fn find_decoder(id: ffmpeg::codec::Id) -> Result<&'static ffmpeg::Codec, CodecError>`
- `fn find_muxer(name: &str) -> Result<&'static ffmpeg::format::Output, CodecError>`
- `fn find_demuxer_probe(buf: &[u8]) -> Result<&'static ffmpeg::format::Input, CodecError>`

**Error contract** — exact message format (users grep on these):

| Condition | SQLSTATE | Message template |
|-----------|----------|------------------|
| Encoder name not found | `feature_not_supported` (0A000) | `pg_ffmpeg: {kind} encoder '{name}' is not available in this FFmpeg build` |
| Decoder for codec id not found | `feature_not_supported` (0A000) | `pg_ffmpeg: decoder for codec '{codec_name}' (id {id}) is not available in this FFmpeg build` |
| Muxer/container name not found | `feature_not_supported` (0A000) | `pg_ffmpeg: container format '{name}' is not available in this FFmpeg build` |
| Demuxer probe failed | `invalid_parameter_value` (22023) | `pg_ffmpeg: could not detect input container format` |
| Encoder exists but wrong kind | `invalid_parameter_value` (22023) | `pg_ffmpeg: '{name}' is a {actual_kind} encoder, expected {expected_kind}` |
| Encoder open failed (codec options invalid) | `invalid_parameter_value` (22023) | `pg_ffmpeg: failed to open {kind} encoder '{name}': {ffmpeg_error}` |

Rules every caller follows:
- Always translate via the helper and propagate `CodecError` — never craft a custom message at the call site.
- HW-encoder fallback (Task F2) uses `find_encoder` for both the HW and SW lookups; when HW fails open, the `WARNING` message is the HW-specific one, then the SW error (if any) comes from this helper unchanged.
- `CodecError::from_ffmpeg(err, context)` attaches the FFmpeg numeric error (`AVERROR`) and the `av_strerror` string so users can correlate with FFmpeg documentation.

**Functions that MUST use this helper** (complete list — reviewed at every milestone gate): `transcode`, `extract_audio`, `trim` (precise), `generate_gif`, `waveform`, `extract_subtitles`, `overlay`, `filter_complex`, `concat`, `encode`, `thumbnail`, `extract_frames`, `media_info`.

Tests:
- `test_find_encoder_unknown_name` — verify exact error message
- `test_find_encoder_wrong_kind` — `libmp3lame` requested as video encoder
- `test_find_muxer_unknown` — `format => 'not_a_real_format'` produces the contract message
- `test_codec_error_sqlstate` — SQL-level test that catches `SQLSTATE '0A000'` for an unknown codec

**Milestone F Deliverables**: 10 files touched, all existing tests still pass. User-facing SQL signatures for the 5 current functions are unchanged. New user-visible surface added in this milestone is limited to the GUCs from Task F4 (`pg_ffmpeg.max_input_bytes`, etc.) and the error message format from Task F7 — no new SQL functions ship in F.

| File | Action |
|------|--------|
| `src/pipeline.rs` | **New** (F1) |
| `src/hwaccel.rs` | **New** (F2) |
| `src/filter_safety.rs` | **New** (F3) |
| `src/limits.rs` | **New** (F4) |
| `src/test_utils.rs` | **New** (F6) |
| `src/codec_lookup.rs` | **New** (F7) |
| `src/transcode.rs` | Modify — refactor `Transcoder` onto `VideoPipeline` (F1), route codec lookups through F7 |
| `src/mem_io.rs` | Modify — hook `write_cb` to call `check_output_size()` (F4) |
| `src/lib.rs` | Modify — declare new modules, register GUCs |
| `.github/workflows/test.yml` | Modify — add `hwaccel_tests` feature flag handling (F5) |

---

## Milestone 1: Core APIs

**Gate to advance**: All Milestone 1 functions have passing tests + benchmarks, README documents them, CI green.
**Parallelism**: Tasks 1A–1E can run in parallel (they depend only on Foundation, not on each other).

### Task 1A: Enhanced `transcode` (`src/transcode.rs`)

New signature (replaces the current 3-arg version; greenfield — breaking change is fine):

```sql
transcode(
    data          bytea,
    format        text     DEFAULT NULL,
    filter        text     DEFAULT NULL,
    codec         text     DEFAULT NULL,   -- 'libx264', 'libx265', 'libvpx-vp9', ...
    preset        text     DEFAULT NULL,
    crf           int      DEFAULT NULL,
    bitrate       int      DEFAULT NULL,   -- video bitrate in bits/sec
    audio_codec   text     DEFAULT NULL,
    audio_filter  text     DEFAULT NULL,
    audio_bitrate int      DEFAULT NULL,
    hwaccel       bool     DEFAULT false   -- opt-in; see below
) → bytea
```

Implementation uses `VideoPipeline` + `AudioPipeline` from Task F1. Codec lookup via `codec_lookup::find_encoder()` (F7) when `codec` is provided. `filter`/`audio_filter` validated via `validate_filter_spec()` (F3).

**HW acceleration policy**:
- Default `hwaccel = false` (opt-in). HW paths have surprising format-compatibility failures; safer to make the opt-in explicit.
- When `hwaccel = true`: try HW encoder; on open failure, log `WARNING "pg_ffmpeg: HW encoder {name} unavailable, falling back to software"` and use software encoder.
- HW+filter interaction: if filter produces SW frames but encoder expects HW frames, insert an `hwupload` filter automatically. Document this in module comment.

**Zero-copy remux path** (no filter/codec/audio params) is the default when those params are NULL.

Tests:
- `test_transcode_with_codec_selection` — MPEG2 → h264 via `libx264`
- `test_transcode_with_crf_and_preset`
- `test_transcode_audio_filter_volume`
- `test_transcode_audio_codec_change`
- `test_transcode_hwaccel_fallback` — `hwaccel=true` on a system without HW; verify software fallback + `WARNING` in logs
- `test_transcode_rejects_unsafe_filter` — `filter => 'movie=/etc/passwd'` must ERROR

### Task 1B: Enhanced `extract_audio` (`src/extract_audio.rs`)

```sql
extract_audio(
    data        bytea,
    format      text DEFAULT NULL,   -- NULL = auto-pick container based on source codec
    codec       text DEFAULT NULL,
    bitrate     int  DEFAULT NULL,
    sample_rate int  DEFAULT NULL,
    channels    int  DEFAULT NULL,
    filter      text DEFAULT NULL
) → bytea
```

**Mode selection** (the previous `format DEFAULT 'mp3'` + stream-copy default was wrong: copying an AAC stream into an MP3 container is invalid, and the old spec didn't say what happened):

- **Fast stream-copy path** — requires ALL of:
  1. `codec`, `bitrate`, `sample_rate`, `channels`, `filter` are all NULL, AND
  2. either `format` is NULL (auto-pick), OR `format` is a container that accepts the source audio codec (`aac`/`adts`, `mp3`, `opus`/`ogg`, `flac`, `wav` → matching container).
  - When `format` is NULL, the auto-pick table is: `aac → adts`, `mp3 → mp3`, `opus → ogg`, `vorbis → ogg`, `flac → flac`, `pcm_s16le → wav`, anything else → ERROR with a message telling the caller to supply `format` + `codec` for re-encode.
- **Re-encode path** — any other combination uses `AudioPipeline` (F1) with codec lookup via F7, filter validation (F3). If `codec` is NULL in this path, it is inferred from `format` via a small container→default-codec table (`mp3→libmp3lame`, `ogg→libopus`, `adts→aac`, `flac→flac`, `wav→pcm_s16le`); unknown `format` ERRORs.
- **Container/codec incompatibility** — the re-encode path validates `codec` is muxable into `format` before opening the encoder; mismatches ERROR with the F7 error contract template rather than failing mid-stream.

Tests: `test_extract_audio_copy_aac_auto_adts`, `test_extract_audio_copy_aac_rejects_mp3_container`, `test_extract_audio_reencode_to_mp3`, `test_extract_audio_with_filter`, `test_extract_audio_rejects_unsafe_filter`, `test_extract_audio_unknown_format_errors`.

### Task 1C: `trim` function (`src/trim.rs`)

**Revised per review feedback**: the hybrid precise-mode design had A/V sync risks. Replaced with two clean modes:

```sql
trim(
    data       bytea,
    start_time float8 DEFAULT 0.0,
    end_time   float8 DEFAULT NULL,
    precise    bool   DEFAULT false
) → bytea
```

- **`precise = false`** (fast, keyframe-aligned):
  - Seek to nearest keyframe ≤ `start_time`
  - Stream-copy packets until `end_time`
  - Rescale all timestamps to start at 0 (apply same offset to all streams for A/V sync)
  - Audio streams: trim at packet boundaries (audio packets are short; near-frame accuracy)
  - Output may start slightly before requested `start_time` — documented behavior
- **`precise = true`** (frame-accurate, full re-encode):
  - **Video and audio streams** are decoded and re-encoded using `VideoPipeline` + `AudioPipeline` (F1). Default: re-encode with the same codec as the input.
  - Video filter graph: `trim=start={start}:end={end},setpts=PTS-STARTPTS`
  - Audio filter graph: `atrim=start={start}:end={end},asetpts=PTS-STARTPTS`
  - **Decode-only codec fallback**: some codecs in FFmpeg builds are decode-only (no encoder linked in — e.g., decoder-only builds of `h264` when only `libx264` provides encoding, or obscure legacy codecs). When `codec_lookup::find_encoder` for the source codec returns `CodecError::NotAvailable`, `trim` falls back to a fixed default encoder per stream type — `libx264` for video, `aac` for audio — and logs `WARNING "pg_ffmpeg: trim(precise=true) source {codec} has no encoder in this FFmpeg build; re-encoding video as libx264/audio as aac"`. If the fallback encoder itself is unavailable, ERROR with the F7 error contract. The fallback set is documented in the README and is NOT user-configurable in v1 (keeps the API surface small).
  - **Subtitle and data streams are dropped** in v1. This is a deliberate scope reduction: cue-level subtitle trimming requires per-codec handling (text vs. image) and is out of scope for the initial trim implementation. Callers who need subtitles preserved across a precise trim should use the fast mode (`precise=false`), which stream-copies all subtitle tracks with timestamp rewriting.
  - A `WARNING` is logged once if subtitles/data streams are present and dropped: `"pg_ffmpeg: trim(precise=true) dropped N subtitle/data stream(s); use precise=false to preserve them"`.
  - Slower and larger output than fast mode but A/V sync is guaranteed
  - Future v2: bring subtitle handling into precise mode (tracked as out-of-scope in the plan's exclusions).
- Tests: `test_trim_fast_keyframe`, `test_trim_precise_reencode`, `test_trim_to_end`, `test_trim_av_sync_precise` (verify audio/video durations match within 1 frame)

### Task 1D: `extract_frames` set-returning function (`src/extract_frames.rs`)

**Revised per review feedback**: original design buffered all frames and did repeated seeks. New design uses a lazy iterator state machine.

```sql
extract_frames(
    data           bytea,
    interval       float8 DEFAULT 1.0,
    format         text   DEFAULT 'png',
    keyframes_only bool   DEFAULT false,
    max_frames     int    DEFAULT 1000
) → TABLE(timestamp float8, frame bytea)
```

- Use pgrx `TableIterator<'static, (name!(timestamp, f64), name!(frame, Vec<u8>))>`
- **Note**: pgrx `TableIterator` materializes the iterator before returning. So we still buffer in memory — but we bound it.
- `max_frames` parameter caps the output; default 1000 is well within memory for HLS-segment-sized inputs
- **Semantics**: exactly up to `max_frames` rows may be emitted. If the input would yield a `(max_frames + 1)`-th frame, the function raises `ERROR "pg_ffmpeg: extract_frames would emit more than max_frames ({max_frames}) rows; increase max_frames or use a larger interval"`. Truncation is never silent.

**Parameter semantics (hard rules)**:
- `interval <= 0.0` → `ERROR "pg_ffmpeg: interval must be > 0"`. No implicit "every frame" mode; callers wanting every frame should use `interval => 1.0 / fps`.
- `max_frames <= 0` → `ERROR "pg_ffmpeg: max_frames must be > 0"`.
- `format` must be `png` | `jpeg` | `jpg` (same set as `thumbnail`); any other value ERRORs.

**Timestamp origin**:
- Emitted `timestamp` values are in **seconds from the start of the input stream**, converted from the video stream's PTS via `pts * stream.time_base`. They are NOT wall-clock times and NOT relative to the first decoded frame.
- First frame's timestamp may not be exactly 0.0 if the stream has a non-zero start offset — documented in the README.

**Interaction between `keyframes_only` and `interval`**:
- `keyframes_only = true` → `interval` parameter is **ignored**; a log `WARNING` is emitted once per call if `interval` was also explicitly set to a non-default value. Emit one row per keyframe in decode order.
- `keyframes_only = false` → emit frames whose timestamp crosses the next `N * interval` threshold (walking N from 0). This snaps to the decoded frame nearest but not before each threshold — not a re-sampled frame rate.

**Decoding strategy**:
- Single forward pass through all packets (no seeks)
- `keyframes_only = true`: check `packet.is_key()` before decode, skip non-keyframes
- `keyframes_only = false`: decode every frame, track last-emitted timestamp, emit when `(frame.pts_sec - last_emit) >= interval`
- Stop scanning when attempting to emit the `(max_frames + 1)`-th row → ERROR (do not silently truncate). Exactly `max_frames` rows are the legal maximum.
- Frame encoding via helper extracted from `src/thumbnail.rs`

**Memory bounding**: row accumulation is bounded by `max_frames` (default 1000) alone. No separate byte-total GUC — `max_frames` plus the per-input `max_input_bytes` cap is sufficient, and Postgres `work_mem`/OOM is the backstop for pathological combinations.

Tests: `test_extract_frames_keyframes_only`, `test_extract_frames_interval`, `test_extract_frames_max_frames_limit`, `test_extract_frames_invalid_interval_errors`, `test_extract_frames_keyframes_only_ignores_interval`

### Task 1E: Enhanced `media_info` (`src/media_info.rs`)

Add to JSONB output:
- `chapters`: array of `{id, start, end, title}` from `ictx.chapters()`
- `tags`: format-level metadata dict from `ictx.metadata()`
- Per-stream:
  - `bit_rate` (from params or codec context)
  - `disposition` (default/forced/hearing_impaired/...)
  - `tags` (per-stream metadata)
  - `language` (from stream tag)
  - For subtitle streams: include `codec_type` = 'text' or 'image' (see Task 2E)
- Tests: `test_media_info_tags`, `test_media_info_chapters_present`

**Milestone 1 Deliverables**: 5 enhanced/new functions, full test coverage, benchmarks for 1A/1C, README updated.

---

## Milestone 2: Advanced & Multi-Input

**Gate to advance**: All Milestone 2 functions tested, security review of `filter_complex` complete, CI green.
**Parallelism**: Tasks 2A–2G can run in parallel after Milestone 1.

### Task 2A: `generate_gif` (`src/animation.rs`)

```sql
generate_gif(
    data       bytea,
    start_time float8 DEFAULT 0.0,
    duration   float8 DEFAULT 5.0,
    width      int    DEFAULT NULL,
    fps        int    DEFAULT 10,
    format     text   DEFAULT 'gif'   -- 'gif', 'apng', 'webp'
) → bytea
```

- Built-in filter spec (not user-provided, so no allow-list check needed): for GIF, `fps={fps},scale={width}:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse`
- For APNG/WebP: just `fps={fps},scale={width}:-1`
- Uses `VideoPipeline` from F1 with custom encoder (`codec::Id::GIF`, `APNG`, `WEBP`)
- Tests: `test_generate_gif` (verify `GIF89a` magic bytes), `test_generate_apng`

### Task 2B: `waveform` (`src/waveform.rs`)

```sql
waveform(
    data   bytea,
    width  int  DEFAULT 800,
    height int  DEFAULT 200,
    format text DEFAULT 'png',
    mode   text DEFAULT 'waveform'    -- 'waveform' or 'spectrum'
) → bytea
```

- Built-in filter: `showwavespic=s={width}x{height}:colors=white` or `showspectrumpic=s={width}x{height}`
- Decodes all audio frames, pushes through filter, collects the single output video frame
- Encodes via frame encoding helper (shared with `extract_frames`, `thumbnail`)
- Tests: `test_waveform_png`, `test_spectrum_png`

### Task 2C: `extract_subtitles` (`src/subtitles.rs`)

**Revised per review feedback**: explicit support matrix. Image-based subtitles (PGS/DVB/DVD) cannot be losslessly extracted to text and require OCR — out of scope.

```sql
extract_subtitles(
    data         bytea,
    format       text DEFAULT 'srt',     -- 'srt', 'ass', 'webvtt'
    stream_index int  DEFAULT NULL
) → text
```

**Support matrix**:

| Input codec | Supported output | Behavior |
|-------------|------------------|----------|
| `subrip` (SRT), `ass`, `ssa`, `webvtt`, `mov_text` | `srt`, `ass`, `webvtt` | Transcode via FFmpeg subtitle codecs |
| `dvd_subtitle`, `hdmv_pgs_subtitle`, `dvb_subtitle` | — | `ERROR`: "image-based subtitles require OCR, use extract_frames + external OCR" |
| No subtitle stream found | — | `ERROR`: "no subtitle stream in input" |

- Uses FFmpeg subtitle decoder → encoder path (not stream copy) so we can convert between text formats
- Returns `String` mapped to SQL `text`
- No separate text-size GUC — `max_input_bytes` bounds the input and subtitle text is bounded by the input duration. Postgres is the ultimate backstop.
- Tests: `test_extract_subtitles_srt_to_webvtt`, `test_extract_subtitles_rejects_pgs`, `test_extract_subtitles_no_stream_errors`

### Task 2D: `overlay` (`src/overlay.rs`)

```sql
overlay(
    background bytea,
    foreground bytea,
    x          int    DEFAULT 0,
    y          int    DEFAULT 0,
    start_time float8 DEFAULT 0.0,
    end_time   float8 DEFAULT NULL
) → bytea
```

- Opens two `MemInput` contexts
- Fixed filter graph (not user-provided, so no allow-list check): `[0:v][1:v]overlay={x}:{y}:enable='between(t,{start},{end})':eof_action=pass`
- Audio from background only (stream-copied)
- Uses `MultiInputGraph` (F1b) to wire two video inputs into the overlay filter, plus its own decoders and encoder for the output
- Tests: `test_overlay_basic` (output dims match background), `test_overlay_time_range`

### Task 2E: `filter_complex` (`src/filter_complex.rs`) — with safety review

**Per review feedback**: this is the highest-risk function. The design includes:

```sql
filter_complex(
    inputs       bytea[],
    filter_graph text,
    format       text DEFAULT 'matroska',
    codec        text DEFAULT NULL,
    audio_codec  text DEFAULT NULL,
    hwaccel      bool DEFAULT false
) → bytea
```

**Input grammar** (documented contract):

The user's `filter_graph` string MUST reference inputs using the fixed pattern `[i0:v]`, `[i0:a]`, `[i1:v]`, `[i1:a]`, ... where `iN` refers to the Nth element of the `inputs` array (zero-indexed). The graph must declare at least one of `[vout]` / `[aout]`: video-only graphs have just `[vout]`, audio-only graphs (e.g., `amix`) have just `[aout]`, and mixed graphs have both. A graph with neither ERRORs. Example:

```sql
filter_complex(
  ARRAY[v1, v2],
  '[i0:v][i1:v]hstack=inputs=2[vout];[i0:a][i1:a]amix=inputs=2[aout]'
)
```

This is stricter than FFmpeg's native filter graph grammar (which allows any label), and it lets us:
- Know unambiguously which filter-graph label maps to which `bytea` input
- Auto-generate the `buffer`/`abuffer` sources without parsing user-supplied labels
- Reject graphs that reference inputs outside the array range before calling into FFmpeg

**Pre-processing step**: before validation, the module rewrites `[iN:v]` / `[iN:a]` labels to FFmpeg-internal pad names (`[in_N_v]` / `[in_N_a]`), then hands the rewritten string to `avfilter_graph_parse_ptr`. If any `[iN:*]` where `N >= inputs.len()` appears, ERROR with a specific message.

**Safety requirements** (must ship with initial implementation):
1. Rewritten `filter_graph` passes through `validate_filter_spec()` (F3) — `movie`/`amovie`/etc. rejected
2. `inputs` array length checked against `pg_ffmpeg.max_inputs` (F4)
3. Each input size checked against `pg_ffmpeg.max_input_bytes` (F4)
4. Output size enforced via `MemOutput` callback (F4)
5. **Input reference check**: every `[iN:*]` label in the user string must satisfy `N < inputs.len()`, else ERROR
6. After parsing via `avfilter_graph_parse_ptr`, verify all unconnected input pads correspond to declared `[iN:*]` labels — no dangling/extra inputs
7. **At least one of `[vout]` / `[aout]` required.** Video-only graphs have `[vout]`; audio-only graphs (e.g., `amix`) have `[aout]`; mixed graphs have both. A graph with neither ERRORs.

**Implementation**:
- Parse user string, extract set of `[iN:*]` references
- Validate N < `inputs.len()` for all references
- Rewrite labels to internal names
- Run allow-list validation on the rewritten string
- Open N `MemInput` contexts
- Build filter graph via `avfilter_graph_parse_ptr` with pre-declared `buffer`/`abuffer` sources bound to the internal input names
- Bind `buffersink`/`abuffersink` to `[vout]`/`[aout]`
- Main loop: read packets from all inputs, decode, feed the matching source
- Collect from sinks, encode to single output

Tests:
- `test_filter_complex_hstack` — two videos side-by-side
- `test_filter_complex_amix` — mix two audio tracks
- `test_filter_complex_rejects_movie_filter`
- `test_filter_complex_rejects_too_many_inputs`
- `test_filter_complex_input_count_mismatch_errors`

**Parser hardening tests** (must ship with initial implementation — per review feedback):
- `test_filter_complex_label_rewrite_escaped_brackets` — `[i0:v]` inside a `drawtext=text='foo [bar]'` string must not be interpreted as an input reference
- `test_filter_complex_label_rewrite_nested_graphs` — `split[a][b];[a]scale=...[out1];[b]scale=...[out2]` with user `[iN:*]` references at the top level
- `test_filter_complex_label_out_of_range` — `[i9:v]` with a 2-element input array must ERROR before invoking FFmpeg
- `test_filter_complex_label_negative_index` — `[i-1:v]` must ERROR
- `test_filter_complex_label_non_numeric` — `[iX:v]` must ERROR
- `test_filter_complex_label_collision_with_internal` — user tries to use `[in_0_v]` (our internal rewritten name); must be rejected or safely namespaced
- `test_filter_complex_empty_filter_graph` — empty string must ERROR
- `test_filter_complex_missing_output_labels` — filter with no `[vout]` or `[aout]` must ERROR
- `test_filter_complex_both_output_labels` — graph with both `[vout]` and `[aout]` must succeed (tested via a graph that produces both)
- `test_filter_complex_unused_input` — `inputs` array has 3 elements but filter only references `[i0:*]` and `[i1:*]`; behavior: ERROR with clear message (unused inputs are a caller mistake, not silently dropped)

**Future extension** (not in initial ship): support multiple outputs via a TableIterator return type. Out of scope for this plan.

### Task 2F: `concat` and `concat_agg` (`src/concat.rs`)

**Revised per review feedback**: the original `Vec<Vec<u8>>` aggregate state could blow backend memory. New design:

```sql
concat(inputs bytea[]) → bytea
concat_agg(bytea ORDER BY ...) → bytea
```

- **`concat(bytea[])`**: Open each input with `MemInput::open(&[u8])` (borrowing directly from the `bytea[]` elements — no copy), verify stream compatibility (codecs, dimensions, sample_rate must match first input — error with specific message if not), stream-copy with timestamp offsetting into one `MemOutput`. Single pass, single output allocation.
- **`concat_agg`**: pgrx `#[pg_aggregate]`
  - State type: `ConcatState { total_bytes: usize, chunks: Vec<Box<[u8]>> }` — each chunk is appended once from the `accum` argument and never re-cloned. `Box<[u8]>` instead of `Vec<u8>` so the allocator returns the memory exactly once on finalize.
  - On each `accum`: check `total_bytes + new.len() <= max_aggregate_state_bytes` (F4), else ERROR. Append the incoming `bytea` as a single `Box<[u8]>`; do not concatenate into a single growing buffer (that would be O(N²)).
  - Finalize: iterate `chunks` in order, open each as a `MemInput` borrowing its slice, mux into one `MemOutput`, then drop each chunk as soon as it has been fully demuxed (freeing memory progressively so peak usage is `total_bytes`, not `2 × total_bytes`).
  - `parallel = unsafe` — aggregates are not parallel-safe (order matters for concat).
  - `COMBINEFUNC` not implemented (same reason).
- Documentation must warn: "concat_agg is O(total_size) in memory. For concatenating many large videos, use an external pipeline."
- Tests: `test_concat_two_videos`, `test_concat_incompatible_formats_errors`, `test_concat_agg_with_order_by`, `test_concat_agg_exceeds_state_limit_errors`

### Task 2G: `encode` from image sequence (`src/encode.rs`)

```sql
encode(
    frames      bytea[],
    fps         int  DEFAULT 24,
    codec       text DEFAULT 'libx264',
    format      text DEFAULT 'mp4',
    crf         int  DEFAULT 23,
    hwaccel     bool DEFAULT false
) → bytea
```

- Each bytea decoded as single image frame
- All frames must share dimensions (error if not)
- Encoder opened via codec name (HW attempt if `hwaccel=true`)
- Tests: `test_encode_from_frames`, `test_encode_hwaccel_fallback`, `test_encode_mismatched_dimensions_errors`

**Milestone 2 Deliverables**: 7 new functions, security allow-list validated, memory limits enforced across all multi-input paths.

---

## Cross-Cutting Tasks (at end of each milestone)

### X1: Update `src/lib.rs` module declarations

Add modules as milestones complete.

### X2: Update `Cargo.toml`

Version bumps: `0.1.10 → 0.2.0` after M1 gate, `0.2.0 → 0.3.0` after M2 gate.

Clippy deny-list (added in F7): add a `clippy.toml` at the repo root with a `disallowed-methods` list naming `ffmpeg::codec::encoder::find_by_name`, `ffmpeg::codec::decoder::find`, `ffmpeg::format::output::find`, and `ffmpeg::format::input`. Clippy's `disallowed_methods` lint does not support per-file allow-lists directly, so `src/codec_lookup.rs` applies `#![allow(clippy::disallowed_methods)]` at the module level. All other modules inherit the deny. CI runs `cargo clippy --all-targets -- -D warnings` so any direct lookup outside `codec_lookup.rs` fails the build.

### X3: Update `README.md`

Document new functions, GUCs, and the security model (allow-list, limits). Add a "when to use HW acceleration" section.

### X4: CI validation (every milestone gate)

**Gating commands** (must pass to advance a milestone):

```bash
cargo fmt --check
cargo clippy --all-targets --features pg16 -- -D warnings
cargo pgrx test pg16
cargo pgrx test pg17
cargo pgrx test pg18
```

**Non-gating commands** (run in separate CI jobs; failures reported but do not block milestone gates):

```bash
# Perf regression watch — flaky and machine-dependent
cargo pgrx bench pg16 --features pg_bench

# HW acceleration smoke tests — require GPU runner
cargo pgrx test pg16 --features hwaccel_tests
```

Per review feedback: benches were previously gating, but perf benchmarks are inherently noisy on shared CI runners and would create flaky gates. They are now tracked as a separate regression-watch job that posts results as PR comments but does not block merges. The `hwaccel_tests` feature (Task F5) runs only on a self-hosted GPU runner when available, also non-gating.

---

## Key Files Summary

| File | Milestone | Action |
|------|-----------|--------|
| `src/pipeline.rs` | F | **New** — shared VideoPipeline/AudioPipeline (F1) + MultiInputGraph (F1b) |
| `src/hwaccel.rs` | F | **New** — lazy per-backend HW detection |
| `src/filter_safety.rs` | F | **New** — filter allow-list validator |
| `src/limits.rs` | F | **New** — GUCs + size enforcement |
| `src/test_utils.rs` | F | **New** — shared test fixture generators |
| `src/codec_lookup.rs` | F | **New** — shared codec/format lookup with error contract (F7) |
| `src/transcode.rs` | F, 1 | Modify — refactor onto `pipeline`, add params |
| `src/extract_audio.rs` | 1 | Modify — add re-encoding path |
| `src/trim.rs` | 1 | **New** — two-mode trim |
| `src/extract_frames.rs` | 1 | **New** — set-returning with max_frames cap |
| `src/media_info.rs` | 1 | Modify — add chapters/tags/disposition |
| `src/animation.rs` | 2 | **New** — animated image generation |
| `src/waveform.rs` | 2 | **New** — audio visualization |
| `src/subtitles.rs` | 2 | **New** — text subtitle extraction only |
| `src/overlay.rs` | 2 | **New** — two-input compositing |
| `src/filter_complex.rs` | 2 | **New** — N-input with allow-list |
| `src/concat.rs` | 2 | **New** — concat + memory-bounded aggregate |
| `src/encode.rs` | 2 | **New** — image sequence → video |
| `src/lib.rs` | F/1/2 | Modify — module declarations |
| `src/mem_io.rs` | F | Modify — hook output size check |
| `Cargo.toml` | 1, 2 | Version bumps |
| `clippy.toml` | F | **New** — deny-list direct codec lookups outside `codec_lookup.rs` |
| `README.md` | 1, 2 | Document new functions + GUCs + security model |

## Reuse Existing Code

- **MemInput / MemOutput** (`src/mem_io.rs`): Used by every function. F4 adds an output-size callback hook.
- **VideoPipeline / AudioPipeline** (`src/pipeline.rs`, new in F1): Replaces the current `Transcoder` struct. Used by `transcode`, `trim` (precise), `generate_gif`, `overlay`, `filter_complex`, `encode`.
- **Frame encoding helper** (extracted from `src/thumbnail.rs` in F1): Reused by `extract_frames`, `waveform`.
- **test_utils** (F6): Reused by every test module.

## What's Deliberately Excluded

- **Real-time streaming** (RTMP/RTSP output): Doesn't fit Postgres request/response model.
- **Device capture**: No devices on a DB server.
- **Custom Postgres types**: `bytea` + `jsonb` + `text` are sufficient.
- **Image-based subtitle OCR**: Out of scope; error clearly when encountered.
- **Multi-output filter_complex**: Single output only in v1.
- **Cross-process HW device sharing**: Each Postgres backend has its own HW context cache.
- **Parallel aggregate support** for `concat_agg`: Order matters; not safe to parallelize.
