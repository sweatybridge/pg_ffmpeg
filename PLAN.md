# Complete pg_ffmpeg: Full FFmpeg Capabilities in PostgreSQL

## Context

pg_ffmpeg is a Rust/pgrx PostgreSQL extension that wraps FFmpeg for in-memory media processing. Today it has 5 functions covering metadata extraction, thumbnails, basic transcoding (with video filters), audio extraction (stream copy only), and HLS segmentation. The goal is to expand this to cover **everything FFmpeg can do** — including hardware-accelerated encoding/decoding — while keeping the SQL API clean and composable.

### Design Philosophy

- Expose **a small number of powerful, composable functions** with well-chosen parameters
- Lean on FFmpeg's filter graph DSL rather than reinventing it
- Use SQL's `SELECT`/`FROM`/`JOIN`/aggregates for orchestration
- Multi-input operations use `bytea[]` arrays
- **Hardware acceleration** (VAAPI, NVENC, QSV, VideoToolbox) whenever available at runtime — transparent fallback to software

### Decisions
- **Scope**: All 12 phases — complete implementation
- **Multi-input approach**: `bytea[]` arrays (simple, composable, all in-memory)
- **Hardware acceleration**: Runtime detection with automatic fallback

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

## Workstreams (Parallelizable)

The implementation is divided into **5 independent workstreams** that can be developed in parallel by separate agents. Dependencies between workstreams are noted where they exist.

---

### Workstream A: Enhanced Transcoding & Hardware Acceleration

**Files to modify**: `src/transcode.rs`, `src/lib.rs`
**New files**: `src/hwaccel.rs`
**Dependencies**: None (extends existing code)

#### Task A1: Hardware acceleration runtime detection (`src/hwaccel.rs`)

Create a shared module for hardware acceleration:
- Probe available HW device types at init time using `av_hwdevice_iterate_types()`
- Provide a `find_hw_encoder(codec_name)` function that tries HW encoders first (e.g., `h264_nvenc`, `h264_vaapi`, `h264_qsv`) then falls back to software
- Provide a `find_hw_decoder(codec_id)` function with same fallback logic
- Manage `AVHWDeviceContext` creation and lifetime
- Handle HW frame ↔ SW frame transfers (`av_hwframe_transfer_data`)
- Expose a `hw_pix_fmt_callback` for decoder HW format negotiation
- Key FFmpeg APIs: `av_hwdevice_ctx_create`, `av_hwdevice_iterate_types`, `av_hwframe_transfer_data`, `avcodec_get_hw_config`

#### Task A2: Enhance `transcode` signature and codec selection (`src/transcode.rs`)

Extend the existing function signature:

```sql
transcode(
    data         bytea,
    format       text     DEFAULT NULL,   -- output container (e.g. 'matroska', 'mp4')
    filter       text     DEFAULT NULL,   -- video filter graph (e.g. 'scale=-1:720')
    codec        text     DEFAULT NULL,   -- video codec name (e.g. 'libx264', 'libx265')
    preset       text     DEFAULT NULL,   -- encoder preset (e.g. 'fast', 'slow')
    crf          int      DEFAULT NULL,   -- constant rate factor (0-51 for x264)
    audio_codec  text     DEFAULT NULL,   -- audio codec (e.g. 'aac', 'libopus')
    audio_filter text     DEFAULT NULL,   -- audio filter graph (e.g. 'volume=0.5')
    audio_bitrate int     DEFAULT NULL,   -- audio bitrate in bits/sec
    hwaccel      bool     DEFAULT true    -- attempt hardware acceleration
) → bytea
```

Implementation:
- When `codec` is provided, look up encoder by name via `codec::encoder::find_by_name()` instead of matching input codec ID. If `hwaccel` is true, try HW variant first via `hwaccel::find_hw_encoder()`.
- When `preset`/`crf` provided, set via `av_opt_set` on the encoder's private data (`(*enc_ptr).priv_data`)
- Build a parallel `AudioTranscoder` struct (mirroring the video `Transcoder`) when `audio_codec` or `audio_filter` is specified
- Audio filter graph: `abuffer → user_spec → abuffersink` (mirrors video pattern but with audio parameters: sample_rate, sample_fmt, channel_layout)
- When only `audio_filter` is provided without `audio_codec`, re-encode with same audio codec after filtering
- Zero-copy remux path remains when no filter/codec/audio params given
- Register module in `src/lib.rs`: `mod hwaccel;` (make it `pub(crate)`)

#### Task A3: Tests for enhanced transcode

Add to `src/transcode.rs` test module:
- `test_transcode_with_codec_selection` — transcode MPEG2 to a different codec, verify via `media_info`
- `test_transcode_with_crf` — verify CRF parameter is accepted
- `test_transcode_audio_filter` — apply volume filter, verify output has audio
- `test_transcode_audio_codec` — re-encode audio to different codec
- `test_transcode_hwaccel_fallback` — verify hwaccel=true doesn't error when no HW available (graceful fallback)

---

### Workstream B: Trim, Extract Audio Enhancement, and Subtitles

**Files to modify**: `src/extract_audio.rs`, `src/lib.rs`
**New files**: `src/trim.rs`, `src/subtitles.rs`
**Dependencies**: None

#### Task B1: `trim` function (`src/trim.rs`)

```sql
trim(
    data         bytea,
    start_time   float8 DEFAULT 0.0,   -- seconds
    end_time     float8 DEFAULT NULL,  -- NULL = to end
    precise      bool   DEFAULT false  -- true = decode for frame-accurate cut
) → bytea
```

Implementation:
- Open MemInput, create MemOutput matching input format
- Convert `start_time`/`end_time` to AV_TIME_BASE units
- When `precise = false`:
  - Call `ictx.seek(start_timestamp, ..)` to seek to nearest keyframe
  - Copy all stream metadata/parameters to output
  - Iterate packets, skip those before `start_time`, stop after `end_time`
  - Rescale all timestamps to start from 0 (subtract start offset)
- When `precise = true`:
  - Seek to keyframe before `start_time`
  - Decode→re-encode video frames at boundaries (first/last GOPs)
  - Stream-copy packets in the middle range
  - Audio: always stream-copy (frame-accurate audio cuts are rarely needed)
- Register module in `src/lib.rs`

#### Task B2: Enhance `extract_audio` (`src/extract_audio.rs`)

```sql
extract_audio(
    data         bytea,
    format       text DEFAULT 'mp3',
    codec        text DEFAULT NULL,    -- audio codec name (e.g. 'libmp3lame', 'aac')
    bitrate      int  DEFAULT NULL,    -- bits/sec
    sample_rate  int  DEFAULT NULL,    -- e.g. 44100
    channels     int  DEFAULT NULL,    -- e.g. 1 for mono
    filter       text DEFAULT NULL     -- audio filter graph (e.g. 'volume=0.5,atempo=1.5')
) → bytea
```

Implementation:
- When all new params are NULL, keep existing stream-copy path (backward compatible)
- When any param is set, switch to decode→filter→encode:
  - Open audio decoder from input stream parameters
  - Build audio filter graph if `filter` provided (`abuffer → spec → abuffersink`)
  - Open encoder by name (or match input codec if `codec` is NULL)
  - Set bitrate/sample_rate/channels on encoder if provided
  - Decode frames → push through filter → encode → write packets

#### Task B3: `extract_subtitles` function (`src/subtitles.rs`)

```sql
extract_subtitles(
    data         bytea,
    format       text DEFAULT 'srt',   -- 'srt', 'ass', 'webvtt'
    stream_index int  DEFAULT NULL     -- NULL = best subtitle stream
) → text
```

Implementation:
- Open MemInput, find subtitle stream (by index or best match)
- Create MemOutput with requested subtitle format
- Copy subtitle packets to output (stream copy — subtitle formats are text-based)
- Return `String` (maps to SQL `text`) via `String::from_utf8` on output bytes
- Register module in `src/lib.rs`

#### Task B4: Tests

- `src/trim.rs`: `test_trim_basic` (trim 1s video to 0.5s range), `test_trim_precise`, `test_trim_to_end`
- `src/extract_audio.rs`: `test_extract_audio_with_codec`, `test_extract_audio_with_filter`
- `src/subtitles.rs`: `test_extract_subtitles_srt` (requires test video with subtitle track)

---

### Workstream C: Frame Extraction and GIF Generation

**Files to modify**: `src/lib.rs`
**New files**: `src/extract_frames.rs`, `src/gif.rs`
**Dependencies**: None

#### Task C1: `extract_frames` set-returning function (`src/extract_frames.rs`)

```sql
extract_frames(
    data           bytea,
    interval       float8 DEFAULT 1.0,   -- seconds between frames
    format         text   DEFAULT 'png',
    keyframes_only bool   DEFAULT false
) → TABLE(timestamp float8, frame bytea)
```

Implementation:
- Use pgrx `TableIterator<'static, (name!(timestamp, f64), name!(frame, Vec<u8>))>` return type
- Open MemInput, find best video stream
- Collect frames into a Vec of (timestamp, encoded_frame) tuples:
  - When `keyframes_only`: check `packet.is_key()`, only decode those
  - When `interval` set: seek to each `N * interval` timestamp, decode one frame
  - Encode each decoded frame using `thumbnail.rs` pattern (RGB24 conversion → PNG/JPEG encoder)
- Return `TableIterator::new(frames)`
- Register module in `src/lib.rs`

#### Task C2: `generate_gif` function (`src/gif.rs`)

```sql
generate_gif(
    data         bytea,
    start_time   float8 DEFAULT 0.0,
    duration     float8 DEFAULT 5.0,
    width        int    DEFAULT NULL,   -- NULL = original
    fps          int    DEFAULT 10,
    format       text   DEFAULT 'gif'   -- also 'apng', 'webp'
) → bytea
```

Implementation:
- Open MemInput, seek to `start_time`
- Build filter graph for high-quality GIF:
  - `fps=<fps>,scale=<width>:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse`
  - For apng/webp: simpler filter — just `fps=<fps>,scale=<width>:-1`
- Decode video frames until `start_time + duration`
- Push through filter graph
- Encode to GIF/APNG/WebP muxer via MemOutput
- For GIF: use `codec::Id::GIF` encoder, format `"gif"`
- For APNG: use `codec::Id::APNG`, format `"apng"`
- Register module in `src/lib.rs`

#### Task C3: Tests

- `src/extract_frames.rs`: `test_extract_frames_interval` (1s video, 0.5s interval → expect ~2 frames), `test_extract_frames_keyframes_only`
- `src/gif.rs`: `test_generate_gif` (verify non-empty output, verify it's valid GIF by checking magic bytes `GIF89a`)

---

### Workstream D: Multi-Input Operations (Overlay, Filter Complex, Concat)

**Files to modify**: `src/lib.rs`
**New files**: `src/overlay.rs`, `src/filter_complex.rs`, `src/concat.rs`
**Dependencies**: Workstream A (hwaccel module) is optional but beneficial

#### Task D1: `overlay` function (`src/overlay.rs`)

```sql
overlay(
    background   bytea,
    foreground   bytea,
    x            int     DEFAULT 0,
    y            int     DEFAULT 0,
    start_time   float8  DEFAULT 0.0,
    end_time     float8  DEFAULT NULL
) → bytea
```

Implementation:
- Open two MemInput contexts (background, foreground)
- Build multi-input filter graph:
  - Two `buffer` sources: `[0:v]` and `[1:v]` with respective dimensions/formats
  - Filter: `[0:v][1:v]overlay=<x>:<y>:enable='between(t,<start>,<end>)'`
  - One `buffersink` output
- Decode frames from both inputs, feeding to respective buffer sources
- Handle different-length inputs (foreground may be shorter — overlay filter handles this with `eof_action=pass`)
- Encode filtered output, copy audio from background
- Write to MemOutput
- Register module in `src/lib.rs`

#### Task D2: `filter_complex` function (`src/filter_complex.rs`)

```sql
filter_complex(
    inputs       bytea[],         -- array of input media
    filter_graph text,            -- full FFmpeg filter graph string
    format       text DEFAULT 'matroska',
    codec        text DEFAULT NULL,
    hwaccel      bool DEFAULT true
) → bytea
```

Implementation:
- Open N MemInput contexts from the `bytea[]` array (pgrx `Vec<Option<Vec<u8>>>`)
- Parse the filter_graph string to determine input/output pad count
- Build FFmpeg filter graph with N buffer sources (`[0:v]`, `[1:v]`, `[0:a]`, etc.)
- Main processing loop: round-robin read packets from all inputs, decode, feed to filter graph
- Collect filtered frames from buffersink, encode to output
- If `codec` specified, use it; otherwise infer from first input
- If `hwaccel` true, attempt HW encoder via `hwaccel::find_hw_encoder()`
- This is the "escape hatch" covering: hstack, vstack, amerge, amix, drawtext, crossfade, etc.
- Register module in `src/lib.rs`

#### Task D3: `concat` aggregate and function (`src/concat.rs`)

```sql
-- Simple function form
concat(
    inputs bytea[]
) → bytea

-- Aggregate form (for use with ORDER BY)
concat_agg(data bytea ORDER BY ...) → bytea
```

Implementation for `concat(bytea[])`:
- Open each input via MemInput, verify stream compatibility (codec, dimensions for video; codec, sample_rate for audio)
- Create MemOutput with stream parameters from first input
- For each input in order: iterate packets, rescale timestamps (offset by cumulative duration), write to output
- Track cumulative duration per stream for timestamp offsetting

Implementation for `concat_agg`:
- Use pgrx `#[pg_aggregate]` derive macro
- State type: `Internal` wrapping a `Vec<Vec<u8>>`
- `state` function: append each `bytea` to the Vec
- `finalize` function: call `concat()` on the accumulated array
- Register module in `src/lib.rs`

#### Task D4: Tests

- `src/overlay.rs`: `test_overlay_basic` (overlay small image on video, verify output dimensions match background)
- `src/filter_complex.rs`: `test_filter_complex_hstack` (stack two videos side-by-side, verify output width = 2x input)
- `src/concat.rs`: `test_concat_two_videos` (concat two 1s videos, verify output duration ~2s via `media_info`), `test_concat_agg`

---

### Workstream E: Metadata, Waveform, and Encode

**Files to modify**: `src/media_info.rs`, `src/lib.rs`
**New files**: `src/waveform.rs`, `src/encode.rs`
**Dependencies**: None

#### Task E1: Enhance `media_info` (`src/media_info.rs`)

Add to the JSONB output:
- `chapters`: iterate `ictx.chapters()` → array of `{id, start, end, title}` (times in seconds)
- `tags`: iterate `ictx.metadata()` → key/value object of format-level tags
- Per-stream additions:
  - `bit_rate`: from `stream.parameters()` or codec context
  - `disposition`: from `stream.disposition()` flags (default, forced, etc.)
  - `tags`: per-stream metadata dictionary
  - `language`: from stream tags `language` key
  - For subtitle streams: include codec info

Implementation: extend the existing `for stream in ictx.streams()` loop and the final JSON object.

#### Task E2: `waveform` function (`src/waveform.rs`)

```sql
waveform(
    data         bytea,
    width        int    DEFAULT 800,
    height       int    DEFAULT 200,
    format       text   DEFAULT 'png',   -- 'png' or 'jpeg'
    mode         text   DEFAULT 'waveform'  -- 'waveform' or 'spectrum'
) → bytea
```

Implementation:
- Open MemInput, find best audio stream
- Build audio→video filter graph:
  - For waveform: `abuffer → showwavespic=s=<width>x<height>:colors=white → buffersink`
  - For spectrum: `abuffer → showspectrumpic=s=<width>x<height> → buffersink`
- Feed all decoded audio frames through the filter
- The filter produces a single video frame (the visualization image)
- Encode that frame as PNG/JPEG using the pattern from `thumbnail.rs`
- Return image bytes
- Register module in `src/lib.rs`

#### Task E3: `encode` function (`src/encode.rs`)

```sql
encode(
    frames       bytea[],         -- array of image frames (PNG/JPEG bytes)
    fps          int DEFAULT 24,
    codec        text DEFAULT 'libx264',
    format       text DEFAULT 'mp4',
    hwaccel      bool DEFAULT true
) → bytea
```

Implementation:
- For each bytea in the array: open as MemInput, decode single image frame
- Create MemOutput with specified format
- Open video encoder by name (try HW variant if `hwaccel`)
- Set dimensions from first frame, set fps, set reasonable defaults (CRF 23, gop=fps*2)
- Encode each decoded frame with incrementing PTS
- Flush encoder, write trailer
- Register module in `src/lib.rs`

#### Task E4: Tests

- `src/media_info.rs`: `test_media_info_tags` (verify tags/chapters fields present in output)
- `src/waveform.rs`: `test_waveform_png` (generate waveform from video with audio, verify PNG magic bytes)
- `src/encode.rs`: `test_encode_from_frames` (extract 3 frames via thumbnail, encode back to video, verify via media_info)

---

## Cross-Workstream Tasks (After All Workstreams Complete)

### Task X1: Update `src/lib.rs` module declarations

Add all new modules:
```rust
mod concat;
mod encode;
mod extract_frames;
mod filter_complex;
mod gif;
pub(crate) mod hwaccel;
mod overlay;
mod subtitles;
mod trim;
mod waveform;
```

### Task X2: Move test video generation to shared module

Move `generate_test_video_bytes()` from `src/transcode.rs` tests to a shared `src/test_utils.rs` module (gated behind `#[cfg(any(test, feature = "pg_test"))]`). Update all test modules to import from there.

### Task X3: Update `Cargo.toml`

Bump version from `0.1.10` to `0.2.0` (new features warrant minor version bump).

### Task X4: Update `README.md`

Add all new functions to the function table and usage examples section.

### Task X5: Run full CI validation

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo pgrx test pg16
cargo pgrx bench pg16
```

---

## Key Files Summary

| File | Workstream | Action |
|------|------------|--------|
| `src/hwaccel.rs` | A | **New** — HW accel runtime detection & fallback |
| `src/transcode.rs` | A | Modify — add codec/crf/preset/audio/hwaccel params |
| `src/trim.rs` | B | **New** — time range extraction |
| `src/extract_audio.rs` | B | Modify — add re-encoding path |
| `src/subtitles.rs` | B | **New** — subtitle extraction |
| `src/extract_frames.rs` | C | **New** — set-returning frame extraction |
| `src/gif.rs` | C | **New** — animated GIF/APNG/WebP |
| `src/overlay.rs` | D | **New** — two-input video compositing |
| `src/filter_complex.rs` | D | **New** — N-input arbitrary filter graphs |
| `src/concat.rs` | D | **New** — concatenation function + aggregate |
| `src/media_info.rs` | E | Modify — add chapters/tags/disposition |
| `src/waveform.rs` | E | **New** — audio visualization |
| `src/encode.rs` | E | **New** — image sequence to video |
| `src/mem_io.rs` | — | No changes (reused by all) |
| `src/lib.rs` | X | Modify — add module declarations |
| `Cargo.toml` | X | Modify — version bump |
| `README.md` | X | Modify — document new functions |

## Reuse Existing Code

- **MemInput / MemOutput** (`src/mem_io.rs`): Used by every new function. No changes needed.
- **Transcoder struct** (`src/transcode.rs`): decode→filter→encode pattern reused by `trim` (precise mode), `gif`, `overlay`, `filter_complex`. If duplicated >2x, factor into shared helper.
- **generate_test_video_bytes** (currently in `src/transcode.rs` tests): Move to shared `test_utils` module for all test modules.
- **Frame encoding pattern** (`src/thumbnail.rs`): RGB24 conversion + PNG/JPEG encoding reused by `extract_frames`, `waveform`.

## What's Deliberately Excluded

- **Real-time streaming** (RTMP/RTSP output): Doesn't fit Postgres request/response model.
- **Device capture**: No devices on a DB server.
- **Custom Postgres types**: `bytea` + `jsonb` + `text` are sufficient. Functions are composable via bytea piping.
