# Simplify `extract_frames` using high-level FFmpeg APIs

## Context

`src/extract_frames.rs` (188 lines, lines 11–188) hand-rolls everything the
FFmpeg filter graph would do for us:

- A manual `ScaleContext` converts decoded frames to RGB24 (lines 75–84).
- Hand-written interval math (`next_threshold`, `should_emit_frame`,
  `advance_threshold_past_timestamp`) decides when to emit a frame
  (lines 87, 146, 162–163, 168–183).
- A 9-parameter helper `drain_decoded_frames` (marked
  `#[allow(clippy::too_many_arguments)]`, lines 131–166) is called twice
  (packets at line 100, EOF flush at line 116) to thread all of that
  mutable state through the decode loop.

The crate already exposes a clean high-level primitive for this:
`pipeline::build_video_filter_graph(&decoder, spec)`
(`src/pipeline.rs:242-247`), which builds and validates a
`buffer → <spec> → buffersink` graph. It is explicitly designed for "a
future frame-extractor" (doc comment at `src/pipeline.rs:237-241`) but is
not yet used here.

Goal: collapse `extract_frames` onto that primitive so FFmpeg does the
frame selection and pixel-format conversion for us.

## Approach

Rewrite `collect_frame_rows` to drive a filter graph instead of a
hand-written sampler/scaler. Keep the public SQL signature, argument
validation, and error semantics (including `max_frames` overflow =
`error!`) identical.

### Filter spec

```
keyframes_only = false  ->  format!("fps=1/{interval},format=rgb24")
keyframes_only = true   ->  "format=rgb24"
```

- `fps=1/N` makes FFmpeg emit one frame every N seconds with grid PTS
  (0, N, 2N, …). This replaces `next_threshold` + `should_emit_frame` +
  `advance_threshold_past_timestamp`.
- `format=rgb24` replaces the `ScaleContext` entirely — `encode_frame`
  (`src/thumbnail.rs:127-153`) already expects RGB24 for PNG and does
  its own YUVJ420P conversion for JPEG, so RGB24 out of the filter is
  the right input for both branches.
- Keyframes mode keeps the packet-level `packet.is_key()` skip — it's a
  real optimization (non-key packets never hit the decoder) and is free
  to preserve.

### New shape of `collect_frame_rows`

1. Open `MemInput`, pick best video stream, build decoder (unchanged).
2. `decoder.set_packet_time_base(input.time_base())` (unchanged).
3. Build spec from `(interval, keyframes_only)`; call
   `pipeline::build_video_filter_graph(&decoder, &spec)`.
4. Read `sink_time_base` once from the buffersink via
   `av_buffersink_get_time_base` — same unsafe snippet already used at
   `src/pipeline.rs:71-86`. The `fps` filter rewrites PTS, so we must
   read the output time base from the sink rather than reuse the
   decoder time base.
5. Packet loop: skip non-video; skip non-key iff `keyframes_only`;
   `send_packet`; call a single `drain(decoder, graph, sink_time_base,
   format, max_frames, &mut rows)`.
6. After the loop: `send_eof` on the decoder, call `drain` once more to
   flush (same shape `VideoPipeline` uses at
   `src/pipeline.rs:171-214`).

### `drain` helper (replaces `drain_decoded_frames`)

Parameters shrink from 9 to 6; no mutable threshold state:

```rust
fn drain(
    decoder: &mut ffmpeg_next::decoder::Video,
    graph: &mut ffmpeg_next::filter::Graph,
    sink_time_base: Rational,
    format: &str,
    max_frames: usize,
    rows: &mut Vec<(f64, Vec<u8>)>,
)
```

Body: `while decoder.receive_frame(&mut decoded).is_ok()` →
`graph.get("in").source().add(&decoded)` → inner
`while graph.get("out").sink().frame(&mut filtered).is_ok()` →
enforce `rows.len() == max_frames` → push
`(frame_timestamp_seconds(&filtered, sink_time_base),
thumbnail::encode_frame(&filtered, format))`.

### Helpers to delete

- `should_emit_frame` (lines 168–170) — subsumed by `fps` filter.
- `advance_threshold_past_timestamp` (lines 180–183) — same.
- `timestamp_seconds` (lines 186–188) — inline the one-line conversion
  into `frame_timestamp_seconds`.
- `ScaleContext` / `Pixel` / `Flags` imports (lines 1, 3) — unused.

### Helpers to keep

- `validate_extract_frames_args` (lines 38–49) — unchanged.
- `frame_timestamp_seconds` (lines 172–178) — retained (test
  `test_frame_timestamp_seconds_uses_frame_pts_directly` exercises it),
  with `timestamp_seconds` inlined.
- The `keyframes_only && interval != 1.0` warning at lines 22–26 —
  unchanged.

## Critical files

- `src/extract_frames.rs` — the rewrite target.
- `src/pipeline.rs:242-247` — `build_video_filter_graph` (reuse).
- `src/pipeline.rs:71-86` — buffersink time-base read pattern
  (replicate).
- `src/thumbnail.rs:127-153` — `encode_frame` (unchanged consumer).
- `src/mem_io.rs` — `MemInput` (unchanged consumer).

## Behavior change to call out

Output timestamps in interval mode will move from "PTS of the first
decoded frame at or after each `N*interval` threshold" to the exact
grid `0, interval, 2*interval, …` produced by the `fps` filter. This
is the whole point of using the filter, and arguably more predictable,
but it does shift the numeric values a consumer would see.

Test impact:

- `test_extract_frames_interval` (lines 216–236) tests the removed
  helpers directly and must go. Replace it with an end-to-end test
  that runs `collect_frame_rows(..., interval=1.0, keyframes_only=false)`
  against the existing `generate_test_video_bytes` fixture and asserts
  (a) the expected row count and (b) timestamps fall on the
  `N*interval` grid (within a tolerance).
- `test_extract_frames_keyframes_only`, `..._max_frames_limit`,
  `..._invalid_interval_errors`,
  `test_frame_timestamp_seconds_uses_frame_pts_directly`, and
  `..._keyframes_only_ignores_interval` should continue to pass
  unchanged.

## Verification

1. `cargo clippy --all-targets` — no new warnings; the
   `#[allow(clippy::too_many_arguments)]` on `drain_decoded_frames`
   disappears with the function.
2. `cargo pgrx test pg17` (or whichever pg version is configured) —
   all `extract_frames` tests green, including the rewritten interval
   test.
3. Manual SQL smoke (optional, in a pg session):
   ```sql
   SELECT count(*), min(timestamp), max(timestamp)
   FROM ffmpeg.extract_frames(
     pg_read_binary_file('/path/to/sample.mp4'),
     interval => 0.5, format => 'jpeg', max_frames => 20
   );
   ```
   Expected: count ≤ 20, timestamps on the 0.5s grid, JPEG magic in
   `frame`.
