# Milestone 2 Review

Review of the current implementation against `docs/PLAN.md` Milestone 2 (Tasks 2A–2G) and the Foundation / Milestone 1 contracts it depends on. Line numbers are from `main` at the time of review (commit `9efd0c0` + `be59bef`).

This review assumes the Milestone 1 regressions flagged in `docs/MILESTONE_1_REVIEW.md` remain open; several of the same patterns recur in Milestone 2 and are noted once per task.

## Task 2A — `generate_gif` (`src/animation.rs`)

- **File name drift**: PLAN §"Task 2A" and the "Key Files Summary" table say `src/gif.rs`. The implementation lives in `src/animation.rs` with no follow-up edit to the plan's mapping. (`src/animation.rs` vs `docs/PLAN.md` table row for Task 2A.)
- **Does not produce an animation**. PLAN mandates the filter graph
  `fps={fps},scale={width}:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse`
  to build a proper paletted, motion-accurate GIF. `encode_animation_frame` (`src/animation.rs:85`) does none of this: `decode_frame_at` (`:28`) decodes **exactly one** frame at `start_time`, and the loop at `:147-161` re-sends that same frame `duration × fps` times with incrementing PTS. The output is a static single-frame GIF played as a slideshow, not an animation.
- **`VideoPipeline` not used**. PLAN §2A says "Uses `VideoPipeline` from F1 with custom encoder (`codec::Id::GIF`, `APNG`, `WEBP`)". The module reimplements decode + encode from scratch — same anti-pattern called out in Milestone 1 for `transcode` / `extract_audio`.
- **Input-size limit not enforced**. `generate_gif` (`src/animation.rs:12`) does not call `limits::check_input_size` on the bytea argument before opening `MemInput`. Every other Milestone 2 function checks it.
- **Zero-copy boundary violation**. `fn generate_gif(data: Vec<u8>, ...)` (`src/animation.rs:14`) takes an owned `Vec<u8>` instead of the pgrx borrowed slice. Same issue flagged in Milestone 1 for `transcode`, `extract_audio`, and `trim`.
- **Per-output-frame `.clone()`**. `let mut frame = frame.clone();` (`:149`) inside the encode loop. The static-output bug above means this clone is unavoidable with the current design; a filter-graph-based implementation per PLAN would stream decoded frames through `palettegen`/`paletteuse` and wouldn't need it.

## Task 2B — `waveform` (`src/waveform.rs`)

- **Zero-copy boundary violation**. `fn waveform(data: Vec<u8>, ...)` (`src/waveform.rs:13`).
- **Input-size limit not enforced**. No `limits::check_input_size` call at the entry point (`:12`).
- **Format allow-list drift**. `validate_waveform_args` accepts `ppm` in addition to `png/jpeg/jpg` (`:32-35`). PLAN §2B lists only `png`. `frame_encoder::encode_frame` does support PPM for the shared thumbnail path, so the output path works, but this widens the documented API surface silently.
- **`ensure_rgb24` always clones the happy-path frame** (`:174`) even when no pixel-format conversion is needed. A caller-owned frame move would avoid this.

## Task 2C — `extract_subtitles` (`src/subtitles.rs`)

- **Hand-rolled parser instead of FFmpeg codec round-trip**. PLAN §2C explicitly states: "Uses FFmpeg subtitle decoder → encoder path (not stream copy) so we can convert between text formats." The implementation never invokes FFmpeg's subtitle decoder or encoder. Instead it reads packet bytes directly (`packet_text`, `:129-151`), manually strips ASS override tags (`strip_ass_override_tags`, `:167-181`), and hand-writes SRT/WebVTT/ASS output (`render_srt` / `render_webvtt` / `render_ass`, `:192-234`). This bypass means:
  - Escaped characters, non-UTF-8 payloads, and container-specific quirks (MOV_TEXT styling records, WebVTT cue settings) are silently stripped or malformed.
  - Future codec additions (e.g., `MICRODVD`, `JACOSUB`) require Postgres-side parser work instead of enabling the FFmpeg decoder.
- **F7 bypass**. PLAN §F7 lists `extract_subtitles` in the MUST-use-`codec_lookup` function set. The module never imports `codec_lookup`. Because the decoder/encoder path was skipped, there is nothing to translate — but the bypass of the FFmpeg path is the root cause, not a secondary choice.
- **Zero-copy boundary violation**. `fn extract_subtitles(data: Vec<u8>, ...)` (`:10`).
- **Input-size limit not enforced**. No `limits::check_input_size` at `:9`.
- **`test_extract_subtitles_rejects_pgs` tests a private helper**. `:286-289` calls `ensure_supported_text_subtitle(codec::Id::HDMV_PGS_SUBTITLE)` directly rather than constructing a PGS-bearing bytea and invoking `extract_subtitles`. The public-function error path is uncovered.
- **Arbitrary 2-second fallback duration**. `end = start + 2.0` when the packet reports zero duration (`:115`). Not in PLAN; silently extends the cue.
- **MOV_TEXT length-prefix fail-open**. If the stored big-endian length exceeds the payload, `packet_text` silently uses `&data[2..]` (`:134-137`) instead of erroring. A corrupt input becomes "best-effort" rather than surfacing a clear error.

## Task 2D — `overlay` (`src/overlay.rs`)

- **Frame clone on every encoded output**. `OverlayVideoOutput::send_frame` clones the frame it is handed (`src/overlay.rs:333`). The method signature takes `&frame::Video`; shipping `frame::Video` by value to `send_frame` would remove the clone. PLAN's Zero-Copy Discipline is explicit: "Frames move, not clone: pipeline stages pass `ffmpeg::frame::Video` / `frame::Audio` by move through decode → filter → encode."
- **Zero-copy boundary violation**. `fn overlay(background: Vec<u8>, foreground: Vec<u8>, ...)` (`:17-18`). Both bytea args should be borrowed.
- **`pipeline::copy_stream` not reused**. `copy_background_audio_stream` (`:260-276`) inlines the `add_stream(codec::Id::None) + set_parameters + codec_tag = 0` ritual that already exists as `pipeline::copy_stream` (`src/pipeline.rs:553`). Minor duplication but the same F1-reuse theme the Milestone 1 review flagged.
- **Missing PLAN-required test**. PLAN §2D lists `test_overlay_time_range`. The module has `test_overlay_basic` (`:494`) and `test_overlay_preserves_background_audio` (`:512`); no test exercises the `enable='between(t,start,end)'` path.
- **Module-level `#![allow(dead_code)]`** (`:1`) hides any genuinely-unused helpers. Scope the allow to the specific items that need it.

## Task 2E — `filter_complex` (`src/filter_complex.rs`)

Functionally the most complete task in M2 — label rewriting, `[vout]`/`[aout]` enforcement, allow-list validation, and per-input F7 encoder selection are all present. The issues below are real blockers on the acceptance contract PLAN wrote down.

- **`hwaccel=true` hard-errors at the entry point** (`:114-116`). PLAN §2E declares `hwaccel bool DEFAULT false` as part of the signature; erroring when the caller sets it to `true` means the parameter is effectively undocumented dead weight. Match the `transcode` policy (try HW, `WARNING` on failure, fall back to software) or drop the parameter entirely.
- **Frame clones in both output pipelines**. `VideoOutputPipeline::send_frame` (`:599`) and `AudioOutputPipeline::send_frame` (`:696`) each clone the frame to mutate PTS. Same "accept frame by value" fix as `overlay`. These are the highest-volume clones in Milestone 2 (once per output frame × number of output streams).
- **Parser hardening tests are invisible to CI**. The `parser_tests` module is gated on `#[cfg(test)]` (`:1194`), not `#[cfg(any(test, feature = "pg_test"))]`. `cargo pgrx test pg<N>` compiles with the `pg_test` feature and does **not** enable plain `#[cfg(test)]`. Effect: the eleven parser hardening tests PLAN §2E says "must ship with initial implementation" never execute in the gating CI job.
- **Missing PLAN-required tests**. Separately from the gating issue above:
  - `test_filter_complex_rejects_too_many_inputs` — not present.
  - `test_filter_complex_input_count_mismatch_errors` — not present. (The closest is `test_filter_complex_unused_input`, which covers the opposite direction.)
- **Module-level `#![allow(dead_code)]`** (`:1`) again.

## Task 2F — `concat` and `concat_agg` (`src/concat.rs`)

Closest-to-spec module in Milestone 2. The state type, parallel-safety, progressive free, and aggregate limit all match PLAN §2F.

- **`pg_aggregate` args force an owned copy**. `type Args = Vec<u8>` (`:72`) — pgrx hands the accum body a `Vec<u8>`, which is a copy of the bytea value per invocation. PLAN's "chunk is appended once" is honored downstream (`.into_boxed_slice()` at `:88`), but the boundary copy is unavoidable with the current pgrx surface. Document this explicitly, or pursue an `&[u8]` aggregate arg if pgrx gains support.
- **Stream-signature compatibility tests are thin**. Only `test_concat_incompatible_formats_errors` (`:409-414`, dimension mismatch) exercises `verify_compatible`. Codec-id mismatch, sample-rate mismatch, channel mismatch, sample-format mismatch, stream-count mismatch, and non-contiguous stream indices all have error paths that no test reaches (`:303-381`, `:267-273`).
- **`concat_chunks` double-drop is redundant** (`:123-124`). `chunk` and `input` are both dropped at the end of each iteration automatically; the explicit `drop(input); drop(chunk)` adds noise without affecting peak memory.

## Task 2G — `encode` (`src/encode.rs`)

- **Silent `movflags` injection for mp4/mov** (`src/encode.rs:321-331`). `write_encode_header` sets `movflags=frag_keyframe+empty_moov+default_base_moof` whenever `format` is `mp4` or `mov`. This is a real behavior change from "default mp4": the output is a fragmented mp4, which many generic mp4 consumers treat differently. PLAN §2G makes no mention of fragmentation. It should either be documented in the README and the SQL signature comment, or driven by an explicit parameter, or (preferred) avoided by letting the muxer produce a non-fragmented mp4 since all writes go into an in-memory buffer that `MemOutput` already supports seeking against.
- **`write_trailer` errors silently swallowed for mp4/mov** (`:143-147`). Combined with the above, this hides failures that would surface as user errors in every other path.
- **Zero-copy boundary is clean** (`Array<'_, &[u8]>` at `:18`). Good.
- **Audio is not produced**. The signature and tests match PLAN: image-sequence → video only, no audio. Worth noting that the plan did not call for audio either, so this is not a gap — but the README could clarify.

## Cross-cutting

- **CI clippy is not `--all-targets`**. `.github/workflows/test.yml:58` runs
  `cargo clippy --features pg${{ matrix.pg_version }} --no-default-features -- -D warnings`.
  PLAN §X4 requires `--all-targets`. Without it, the `disallowed_methods` deny-list in `clippy.toml` is not enforced for tests / benches. Concretely:
  - `src/test_utils.rs:148`, `:272`, `:273`, `:448`, `:565` call `ffmpeg_next::encoder::find(...)` directly.
  - `src/hls.rs:275` calls it too, but is gated on `#[cfg(any(test, feature = "pg_test", feature = "pg_bench"))]`.
  These are exactly the bypasses F7 set out to prevent; clippy would catch them with `--all-targets`.
- **Bench step is gating.** The same workflow runs `cargo pgrx bench "pg${{ matrix.pg_version }}"` as the final step of the `test` matrix (`.github/workflows/test.yml:64`). PLAN §X4 explicitly labels benches "non-gating" and wants them in a separate job. Today, a flaky bench fails the PR check.
- **README missing "when to use HW acceleration" section** (PLAN §X3). The `filter_complex` and `encode` docs expose the `hwaccel` parameter but the README does not explain fallback, supported codecs, or when to opt in.
- **`pipeline::copy_stream` is not uniformly adopted**. `overlay` inlines its own equivalent (see Task 2D). `concat` uses it (`src/concat.rs:180`). Make it the single source of truth.
- **`Vec<u8>` at pgrx boundary is now a repeated pattern**. Functions with `fn <name>(data: Vec<u8>, ...)`: `transcode` (M1), `extract_audio` (M1), `trim` (M1), `generate_gif` (M2), `waveform` (M2), `extract_subtitles` (M2), `overlay` (M2). Functions that correctly use `Array<'_, &[u8]>` for multi-input: `concat`, `filter_complex`, `encode`. The `Array<'_, &[u8]>` approach proves pgrx supports borrowed bytea; single-bytea entry points can be rewritten to `&[u8]` (via `pgrx::composite_type!` / `pgrx::bytea` pattern) the same way. This is the single biggest cross-milestone lever.

## Summary Table

| # | Issue | File:Line | Severity |
|---|-------|-----------|----------|
| 1 | `generate_gif` produces a static single-frame GIF, not an animation | `src/animation.rs:24-26, 147-161` | **CRITICAL** |
| 2 | `extract_subtitles` hand-parses packet bytes instead of using FFmpeg decoder/encoder | `src/subtitles.rs:129-234` | HIGH |
| 3 | `filter_complex` `hwaccel=true` hard-errors | `src/filter_complex.rs:114-116` | HIGH |
| 4 | Parser hardening tests not compiled by `cargo pgrx test` | `src/filter_complex.rs:1194` | HIGH |
| 5 | Frame `.clone()` in output pipelines (Zero-Copy Discipline) | `src/overlay.rs:333`, `src/filter_complex.rs:599, 696`, `src/animation.rs:149` | HIGH |
| 6 | `VideoPipeline` / `AudioPipeline` not used (same as M1) | `src/animation.rs:85+` | HIGH |
| 7 | CI clippy missing `--all-targets`; F7 deny-list under-enforced | `.github/workflows/test.yml:58` | MEDIUM |
| 8 | `encode` silently fragments mp4/mov output | `src/encode.rs:321-331` | MEDIUM |
| 9 | `Vec<u8>` pgrx entry points (Zero-Copy boundary) | `animation.rs:14`, `waveform.rs:13`, `subtitles.rs:10`, `overlay.rs:17-18` | MEDIUM |
| 10 | `limits::check_input_size` not called at entry | `animation.rs:12`, `waveform.rs:12`, `subtitles.rs:9` | MEDIUM |
| 11 | F7 `codec_lookup` not used by `extract_subtitles` | `src/subtitles.rs` (module-wide) | MEDIUM |
| 12 | Missing PLAN-required tests | `overlay.rs` (time_range), `filter_complex.rs` (too_many_inputs, input_count_mismatch), `concat.rs` (codec / sample-rate / channel / sample-format mismatch) | MEDIUM |
| 13 | Bench step is gating on main CI | `.github/workflows/test.yml:64` | MEDIUM |
| 14 | README missing HW-acceleration guidance (X3) | `README.md` | LOW |
| 15 | `pipeline::copy_stream` not uniformly reused | `src/overlay.rs:260-276` | LOW |
| 16 | Module-level `#![allow(dead_code)]` in two M2 files | `src/overlay.rs:1`, `src/filter_complex.rs:1` | LOW |
| 17 | `waveform` allows `ppm` beyond the PLAN's `png/jpeg/jpg` list | `src/waveform.rs:32-35` | LOW |
| 18 | `animation.rs` filename doesn't match PLAN's `gif.rs` entry | `docs/PLAN.md` key files table, `src/animation.rs` | LOW |

## Milestone 2 Gate Verdict

**Not passing as specified.** Two functions have incorrect behavior against their PLAN contracts:

1. **`generate_gif` does not animate.** It decodes one frame at `start_time` and replays it. The PLAN-mandated `palettegen`/`paletteuse` filter graph is absent. A user asking for a 5-second GIF of the first 5 seconds of a video gets a 5-second slideshow of frame 0. This is a correctness bug, not a polish item.
2. **`extract_subtitles` bypasses FFmpeg.** The PLAN is explicit: use the decoder→encoder path. The implementation hand-parses raw packets, which is brittle across containers and codecs.

Additionally, Milestone 1's structural regressions continue to propagate into Milestone 2 unchanged:

3. **Zero-Copy Discipline is still broken**, both at the pgrx entry (`Vec<u8>` arguments) and in the output pipelines (`frame.clone()` per encoded frame). Milestone 2 introduced four new `frame.clone()` sites on the hot path.
4. **`VideoPipeline`/`AudioPipeline` are still avoided.** `generate_gif` reimplements the decode→encode loop rather than using the F1 primitive — same failure mode flagged in `docs/MILESTONE_1_REVIEW.md` for `transcode` and `extract_audio`.
5. **F7 enforcement is still partial** (`extract_subtitles` does not use `codec_lookup`; CI clippy command still does not use `--all-targets`, so the deny-list won't catch test-only bypasses).

On the positive side, `concat` / `concat_agg`, `filter_complex` label parsing, and `encode` are structurally close to PLAN. The `MultiInputGraph` primitive (F1b) landed and is used by both `overlay` and `filter_complex`, which is the one real piece of Foundation reuse Milestone 2 delivered.

**Recommendation**: block the Milestone 2 sign-off until:

1. `generate_gif` is rebuilt on top of `VideoPipeline` + the palettegen/paletteuse filter graph so that it actually produces a moving image.
2. `extract_subtitles` uses FFmpeg's subtitle decoder/encoder (even if only the text-subtitle subset — `subrip`/`ass`/`webvtt`/`mov_text`) and routes codec lookups through `codec_lookup`.
3. `filter_complex` either implements `hwaccel=true` (with software fallback, matching `transcode`) or drops the parameter from the signature.
4. Parser-hardening tests move to `#[cfg(any(test, feature = "pg_test"))]` so CI actually runs them, and the two missing PLAN tests (`rejects_too_many_inputs`, `input_count_mismatch_errors`) are added.
5. The four new `frame.clone()` sites (`overlay.rs:333`, `filter_complex.rs:599`, `:696`, `animation.rs:149`) are removed by threading frames through `send_frame` by value.
6. CI clippy is switched to `--all-targets` and the bench step is moved to a separate, non-gating job.
7. All new `#[pg_extern]` entry points call `limits::check_input_size` and borrow their bytea inputs.
