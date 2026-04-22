# Milestone 1 Review

Review of the current implementation against `PLAN.md` Milestone 1 (Tasks 1A–1E) and the Foundation milestone it depends on.

## Task 1A — `transcode` (`src/transcode.rs`)

- **Zero-copy violation at pgrx boundary**: signature `data: Vec<u8>` (`src/transcode.rs:56`) forces an owned copy. Per F1, the function must accept a borrowed slice.
- **F1 abstraction not honored**: `build_audio_filter_graph` is duplicated locally (`src/transcode.rs:877`) while a version exists in `src/pipeline.rs:304`. A private `AudioTranscodePipeline` (`src/transcode.rs:42-52`) is used instead of the shared `AudioPipeline` that Task 1B also needs.

## Task 1B — `extract_audio` (`src/extract_audio.rs`)

- **Same owned-`Vec<u8>` param** (`src/extract_audio.rs:59`).
- **`AudioPipeline` not used**: re-implements decode→filter→encode inline (`src/extract_audio.rs:184-311`), duplicating 1A's code.
- **Frame clones violate Zero-Copy Discipline**: `src/extract_audio.rs:355`, `:384`, `:447`, `:493` — four `.clone()` calls on `frame::Audio` with no split filter.
- **Late container/codec compat check**: per 1B spec, mismatches must ERROR *before* `encoder.open_as` using the F7 template; current code only errors when FFmpeg bails during open (`src/extract_audio.rs:226` → `:229`/`:555`).
- **Test contract mismatches**: PLAN requires `test_extract_audio_copy_aac_rejects_mp3_container` but the file has `..._rejects_wav_container_stream_copy` (`src/extract_audio.rs:744`); `test_extract_audio_reencode_to_mp3` is missing (replaced by `..._to_wav_pcm`).

## Task 1C — `trim` (`src/trim.rs`)

- **F7 bypass**: `precise_codec_name()` at `src/trim.rs:333` calls `codec::encoder::find` directly — forbidden outside `codec_lookup.rs`.
- **Fallback not validated**: when the source codec has no encoder, the hardcoded `libx264`/`aac` fallback isn't re-checked via `codec_lookup::find_encoder`, so an absent fallback produces a late FFmpeg error instead of the F7-contract ERROR the PLAN mandates (`src/trim.rs:332-349`).
- **Owned `Vec<u8>` params**: `fast_trim` (`src/trim.rs:39`) and `precise_trim` (`src/trim.rs:169`) take `Vec<u8>` instead of `&[u8]`.

## Task 1D — `extract_frames` (`src/extract_frames.rs`)

- **Shared frame encoder not extracted**: PLAN §721 says the helper must be extracted from `thumbnail.rs` in F1; instead `extract_frames` calls `thumbnail::encode_frame` directly (`src/extract_frames.rs:160`), leaving the coupling the extraction was meant to remove.
- Signature, parameter validation, single-pass decode, `max_frames` overflow ERROR, and all five required tests are present and correct.

## Task 1E — `media_info` (`src/media_info.rs`)

Fully compliant: chapters (`src/media_info.rs:67-78`), format tags (`:85`), per-stream `bit_rate`/`disposition`/`tags`/`language` (`:29-36`), and both PLAN-required tests (`:264-327`). No issues found.

## Foundation (F1/F2/F3/F4/F6/F7)

All foundation modules match the spec:

- `MemInput::open(&'a [u8])` borrows (`src/mem_io.rs:83`).
- Filter validation uses `avfilter_graph_parse2` (`src/filter_safety.rs:171`) with the denylist and `drawtext_font_dir` rules.
- GUCs are registered (`src/limits.rs:176-232`) with cumulative `check_output_size` wired into the AVIO write callback (`src/mem_io.rs:187-200`, returning `AVERROR(ENOMEM)`).
- `src/hwaccel.rs` uses a thread-local cache with lazy per-backend probing.
- `src/test_utils.rs` provides the fixtures and `CountingAllocator`.
- `src/codec_lookup.rs` produces the exact error strings from the PLAN error contract.

## Summary Table

| Issue | File:Line | Severity |
|-------|-----------|----------|
| Owned `Vec<u8>` param (vs borrow) | `transcode.rs:56`, `extract_audio.rs:59`, `trim.rs:39,169` | HIGH |
| `build_audio_filter_graph` duplicated | `transcode.rs:877` | HIGH |
| `AudioPipeline` not used (custom pipeline) | `extract_audio.rs:184-311`, `transcode.rs:42-52` | HIGH |
| Frame clones in `extract_audio.rs` | `:355, :384, :447, :493` | HIGH |
| F7 bypass (direct `codec::encoder::find`) | `trim.rs:333` | HIGH |
| Fallback encoder not validated via F7 | `trim.rs:332-349` | MEDIUM |
| Container/codec compat check missing pre-open | `extract_audio.rs:226` | MEDIUM |
| Test name mismatch (`..._mp3_container`) | `extract_audio.rs:744` | MEDIUM |
| Missing `test_extract_audio_reencode_to_mp3` | `extract_audio.rs` | MEDIUM |
| Frame encoder helper not extracted from `thumbnail.rs` | `extract_frames.rs:160` | LOW |

## Milestone 1 Gate Verdict

**Not passing as specified.** The foundation is solid, but the Core-APIs layer has three structural regressions against its own PLAN:

1. **`AudioPipeline` was never actually built/used.** Both `transcode` and `extract_audio` reimplement it inline — the exact duplication F1 was supposed to prevent — and this will compound in Milestone 2 (`overlay`, `filter_complex`, `concat`, `encode` all assume it exists).
2. **Zero-Copy Discipline is broken where it matters most.** The four audio-frame clones in `extract_audio.rs` plus the `Vec<u8>` entry-point copies across 1A/1B/1C are precisely the "concurrency × unnecessary copies" failure mode the PLAN calls out. The boundary allocation tests F6 was created to enforce don't appear to be wired up for these functions.
3. **F7 enforcement has a hole.** `trim.rs:333` calls `codec::encoder::find` directly; the `clippy.toml` deny-list should have caught this, so either the lint isn't running or the disallowed-methods list is incomplete.

Secondary issues (test-name drift, missing `mp3` re-encode test, late container/codec compat check, un-extracted frame encoder helper) are smaller but each one weakens the acceptance contract the PLAN deliberately wrote down.

**Recommendation**: block the M1 → M2 gate until:

1. `AudioPipeline` is genuinely shared in `src/pipeline.rs` and consumed by both `transcode` and `extract_audio`.
2. The four frame clones in `extract_audio` are removed.
3. `trim` routes all codec lookups through `codec_lookup`, and the fallback encoder is validated via the F7 contract.
4. `clippy.toml` deny-list is verified to catch direct `codec::encoder::find` usage.
5. Missing/misnamed tests are reconciled with the PLAN's acceptance list.
