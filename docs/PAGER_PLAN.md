# Implementation plan: `pg_bager`

A small Rust binary that psql invokes via `PSQL_PAGER`. It detects PNG-shaped
bytea cells in psql's output stream and replaces them with terminal graphics
escapes for Kitty or iTerm2.

This is a **filter masquerading as a pager**, so the user trades normal pager
navigation (search, scrollback, less-style keys) for inline images. The plan
addresses that tradeoff explicitly under "Invocation" and "Chained pager"
below.

## Scope (v1)

Tightened per review feedback to a small, demonstrable core:

- **Format**: PNG `bytea` only. JPEG/GIF/WebP are deferred to v2 because
  Kitty's inline graphics protocol expects PNG (`f=100`) or raw pixel buffers
  (`f=24`/`f=32`); supporting other compressed formats requires a transcode
  step we don't want in v1.
- **Terminals**: Kitty graphics protocol and iTerm2 inline images. Sixel and
  auto-detection are deferred (see "Out of scope").
- **Input**: psql's already-formatted text for **single-column result rows**
  only (aligned, unaligned, expanded display mode). No DB connection; no
  ffmpeg calls.
- **Multi-column output**: pass through unchanged to the default pager path.
  v1 must not attempt to preserve or rewrite arbitrary multi-column psql
  layouts.
- **Invocation**: requires forced pager mode. Without `\pset pager always`
  (or `psql -P pager=always`) psql only spawns the pager when output exceeds
  the terminal height, so small thumbnail queries never reach this binary.
  v1 docs and integration tests must set this explicitly.

## Crate layout

1. Convert repo root to a Cargo workspace.
   - Add `[workspace] members = ["pager"]` to the root `Cargo.toml`. The root
     package is implicitly a workspace member when `[package]` and
     `[workspace]` share the same manifest, so it does not need to be listed.
     Existing `cdylib` build stays intact.
   - Verify (must all pass):
     - `cargo build -p pg_ffmpeg`
     - `cargo build -p pg_bager`
     - `cargo pgrx package` — confirms the extension still installs
       correctly under pgrx; the workspace must not break the cdylib's
       artifact paths.
     - `cargo pgrx test` — confirms existing extension tests still run.
2. New crate `pager/` with `Cargo.toml` (`name = "pg_bager"`,
   `[[bin]] name = "pg_bager"`), deps: `base64` (streaming Kitty/iTerm2
   payloads), `png` (streaming PNG validation / metadata read), hand-rolled
   hex decode, no async runtime, no regex crate (manual scan to keep it
   small).

## Invocation and chained pager

- Document in `pager/README.md`:
  - `export PSQL_PAGER=pg_bager`
  - `\pset pager always` in psql, or `psql -P pager=always`.
  - This binary does not provide pager navigation. For eligible single-column
    image output, the default behavior writes directly to the terminal, which
    gives correct image rendering but no scrollback / search.
  - Passthrough cases (multi-column output, disabled mode, unsupported
    formats, oversized rows) use the default pager path: `PG_BAGER_FALLBACK` if
    set, otherwise `$PAGER`, falling back to `less -R`, then `cat` if no pager
    executable is available.
  - Optional: set `PG_BAGER_FALLBACK` to a pager command to chain output for all
    cases. **Caveats**:
    - Most pagers strip or display-as-text terminal control sequences by
      default. `less` requires `-R` ("output raw control characters") to
      forward common SGR escapes, but `-R` does **not** guarantee that
      Kitty APC graphics sequences (`\x1b_G...\x1b\\`) or iTerm2 OSC 1337
      sequences pass through correctly, and `less` repaints the screen
      on scroll which destroys already-rendered images.
    - A known-good v1 invocation for plain text only:
      `PG_BAGER_FALLBACK='less -R'`. Inline images may render on
      first paint and disappear on scroll; this is a `less` limitation,
      not a bug in this binary.
    - `PG_BAGER_FALLBACK='cat'` is the no-op chain (rarely useful).
    - For reliable inline images in single-column output, leave
      `PG_BAGER_FALLBACK` unset.
  - Multi-column output always uses the default pager path and receives the
    original, unmodified stream.
- Verify: integration test runs psql with both `PSQL_PAGER` and
  `\pset pager always` and confirms the binary is invoked. A second
  test exercises `PG_BAGER_FALLBACK='less -R'` and asserts that the
  fallback process receives the rewritten stream — it does **not** assert
  that images survive scrolling in `less`, since they don't.

## Module breakdown (`pager/src/`)

- `main.rs` — argv/env wiring, opens stdin/stdout (or stdin → fallback pager
  stdin), dispatches to `stream::run`.
- `term.rs` — detect protocol, env-driven only in v1:
  - `PG_BAGER_PROTOCOL=kitty|iterm2|none` — explicit override.
  - Otherwise:
    - Kitty: `TERM == "xterm-kitty"` or `KITTY_WINDOW_ID` set or
      `TERM_PROGRAM == "WezTerm"`.
    - iTerm2: `TERM_PROGRAM == "iTerm.app"`.
    - Fallback: `Protocol::None`.
  - **No TTY probing in v1.** Device-Attributes queries on the controlling
    tty require raw mode, a timeout, and careful interaction with
    concurrent terminal input — out of scope for the first cut. Sixel users
    must opt in via the env var (and the v2 milestone will add an encoder).
  - Verify: unit tests with env-var fixtures.
- `scan.rs` — **byte-stream scanner with bounded lookahead**. The previous
  line-based design was wrong: psql emits a `bytea` value as one very long
  physical line, so reading whole lines defeats bounded memory. Instead:
  - Read fixed-size chunks (e.g. 64 KiB) from stdin into a rolling buffer.
  - Search for the literal start sequence `\x` (single backslash, lowercase
    `x`) followed by `[0-9A-Fa-f]`. In a Rust regex literal that pattern is
    written `r"\\x[0-9A-Fa-f]{16,}"`; the doubled backslash is the regex
    escape, not part of the input bytes.
  - Before rewriting any token, the layout layer must classify the result as
    single-column. If the stream is multi-column or ambiguous, switch to
    passthrough mode and send all buffered plus remaining input unchanged to
    the default pager path.
  - On a match, stream hex pairs through a decoder while the physical row
    remains within `PG_BAGER_MAX_ROW_BYTES`. If the row or token exceeds that
    cap, abort the rewrite for that row, flush the original bytes verbatim to
    the default pager path, and resume passthrough.
  - Feed hex-decoded bytes incrementally into `png_stream.rs`, which validates
    the PNG signature and parses enough PNG structure to confirm a complete PNG
    and read IHDR metadata. Do **not** decode the full raster into pixels in
    v1; terminal protocols consume the original compressed PNG bytes.
  - Tee the compressed PNG bytes into a bounded spool owned by the candidate
    token (memory buffer first; spill to a temp file is acceptable if the
    implementation wants lower RSS). The spool is capped by
    `PG_BAGER_MAX_ROW_BYTES`.
  - When the hex run ends and the streaming PNG parser reports one complete
    valid PNG, hand off to the layout layer with `(original_token, png_spool,
    png_metadata)`.
  - Pass everything else through unchanged.
  - Verify: unit tests fed pre-canned byte streams that include
    >LINE_MAX-byte rows; assert peak resident memory stays bounded.
- `png_stream.rs` — streaming PNG validation and metadata extraction:
  - Accept hex-decoded compressed PNG bytes in small chunks from `scan.rs`.
  - Validate the 8-byte PNG signature, IHDR, chunk lengths/CRCs, and IEND.
  - Expose width/height from IHDR for sizing decisions.
  - Reject trailing non-PNG data inside the hex token for v1; fall back to
    passthrough rather than trying to render partial or concatenated images.
  - Never allocate an RGBA frame buffer.
- `encode.rs` — protocol encoders. Signature:

  ```rust
  fn write_png(out: &mut dyn Write, original: &str, png: &mut dyn Read) -> io::Result<()>;
  ```

  Encoders base64-encode from the compressed PNG stream; they do not receive or
  require decoded pixel bytes. Once a byte has reached `out` the terminal owns
  it and we can't roll back, so fallback is only meaningful before any protocol
  bytes have been emitted. Concretely:
  - PNG validation or spool-open error before writing protocol bytes → write
    `original` to `out` instead and return `Ok(())`.
  - Protocol write failure after the first escape byte → propagate the
    `io::Error`. The stream is unrecoverable; we don't try to "fix"
    half-written escape sequences.

  - `kitty::write` — emits chunked Kitty graphics frames for PNG payloads
    only. Per the protocol, `m=1` means "more chunks follow" and `m=0`
    (or omitting the `m` key) marks the final chunk. The encoder must:
    - For payloads that fit in one chunk (≤ 4096 base64 bytes), emit a
      single frame with no `m` key:
      `\x1b_Gf=100,a=T;<base64>\x1b\\`.
    - For larger payloads, emit the first chunk with the full key list
      and `m=1`, intermediate chunks with `m=1` only, and the **final**
      chunk with `m=0` (terminals wait for more data otherwise and the
      image never paints).
    Non-PNG formats are not supported in v1.
    The encoder base64-encodes from `png` into 4096-byte protocol chunks; it
    must not build one giant base64 string for large PNGs.
  - `iterm2::write` —
    `\x1b]1337;File=inline=1;size=<n>;preserveAspectRatio=1:<base64>\x07`.
    iTerm2 requires one OSC payload, so the encoder may need to stream into a
    bounded temp/spool buffer before the final write; it must not allocate a
    decoded image buffer.
  - `none::write` — writes `original` verbatim.

- `layout.rs` — single source of truth for *whether* and *how* a recognized
  cell is rewritten. The scanner only identifies tokens; layout decides the
  emission shape and owns single-column detection.
  - v1 rewrite eligibility: exactly one result column. Multi-column output,
    footer-only output, or ambiguous format detection must pass through
    unchanged to the default pager path.
  - Single-column `aligned` output: replace the matched hex token in-place
    with a fixed-width placeholder (e.g. `[img]` padded to the original token
    width) so the one-column border stays aligned. Buffer the row up to its
    terminator (`\n`), flush it, then emit the image-protocol escape sequence
    on the following line(s).
  - Single-column `unaligned` output: replace the token with `[img]` and emit
    the image escape immediately after the row's record separator.
  - Single-column `expanded` display mode: no width constraint — emit the
    image directly after the value line.
  - Detecting single-column mode:
    - aligned: parse the header separator and data rows; more than one cell
      delimiter (`|`) in a row means multi-column passthrough;
    - unaligned: no field separator in the data row means single-column; one
      or more separators means multi-column passthrough;
    - expanded: one field/value line per record means single-column; multiple
      field/value lines per record means multi-column passthrough.
    Detection should happen from the initial buffered prelude plus the first
    candidate data row. Misclassification must prefer passthrough.
  - Verify: snapshot tests against captured single-column `psql -A`, default
    aligned, and `\x` (expanded) outputs, plus multi-column fixtures that
    assert byte-for-byte passthrough.
- `config.rs` — env-driven knobs:
  - `PG_BAGER_MAX_ROW_BYTES` defaults to PostgreSQL's maximum TOAST-able datum
    size (the implementation should define this as a named constant matching
    PostgreSQL's documented maximum field size, approximately 1 GiB). This is
    the physical row/token buffer cap for single-column placeholder rewrite;
    rows beyond it pass through unchanged. There is no separate decoded image
    byte cap in v1.
  - `PG_BAGER_MAX_PIXELS_W/H` — passed to Kitty/iTerm2 for sizing.
  - `PG_BAGER_DISABLE=1` — full passthrough.
  - `PG_BAGER_FALLBACK` — chained/default pager command override (see
    "Invocation and chained pager" for caveats).

## Streaming and back-pressure

- Chunked byte-stream loop, **not** line-buffered.
- Bounded buffering. The scanner holds at most:
  - the rolling read chunk (e.g. 64 KiB);
  - one in-progress hex token capped by the current physical row cap;
  - the compressed PNG spool for that token, bounded by roughly half the hex
    token size;
  - small streaming PNG decoder/parser state;
  - in single-column aligned mode, the current partial physical row capped at
    `MAX_ROW_BYTES`.
- **`MAX_ROW_BYTES` defaults to PostgreSQL's maximum TOAST-able datum size**,
  not a small constant derived from an image cap. Hex-encoded `bytea` is
  ~2× the compressed PNG size, plus `\x` prefix, column padding, and borders;
  the row cap exists only to prevent unbounded physical-row buffering.
  Override via `PG_BAGER_MAX_ROW_BYTES` only for test or constrained
  environments.
- Single-column aligned rows that exceed `MAX_ROW_BYTES` are passed through
  verbatim to the default pager path (no placeholder rewrite, no inline
  image) so the binary degrades gracefully instead of dropping data.
- Multi-column output is never rewritten. Once detected, flush any buffered
  bytes unchanged to the default pager path and stream the remainder without
  scanning for image replacements.
- Flush stdout after every row terminator so psql's progressive output
  stays interactive.
- Verify: feed a 100k-row result set with one ~100 MiB bytea cell;
  assert RSS stays under the rolling chunk plus the row/token buffers, the PNG
  compressed-byte spool, and small decoder state implied by `MAX_ROW_BYTES`,
  with `MAX_ROW_BYTES` set to the TOAST-size default.

## Error handling

- PNG validation/spool-open/encoding setup error before any protocol bytes hit
  `out` → encoder writes the original token verbatim and returns `Ok(())`.
  Encoders may stream protocol bytes after validation starts, but a partial
  escape sequence on stdout cannot be undone.
- Write failure after protocol output begins → propagate `io::Error` and exit
  with non-zero status. The stream is unrecoverable.
- Decode errors (odd-length hex, non-hex character mid-run) → emit
  original bytes; do not abort.
- Token or row exceeds `MAX_ROW_BYTES` → emit original bytes through the
  default pager path; log to stderr at most once per run.
- Multi-column or ambiguous output → pass the whole stream through unchanged
  to the default pager path; log once in debug/verbose mode only.
- Single-column aligned row exceeds `MAX_ROW_BYTES` → fall back to passthrough
  for that row (no placeholder rewrite, no inline image); log once. See
  "Streaming and back-pressure" for the cap derivation.
- Never panic on malformed input.

## Tests

1. Unit: `term::detect` across env permutations (Kitty / iTerm2 / WezTerm
   / explicit override / fallback).
2. Unit: `scan` finds PNG tokens in single-column aligned/unaligned/expanded
   fixtures; ignores non-PNG bytea (`\xdeadbeef`, JPEG/GIF/WebP magic —
   those are passed through verbatim in v1).
3. Unit: `png_stream` accepts chunked valid PNG bytes, rejects bad signatures,
   bad CRCs, missing IEND, and trailing bytes, and exposes IHDR width/height
   without allocating an RGBA frame.
4. Unit: scanner with a single-row, multi-MiB bytea cell respects
   `MAX_ROW_BYTES` and stays under a memory budget.
5. Unit/golden: multi-column aligned, unaligned, and expanded fixtures pass
   through byte-for-byte to the default pager path; no image escape is emitted.
6. Golden: feed canned single-column psql output + a known PNG; assert exact
   stdout bytes for Kitty, iTerm2, and `none`.
7. Integration (gated on `cargo test --features it`): launch real psql
   with `PSQL_PAGER=target/debug/pg_bager` **and**
   `\pset pager always`, run `SELECT thumbnail(...)`, assert the pager
   exits 0 and emits an escape sequence.
8. Integration: launch a two-column query containing one PNG-shaped bytea and
   one scalar column; assert the stream is delivered unchanged to the default
   pager path.
9. Workspace regression: `cargo pgrx test` continues to pass after the
   workspace conversion.

## Docs and packaging

- `pager/README.md`: install, both `PSQL_PAGER` *and* `\pset pager always`
  required, env knobs, supported terminals (Kitty, iTerm2 in v1),
  known limitations (single-column rows only; multi-column output passes
  through to the default pager path; no scrollback by default; `PG_BAGER_FALLBACK`
  exists but does not reliably preserve images through pagers like
  `less`; PNG only in v1).
- Add a row to top-level `README.md` linking to the pager.
- `cargo install --path pager` works standalone.

## Milestones / verify gates

1. Workspace + empty pager binary that is a pure passthrough →
   `cargo pgrx test` still green; `psql` runs unchanged with
   `PSQL_PAGER=pg_bager` and `\pset pager always`.
2. Byte-stream PNG scanner + streaming PNG validator + Kitty encoder → verify: a
   single-column `SELECT thumbnail(video)` in Kitty shows the image;
   non-Kitty terminals unchanged.
3. iTerm2 encoder → verify: same query in iTerm2.
4. Single-column layout preservation → verify: borders stay aligned in
   default psql mode, and multi-column queries pass through unchanged to the
   default pager path.
5. `PG_BAGER_FALLBACK='less -R'` chaining → verify: the rewritten
   stream reaches `less`'s stdin and text scrollback works. Inline
   images surviving scroll is **not** a v1 acceptance criterion (see
   "Invocation and chained pager" caveats).
6. Docs, tests, `cargo install` story → ship.

## Out of scope (call out, don't build)

- JPEG / GIF / WebP rendering. Requires a transcode-to-PNG step (or
  decoder for raw RGB(A) Kitty frames) we don't want in v1.
- Sixel encoder.
- Sixel/automatic terminal probing via Device Attributes — needs raw-mode
  TTY handling and a timeout; deferred.
- Calling back into Postgres to transcode video bytea → PNG via
  `pg_ffmpeg`. Requires libpq or a side channel; revisit after v1.
- Native (in-process) sixel and non-PNG image decoders.
- Windows terminal protocols.
