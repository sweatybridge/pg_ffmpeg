# Implementation plan: `pg_ffmpeg_pager`

A small Rust binary that psql invokes via `PSQL_PAGER`, detects image-shaped
bytea cells in psql's output stream, and replaces them with terminal graphics
escapes.

## Scope (v1)

- Input: psql's already-formatted text on stdin (aligned, unaligned, and
  `expanded` formats).
- Detection: bytea hex literals for PNG/JPEG/GIF/WebP. Magic bytes (after
  hex-decoding the cell content; `??` marks variable bytes the scanner
  ignores):
  - PNG: `89 50 4E 47 0D 0A 1A 0A`
  - JPEG: `FF D8 FF`
  - GIF: `47 49 46 38 (37|39) 61` (`GIF87a` / `GIF89a`)
  - WebP: `52 49 46 46 ?? ?? ?? ?? 57 45 42 50` (`RIFF <4-byte size> WEBP`)
- Output: pass text through unchanged, but replace recognized bytea cells with
  inline-image escapes for the active terminal protocol.
- Terminals: Kitty graphics protocol, iTerm2 inline images, Sixel; fallback =
  identity passthrough.
- No DB connection, no ffmpeg calls in v1. Calling into `pg_ffmpeg` for
  non-image bytea (video frames → PNG) is explicitly out of scope; noted as v2.

## Crate layout

1. Convert repo root to a Cargo workspace.
   - Add `[workspace] members = ["pager"]` to the root `Cargo.toml`. The root
     package is implicitly a workspace member when `[package]` and
     `[workspace]` share the same manifest, so it does not need to be listed.
     Existing `cdylib` build stays intact.
   - Verify: `cargo build -p pg_ffmpeg` and `cargo build -p pg_ffmpeg_pager`
     both succeed.
2. New crate `pager/` with `Cargo.toml` (`name = "pg_ffmpeg_pager"`,
   `[[bin]] name = "pg_ffmpeg_pager"`), deps: `base16ct` (or hand-rolled hex),
   `atty`, `base64` (iTerm2/Kitty payloads), `terminfo` or env sniffing, no
   async runtime.

## Module breakdown (`pager/src/`)

- `main.rs` — argv/env wiring, opens stdin/stdout, dispatches to `stream::run`.
- `term.rs` — detect protocol:
  - Kitty: `TERM == "xterm-kitty"` or `KITTY_WINDOW_ID` set or
    `TERM_PROGRAM == "WezTerm"`.
  - iTerm2: `TERM_PROGRAM == "iTerm.app"`.
  - Sixel: query `\x1b[c` on the controlling tty, parse Device Attributes for
    `;4;`. Skip the query if stdout is not a tty.
  - Fallback: `Protocol::None`.
  - Override via `PG_FFMPEG_PAGER_PROTOCOL=kitty|iterm2|sixel|none`.
  - Verify: unit tests with env-var fixtures.
- `scan.rs` — line-oriented scanner that finds bytea hex tokens. Logic:
  - Read input line by line (`BufReader`).
  - Scan for psql's hex bytea token in the input stream: the literal
    sequence `\x` (single backslash, lowercase `x`) followed by a run of
    `[0-9A-Fa-f]` of length ≥ 16 (enough hex for any of the magics above).
    In a Rust regex literal this is written `r"\\x[0-9A-Fa-f]{16,}"`; the
    double backslash is the regex escape, not part of the input.
  - Decode the first N bytes, sniff magic, classify as
    `Png|Jpeg|Gif|Webp|Other`.
  - For matches, emit: original line up to token start → image escape with
    full decoded bytes → newline → original line continuation. (Most psql
    formats put one cell per line section; aligned format may need
    column-width preservation — see "Layout" below.)
  - Verify: golden tests with captured psql outputs (aligned, unaligned,
    and expanded display mode on/off).
- `encode.rs` — protocol encoders, each takes `&[u8]` and writes to
  `&mut dyn Write`:
  - `kitty::write` — chunked `\x1b_Gf=100,a=T,m=1;<base64>\x1b\\` frames per
    the Kitty graphics protocol; payloads ≤ 4096 bytes per chunk.
  - `iterm2::write` — `\x1b]1337;File=inline=1;size=<n>;preserveAspectRatio=1:<base64>\x07`.
  - `sixel::write` — convert PNG/JPEG/etc. to sixel. Avoid pulling a heavy
    decoder in v1: shell out to `img2sixel` if available; if not, downgrade
    to passthrough and log once to stderr. (Document this as a known
    limitation; native sixel encoder is a v2 item.)
  - `none::write` — write the original hex unchanged.
- `layout.rs` — small helpers to keep psql's table grid intact:
  - For `aligned` output, replace the matched hex token with a fixed-width
    placeholder (e.g. `[img]` padded to original token width) on the table
    line, then emit the inline-image escape on its own line *after* the row
    terminator. This keeps column borders aligned even on terminals that
    render images out-of-band.
  - For `expanded` (`\x`) output, no width constraint — emit the image
    directly after the value line.
  - Verify: snapshot tests against `psql -A`, default aligned, and `\x`
    outputs.
- `config.rs` — env-driven knobs:
  - `PG_FFMPEG_PAGER_MAX_BYTES` (default 4 MiB) — skip cells larger than this.
  - `PG_FFMPEG_PAGER_MAX_PIXELS_W/H` — pass through to Kitty/iTerm2 for
    sizing.
  - `PG_FFMPEG_PAGER_DISABLE=1` — full passthrough.

## Streaming and back-pressure

- Single-threaded `BufReader`/`BufWriter` loop, line buffered.
- Never buffer the whole input. The only buffering is per matched hex token
  (bounded by `MAX_BYTES`).
- Flush stdout after each line so psql's progressive output stays interactive.
- Verify: feed a 100k-row result set; pager memory stays flat.

## Error handling

- All terminal-protocol write failures → fall back to writing the original
  hex for that cell, continue.
- Decode errors → passthrough that token.
- Never panic on malformed hex; log to stderr at most once per run.

## Tests

1. Unit: `term::detect` across env permutations.
2. Unit: `scan` finds tokens in aligned/unaligned/`\x` fixtures; ignores
   non-image bytea (e.g. random `\xdeadbeef`).
3. Golden: feed canned psql output + a known PNG; assert exact stdout bytes
   for Kitty, iTerm2, none. Sixel test gated on `img2sixel` being on PATH.
4. Integration (optional, gated on `cargo test --features it`): start a real
   psql with `PSQL_PAGER=target/debug/pg_ffmpeg_pager`, run
   `SELECT thumbnail(...)`, assert the pager exits 0 and emits an escape
   sequence.

## Docs and packaging

- `pager/README.md`: install, `export PSQL_PAGER=pg_ffmpeg_pager`, env knobs,
  supported terminals, known limitations.
- Add a row to top-level `README.md` linking to the pager.
- `cargo install --path pager` works standalone.

## Milestones / verify gates

1. Workspace + empty pager binary that is a pure passthrough → verify: `psql`
   runs unchanged with `PSQL_PAGER=pg_ffmpeg_pager`.
2. `term::detect` + Kitty encoder + PNG sniff → verify: a
   `SELECT thumbnail(video)` in Kitty shows the image; other terminals
   unchanged.
3. JPEG/GIF/WebP sniff + iTerm2 encoder → verify: same query in iTerm2.
4. Aligned-format layout preservation → verify: borders stay aligned in
   `psql` default mode.
5. Sixel via `img2sixel` shellout + size/disable knobs → verify: works in
   xterm with sixel build.
6. Docs, tests, `cargo install` story → ship.

## Out of scope (call out, don't build)

- Calling back into Postgres to transcode video bytea → PNG. Requires either
  embedding libpq or a separate side-channel; revisit once v1 lands.
- Native sixel encoder.
- Windows terminal protocols.
