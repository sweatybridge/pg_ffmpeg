# pg_bager

`pg_bager` is a small `psql` pager filter for rendering PNG-shaped `bytea`
cells inline in terminals that support image escape sequences.

It is intentionally narrow in v1:

- PNG `bytea` output only.
- Kitty graphics protocol and iTerm2 inline images.
- Single-column `psql` result rows only, in aligned, unaligned, or expanded
  display modes.
- Multi-column, unsupported, disabled, or oversized output is passed through to
  a normal pager path unchanged.

## Install

```bash
cargo install --path pager
```

From this repository you can also build it with:

```bash
cargo build -p pg_bager
```

## Use With psql

```bash
export PSQL_PAGER=pg_bager
psql -P pager=always
```

Inside an existing `psql` session:

```sql
\pset pager always
```

Forced pager mode is required. Without `\pset pager always` or
`psql -P pager=always`, `psql` only starts the pager when output exceeds the
terminal height, so small image queries will never reach `pg_bager`.

Example:

```sql
SELECT ffmpeg.thumbnail(pg_read_binary_file('/path/to/video.mp4'));
```

## Pager Behavior

`pg_bager` is a filter masquerading as a pager. For eligible single-column image
output it writes directly to the terminal so the image escape can render
reliably. It does not provide pager navigation, search, or `less`-style keys.

Passthrough cases use:

1. `PG_BAGER_FALLBACK`, if set.
2. `$PAGER`, if set.
3. `less -R`, when `less` is available.
4. `cat`.

Set `PG_BAGER_FALLBACK` to force a chained pager command for all output,
including rewritten image output:

```bash
PG_BAGER_FALLBACK='less -R' PSQL_PAGER=pg_bager psql -P pager=always
```

Most pagers strip or display terminal image controls as text by default.
`less -R` forwards common SGR escapes, but it does not guarantee that Kitty APC
graphics sequences or iTerm2 OSC 1337 sequences survive repainting. Inline
images may render on first paint and disappear when scrolling; that is a pager
limitation, not a `pg_bager` bug. Leave `PG_BAGER_FALLBACK` unset for reliable
single-column inline images.

`PG_BAGER_FALLBACK='cat'` is the no-op chain.

## Environment

| Variable | Description |
| --- | --- |
| `PG_BAGER_PROTOCOL=kitty\|iterm2\|none` | Explicit terminal protocol override. |
| `PG_BAGER_DISABLE=1` | Disable rewriting and pass through unchanged. |
| `PG_BAGER_FALLBACK='<command>'` | Pager command used for all output. |
| `PG_BAGER_MAX_ROW_BYTES=<n>` | Physical row/token cap. Defaults to PostgreSQL's approximate maximum TOAST-able datum size, 1 GiB. |
| `PG_BAGER_MAX_PIXELS_W=<n>` | Optional maximum rendered image width hint. |
| `PG_BAGER_MAX_PIXELS_H=<n>` | Optional maximum rendered image height hint. |

Without `PG_BAGER_PROTOCOL`, detection is environment-only:

- Kitty: `TERM=xterm-kitty`, `KITTY_WINDOW_ID` set, or `TERM_PROGRAM=WezTerm`.
- iTerm2: `TERM_PROGRAM=iTerm.app`.
- Otherwise: no image protocol.

There is no terminal probing in v1.

## Limitations

Multi-column output is never rewritten. It is passed byte-for-byte to the
fallback pager path.

JPEG, GIF, WebP, Sixel, automatic terminal probing, and calls back into
PostgreSQL or FFmpeg are out of scope for v1.
