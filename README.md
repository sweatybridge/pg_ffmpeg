# pg_ffmpeg

A PostgreSQL extension that exposes FFmpeg media processing functions, built with [pgrx](https://github.com/pgcentralfoundation/pgrx).

## Functions

All functions are in the `ffmpeg` schema.

| Function | Description |
|----------|-------------|
| `media_info(data bytea) → jsonb` | Extract metadata: format, duration, codecs, resolution, streams |
| `thumbnail(data bytea, seconds float8 DEFAULT 0.0) → bytea` | Extract a video frame as PPM image at the given timestamp |
| `transcode(data bytea, format text) → bytea` | Remux media into a different container format |
| `extract_audio(data bytea, format text DEFAULT 'mp3') → bytea` | Extract the audio track from a video file |
| `hls(url text, segment_duration int DEFAULT 6) → bigint` | Fetch a video via URL, split into HLS segments, and store in `ffmpeg.hls_playlists` / `ffmpeg.hls_segments` |

## Prerequisites

- PostgreSQL 14–17
- Rust toolchain
- `cargo-pgrx` (`cargo install cargo-pgrx`)
- FFmpeg development libraries:

```bash
# Debian/Ubuntu
apt-get install libavcodec-dev libavformat-dev libavutil-dev \
  libavfilter-dev libswscale-dev libswresample-dev \
  clang libclang-dev pkg-config
```

## Build & Install

```bash
cargo pgrx init --pg16=$(which pg_config)  # adjust for your PG version
cargo pgrx install --release
```

## Usage

```sql
CREATE EXTENSION pg_ffmpeg;

-- Get media metadata
SELECT ffmpeg.media_info(pg_read_binary_file('/path/to/video.mp4'));

-- Extract a thumbnail at 5 seconds
SELECT ffmpeg.thumbnail(pg_read_binary_file('/path/to/video.mp4'), 5.0);

-- Remux to MKV
SELECT ffmpeg.transcode(pg_read_binary_file('/path/to/video.mp4'), 'matroska');

-- Extract audio as MP3
SELECT ffmpeg.extract_audio(pg_read_binary_file('/path/to/video.mp4'), 'mp3');

-- Split a remote video into HLS segments (6s default)
SELECT ffmpeg.hls('https://example.com/video.mp4', 6);
```

## License

MIT
