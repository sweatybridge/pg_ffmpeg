# pg_ffmpeg

A PostgreSQL extension that exposes FFmpeg media processing functions, built with [pgrx](https://github.com/pgcentralfoundation/pgrx).

## Functions

All functions are in the `ffmpeg` schema.

| Function | Description |
|----------|-------------|
| `media_info(data bytea) → jsonb` | Extract metadata: format, duration, codecs, resolution, streams |
| `thumbnail(data bytea, seconds float8 DEFAULT 0.0, format text DEFAULT 'png') → bytea` | Extract a video frame as an image at the given timestamp |
| `transcode(data bytea, format text DEFAULT NULL, filter text DEFAULT NULL) → bytea` | Remux media into a different container format, optionally applying a filter |
| `extract_audio(data bytea, format text DEFAULT 'mp3') → bytea` | Extract the audio track from a video file |
| `hls(url text, segment_duration int DEFAULT 6) → bigint` | Fetch a video via URL, split into HLS segments, and store in `ffmpeg.hls_playlists` / `ffmpeg.hls_segments` |

## Prerequisites

- PostgreSQL 16–18
- Rust toolchain
- `cargo-pgrx` (`cargo install cargo-pgrx`)
- FFmpeg development libraries:

```bash
# Debian/Ubuntu
apt-get install libavcodec-dev libavformat-dev libavutil-dev \
  libavfilter-dev libavdevice-dev libswscale-dev libswresample-dev \
  clang libclang-dev pkg-config \
  build-essential libreadline-dev zlib1g-dev flex bison \
  libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache
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

-- Extract a thumbnail at 5 seconds (PNG by default)
SELECT ffmpeg.thumbnail(pg_read_binary_file('/path/to/video.mp4'), seconds => 5.0);

-- Extract a thumbnail as JPEG
SELECT ffmpeg.thumbnail(pg_read_binary_file('/path/to/video.mp4'), format => 'mjpeg');

-- Remux to MKV
SELECT ffmpeg.transcode(pg_read_binary_file('/path/to/video.mp4'), format => 'matroska');

-- Transcode with a filter (e.g. scale to 720p)
SELECT ffmpeg.transcode(pg_read_binary_file('/path/to/video.mp4'), filter => 'scale=-1:720');

-- Extract audio as MP3
SELECT ffmpeg.extract_audio(pg_read_binary_file('/path/to/video.mp4'), format => 'mp3');

-- Split a remote video into HLS segments (6s default)
SELECT ffmpeg.hls('https://example.com/video.mp4', segment_duration => 6);
```

## License

MIT
