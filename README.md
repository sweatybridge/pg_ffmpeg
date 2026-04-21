# pg_ffmpeg

A PostgreSQL extension that exposes FFmpeg media processing functions, built with [pgrx](https://github.com/pgcentralfoundation/pgrx).

## Functions

All functions are in the `ffmpeg` schema.

| Function | Description |
|----------|-------------|
| `media_info(data bytea) -> jsonb` | Extract container metadata, chapters, tags, stream codecs, bit rates, dispositions, and per-stream tags |
| `thumbnail(data bytea, seconds float8 DEFAULT 0.0, format text DEFAULT 'png') -> bytea` | Extract a video frame as PNG or JPEG |
| `transcode(data bytea, format text DEFAULT NULL, filter text DEFAULT NULL, codec text DEFAULT NULL, preset text DEFAULT NULL, crf int DEFAULT NULL, bitrate int DEFAULT NULL, audio_codec text DEFAULT NULL, audio_filter text DEFAULT NULL, audio_bitrate int DEFAULT NULL, hwaccel bool DEFAULT false) -> bytea` | Remux or transcode audio/video, optionally with validated filter graphs |
| `extract_audio(data bytea, format text DEFAULT NULL, codec text DEFAULT NULL, bitrate int DEFAULT NULL, sample_rate int DEFAULT NULL, channels int DEFAULT NULL, filter text DEFAULT NULL) -> bytea` | Stream-copy compatible audio tracks or re-encode audio with optional filtering |
| `trim(data bytea, start_time float8 DEFAULT 0.0, end_time float8 DEFAULT NULL, precise bool DEFAULT false) -> bytea` | Trim media with either keyframe-aligned stream copy or frame-accurate re-encode |
| `extract_frames(data bytea, interval float8 DEFAULT 1.0, format text DEFAULT 'png', keyframes_only bool DEFAULT false, max_frames int DEFAULT 1000) -> TABLE(timestamp float8, frame bytea)` | Extract bounded frame sets as PNG or JPEG rows |
| `hls(url text, segment_duration int DEFAULT 6) -> bigint` | Fetch a video via URL, split into HLS segments, and store in `ffmpeg.hls_playlists` / `ffmpeg.hls_segments` |
| `generate_gif(data bytea, start_time float8 DEFAULT 0.0, duration float8 DEFAULT 5.0, width int DEFAULT NULL, fps int DEFAULT 10, format text DEFAULT 'gif') -> bytea` | Generate GIF, APNG, or WebP preview output from a video frame |
| `waveform(data bytea, width int DEFAULT 800, height int DEFAULT 200, format text DEFAULT 'png', mode text DEFAULT 'waveform') -> bytea` | Render audio waveform or spectrum images |
| `extract_subtitles(data bytea, format text DEFAULT 'srt', stream_index int DEFAULT NULL) -> text` | Extract supported text subtitles as SRT, ASS, or WebVTT |
| `overlay(background bytea, foreground bytea, x int DEFAULT 0, y int DEFAULT 0, start_time float8 DEFAULT 0.0, end_time float8 DEFAULT NULL) -> bytea` | Overlay one video on another while preserving background audio |
| `filter_complex(inputs bytea[], filter_graph text, format text DEFAULT 'matroska', codec text DEFAULT NULL, audio_codec text DEFAULT NULL, hwaccel bool DEFAULT false) -> bytea` | Run validated multi-input filter graphs with `[iN:v]` / `[iN:a]` labels |
| `concat(inputs bytea[]) -> bytea` | Concatenate compatible media segments with stream-copy timestamp offsetting |
| `concat_agg(bytea ORDER BY ...) -> bytea` | Ordered aggregate form of concat; O(total_size) aggregate state |
| `encode(frames bytea[], fps int DEFAULT 24, codec text DEFAULT 'libx264', format text DEFAULT 'mp4', crf int DEFAULT 23, hwaccel bool DEFAULT false) -> bytea` | Encode same-sized image frames into a video |

## Milestone 1 notes

`transcode` defaults to a remux path when no video/audio transcoding parameters are supplied. User-provided `filter` and `audio_filter` values are validated against the allow-list in `src/filter_safety.rs`; unsafe sources such as `movie=` are rejected.

`transcode(..., hwaccel => true)` is opt-in. If no suitable hardware encoder can be opened, pg_ffmpeg logs `WARNING "pg_ffmpeg: HW encoder {name} unavailable, falling back to software"` and continues with the software encoder.

`extract_audio` uses stream copy only when `codec`, `bitrate`, `sample_rate`, `channels`, and `filter` are all `NULL` and the requested output container accepts the source codec. With `format => NULL`, the auto-pick table is:

- `aac -> adts`
- `mp3 -> mp3`
- `opus -> ogg`
- `vorbis -> ogg`
- `flac -> flac`
- `pcm_s16le -> wav`

`trim(..., precise => false)` seeks to the nearest keyframe at or before `start_time`, stream-copies packets until `end_time`, and rewrites timestamps to start at 0. `trim(..., precise => true)` decodes and re-encodes audio/video with FFmpeg `trim` / `atrim` filters for frame accuracy.

When `trim(..., precise => true)` cannot re-open the source codec as an encoder in the current FFmpeg build, it falls back to `libx264` for video and `aac` for audio. Subtitle and data streams are dropped in precise mode; fast mode preserves them.

`extract_frames` is bounded by `max_frames`. It never truncates silently: if a `(max_frames + 1)`th row would be emitted, the function raises an error. `keyframes_only => true` ignores `interval` and emits one row per keyframe in decode order.

## Milestone 2 notes

`filter_complex` accepts only the pg_ffmpeg label contract: inputs must be referenced as `[i0:v]`, `[i0:a]`, `[i1:v]`, and so on, and the graph must produce `[vout]`, `[aout]`, or both. Labels are rewritten internally before FFmpeg sees the graph. Unsafe filters such as `movie` and `amovie` are rejected by the same allow-list used by `transcode`.

`concat(bytea[])` requires all inputs to expose compatible streams: codec, stream type, video dimensions, audio sample rate, channel count, and sample format must match the first input. `concat_agg` stores each incoming bytea once as aggregate state, enforces `pg_ffmpeg.max_aggregate_state_bytes`, and is not parallel-safe because input order matters. It is O(total_size) in memory; for concatenating many large videos, use an external pipeline.

`encode` validates that every frame decodes to the same dimensions before encoding. For MP4/MOV output, pg_ffmpeg writes fragmented output suitable for the in-memory `MemOutput` sink.

## Prerequisites

- PostgreSQL 16-18
- Rust toolchain
- `cargo install --locked cargo-pgrx`
- FFmpeg development libraries:

```bash
# Debian/Ubuntu (FFmpeg 8 from the ubuntuhandbook1 PPA)
add-apt-repository -y ppa:ubuntuhandbook1/ffmpeg8
apt-get update
apt-get install libavcodec-dev libavdevice-dev libavfilter-dev \
  libavformat-dev libavutil-dev libswresample-dev libswscale-dev \
  clang libclang-dev pkg-config
```

## Build & Install

```bash
cargo pgrx init --pg18=$(which pg_config)  # adjust for your PG version
cargo pgrx install --release
```

Additional libraries if you plan to build PostgreSQL from source:

```bash
# Debian/Ubuntu
apt-get install build-essential libreadline-dev zlib1g-dev flex bison \
  libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache
```

## Usage

```sql
CREATE EXTENSION pg_ffmpeg;

-- Rich metadata, including chapters/tags/disposition
SELECT ffmpeg.media_info(pg_read_binary_file('/path/to/video.mkv'));

-- Remux when no transcoding parameters are supplied
SELECT ffmpeg.transcode(pg_read_binary_file('/path/to/video.ts'), format => 'matroska');

-- Video + audio transcode with filters
SELECT ffmpeg.transcode(
  pg_read_binary_file('/path/to/video.mp4'),
  format => 'matroska',
  filter => 'scale=-2:720',
  codec => 'libx264',
  preset => 'medium',
  crf => 23,
  audio_codec => 'aac',
  audio_filter => 'volume=0.8'
);

-- Extract audio without forcing MP3 when stream copy is possible
SELECT ffmpeg.extract_audio(pg_read_binary_file('/path/to/input.mp4'));

-- Re-encode extracted audio explicitly
SELECT ffmpeg.extract_audio(
  pg_read_binary_file('/path/to/input.mp4'),
  format => 'wav',
  codec => 'pcm_s16le'
);

-- Fast keyframe-aligned trim
SELECT ffmpeg.trim(
  pg_read_binary_file('/path/to/video.ts'),
  start_time => 12.5,
  end_time => 18.0
);

-- Frame-accurate trim with re-encode
SELECT ffmpeg.trim(
  pg_read_binary_file('/path/to/video.ts'),
  start_time => 12.5,
  end_time => 18.0,
  precise => true
);

-- Extract frames every 0.5s
SELECT *
FROM ffmpeg.extract_frames(
  pg_read_binary_file('/path/to/video.mp4'),
  interval => 0.5,
  format => 'jpeg',
  max_frames => 20
);

-- Extract only keyframes
SELECT *
FROM ffmpeg.extract_frames(
  pg_read_binary_file('/path/to/video.mp4'),
  keyframes_only => true
);

-- Split a remote video into HLS segments
SELECT ffmpeg.hls('https://example.com/video.mp4', segment_duration => 6);

-- Render a waveform image
SELECT ffmpeg.waveform(pg_read_binary_file('/path/to/audio.aac'), width => 1200, height => 300);

-- Stack two clips side by side with the hardened filter_complex label syntax
SELECT ffmpeg.filter_complex(
  ARRAY[
    pg_read_binary_file('/path/to/left.ts'),
    pg_read_binary_file('/path/to/right.ts')
  ],
  '[i0:v][i1:v]hstack=inputs=2[vout]',
  format => 'matroska',
  codec => 'mpeg2video'
);

-- Encode a video from image bytea values
SELECT ffmpeg.encode(
  ARRAY[
    pg_read_binary_file('/path/to/frame-001.png'),
    pg_read_binary_file('/path/to/frame-002.png')
  ],
  fps => 24,
  codec => 'libx264',
  format => 'mp4'
);
```

## License

MIT
