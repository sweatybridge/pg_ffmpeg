FROM postgres:18-bookworm

ARG PG_FFMPEG_VERSION
ARG TARGETARCH=amd64

ADD https://github.com/sweatybridge/pg_ffmpeg/releases/download/v${PG_FFMPEG_VERSION}/pg-ffmpeg-pg18_${PG_FFMPEG_VERSION}-1.bookworm_${TARGETARCH}.deb /tmp/pg_ffmpeg.deb

RUN apt-get update \
  && apt-get install -y --no-install-recommends /tmp/pg_ffmpeg.deb \
  && rm -rf /var/lib/apt/lists/* /tmp/pg_ffmpeg.deb
