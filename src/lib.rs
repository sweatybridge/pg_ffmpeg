use pgrx::prelude::*;

::pgrx::pg_module_magic!();

mod extract_audio;
mod hls;
mod media_info;
pub mod mem_io;
mod thumbnail;
mod transcode;

#[cfg(feature = "pg_bench")]
pub(crate) mod bench_common {
    pub fn sample_video_path() -> std::path::PathBuf {
        std::env::temp_dir().join("pg_ffmpeg_bench_sample.ts")
    }

    pub fn generate_sample_video() {
        let path = sample_video_path();
        crate::hls::generate_video(&path, 640, 480, 25, 30, 2_000_000);
    }

    pub fn sample_video_bytes() -> Vec<u8> {
        std::fs::read(sample_video_path()).expect("sample video not generated")
    }
}

extension_sql!(
    r#"
CREATE TABLE hls_playlists (
    id              bigserial PRIMARY KEY,
    target_duration int NOT NULL DEFAULT 0
);

CREATE TABLE hls_segments (
    id            bigserial PRIMARY KEY,
    playlist_id   bigint NOT NULL REFERENCES hls_playlists(id),
    segment_index int NOT NULL,
    duration      float8,
    data          bytea NOT NULL
);

CREATE INDEX ON hls_segments (playlist_id);

ALTER TABLE hls_segments ALTER COLUMN data SET STORAGE EXTERNAL;
"#,
    name = "create_hls_tables",
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_extension_loads() {
        // Extension loaded successfully if we get here
        assert!(true);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
