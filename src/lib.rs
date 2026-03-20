use pgrx::prelude::*;

::pgrx::pg_module_magic!();

#[pg_schema]
mod ffmpeg {}

mod extract_audio;
mod hls;
mod media_info;
mod thumbnail;
mod transcode;

extension_sql!(
    r#"
CREATE TABLE ffmpeg.hls_playlists (
    id              bigserial PRIMARY KEY,
    target_duration int NOT NULL DEFAULT 0,
    media_sequence  int NOT NULL DEFAULT 0
);

CREATE TABLE ffmpeg.hls_segments (
    id            bigserial PRIMARY KEY,
    playlist_id   bigint NOT NULL REFERENCES ffmpeg.hls_playlists(id),
    segment_index int NOT NULL,
    duration      float8,
    data          bytea NOT NULL
);

CREATE INDEX ON ffmpeg.hls_segments (playlist_id);
"#,
    name = "create_hls_tables",
);

/// Write bytea data to a temporary file with the given suffix.
pub fn write_to_tempfile(
    data: &[u8],
    suffix: &str,
) -> Result<tempfile::NamedTempFile, std::io::Error> {
    use std::io::Write;
    let mut tmp = tempfile::Builder::new().suffix(suffix).tempfile()?;
    tmp.write_all(data)?;
    tmp.flush()?;
    Ok(tmp)
}

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
