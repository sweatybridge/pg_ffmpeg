::pgrx::pg_module_magic!();

mod extract_audio;
mod media_info;
mod thumbnail;
mod transcode;

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
