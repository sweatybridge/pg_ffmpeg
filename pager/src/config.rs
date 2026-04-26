use std::env;

pub const DEFAULT_MAX_ROW_BYTES: usize = 1 << 30;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub max_row_bytes: usize,
    pub max_pixels_w: Option<u32>,
    pub max_pixels_h: Option<u32>,
    pub disable: bool,
    pub fallback: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            max_row_bytes: parse_usize_env("PG_BAGER_MAX_ROW_BYTES")
                .unwrap_or(DEFAULT_MAX_ROW_BYTES),
            max_pixels_w: parse_u32_env("PG_BAGER_MAX_PIXELS_W"),
            max_pixels_h: parse_u32_env("PG_BAGER_MAX_PIXELS_H"),
            disable: env::var("PG_BAGER_DISABLE").is_ok_and(|value| value == "1"),
            fallback: env::var("PG_BAGER_FALLBACK")
                .ok()
                .filter(|value| !value.trim().is_empty()),
        }
    }
}

fn parse_usize_env(name: &str) -> Option<usize> {
    env::var(name).ok()?.parse().ok()
}

fn parse_u32_env(name: &str) -> Option<u32> {
    env::var(name).ok()?.parse().ok()
}
