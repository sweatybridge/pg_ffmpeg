use std::io::{self, Write};

use base64::{engine::general_purpose::STANDARD, Engine as _};

use crate::{config::Config, term::Protocol};

const KITTY_CHUNK_BYTES: usize = 4096;

pub fn write(
    out: &mut dyn Write,
    protocol: Protocol,
    original: &str,
    decoded: &[u8],
    config: &Config,
) -> io::Result<()> {
    let mut buf = Vec::new();
    match protocol {
        Protocol::Kitty => kitty(&mut buf, decoded, config),
        Protocol::ITerm2 => iterm2(&mut buf, decoded, config),
        Protocol::None => {
            buf.extend_from_slice(original.as_bytes());
            Ok(())
        }
    }?;

    if buf.is_empty() {
        out.write_all(original.as_bytes())
    } else {
        out.write_all(&buf)
    }
}

fn kitty(out: &mut Vec<u8>, decoded: &[u8], config: &Config) -> io::Result<()> {
    if !decoded.starts_with(crate::scan::PNG_MAGIC) {
        return Ok(());
    }

    let payload = STANDARD.encode(decoded);
    if payload.len() <= KITTY_CHUNK_BYTES {
        write!(out, "\x1b_Gf=100,a=T")?;
        write_kitty_size(out, config)?;
        write!(out, ";{payload}\x1b\\")?;
        return Ok(());
    }

    for (index, chunk) in payload.as_bytes().chunks(KITTY_CHUNK_BYTES).enumerate() {
        let chunk = std::str::from_utf8(chunk).expect("base64 is valid utf-8");
        if index == 0 {
            write!(out, "\x1b_Gf=100,a=T,m=1")?;
            write_kitty_size(out, config)?;
            write!(out, ";{chunk}\x1b\\")?;
        } else if (index + 1) * KITTY_CHUNK_BYTES >= payload.len() {
            write!(out, "\x1b_Gm=0;{chunk}\x1b\\")?;
        } else {
            write!(out, "\x1b_Gm=1;{chunk}\x1b\\")?;
        }
    }

    Ok(())
}

fn write_kitty_size(out: &mut Vec<u8>, config: &Config) -> io::Result<()> {
    if let Some(width) = config.max_pixels_w {
        write!(out, ",w={width}")?;
    }
    if let Some(height) = config.max_pixels_h {
        write!(out, ",h={height}")?;
    }
    Ok(())
}

fn iterm2(out: &mut Vec<u8>, decoded: &[u8], config: &Config) -> io::Result<()> {
    if !decoded.starts_with(crate::scan::PNG_MAGIC) {
        return Ok(());
    }

    let payload = STANDARD.encode(decoded);
    write!(out, "\x1b]1337;File=inline=1;size={}", decoded.len())?;
    if let Some(width) = config.max_pixels_w {
        write!(out, ";width={width}px")?;
    }
    if let Some(height) = config.max_pixels_h {
        write!(out, ";height={height}px")?;
    }
    write!(out, ";preserveAspectRatio=1:{payload}\x07")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::write;
    use crate::{config::Config, term::Protocol};

    fn config() -> Config {
        Config {
            max_row_bytes: 1024,
            max_pixels_w: None,
            max_pixels_h: None,
            disable: false,
            fallback: None,
        }
    }

    #[test]
    fn kitty_single_chunk_omits_more_flag() {
        let mut out = Vec::new();
        write(
            &mut out,
            Protocol::Kitty,
            "orig",
            crate::scan::PNG_MAGIC,
            &config(),
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with("\x1b_Gf=100,a=T;"));
        assert!(!text.contains("m=1"));
        assert!(text.ends_with("\x1b\\"));
    }

    #[test]
    fn kitty_multi_chunk_terminates_with_m_zero() {
        let mut png = crate::scan::PNG_MAGIC.to_vec();
        png.resize(5000, 1);
        let mut out = Vec::new();
        write(&mut out, Protocol::Kitty, "orig", &png, &config()).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("f=100,a=T,m=1"));
        assert!(text.contains("\x1b_Gm=0;"));
    }

    #[test]
    fn iterm2_writes_inline_file_escape() {
        let mut out = Vec::new();
        write(
            &mut out,
            Protocol::ITerm2,
            "orig",
            crate::scan::PNG_MAGIC,
            &config(),
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with("\x1b]1337;File=inline=1;size=8;preserveAspectRatio=1:"));
        assert!(text.ends_with('\x07'));
    }

    #[test]
    fn none_writes_original() {
        let mut out = Vec::new();
        write(
            &mut out,
            Protocol::None,
            "orig",
            crate::scan::PNG_MAGIC,
            &config(),
        )
        .unwrap();
        assert_eq!(out, b"orig");
    }
}
