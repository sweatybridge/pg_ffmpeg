pub const PNG_MAGIC: &[u8] = b"\x89PNG\r\n\x1a\n";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Token {
    pub start: usize,
    pub end: usize,
    pub decoded: Vec<u8>,
}

pub fn png_tokens(line: &[u8], max_token_bytes: usize) -> Result<Vec<Token>, ScanError> {
    let mut tokens = Vec::new();
    let mut index = 0;

    while index + 3 <= line.len() {
        if line[index] != b'\\' || line[index + 1] != b'x' || !is_hex(line[index + 2]) {
            index += 1;
            continue;
        }

        let start = index;
        let mut end = index + 2;
        while end < line.len() && is_hex(line[end]) {
            end += 1;
        }

        if end - start > max_token_bytes {
            return Err(ScanError::TokenTooLarge);
        }

        if let Some(decoded) = decode_hex_png_prefix(&line[start + 2..end]) {
            tokens.push(Token {
                start,
                end,
                decoded,
            });
        }
        index = end;
    }

    Ok(tokens)
}

fn decode_hex_png_prefix(hex: &[u8]) -> Option<Vec<u8>> {
    if hex.len() < PNG_MAGIC.len() * 2 || !hex.len().is_multiple_of(2) {
        return None;
    }

    let mut decoded = Vec::with_capacity(hex.len() / 2);
    for pair in hex.chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        decoded.push((high << 4) | low);
    }

    decoded.starts_with(PNG_MAGIC).then_some(decoded)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanError {
    TokenTooLarge,
}

fn is_hex(byte: u8) -> bool {
    byte.is_ascii_hexdigit()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{png_tokens, ScanError, PNG_MAGIC};

    fn png_hex() -> String {
        let mut out = String::from("\\x");
        for byte in PNG_MAGIC {
            out.push_str(&format!("{byte:02x}"));
        }
        out
    }

    #[test]
    fn finds_png_token() {
        let line = format!("prefix {} suffix", png_hex());
        let tokens = png_tokens(line.as_bytes(), 1024).unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].decoded, PNG_MAGIC);
    }

    #[test]
    fn ignores_non_png_bytea() {
        let tokens = png_tokens(br"\xdeadbeef", 1024).unwrap();
        assert!(tokens.is_empty());
    }

    #[test]
    fn reports_large_token() {
        let err = png_tokens(br"\x89504e470d0a1a0a", 8).unwrap_err();
        assert_eq!(err, ScanError::TokenTooLarge);
    }
}
