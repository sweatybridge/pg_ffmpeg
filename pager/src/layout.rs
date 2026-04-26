use std::io;

use crate::{config::Config, encode, scan, term::Protocol};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Processed {
    pub bytes: Vec<u8>,
    pub rewritten: bool,
    pub passthrough: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Aligned,
    Unaligned,
    Expanded,
}

pub fn process(input: &[u8], protocol: Protocol, config: &Config) -> io::Result<Processed> {
    if config.disable || protocol == Protocol::None {
        return Ok(passthrough(input));
    }

    let lines = split_inclusive_lines(input);
    let mut candidate_lines = Vec::new();
    let mut too_large = false;

    for (line_index, line) in lines.iter().enumerate() {
        if line.bytes.len() > config.max_row_bytes {
            if line.bytes.windows(2).any(|window| window == b"\\x") {
                too_large = true;
                break;
            }
            continue;
        }

        match scan::png_tokens(line.bytes, config.max_row_bytes) {
            Ok(tokens) if !tokens.is_empty() => candidate_lines.push((line_index, tokens)),
            Ok(_) => {}
            Err(scan::ScanError::TokenTooLarge) => {
                too_large = true;
                break;
            }
        }
    }

    if too_large || candidate_lines.is_empty() {
        return Ok(passthrough(input));
    }

    let mode = match classify(&lines, &candidate_lines) {
        Some(mode) => mode,
        None => return Ok(passthrough(input)),
    };

    let mut out = Vec::with_capacity(input.len());
    let mut rewritten = false;
    for (line_index, line) in lines.iter().enumerate() {
        let tokens = candidate_lines
            .iter()
            .find_map(|(candidate_index, tokens)| {
                (*candidate_index == line_index).then_some(tokens)
            });

        if let Some(tokens) = tokens {
            rewrite_line(&mut out, line.bytes, tokens, mode, protocol, config)?;
            rewritten = true;
        } else {
            out.extend_from_slice(line.bytes);
        }
    }

    Ok(Processed {
        bytes: out,
        rewritten,
        passthrough: false,
    })
}

fn passthrough(input: &[u8]) -> Processed {
    Processed {
        bytes: input.to_vec(),
        rewritten: false,
        passthrough: true,
    }
}

fn classify(lines: &[Line<'_>], candidate_lines: &[(usize, Vec<scan::Token>)]) -> Option<Mode> {
    if looks_expanded(lines) {
        return expanded_is_single_column(lines).then_some(Mode::Expanded);
    }

    if looks_aligned(lines) {
        return candidate_lines
            .iter()
            .all(|(line_index, _)| !lines[*line_index].content_without_newline().contains(&b'|'))
            .then_some(Mode::Aligned);
    }

    candidate_lines
        .iter()
        .all(|(line_index, _)| !lines[*line_index].content_without_newline().contains(&b'|'))
        .then_some(Mode::Unaligned)
}

fn looks_expanded(lines: &[Line<'_>]) -> bool {
    lines
        .iter()
        .any(|line| line.content_without_newline().starts_with(b"-[ RECORD"))
}

fn expanded_is_single_column(lines: &[Line<'_>]) -> bool {
    let mut fields_in_record = 0usize;
    let mut saw_record = false;
    let mut saw_value = false;

    for line in lines {
        let content = line.content_without_newline();
        if content.starts_with(b"-[ RECORD") {
            if saw_record && fields_in_record > 1 {
                return false;
            }
            saw_record = true;
            fields_in_record = 0;
            continue;
        }

        if content.trim_ascii().starts_with(b"(") || content.trim_ascii().is_empty() {
            continue;
        }

        if content.windows(3).any(|window| window == b" | ") {
            fields_in_record += 1;
            saw_value = true;
        }
    }

    saw_record && saw_value && fields_in_record <= 1
}

fn looks_aligned(lines: &[Line<'_>]) -> bool {
    lines.iter().any(|line| {
        let content = line.content_without_newline().trim_ascii();
        !content.is_empty()
            && content
                .iter()
                .all(|byte| matches!(byte, b'-' | b'+' | b' '))
            && content.contains(&b'-')
    })
}

fn rewrite_line(
    out: &mut Vec<u8>,
    line: &[u8],
    tokens: &[scan::Token],
    mode: Mode,
    protocol: Protocol,
    config: &Config,
) -> io::Result<()> {
    let mut cursor = 0;
    for token in tokens {
        out.extend_from_slice(&line[cursor..token.start]);
        match mode {
            Mode::Aligned => write_aligned_placeholder(out, token.end - token.start),
            Mode::Unaligned | Mode::Expanded => out.extend_from_slice(b"[img]"),
        }
        cursor = token.end;
    }
    out.extend_from_slice(&line[cursor..]);

    for token in tokens {
        let original = std::str::from_utf8(&line[token.start..token.end]).unwrap_or("");
        encode::write(out, protocol, original, &token.decoded, config)?;
        if !out.ends_with(b"\n") {
            out.push(b'\n');
        }
    }

    Ok(())
}

fn write_aligned_placeholder(out: &mut Vec<u8>, width: usize) {
    let placeholder = b"[img]";
    out.extend_from_slice(placeholder);
    if width > placeholder.len() {
        out.extend(std::iter::repeat_n(b' ', width - placeholder.len()));
    }
}

#[derive(Clone, Copy)]
struct Line<'a> {
    bytes: &'a [u8],
}

impl Line<'_> {
    fn content_without_newline(&self) -> &[u8] {
        self.bytes.strip_suffix(b"\n").unwrap_or(self.bytes)
    }
}

fn split_inclusive_lines(input: &[u8]) -> Vec<Line<'_>> {
    if input.is_empty() {
        return Vec::new();
    }

    let mut lines = Vec::new();
    let mut start = 0;
    for (index, byte) in input.iter().enumerate() {
        if *byte == b'\n' {
            lines.push(Line {
                bytes: &input[start..=index],
            });
            start = index + 1;
        }
    }
    if start < input.len() {
        lines.push(Line {
            bytes: &input[start..],
        });
    }
    lines
}

#[cfg(test)]
mod tests {
    use crate::{config::Config, layout::process, scan::PNG_MAGIC, term::Protocol};

    fn config() -> Config {
        Config {
            max_row_bytes: 4096,
            max_pixels_w: None,
            max_pixels_h: None,
            disable: false,
            fallback: None,
        }
    }

    fn png_hex() -> String {
        let mut out = String::from("\\x");
        for byte in PNG_MAGIC {
            out.push_str(&format!("{byte:02x}"));
        }
        out
    }

    #[test]
    fn rewrites_single_column_unaligned() {
        let input = format!("thumbnail\n{}\n(1 row)\n", png_hex());
        let processed = process(input.as_bytes(), Protocol::Kitty, &config()).unwrap();
        let text = String::from_utf8(processed.bytes).unwrap();
        assert!(processed.rewritten);
        assert!(text.contains("[img]\n\x1b_Gf=100,a=T;"));
    }

    #[test]
    fn rewrites_single_column_aligned() {
        let input = format!(" thumbnail \n-----------\n {} \n(1 row)\n", png_hex());
        let processed = process(input.as_bytes(), Protocol::Kitty, &config()).unwrap();
        let text = String::from_utf8(processed.bytes).unwrap();
        assert!(processed.rewritten);
        assert!(text.contains(" [img]"));
        assert!(text.contains("\x1b_Gf=100,a=T;"));
    }

    #[test]
    fn rewrites_single_column_expanded() {
        let input = format!("-[ RECORD 1 ]-----\nthumbnail | {}\n", png_hex());
        let processed = process(input.as_bytes(), Protocol::ITerm2, &config()).unwrap();
        let text = String::from_utf8(processed.bytes).unwrap();
        assert!(processed.rewritten);
        assert!(text.contains("thumbnail | [img]\n\x1b]1337;File="));
    }

    #[test]
    fn multi_column_unaligned_passthrough() {
        let input = format!("thumbnail|id\n{}|1\n(1 row)\n", png_hex());
        let processed = process(input.as_bytes(), Protocol::Kitty, &config()).unwrap();
        assert!(processed.passthrough);
        assert_eq!(processed.bytes, input.as_bytes());
    }

    #[test]
    fn multi_column_aligned_passthrough() {
        let input = format!(
            " thumbnail | id \n-----------+----\n {} | 1\n(1 row)\n",
            png_hex()
        );
        let processed = process(input.as_bytes(), Protocol::Kitty, &config()).unwrap();
        assert!(processed.passthrough);
        assert_eq!(processed.bytes, input.as_bytes());
    }

    #[test]
    fn multi_column_expanded_passthrough() {
        let input = format!(
            "-[ RECORD 1 ]-----\nthumbnail | {}\nid        | 1\n",
            png_hex()
        );
        let processed = process(input.as_bytes(), Protocol::Kitty, &config()).unwrap();
        assert!(processed.passthrough);
        assert_eq!(processed.bytes, input.as_bytes());
    }

    #[test]
    fn none_protocol_passthrough() {
        let input = format!("{}\n", png_hex());
        let processed = process(input.as_bytes(), Protocol::None, &config()).unwrap();
        assert!(processed.passthrough);
        assert_eq!(processed.bytes, input.as_bytes());
    }

    #[test]
    fn row_over_cap_passthrough() {
        let input = format!("{}\n", png_hex());
        let mut config = config();
        config.max_row_bytes = 8;
        let processed = process(input.as_bytes(), Protocol::Kitty, &config).unwrap();
        assert!(processed.passthrough);
        assert_eq!(processed.bytes, input.as_bytes());
    }
}
