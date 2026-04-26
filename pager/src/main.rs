mod config;
mod encode;
mod layout;
mod scan;
mod term;

use std::{
    env,
    io::{self, Read, Write},
    process::{Command, Stdio},
};

use config::Config;

fn main() {
    if let Err(error) = run() {
        eprintln!("pg_bager: {error}");
        std::process::exit(1);
    }
}

fn run() -> io::Result<()> {
    let config = Config::from_env();
    let protocol = term::Protocol::detect();

    let mut input = Vec::new();
    io::stdin().read_to_end(&mut input)?;
    let processed = layout::process(&input, protocol, &config)?;

    if let Some(command) = config.fallback.as_deref() {
        return write_to_pager(command, &processed.bytes);
    }

    if processed.rewritten {
        let mut stdout = io::stdout().lock();
        stdout.write_all(&processed.bytes)?;
        stdout.flush()
    } else {
        let command = default_pager_command();
        write_to_pager(&command, &processed.bytes)
    }
}

fn default_pager_command() -> String {
    env::var("PAGER")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            if command_exists("less") {
                "less -R".to_string()
            } else {
                "cat".to_string()
            }
        })
}

fn command_exists(name: &str) -> bool {
    env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|path| {
            let candidate = path.join(name);
            candidate.is_file()
        })
    })
}

fn write_to_pager(command: &str, bytes: &[u8]) -> io::Result<()> {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdin(Stdio::piped())
        .spawn()?;

    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "pager stdin unavailable"))?;
        stdin.write_all(bytes)?;
    }

    let status = child.wait()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "pager command exited with {status}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{command_exists, write_to_pager};
    use crate::{config::Config, layout, scan::PNG_MAGIC, term::Protocol};

    #[test]
    fn finds_shell() {
        assert!(command_exists("sh"));
    }

    #[test]
    fn fallback_command_receives_rewritten_stream() {
        let dir = tempfile::tempdir().unwrap();
        let output_path = dir.path().join("pager.out");
        let command = format!("cat > {}", output_path.display());

        let mut input = String::from("thumbnail\n\\x");
        for byte in PNG_MAGIC {
            input.push_str(&format!("{byte:02x}"));
        }
        input.push('\n');

        let config = Config {
            max_row_bytes: 4096,
            max_pixels_w: None,
            max_pixels_h: None,
            disable: false,
            fallback: Some(command.clone()),
        };
        let processed = layout::process(input.as_bytes(), Protocol::Kitty, &config).unwrap();

        write_to_pager(&command, &processed.bytes).unwrap();

        let written = fs::read(output_path).unwrap();
        assert_eq!(written, processed.bytes);
        assert!(String::from_utf8(written)
            .unwrap()
            .contains("\x1b_Gf=100,a=T;"));
    }
}
