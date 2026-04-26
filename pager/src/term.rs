use std::env;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Kitty,
    ITerm2,
    None,
}

impl Protocol {
    pub fn detect() -> Self {
        let vars: Vec<(String, String)> = env::vars().collect();
        detect_from(
            vars.iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
    }
}

pub fn detect_from<'a>(vars: impl IntoIterator<Item = (&'a str, &'a str)>) -> Protocol {
    let vars: Vec<(&str, &str)> = vars.into_iter().collect();

    if let Some((_, value)) = vars.iter().find(|(key, _)| *key == "PG_BAGER_PROTOCOL") {
        return match *value {
            "kitty" => Protocol::Kitty,
            "iterm2" => Protocol::ITerm2,
            "none" => Protocol::None,
            _ => Protocol::None,
        };
    }

    let get = |name: &str| {
        vars.iter()
            .find_map(|(key, value)| (*key == name).then_some(*value))
    };

    if get("TERM") == Some("xterm-kitty")
        || get("KITTY_WINDOW_ID").is_some()
        || get("TERM_PROGRAM") == Some("WezTerm")
    {
        return Protocol::Kitty;
    }

    if get("TERM_PROGRAM") == Some("iTerm.app") {
        return Protocol::ITerm2;
    }

    Protocol::None
}

#[cfg(test)]
mod tests {
    use super::{detect_from, Protocol};

    #[test]
    fn explicit_override_wins() {
        assert_eq!(
            detect_from([("PG_BAGER_PROTOCOL", "iterm2"), ("TERM", "xterm-kitty")]),
            Protocol::ITerm2
        );
        assert_eq!(
            detect_from([("PG_BAGER_PROTOCOL", "none"), ("TERM", "xterm-kitty")]),
            Protocol::None
        );
    }

    #[test]
    fn detects_kitty_like_terminals() {
        assert_eq!(detect_from([("TERM", "xterm-kitty")]), Protocol::Kitty);
        assert_eq!(detect_from([("KITTY_WINDOW_ID", "1")]), Protocol::Kitty);
        assert_eq!(detect_from([("TERM_PROGRAM", "WezTerm")]), Protocol::Kitty);
    }

    #[test]
    fn detects_iterm2() {
        assert_eq!(
            detect_from([("TERM_PROGRAM", "iTerm.app")]),
            Protocol::ITerm2
        );
    }

    #[test]
    fn falls_back_to_none() {
        assert_eq!(detect_from([("TERM", "xterm-256color")]), Protocol::None);
    }
}
