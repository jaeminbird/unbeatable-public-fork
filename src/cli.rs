//! Subcommand dispatch. Default (no subcommand) is the engine loop, so a service
//! unit needs no argument.

#[derive(Debug, PartialEq)]
pub enum Command {
    Engine { dry: bool, once: bool },
    Balance,
    Orders,
    Cancel(String),
    Persist,
    Stats,
    Publish,
    Help,
}

/// The only flags the engine path recognises. Any other dash-prefixed argument is a
/// typo, not a flag, and must not be silently swallowed into `sub = None` (which would
/// route to `Command::Engine` and start live trading on a mistyped subcommand).
const KNOWN_ENGINE_FLAGS: [&str; 2] = ["--dry-run", "--once"];

pub fn parse(args: &[String]) -> Command {
    let flags = |name: &str| args.iter().any(|a| a == name);

    // Walk all arguments once: any dash-prefixed token that isn't a known engine flag
    // is unrecognised and must produce Help immediately, regardless of position. The
    // first non-dash token found (if any) is the subcommand.
    let mut sub: Option<&str> = None;
    for a in args.iter().skip(1) {
        if a.starts_with('-') {
            if !KNOWN_ENGINE_FLAGS.contains(&a.as_str()) {
                return Command::Help;
            }
        } else if sub.is_none() {
            sub = Some(a.as_str());
        }
    }

    match sub {
        None => Command::Engine {
            dry: flags("--dry-run"),
            once: flags("--once"),
        },
        Some("balance") => Command::Balance,
        Some("orders") => Command::Orders,
        Some("cancel") => match args.iter().skip(1).filter(|a| !a.starts_with('-')).nth(1) {
            Some(id) => Command::Cancel(id.clone()),
            None => Command::Help,
        },
        Some("persist") => Command::Persist,
        Some("stats") => Command::Stats,
        Some("publish") => Command::Publish,
        Some(_) => Command::Help,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn no_subcommand_runs_the_engine() {
        assert_eq!(
            parse(&v(&["uge"])),
            Command::Engine {
                dry: false,
                once: false
            }
        );
    }

    #[test]
    fn engine_flags_still_parse() {
        assert_eq!(
            parse(&v(&["uge", "--dry-run", "--once"])),
            Command::Engine {
                dry: true,
                once: true
            }
        );
    }

    #[test]
    fn subcommands_dispatch() {
        assert_eq!(parse(&v(&["uge", "balance"])), Command::Balance);
        assert_eq!(parse(&v(&["uge", "orders"])), Command::Orders);
        assert_eq!(parse(&v(&["uge", "persist"])), Command::Persist);
        assert_eq!(parse(&v(&["uge", "stats"])), Command::Stats);
        assert_eq!(parse(&v(&["uge", "publish"])), Command::Publish);
    }

    #[test]
    fn cancel_requires_an_order_id() {
        assert_eq!(
            parse(&v(&["uge", "cancel", "01a03810"])),
            Command::Cancel("01a03810".to_string())
        );
        assert_eq!(parse(&v(&["uge", "cancel"])), Command::Help);
    }

    #[test]
    fn unknown_subcommand_is_help_not_engine() {
        // Critical: a typo must NOT silently start trading.
        assert_eq!(parse(&v(&["uge", "balnce"])), Command::Help);
    }

    #[test]
    fn unrecognised_dash_prefixed_args_are_help_not_engine() {
        // Critical (Finding 1): a mistyped or unknown dash-prefixed argument must NOT
        // be discarded as "just a flag" — that used to leave sub = None and silently
        // route to Command::Engine, starting live trading on a typo.
        assert_eq!(parse(&v(&["uge", "--balance"])), Command::Help);
        assert_eq!(parse(&v(&["uge", "-cancel"])), Command::Help);
        assert_eq!(parse(&v(&["uge", "--foo"])), Command::Help);
        assert_eq!(parse(&v(&["uge", "-x"])), Command::Help);
    }

    #[test]
    fn known_engine_flags_still_reach_the_engine() {
        assert_eq!(
            parse(&v(&["uge", "--dry-run"])),
            Command::Engine {
                dry: true,
                once: false
            }
        );
        assert_eq!(
            parse(&v(&["uge", "--once"])),
            Command::Engine {
                dry: false,
                once: true
            }
        );
        assert_eq!(
            parse(&v(&["uge", "--dry-run", "--once"])),
            Command::Engine {
                dry: true,
                once: true
            }
        );
        assert_eq!(
            parse(&v(&["uge", "--once", "--dry-run"])),
            Command::Engine {
                dry: true,
                once: true
            }
        );
    }

    #[test]
    fn no_arguments_at_all_is_still_the_bare_engine_path() {
        // A service unit passes no arguments at all.
        assert_eq!(
            parse(&v(&["uge"])),
            Command::Engine {
                dry: false,
                once: false
            }
        );
    }

    #[test]
    fn cancel_with_dash_prefixed_id_is_help_not_cancel() {
        // Finding 2: once Finding 1 is fixed, an unrecognised dash-prefixed "id" is
        // caught as an unrecognised flag before the cancel arm ever runs, so this must
        // NOT produce Command::Cancel("-foo") or Command::Cancel of the wrong id.
        match parse(&v(&["uge", "cancel", "-foo"])) {
            Command::Cancel(_) => panic!("cancel -foo must not parse to Command::Cancel"),
            other => assert_eq!(other, Command::Help),
        }
    }
}
