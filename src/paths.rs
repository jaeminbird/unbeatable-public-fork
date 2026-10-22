//! Filesystem roots. Configurable via environment variables for portability;
//! defaults are sensible for a typical deployment.

pub fn root() -> String {
    std::env::var("UGE_ROOT").unwrap_or_else(|_| "./state".to_string())
}

pub fn key_dir() -> String {
    std::env::var("UGE_KEY_DIR").unwrap_or_else(|_| "./keys".to_string())
}

/// Directory for logs (e.g., tick rollups).
pub fn log_dir() -> String {
    std::env::var("UGE_LOG_DIR").unwrap_or_else(|_| "./logs".to_string())
}

pub fn in_root(rel: &str) -> String {
    format!(
        "{}/{}",
        root().trim_end_matches('/'),
        rel.trim_start_matches('/')
    )
}

/// Lock for env-var mutations in tests. Since env vars are process-global,
/// every test that mutates UGE_ROOT / UGE_KEY_DIR / UGE_LOG_DIR must hold
/// this lock for its entire body to ensure test isolation under parallel execution.
#[cfg(test)]
pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_root_joins_without_doubling_slashes() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("UGE_ROOT", "/tmp/x/");
        assert_eq!(
            in_root("/state/bankroll.json"),
            "/tmp/x/state/bankroll.json"
        );
        assert_eq!(in_root("state/STATUS.md"), "/tmp/x/state/STATUS.md");
        std::env::remove_var("UGE_ROOT");
    }

    #[test]
    fn defaults_are_relative_paths() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("UGE_ROOT");
        std::env::remove_var("UGE_KEY_DIR");
        assert_eq!(root(), "./state");
        assert_eq!(key_dir(), "./keys");
    }
}
