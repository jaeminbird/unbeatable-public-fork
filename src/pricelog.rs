//! Delta-only price logging from the live market feed.
//!
//! Reads the `live` market map already fetched by polling — zero additional API calls.
//! Records only when a market's bid or ask changes (baseline on first sight), so
//! static markets incur minimal overhead. Log rotation is daily at 04:00 ET, gzipped
//! with flate2, and pruned at 14 days.
//!
//! Same doctrine as scanlog.rs: nothing here may ever abort a tick. Every fallible
//! call on the write path is `let _ = ...` or pattern-matched; no `unwrap`, no
//! `expect`, no indexing, no lock.

use serde_json::Value;
use std::collections::HashMap;

use crate::kalshi::f_of;
use crate::paths;

pub const KEEP_DAYS: i64 = 14;

/// The 4am-anchored ET log day for a given ET instant: 03:59 belongs to the prior day,
/// 04:00 opens the new one. Anchoring at 4am keeps a late game's whole price path in
/// one daily file.
pub(crate) fn log_day_of(dt: chrono::DateTime<chrono_tz::Tz>) -> String {
    (dt - chrono::Duration::hours(4))
        .format("%Y-%m-%d")
        .to_string()
}

pub(crate) fn log_day_et() -> String {
    log_day_of(crate::now_et())
}

/// Determine if a market should be logged this tick.
///
/// A market is logged once it is open: if the quote carries an `open_time` in the future
/// the book is not live yet and nothing is recorded. Fails open on a missing or
/// unparseable time, because suppressing quotes on bad metadata would silently blind the log.
pub(crate) fn should_log(m: &Value, now_ts: i64) -> bool {
    let open = m
        .get("open_time")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.timestamp());
    !matches!(open, Some(ts) if ts > now_ts)
}

/// The delta identity of a quote: (bid, ask) in tenth-cents, so float noise cannot
/// fake a change. Non-finite or missing prices clamp to 0 — a corrupt quote still
/// gets a stable key rather than corrupting the log.
pub(crate) fn delta_key(m: &Value) -> (i64, i64) {
    let cents = |k: &str| -> i64 {
        let x = f_of(m, k).unwrap_or(0.0) * 100.0;
        if x.is_finite() {
            (x * 10.0).round() as i64
        } else {
            0
        }
    };
    (cents("yes_bid_dollars"), cents("yes_ask_dollars"))
}

pub struct PriceLog {
    /// The 4am-anchored ET log day the active file is accumulating.
    day: String,
    /// ticker -> last written (bid, ask) in tenth-cents. Evicted when a ticker leaves
    /// the `live` map, so the map is bounded by the watchlist and a returning ticker
    /// re-baselines.
    last: HashMap<String, (i64, i64)>,
}

impl PriceLog {
    pub fn new() -> PriceLog {
        PriceLog {
            day: log_day_et(),
            last: HashMap::new(),
        }
    }

    fn active(&self) -> String {
        format!("{}/prices.jsonl", paths::log_dir().trim_end_matches('/'))
    }

    /// Once per tick, right after `poll_markets`. Purely observational; every write is
    /// best-effort and nothing here may abort a tick.
    pub fn record(&mut self, live: &HashMap<String, Value>) {
        let today = log_day_et();
        if today != self.day {
            self.rotate(&today);
        }
        self.last.retain(|t, _| live.contains_key(t));
        let now_ts = crate::now_et().timestamp();
        let mut rows = String::new();
        for (ticker, m) in live {
            if !should_log(m, now_ts) {
                continue;
            }
            let key = delta_key(m);
            if self.last.get(ticker) == Some(&key) {
                continue;
            }
            self.last.insert(ticker.clone(), key);
            let row = serde_json::json!({
                "ts_et": crate::ts_et(),
                "ticker": ticker,
                "bid": key.0 as f64 / 10.0,
                "ask": key.1 as f64 / 10.0,
            });
            rows.push_str(&row.to_string());
            rows.push('\n');
        }
        if rows.is_empty() {
            return;
        }
        let _ = std::fs::create_dir_all(paths::log_dir());
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.active())
        {
            use std::io::Write as _;
            let _ = f.write_all(rows.as_bytes());
        }
    }

    /// Close out `self.day` and start `today`: gzip the active file to
    /// `prices-<closing day>.jsonl.gz`, fall back to a plain rename if compression
    /// fails (a day is never lost to a gzip error), prune past the window. Best-effort
    /// throughout, and the day advances even if every step failed — retrying a failed
    /// rotation every tick for a whole day would be a hot-path tax for nothing.
    fn rotate(&mut self, today: &str) {
        let dir = paths::log_dir();
        let dir = dir.trim_end_matches('/');
        let active = self.active();
        let gz = format!("{dir}/prices-{}.jsonl.gz", self.day);
        if std::path::Path::new(&active).exists() {
            match gzip_file(&active, &gz) {
                Ok(()) => {
                    let _ = std::fs::remove_file(&active);
                }
                Err(_) => {
                    // A half-written .gz from the failed attempt must not survive
                    // alongside the fallback; remove it before renaming.
                    let _ = std::fs::remove_file(&gz);
                    let _ = std::fs::rename(&active, format!("{dir}/prices-{}.jsonl", self.day));
                }
            }
        }
        let names: Vec<String> = std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter_map(|e| e.file_name().into_string().ok())
                    .collect()
            })
            .unwrap_or_default();
        for n in files_to_prune(&names, today, KEEP_DAYS) {
            let _ = std::fs::remove_file(format!("{dir}/{n}"));
        }
        self.day = today.to_string();
        // The delta state describes quotes already recorded in the CLOSED file; keep it,
        // or every watched market would write a spurious "change" row at 4am.
    }
}

/// Stream-compress `src` into `dst`. Any failure returns Err and leaves `src` intact
/// so the caller can fall back to a rename.
fn gzip_file(src: &str, dst: &str) -> std::io::Result<()> {
    let mut input = std::fs::File::open(src)?;
    let out = std::fs::File::create(dst)?;
    let mut enc = flate2::write::GzEncoder::new(out, flate2::Compression::default());
    std::io::copy(&mut input, &mut enc)?;
    enc.finish()?.sync_all()
}

/// Which daily price files are old enough to unlink. Fail-closed like scanlog's
/// pruner: an unparseable `today` prunes nothing, and only names of exactly the form
/// `prices-YYYY-MM-DD.jsonl` or `prices-YYYY-MM-DD.jsonl.gz` can ever match — the
/// active `prices.jsonl` has no date and can never be selected.
pub fn files_to_prune(names: &[String], today: &str, keep_days: i64) -> Vec<String> {
    let Ok(today_d) = chrono::NaiveDate::parse_from_str(today, "%Y-%m-%d") else {
        return Vec::new();
    };
    names
        .iter()
        .filter(|n| {
            let Some(stem) = n.strip_prefix("prices-").and_then(|s| {
                s.strip_suffix(".jsonl.gz")
                    .or_else(|| s.strip_suffix(".jsonl"))
            }) else {
                return false;
            };
            match chrono::NaiveDate::parse_from_str(stem, "%Y-%m-%d") {
                Ok(d) => (today_d - d).num_days() > keep_days,
                Err(_) => false,
            }
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use chrono_tz::America::New_York;
    use serde_json::json;

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        New_York
            .with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .unwrap()
            .timestamp()
    }

    #[test]
    fn logs_a_market_that_is_already_open() {
        let m = json!({"open_time": "2030-01-01T12:00:00Z"});
        assert!(should_log(&m, ts(2030, 1, 1, 19, 41)));
    }

    #[test]
    fn rejects_a_market_before_it_opens() {
        let m = json!({"open_time": "2030-01-02T12:00:00Z"});
        assert!(!should_log(&m, ts(2030, 1, 1, 19, 39)));
    }

    #[test]
    fn fails_open_when_no_open_time_can_be_established() {
        assert!(should_log(&json!({}), ts(2030, 1, 1, 6, 0)));
        assert!(should_log(
            &json!({"open_time": "garbage"}),
            ts(2030, 1, 1, 6, 0)
        ));
    }

    #[test]
    fn delta_key_is_tenth_cents() {
        let m = json!({"yes_bid_dollars": 0.52, "yes_ask_dollars": 0.53});
        assert_eq!(delta_key(&m), (520, 530));
        // Kalshi sends strings too; f_of handles both.
        let s = json!({"yes_bid_dollars": "0.525", "yes_ask_dollars": "0.53"});
        assert_eq!(delta_key(&s), (525, 530));
    }

    #[test]
    fn delta_key_clamps_missing_and_non_finite() {
        assert_eq!(delta_key(&json!({})), (0, 0));
        // f64::MAX * 1000 overflows to +inf, so the clamp catches it too.
        let m = json!({"yes_bid_dollars": f64::MAX, "yes_ask_dollars": 0.5});
        assert_eq!(delta_key(&m).0, 0);
        let inf = json!({"yes_bid_dollars": "inf", "yes_ask_dollars": "0.5"});
        assert_eq!(delta_key(&inf).0, 0);
    }

    #[test]
    fn log_day_is_anchored_at_4am_et() {
        let before = New_York
            .with_ymd_and_hms(2031, 8, 27, 3, 59, 0)
            .single()
            .unwrap();
        let after = New_York
            .with_ymd_and_hms(2031, 8, 27, 4, 0, 0)
            .single()
            .unwrap();
        assert_eq!(log_day_of(before), "2031-08-26");
        assert_eq!(log_day_of(after), "2031-08-27");
    }

    #[test]
    fn prunes_only_dated_price_files_past_the_window() {
        let names: Vec<String> = [
            "prices-2031-08-26.jsonl.gz",
            "prices-2031-08-13.jsonl.gz",
            "prices-2031-08-12.jsonl",
            "prices-2031-08-12.jsonl.gz",
            "scan-2031-08-01.jsonl",
            "prices.jsonl",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let out = files_to_prune(&names, "2031-08-27", KEEP_DAYS);
        assert_eq!(
            out,
            vec![
                "prices-2031-08-12.jsonl".to_string(),
                "prices-2031-08-12.jsonl.gz".to_string()
            ]
        );
    }

    // -------------------------------------------------------------- rotation

    #[test]
    fn rotation_gzips_the_closing_day_and_prunes_stale_files() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        std::fs::write(dir.path().join("prices-2000-01-01.jsonl.gz"), "old").unwrap();
        let mut pl = PriceLog::new();
        pl.record(&live_of(&[("EXAMPLE-1", 52.0, 53.0)]));
        // Backdate the in-memory day one log day - what the first tick past 4am ET sees.
        let yday = log_day_of(crate::now_et() - chrono::Duration::days(1));
        pl.day = yday.clone();
        pl.record(&live_of(&[("EXAMPLE-1", 51.0, 53.0)]));
        // Closing day gzipped and decodes to the baseline row; active holds the new row.
        let gz_path = dir.path().join(format!("prices-{yday}.jsonl.gz"));
        assert!(gz_path.exists());
        let mut dec = flate2::read::GzDecoder::new(std::fs::File::open(&gz_path).unwrap());
        let mut text = String::new();
        use std::io::Read as _;
        dec.read_to_string(&mut text).unwrap();
        assert!(text.contains("\"bid\":52.0"));
        assert_eq!(text.lines().count(), 1);
        let rows = read_rows(dir.path());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["bid"], 51.0);
        assert!(!dir.path().join("prices-2000-01-01.jsonl.gz").exists());
        assert_eq!(pl.day, log_day_et());
        std::env::remove_var("UGE_LOG_DIR");
    }

    #[test]
    fn rotation_with_no_active_file_still_advances_the_day() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        let mut pl = PriceLog::new();
        let yday = log_day_of(crate::now_et() - chrono::Duration::days(1));
        pl.day = yday.clone();
        pl.record(&live_of(&[]));
        assert_eq!(pl.day, log_day_et());
        assert!(!dir.path().join(format!("prices-{yday}.jsonl.gz")).exists());
        std::env::remove_var("UGE_LOG_DIR");
    }

    #[test]
    fn gzip_failure_leaves_source_for_the_rename_fallback() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("prices.jsonl");
        std::fs::write(&src, "row\n").unwrap();
        // Destination in a nonexistent directory: create fails, source untouched.
        let bad_dst = dir.path().join("nope").join("out.gz");
        assert!(gzip_file(src.to_str().unwrap(), bad_dst.to_str().unwrap()).is_err());
        assert_eq!(std::fs::read_to_string(&src).unwrap(), "row\n");
    }

    // ------------------------------------------------------------- recording

    /// Build a test market map. Bid and ask in cents.
    fn live_of(entries: &[(&str, f64, f64)]) -> HashMap<String, Value> {
        entries
            .iter()
            .map(|(t, bid, ask)| {
                (
                    t.to_string(),
                    json!({"yes_bid_dollars": bid / 100.0, "yes_ask_dollars": ask / 100.0}),
                )
            })
            .collect()
    }

    fn read_rows(dir: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(dir.join("prices.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn baseline_then_suppression_then_delta() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        let mut pl = PriceLog::new();
        let example = "EXAMPLE-1";
        pl.record(&live_of(&[(example, 52.0, 53.0)])); // baseline
        pl.record(&live_of(&[(example, 52.0, 53.0)])); // unchanged
        pl.record(&live_of(&[(example, 51.0, 53.0)])); // bid moved
        let rows = read_rows(dir.path());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["bid"], 52.0);
        assert_eq!(rows[0]["ask"], 53.0);
        assert_eq!(rows[0]["ticker"], example);
        assert!(rows[0]["ts_et"].as_str().unwrap().contains('T'));
        assert_eq!(rows[1]["bid"], 51.0);
        std::env::remove_var("UGE_LOG_DIR");
    }

    #[test]
    fn eviction_rebaselines_a_returning_ticker() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        let mut pl = PriceLog::new();
        let example = "EXAMPLE-1";
        pl.record(&live_of(&[(example, 52.0, 53.0)]));
        pl.record(&live_of(&[])); // ticker gone -> evicted
        assert!(pl.last.is_empty());
        pl.record(&live_of(&[(example, 52.0, 53.0)])); // same quote, fresh baseline
        assert_eq!(read_rows(dir.path()).len(), 2);
        std::env::remove_var("UGE_LOG_DIR");
    }

    #[test]
    fn churn_at_watchlist_scale_writes_exactly_the_deltas() {
        // 2,000 markets over 20 ticks; quotes change on even ticks only.
        // Expected: 2,000 baselines + 2,000 per delta tick. `last` bounded by live-map size.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        let mut pl = PriceLog::new();
        for tick in 0..20u32 {
            let bid = 52.0 - (tick / 2) as f64; // integer halving: moves on even ticks
            let live: HashMap<String, Value> = (0..2000)
                .map(|i| {
                    (
                        format!("T{i}"),
                        json!({"yes_bid_dollars": bid / 100.0, "yes_ask_dollars": 0.53}),
                    )
                })
                .collect();
            pl.record(&live);
        }
        // Bid changes 10 times total (ticks 0,2,4,...18), each affecting all 2,000 markets.
        assert_eq!(read_rows(dir.path()).len(), 2000 * 10);
        assert_eq!(pl.last.len(), 2000);
        std::env::remove_var("UGE_LOG_DIR");
    }

    #[test]
    fn record_never_panics_when_the_log_dir_is_unwritable() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("UGE_LOG_DIR", "/nonexistent/deeply/nested/path");
        let mut pl = PriceLog::new();
        pl.record(&live_of(&[("EXAMPLE-1", 52.0, 53.0)]));
        std::env::remove_var("UGE_LOG_DIR");
    }

    #[test]
    fn a_bad_today_prunes_nothing() {
        let names = vec!["prices-2020-01-01.jsonl.gz".to_string()];
        assert!(files_to_prune(&names, "garbage", KEEP_DAYS).is_empty());
    }
}
