//! Per-tick scan telemetry. Answers "was the engine healthy and what did it see"
//! without recording every quote.
//!
//! ~17k ticks/day at ~200 bytes is ~3MB/day, which is why this is NEVER committed - it
//! lives in ./state/logs and ages out after 7 days. Rotation is this module's own
//! job rather than relying on external log management, so files are managed consistently.
//!
//! Nothing here may ever abort a tick: a full disk degrades logging, not trading.
//! Every fallible call in this file is `let _ = ...` or pattern-matched; there is no
//! `unwrap`, no `expect`, no indexing, no lock, and no arithmetic that can overflow on
//! the write path.

use serde_json::json;

use crate::paths;

pub const KEEP_DAYS: i64 = 7;

/// Which daily files are old enough to unlink, given today's ET date.
///
/// Fails closed on purpose: this function's output is fed straight to `remove_file`, so
/// an unparseable `today` prunes nothing, and a name that is not exactly
/// `scan-YYYY-MM-DD.jsonl` is never matched. The active `scan.jsonl` has no `scan-`
/// prefix and so can never be selected.
pub fn files_to_prune(names: &[String], today: &str, keep_days: i64) -> Vec<String> {
    let Ok(today_d) = chrono::NaiveDate::parse_from_str(today, "%Y-%m-%d") else {
        return Vec::new();
    };
    names
        .iter()
        .filter(|n| {
            let Some(date) = n
                .strip_prefix("scan-")
                .and_then(|s| s.strip_suffix(".jsonl"))
            else {
                return false;
            };
            match chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d") {
                // NaiveDate subtraction yields a chrono TimeDelta whose day count is well
                // inside i64 for any date chrono can parse, so this cannot overflow.
                Ok(d) => (today_d - d).num_days() > keep_days,
                Err(_) => false,
            }
        })
        .cloned()
        .collect()
}

/// Non-finite floats would serialize as JSON `null` and make a row unparseable by the
/// consumers; clamp them instead so a bad divide upstream cannot corrupt the log.
fn finite(x: f64) -> f64 {
    if x.is_finite() {
        x
    } else {
        0.0
    }
}

pub struct Tick {
    pub seq: u64,
    pub series_scanned: Vec<String>,
    pub markets_seen: usize,
    pub watching: usize,
    pub armed: usize,
    pub resting: usize,
    pub resting_usd: f64,
    pub open: usize,
    pub avail_usd: f64,
    /// Capital locked in open positions this tick. `avail + in_play` sampled every
    /// tick is the intraday equity series - the only source fine-grained enough to
    /// reconstruct max drawdown WITHIN a day, which the settle-order calibration
    /// ledger cannot see.
    pub in_play_usd: f64,
    pub api_calls: u32,
    pub api_errors: u32,
    pub worst_latency_ms: u64,
    pub duration_ms: u64,
    /// Sub-tick sampling, this tick. `catches` counts the times a fast re-sample
    /// observed a resting order's running-low bid below anything the main tick had
    /// seen - the direct measure of whether the finer sampling buys anything.
    pub subtick_samples: u64,
    pub subtick_catches: u64,
}

/// Hard ceiling on the per-day latency sample buffer. A normal day is ~17.3k ticks at
/// TICK_S=5, so this is ~1.4x a full day and costs ~200KB. It exists for the abnormal
/// case: if the ET date never advances (frozen clock) or `record` is called far more
/// often than once per tick, the Vec would otherwise grow without bound for the life of
/// the process. Past the cap we stop sampling rather than evicting, so an overflowing
/// day's p95 describes its first 25k samples - a degraded statistic, never a leak.
const MAX_LATENCY_SAMPLES: usize = 25_000;

pub struct ScanLog {
    day: String,
    // Accumulators for the day named by `day`. Reset by `rotate` after the digest for
    // that day is written. Every add saturates: telemetry must never overflow-panic.
    ticks: u64,
    markets_seen: u64,
    arms: u64,
    fills: u64,
    api_errors: u64,
    latencies: Vec<u64>,
    subtick_samples: u64,
    subtick_catches: u64,
}

impl ScanLog {
    pub fn new() -> ScanLog {
        let sl = ScanLog {
            day: crate::day_et(),
            ticks: 0,
            markets_seen: 0,
            arms: 0,
            fills: 0,
            api_errors: 0,
            latencies: Vec::new(),
            subtick_samples: 0,
            subtick_catches: 0,
        };
        sl.close_out_days_missed_while_down();
        sl
    }

    /// Write the digest for any completed ET day still sitting in the active scan file.
    ///
    /// `rotate` only fires on a tick, so a day that ended while the engine was down - or
    /// during the gap of a restart that straddled ET midnight - is never closed. That day
    /// silently gets no row in scan-daily.jsonl at all, and its rows stay in `scan.jsonl`
    /// until the next rotation files them under the wrong date. Run once at startup, so
    /// the recovery costs one sequential read per process start and nothing per tick.
    ///
    /// If the engine missed several midnights the digests are still one-per-day and
    /// correct, but the rename can only pick one name: the file is filed under the newest
    /// stale day. The committed record (scan-daily.jsonl) is the one that has to be right.
    ///
    /// Best-effort like every other write in this module - a failure here loses a digest
    /// row, it never blocks startup.
    fn close_out_days_missed_while_down(&self) {
        let active = self.active();
        let stale = stale_days_in(&active, &self.day);
        let Some(newest) = stale.last().cloned() else {
            return;
        };
        for day in &stale {
            if let Some(d) = digest_from_scanfile(&active, day) {
                write_digest(day, &d);
            }
        }
        let dir = paths::log_dir();
        let _ = std::fs::rename(
            &active,
            format!("{}/scan-{}.jsonl", dir.trim_end_matches('/'), newest),
        );
    }

    /// The digest for the day currently being accumulated, from the in-process
    /// accumulators.
    ///
    /// FALLBACK ONLY. These counters start at zero on every process start, so on any day
    /// with a restart they describe the span since that restart and not the day.
    /// `rotate` prefers `digest_from_scanfile`; this runs only when that returns `None`.
    fn digest(&mut self) -> Digest {
        let up = 100.0 * self.ticks as f64 * crate::TICK_S as f64 / 86400.0;
        Digest {
            ticks: self.ticks,
            markets_seen: self.markets_seen,
            arms: self.arms,
            fills: self.fills,
            api_errors: self.api_errors,
            p95_latency_ms: p95(&mut self.latencies),
            uptime_pct: finite(up).min(100.0),
            subtick_samples: self.subtick_samples,
            subtick_catches: self.subtick_catches,
            source: "memory",
        }
    }

    fn reset_day(&mut self) {
        self.ticks = 0;
        self.markets_seen = 0;
        self.arms = 0;
        self.fills = 0;
        self.api_errors = 0;
        self.latencies.clear();
        self.subtick_samples = 0;
        self.subtick_catches = 0;
    }

    fn active(&self) -> String {
        format!("{}/scan.jsonl", paths::log_dir().trim_end_matches('/'))
    }

    /// Rename yesterday's file and unlink anything past the window. Best-effort:
    /// every failure is swallowed, because rotation must never abort a tick.
    fn rotate(&mut self, today: &str) {
        // The digest is written FIRST and describes the day being closed; only then are
        // the accumulators cleared for `today`. It is read back out of the append-only
        // scan file rather than taken from those accumulators, because the accumulators
        // do not survive a restart and the file does - see `digest_from_scanfile`. The
        // read happens BEFORE the rename below, while the day's rows are still in
        // `scan.jsonl`.
        let active = self.active();
        let from_file = digest_from_scanfile(&active, &self.day);
        let d = match from_file {
            Some(d) => d,
            None => self.digest(),
        };
        write_digest(&self.day, &d);
        self.reset_day();
        let dir = paths::log_dir();
        let _ = std::fs::rename(
            self.active(),
            format!("{}/scan-{}.jsonl", dir.trim_end_matches('/'), self.day),
        );
        let names: Vec<String> = std::fs::read_dir(&dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter_map(|e| e.file_name().into_string().ok())
                    .collect()
            })
            .unwrap_or_default();
        for n in files_to_prune(&names, today, KEEP_DAYS) {
            let _ = std::fs::remove_file(format!("{}/{n}", dir.trim_end_matches('/')));
        }
        // Advance the day even if the rename or the prune failed: retrying a failed
        // rename every tick for the rest of the day would be a hot-path tax for nothing.
        self.day = today.to_string();
    }

    /// `arms` / `fills` are this tick's counts, taken by the caller from the tick's log
    /// lines (prefixes `ARMED` and `FILLED`) - the scan log has no other view of them.
    pub fn record(&mut self, t: &Tick, arms: usize, fills: usize) {
        let today = crate::day_et();
        if today != self.day {
            self.rotate(&today);
        }
        self.ticks = self.ticks.saturating_add(1);
        self.markets_seen = self.markets_seen.saturating_add(t.markets_seen as u64);
        self.arms = self.arms.saturating_add(arms as u64);
        self.fills = self.fills.saturating_add(fills as u64);
        self.api_errors = self.api_errors.saturating_add(t.api_errors as u64);
        self.subtick_samples = self.subtick_samples.saturating_add(t.subtick_samples);
        self.subtick_catches = self.subtick_catches.saturating_add(t.subtick_catches);
        if self.latencies.len() < MAX_LATENCY_SAMPLES {
            self.latencies.push(t.worst_latency_ms);
        }
        let row = json!({
            "ts_et": crate::ts_et(),
            "seq": t.seq,
            "series_scanned": t.series_scanned,
            "markets_seen": t.markets_seen,
            // arms/fills are per-tick counts the caller derives from this tick's log
            // lines. They are written here so the daily digest can be rebuilt from this
            // file after a restart; without them `digest_from_scanfile` could not
            // recover these two fields at all.
            "arms": arms,
            "fills": fills,
            "watching": t.watching,
            "armed": t.armed,
            "resting": t.resting,
            "resting_usd": finite(t.resting_usd),
            "open": t.open,
            "avail_usd": finite(t.avail_usd),
            "in_play_usd": finite(t.in_play_usd),
            "api_calls": t.api_calls,
            "api_errors": t.api_errors,
            "worst_latency_ms": t.worst_latency_ms,
            "duration_ms": t.duration_ms,
            // Written per tick for the same reason arms/fills are: the daily digest is
            // rebuilt from THIS file after a restart, and a counter living only in the
            // process cannot survive one.
            "subtick_samples": t.subtick_samples,
            "subtick_catches": t.subtick_catches,
        });
        let _ = std::fs::create_dir_all(paths::log_dir());
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.active())
        {
            use std::io::Write as _;
            let _ = writeln!(f, "{row}");
        }
    }
}

pub struct Digest {
    pub ticks: u64,
    pub markets_seen: u64,
    pub arms: u64,
    pub fills: u64,
    pub api_errors: u64,
    pub p95_latency_ms: u64,
    pub uptime_pct: f64,
    pub subtick_samples: u64,
    pub subtick_catches: u64,
    /// Where the counts came from: `"scanfile"` (the append-only per-tick log, the
    /// restart-proof source and the normal case) or `"memory"` (the in-process
    /// accumulators, used only when the scan file could not be read or held no row for
    /// the day). A `"memory"` row may be truncated by a restart - see
    /// `digest_from_scanfile`. Emitted as `ticks_source` so a reader can tell.
    pub source: &'static str,
}

/// Nearest-rank p95. Sorts in place; empty input is 0, not a panic.
///
/// The index is `ceil(len * 0.95) - 1`, saturating at 0 and clamped to `len - 1`, so it
/// is in bounds for every non-empty length: len 1 -> 0, 2 -> 1, 19 -> 17, 20 -> 18,
/// 21 -> 19. `len - 1` is only evaluated after the empty case has returned.
pub fn p95(latencies: &mut [u64]) -> u64 {
    if latencies.is_empty() {
        return 0;
    }
    latencies.sort_unstable();
    let idx = (((latencies.len() as f64) * 0.95).ceil() as usize).saturating_sub(1);
    latencies
        .get(idx.min(latencies.len() - 1))
        .copied()
        .unwrap_or(0)
}

/// Aggregate one ET day's digest from the active per-tick scan file.
///
/// **This is the restart-proof source, and the reason it exists.** `ScanLog`'s counters
/// live in the process; `ScanLog::new()` zeroes them at every start and only `rotate`
/// clears them after. A mid-day restart therefore made the midnight digest describe the
/// span since that restart rather than the whole day. Without this, `markets_seen`,
/// `arms`, `fills` and `api_errors` would be truncated to just the post-restart span,
/// making scan-daily.jsonl under-report any day the engine was restarted. The fix is to
/// compute the digest from the append-only per-tick log instead of the accumulators.
///
/// `scan.jsonl` is append-only and every instance writes to the same file, so it is the
/// only per-day record that survives a restart. Rows are selected by their own `ts_et`
/// prefix, so a file that spans a midnight - the engine down or restarting across the ET
/// roll - still yields one correct digest per day instead of mixing the two.
///
/// Returns `None` when the file cannot be opened or holds no row for `day`; the caller
/// falls back to the accumulators. Unparseable lines are skipped rather than failing the
/// whole digest: a torn final line from a hard kill must not cost the day its row.
///
/// Cost is one ~3MB sequential read per day, off the hot path - `rotate` runs on the
/// first tick after midnight ET.
///
/// **Deploy note:** `arms` and `fills` were added to the per-tick row in the same change
/// as this function. Rows written by an older binary have neither key and count as zero,
/// so the first digest after this ships under-reports those two fields for the part of
/// the day that the old binary wrote. `ticks`, `markets_seen`, `api_errors` and the
/// latencies are unaffected - those keys already existed.
fn digest_from_scanfile(path: &str, day: &str) -> Option<Digest> {
    use std::io::BufRead as _;
    let f = std::fs::File::open(path).ok()?;
    let mut ticks: u64 = 0;
    let mut markets_seen: u64 = 0;
    let mut arms: u64 = 0;
    let mut fills: u64 = 0;
    let mut api_errors: u64 = 0;
    let mut subtick_samples: u64 = 0;
    let mut subtick_catches: u64 = 0;
    let mut latencies: Vec<u64> = Vec::new();
    for line in std::io::BufReader::new(f).lines() {
        let Ok(line) = line else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        // A row with no ts_et, or one stamped for another day, is not this day's tick.
        if !v["ts_et"]
            .as_str()
            .map(|s| s.starts_with(day))
            .unwrap_or(false)
        {
            continue;
        }
        ticks = ticks.saturating_add(1);
        markets_seen = markets_seen.saturating_add(v["markets_seen"].as_u64().unwrap_or(0));
        arms = arms.saturating_add(v["arms"].as_u64().unwrap_or(0));
        fills = fills.saturating_add(v["fills"].as_u64().unwrap_or(0));
        api_errors = api_errors.saturating_add(v["api_errors"].as_u64().unwrap_or(0));
        subtick_samples =
            subtick_samples.saturating_add(v["subtick_samples"].as_u64().unwrap_or(0));
        subtick_catches =
            subtick_catches.saturating_add(v["subtick_catches"].as_u64().unwrap_or(0));
        // Same cap as the live accumulator, and for the same reason: a pathological file
        // must not be able to balloon this Vec. Past the cap the p95 describes the day's
        // first MAX_LATENCY_SAMPLES ticks - degraded, never unbounded.
        if latencies.len() < MAX_LATENCY_SAMPLES {
            latencies.push(v["worst_latency_ms"].as_u64().unwrap_or(0));
        }
    }
    if ticks == 0 {
        return None;
    }
    let up = 100.0 * ticks as f64 * crate::TICK_S as f64 / 86400.0;
    Some(Digest {
        ticks,
        markets_seen,
        arms,
        fills,
        api_errors,
        p95_latency_ms: p95(&mut latencies),
        uptime_pct: finite(up).min(100.0),
        subtick_samples,
        subtick_catches,
        source: "scanfile",
    })
}

/// ET days present in the active scan file that are strictly earlier than `today`.
///
/// Non-empty only when the engine was not running at an ET midnight, so nothing called
/// `rotate` for the day that ended. Without this the day silently gets no digest row at
/// all, and its rows sit in `scan.jsonl` until the next rotation renames them under the
/// wrong date. Sorted and deduped so the caller writes one digest per day, oldest first.
fn stale_days_in(path: &str, today: &str) -> Vec<String> {
    use std::io::BufRead as _;
    let Ok(f) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let mut days: Vec<String> = Vec::new();
    for line in std::io::BufReader::new(f).lines() {
        let Ok(line) = line else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let Some(ts) = v["ts_et"].as_str() else {
            continue;
        };
        // ts_et is "YYYY-MM-DDTHH:MM:SS-0400"; the date is the first 10 bytes, all ASCII.
        if ts.len() < 10 || !ts.is_char_boundary(10) {
            continue;
        }
        let d = &ts[..10];
        if d < today && !days.iter().any(|x| x == d) {
            days.push(d.to_string());
        }
    }
    days.sort();
    days
}

/// One row per day, upserted into state/scan-daily.jsonl - this file IS committed.
/// ~365 rows/year, against the ~6M rows the raw per-tick log would add.
/// Best-effort like every other write in this module: a failure loses a digest row,
/// it never aborts a tick.
pub fn write_digest(day: &str, d: &Digest) {
    let row = json!({
        "date": day,
        "ticks": d.ticks,
        "markets_seen": d.markets_seen,
        "arms": d.arms,
        "fills": d.fills,
        "api_errors": d.api_errors,
        "p95_latency_ms": d.p95_latency_ms,
        "uptime_pct": finite(d.uptime_pct),
        "subtick_samples": d.subtick_samples,
        "subtick_catches": d.subtick_catches,
        "ticks_source": d.source,
    });
    let path = paths::in_root("state/scan-daily.jsonl");
    // `create_dir_all` makes the parent: on a fresh UGE_ROOT with no state/ yet, every
    // digest row would otherwise be silently dropped. Best-effort like the rest of this
    // module - a failure here must not be able to abort a tick.
    let _ = std::fs::create_dir_all(paths::in_root("state"));
    // ONE ROW PER DAY, the fuller one. `ticks` is a count of rows seen, so of two digests
    // for a day the one with more ticks is always the more complete description; a
    // second write for a day never appends and never downgrades. Pre-existing duplicates
    // in the file are collapsed by the same rule on every write, so the engine's own next
    // rotation repairs the committed record - sessions never hand-edit state/.
    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let mut by_day: std::collections::BTreeMap<String, (u64, String)> =
        std::collections::BTreeMap::new();
    // Rows without a parseable date are not ours to judge: kept verbatim, after the dated ones.
    let mut undated: Vec<String> = Vec::new();
    let mut upsert = |date: &str, ticks: u64, line: &str| match by_day.get(date) {
        Some((have, _)) if *have >= ticks => {}
        _ => {
            by_day.insert(date.to_string(), (ticks, line.to_string()));
        }
    };
    for line in existing.lines().filter(|l| !l.trim().is_empty()) {
        let parsed = serde_json::from_str::<serde_json::Value>(line).ok();
        match parsed.as_ref().and_then(|v| v["date"].as_str()) {
            Some(date) => {
                let ticks = parsed
                    .as_ref()
                    .and_then(|v| v["ticks"].as_u64())
                    .unwrap_or(0);
                upsert(date, ticks, line);
            }
            None => undated.push(line.to_string()),
        }
    }
    upsert(day, d.ticks, &row.to_string());
    let mut out: Vec<String> = by_day.into_values().map(|(_, l)| l).collect();
    out.extend(undated);
    out.push(String::new()); // trailing newline
                             // Whole-file rewrite through a temp file so a crash mid-write cannot leave the
                             // committed record truncated.
    let tmp = format!("{path}.tmp");
    if std::fs::write(&tmp, out.join("\n")).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn prunes_only_files_older_than_the_window() {
        let names = v(&[
            "scan-2031-08-25.jsonl",
            "scan-2031-08-18.jsonl",
            "scan-2031-08-17.jsonl",
            "scan.jsonl",
        ]);
        let out = files_to_prune(&names, "2031-08-25", 7);
        assert_eq!(out, v(&["scan-2031-08-17.jsonl"]));
    }

    #[test]
    fn never_prunes_the_active_file() {
        let names = v(&["scan.jsonl"]);
        assert!(files_to_prune(&names, "2031-08-25", 7).is_empty());
    }

    #[test]
    fn ignores_unrelated_and_malformed_names() {
        let names = v(&[
            "bankroll.json",
            "scan-notadate.jsonl",
            "scan-2020-01-01.jsonl.gz",
        ]);
        assert!(files_to_prune(&names, "2031-08-25", 7).is_empty());
    }

    #[test]
    fn a_bad_today_prunes_nothing() {
        // Fail closed: never delete when the reference date is unparseable.
        let names = v(&["scan-2020-01-01.jsonl"]);
        assert!(files_to_prune(&names, "garbage", 7).is_empty());
    }

    #[test]
    fn record_never_panics_when_the_log_dir_is_unwritable() {
        // A full or read-only disk must degrade logging, not stop trading.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // UGE_ROOT MUST be redirected too: `ScanLog::new()` closes out stale days from the
        // log dir into UGE_ROOT/state/scan-daily.jsonl, so both must be isolated in tests.
        let root = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", root.path().to_str().unwrap());
        std::env::set_var("UGE_LOG_DIR", "/nonexistent/deeply/nested/path");
        let mut sl = ScanLog::new();
        sl.record(
            &Tick {
                seq: 1,
                series_scanned: Vec::new(),
                markets_seen: 0,
                watching: 0,
                armed: 0,
                resting: 0,
                resting_usd: 0.0,
                open: 0,
                avail_usd: 0.0,
                in_play_usd: 0.0,
                api_calls: 0,
                api_errors: 0,
                worst_latency_ms: 0,
                duration_ms: 1,
                subtick_samples: 0,
                subtick_catches: 0,
            },
            0,
            0,
        );
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }

    fn subtick_tick(samples: u64, catches: u64) -> Tick {
        Tick {
            seq: 1,
            series_scanned: Vec::new(),
            markets_seen: 0,
            watching: 0,
            armed: 0,
            resting: 0,
            resting_usd: 0.0,
            open: 0,
            avail_usd: 0.0,
            in_play_usd: 0.0,
            api_calls: 0,
            api_errors: 0,
            worst_latency_ms: 0,
            duration_ms: 0,
            subtick_samples: samples,
            subtick_catches: catches,
        }
    }

    #[test]
    fn a_tick_row_records_what_the_sub_tick_sampler_did() {
        let _g = paths::ENV_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", dir.path());
        std::env::set_var("UGE_LOG_DIR", dir.path());
        let mut sl = ScanLog::new();
        sl.record(&subtick_tick(3, 2), 0, 0);
        let txt = std::fs::read_to_string(dir.path().join("scan.jsonl")).unwrap_or_default();
        let v: serde_json::Value =
            serde_json::from_str(txt.lines().next().unwrap_or("{}")).unwrap();
        assert_eq!(v["subtick_samples"], json!(3));
        assert_eq!(v["subtick_catches"], json!(2));
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }

    #[test]
    fn the_daily_digest_sums_subtick_counters_across_a_restart() {
        // Rebuilt from the append-only scan file, exactly like arms/fills - an
        // in-process counter would be zeroed by the restart.
        let _g = paths::ENV_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", dir.path());
        std::env::set_var("UGE_LOG_DIR", dir.path());
        let p = dir.path().join("scan.jsonl");
        std::fs::write(
            &p,
            concat!(
                r#"{"ts_et":"2031-09-11T01:00:00-0400","subtick_samples":3,"subtick_catches":1}"#,
                "
",
                r#"{"ts_et":"2031-09-11T01:00:05-0400","subtick_samples":2,"subtick_catches":0}"#,
                "
",
            ),
        )
        .unwrap();
        let d = digest_from_scanfile(p.to_str().unwrap(), "2031-09-11").unwrap();
        assert_eq!(d.subtick_samples, 5);
        assert_eq!(d.subtick_catches, 1);
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }

    #[test]
    fn the_committed_daily_row_carries_the_subtick_counters() {
        let _g = paths::ENV_LOCK.lock();
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", dir.path());
        std::env::set_var("UGE_LOG_DIR", dir.path());
        let mut d = digest(10);
        d.subtick_samples = 30;
        d.subtick_catches = 7;
        write_digest("2031-09-11", &d);
        let rows = scan_daily_rows(dir.path());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["subtick_samples"], json!(30));
        assert_eq!(rows[0]["subtick_catches"], json!(7));
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }

    fn digest(ticks: u64) -> Digest {
        Digest {
            ticks,
            markets_seen: ticks * 2,
            arms: 0,
            fills: 0,
            api_errors: 0,
            p95_latency_ms: 10,
            uptime_pct: 1.0,
            subtick_samples: 0,
            subtick_catches: 0,
            source: "scanfile",
        }
    }

    fn scan_daily_rows(root: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(root.join("state/scan-daily.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn a_second_digest_for_the_same_day_replaces_the_row_rather_than_appending() {
        // scan-daily.jsonl carried two rows for 2031-09-10 (17173 ticks and 12 ticks)
        // after a stray ScanLog::new() outside the unit wrote a second one. One row per
        // day is the contract every reader assumes.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let root = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", root.path().to_str().unwrap());
        write_digest("2031-09-09", &digest(100));
        write_digest("2031-09-10", &digest(12));
        write_digest("2031-09-10", &digest(17173));
        let rows = scan_daily_rows(root.path());
        std::env::remove_var("UGE_ROOT");
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0]["date"], "2031-09-09");
        assert_eq!(rows[1]["date"], "2031-09-10");
        assert_eq!(rows[1]["ticks"], 17173, "the fuller digest wins");
    }

    #[test]
    fn a_smaller_digest_never_overwrites_a_fuller_one_for_the_same_day() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let root = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", root.path().to_str().unwrap());
        write_digest("2031-09-10", &digest(17173));
        write_digest("2031-09-10", &digest(12));
        write_digest("2031-09-11", &digest(3));
        let rows = scan_daily_rows(root.path());
        std::env::remove_var("UGE_ROOT");
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(
            rows[0]["ticks"], 17173,
            "the 12-tick stray is dropped on the floor"
        );
        assert_eq!(rows[1]["date"], "2031-09-11");
    }

    #[test]
    fn write_digest_dedupes_rows_already_in_the_file() {
        // The live file already holds the duplicate; the engine's own next write is
        // what cleans it - sessions never hand-edit state/.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("state")).unwrap();
        std::fs::write(
            root.path().join("state/scan-daily.jsonl"),
            "{\"date\":\"2031-09-10\",\"ticks\":17173}\n{\"date\":\"2031-09-10\",\"ticks\":12}\n",
        )
        .unwrap();
        std::env::set_var("UGE_ROOT", root.path().to_str().unwrap());
        write_digest("2031-09-11", &digest(5));
        let rows = scan_daily_rows(root.path());
        std::env::remove_var("UGE_ROOT");
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0]["ticks"], 17173);
        assert_eq!(rows[1]["date"], "2031-09-11");
    }

    #[test]
    fn non_finite_floats_never_corrupt_a_row() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        let mut sl = ScanLog::new();
        sl.record(
            &Tick {
                seq: 9,
                series_scanned: Vec::new(),
                markets_seen: 0,
                watching: 0,
                armed: 0,
                resting: 0,
                resting_usd: f64::NAN,
                open: 0,
                avail_usd: f64::INFINITY,
                in_play_usd: 0.0,
                api_calls: 0,
                api_errors: 0,
                worst_latency_ms: 0,
                duration_ms: 1,
                subtick_samples: 0,
                subtick_catches: 0,
            },
            0,
            0,
        );
        let text = std::fs::read_to_string(dir.path().join("scan.jsonl")).unwrap();
        let row: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(row["resting_usd"], 0.0);
        assert_eq!(row["avail_usd"], 0.0);
        std::env::remove_var("UGE_LOG_DIR");
    }

    #[test]
    fn p95_picks_the_nearest_rank() {
        let mut v: Vec<u64> = (1..=100).collect();
        assert_eq!(p95(&mut v), 95);
        let mut one = vec![42];
        assert_eq!(p95(&mut one), 42);
        let mut none: Vec<u64> = Vec::new();
        assert_eq!(p95(&mut none), 0);
    }

    #[test]
    fn p95_sorts_unordered_input() {
        let mut v = vec![500, 10, 20, 30, 40];
        assert_eq!(p95(&mut v), 500);
    }

    #[test]
    fn p95_index_is_in_bounds_at_every_awkward_length() {
        // ceil(len*0.95)-1 must never underflow or run past the end.
        for len in [1usize, 2, 19, 20, 21, 100] {
            let mut v: Vec<u64> = (1..=len as u64).collect();
            let got = p95(&mut v);
            assert!(got >= 1 && got <= len as u64, "len {len} gave {got}");
        }
    }

    #[test]
    fn digest_appends_exactly_one_row_per_day() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", dir.path().to_str().unwrap());
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let d = Digest {
            ticks: 17280,
            markets_seen: 51840,
            arms: 12,
            fills: 3,
            api_errors: 4,
            p95_latency_ms: 310,
            uptime_pct: 99.8,
            source: "scanfile",
            subtick_samples: 0,
            subtick_catches: 0,
        };
        write_digest("2031-08-25", &d);
        write_digest("2031-08-26", &d);
        let text = std::fs::read_to_string(dir.path().join("state/scan-daily.jsonl")).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let a: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(a["date"], "2031-08-25");
        assert_eq!(a["ticks"], 17280);
        assert_eq!(a["fills"], 3);
        std::env::remove_var("UGE_ROOT");
    }

    #[test]
    fn digest_creates_state_dir_on_a_fresh_root() {
        // No state/ pre-created here, unlike the test above: a fresh UGE_ROOT must not
        // silently swallow every digest row.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", dir.path().to_str().unwrap());
        let d = Digest {
            ticks: 1,
            markets_seen: 2,
            arms: 0,
            fills: 0,
            api_errors: 0,
            p95_latency_ms: 5,
            uptime_pct: 1.0,
            source: "scanfile",
            subtick_samples: 0,
            subtick_catches: 0,
        };
        write_digest("2031-08-25", &d);
        std::env::remove_var("UGE_ROOT");
        let text = std::fs::read_to_string(dir.path().join("state/scan-daily.jsonl"))
            .expect("digest row was dropped because state/ did not exist");
        assert!(text.contains("2031-08-25"));
    }

    #[test]
    fn rotation_writes_one_digest_for_the_closing_day_and_resets() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        std::env::set_var("UGE_ROOT", dir.path().to_str().unwrap());
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let mut sl = ScanLog::new();
        let tick = |seq| Tick {
            seq,
            series_scanned: Vec::new(),
            markets_seen: 3,
            watching: 0,
            armed: 0,
            resting: 0,
            resting_usd: 0.0,
            open: 0,
            avail_usd: 0.0,
            in_play_usd: 0.0,
            api_calls: 2,
            api_errors: 1,
            worst_latency_ms: 100 + seq,
            duration_ms: 1,
            subtick_samples: 0,
            subtick_catches: 0,
        };
        sl.record(&tick(1), 1, 0);
        sl.record(&tick(2), 0, 2);
        // Backdate one day: exactly what the first tick after midnight ET sees.
        let yday = (crate::now_et().date_naive() - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        sl.day = yday.clone();
        sl.record(&tick(3), 0, 0);
        let text = std::fs::read_to_string(dir.path().join("state/scan-daily.jsonl")).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1);
        let a: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(a["date"], yday);
        assert_eq!(a["ticks"], 2);
        assert_eq!(a["markets_seen"], 6);
        assert_eq!(a["arms"], 1);
        assert_eq!(a["fills"], 2);
        assert_eq!(a["api_errors"], 2);
        assert_eq!(a["p95_latency_ms"], 102);
        assert!(a["uptime_pct"].as_f64().unwrap() > 0.0);
        // Accumulators now describe the new day only: one tick since the rollover.
        assert_eq!(sl.ticks, 1);
        assert_eq!(sl.arms, 0);
        assert_eq!(sl.latencies.len(), 1);
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }

    #[test]
    fn uptime_pct_never_exceeds_one_hundred() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut sl = ScanLog::new();
        sl.ticks = 10_000_000;
        assert_eq!(sl.digest().uptime_pct, 100.0);
    }

    #[test]
    fn latency_samples_are_capped() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        let mut sl = ScanLog::new();
        sl.latencies = vec![7u64; MAX_LATENCY_SAMPLES];
        sl.record(
            &Tick {
                seq: 1,
                series_scanned: Vec::new(),
                markets_seen: 0,
                watching: 0,
                armed: 0,
                resting: 0,
                resting_usd: 0.0,
                open: 0,
                avail_usd: 0.0,
                in_play_usd: 0.0,
                api_calls: 0,
                api_errors: 0,
                worst_latency_ms: 9,
                duration_ms: 1,
                subtick_samples: 0,
                subtick_catches: 0,
            },
            0,
            0,
        );
        assert_eq!(sl.latencies.len(), MAX_LATENCY_SAMPLES);
        std::env::remove_var("UGE_LOG_DIR");
    }

    #[test]
    fn rotation_renames_the_active_file_and_prunes_the_stale_one() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        // Rotation now also emits a digest; keep that write inside the tempdir.
        std::env::set_var("UGE_ROOT", dir.path().to_str().unwrap());
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        std::fs::write(dir.path().join("scan.jsonl"), "{\"seq\":1}\n").unwrap();
        std::fs::write(dir.path().join("scan-2000-01-01.jsonl"), "old\n").unwrap();
        let mut sl = ScanLog::new();
        // Force a day change by backdating the in-memory day marker one day - which is
        // exactly what the first tick after midnight ET sees. Yesterday on purpose: back
        // it further and the freshly renamed file would be older than KEEP_DAYS and get
        // pruned by the same call that created it.
        let yday = (crate::now_et().date_naive() - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        sl.day = yday.clone();
        sl.record(
            &Tick {
                seq: 2,
                series_scanned: Vec::new(),
                markets_seen: 0,
                watching: 0,
                armed: 0,
                resting: 0,
                resting_usd: 0.0,
                open: 0,
                avail_usd: 0.0,
                in_play_usd: 0.0,
                api_calls: 0,
                api_errors: 0,
                worst_latency_ms: 0,
                duration_ms: 1,
                subtick_samples: 0,
                subtick_catches: 0,
            },
            0,
            0,
        );
        assert!(dir.path().join(format!("scan-{yday}.jsonl")).exists());
        assert!(!dir.path().join("scan-2000-01-01.jsonl").exists());
        let text = std::fs::read_to_string(dir.path().join("scan.jsonl")).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert!(text.contains("\"seq\":2"));
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }

    // ---- restart-proof digest (regression test) ----

    /// Synthetic scan.jsonl row for testing. `arms`/`fills` omitted when `legacy`,
    /// matching a row written by a pre-enrichment binary.
    fn scan_row(day: &str, hour: u32, legacy: bool) -> String {
        if legacy {
            format!(
                "{{\"ts_et\":\"{day}T{hour:02}:00:00-0400\",\"markets_seen\":10,\"api_errors\":3,\"worst_latency_ms\":100}}\n"
            )
        } else {
            format!(
                "{{\"ts_et\":\"{day}T{hour:02}:00:00-0400\",\"markets_seen\":10,\"arms\":1,\"fills\":2,\"api_errors\":3,\"worst_latency_ms\":100}}\n"
            )
        }
    }

    #[test]
    fn scanfile_digest_counts_the_whole_day_not_just_since_the_last_restart() {
        // Regression fix: ScanLog's accumulators zero on every process start, so the
        // midnight digest used to describe only the span since the last restart. The scan
        // file is append-only and every instance appends to the same file, so all ticks
        // must be counted even across restarts.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scan.jsonl");
        let mut text = String::new();
        for h in 0..15 {
            text.push_str(&scan_row("2031-09-01", h, false));
        }
        std::fs::write(&path, &text).unwrap();
        let d = digest_from_scanfile(path.to_str().unwrap(), "2031-09-01").unwrap();
        assert_eq!(d.ticks, 15);
        assert_eq!(d.markets_seen, 150);
        assert_eq!(d.arms, 15);
        assert_eq!(d.fills, 30);
        assert_eq!(d.api_errors, 45);
        assert_eq!(d.p95_latency_ms, 100);
        assert_eq!(d.source, "scanfile");
    }

    #[test]
    fn scanfile_digest_takes_only_rows_stamped_for_the_named_day() {
        // A file that spans a midnight - engine down or restarting across the ET roll -
        // must still yield one correct digest per day, never a blend of the two.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scan.jsonl");
        let mut text = String::new();
        for h in 0..4 {
            text.push_str(&scan_row("2031-08-31", h, false));
        }
        for h in 0..9 {
            text.push_str(&scan_row("2031-09-01", h, false));
        }
        std::fs::write(&path, &text).unwrap();
        let p = path.to_str().unwrap();
        assert_eq!(digest_from_scanfile(p, "2031-08-31").unwrap().ticks, 4);
        assert_eq!(digest_from_scanfile(p, "2031-09-01").unwrap().ticks, 9);
        // A day with no rows must not produce a zeroed row; the caller falls back.
        assert!(digest_from_scanfile(p, "2031-09-02").is_none());
        assert!(digest_from_scanfile("/nonexistent/scan.jsonl", "2031-09-01").is_none());
    }

    #[test]
    fn scanfile_digest_skips_torn_and_keyless_rows_without_losing_the_day() {
        // A hard kill leaves a truncated final line, and rows written before arms/fills
        // were added carry neither key. Neither may cost the day its digest.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scan.jsonl");
        let mut text = String::new();
        text.push_str(&scan_row("2031-09-01", 1, false));
        text.push_str(&scan_row("2031-09-01", 2, true)); // legacy: no arms/fills
        text.push_str("{\"ts_et\":\"2031-09-01T03:00:00-04\n"); // torn
        text.push_str("not json at all\n");
        text.push_str("{\"markets_seen\":99}\n"); // no ts_et
        text.push_str(&scan_row("2031-09-01", 4, false));
        std::fs::write(&path, &text).unwrap();
        let d = digest_from_scanfile(path.to_str().unwrap(), "2031-09-01").unwrap();
        assert_eq!(d.ticks, 3);
        assert_eq!(d.markets_seen, 30);
        assert_eq!(d.arms, 2); // the legacy row contributes 0, and says so by omission
        assert_eq!(d.fills, 4);
    }

    #[test]
    fn stale_days_lists_only_completed_days_sorted_and_deduped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scan.jsonl");
        let mut text = String::new();
        for h in 0..3 {
            text.push_str(&scan_row("2031-09-01", h, false));
        }
        for h in 0..2 {
            text.push_str(&scan_row("2031-08-31", h, false));
        }
        for h in 0..2 {
            text.push_str(&scan_row("2031-09-02", h, false));
        }
        std::fs::write(&path, &text).unwrap();
        let p = path.to_str().unwrap();
        assert_eq!(
            stale_days_in(p, "2031-09-02"),
            vec!["2031-08-31", "2031-09-01"]
        );
        assert!(stale_days_in(p, "2031-08-31").is_empty());
        assert!(stale_days_in("/nonexistent/scan.jsonl", "2031-09-02").is_empty());
    }

    #[test]
    fn rotation_reads_the_closing_day_out_of_the_file_across_a_restart() {
        // End to end through ScanLog: three ticks, then a simulated restart (a fresh
        // ScanLog with zeroed accumulators over the same log dir), then one more tick.
        // The digest must show all four. Before the fix it showed one.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        std::env::set_var("UGE_ROOT", dir.path().to_str().unwrap());
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let tick = |seq| Tick {
            seq,
            series_scanned: Vec::new(),
            markets_seen: 5,
            watching: 0,
            armed: 0,
            resting: 0,
            resting_usd: 0.0,
            open: 0,
            avail_usd: 0.0,
            in_play_usd: 0.0,
            api_calls: 1,
            api_errors: 1,
            worst_latency_ms: 50,
            duration_ms: 1,
            subtick_samples: 0,
            subtick_catches: 0,
        };
        let mut sl = ScanLog::new();
        sl.record(&tick(1), 1, 1);
        sl.record(&tick(2), 1, 1);
        sl.record(&tick(3), 1, 1);
        drop(sl);
        let mut sl2 = ScanLog::new(); // the restart: accumulators back to zero
        sl2.record(&tick(4), 1, 1);
        assert_eq!(sl2.ticks, 1, "the in-memory count is the truncated one");
        let today = crate::day_et();
        let tomorrow = (crate::now_et().date_naive() + chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        sl2.rotate(&tomorrow);
        let text = std::fs::read_to_string(dir.path().join("state/scan-daily.jsonl")).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1);
        let a: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(a["date"], today);
        assert_eq!(
            a["ticks"], 4,
            "digest must span the restart, not start at it"
        );
        assert_eq!(a["markets_seen"], 20);
        assert_eq!(a["arms"], 4);
        assert_eq!(a["fills"], 4);
        assert_eq!(a["api_errors"], 4);
        assert_eq!(a["ticks_source"], "scanfile");
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }

    #[test]
    fn rotation_falls_back_to_memory_and_says_so_when_the_scan_file_is_gone() {
        // A digest from the accumulators may be restart-truncated, so it is the fallback
        // and it is labelled. A reader must be able to tell the two apart.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        std::env::set_var("UGE_ROOT", dir.path().to_str().unwrap());
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let mut sl = ScanLog::new();
        sl.record(
            &Tick {
                seq: 1,
                series_scanned: Vec::new(),
                markets_seen: 7,
                watching: 0,
                armed: 0,
                resting: 0,
                resting_usd: 0.0,
                open: 0,
                avail_usd: 0.0,
                in_play_usd: 0.0,
                api_calls: 1,
                api_errors: 0,
                worst_latency_ms: 9,
                duration_ms: 1,
                subtick_samples: 0,
                subtick_catches: 0,
            },
            0,
            0,
        );
        std::fs::remove_file(dir.path().join("scan.jsonl")).unwrap();
        let tomorrow = (crate::now_et().date_naive() + chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        sl.rotate(&tomorrow);
        let text = std::fs::read_to_string(dir.path().join("state/scan-daily.jsonl")).unwrap();
        let a: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert_eq!(a["ticks"], 1);
        assert_eq!(a["markets_seen"], 7);
        assert_eq!(a["ticks_source"], "memory");
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }

    #[test]
    fn startup_closes_out_a_day_that_ended_while_the_engine_was_down() {
        // rotate() only fires on a tick, so a midnight passed with the engine down left
        // the finished day with NO digest row at all, and its rows sat in scan.jsonl to
        // be filed under the wrong date by the next rotation.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        std::env::set_var("UGE_ROOT", dir.path().to_str().unwrap());
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let yday = (crate::now_et().date_naive() - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        let mut text = String::new();
        for h in 0..6 {
            text.push_str(&scan_row(&yday, h, false));
        }
        std::fs::write(dir.path().join("scan.jsonl"), &text).unwrap();
        let _sl = ScanLog::new(); // startup recovery runs here
        let out = std::fs::read_to_string(dir.path().join("state/scan-daily.jsonl"))
            .expect("the completed day must get a digest row at startup");
        let a: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(a["date"], yday);
        assert_eq!(a["ticks"], 6);
        assert_eq!(a["ticks_source"], "scanfile");
        // and the rows are filed under the day they belong to, not left to contaminate today
        assert!(dir.path().join(format!("scan-{yday}.jsonl")).exists());
        assert!(!dir.path().join("scan.jsonl").exists());
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }

    #[test]
    fn startup_recovery_is_a_no_op_on_an_ordinary_restart() {
        // The common case: a restart mid-day. The file holds only today's rows, so there
        // is nothing to close out and nothing may be renamed out from under the new
        // process.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_LOG_DIR", dir.path().to_str().unwrap());
        std::env::set_var("UGE_ROOT", dir.path().to_str().unwrap());
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let today = crate::day_et();
        let mut text = String::new();
        for h in 0..3 {
            text.push_str(&scan_row(&today, h, false));
        }
        std::fs::write(dir.path().join("scan.jsonl"), &text).unwrap();
        let _sl = ScanLog::new();
        assert!(
            dir.path().join("scan.jsonl").exists(),
            "today's file must survive"
        );
        assert!(
            !dir.path().join("state/scan-daily.jsonl").exists(),
            "no digest may be written for a day that has not ended"
        );
        std::env::remove_var("UGE_LOG_DIR");
        std::env::remove_var("UGE_ROOT");
    }
}
