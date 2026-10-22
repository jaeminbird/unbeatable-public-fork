//! uge - a small trading-engine skeleton for Kalshi, in Rust.
//!
//! The engine owns everything mechanical:
//!
//! ```text
//!   watchlist scan (rotating slices) -> batched poll -> settle -> reconcile resting
//!     -> Strategy::evaluate -> apply actions (validated, idempotent, backed off)
//!     -> sub-tick re-sampling -> per-tick telemetry -> heartbeat
//! ```
//!
//! What to trade is decided behind the [`strategy::Strategy`] trait; the only strategy in
//! this crate is a toy. The production strategy is private.
//!
//! Configuration is by environment variable (see [`Config::from_env`]). Nothing trades
//! unless `UGE_LIVE=1`, credentials load, and `--dry-run` is absent.

#![cfg_attr(
    test,
    allow(clippy::unnecessary_literal_unwrap, clippy::unnecessary_get_then_check)
)]

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::time::{Duration, Instant};

use chrono::Utc;
use chrono_tz::America::New_York;
use serde_json::{json, Value};

mod cli;
mod kalshi;
mod ledger;
mod market;
mod orders;
mod paths;
mod persist;
mod pricelog;
mod publish;
mod scanlog;
mod stats;
mod strategy;
mod subtick;

use kalshi::Signer;
use orders::{Backoff, Exchange, Resting};
use strategy::{EngineContext, Strategy};

/// Seconds per engine tick. Also read by `scanlog` to compute uptime.
pub(crate) const TICK_S: u64 = 5;
const HEARTBEAT_S: u64 = 600;
const BALANCE_SYNC_S: u64 = 300;

// -------------------------------------------------------------------- time/log

pub(crate) fn now_et() -> chrono::DateTime<chrono_tz::Tz> {
    Utc::now().with_timezone(&New_York)
}

pub(crate) fn ts_et() -> String {
    now_et().format("%Y-%m-%dT%H:%M:%S%z").to_string()
}

pub(crate) fn day_et() -> String {
    now_et().format("%Y-%m-%d").to_string()
}

pub(crate) fn log(msg: &str) {
    println!("[{}] {}", ts_et(), msg);
    let _ = std::io::stdout().flush();
}

// ---------------------------------------------------------------------- config

/// Engine configuration, all from environment variables.
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    /// `UGE_SERIES`: comma-separated series tickers to watch.
    pub series: Vec<String>,
    /// `UGE_SCAN_PER_TICK`: series scanned per tick (default 5).
    pub scan_per_tick: usize,
    /// `UGE_WATCH_MIN_BID_C`: minimum best bid, in cents, for a market to be watched (default 1).
    pub watch_min_bid_c: f64,
    /// `UGE_MAX_ORDER_USD`: per-order notional cap (default 5).
    pub max_order_usd: f64,
    /// `UGE_OPENING_USD`: seed capital for a brand-new ledger (unset = refuse to invent one).
    pub opening_usd: Option<f64>,
}

impl Config {
    pub fn from_env() -> Config {
        Config::from_lookup(|k| std::env::var(k).ok())
    }

    /// Parsing is split from the environment so it can be tested without mutating it.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Config {
        let num = |k: &str, d: f64| {
            get(k)
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|x| x.is_finite() && *x >= 0.0)
                .unwrap_or(d)
        };
        Config {
            series: get("UGE_SERIES")
                .map(|s| {
                    s.split(',')
                        .map(|x| x.trim().to_string())
                        .filter(|x| !x.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            scan_per_tick: num("UGE_SCAN_PER_TICK", 5.0) as usize,
            watch_min_bid_c: num("UGE_WATCH_MIN_BID_C", 1.0),
            max_order_usd: num("UGE_MAX_ORDER_USD", 5.0),
            opening_usd: get("UGE_OPENING_USD")
                .and_then(|v| v.trim().parse().ok())
                .filter(|x: &f64| x.is_finite() && *x > 0.0),
        }
    }
}

// ----------------------------------------------------------------- engine step

/// Mutable engine state that lives across ticks.
#[derive(Default)]
struct EngineState {
    /// ticker -> series, the current watchlist.
    watch: HashMap<String, String>,
    resting: HashMap<String, Resting>,
    hard_fail: HashMap<String, Instant>,
    backoff: Backoff,
    /// Dry-run log lines already printed, so a dry run is not a wall of repeats.
    dry_seen: HashSet<String>,
}

/// One pass of the trading logic over an already-fetched market snapshot. No scanning or
/// polling happens here, which is what makes the whole lifecycle testable with a fake
/// exchange: settle -> reconcile -> strategy -> apply.
#[allow(clippy::too_many_arguments)]
fn step<E: Exchange, S: Strategy + ?Sized>(
    bk: &mut Value,
    live: &HashMap<String, Value>,
    st: &mut EngineState,
    signer: &Option<E>,
    strat: &mut S,
    cfg: &Config,
    dry: bool,
    fetch: &dyn Fn(&str) -> Option<Value>,
) -> Vec<String> {
    let mut logs = ledger::settle_positions(bk, live, fetch);

    let (l, fills) = orders::reconcile_resting(bk, &mut st.resting, live, signer);
    logs.extend(l);
    for (ticker, qty, px) in fills {
        strat.on_fill(&ticker, qty, px);
    }

    let held: HashSet<String> = bk["positions_open"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| p["ticker"].as_str().map(String::from))
        .collect();
    let available = bk["ledger"]["available"].as_f64().unwrap_or(0.0);
    let ctx = EngineContext {
        now_ts: Utc::now().timestamp(),
        available_usd: available,
        max_order_usd: cfg.max_order_usd,
        held,
    };
    let mut views: Vec<_> = live
        .iter()
        .map(|(t, m)| {
            let series = st
                .watch
                .get(t)
                .cloned()
                .unwrap_or_else(|| t.split('-').next().unwrap_or("").to_string());
            market::market_view(t, &series, m)
        })
        .collect();
    views.sort_by(|a, b| a.ticker.cmp(&b.ticker)); // stable input order for the strategy
    let mut rest: Vec<_> = st.resting.values().map(|r| r.view()).collect();
    rest.sort_by(|a, b| a.order_id.cmp(&b.order_id));

    let actions = strat.evaluate(&ctx, &views, &rest);
    let apply = orders::ApplyCtx {
        signer,
        dry,
        available,
        max_order_usd: cfg.max_order_usd,
    };
    let applied = orders::apply_actions(
        actions,
        &apply,
        &mut st.resting,
        &mut st.hard_fail,
        &mut st.backoff,
    );
    logs.extend(
        applied
            .into_iter()
            .filter(|l| !l.starts_with("DRY") || st.dry_seen.insert(l.clone())),
    );

    // Bounded like every other long-lived map: a day is far past the cooldown.
    st.hard_fail.retain(|_, t| t.elapsed().as_secs() < 86_400);
    let keep: HashSet<String> = live.keys().cloned().collect();
    st.backoff.retain_tickers(&keep);
    logs
}

/// Sub-tick pass: re-read `targets` up to `n_samples` times, tightening the running-low
/// bid on resting orders. Generic over the poller and sleeper so it is testable offline.
/// Returns `(samples_taken, catches)`; it never places or cancels anything.
fn subtick_pass<P: Fn(&[String]) -> HashMap<String, Value>, S: Fn()>(
    n_samples: usize,
    targets: &[String],
    poll: P,
    sleep: S,
    resting: &mut HashMap<String, Resting>,
) -> (u64, u64) {
    let (mut taken, mut catches) = (0u64, 0u64);
    if targets.is_empty() {
        return (taken, catches);
    }
    for _ in 0..n_samples {
        // Sleep first: the tick's own poll read these markets moments ago.
        sleep();
        let bids = subtick::observed_bids(&poll(targets));
        taken = taken.saturating_add(1);
        for r in resting.values_mut() {
            let Some(&bid) = bids.get(&r.ticker) else {
                continue;
            };
            let merged = subtick::merged_min_bid(r.min_bid_seen, bid);
            if merged < r.min_bid_seen {
                r.min_bid_seen = merged;
                catches = catches.saturating_add(1);
            }
        }
    }
    (taken, catches)
}

// ---------------------------------------------------------------------- status

fn write_status(bk: &Value, resting: &HashMap<String, Resting>, note: &str) {
    let led = &bk["ledger"];
    let avail = led["available"].as_f64().unwrap_or(0.0);
    let in_play = led["in_play"].as_f64().unwrap_or(0.0);
    let mut s = String::from("# uge - status\n\n");
    s.push_str(&format!(
        "*Last state-changing tick: {} ET - {note}*\n\n",
        ts_et()
    ));
    s.push_str(&format!(
        "- **Book (total):** ${:.2}  (available ${avail:.2} + in play ${in_play:.2})\n",
        ledger::money(avail + in_play)
    ));
    s.push_str(&format!(
        "- **Settled P&L:** ${:+.2}\n",
        led["settled_pnl"].as_f64().unwrap_or(0.0)
    ));
    s.push_str(&format!(
        "- **Committed to resting orders:** ${:.2} ({} order(s))\n\n",
        orders::resting_committed(resting),
        resting.len()
    ));

    s.push_str("## Resting orders\n\n");
    if resting.is_empty() {
        s.push_str("*None.*\n");
    } else {
        s.push_str("| ticker | slot | qty | committed | limit | placed (ET) |\n|---|---|---|---|---|---|\n");
        let mut rows: Vec<_> = resting.values().collect();
        rows.sort_by(|a, b| a.ticker.cmp(&b.ticker).then(a.slot.cmp(&b.slot)));
        for r in rows {
            s.push_str(&format!(
                "| {} | {} | {} | ${:.2} | {:.0}c | {} |\n",
                r.ticker, r.slot, r.qty, r.notional, r.limit_price, r.placed_et
            ));
        }
    }

    s.push_str("\n## Open positions\n\n");
    let open = bk["positions_open"].as_array().cloned().unwrap_or_default();
    if open.is_empty() {
        s.push_str("*None.*\n");
    } else {
        s.push_str("| ticker | qty | entry | risk | opened (ET) |\n|---|---|---|---|---|\n");
        for p in open {
            s.push_str(&format!(
                "| {} | {} | {}c | ${:.2} | {} |\n",
                p["ticker"].as_str().unwrap_or(""),
                p["qty"].as_i64().unwrap_or(0),
                p["fill_price_cents"],
                p["risk_usd"].as_f64().unwrap_or(0.0),
                p["opened_et"].as_str().unwrap_or("")
            ));
        }
    }

    s.push_str("\n## Statistics (derived from state/calibration.jsonl)\n\n");
    let rows = ledger::load_jsonl(&paths::in_root("state/calibration.jsonl"));
    s.push_str(&stats::render_md(&stats::compute(
        &rows,
        stats::opening_capital(&rows),
    )));
    let _ = std::fs::create_dir_all(paths::in_root("state"));
    let _ = std::fs::write(paths::in_root("state/STATUS.md"), s);
}

// ---------------------------------------------------------------- engine loop

/// Seed `resting` from orders already on the exchange so they are cancelled at close.
fn adopt_resting(sg: &Signer, resting: &mut HashMap<String, Resting>) {
    match sg.get("/portfolio/orders", "status=resting") {
        Ok(d) => {
            let list = d
                .get("orders")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            for r in orders::adopt_candidates(&list) {
                log(&format!(
                    "startup: adopted {} x{} @ {:.0}c (order {})",
                    r.ticker,
                    r.qty,
                    r.limit_price,
                    &r.order_id[..8.min(r.order_id.len())]
                ));
                resting.insert(r.order_id.clone(), r);
            }
        }
        Err(e) => log(&format!("startup resting-order check FAILED: {e}")),
    }
}

/// Exchange balance is the ledger of record: overwrite `available` when it disagrees.
fn sync_balance(sg: &Signer, bk: &mut Value, logs: &mut Vec<String>) {
    match sg.balances() {
        Ok((bal, by_shard)) => {
            if !by_shard.is_empty() {
                bk["ledger"]["available_by_shard"] = json!(by_shard);
            }
            let free = ledger::money(bal);
            let cur = bk["ledger"]["available"].as_f64().unwrap_or(0.0);
            if (free - cur).abs() > 0.01 {
                logs.push(format!("LIVE balance sync: ${cur:.2} -> ${free:.2}"));
                bk["ledger"]["available"] = json!(free);
            }
        }
        Err(e) => logs.push(format!("LIVE balance sync FAILED: {e}")),
    }
}

fn run_engine(dry: bool, once: bool) {
    let cfg = Config::from_env();
    let want_live = std::env::var("UGE_LIVE").ok().as_deref() == Some("1") && !dry;
    let signer = if want_live { Signer::load() } else { None };
    let mut strat = strategy::ThresholdBid::from_env();

    log(&format!(
        "uge starting (dry={dry}, live={}, tick {TICK_S}s, strategy {}, {} series x{}/tick)",
        signer.is_some(),
        strat.name(),
        cfg.series.len(),
        cfg.scan_per_tick
    ));
    log(&format!(
        "paths: root={} log_dir={} signer: {}",
        paths::root(),
        paths::log_dir(),
        if signer.is_some() {
            "present"
        } else {
            "absent"
        }
    ));
    if want_live && signer.is_none() {
        log("WARNING: UGE_LIVE=1 but credentials failed to load - no orders will be placed");
    }
    if cfg.series.is_empty() {
        log("WARNING: UGE_SERIES is empty - nothing to watch");
    }

    let mut st = EngineState::default();
    if let Some(sg) = &signer {
        adopt_resting(sg, &mut st.resting);
    }
    let series: Vec<&str> = cfg.series.iter().map(|s| s.as_str()).collect();
    let mut cursor = 0usize;
    let mut last_hb = Instant::now() - Duration::from_secs(HEARTBEAT_S * 2);
    let mut last_sync = Instant::now() - Duration::from_secs(BALANCE_SYNC_S * 2);
    let mut scan_log = scanlog::ScanLog::new();
    let mut price_log = pricelog::PriceLog::new();
    let mut tick_seq = 0u64;
    let mut prev_busy = false;

    loop {
        let t0 = Instant::now();
        let mut bk = ledger::load_bankroll();
        if bk.get("ledger").is_none() {
            // Seed a book only when explicitly asked: a mistyped UGE_ROOT must never
            // silently start a fresh one.
            match cfg.opening_usd {
                Some(x) => {
                    log(&format!(
                        "no ledger found - seeding ${x:.2} from UGE_OPENING_USD"
                    ));
                    bk = ledger::new_bankroll(x);
                }
                None => {
                    log("bankroll missing/unreadable (set UGE_OPENING_USD to seed one) - retrying next tick");
                    if once {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(TICK_S));
                    continue;
                }
            }
        }
        let before = bk.clone();
        ledger::roll_day(&mut bk);

        // Rotating watchlist scan: a slice of the roster per tick keeps each tick short
        // and the request rate flat. Only the series just scanned are replaced.
        let (slice, next) = market::next_slice(&series, cursor, cfg.scan_per_tick);
        cursor = next;
        let (scanned, found) = market::scan_series_slice(&slice, cfg.watch_min_bid_c);
        st.watch.retain(|_, sv| !scanned.contains(sv));
        st.watch.extend(found);

        // Poll everything that can still matter: watched, resting, and held markets.
        let mut tickers: Vec<String> = st.watch.keys().cloned().collect();
        let extra = st.resting.values().map(|r| r.ticker.clone()).chain(
            bk["positions_open"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|p| p["ticker"].as_str().map(String::from)),
        );
        for t in extra {
            if !tickers.contains(&t) {
                tickers.push(t);
            }
        }
        let live = if tickers.is_empty() {
            HashMap::new()
        } else {
            market::poll_markets(&tickers)
        };
        price_log.record(&live);

        let mut logs = Vec::new();
        if let Some(sg) = &signer {
            if prev_busy || last_sync.elapsed().as_secs() >= BALANCE_SYNC_S {
                sync_balance(sg, &mut bk, &mut logs);
                last_sync = Instant::now();
            }
        }
        logs.extend(step(
            &mut bk,
            &live,
            &mut st,
            &signer,
            &mut strat,
            &cfg,
            dry,
            &market::fetch_market,
        ));
        prev_busy = logs
            .iter()
            .any(|l| l.starts_with("FILLED") || l.starts_with("SETTLE"));

        let changed = bk != before
            || logs.iter().any(|l| {
                ["REST", "FILLED", "CANCEL", "SETTLE"]
                    .iter()
                    .any(|p| l.starts_with(p))
            });
        if changed && !dry {
            write_status(&bk, &st.resting, &logs.join("; "));
            ledger::save_bankroll(&bk);
        }
        for l in &logs {
            log(&format!("  {l}"));
        }
        if last_hb.elapsed().as_secs() >= HEARTBEAT_S {
            log(&format!(
                "heartbeat: watching {} market(s), resting {} (${:.2}), open {}, avail ${:.2}",
                st.watch.len(),
                st.resting.len(),
                orders::resting_committed(&st.resting),
                bk["positions_open"]
                    .as_array()
                    .map(|a| a.len())
                    .unwrap_or(0),
                bk["ledger"]["available"].as_f64().unwrap_or(0.0)
            ));
            last_hb = Instant::now();
        }

        // The tick's work is done; `work_ms` excludes the sub-tick sampler so
        // `duration_ms` keeps meaning "time spent working".
        tick_seq += 1;
        let work_ms = t0.elapsed().as_millis() as u64;
        let mut priority: Vec<String> = st.resting.values().map(|r| r.ticker.clone()).collect();
        priority.sort();
        let mut watch_keys: Vec<String> = st.watch.keys().cloned().collect();
        watch_keys.sort();
        let fast = subtick::fast_targets(&priority, &watch_keys, subtick::MAX_FAST_TARGETS);
        let budget = subtick::samples_in_budget(
            work_ms,
            TICK_S * 1_000,
            subtick::SUBTICK_MS,
            subtick::MAX_SAMPLES_PER_TICK,
        );
        let (samples, catches) = subtick_pass(
            budget,
            &fast,
            market::poll_markets,
            || std::thread::sleep(Duration::from_millis(subtick::SUBTICK_MS)),
            &mut st.resting,
        );

        // Read the counters ONCE so the three logged numbers describe the same instant.
        let (api_calls, api_errors, worst_latency_ms) = kalshi::api_counters();
        scan_log.record(
            &scanlog::Tick {
                seq: tick_seq,
                series_scanned: scanned,
                markets_seen: live.len(),
                watching: st.watch.len(),
                armed: priority.len(),
                resting: st.resting.len(),
                resting_usd: orders::resting_committed(&st.resting),
                open: bk["positions_open"]
                    .as_array()
                    .map(|a| a.len())
                    .unwrap_or(0),
                avail_usd: bk["ledger"]["available"].as_f64().unwrap_or(0.0),
                in_play_usd: bk["ledger"]["in_play"].as_f64().unwrap_or(0.0),
                api_calls,
                api_errors,
                worst_latency_ms,
                duration_ms: work_ms,
                subtick_samples: samples,
                subtick_catches: catches,
            },
            logs.iter().filter(|l| l.starts_with("REST ")).count(),
            logs.iter().filter(|l| l.starts_with("FILLED")).count(),
        );
        kalshi::reset_api_counters();

        if once {
            break;
        }
        let spent = t0.elapsed().as_secs_f64();
        if spent < TICK_S as f64 {
            std::thread::sleep(Duration::from_secs_f64(TICK_S as f64 - spent));
        }
    }
}

// ------------------------------------------------------------------------ cli

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match cli::parse(&args) {
        cli::Command::Engine { dry, once } => run_engine(dry, once),
        cli::Command::Balance => cmd_balance(),
        cli::Command::Orders => cmd_orders(),
        cli::Command::Cancel(id) => cmd_cancel(&id),
        cli::Command::Persist => std::process::exit(persist::run()),
        cli::Command::Stats => std::process::exit(stats::run()),
        cli::Command::Publish => std::process::exit(publish::run()),
        cli::Command::Help => {
            eprintln!("uge - a trading-engine skeleton\n\
                       \n\
                       usage:\n\
                       \x20 uge [--dry-run] [--once]   run the engine loop (default)\n\
                       \x20 uge balance                print the exchange balance\n\
                       \x20 uge orders                 list resting orders\n\
                       \x20 uge cancel <order_id>      cancel one resting order\n\
                       \x20 uge stats                  print win rate / drawdown / slice tables\n\
                       \x20 uge persist                commit and push state/\n\
                       \x20 uge publish                push a stats snapshot to a dashboard endpoint");
            std::process::exit(2);
        }
    }
}

fn require_signer() -> Signer {
    Signer::load().unwrap_or_else(|| {
        eprintln!("no credentials at {}", paths::key_dir());
        std::process::exit(1);
    })
}

fn cmd_balance() {
    match require_signer().balance_usd() {
        Ok(b) => println!("{b:.2}"),
        Err(e) => {
            eprintln!("balance failed: {e}");
            std::process::exit(1);
        }
    }
}

/// Read-only. One line per resting order: ticker, remaining qty, order id.
fn cmd_orders() {
    match require_signer().get("/portfolio/orders", "status=resting") {
        Ok(d) => {
            let list = d
                .get("orders")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if list.is_empty() {
                println!("no resting orders");
            }
            for o in list {
                let tk = o.get("ticker").and_then(|v| v.as_str()).unwrap_or("?");
                let oid = o.get("order_id").and_then(|v| v.as_str()).unwrap_or("?");
                let qty = kalshi::qty_of(&o, "remaining_count").unwrap_or(0.0) as i64;
                println!("{tk}\t{qty}\t{oid}");
            }
        }
        Err(e) => {
            eprintln!("orders failed: {e}");
            std::process::exit(1);
        }
    }
}

/// Manual escape hatch for a stranded order while the engine is down. Uses the same
/// `Signer::cancel` as the engine, so there is exactly one cancel-request builder.
fn cmd_cancel(order_id: &str) {
    match require_signer().cancel(order_id) {
        Ok(_) => println!("cancelled {order_id}"),
        Err(e) => {
            eprintln!("cancel failed: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::ThresholdBid;
    use std::cell::RefCell;

    #[derive(Default)]
    struct Fake {
        on_book: RefCell<HashMap<String, i64>>,
        fills: RefCell<HashMap<String, (i64, Option<f64>)>>,
        n: RefCell<u32>,
    }

    impl Exchange for Fake {
        fn resting_orders(&self) -> Result<HashMap<String, i64>, String> {
            Ok(self.on_book.borrow().clone())
        }
        fn fills_for(&self, id: &str) -> Result<(i64, Option<f64>), String> {
            Ok(self.fills.borrow().get(id).cloned().unwrap_or((0, None)))
        }
        fn cancel(&self, id: &str) -> Result<i64, String> {
            self.on_book.borrow_mut().remove(id);
            Ok(1)
        }
        fn place_gtc_bid(
            &self,
            _t: &str,
            _q: i64,
            _p: f64,
        ) -> Result<(String, i64, Option<f64>), String> {
            *self.n.borrow_mut() += 1;
            let id = format!("order-{}", self.n.borrow());
            self.on_book.borrow_mut().insert(id.clone(), 0);
            Ok((id, 0, None))
        }
    }

    fn cfg() -> Config {
        Config::from_lookup(|_| None)
    }

    fn quote(status: &str, bid: &str, ask: &str) -> Value {
        json!({"status": status, "yes_bid_dollars": bid, "yes_ask_dollars": ask})
    }

    #[test]
    fn config_parses_the_environment_and_falls_back_on_garbage() {
        let env: HashMap<&str, &str> = [
            ("UGE_SERIES", " AAA, BBB ,,"),
            ("UGE_SCAN_PER_TICK", "7"),
            ("UGE_MAX_ORDER_USD", "nope"),
            ("UGE_OPENING_USD", "250"),
        ]
        .into();
        let c = Config::from_lookup(|k| env.get(k).map(|v| v.to_string()));
        assert_eq!(c.series, vec!["AAA", "BBB"]);
        assert_eq!(c.scan_per_tick, 7);
        assert_eq!(c.max_order_usd, 5.0);
        assert_eq!(c.opening_usd, Some(250.0));
        assert_eq!(cfg().opening_usd, None);
        assert!(cfg().series.is_empty());
    }

    /// The whole loop body, end to end, against a fake exchange and the toy strategy:
    /// place -> rest -> fill -> position -> settle -> ledger.
    #[test]
    fn full_order_lifecycle_through_the_strategy_seam() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", dir.path());

        let sg = Some(Fake::default());
        let mut st = EngineState::default();
        let mut strat = ThresholdBid::default();
        let mut bk = ledger::new_bankroll(100.0);
        let no_fetch = |_: &str| None;

        // Tick 1: a cheap active market -> the toy strategy rests one bid.
        let live: HashMap<String, Value> =
            [("EXAMPLE-1".to_string(), quote("active", "0.2000", "0.2200"))].into();
        let logs = step(
            &mut bk,
            &live,
            &mut st,
            &sg,
            &mut strat,
            &cfg(),
            false,
            &no_fetch,
        );
        assert!(
            logs.iter().any(|l| l.starts_with("REST EXAMPLE-1")),
            "{logs:?}"
        );
        assert_eq!(st.resting.len(), 1);

        // Tick 2: nothing changed -> no duplicate order.
        step(
            &mut bk,
            &live,
            &mut st,
            &sg,
            &mut strat,
            &cfg(),
            false,
            &no_fetch,
        );
        assert_eq!(*sg.as_ref().unwrap().n.borrow(), 1);

        // Tick 3: the order fills (leaves the book, fills feed reports it).
        let oid = st.resting.keys().next().unwrap().clone();
        sg.as_ref().unwrap().on_book.borrow_mut().remove(&oid);
        sg.as_ref()
            .unwrap()
            .fills
            .borrow_mut()
            .insert(oid, (1, Some(20.0)));
        let logs = step(
            &mut bk,
            &live,
            &mut st,
            &sg,
            &mut strat,
            &cfg(),
            false,
            &no_fetch,
        );
        assert!(logs.iter().any(|l| l.starts_with("FILLED")), "{logs:?}");
        assert_eq!(bk["positions_open"].as_array().unwrap().len(), 1);
        assert!(st.resting.is_empty());
        // The held market is not re-bid.
        assert_eq!(*sg.as_ref().unwrap().n.borrow(), 1);

        // Tick 4: the market settles YES -> the position pays out into the ledger.
        let live: HashMap<String, Value> = [(
            "EXAMPLE-1".to_string(),
            json!({"status": "finalized", "result": "yes"}),
        )]
        .into();
        let logs = step(
            &mut bk,
            &live,
            &mut st,
            &sg,
            &mut strat,
            &cfg(),
            false,
            &no_fetch,
        );
        assert!(
            logs.iter()
                .any(|l| l.starts_with("SETTLE") && l.contains("WIN")),
            "{logs:?}"
        );
        assert!(bk["positions_open"].as_array().unwrap().is_empty());
        let pnl = bk["ledger"]["settled_pnl"].as_f64().unwrap();
        assert!(
            pnl > 0.7 && pnl < 0.81,
            "1 contract bought at 20c pays $1: {pnl}"
        );
        assert_eq!(bk["ledger"]["in_play"], json!(0.0));
        std::env::remove_var("UGE_ROOT");
    }

    #[test]
    fn dry_run_logs_intent_once_and_places_nothing() {
        let sg: Option<Fake> = Some(Fake::default());
        let mut st = EngineState::default();
        let mut strat = ThresholdBid::default();
        let mut bk = ledger::new_bankroll(100.0);
        let live: HashMap<String, Value> =
            [("EXAMPLE-1".to_string(), quote("active", "0.1000", "0.1200"))].into();
        let first = step(
            &mut bk,
            &live,
            &mut st,
            &sg,
            &mut strat,
            &cfg(),
            true,
            &|_| None,
        );
        let second = step(
            &mut bk,
            &live,
            &mut st,
            &sg,
            &mut strat,
            &cfg(),
            true,
            &|_| None,
        );
        assert_eq!(first.len(), 1);
        assert!(second.is_empty(), "the same dry-run line is not repeated");
        assert_eq!(*sg.unwrap().n.borrow(), 0);
    }

    #[test]
    fn the_strategy_can_be_driven_as_a_trait_object() {
        let sg: Option<Fake> = None;
        let mut st = EngineState::default();
        let mut strat: Box<dyn Strategy> = Box::new(ThresholdBid::default());
        let mut bk = ledger::new_bankroll(100.0);
        let live: HashMap<String, Value> =
            [("EXAMPLE-1".to_string(), quote("active", "0.1000", "0.1200"))].into();
        let logs = step(
            &mut bk,
            &live,
            &mut st,
            &sg,
            strat.as_mut(),
            &cfg(),
            false,
            &|_| None,
        );
        assert!(
            logs[0].starts_with("DRY place"),
            "no signer means no orders: {logs:?}"
        );
    }

    #[test]
    fn subtick_pass_tightens_the_running_low_and_counts_catches() {
        let mut resting: HashMap<String, Resting> = HashMap::new();
        resting.insert(
            "o".into(),
            Resting {
                order_id: "o".into(),
                ticker: "EXAMPLE-1".into(),
                series: "S".into(),
                slot: 1,
                qty: 1,
                notional: 0.2,
                limit_price: 20.0,
                placed_et: String::new(),
                placed_ts: 0,
                close_ts: 0,
                min_bid_seen: 25.0,
                cancel_failed_ts: 0,
                cancel_unreachable: false,
            },
        );
        let calls = RefCell::new(0u32);
        let poll = |_: &[String]| {
            *calls.borrow_mut() += 1;
            let bid = if *calls.borrow() == 1 {
                "0.2200"
            } else {
                "0.2400"
            };
            HashMap::from([("EXAMPLE-1".to_string(), json!({"yes_bid_dollars": bid}))])
        };
        let (taken, catches) =
            subtick_pass(3, &["EXAMPLE-1".to_string()], poll, || {}, &mut resting);
        assert_eq!((taken, catches), (3, 1));
        assert_eq!(resting["o"].min_bid_seen, 22.0);
        assert_eq!(
            subtick_pass(3, &[], |_| HashMap::new(), || {}, &mut resting),
            (0, 0)
        );
    }

    #[test]
    fn status_renders_without_a_signer_or_history() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", dir.path());
        write_status(&ledger::new_bankroll(100.0), &HashMap::new(), "test");
        let md = std::fs::read_to_string(paths::in_root("state/STATUS.md")).unwrap();
        assert!(md.contains("Book (total)") && md.contains("$100.00"));
        std::env::remove_var("UGE_ROOT");
    }
}
