//! The local ledger: bankroll file, fill accounting, day roll and settlement.
//!
//! The ledger is a plain JSON document (`state/bankroll.json`) plus append-only JSONL
//! files. Nothing here talks to the network; the engine hands in market snapshots.
//!
//! Capital model: a resting order reserves nothing. Capital is charged exactly once, in
//! [`charge_fill`], when an order actually fills, and released in [`settle_positions`].
//! The exchange balance is the ledger of record and overwrites `available` on each sync.

use std::collections::HashMap;
use std::fs;
use std::io::Write as _;

use serde_json::{json, Value};

use crate::paths;

/// Round to cents, nudged so values like 1.005 round the way a human expects.
pub fn money(x: f64) -> f64 {
    ((x + 1e-9) * 100.0).round() / 100.0
}

/// Exchange maker fee for a resting order that fills: `0.0175 * P * (1-P)` per contract,
/// to four decimals.
pub fn maker_fee_usd(price_cents: f64, qty: i64) -> f64 {
    let p = price_cents / 100.0;
    ((qty as f64 * 1.75 * p * (1.0 - p) / 100.0) * 10_000.0).round() / 10_000.0
}

/// Taker fee estimate: `0.07 * P * (1-P)` per contract, rounded to cents. Used to budget
/// the worst case when sizing an order.
pub fn taker_fee_usd(price_cents: f64, qty: i64) -> f64 {
    let p = price_cents / 100.0;
    money(qty as f64 * 7.0 * p * (1.0 - p) / 100.0)
}

/// Cost, fee and total risk of a fill, in dollars. `risk` is what gets charged.
pub fn fill_accounting(filled: i64, fill_c: f64) -> (f64, f64, f64) {
    let cost = money(filled as f64 * fill_c / 100.0);
    let fee = maker_fee_usd(fill_c, filled);
    (cost, fee, ((cost + fee) * 10_000.0).round() / 10_000.0)
}

// ------------------------------------------------------------------- file io

pub fn bankroll_path() -> String {
    paths::in_root("state/bankroll.json")
}

pub fn load_json(path: &str) -> Option<Value> {
    serde_json::from_str(&fs::read_to_string(path).ok()?).ok()
}

/// Every parseable line of a JSONL file. A truncated final line (hard kill mid-write) is
/// skipped rather than failing the whole read.
pub fn load_jsonl(path: &str) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

pub fn load_bankroll() -> Value {
    load_json(&bankroll_path()).unwrap_or_else(|| json!({}))
}

pub fn save_bankroll(bk: &Value) {
    let mut s = serde_json::to_string_pretty(bk).unwrap_or_default();
    s.push('\n');
    let _ = fs::create_dir_all(paths::in_root("state"));
    let _ = fs::write(bankroll_path(), s);
}

pub fn append_line(path: &str, v: &Value) {
    if let Some(dir) = std::path::Path::new(path).parent() {
        let _ = fs::create_dir_all(dir);
    }
    if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(f, "{}", serde_json::to_string(v).unwrap_or_default());
    }
}

/// A fresh book holding `opening_usd` of free capital.
pub fn new_bankroll(opening_usd: f64) -> Value {
    json!({
        "opening_capital_usd": opening_usd,
        "ledger": {"available": opening_usd, "in_play": 0.0, "settled_pnl": 0.0},
        "positions_open": [],
        "positions_settled_today": [],
        "markets_traded_today": [],
    })
}

// ------------------------------------------------------------------- day roll

/// Roll the book to a new ET day. Digests the day being closed before the marker moves;
/// the digest is keyed by date and idempotent, so a crash-restart cannot double-count it.
pub fn roll_day(bk: &mut Value) {
    let today = crate::day_et();
    if bk.get("day_marker").and_then(|v| v.as_str()) == Some(today.as_str()) {
        return;
    }
    if let Some(prev) = bk.get("day_marker").and_then(|v| v.as_str()) {
        let rows = load_jsonl(&paths::in_root("state/calibration.jsonl"));
        crate::stats::append_daily_digest(&rows, prev);
    }
    bk["day_marker"] = json!(today);
    bk["positions_settled_today"] = json!([]);
    let open: Vec<Value> = bk["positions_open"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|p| p.get("ticker").cloned())
        .collect();
    bk["markets_traded_today"] = Value::Array(open);
}

// ------------------------------------------------------------------- capital

/// Move `risk` from `available` into `in_play`. This is the only place capital is charged.
///
/// `available` may go negative when several orders fill inside one tick. That is tolerated
/// rather than clamped (clamping would invent money); the next balance sync overwrites it
/// with the exchange's own figure.
pub fn charge_fill(bk: &mut Value, risk: f64) {
    // A non-finite risk would serialise as `null` and silently turn `available` into
    // "not a number". Refuse rather than corrupt.
    if !risk.is_finite() {
        crate::log(&format!(
            "REFUSED to charge a non-finite risk ({risk}) - ledger left intact"
        ));
        return;
    }
    let led = &mut bk["ledger"];
    led["available"] = json!(money(led["available"].as_f64().unwrap_or(0.0) - risk));
    led["in_play"] = json!(money(led["in_play"].as_f64().unwrap_or(0.0) + risk));
}

/// The open-position record written when a resting bid fills.
#[allow(clippy::too_many_arguments)]
pub fn position_record(
    ticker: &str,
    series: &str,
    order_id: &str,
    slot: u64,
    limit_price: f64,
    close_ts: i64,
    filled: i64,
    fill_c: f64,
) -> Value {
    let (cost, fee, risk) = fill_accounting(filled, fill_c);
    json!({
        "ticker": ticker, "series": series, "side": "yes",
        "qty": filled, "fill_price_cents": (fill_c * 10.0).round() / 10.0,
        "cost_usd": cost, "fee_est_usd": fee, "risk_usd": risk,
        "opened_et": crate::ts_et(),
        "state_at_entry": format!("resting {limit_price:.0}c GTC bid filled"),
        "order_id": order_id, "close_ts": close_ts, "slot": slot,
    })
}

/// Settle every open position whose market has resolved. Returns log lines.
///
/// Positions ride to settlement; there is no stop-loss. A market missing from `live` is
/// looked up with `fetch`, and a lookup failure leaves the position open for next tick.
pub fn settle_positions<F: Fn(&str) -> Option<Value>>(
    bk: &mut Value,
    live: &HashMap<String, Value>,
    fetch: F,
) -> Vec<String> {
    let mut logs = Vec::new();
    let open = bk["positions_open"].as_array().cloned().unwrap_or_default();
    let mut still = Vec::new();
    for pos in open {
        let ticker = pos["ticker"].as_str().unwrap_or("").to_string();
        let m = match live.get(&ticker) {
            Some(m) => m.clone(),
            None => match fetch(&ticker) {
                Some(m) => m,
                None => {
                    still.push(pos);
                    continue;
                }
            },
        };
        let status = m.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let result = m.get("result").and_then(|v| v.as_str()).unwrap_or("");
        if !(status == "finalized" || status == "settled" || result == "yes" || result == "no") {
            still.push(pos);
            continue;
        }
        let side = pos["side"].as_str().unwrap_or("yes");
        let won = result == side;
        let qty = pos["qty"].as_i64().unwrap_or(0);
        let risk = pos["risk_usd"].as_f64().unwrap_or(0.0);
        let proceeds = money(if won { qty as f64 } else { 0.0 });
        let net = money(proceeds - risk);

        let led = &mut bk["ledger"];
        led["in_play"] = json!(money(led["in_play"].as_f64().unwrap_or(0.0) - risk));
        led["available"] = json!(money(led["available"].as_f64().unwrap_or(0.0) + proceeds));
        led["settled_pnl"] = json!(money(led["settled_pnl"].as_f64().unwrap_or(0.0) + net));

        let mut rec = pos.clone();
        rec["result"] = json!(if won { "WIN" } else { "LOSS" });
        rec["exit_reason"] = json!("settlement");
        rec["payout_usd"] = json!(proceeds);
        rec["net_pnl_usd"] = json!(net);
        rec["settled_et"] = json!(crate::ts_et());
        if let Some(a) = bk["positions_settled_today"].as_array_mut() {
            a.push(rec);
        }

        append_line(
            &paths::in_root("state/calibration.jsonl"),
            &json!({
                "date": crate::day_et(), "series": pos.get("series").cloned().unwrap_or(Value::Null),
                "ticker": ticker, "side": side, "qty": qty,
                "entry_cents": pos["fill_price_cents"],
                "exit_reason": "settlement", "realized": if won { 1 } else { 0 },
                "slot": pos.get("slot").cloned().unwrap_or(Value::Null),
                "net_pnl_usd": net, "engine": "uge",
                "risk_usd": pos.get("risk_usd").cloned().unwrap_or(Value::Null),
                "opened_et": pos.get("opened_et").cloned().unwrap_or(Value::Null),
                "settled_et": crate::ts_et(),
                "close_ts": pos.get("close_ts").cloned().unwrap_or(Value::Null),
            }),
        );
        logs.push(format!(
            "SETTLE {ticker}: {} net ${:+.2}",
            if won { "WIN" } else { "LOSS" },
            net
        ));
    }
    bk["positions_open"] = Value::Array(still);
    logs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_root<T>(f: impl FnOnce() -> T) -> T {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", dir.path());
        let out = f();
        std::env::remove_var("UGE_ROOT");
        out
    }

    #[test]
    fn money_rounds_to_cents() {
        assert_eq!(money(1.005), 1.01);
        assert_eq!(money(2.0), 2.0);
    }

    #[test]
    fn fees_are_symmetric_around_fifty_cents_and_zero_at_the_edges() {
        assert_eq!(maker_fee_usd(30.0, 10), maker_fee_usd(70.0, 10));
        assert_eq!(maker_fee_usd(100.0, 10), 0.0);
        assert!(taker_fee_usd(50.0, 10) > maker_fee_usd(50.0, 10));
    }

    #[test]
    fn charging_a_fill_moves_available_into_in_play() {
        let mut bk = new_bankroll(100.0);
        let (_, _, risk) = fill_accounting(4, 25.0);
        charge_fill(&mut bk, risk);
        let avail = bk["ledger"]["available"].as_f64().unwrap();
        let in_play = bk["ledger"]["in_play"].as_f64().unwrap();
        assert!((avail + in_play - 100.0).abs() < 0.011);
        assert!(in_play > 1.0);
    }

    #[test]
    fn a_non_finite_risk_never_reaches_the_ledger() {
        let mut bk = new_bankroll(100.0);
        charge_fill(&mut bk, f64::NAN);
        assert_eq!(bk["ledger"]["available"], json!(100.0));
    }

    #[test]
    fn settlement_pays_winners_and_writes_the_calibration_row() {
        with_root(|| {
            let mut bk = new_bankroll(100.0);
            let pos = position_record("EXAMPLE-1", "SERIES-A", "oid", 1, 25.0, 0, 4, 25.0);
            let risk = pos["risk_usd"].as_f64().unwrap();
            charge_fill(&mut bk, risk);
            bk["positions_open"] = json!([pos]);
            let live: HashMap<String, Value> = [(
                "EXAMPLE-1".to_string(),
                json!({"status": "finalized", "result": "yes"}),
            )]
            .into();
            let logs = settle_positions(&mut bk, &live, |_| None);
            assert_eq!(logs.len(), 1);
            assert!(logs[0].contains("WIN"));
            assert_eq!(bk["positions_open"].as_array().unwrap().len(), 0);
            assert_eq!(bk["ledger"]["in_play"], json!(0.0));
            assert!(bk["ledger"]["settled_pnl"].as_f64().unwrap() > 2.9);
            let rows = load_jsonl(&paths::in_root("state/calibration.jsonl"));
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["realized"], 1);
        });
    }

    #[test]
    fn settlement_writes_off_losers_and_leaves_unresolved_markets_open() {
        with_root(|| {
            let mut bk = new_bankroll(100.0);
            let a = position_record("EXAMPLE-L", "SERIES-A", "o1", 1, 25.0, 0, 2, 25.0);
            let b = position_record("EXAMPLE-O", "SERIES-A", "o2", 1, 25.0, 0, 2, 25.0);
            bk["positions_open"] = json!([a, b]);
            let live: HashMap<String, Value> = [
                (
                    "EXAMPLE-L".to_string(),
                    json!({"status": "settled", "result": "no"}),
                ),
                ("EXAMPLE-O".to_string(), json!({"status": "active"})),
            ]
            .into();
            let logs = settle_positions(&mut bk, &live, |_| None);
            assert!(logs[0].contains("LOSS"));
            assert_eq!(bk["positions_open"].as_array().unwrap().len(), 1);
            assert!(bk["ledger"]["settled_pnl"].as_f64().unwrap() < 0.0);
        });
    }

    #[test]
    fn a_failed_lookup_leaves_the_position_open() {
        with_root(|| {
            let mut bk = new_bankroll(100.0);
            bk["positions_open"] =
                json!([position_record("EXAMPLE-X", "S", "o", 1, 25.0, 0, 1, 25.0)]);
            let logs = settle_positions(&mut bk, &HashMap::new(), |_| None);
            assert!(logs.is_empty());
            assert_eq!(bk["positions_open"].as_array().unwrap().len(), 1);
        });
    }

    #[test]
    fn roll_day_is_idempotent_within_a_day() {
        with_root(|| {
            let mut bk = new_bankroll(100.0);
            roll_day(&mut bk);
            let marker = bk["day_marker"].clone();
            bk["positions_settled_today"] = json!([{"x": 1}]);
            roll_day(&mut bk);
            assert_eq!(bk["day_marker"], marker);
            assert_eq!(bk["positions_settled_today"].as_array().unwrap().len(), 1);
        });
    }

    #[test]
    fn jsonl_reader_skips_a_truncated_final_line() {
        with_root(|| {
            let p = paths::in_root("state/x.jsonl");
            append_line(&p, &json!({"a": 1}));
            let mut f = fs::OpenOptions::new().append(true).open(&p).unwrap();
            write!(f, "{{\"a\": ").unwrap();
            assert_eq!(load_jsonl(&p).len(), 1);
        });
    }
}
