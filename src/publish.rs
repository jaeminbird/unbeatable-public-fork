//! Publishes aggregated statistics snapshots to an external endpoint via HTTP POST.
//!
//! Runs on its own timer (separate from the main engine loop): the external endpoint can
//! never block trading. Uses bearer token authentication and a 20-second timeout. The
//! snapshot is built from local ledger files and optional exchange API queries.
//!
//! Security: Live open and resting positions are summarized as counts and notional amounts
//! only. No individual tickers, prices, or identifying series details appear in the payload.
//! This redaction is tested: see `live_exposure`.
//!
//! Config: `UGE_SITE_URL` (the ingest endpoint) and the bearer token from the file named by
//! `UGE_SITE_TOKEN_FILE` (default `.uge-site-token` relative to the working directory).
//! The token is never logged.

use serde_json::{json, Value};

use crate::kalshi::Signer;
use crate::ledger;
use crate::paths;

fn token_path() -> String {
    std::env::var("UGE_SITE_TOKEN_FILE").unwrap_or_else(|_| ".uge-site-token".to_string())
}

/// Summarize live exposure with no identifying detail: counts and dollars only.
/// `positions` are open positions, `orders` are resting orders from the exchange.
pub fn live_exposure(positions: &[Value], orders: &[Value]) -> Value {
    let pos_cost: f64 = positions
        .iter()
        .filter_map(|p| p.get("risk_usd").and_then(|v| v.as_f64()))
        .sum();
    let pos_markets: std::collections::HashSet<&str> = positions
        .iter()
        .filter_map(|p| p.get("ticker").and_then(|v| v.as_str()))
        .collect();
    let mut notional = 0.0;
    let mut markets: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for o in orders {
        let qty = crate::kalshi::qty_of(o, "remaining_count").unwrap_or(0.0);
        notional += qty * crate::orders::order_price_cents(o) / 100.0;
        if let Some(t) = o.get("ticker").and_then(|v| v.as_str()) {
            markets.insert(t);
        }
    }
    json!({
        "open": {"orders": positions.len(), "markets": pos_markets.len(),
                 "cost": readme_money(pos_cost)},
        "resting": {"orders": orders.len(), "markets": markets.len(),
                    "notional": readme_money(notional)},
    })
}

fn readme_money(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// One settled ledger row in compact form: [date, settled_et, opened_et, series, ticker,
/// slot, entry_price, quantity, risk, P&L, won]. Settled/closed positions only.
fn ledger_row(r: &Value) -> Option<Value> {
    r.get("exit_reason")?;
    let net = r.get("net_pnl_usd")?.as_f64()?;
    Some(json!([
        r.get("date").cloned().unwrap_or(Value::Null),
        r.get("settled_et").cloned().unwrap_or(Value::Null),
        r.get("opened_et").cloned().unwrap_or(Value::Null),
        r.get("series").cloned().unwrap_or(Value::Null),
        r.get("ticker").cloned().unwrap_or(Value::Null),
        r.get("slot").cloned().unwrap_or(Value::Null),
        r.get("entry_cents").cloned().unwrap_or(Value::Null),
        r.get("qty").cloned().unwrap_or(Value::Null),
        r.get("risk_usd").cloned().unwrap_or(Value::Null),
        net,
        r.get("realized").and_then(|v| v.as_i64()).unwrap_or(0) == 1,
    ]))
}

/// Capital flows (deposits and withdrawals) as `(date, amount)` tuples from
/// `state/capital-flows.json`: `[{"date": "YYYY-MM-DD", "usd": <amount>, ...}, ...]`.
/// Manual entries only — the exchange API provides no transfer history. This separates
/// external capital movements from trading P&L in reconciliation.
pub fn load_flows(v: &Value) -> Vec<(String, f64)> {
    let mut out: Vec<(String, f64)> = v
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| {
                    let d = f.get("date")?.as_str()?;
                    let usd = f.get("usd")?.as_f64()?;
                    (d.len() == 10 && usd.is_finite() && usd != 0.0).then(|| (d.to_string(), usd))
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// The whole snapshot. Pure given its inputs so the payload shape is testable offline.
pub fn build(
    bk: &Value,
    rows: &[Value],
    orders: &[Value],
    exchange: Option<f64>,
    flows: &[(String, f64)],
    now_et: &str,
) -> Value {
    let available = bk
        .pointer("/ledger/available")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let in_play = bk
        .pointer("/ledger/in_play")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let book = available + in_play;
    let daily = crate::stats::daily_pnl(rows);
    let opening = bk
        .pointer("/opening_capital_usd")
        .and_then(|v| v.as_f64())
        .filter(|x| *x > 0.0)
        .unwrap_or_else(|| (book - daily.iter().map(|(_, p)| p).sum::<f64>()).max(0.01));
    let positions = bk
        .get("positions_open")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    json!({
        "v": 1,
        "generated_et": now_et,
        "book": {"available": readme_money(available), "in_play": readme_money(in_play),
                 "total": readme_money(book), "opening": opening,
                 "exchange_cash": exchange.map(readme_money)},
        "exposure": live_exposure(&positions, orders),
        "daily": daily.iter().map(|(d, p)| json!([d, readme_money(*p)])).collect::<Vec<_>>(),
        "flows": flows.iter().map(|(d, u)| json!([d, readme_money(*u)])).collect::<Vec<_>>(),
        "ledger": rows.iter().filter_map(ledger_row).collect::<Vec<_>>(),
    })
}

pub fn run() -> i32 {
    let Ok(url) = std::env::var("UGE_SITE_URL") else {
        eprintln!("publish: UGE_SITE_URL not set");
        return 1;
    };
    let token = match std::fs::read_to_string(token_path()) {
        Ok(t) if !t.trim().is_empty() => t.trim().to_string(),
        _ => {
            eprintln!("publish: no token at {}", token_path());
            return 1;
        }
    };
    let bk = ledger::load_json(&paths::in_root("state/bankroll.json")).unwrap_or(Value::Null);
    let rows = ledger::load_jsonl(&paths::in_root("state/calibration.jsonl"));
    let flows = load_flows(
        &ledger::load_json(&paths::in_root("state/capital-flows.json")).unwrap_or(Value::Null),
    );
    // Read-only exchange calls. A failure degrades the snapshot, it never blocks it.
    let (orders, exchange) = match Signer::load() {
        Some(sg) => {
            let orders = sg
                .get("/portfolio/orders", "status=resting&limit=1000")
                .ok()
                .and_then(|d| d.get("orders").and_then(|v| v.as_array()).cloned())
                .unwrap_or_default();
            (orders, sg.balances().ok().map(|(t, _)| t))
        }
        None => (Vec::new(), None),
    };
    let now = crate::now_et().format("%Y-%m-%dT%H:%M:%S%z").to_string();
    let body = build(&bk, &rows, &orders, exchange, &flows, &now);
    let res = ureq::AgentBuilder::new()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .post(&url)
        .set("Authorization", &format!("Bearer {token}"))
        .send_json(body);
    match res {
        Ok(_) => 0,
        Err(ureq::Error::Status(code, _)) => {
            eprintln!("publish: site answered {code}");
            1
        }
        Err(e) => {
            eprintln!("publish: {e}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_exposure_carries_no_ticker_series_or_price() {
        let positions = vec![json!({"ticker": "EXAMPLE-1", "series": "SERIES-A",
                                    "risk_usd": 3.81, "fill_price_cents": 95.0})];
        let orders = vec![json!({"ticker": "EXAMPLE-2", "remaining_count": 4,
                                 "yes_price_dollars": "0.95"})];
        let e = live_exposure(&positions, &orders).to_string();
        assert!(
            !e.contains("EXAMPLE"),
            "no ticker or series may leave the box: {e}"
        );
        assert!(!e.contains("95"), "no price may leave the box: {e}");
        let v: Value = serde_json::from_str(&e).unwrap();
        assert_eq!(v["open"]["orders"], 1);
        assert_eq!(v["resting"]["orders"], 1);
        assert_eq!(v["resting"]["notional"], 3.8);
    }

    #[test]
    fn the_snapshot_holds_open_positions_only_as_counts() {
        let bk = json!({"opening_capital_usd": 100.0,
                        "ledger": {"available": 96.19, "in_play": 3.81},
                        "positions_open": [{"ticker": "EXAMPLE-OPEN", "risk_usd": 3.81}]});
        let rows = vec![
            json!({"date": "2030-01-01", "ticker": "EXAMPLE-SETTLED", "series": "SERIES-A",
                               "net_pnl_usd": 0.3, "exit_reason": "settlement", "realized": 1,
                               "qty": 4, "entry_cents": 92.0, "risk_usd": 3.7}),
        ];
        let s = build(&bk, &rows, &[], Some(96.2), &[], "2030-01-02T00:00:00-0400").to_string();
        assert!(!s.contains("EXAMPLE-OPEN"), "an open ticker leaked: {s}");
        assert!(
            s.contains("EXAMPLE-SETTLED"),
            "settled rows keep their ticker"
        );
    }

    #[test]
    fn flows_are_validated_and_daily_pnl_is_trading_only() {
        let bk = json!({"opening_capital_usd": 100.0,
                        "ledger": {"available": 100.5, "in_play": 0.0}});
        let row = |d: &str, net: f64| {
            json!({"date": d, "ticker": format!("EXAMPLE-{d}"),
            "series": "SERIES-A", "net_pnl_usd": net, "exit_reason": "settlement",
            "realized": 1, "qty": 4, "entry_cents": 50.0, "risk_usd": 2.0})
        };
        let rows = vec![
            row("2030-01-01", 1.0),
            row("2030-01-01", 0.5),
            row("2030-01-02", -1.0),
        ];
        let flows = load_flows(&json!([
            {"date": "2030-01-03", "usd": 25.0, "note": "deposit"},
            {"date": "bad", "usd": 5.0}, {"date": "2030-01-04", "usd": "x"}]));
        assert_eq!(
            flows,
            vec![("2030-01-03".to_string(), 25.0)],
            "malformed entries dropped"
        );
        let v = build(&bk, &rows, &[], None, &flows, "2030-01-03T09:00:00-0400");
        assert_eq!(
            v["daily"],
            json!([["2030-01-01", 1.5], ["2030-01-02", -1.0]])
        );
        assert_eq!(v["flows"], json!([["2030-01-03", 25.0]]));
    }
}
