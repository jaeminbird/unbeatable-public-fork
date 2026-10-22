//! Derived performance statistics, computed from `state/calibration.jsonl`: win rate, average
//! win %, average loss %, profit factor, max drawdown, and performance sliced by
//! hour-of-day, series and order slot.
//!
//! DERIVED, NEVER STORED. Every figure here is recomputed from the settled-position
//! ledger on demand, so there is no second book to drift out of sync and no
//! counter to survive a restart wrongly . The ledger rows carry the raw fields (`risk_usd`,
//! `opened_et`, `settled_et`, `series`, `slot`) precisely so this module can
//! stay stateless.
//!
//! Three surfaces: `render_md` inside STATUS.md every state-changing tick, the
//! `uge stats` subcommand for a terminal view of the same, and one digest row per
//! day appended to `state/stats-daily.jsonl` at day roll (idempotent per date).
//!
//! Max drawdown here is SETTLE-ORDER drawdown: the equity path sampled at each
//! settlement, which is what the calibration ledger can see. Intraday drawdown —
//! equity falling between settlements while orders are open — needs the per-tick
//! `avail_usd`/`in_play_usd` samples in the scan log and is a research-repo job.

use serde_json::{json, Value};

use crate::paths;

pub struct Stats {
    pub fills: u64,
    pub wins: u64,
    pub losses: u64,
    pub net_pnl: f64,
    pub avg_win_pct: f64,   // mean net/risk over winning orders
    pub avg_loss_pct: f64,  // mean net/risk over losing orders (negative)
    pub profit_factor: f64, // gross wins / |gross losses|; INFINITY when lossless
    pub max_dd_pct: f64,    // settle-order, as a share of peak equity
    /// (label, fills, wins, net) — hour-of-day ET, series, slot. Only non-empty
    /// slices are kept.
    pub by_hour: Vec<(String, u64, u64, f64)>,
    pub by_series: Vec<(String, u64, u64, f64)>,
    pub by_slot: Vec<(String, u64, u64, f64)>,
}

fn slice_of(map: &mut Vec<(String, u64, u64, f64)>, key: String, won: bool, net: f64) {
    match map.iter_mut().find(|(k, _, _, _)| *k == key) {
        Some(e) => {
            e.1 += 1;
            if won {
                e.2 += 1;
            }
            e.3 += net;
        }
        None => map.push((key, 1, u64::from(won), net)),
    }
}

/// The hour an order's money was actually put at risk — the FILL hour, not the
/// settle hour. Time-of-day performance keyed on settlement would smear every
/// evening game into the small hours it happened to settle in.
fn fill_hour(r: &Value) -> Option<String> {
    let et = r.get("opened_et")?.as_str()?;
    // "2031-09-09T21:47:06-0400" -> "21"
    et.get(11..13).map(|h| format!("{h}:00 ET"))
}

/// `opening` is the capital the book started with; drawdown is measured against
/// peak EQUITY (opening + cumulative P&L), not peak profit.
pub fn compute(rows: &[Value], opening: f64) -> Stats {
    let mut st = Stats {
        fills: 0,
        wins: 0,
        losses: 0,
        net_pnl: 0.0,
        avg_win_pct: 0.0,
        avg_loss_pct: 0.0,
        profit_factor: 0.0,
        max_dd_pct: 0.0,
        by_hour: Vec::new(),
        by_series: Vec::new(),
        by_slot: Vec::new(),
    };
    let (mut win_pct_sum, mut loss_pct_sum) = (0.0, 0.0);
    let (mut gross_win, mut gross_loss) = (0.0, 0.0);
    let base = if opening > 0.0 { opening } else { 0.0 };
    let (mut equity, mut peak): (f64, f64) = (base, base);
    for r in rows {
        let Some(net) = r.get("net_pnl_usd").and_then(|v| v.as_f64()) else {
            continue;
        };
        if r.get("exit_reason").is_none() {
            continue; // not a settled row
        }
        let won = net >= 0.0;
        st.fills += 1;
        st.net_pnl += net;
        // %-of-stake needs the stake; rows from before the enrichment lack it and
        // simply do not contribute to the averages (never guessed).
        let risk = r
            .get("risk_usd")
            .and_then(|v| v.as_f64())
            .filter(|x| *x > 0.0);
        if won {
            st.wins += 1;
            gross_win += net;
            if let Some(k) = risk {
                win_pct_sum += net / k;
            }
        } else {
            st.losses += 1;
            gross_loss += -net;
            if let Some(k) = risk {
                loss_pct_sum += net / k;
            }
        }
        equity += net;
        peak = peak.max(equity);
        if peak > 0.0 {
            st.max_dd_pct = st.max_dd_pct.max((peak - equity) / peak);
        }
        if let Some(h) = fill_hour(r) {
            slice_of(&mut st.by_hour, h, won, net);
        }
        if let Some(sr) = r.get("series").and_then(|v| v.as_str()) {
            slice_of(&mut st.by_series, sr.to_string(), won, net);
        }
        if let Some(k) = r.get("slot").and_then(|v| v.as_u64()) {
            slice_of(&mut st.by_slot, format!("K={k}"), won, net);
        }
    }
    if st.wins > 0 {
        st.avg_win_pct = win_pct_sum / st.wins as f64;
    }
    if st.losses > 0 {
        st.avg_loss_pct = loss_pct_sum / st.losses as f64;
    }
    st.profit_factor = if gross_loss > 0.0 {
        gross_win / gross_loss
    } else {
        f64::INFINITY
    };
    st.by_hour.sort_by(|a, b| a.0.cmp(&b.0));
    st.by_series
        .sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));
    st.by_slot.sort_by(|a, b| a.0.cmp(&b.0));
    st
}

fn slice_table(title: &str, rows: &[(String, u64, u64, f64)]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let mut s =
        format!("\n**{title}**\n\n| slice | fills | W-L | win% | net |\n|---|---|---|---|---|\n");
    for (k, n, w, net) in rows {
        s.push_str(&format!(
            "| {k} | {n} | {w}W-{}L | {:.1}% | `${:+.2}` |\n",
            n - w,
            100.0 * *w as f64 / *n as f64,
            net
        ));
    }
    s
}

/// The stats block STATUS.md embeds and `uge stats` prints.
pub fn render_md(st: &Stats) -> String {
    if st.fills == 0 {
        return "*No settled orders yet — statistics begin with the first settlement.*\n"
            .to_string();
    }
    let mut s = String::new();
    s.push_str(&format!(
        "| | |\n|---|---|\n\
         | settled orders | {} ({}W-{}L) |\n\
         | win rate | {:.2}% |\n\
         | avg win | {:+.2}% of stake |\n\
         | avg loss | {:+.2}% of stake |\n\
         | profit factor | {} |\n\
         | net P&L | `${:+.2}` |\n\
         | max drawdown (settle-order) | {:.1}% |\n",
        st.fills,
        st.wins,
        st.losses,
        100.0 * st.wins as f64 / st.fills as f64,
        100.0 * st.avg_win_pct,
        100.0 * st.avg_loss_pct,
        if st.profit_factor.is_finite() {
            format!("{:.2}", st.profit_factor)
        } else {
            "∞ (no losses yet)".to_string()
        },
        st.net_pnl,
        100.0 * st.max_dd_pct
    ));
    s.push_str(&slice_table("By fill hour (ET)", &st.by_hour));
    s.push_str(&slice_table("By series", &st.by_series));
    s.push_str(&slice_table("By slot", &st.by_slot));
    s
}

/// Append one digest row for `date` to `state/stats-daily.jsonl`, once. Called at
/// day roll with the day just ended; idempotent because a crash between roll and
/// append (or a restart re-rolling) must not double-count a day.
pub fn append_daily_digest(rows: &[Value], date: &str) {
    let path = paths::in_root("state/stats-daily.jsonl");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        if existing
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .any(|r| r.get("date").and_then(|v| v.as_str()) == Some(date))
        {
            return;
        }
    }
    let day_rows: Vec<Value> = rows
        .iter()
        .filter(|r| r.get("date").and_then(|v| v.as_str()) == Some(date))
        .cloned()
        .collect();
    if day_rows.is_empty() {
        return; // a quiet day writes nothing; the calendar gap itself is the record
    }
    let opening = opening_capital(rows);
    let day = compute(&day_rows, opening);
    let all = compute(rows, opening);
    crate::ledger::append_line(
        &path,
        &json!({
            "date": date,
            "fills": day.fills, "wins": day.wins, "losses": day.losses,
            "net_pnl_usd": (day.net_pnl * 100.0).round() / 100.0,
            "win_rate": if day.fills > 0 { day.wins as f64 / day.fills as f64 } else { 0.0 },
            "cum_fills": all.fills,
            "cum_net_pnl_usd": (all.net_pnl * 100.0).round() / 100.0,
            "cum_max_dd_pct": (all.max_dd_pct * 10000.0).round() / 10000.0,
        }),
    );
}

/// Opening capital: the stored figure if present, else current book minus all
/// settled P&L. 0.0 (drawdown vs peak-profit) only when no ledger exists at all.
pub fn opening_capital(rows: &[Value]) -> f64 {
    let bk =
        crate::ledger::load_json(&paths::in_root("state/bankroll.json")).unwrap_or(Value::Null);
    if let Some(x) = bk.get("opening_capital_usd").and_then(|v| v.as_f64()) {
        if x > 0.0 {
            return x;
        }
    }
    let avail = bk
        .pointer("/ledger/available")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let in_play = bk
        .pointer("/ledger/in_play")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let net: f64 = rows
        .iter()
        .filter(|r| r.get("exit_reason").is_some())
        .filter_map(|r| r.get("net_pnl_usd").and_then(|v| v.as_f64()))
        .sum();
    (avail + in_play - net).max(0.0)
}

/// `uge stats` — print the full table to stdout. Read-only.
pub fn run() -> i32 {
    let rows = crate::ledger::load_jsonl(&paths::in_root("state/calibration.jsonl"));
    let opening = opening_capital(&rows);
    print!("{}", render_md(&compute(&rows, opening)));
    0
}

/// Net P&L per settlement date, ascending by date. Only rows that carry an
/// `exit_reason` (i.e. settled positions) count.
pub fn daily_pnl(rows: &[Value]) -> Vec<(String, f64)> {
    let mut m: std::collections::BTreeMap<String, f64> = std::collections::BTreeMap::new();
    for r in rows.iter().filter(|r| r.get("exit_reason").is_some()) {
        if let (Some(d), Some(n)) = (
            r.get("date").and_then(|v| v.as_str()),
            r.get("net_pnl_usd").and_then(|v| v.as_f64()),
        ) {
            *m.entry(d.to_string()).or_insert(0.0) += n;
        }
    }
    m.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(date: &str, opened: &str, series: &str, k: u64, net: f64, risk: f64) -> Value {
        json!({"date": date, "opened_et": opened, "series": series, "slot": k,
               "net_pnl_usd": net, "risk_usd": risk, "exit_reason": "settlement",
               "realized": if net >= 0.0 {1} else {0}})
    }

    #[test]
    fn the_headline_numbers_are_computed_not_stored() {
        let rows = vec![
            row(
                "2031-09-10",
                "2031-09-10T19:05:00-0400",
                "EXAMPLE-1",
                1,
                0.30,
                3.0,
            ),
            row(
                "2031-09-10",
                "2031-09-10T19:20:00-0400",
                "EXAMPLE-1",
                2,
                0.60,
                3.0,
            ),
            row(
                "2031-09-11",
                "2031-09-11T21:05:00-0400",
                "EXAMPLE-2",
                1,
                -3.0,
                3.0,
            ),
        ];
        let st = compute(&rows, 100.0);
        assert_eq!((st.fills, st.wins, st.losses), (3, 2, 1));
        assert!((st.net_pnl - -2.10).abs() < 1e-9);
        assert!((st.avg_win_pct - 0.15).abs() < 1e-9, "mean of 10% and 20%");
        assert!(
            (st.avg_loss_pct - -1.0).abs() < 1e-9,
            "the loss cost 100% of stake"
        );
        assert!(
            (st.profit_factor - 0.30).abs() < 1e-9,
            "0.90 won / 3.00 lost"
        );
        // equity path computation: starting at 100, peaks at 100.90, ends at 97.90
        assert!((st.max_dd_pct - 3.0 / 100.9).abs() < 1e-9);
    }

    #[test]
    fn slices_key_on_fill_hour_series_and_slot() {
        let rows = vec![
            row(
                "2031-09-10",
                "2031-09-10T19:05:00-0400",
                "EXAMPLE-1",
                1,
                0.30,
                3.0,
            ),
            row(
                "2031-09-10",
                "2031-09-10T19:59:00-0400",
                "EXAMPLE-1",
                1,
                0.30,
                3.0,
            ),
            row(
                "2031-09-10",
                "2031-09-10T04:10:00-0400",
                "EXAMPLE-2",
                2,
                -3.0,
                3.0,
            ),
        ];
        let st = compute(&rows, 100.0);
        assert_eq!(st.by_hour.len(), 2, "19:00 and 04:00 buckets");
        let h19 = st.by_hour.iter().find(|(k, ..)| k == "19:00 ET").unwrap();
        assert_eq!((h19.1, h19.2), (2, 2));
        assert_eq!(st.by_series.len(), 2);
        assert_eq!(st.by_slot.len(), 2);
    }

    #[test]
    fn a_lossless_book_renders_and_does_not_divide_by_zero() {
        let rows = vec![row(
            "2031-09-10",
            "2031-09-10T19:05:00-0400",
            "EXAMPLE-1",
            1,
            0.30,
            3.0,
        )];
        let st = compute(&rows, 100.0);
        assert!(st.profit_factor.is_infinite());
        assert_eq!(st.max_dd_pct, 0.0);
        let md = render_md(&st);
        assert!(md.contains("no losses yet"), "{md}");
        assert!(render_md(&compute(&[], 100.0)).contains("No settled orders yet"));
    }

    #[test]
    fn rows_without_risk_never_poison_the_percentages() {
        // A row missing risk_usd (pre-enrichment shape) counts toward W-L and net,
        // but contributes nothing to the %-of-stake averages - never guessed.
        let mut r = row(
            "2031-09-10",
            "2031-09-10T19:05:00-0400",
            "EXAMPLE-1",
            1,
            0.5,
            3.0,
        );
        r.as_object_mut().unwrap().remove("risk_usd");
        let st = compute(&[r], 100.0);
        assert_eq!(st.wins, 1);
        assert_eq!(st.avg_win_pct, 0.0, "no stake, no percentage");
    }
}
