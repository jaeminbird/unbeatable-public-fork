//! Market data: watchlist scanning, batched polling, and snapshot extraction.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::Value;

use crate::kalshi::{f_of, get_json, KALSHI};
use crate::strategy::MarketView;

/// Courtesy delay between per-series list requests, so a scan slice averages well under
/// the exchange's read budget instead of bursting.
const SCAN_PAUSE_MS: u64 = 150;

/// Take the next `n` entries of `list` starting at `cursor`, returning the slice and the
/// new cursor. The tail slice may be short; the cursor wraps to 0 after it rather than
/// straddling the end, so every entry is visited exactly once per cycle.
pub fn next_slice<'a>(list: &[&'a str], cursor: usize, n: usize) -> (Vec<&'a str>, usize) {
    if list.is_empty() || n == 0 {
        return (Vec::new(), 0);
    }
    let start = cursor % list.len();
    let end = (start + n).min(list.len());
    let next = if end >= list.len() { 0 } else { end };
    (list[start..end].to_vec(), next)
}

/// Watchlist admission, pure so it is testable without the network: the market must be
/// active and its best bid at least `min_bid_c`.
pub fn scan_admits(m: &Value, min_bid_c: f64) -> bool {
    if m.get("status").and_then(|v| v.as_str()) != Some("active") {
        return false;
    }
    f_of(m, "yes_bid_dollars").unwrap_or(0.0) * 100.0 >= min_bid_c
}

/// Scan one slice of the series list. Returns (series scanned, ticker -> series admitted)
/// so the caller can replace exactly those series' entries and leave the rest of the
/// watchlist intact: a partial refresh, not a partial watchlist.
pub fn scan_series_slice(
    series_slice: &[&str],
    min_bid_c: f64,
) -> (Vec<String>, HashMap<String, String>) {
    let mut watch = HashMap::new();
    for series in series_slice {
        let url = format!("{KALSHI}/markets?series_ticker={series}&status=open&limit=200");
        let Ok(d) = get_json(&url) else { continue };
        for m in d
            .get("markets")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            let Some(ticker) = m.get("ticker").and_then(|v| v.as_str()) else {
                continue;
            };
            if scan_admits(&m, min_bid_c) {
                watch.insert(ticker.to_string(), series.to_string());
            }
        }
        std::thread::sleep(Duration::from_millis(SCAN_PAUSE_MS));
    }
    (series_slice.iter().map(|s| s.to_string()).collect(), watch)
}

/// Batched snapshot of many markets, 20 tickers per request. A failed chunk is skipped;
/// the next tick re-reads it.
pub fn poll_markets(tickers: &[String]) -> HashMap<String, Value> {
    let mut out = HashMap::new();
    for chunk in tickers.chunks(20) {
        let url = format!("{KALSHI}/markets?tickers={}", chunk.join(","));
        let Ok(d) = get_json(&url) else { continue };
        for m in d
            .get("markets")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            if let Some(t) = m.get("ticker").and_then(|v| v.as_str()) {
                out.insert(t.to_string(), m);
            }
        }
    }
    out
}

/// One market by ticker, for positions that dropped out of the batched poll.
pub fn fetch_market(ticker: &str) -> Option<Value> {
    let d = get_json(&format!("{KALSHI}/markets/{ticker}")).ok()?;
    Some(d.get("market").cloned().unwrap_or(d))
}

/// Kalshi `close_time` (RFC 3339) -> epoch seconds.
pub fn parse_close_ts(m: &Value) -> Option<i64> {
    let s = m.get("close_time")?.as_str()?;
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp())
}

/// Build the strategy-facing snapshot of one market.
pub fn market_view(ticker: &str, series: &str, m: &Value) -> MarketView {
    let cents = |k: &str| {
        f_of(m, k)
            .filter(|d| *d > 0.0)
            .map(|d| (d * 1000.0).round() / 10.0)
    };
    MarketView {
        ticker: ticker.to_string(),
        series: series.to_string(),
        status: m
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        bid_c: cents("yes_bid_dollars"),
        ask_c: cents("yes_ask_dollars"),
        close_ts: parse_close_ts(m).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn next_slice_visits_every_entry_once_per_cycle() {
        let list = ["a", "b", "c", "d", "e"];
        let (s1, c1) = next_slice(&list, 0, 2);
        let (s2, c2) = next_slice(&list, c1, 2);
        let (s3, c3) = next_slice(&list, c2, 2);
        assert_eq!(
            (s1, s2, s3.clone()),
            (vec!["a", "b"], vec!["c", "d"], vec!["e"])
        );
        assert_eq!(c3, 0, "cursor wraps after a short tail slice");
    }

    #[test]
    fn next_slice_handles_empty_and_oversized_requests() {
        assert_eq!(next_slice(&[], 3, 5), (Vec::<&str>::new(), 0));
        let list = ["a", "b"];
        assert_eq!(next_slice(&list, 0, 10), (vec!["a", "b"], 0));
        assert_eq!(
            next_slice(&list, 7, 1).0,
            vec!["b"],
            "a stale cursor is reduced modulo len"
        );
    }

    #[test]
    fn admission_needs_an_active_market_with_enough_bid() {
        let m = json!({"status": "active", "yes_bid_dollars": "0.40"});
        assert!(scan_admits(&m, 40.0));
        assert!(!scan_admits(&m, 41.0));
        assert!(!scan_admits(
            &json!({"status": "closed", "yes_bid_dollars": "0.90"}),
            1.0
        ));
        assert!(!scan_admits(&json!({"status": "active"}), 1.0));
    }

    #[test]
    fn snapshot_converts_dollars_to_cents_and_parses_close() {
        let m = json!({"status": "active", "yes_bid_dollars": "0.2500", "yes_ask_dollars": 0.27,
                       "close_time": "2030-01-01T00:00:00Z"});
        let v = market_view("EXAMPLE-1", "SERIES-A", &m);
        assert_eq!(v.bid_c, Some(25.0));
        assert_eq!(v.ask_c, Some(27.0));
        assert_eq!(v.close_ts, 1_893_456_000);
        assert!(v.is_active());
    }

    #[test]
    fn an_empty_book_yields_no_price_not_zero() {
        let v = market_view(
            "EXAMPLE-1",
            "S",
            &json!({"status": "active", "yes_bid_dollars": "0.0000"}),
        );
        assert_eq!(v.bid_c, None);
    }
}
