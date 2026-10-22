//! Sub-tick book sampling. Spends the idle tail of each tick re-reading a small set of
//! watched markets at a finer interval, so the per-order "lowest bid seen" record
//! reflects what the book actually did between the coarse ticks.
//!
//! The sampler is telemetry only: it never places, cancels or re-prices an order. Its one
//! effect is tightening the running-low bid recorded on resting orders.
//!
//! Same write-path doctrine as `scanlog`: nothing here may abort a tick. No `unwrap`, no
//! `expect`, no indexing, no lock, no arithmetic that can overflow.

use serde_json::Value;

/// The sub-tick sampling interval.
pub const SUBTICK_MS: u64 = 1_000;

/// Hard ceiling on samples per tick, so a tick that returns instantly (every read failing
/// fast) cannot turn into a hot polling loop against the exchange.
pub const MAX_SAMPLES_PER_TICK: usize = 3;

/// One batched market read covers 20 tickers; past that, sampling would cost a second
/// request, so we sample fewer markets rather than spend more tick.
pub const MAX_FAST_TARGETS: usize = 20;

/// How many sub-tick samples fit in what is left of the tick.
///
/// Saturating throughout: a tick that somehow reports more elapsed than its own length
/// yields zero samples rather than wrapping to a huge count.
pub fn samples_in_budget(spent_ms: u64, tick_ms: u64, subtick_ms: u64, cap: usize) -> usize {
    if subtick_ms == 0 {
        return 0;
    }
    let remaining = tick_ms.saturating_sub(spent_ms);
    ((remaining / subtick_ms) as usize).min(cap)
}

/// The markets worth re-reading: those with orders resting first (priority), then the
/// rest of the watchlist. Deduped, order preserved so the set is stable across ticks,
/// and truncated to one batched request.
pub fn fast_targets(priority: &[String], watch: &[String], cap: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for t in priority.iter().chain(watch.iter()) {
        if out.len() >= cap {
            break;
        }
        if !out.iter().any(|x| x == t) {
            out.push(t.clone());
        }
    }
    out
}

/// Fold one observed bid into a running minimum.
///
/// A bid of 0 under an empty book is the book vanishing at settlement, not a price, and
/// is excluded - otherwise every settled market would record a spurious 0c touch.
pub fn merged_min_bid(current: f64, observed: f64) -> f64 {
    if observed > 0.0 && observed < current {
        observed
    } else {
        current
    }
}

/// Pull the YES bid (in cents) out of a batched `/markets` response.
///
/// The exchange serves `yes_bid_dollars` as a decimal string; the engine works in cents
/// everywhere, so the conversion happens once, here. A market missing the field yields no
/// entry rather than a 0c bid - absent is not the same as empty.
pub fn observed_bids(
    live: &std::collections::HashMap<String, Value>,
) -> std::collections::HashMap<String, f64> {
    let mut out = std::collections::HashMap::new();
    for (ticker, m) in live.iter() {
        let Some(v) = m.get("yes_bid_dollars") else {
            continue;
        };
        let dollars = match v {
            Value::String(s) => s.parse::<f64>().ok(),
            Value::Number(n) => n.as_f64(),
            _ => None,
        };
        if let Some(d) = dollars {
            if d.is_finite() {
                out.insert(ticker.clone(), d * 100.0);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---------------------------------------------------------------- budget

    #[test]
    fn a_tick_that_consumed_its_whole_window_gets_no_samples() {
        assert_eq!(samples_in_budget(4_800, 5_000, 1_000, 3), 0);
    }

    #[test]
    fn a_tick_that_overran_its_window_gets_no_samples() {
        assert_eq!(samples_in_budget(7_500, 5_000, 1_000, 3), 0);
    }

    #[test]
    fn the_idle_tail_of_a_median_tick_yields_three_samples() {
        // A typical fast tick leaves most of the window idle.
        assert_eq!(samples_in_budget(1_848, 5_000, 1_000, 3), 3);
    }

    #[test]
    fn a_slow_tick_still_yields_the_samples_it_can_afford() {
        assert_eq!(samples_in_budget(3_400, 5_000, 1_000, 3), 1);
    }

    #[test]
    fn the_cap_bounds_a_tick_that_returned_instantly() {
        assert_eq!(samples_in_budget(0, 5_000, 1_000, 3), 3);
    }

    #[test]
    fn without_a_cap_an_instant_tick_would_fill_the_window() {
        assert_eq!(samples_in_budget(0, 5_000, 1_000, 99), 5);
    }

    #[test]
    fn a_zero_interval_cannot_divide_by_zero() {
        assert_eq!(samples_in_budget(0, 5_000, 0, 3), 0);
    }

    // --------------------------------------------------------------- targets

    #[test]
    fn a_market_both_prioritised_and_watched_is_read_once() {
        let p = vec!["B".to_string(), "C".to_string()];
        let w = vec!["A".to_string(), "B".to_string()];
        assert_eq!(fast_targets(&p, &w, 20), vec!["B", "C", "A"]);
    }

    #[test]
    fn targets_never_exceed_one_batched_request() {
        let p: Vec<String> = (0..30).map(|i| format!("P{i}")).collect();
        let w: Vec<String> = (0..30).map(|i| format!("W{i}")).collect();
        assert_eq!(fast_targets(&p, &w, MAX_FAST_TARGETS).len(), 20);
    }

    #[test]
    fn priority_markets_come_first() {
        let p = vec!["P".to_string()];
        let w: Vec<String> = (0..30).map(|i| format!("W{i}")).collect();
        let got = fast_targets(&p, &w, 20);
        assert_eq!(got.first().map(|s| s.as_str()), Some("P"));
    }

    #[test]
    fn an_empty_roster_asks_for_nothing() {
        assert!(fast_targets(&[], &[], 20).is_empty());
    }

    // --------------------------------------------------------------- min bid

    #[test]
    fn a_lower_observed_bid_replaces_the_running_minimum() {
        assert_eq!(merged_min_bid(95.0, 87.0), 87.0);
    }

    #[test]
    fn a_higher_observed_bid_leaves_the_running_minimum_alone() {
        assert_eq!(merged_min_bid(87.0, 95.0), 87.0);
    }

    #[test]
    fn an_empty_book_never_lowers_the_running_minimum() {
        // Settlement empties the book; 0c is the absence of a price, not a 0c touch.
        assert_eq!(merged_min_bid(95.0, 0.0), 95.0);
    }

    #[test]
    fn the_sentinel_minimum_is_replaced_by_the_first_real_observation() {
        assert_eq!(merged_min_bid(999.0, 99.0), 99.0);
    }

    // -------------------------------------------------------------- extraction

    #[test]
    fn a_dollar_quote_is_read_as_cents() {
        let mut live = std::collections::HashMap::new();
        live.insert("T".to_string(), json!({"yes_bid_dollars": "0.87"}));
        assert_eq!(observed_bids(&live).get("T"), Some(&87.0));
    }

    #[test]
    fn a_numeric_dollar_quote_is_read_as_cents_too() {
        let mut live = std::collections::HashMap::new();
        live.insert("T".to_string(), json!({"yes_bid_dollars": 0.99}));
        assert_eq!(observed_bids(&live).get("T"), Some(&99.0));
    }

    #[test]
    fn a_market_missing_the_bid_field_yields_no_entry() {
        let mut live = std::collections::HashMap::new();
        live.insert("T".to_string(), json!({"status": "active"}));
        assert!(observed_bids(&live).get("T").is_none());
    }

    #[test]
    fn a_retired_cents_field_is_not_mistaken_for_the_dollar_one() {
        // yes_bid (cents) was the old field; reading it as dollars would report 9500c.
        let mut live = std::collections::HashMap::new();
        live.insert("T".to_string(), json!({"yes_bid": 95}));
        assert!(observed_bids(&live).get("T").is_none());
    }

    #[test]
    fn every_market_in_the_batch_is_extracted() {
        let mut live = std::collections::HashMap::new();
        live.insert("A".to_string(), json!({"yes_bid_dollars": "0.52"}));
        live.insert("B".to_string(), json!({"yes_bid_dollars": "0.42"}));
        let got = observed_bids(&live);
        assert_eq!(got.len(), 2);
        assert_eq!(got.get("B"), Some(&42.0));
    }
}
