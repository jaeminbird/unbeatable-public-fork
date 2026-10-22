//! The strategy seam.
//!
//! The engine owns everything mechanical: scanning, polling, order placement, fill
//! detection, accounting, telemetry. A [`Strategy`] owns exactly one decision: given the
//! current market snapshots and what is already resting, which orders should exist?
//! The engine drives orders ONLY through this trait; a strategy never touches the
//! exchange, the ledger or the clock.
//!
//! Contract the engine enforces on whatever a strategy returns:
//!   - a `Place` for a `(ticker, slot)` that already has a resting order is dropped, so a
//!     strategy may re-emit its intents every tick and stay idempotent;
//!   - quantity must be >= 1, price strictly inside (0, 100) cents, and notional must fit
//!     `max_order_usd`;
//!   - placement is subject to the engine's cooldowns and backoff, which a strategy cannot
//!     bypass;
//!   - tickers with an open position are listed in [`EngineContext::held`], leaving
//!     "one position per market" to the strategy's own policy.
//!
//! The production strategy is private and is not part of this crate. [`ThresholdBid`] is
//! a toy that exists to exercise the seam end to end.

use std::collections::HashSet;

/// A point-in-time view of one watched market. Prices are in cents.
#[derive(Clone, Debug, PartialEq)]
pub struct MarketView {
    pub ticker: String,
    pub series: String,
    pub status: String,
    pub bid_c: Option<f64>,
    pub ask_c: Option<f64>,
    /// Scheduled close, epoch seconds; 0 when unknown.
    pub close_ts: i64,
}

impl MarketView {
    pub fn is_active(&self) -> bool {
        self.status == "active"
    }
}

/// A resting order the engine is tracking.
#[derive(Clone, Debug, PartialEq)]
pub struct RestingOrder {
    pub order_id: String,
    pub ticker: String,
    pub slot: u64,
    pub limit_price_c: f64,
    pub qty: i64,
}

/// Engine state a strategy may condition on. Read-only.
#[derive(Clone, Debug, Default)]
pub struct EngineContext {
    pub now_ts: i64,
    pub available_usd: f64,
    pub max_order_usd: f64,
    /// Tickers that currently have an open (filled, unsettled) position.
    pub held: HashSet<String>,
}

/// A request to rest a GTC buy-YES limit order. `slot` distinguishes several orders on
/// one market (slot 1 is the first); the engine treats `(ticker, slot)` as the identity.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderIntent {
    pub ticker: String,
    pub series: String,
    pub slot: u64,
    pub qty: i64,
    pub limit_price_c: f64,
    pub close_ts: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    Place(OrderIntent),
    Cancel { order_id: String, reason: String },
}

pub trait Strategy {
    /// Short stable name, written into logs.
    fn name(&self) -> &'static str;

    /// Called once per tick with fresh snapshots. Returns the actions the engine should
    /// attempt. Must be a pure function of its inputs and its own private state.
    fn evaluate(
        &mut self,
        ctx: &EngineContext,
        markets: &[MarketView],
        resting: &[RestingOrder],
    ) -> Vec<Action>;

    /// Notification that a tracked order filled. Default: ignore.
    fn on_fill(&mut self, _ticker: &str, _qty: i64, _price_c: f64) {}
}

/// TOY EXAMPLE - illustrative only; it has no edge and is not the production strategy.
/// The real strategy is private.
///
/// Policy: on any active market whose best bid is at or below `max_bid_c`, rest a small
/// GTC bid joined to that bid (never crossing the ask). Do not stack orders on a market
/// that already has one resting or an open position. Cancel a resting order once the
/// market's bid has drifted more than `stale_gap_c` away from it.
#[derive(Clone, Debug)]
pub struct ThresholdBid {
    pub max_bid_c: f64,
    pub qty: i64,
    pub stale_gap_c: f64,
}

impl Default for ThresholdBid {
    fn default() -> Self {
        ThresholdBid {
            max_bid_c: 30.0,
            qty: 1,
            stale_gap_c: 10.0,
        }
    }
}

impl ThresholdBid {
    /// Build from `UGE_TOY_MAX_BID_C`, `UGE_TOY_QTY` and `UGE_TOY_STALE_GAP_C`.
    pub fn from_env() -> Self {
        let d = ThresholdBid::default();
        let f = |k: &str, dv: f64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(dv)
        };
        ThresholdBid {
            max_bid_c: f("UGE_TOY_MAX_BID_C", d.max_bid_c),
            qty: f("UGE_TOY_QTY", d.qty as f64) as i64,
            stale_gap_c: f("UGE_TOY_STALE_GAP_C", d.stale_gap_c),
        }
    }
}

impl Strategy for ThresholdBid {
    fn name(&self) -> &'static str {
        "toy-threshold-bid"
    }

    fn evaluate(
        &mut self,
        ctx: &EngineContext,
        markets: &[MarketView],
        resting: &[RestingOrder],
    ) -> Vec<Action> {
        let mut out = Vec::new();

        for r in resting {
            let Some(m) = markets.iter().find(|m| m.ticker == r.ticker) else {
                continue;
            };
            if let Some(bid) = m.bid_c {
                if (bid - r.limit_price_c).abs() > self.stale_gap_c {
                    out.push(Action::Cancel {
                        order_id: r.order_id.clone(),
                        reason: format!("bid {bid:.0}c drifted from {:.0}c", r.limit_price_c),
                    });
                }
            }
        }

        for m in markets.iter().filter(|m| m.is_active()) {
            let Some(bid) = m.bid_c else { continue };
            if bid < 1.0 || bid > self.max_bid_c {
                continue;
            }
            if ctx.held.contains(&m.ticker) || resting.iter().any(|r| r.ticker == m.ticker) {
                continue;
            }
            // Never cross: stay at least 1c under the ask when one is quoted.
            let limit = match m.ask_c {
                Some(ask) if ask - 1.0 < bid => ask - 1.0,
                _ => bid,
            };
            if limit < 1.0 {
                continue;
            }
            // Respect the engine's context: skip markets about to close, and orders the
            // book could not fund or the per-order cap would reject anyway.
            let cost = self.qty as f64 * limit / 100.0;
            if (m.close_ts > 0 && m.close_ts <= ctx.now_ts)
                || cost > ctx.available_usd
                || cost > ctx.max_order_usd
            {
                continue;
            }
            out.push(Action::Place(OrderIntent {
                ticker: m.ticker.clone(),
                series: m.series.clone(),
                slot: 1,
                qty: self.qty,
                limit_price_c: limit,
                close_ts: m.close_ts,
            }));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mv(t: &str, bid: Option<f64>, ask: Option<f64>) -> MarketView {
        MarketView {
            ticker: t.into(),
            series: "SERIES-A".into(),
            status: "active".into(),
            bid_c: bid,
            ask_c: ask,
            close_ts: 0,
        }
    }

    fn ctx() -> EngineContext {
        EngineContext {
            max_order_usd: 5.0,
            available_usd: 100.0,
            ..Default::default()
        }
    }

    #[test]
    fn places_a_bid_joined_to_a_cheap_market() {
        let mut s = ThresholdBid::default();
        let acts = s.evaluate(&ctx(), &[mv("EXAMPLE-1", Some(20.0), Some(22.0))], &[]);
        assert_eq!(acts.len(), 1);
        match &acts[0] {
            Action::Place(i) => {
                assert_eq!(i.ticker, "EXAMPLE-1");
                assert_eq!(i.limit_price_c, 20.0);
                assert_eq!(i.slot, 1);
            }
            other => panic!("expected a placement, got {other:?}"),
        }
    }

    #[test]
    fn ignores_markets_above_the_threshold_or_inactive() {
        let mut s = ThresholdBid::default();
        let mut closed = mv("EXAMPLE-2", Some(10.0), Some(12.0));
        closed.status = "closed".into();
        let acts = s.evaluate(
            &ctx(),
            &[mv("EXAMPLE-1", Some(80.0), Some(82.0)), closed],
            &[],
        );
        assert!(acts.is_empty());
    }

    #[test]
    fn never_crosses_the_ask() {
        let mut s = ThresholdBid::default();
        let acts = s.evaluate(&ctx(), &[mv("EXAMPLE-1", Some(20.0), Some(20.0))], &[]);
        match &acts[0] {
            Action::Place(i) => assert_eq!(i.limit_price_c, 19.0),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn does_not_stack_on_resting_or_held_markets() {
        let mut s = ThresholdBid::default();
        let resting = vec![RestingOrder {
            order_id: "o1".into(),
            ticker: "EXAMPLE-1".into(),
            slot: 1,
            limit_price_c: 20.0,
            qty: 1,
        }];
        let mut c = ctx();
        c.held.insert("EXAMPLE-2".into());
        let acts = s.evaluate(
            &c,
            &[
                mv("EXAMPLE-1", Some(20.0), Some(22.0)),
                mv("EXAMPLE-2", Some(20.0), Some(22.0)),
            ],
            &resting,
        );
        assert!(acts.is_empty(), "{acts:?}");
    }

    #[test]
    fn cancels_a_stale_order_once_the_bid_drifts() {
        let mut s = ThresholdBid::default();
        let resting = vec![RestingOrder {
            order_id: "o1".into(),
            ticker: "EXAMPLE-1".into(),
            slot: 1,
            limit_price_c: 20.0,
            qty: 1,
        }];
        let acts = s.evaluate(&ctx(), &[mv("EXAMPLE-1", Some(45.0), Some(47.0))], &resting);
        assert_eq!(
            acts,
            vec![Action::Cancel {
                order_id: "o1".into(),
                reason: "bid 45c drifted from 20c".into(),
            }]
        );
    }

    #[test]
    fn respects_funds_cap_and_close_time() {
        let mut s = ThresholdBid {
            qty: 10,
            ..Default::default()
        };
        let m = [mv("EXAMPLE-1", Some(30.0), Some(32.0))];
        let mut c = ctx();
        c.max_order_usd = 2.0; // 10 x 30c = $3 > cap
        assert!(s.evaluate(&c, &m, &[]).is_empty());
        let mut c = ctx();
        c.available_usd = 1.0;
        assert!(s.evaluate(&c, &m, &[]).is_empty());
        let mut s = ThresholdBid::default();
        let mut closing = mv("EXAMPLE-1", Some(20.0), Some(22.0));
        closing.close_ts = 100;
        let mut c = ctx();
        c.now_ts = 200;
        assert!(s.evaluate(&c, &[closing], &[]).is_empty());
    }

    #[test]
    fn is_deterministic_for_the_same_inputs() {
        let ms = [
            mv("EXAMPLE-1", Some(20.0), Some(25.0)),
            mv("EXAMPLE-2", Some(5.0), None),
        ];
        let a = ThresholdBid::default().evaluate(&ctx(), &ms, &[]);
        let b = ThresholdBid::default().evaluate(&ctx(), &ms, &[]);
        assert_eq!(a, b);
        assert_eq!(a.len(), 2);
    }
}
