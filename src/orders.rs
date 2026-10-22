//! Order lifecycle: place, track, cancel, detect fills.
//!
//! Resting GTC bids are tracked in a map keyed by exchange order id. Each tick the engine
//! (1) reconciles that map against the exchange's resting list, charging the ledger for
//! anything that filled, (2) cancels orders sitting on finished markets, and (3) applies
//! the strategy's actions through [`apply_actions`], which enforces the engine's safety
//! contract (validation, idempotency, cooldowns, backoff) regardless of what the strategy
//! asked for.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::kalshi::{f_of, Signer};
use crate::ledger::{append_line, charge_fill, money, position_record, taker_fee_usd};
use crate::paths;
use crate::strategy::{Action, OrderIntent, RestingOrder};
use crate::ts_et;

/// The narrow slice of the exchange the order lifecycle needs. Kept small on purpose so a
/// test fake cannot drift far from the real client.
pub trait Exchange {
    /// order_id -> filled quantity, for every order still resting.
    fn resting_orders(&self) -> Result<HashMap<String, i64>, String>;
    /// (filled quantity, average fill price in cents) for one order.
    fn fills_for(&self, order_id: &str) -> Result<(i64, Option<f64>), String>;
    /// Returns the number of contracts pulled.
    fn cancel(&self, order_id: &str) -> Result<i64, String>;
    /// Returns (order_id, immediately filled qty, average fill cents).
    fn place_gtc_bid(
        &self,
        ticker: &str,
        qty: i64,
        price_cents: f64,
    ) -> Result<(String, i64, Option<f64>), String>;
}

impl Exchange for Signer {
    fn resting_orders(&self) -> Result<HashMap<String, i64>, String> {
        Signer::resting_orders(self)
    }
    fn fills_for(&self, order_id: &str) -> Result<(i64, Option<f64>), String> {
        Signer::fills_for(self, order_id)
    }
    fn cancel(&self, order_id: &str) -> Result<i64, String> {
        Signer::cancel(self, order_id)
    }
    fn place_gtc_bid(
        &self,
        ticker: &str,
        qty: i64,
        price_cents: f64,
    ) -> Result<(String, i64, Option<f64>), String> {
        Signer::place_gtc_bid(self, ticker, qty, price_cents)
    }
}

/// A resting order this engine is tracking.
#[derive(Clone, Debug)]
pub struct Resting {
    pub order_id: String,
    pub ticker: String,
    pub series: String,
    pub slot: u64,
    pub qty: i64,
    pub notional: f64,
    pub limit_price: f64,
    pub placed_et: String,
    /// Epoch seconds; 0 for orders adopted at startup, whose true time is unknowable.
    pub placed_ts: i64,
    pub close_ts: i64,
    /// Lowest bid observed while the order rested; 999.0 = never seen. Tick sampling
    /// understates the true low, so a "touch" derived from it is a lower bound.
    pub min_bid_seen: f64,
    /// Epoch seconds of the last failed cancel; 0 = never failed.
    pub cancel_failed_ts: i64,
    /// The order host answered a cancel with 404: no further cancel can succeed, so the
    /// order stays tracked for the resting-order sync to classify but is never re-cancelled.
    pub cancel_unreachable: bool,
}

impl Resting {
    pub fn view(&self) -> RestingOrder {
        RestingOrder {
            order_id: self.order_id.clone(),
            ticker: self.ticker.clone(),
            slot: self.slot,
            limit_price_c: self.limit_price,
            qty: self.qty,
        }
    }
}

pub fn resting_committed(resting: &HashMap<String, Resting>) -> f64 {
    resting.values().map(|r| r.notional).sum()
}

/// The resting price of an exchange order object, in cents, or 0.0 when absent. Reads the
/// fixed-point dollar string and falls back to the legacy integer-cent field.
pub fn order_price_cents(o: &Value) -> f64 {
    f_of(o, "yes_price_dollars")
        .map(|d| (d * 1000.0).round() / 10.0)
        .or_else(|| f_of(o, "yes_price"))
        .filter(|c| c.is_finite() && *c > 0.0 && *c <= 100.0)
        .unwrap_or(0.0)
}

// ------------------------------------------------------------ error handling

/// Seconds a ticker is left alone after an order failure that cannot fix itself.
pub const HARD_FAIL_COOLDOWN_S: u64 = 900;

/// Does this order failure mean "stop asking", or "ask again later"? Unknown-account and
/// not-found failures latch the ticker; `insufficient_balance` is expected traffic that
/// self-resolves as capital frees, so it is deliberately not hard.
pub fn is_hard_order_failure(e: &str) -> bool {
    if e.contains("insufficient_balance") {
        return false;
    }
    e.contains("user_not_found") || e.starts_with("404")
}

pub fn is_rate_limited(e: &str) -> bool {
    e.contains("too_many_requests") || e.starts_with("429")
}

pub fn is_market_closed(e: &str) -> bool {
    e.contains("market_closed")
}

pub fn is_insufficient_balance(e: &str) -> bool {
    e.contains("insufficient_balance")
}

pub fn is_latched(hard_fail: &HashMap<String, Instant>, ticker: &str) -> bool {
    hard_fail
        .get(ticker)
        .map(|t| t.elapsed().as_secs() < HARD_FAIL_COOLDOWN_S)
        .unwrap_or(false)
}

const DEFER_BASE_S: u64 = 5;
const DEFER_MAX_S: u64 = 300;
const THROTTLE_BASE_S: u64 = 2;
const THROTTLE_MAX_S: u64 = 30;

struct Deferral {
    attempts: u32,
    next_attempt: Instant,
    /// `available` at the moment of the refusal; capital rising past it by the order's own
    /// notional is the signal that something settled and a retry is worthwhile.
    free_at_refusal: f64,
}

/// Placement backoff. A refused order is not re-sent every tick: it is deferred with
/// exponential delay, released early if capital frees up. A 429 pauses placement
/// engine-wide, since it is about request rate rather than any one order. This is a
/// retry limiter, not a capital check; the first attempt is never delayed.
#[derive(Default)]
pub struct Backoff {
    slots: HashMap<(String, u64), Deferral>,
    throttled_until: Option<Instant>,
    throttle_s: u64,
}

impl Backoff {
    pub fn paused(&self) -> bool {
        self.throttled_until
            .map(|t| Instant::now() < t)
            .unwrap_or(false)
    }

    pub fn may_attempt(&self, ticker: &str, slot: u64, avail: f64, notional: f64) -> bool {
        match self.slots.get(&(ticker.to_string(), slot)) {
            None => true,
            Some(d) => avail > d.free_at_refusal + notional || Instant::now() >= d.next_attempt,
        }
    }

    /// Returns true on the FIRST refusal of this order, the only one worth a log line.
    pub fn note_refusal(&mut self, ticker: &str, slot: u64, avail: f64) -> bool {
        let e = self
            .slots
            .entry((ticker.to_string(), slot))
            .or_insert(Deferral {
                attempts: 0,
                next_attempt: Instant::now(),
                free_at_refusal: avail,
            });
        e.attempts += 1;
        e.free_at_refusal = avail;
        let wait = DEFER_MAX_S.min(DEFER_BASE_S << (e.attempts - 1).min(6));
        e.next_attempt = Instant::now() + Duration::from_secs(wait);
        e.attempts == 1
    }

    /// A closed market never takes an order again: park the slot for a day.
    pub fn note_closed(&mut self, ticker: &str, slot: u64) {
        self.slots.insert(
            (ticker.to_string(), slot),
            Deferral {
                attempts: 1,
                next_attempt: Instant::now() + Duration::from_secs(86_400),
                free_at_refusal: f64::INFINITY,
            },
        );
    }

    pub fn note_rate_limit(&mut self) {
        self.throttle_s = if self.throttle_s == 0 {
            THROTTLE_BASE_S
        } else {
            THROTTLE_MAX_S.min(self.throttle_s * 2)
        };
        self.throttled_until = Some(Instant::now() + Duration::from_secs(self.throttle_s));
    }

    pub fn note_success(&mut self, ticker: &str, slot: u64) {
        self.slots.remove(&(ticker.to_string(), slot));
        self.throttled_until = None;
        self.throttle_s = 0;
    }

    /// Forget deferrals for markets no longer being tracked, bounding memory.
    pub fn retain_tickers(&mut self, keep: &HashSet<String>) {
        self.slots.retain(|(t, _), _| keep.contains(t));
    }
}

// ------------------------------------------------------------- pure decisions

/// Seconds between cancel attempts on one order after a cancel has failed.
pub const CANCEL_RETRY_S: i64 = 60;

/// Order ids sitting on a finished market (not `active`, or past close time). Decided from
/// local state only, before the exchange sync, so a network failure cannot lose the
/// decision. A market merely missing from `live` is not assumed finished: only `close_ts`
/// may retire such an order. Sorted for reproducible logs.
pub fn orders_to_cancel(
    resting: &HashMap<String, Resting>,
    live: &HashMap<String, Value>,
    now: i64,
) -> Vec<String> {
    let mut out: Vec<String> = resting
        .iter()
        .filter(|(_, r)| {
            if r.cancel_unreachable
                || (r.cancel_failed_ts > 0 && now - r.cancel_failed_ts < CANCEL_RETRY_S)
            {
                return false;
            }
            let done = live
                .get(&r.ticker)
                .map(|m| m.get("status").and_then(|v| v.as_str()).unwrap_or("") != "active")
                .unwrap_or(false);
            done || (r.close_ts > 0 && now > r.close_ts)
        })
        .map(|(oid, _)| oid.clone())
        .collect();
    out.sort();
    out
}

/// Tracked orders no longer on the exchange's resting list. Absence is ambiguous: the
/// order filled or was cancelled/expired, with opposite accounting. The caller must ask
/// `fills_for` which happened.
pub fn orders_gone(
    resting: &HashMap<String, Resting>,
    on_book: &HashMap<String, i64>,
) -> Vec<String> {
    let mut out: Vec<String> = resting
        .iter()
        .filter(|(_, r)| !on_book.contains_key(&r.order_id))
        .map(|(oid, _)| oid.clone())
        .collect();
    out.sort();
    out
}

/// One line per resting order when it leaves the book, however it leaves.
fn outcome_row(r: &Resting, outcome: &str, filled_qty: i64, fill_c: Option<f64>) -> Value {
    let now = chrono::Utc::now().timestamp();
    let seen = r.min_bid_seen < 999.0;
    json!({
        "ts_et": ts_et(), "ticker": r.ticker, "series": r.series, "slot": r.slot,
        "limit_price": r.limit_price, "qty_requested": r.qty,
        "outcome": outcome, "filled_qty": filled_qty,
        "fill_price_cents": fill_c.map(Value::from).unwrap_or(Value::Null),
        "placed_et": r.placed_et,
        "secs_resting": if r.placed_ts > 0 { json!(now - r.placed_ts) } else { Value::Null },
        "min_bid_seen": if seen { json!(r.min_bid_seen) } else { Value::Null },
        "touched": if seen { json!(r.min_bid_seen <= r.limit_price) } else { Value::Null },
        "close_ts": r.close_ts,
    })
}

fn log_outcome(r: &Resting, outcome: &str, filled_qty: i64, fill_c: Option<f64>) {
    append_line(
        &paths::in_root("state/order-outcomes.jsonl"),
        &outcome_row(r, outcome, filled_qty, fill_c),
    );
}

// ------------------------------------------------------------------ lifecycle

/// Cancel one tracked order. On failure the order stays tracked (dropping it would strand
/// a live order with nothing watching it) and retries are paced; a 404 ends retrying.
fn cancel_one<E: Exchange>(
    sg: &E,
    resting: &mut HashMap<String, Resting>,
    oid: &str,
    why: &str,
    now: i64,
) -> Option<String> {
    let r = resting.get(oid)?;
    let (ticker, slot, qty) = (r.ticker.clone(), r.slot, r.qty);
    match sg.cancel(oid) {
        Ok(n) => {
            // `n` is the exchange's own count of contracts pulled. All of them means
            // nothing filled; fewer means some filled first, which is not a clean miss.
            // 0 is ambiguous (the response may omit the field), so it is named as such.
            let outcome = if n <= 0 {
                "cancelled_unverified"
            } else if qty - n > 0 {
                "cancelled_partial"
            } else {
                "cancelled_no_fill"
            };
            log_outcome(r, outcome, (qty - n).max(0), None);
            resting.remove(oid);
            Some(format!("CANCEL {ticker} slot={slot}: {why}, {n} pulled"))
        }
        Err(e) => {
            let unreachable = e.contains("404");
            if let Some(r) = resting.get_mut(oid) {
                r.cancel_failed_ts = now;
                r.cancel_unreachable = unreachable;
            }
            Some(if unreachable {
                format!(
                    "CANCEL FAILED {ticker} slot={slot}: {e} - not on the order host, \
                         leaving it to the resting-order sync"
                )
            } else {
                format!("CANCEL FAILED {ticker} slot={slot}: {e} - retry in {CANCEL_RETRY_S}s")
            })
        }
    }
}

/// Resting-order lifecycle: detect fills, cancel at close, free capital. Runs BEFORE
/// strategy evaluation each tick so freed capital is usable the same tick. Returns log
/// lines and the fills `(ticker, qty, price_c)` so the strategy can be notified.
pub fn reconcile_resting<E: Exchange>(
    bk: &mut Value,
    resting: &mut HashMap<String, Resting>,
    live: &HashMap<String, Value>,
    signer: &Option<E>,
) -> (Vec<String>, Vec<(String, i64, f64)>) {
    let mut logs = Vec::new();
    let mut fills = Vec::new();
    let Some(sg) = signer else {
        return (logs, fills);
    };
    if resting.is_empty() {
        return (logs, fills);
    }
    // Track the lowest live bid seen while each order rests. A bid of 0 under an empty
    // book (settlement) is the book vanishing, not a price.
    for r in resting.values_mut() {
        if let Some(m) = live.get(&r.ticker) {
            let bid = f_of(m, "yes_bid_dollars").unwrap_or(0.0) * 100.0;
            if bid > 0.0 && bid < r.min_bid_seen {
                r.min_bid_seen = bid;
            }
        }
    }
    let now = chrono::Utc::now().timestamp();
    let to_cancel = orders_to_cancel(resting, live, now);

    let on_book = match sg.resting_orders() {
        Ok(v) => v,
        Err(e) => {
            logs.push(format!("resting-order sync FAILED: {e} - holding state"));
            return (logs, fills);
        }
    };

    for oid in orders_gone(resting, &on_book) {
        let Some(r) = resting.remove(&oid) else {
            continue;
        };
        let (filled, avg) = sg.fills_for(&r.order_id).unwrap_or((0, None));
        if filled < 1 {
            log_outcome(&r, "closed_no_fill", 0, None);
            logs.push(format!(
                "REST GONE {} slot={}: no fill, order closed",
                r.ticker, r.slot
            ));
            continue;
        }
        // The average price is exchange-controlled: only a finite price in (0, 100] is a
        // real contract price, anything else falls back to the limit we posted.
        let fill_c = avg
            .filter(|a| a.is_finite() && *a > 0.0 && *a <= 100.0)
            .unwrap_or(r.limit_price);
        let pos = position_record(
            &r.ticker,
            &r.series,
            &r.order_id,
            r.slot,
            r.limit_price,
            r.close_ts,
            filled,
            fill_c,
        );
        let risk = pos["risk_usd"].as_f64().unwrap_or(0.0);
        charge_fill(bk, risk);
        if let Some(a) = bk["positions_open"].as_array_mut() {
            a.push(pos);
        }
        if let Some(a) = bk["markets_traded_today"].as_array_mut() {
            a.push(json!(r.ticker));
        }
        log_outcome(&r, "filled", filled, Some(fill_c));
        logs.push(format!(
            "FILLED {} slot={}: {filled} @ {fill_c:.1}c (risk ${risk:.2})",
            r.ticker, r.slot
        ));
        fills.push((r.ticker.clone(), filled, fill_c));
    }

    for oid in to_cancel {
        if let Some(l) = cancel_one(sg, resting, &oid, "market finished", now) {
            logs.push(l);
        }
    }
    (logs, fills)
}

/// What the engine needs to apply strategy actions.
pub struct ApplyCtx<'a, E: Exchange> {
    pub signer: &'a Option<E>,
    pub dry: bool,
    pub available: f64,
    pub max_order_usd: f64,
}

/// Apply a strategy's actions under the engine's safety contract. Returns log lines.
pub fn apply_actions<E: Exchange>(
    actions: Vec<Action>,
    cx: &ApplyCtx<E>,
    resting: &mut HashMap<String, Resting>,
    hard_fail: &mut HashMap<String, Instant>,
    backoff: &mut Backoff,
) -> Vec<String> {
    let mut logs = Vec::new();
    let now = chrono::Utc::now().timestamp();
    for a in actions {
        match a {
            Action::Cancel { order_id, reason } => match cx.signer {
                Some(sg) if !cx.dry => {
                    let paced = resting.get(&order_id).map(|r| {
                        r.cancel_unreachable
                            || (r.cancel_failed_ts > 0 && now - r.cancel_failed_ts < CANCEL_RETRY_S)
                    });
                    if paced == Some(false) {
                        logs.extend(cancel_one(sg, resting, &order_id, &reason, now));
                    }
                }
                _ => logs.push(format!("DRY cancel {order_id}: {reason}")),
            },
            Action::Place(i) => logs.extend(place_one(i, cx, resting, hard_fail, backoff, now)),
        }
    }
    logs
}

fn place_one<E: Exchange>(
    i: OrderIntent,
    cx: &ApplyCtx<E>,
    resting: &mut HashMap<String, Resting>,
    hard_fail: &mut HashMap<String, Instant>,
    backoff: &mut Backoff,
    now: i64,
) -> Option<String> {
    if i.qty < 1 || !(i.limit_price_c > 0.0 && i.limit_price_c < 100.0) {
        return Some(format!(
            "REJECT {} slot={}: bad qty/price ({} @ {}c)",
            i.ticker, i.slot, i.qty, i.limit_price_c
        ));
    }
    let notional =
        money(i.qty as f64 * i.limit_price_c / 100.0 + taker_fee_usd(i.limit_price_c, i.qty));
    if notional > cx.max_order_usd {
        return Some(format!(
            "REJECT {} slot={}: notional ${notional:.2} over cap ${:.2}",
            i.ticker, i.slot, cx.max_order_usd
        ));
    }
    // Idempotency: one resting order per (ticker, slot).
    if resting
        .values()
        .any(|r| r.ticker == i.ticker && r.slot == i.slot)
    {
        return None;
    }
    if is_latched(hard_fail, &i.ticker)
        || backoff.paused()
        || !backoff.may_attempt(&i.ticker, i.slot, cx.available, notional)
    {
        return None;
    }
    let sg = match cx.signer {
        Some(sg) if !cx.dry => sg,
        _ => {
            return Some(format!(
                "DRY place {} slot={}: {} @ {:.0}c",
                i.ticker, i.slot, i.qty, i.limit_price_c
            ));
        }
    };
    match sg.place_gtc_bid(&i.ticker, i.qty, i.limit_price_c) {
        Ok((oid, _immediate, _avg)) => {
            // An immediately marketable order comes back filled; the next reconcile finds
            // it missing from the resting list and charges it via the fills feed.
            backoff.note_success(&i.ticker, i.slot);
            let msg = format!(
                "REST {} slot={}: {} @ {:.0}c (order {})",
                i.ticker,
                i.slot,
                i.qty,
                i.limit_price_c,
                &oid[..8.min(oid.len())]
            );
            resting.insert(
                oid.clone(),
                Resting {
                    order_id: oid,
                    ticker: i.ticker,
                    series: i.series,
                    slot: i.slot,
                    qty: i.qty,
                    notional,
                    limit_price: i.limit_price_c,
                    placed_et: ts_et(),
                    placed_ts: now,
                    close_ts: i.close_ts,
                    min_bid_seen: 999.0,
                    cancel_failed_ts: 0,
                    cancel_unreachable: false,
                },
            );
            Some(msg)
        }
        Err(e) => {
            if is_rate_limited(&e) {
                backoff.note_rate_limit();
                Some(format!("THROTTLED placing {}: {e}", i.ticker))
            } else if is_market_closed(&e) {
                backoff.note_closed(&i.ticker, i.slot);
                None
            } else if is_insufficient_balance(&e) {
                backoff
                    .note_refusal(&i.ticker, i.slot, cx.available)
                    .then(|| {
                        format!(
                            "REFUSED {} slot={}: insufficient balance - deferring",
                            i.ticker, i.slot
                        )
                    })
            } else if is_hard_order_failure(&e) {
                hard_fail.insert(i.ticker.clone(), Instant::now());
                Some(format!(
                    "HARD FAIL {}: {e} - cooling down {HARD_FAIL_COOLDOWN_S}s",
                    i.ticker
                ))
            } else {
                backoff
                    .note_refusal(&i.ticker, i.slot, cx.available)
                    .then(|| format!("PLACE FAILED {} slot={}: {e}", i.ticker, i.slot))
            }
        }
    }
}

/// Convert the exchange's resting-order list into tracked orders at startup, so an order
/// left on the book across a restart is still cancelled at close. Sells are not ours to
/// adopt. Slot 0 and placement time 0 mean "adopted: slot and time unknowable".
pub fn adopt_candidates(orders: &[Value]) -> Vec<Resting> {
    let mut out = Vec::new();
    for o in orders {
        let (Some(oid), Some(tk)) = (
            o.get("order_id").and_then(|v| v.as_str()),
            o.get("ticker").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let is_sell = o.get("action").and_then(|v| v.as_str()) == Some("sell")
            || o.get("book_side").and_then(|v| v.as_str()) == Some("ask");
        let qty = crate::kalshi::qty_of(o, "remaining_count").unwrap_or(0.0) as i64;
        if is_sell || qty < 1 {
            continue;
        }
        let price = order_price_cents(o);
        out.push(Resting {
            order_id: oid.to_string(),
            ticker: tk.to_string(),
            series: tk.split('-').next().unwrap_or("").to_string(),
            slot: 0,
            qty,
            notional: money(qty as f64 * price / 100.0 + taker_fee_usd(price, qty)),
            limit_price: price,
            placed_et: ts_et(),
            placed_ts: 0,
            close_ts: 0,
            min_bid_seen: 999.0,
            cancel_failed_ts: 0,
            cancel_unreachable: false,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::new_bankroll;
    use std::cell::RefCell;

    /// In-memory exchange: records calls, answers from canned state.
    #[derive(Default)]
    struct Fake {
        on_book: RefCell<HashMap<String, i64>>,
        fills: RefCell<HashMap<String, (i64, Option<f64>)>>,
        place_err: RefCell<Option<String>>,
        placed: RefCell<Vec<(String, i64, f64)>>,
        cancel_err: RefCell<Option<String>>,
    }

    impl Exchange for Fake {
        fn resting_orders(&self) -> Result<HashMap<String, i64>, String> {
            Ok(self.on_book.borrow().clone())
        }
        fn fills_for(&self, id: &str) -> Result<(i64, Option<f64>), String> {
            Ok(self.fills.borrow().get(id).cloned().unwrap_or((0, None)))
        }
        fn cancel(&self, _id: &str) -> Result<i64, String> {
            if let Some(e) = self.cancel_err.borrow().clone() {
                return Err(e);
            }
            Ok(1)
        }
        fn place_gtc_bid(
            &self,
            t: &str,
            q: i64,
            p: f64,
        ) -> Result<(String, i64, Option<f64>), String> {
            if let Some(e) = self.place_err.borrow().clone() {
                return Err(e);
            }
            let n = self.placed.borrow().len();
            self.placed.borrow_mut().push((t.to_string(), q, p));
            let id = format!("order-{n:04}");
            self.on_book.borrow_mut().insert(id.clone(), 0);
            Ok((id, 0, None))
        }
    }

    fn intent(t: &str, slot: u64, qty: i64, px: f64) -> Action {
        Action::Place(OrderIntent {
            ticker: t.into(),
            series: "SERIES-A".into(),
            slot,
            qty,
            limit_price_c: px,
            close_ts: 0,
        })
    }

    fn cx(sg: &Option<Fake>, dry: bool) -> ApplyCtx<'_, Fake> {
        ApplyCtx {
            signer: sg,
            dry,
            available: 100.0,
            max_order_usd: 5.0,
        }
    }

    fn with_root<T>(f: impl FnOnce() -> T) -> T {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("UGE_ROOT", dir.path());
        let out = f();
        std::env::remove_var("UGE_ROOT");
        out
    }

    type State = (HashMap<String, Resting>, HashMap<String, Instant>, Backoff);

    fn state() -> State {
        (HashMap::new(), HashMap::new(), Backoff::default())
    }

    #[test]
    fn placement_tracks_the_order_and_is_idempotent_per_slot() {
        with_root(|| {
            let sg = Some(Fake::default());
            let (mut resting, mut hf, mut bo) = state();
            let logs = apply_actions(
                vec![intent("EXAMPLE-1", 1, 2, 20.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert!(logs[0].starts_with("REST"));
            assert_eq!(resting.len(), 1);
            // Re-emitting the same intent next tick must not place a second order.
            apply_actions(
                vec![intent("EXAMPLE-1", 1, 2, 20.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert_eq!(sg.as_ref().unwrap().placed.borrow().len(), 1);
            // A different slot on the same market is a distinct order.
            apply_actions(
                vec![intent("EXAMPLE-1", 2, 2, 18.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert_eq!(resting.len(), 2);
        });
    }

    #[test]
    fn invalid_or_oversized_intents_never_reach_the_exchange() {
        with_root(|| {
            let sg = Some(Fake::default());
            let (mut resting, mut hf, mut bo) = state();
            let logs = apply_actions(
                vec![
                    intent("EXAMPLE-1", 1, 0, 20.0),
                    intent("EXAMPLE-1", 1, 1, 100.0),
                    intent("EXAMPLE-1", 1, 1000, 50.0),
                ],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert!(logs.iter().all(|l| l.starts_with("REJECT")), "{logs:?}");
            assert!(sg.unwrap().placed.borrow().is_empty());
        });
    }

    #[test]
    fn dry_run_places_nothing() {
        with_root(|| {
            let sg = Some(Fake::default());
            let (mut resting, mut hf, mut bo) = state();
            let logs = apply_actions(
                vec![intent("EXAMPLE-1", 1, 1, 20.0)],
                &cx(&sg, true),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert!(logs[0].starts_with("DRY"));
            assert!(resting.is_empty());
            assert!(sg.unwrap().placed.borrow().is_empty());
        });
    }

    #[test]
    fn a_hard_failure_latches_the_ticker_and_a_refusal_is_logged_once() {
        with_root(|| {
            let sg = Some(Fake::default());
            let (mut resting, mut hf, mut bo) = state();
            *sg.as_ref().unwrap().place_err.borrow_mut() = Some("404: user_not_found".into());
            let l1 = apply_actions(
                vec![intent("EXAMPLE-1", 1, 1, 20.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert!(l1[0].starts_with("HARD FAIL"));
            assert!(is_latched(&hf, "EXAMPLE-1"));
            let l2 = apply_actions(
                vec![intent("EXAMPLE-1", 1, 1, 20.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert!(l2.is_empty(), "latched ticker is left alone");

            *sg.as_ref().unwrap().place_err.borrow_mut() = Some("400: insufficient_balance".into());
            let first = apply_actions(
                vec![intent("EXAMPLE-2", 1, 1, 20.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            let again = apply_actions(
                vec![intent("EXAMPLE-2", 1, 1, 20.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert_eq!(first.len(), 1);
            assert!(again.is_empty(), "deferred, not re-sent every tick");
        });
    }

    #[test]
    fn a_rate_limit_pauses_all_placement() {
        with_root(|| {
            let sg = Some(Fake::default());
            let (mut resting, mut hf, mut bo) = state();
            *sg.as_ref().unwrap().place_err.borrow_mut() = Some("429: too_many_requests".into());
            apply_actions(
                vec![intent("EXAMPLE-1", 1, 1, 20.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert!(bo.paused());
            *sg.as_ref().unwrap().place_err.borrow_mut() = None;
            apply_actions(
                vec![intent("EXAMPLE-2", 1, 1, 20.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            assert!(sg.unwrap().placed.borrow().is_empty());
        });
    }

    #[test]
    fn a_fill_is_detected_charged_and_opens_a_position() {
        with_root(|| {
            let sg = Some(Fake::default());
            let (mut resting, mut hf, mut bo) = state();
            apply_actions(
                vec![intent("EXAMPLE-1", 1, 4, 25.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            let oid = resting.keys().next().unwrap().clone();
            // The order leaves the book and the fills feed says it filled.
            sg.as_ref().unwrap().on_book.borrow_mut().remove(&oid);
            sg.as_ref()
                .unwrap()
                .fills
                .borrow_mut()
                .insert(oid, (4, Some(25.0)));
            let mut bk = new_bankroll(100.0);
            let (logs, fills) = reconcile_resting(&mut bk, &mut resting, &HashMap::new(), &sg);
            assert!(logs[0].starts_with("FILLED"));
            assert_eq!(fills, vec![("EXAMPLE-1".to_string(), 4, 25.0)]);
            assert!(resting.is_empty());
            assert_eq!(bk["positions_open"].as_array().unwrap().len(), 1);
            let in_play = bk["ledger"]["in_play"].as_f64().unwrap();
            assert!(in_play > 1.0 && in_play < 1.1, "{in_play}");
        });
    }

    #[test]
    fn an_order_that_vanishes_without_fills_is_closed_with_no_charge() {
        with_root(|| {
            let sg = Some(Fake::default());
            let (mut resting, mut hf, mut bo) = state();
            apply_actions(
                vec![intent("EXAMPLE-1", 1, 4, 25.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            sg.as_ref().unwrap().on_book.borrow_mut().clear();
            let mut bk = new_bankroll(100.0);
            let (logs, fills) = reconcile_resting(&mut bk, &mut resting, &HashMap::new(), &sg);
            assert!(logs[0].starts_with("REST GONE"));
            assert!(fills.is_empty());
            assert_eq!(bk["ledger"]["available"], json!(100.0));
        });
    }

    #[test]
    fn orders_on_a_finished_market_are_cancelled_and_failures_are_paced() {
        with_root(|| {
            let sg = Some(Fake::default());
            let (mut resting, mut hf, mut bo) = state();
            apply_actions(
                vec![intent("EXAMPLE-1", 1, 1, 25.0)],
                &cx(&sg, false),
                &mut resting,
                &mut hf,
                &mut bo,
            );
            let live: HashMap<String, Value> =
                [("EXAMPLE-1".to_string(), json!({"status": "closed"}))].into();
            let mut bk = new_bankroll(100.0);

            *sg.as_ref().unwrap().cancel_err.borrow_mut() = Some("500: boom".into());
            let (logs, _) = reconcile_resting(&mut bk, &mut resting, &live, &sg);
            assert!(logs[0].starts_with("CANCEL FAILED"));
            assert_eq!(resting.len(), 1, "a failed cancel keeps the order tracked");
            let (again, _) = reconcile_resting(&mut bk, &mut resting, &live, &sg);
            assert!(again.is_empty(), "retry is paced, not per tick");

            resting.values_mut().for_each(|r| r.cancel_failed_ts = 0);
            *sg.as_ref().unwrap().cancel_err.borrow_mut() = None;
            let (logs, _) = reconcile_resting(&mut bk, &mut resting, &live, &sg);
            assert!(logs[0].starts_with("CANCEL EXAMPLE-1"), "{logs:?}");
            assert!(resting.is_empty());
        });
    }

    #[test]
    fn orders_to_cancel_ignores_missing_quotes_but_honours_close_time() {
        let mk = |close_ts| Resting {
            order_id: "o".into(),
            ticker: "EXAMPLE-1".into(),
            series: "S".into(),
            slot: 1,
            qty: 1,
            notional: 0.0,
            limit_price: 20.0,
            placed_et: String::new(),
            placed_ts: 0,
            close_ts,
            min_bid_seen: 999.0,
            cancel_failed_ts: 0,
            cancel_unreachable: false,
        };
        let live = HashMap::new();
        let m: HashMap<String, Resting> = [("o".to_string(), mk(0))].into();
        assert!(
            orders_to_cancel(&m, &live, 1_000).is_empty(),
            "a quote gap is not a settlement"
        );
        let m: HashMap<String, Resting> = [("o".to_string(), mk(500))].into();
        assert_eq!(orders_to_cancel(&m, &live, 1_000), vec!["o"]);
    }

    #[test]
    fn order_price_reads_dollar_strings_and_rejects_garbage() {
        assert_eq!(
            order_price_cents(&json!({"yes_price_dollars": "0.83"})),
            83.0
        );
        assert_eq!(order_price_cents(&json!({"yes_price": 41})), 41.0);
        assert_eq!(order_price_cents(&json!({"yes_price_dollars": "7.5"})), 0.0);
        assert_eq!(order_price_cents(&json!({})), 0.0);
    }

    #[test]
    fn adoption_takes_resting_buys_and_skips_sells() {
        let orders = vec![
            json!({"order_id": "a", "ticker": "EXAMPLE-1", "remaining_count": "3",
                   "yes_price_dollars": "0.20"}),
            json!({"order_id": "b", "ticker": "EXAMPLE-2", "remaining_count": "3",
                   "yes_price_dollars": "0.60", "action": "sell"}),
            json!({"order_id": "c", "ticker": "EXAMPLE-3", "remaining_count": "0"}),
        ];
        let got = adopt_candidates(&orders);
        assert_eq!(got.len(), 1);
        assert_eq!((got[0].qty, got[0].limit_price, got[0].slot), (3, 20.0, 0));
    }

    #[test]
    fn error_classifiers_draw_the_hard_boundary_correctly() {
        assert!(is_hard_order_failure("404: user_not_found"));
        assert!(!is_hard_order_failure("400: insufficient_balance"));
        assert!(!is_hard_order_failure("429: too_many_requests"));
        assert!(is_rate_limited("429: x"));
        assert!(is_market_closed("409: market_closed"));
    }
}
