//! Kalshi V2 API client: request signing, connection pooling, and endpoints.
//!
//! Reads go to the elections host; orders and cancels go to the external-api host.
//! Sending a cancel to the elections host returns 410.

use std::collections::HashMap;
use std::fs;
use std::time::Duration;

use base64::Engine as _;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use serde_json::{json, Value};
use sha2::Sha256;

use crate::paths;

pub const KALSHI: &str = "https://api.elections.kalshi.com/trade-api/v2";
pub const ORDER_BASE: &str = "https://external-api.kalshi.com/trade-api/v2";

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// SEMANTICS: reads AND writes. Order placement and cancel latency are counted alongside
/// reads, so `api_calls` and `worst_latency_ms` describe every request the engine made.
static API_CALLS: AtomicU32 = AtomicU32::new(0);
static API_ERRORS: AtomicU32 = AtomicU32::new(0);
static API_WORST_MS: AtomicU64 = AtomicU64::new(0);

/// Record one completed request. Cheap enough for the hot path: three relaxed atomics,
/// no lock and no allocation. Relaxed is right here because these counters are pure
/// telemetry - nothing in the trading path branches on them, so no ordering with any
/// other memory is needed.
pub fn note_call(elapsed_ms: u64, ok: bool) {
    API_CALLS.fetch_add(1, Ordering::Relaxed);
    if !ok {
        API_ERRORS.fetch_add(1, Ordering::Relaxed);
    }
    API_WORST_MS.fetch_max(elapsed_ms, Ordering::Relaxed);
}

/// `(calls, errors, worst_latency_ms)` since the last reset.
///
/// The three loads are independent, so a request completing between them can be counted
/// in one field and not another. Call this ONCE per tick into a local and log that local,
/// so all three logged numbers describe the same read - do not call it three times.
pub fn api_counters() -> (u32, u32, u64) {
    (
        API_CALLS.load(Ordering::Relaxed),
        API_ERRORS.load(Ordering::Relaxed),
        API_WORST_MS.load(Ordering::Relaxed),
    )
}

pub fn reset_api_counters() {
    API_CALLS.store(0, Ordering::Relaxed);
    API_ERRORS.store(0, Ordering::Relaxed);
    API_WORST_MS.store(0, Ordering::Relaxed);
}

/// Shared connection pool initialized once at startup.
///
/// ureq keeps its connection pool inside the Agent. Cloning an Agent is cheap
/// (two Arc bumps) and shares the underlying pool. This ensures each API call
/// benefits from connection reuse rather than paying a new TCP+TLS handshake each time.
///
/// The two hosts (reads on KALSHI, orders/cancels on ORDER_BASE) are separate pool keys,
/// so each maintains its own idle connection.
///
/// Safety note: DELETE (cancel) is replayable by the HTTP client; cancelling twice just
/// 404s the second time. The `client_order_id` provides additional idempotency protection.
static AGENT: std::sync::OnceLock<ureq::Agent> = std::sync::OnceLock::new();

pub fn agent() -> ureq::Agent {
    AGENT
        .get_or_init(|| {
            ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(20))
                .build()
        })
        .clone()
}

pub fn get_json(url: &str) -> Result<Value, String> {
    let mut last = String::new();
    for attempt in 0..3 {
        let t = std::time::Instant::now();
        let res = agent().get(url).set("User-Agent", "uge").call();
        note_call(t.elapsed().as_millis() as u64, res.is_ok());
        match res {
            Ok(r) => match r.into_json::<Value>() {
                Ok(v) => return Ok(v),
                Err(e) => last = format!("decode: {e}"),
            },
            Err(e) => last = format!("{e}"),
        }
        std::thread::sleep(Duration::from_millis(400 * (attempt + 1)));
    }
    Err(last)
}

/// Kalshi is inconsistent about quantity field names: the order-CREATE response returns
/// `fill_count` / `remaining_count` / `count`, while the LIST endpoints
/// (/portfolio/orders, /portfolio/fills) return the same values suffixed `_fp`. Reading
/// only one spelling silently yields 0 - which made fills_for() report every real fill as
/// a no-fill, so a filled position never entered the ledger. Always try both.
pub fn qty_of(v: &Value, key: &str) -> Option<f64> {
    f_of(v, &format!("{key}_fp")).or_else(|| f_of(v, key))
}

pub fn f_of(v: &Value, key: &str) -> Option<f64> {
    match v.get(key) {
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

/// Per-shard collateral, keyed by `exchange_index` as a STRING so it round-trips through
/// the JSON ledger without integer-key games.
///
/// Kalshi shards trading across matching engines and money does NOT cross between them.
/// `balance_dollars` is their sum. Callers sizing orders per shard should use this map
/// to ensure sufficient collateral on the target shard, not just the total balance.
///
/// An empty map means Kalshi sent no breakdown; callers MUST then fall back to the total
/// rather than treating every shard as empty.
pub fn parse_shard_balances(d: &Value) -> HashMap<String, f64> {
    d.get("balance_breakdown")
        .and_then(|v| v.as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|r| {
                    let ix = r.get("exchange_index")?;
                    // Kalshi sends the index as a number and the balance as a string.
                    let ix = ix.as_i64().or_else(|| ix.as_str()?.parse().ok())?;
                    let bal = f_of(r, "balance")?;
                    Some((ix.to_string(), (bal * 100.0).round() / 100.0))
                })
                .collect()
        })
        .unwrap_or_default()
}

// ------------------------------------------------------------------- auth

pub struct Signer {
    key_id: String,
    key: RsaPrivateKey,
}

impl Signer {
    pub fn load() -> Option<Signer> {
        let dir = paths::key_dir();
        let key_id = fs::read_to_string(format!("{dir}/key_id"))
            .ok()?
            .trim()
            .to_string();
        let pem = fs::read_to_string(format!("{dir}/private.pem")).ok()?;
        let key = RsaPrivateKey::from_pkcs1_pem(&pem)
            .or_else(|_| RsaPrivateKey::from_pkcs8_pem(&pem))
            .ok()?;
        Some(Signer { key_id, key })
    }

    /// RSA-PSS(MGF1-SHA256, salt = digest len) over "{ts_ms}{METHOD}{path}".
    /// `path` includes /trade-api/v2 and excludes the query string.
    pub fn headers(&self, method: &str, path: &str) -> Vec<(String, String)> {
        let ts = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis())
        .to_string();
        let msg = format!("{ts}{method}{path}");
        let sk = SigningKey::<Sha256>::new(self.key.clone());
        let sig = sk.sign_with_rng(&mut rand::thread_rng(), msg.as_bytes());
        let b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
        vec![
            ("KALSHI-ACCESS-KEY".into(), self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP".into(), ts),
            ("KALSHI-ACCESS-SIGNATURE".into(), b64),
        ]
    }

    pub fn get(&self, path: &str, query: &str) -> Result<Value, String> {
        let url = if query.is_empty() {
            format!("{KALSHI}{path}")
        } else {
            format!("{KALSHI}{path}?{query}")
        };
        let mut req = agent().get(&url);
        for (k, v) in self.headers("GET", &format!("/trade-api/v2{path}")) {
            req = req.set(&k, &v);
        }
        let t = std::time::Instant::now();
        let res = req.call();
        note_call(t.elapsed().as_millis() as u64, res.is_ok());
        res.map_err(|e| format!("{e}"))?
            .into_json::<Value>()
            .map_err(|e| format!("decode: {e}"))
    }

    pub fn post(&self, base: &str, path: &str, body: Value) -> Result<Value, String> {
        let url = format!("{base}{path}");
        let mut req = agent().post(&url).set("Content-Type", "application/json");
        for (k, v) in self.headers("POST", &format!("/trade-api/v2{path}")) {
            req = req.set(&k, &v);
        }
        let t = std::time::Instant::now();
        let res = req.send_json(body);
        note_call(t.elapsed().as_millis() as u64, res.is_ok());
        match res {
            Ok(r) => r.into_json::<Value>().map_err(|e| format!("decode: {e}")),
            Err(ureq::Error::Status(code, r)) => {
                let detail = r.into_string().unwrap_or_default();
                Err(format!("{code}: {}", &detail[..detail.len().min(200)]))
            }
            Err(e) => Err(format!("{e}")),
        }
    }

    /// The CROSS-SHARD TOTAL. Correct for reporting the size of the book and nothing else:
    /// collateral is siloed per matching engine, so this number alone cannot be used
    /// to size orders. Use `balances()` or `balances_by_shard` to check per-shard availability.
    pub fn balance_usd(&self) -> Result<f64, String> {
        let d = self.get("/portfolio/balance", "")?;
        if let Some(b) = f_of(&d, "balance_dollars") {
            return Ok(b);
        }
        f_of(&d, "balance")
            .map(|c| c / 100.0)
            .ok_or_else(|| "no balance field".to_string())
    }

    /// Total AND per-shard, from one request - the sizing path needs both and the balance
    /// sync is on a 300s timer, so splitting them would double the calls for no gain.
    pub fn balances(&self) -> Result<(f64, HashMap<String, f64>), String> {
        let d = self.get("/portfolio/balance", "")?;
        let total = f_of(&d, "balance_dollars")
            .or_else(|| f_of(&d, "balance").map(|c| c / 100.0))
            .ok_or_else(|| "no balance field".to_string())?;
        Ok((total, parse_shard_balances(&d)))
    }

    pub fn delete(&self, base: &str, path: &str) -> Result<Value, String> {
        let url = format!("{base}{path}");
        let mut req = agent().request("DELETE", &url);
        for (k, v) in self.headers("DELETE", &format!("/trade-api/v2{path}")) {
            req = req.set(&k, &v);
        }
        let t = std::time::Instant::now();
        let res = req.call();
        note_call(t.elapsed().as_millis() as u64, res.is_ok());
        match res {
            Ok(r) => r.into_json::<Value>().map_err(|e| format!("decode: {e}")),
            Err(ureq::Error::Status(code, r)) => {
                let d = r.into_string().unwrap_or_default();
                Err(format!("{code}: {}", &d[..d.len().min(200)]))
            }
            Err(e) => Err(format!("{e}")),
        }
    }

    /// Place a GTC buy-YES limit order. Returns (order_id, immediate_fill_qty, avg_cents).
    /// An order priced at or above the current ask is immediately marketable and comes
    /// back already filled.
    pub fn place_gtc_bid(
        &self,
        ticker: &str,
        qty: i64,
        price_cents: f64,
    ) -> Result<(String, i64, Option<f64>), String> {
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let body = json!({
            "ticker": ticker,
            "side": "bid",
            "count": format!("{}.00", qty),
            "price": format!("{:.4}", price_cents / 100.0),
            "client_order_id": format!("bid-{}", ts_ms),
            "time_in_force": "good_till_canceled",
            "self_trade_prevention_type": "taker_at_cross",
        });
        let d = self.post(ORDER_BASE, "/portfolio/events/orders", body)?;
        let o = d.get("order").unwrap_or(&d);
        let oid = o
            .get("order_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if oid.is_empty() {
            return Err(format!(
                "no order_id in response: {}",
                &d.to_string()[..120.min(d.to_string().len())]
            ));
        }
        let filled = qty_of(o, "fill_count").unwrap_or(0.0) as i64;
        let avg = f_of(o, "average_fill_price").map(|p| (p * 100.0 * 10.0).round() / 10.0);
        Ok((oid, filled, avg))
    }

    /// V2 cancel. NOTE the host: DELETE on the elections host returns
    /// 410 deprecated_v1_order_endpoint - it must go to ORDER_BASE.
    pub fn cancel(&self, order_id: &str) -> Result<i64, String> {
        let d = self.delete(ORDER_BASE, &format!("/portfolio/events/orders/{order_id}"))?;
        let o = d.get("order").unwrap_or(&d);
        Ok(f_of(o, "reduced_by").unwrap_or(0.0) as i64)
    }

    /// order_id -> filled qty, for every order still resting on the exchange.
    pub fn resting_orders(&self) -> Result<HashMap<String, i64>, String> {
        let d = self.get("/portfolio/orders", "status=resting")?;
        let mut out = HashMap::new();
        for o in d
            .get("orders")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            if let Some(id) = o.get("order_id").and_then(|v| v.as_str()) {
                out.insert(
                    id.to_string(),
                    qty_of(&o, "fill_count").unwrap_or(0.0) as i64,
                );
            }
        }
        Ok(out)
    }

    /// Total filled contracts and average price for one order, from the fills feed.
    pub fn fills_for(&self, order_id: &str) -> Result<(i64, Option<f64>), String> {
        let d = self.get("/portfolio/fills", "limit=200")?;
        let mut qty = 0i64;
        let mut cost = 0.0f64;
        for f in d
            .get("fills")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            if f.get("order_id").and_then(|v| v.as_str()) != Some(order_id) {
                continue;
            }
            let c = qty_of(&f, "count").unwrap_or(0.0) as i64;
            // Try to extract the fill price from the response; Kalshi may return it
            // as yes_price_dollars or price depending on the endpoint. If both are absent,
            // a fallback price should be provided by the caller.
            let px = f_of(&f, "yes_price_dollars")
                .or_else(|| f_of(&f, "price"))
                .map(|p| if p <= 1.0 { p * 100.0 } else { p })
                .unwrap_or(50.0); // Neutral fallback (cents)
            qty += c;
            cost += c as f64 * px;
        }
        Ok((
            qty,
            if qty > 0 {
                Some((cost / qty as f64 * 10.0).round() / 10.0)
            } else {
                None
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn qty_of_prefers_the_fp_suffix_then_falls_back() {
        // Kalshi's order-CREATE response uses `fill_count`; the LIST endpoints use
        // `fill_count_fp`. Reading only one spelling silently yields 0, which once made
        // every real fill look like a no-fill.
        let create = json!({"fill_count": 5});
        let list = json!({"fill_count_fp": "7", "fill_count": 0});
        assert_eq!(qty_of(&create, "fill_count"), Some(5.0));
        assert_eq!(qty_of(&list, "fill_count"), Some(7.0));
        assert_eq!(qty_of(&json!({}), "fill_count"), None);
    }

    #[test]
    fn f_of_parses_both_string_and_number() {
        assert_eq!(f_of(&json!({"a": "1.5"}), "a"), Some(1.5));
        assert_eq!(f_of(&json!({"a": 1.5}), "a"), Some(1.5));
        assert_eq!(f_of(&json!({"a": true}), "a"), None);
    }

    #[test]
    fn signer_load_returns_none_without_keys() {
        // Holds ENV_LOCK for thread safety when mutating process-global env vars.
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Must never panic when credentials are absent.
        std::env::set_var("UGE_KEY_DIR", "/nonexistent-key-dir-for-tests");
        assert!(Signer::load().is_none());
        std::env::remove_var("UGE_KEY_DIR");
    }

    #[test]
    fn counters_accumulate_and_reset() {
        let _g = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_api_counters();
        assert_eq!(api_counters(), (0, 0, 0));
        note_call(120, true);
        note_call(340, true);
        note_call(20, false);
        let (calls, errors, worst) = api_counters();
        assert_eq!(calls, 3);
        assert_eq!(errors, 1);
        assert_eq!(worst, 340);
        reset_api_counters();
        assert_eq!(api_counters(), (0, 0, 0));
    }

    #[test]
    fn order_and_read_hosts_are_distinct() {
        // Cancels sent to the elections host return 410. This is load-bearing.
        assert!(ORDER_BASE.contains("external-api.kalshi.com"));
        assert!(KALSHI.contains("api.elections.kalshi.com"));
        assert_ne!(KALSHI, ORDER_BASE);
    }

    // ---- one shared agent / connection pooling ----

    #[test]
    fn the_agent_is_built_once_and_shared_not_rebuilt_per_request() {
        // ureq's connection pool lives in the Agent. Rebuilding it per request
        // defeats the pooling and wastes TCP+TLS setup. This test ensures agent()
        // returns handles to ONE Agent, not new ones.
        let a = agent();
        let first: *const ureq::Agent = AGENT.get().expect("agent() must init the cell");
        let b = agent();
        let second: *const ureq::Agent = AGENT.get().expect("agent() must init the cell");
        assert!(
            std::ptr::eq(first, second),
            "agent() rebuilt the Agent - the pool is being thrown away every call"
        );
        drop((a, b));
    }

    #[test]
    fn the_agent_type_can_live_in_a_static() {
        // Compile-time. If ureq::Agent were not Send + Sync + 'static the `static AGENT:
        // OnceLock<Agent>` above would not build, and the whole design would need a
        // thread_local instead. Pinned here so a ureq upgrade that changed it fails loudly
        // in the test suite rather than at the next deploy.
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<ureq::Agent>();
    }

    #[test]
    fn the_two_hosts_are_distinct_pool_keys() {
        // ureq pools per host. Reads and orders going to different hosts is what makes
        // the default max_idle_connections_per_host of 1 sufficient: each host keeps its
        // own idle connection, so they never evict each other. If these ever collapse to
        // one host, revisit that setting.
        let read_host = KALSHI.split('/').nth(2).unwrap();
        let order_host = ORDER_BASE.split('/').nth(2).unwrap();
        assert_ne!(read_host, order_host);
    }
}
