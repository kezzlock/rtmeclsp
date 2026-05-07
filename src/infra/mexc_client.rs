use chrono::{TimeZone, Utc};

use crate::domain::{
    exchange::Exchange,
    order_book::{OrderBook, OrderBookLevel},
    symbol::Symbol,
};
use crate::infra::ws_feed::{WsAdapter, WsUpdate, run_ws_feed};
use crate::state::snapshot_store::SharedSnapshotStore;

const WS_BASE: &str = "wss://wbs.mexc.com/ws";

pub struct MexcAdapter {
    symbols: Vec<Symbol>,
    subscribe_json: String,
}

impl MexcAdapter {
    pub fn new(symbols: Vec<Symbol>) -> Self {
        let params: Vec<String> = symbols
            .iter()
            .map(|s| format!("spot@public.limit.depth.v3.api@{}@5", s.mexc_symbol()))
            .collect();

        let sub = serde_json::json!({
            "method": "SUBSCRIPTION",
            "params": params
        });

        Self {
            symbols,
            subscribe_json: sub.to_string(),
        }
    }
}

impl WsAdapter for MexcAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Mexc
    }

    fn ws_url(&self) -> String {
        WS_BASE.to_string()
    }

    fn subscribe_message(&self) -> Option<String> {
        Some(self.subscribe_json.clone())
    }

    fn parse_message(&self, text: &str) -> Result<Vec<WsUpdate>, String> {
        let v: serde_json::Value = serde_json::from_str(text).map_err(|e| format!("json: {e}"))?;

        let c = match v.get("c").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => return Ok(vec![]),
        };
        let d = match v.get("d") {
            Some(d) => d,
            None => return Ok(vec![]),
        };
        let t = v.get("t").and_then(|v| v.as_u64()).unwrap_or(0);

        let symbol_str = c.split('@').nth(2).unwrap_or_default();
        let symbol = self
            .symbols
            .iter()
            .find(|&&s| s.mexc_symbol() == symbol_str)
            .copied()
            .ok_or_else(|| format!("unknown mexc symbol: {symbol_str}"))?;

        let bids = parse_mexc_levels(d.get("bids"))?;
        let asks = parse_mexc_levels(d.get("asks"))?;

        let ts = Utc
            .timestamp_millis_opt(t as i64)
            .single()
            .unwrap_or_else(Utc::now);

        let ob = OrderBook {
            exchange: Exchange::Mexc,
            symbol,
            bids,
            asks,
            timestamp: ts,
        };

        Ok(vec![WsUpdate::OrderBook(ob)])
    }
}

fn parse_mexc_levels(v: Option<&serde_json::Value>) -> Result<Vec<OrderBookLevel>, String> {
    let arr = match v.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Ok(vec![]),
    };

    arr.iter()
        .map(|l| {
            let p = l.get("p").and_then(|v| v.as_str()).ok_or("missing p")?;
            let v = l.get("v").and_then(|v| v.as_str()).ok_or("missing v")?;
            Ok(OrderBookLevel {
                price: p.parse().map_err(|e| format!("price parse: {e}"))?,
                quantity: v.parse().map_err(|e| format!("qty parse: {e}"))?,
            })
        })
        .collect()
}

pub async fn run(
    store: SharedSnapshotStore,
    symbols: Vec<Symbol>,
    reconnect_backoff_ms: u64,
    reconnect_max_attempts: u32,
) {
    run_ws_feed(
        MexcAdapter::new(symbols),
        store,
        reconnect_backoff_ms,
        reconnect_max_attempts,
    )
    .await;
}
