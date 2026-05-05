use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{
    exchange::Exchange,
    order_book::{OrderBook, OrderBookLevel},
    symbol::Symbol,
};
use crate::infra::ws_feed::{WsAdapter, WsUpdate, run_ws_feed};
use crate::state::snapshot_store::SharedSnapshotStore;

const WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

#[derive(Debug, Serialize)]
struct SubscribeRequest {
    op: &'static str,
    args: Vec<SubscribeArg>,
}

#[derive(Debug, Serialize)]
struct SubscribeArg {
    channel: &'static str,
    #[serde(rename = "instId")]
    inst_id: &'static str,
}

#[derive(Debug, Deserialize)]
struct OkxMessage {
    arg: Option<OkxArg>,
    data: Option<Vec<OkxBookData>>,
}

#[derive(Debug, Deserialize)]
struct OkxArg {
    #[serde(rename = "instId")]
    inst_id: String,
}

#[derive(Debug, Deserialize)]
struct OkxBookData {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
    ts: String,
}

pub struct OkxAdapter {
    symbols: Vec<Symbol>,
    subscribe_json: String,
}

impl OkxAdapter {
    fn new(symbols: Vec<Symbol>) -> Self {
        let args: Vec<SubscribeArg> = symbols
            .iter()
            .map(|s| SubscribeArg {
                channel: "books5",
                inst_id: s.okx_symbol(),
            })
            .collect();
        let sub = SubscribeRequest {
            op: "subscribe",
            args,
        };
        let subscribe_json =
            serde_json::to_string(&sub).expect("subscribe serialization is infallible");
        Self {
            symbols,
            subscribe_json,
        }
    }
}

impl WsAdapter for OkxAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Okx
    }

    fn ws_url(&self) -> String {
        WS_URL.to_string()
    }

    fn subscribe_message(&self) -> Option<String> {
        Some(self.subscribe_json.clone())
    }

    fn parse_message(&self, text: &str) -> Result<Vec<WsUpdate>, String> {
        if text == "ping" || text == "pong" {
            return Ok(vec![]);
        }

        let msg: OkxMessage = serde_json::from_str(text).map_err(|e| format!("deserialize: {e}"))?;

        let inst_id = match &msg.arg {
            Some(a) => a.inst_id.as_str(),
            None => return Ok(vec![]),
        };

        let data = match &msg.data {
            Some(d) if !d.is_empty() => d,
            _ => return Ok(vec![]),
        };

        let symbol = self
            .symbols
            .iter()
            .find(|&&s| s.okx_symbol() == inst_id)
            .copied()
            .ok_or_else(|| format!("unknown okx instId: {inst_id}"))?;

        let entry = &data[0];
        let bids = parse_okx_levels(&entry.bids)?;
        let asks = parse_okx_levels(&entry.asks)?;

        let ts = entry
            .ts
            .parse::<i64>()
            .ok()
            .and_then(|ts| Utc.timestamp_millis_opt(ts).single())
            .unwrap_or_else(Utc::now);

        let ob = OrderBook {
            exchange: Exchange::Okx,
            symbol,
            bids,
            asks,
            timestamp: ts,
        };

        Ok(vec![WsUpdate::OrderBook(ob)])
    }
}

fn parse_okx_levels(raw: &[Vec<String>]) -> Result<Vec<OrderBookLevel>, String> {
    raw.iter()
        .map(|v| {
            if v.len() < 2 {
                return Err("invalid okx level format".into());
            }
            Ok(OrderBookLevel {
                price: v[0].parse().map_err(|e| format!("price parse: {e}"))?,
                quantity: v[1].parse().map_err(|e| format!("qty parse: {e}"))?,
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
        OkxAdapter::new(symbols),
        store,
        reconnect_backoff_ms,
        reconnect_max_attempts,
    )
    .await;
}
