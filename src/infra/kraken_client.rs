use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{
    exchange::Exchange,
    order_book::{OrderBook, OrderBookLevel},
    symbol::Symbol,
};
use crate::infra::ws_feed::{WsAdapter, WsUpdate, run_ws_feed};
use crate::state::snapshot_store::SharedSnapshotStore;

const WS_URL: &str = "wss://ws.kraken.com/v2";

#[derive(Debug, Serialize)]
struct SubscribeRequest {
    method: &'static str,
    params: SubscribeParams,
}

#[derive(Debug, Serialize)]
struct SubscribeParams {
    channel: &'static str,
    symbol: Vec<&'static str>,
    depth: u32,
}

#[derive(Debug, Deserialize)]
struct KrakenEnvelope {
    channel: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct KrakenBookEntry {
    symbol: String,
    bids: Vec<KrakenLevel>,
    asks: Vec<KrakenLevel>,
    _timestamp: String,
}

#[derive(Debug, Deserialize)]
struct KrakenLevel {
    price: Decimal,
    qty: Decimal,
}

pub struct KrakenAdapter {
    symbols: Vec<Symbol>,
    subscribe_json: String,
}

impl KrakenAdapter {
    fn new(symbols: Vec<Symbol>) -> Self {
        let symbol_strs: Vec<&'static str> = symbols.iter().map(|s| s.kraken_symbol()).collect();
        let sub = SubscribeRequest {
            method: "subscribe",
            params: SubscribeParams {
                channel: "book",
                symbol: symbol_strs,
                depth: 10,
            },
        };
        let subscribe_json =
            serde_json::to_string(&sub).expect("subscribe serialization is infallible");
        Self {
            symbols,
            subscribe_json,
        }
    }
}

impl WsAdapter for KrakenAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Kraken
    }

    fn ws_url(&self) -> String {
        WS_URL.to_string()
    }

    fn subscribe_message(&self) -> Option<String> {
        Some(self.subscribe_json.clone())
    }

    fn parse_message(&self, text: &str) -> Result<Vec<WsUpdate>, String> {
        let envelope: KrakenEnvelope =
            serde_json::from_str(text).map_err(|e| format!("deserialize: {e}"))?;

        if envelope.channel.as_deref() != Some("book") {
            return Ok(vec![]);
        }
        match envelope.msg_type.as_deref() {
            Some("snapshot") | Some("update") => {}
            _ => return Ok(vec![]),
        }

        let raw_entries = match &envelope.data {
            Some(d) if !d.is_empty() => d,
            _ => return Ok(vec![]),
        };

        let mut updates = Vec::with_capacity(raw_entries.len());
        for raw in raw_entries {
            let entry: KrakenBookEntry =
                serde_json::from_value(raw.clone()).map_err(|e| format!("book entry: {e}"))?;

            let symbol = self
                .symbols
                .iter()
                .find(|&&s| s.kraken_symbol() == entry.symbol.as_str())
                .copied()
                .ok_or_else(|| format!("unknown kraken symbol: {}", entry.symbol))?;

            let bids = entry
                .bids
                .into_iter()
                .map(|l| OrderBookLevel {
                    price: l.price,
                    quantity: l.qty,
                })
                .collect();
            let asks = entry
                .asks
                .into_iter()
                .map(|l| OrderBookLevel {
                    price: l.price,
                    quantity: l.qty,
                })
                .collect();

            let ob = OrderBook::new(Exchange::Kraken, symbol, bids, asks);
            updates.push(WsUpdate::OrderBook(ob));
        }

        Ok(updates)
    }
}

pub async fn run(
    store: SharedSnapshotStore,
    symbols: Vec<Symbol>,
    reconnect_backoff_ms: u64,
    reconnect_max_attempts: u32,
) {
    run_ws_feed(
        KrakenAdapter::new(symbols),
        store,
        reconnect_backoff_ms,
        reconnect_max_attempts,
    )
    .await;
}
