use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::domain::{exchange::Exchange, symbol::Symbol};
use crate::infra::ws_feed::{WsAdapter, run_ws_feed};
use crate::state::snapshot_store::SharedSnapshotStore;

const WS_BASE: &str = "wss://stream.binance.com:9443/stream";

#[derive(Debug, Deserialize)]
struct BinanceCombinedMessage {
    stream: String,
    data: BinanceTickerData,
}

#[derive(Debug, Deserialize)]
struct BinanceTickerData {
    #[serde(rename = "c")]
    last_price: String,
    #[serde(rename = "E", default)]
    event_time: u64,
}

pub struct BinanceAdapter {
    symbols: Vec<Symbol>,
    url: String,
}

impl BinanceAdapter {
    fn new(symbols: Vec<Symbol>) -> Self {
        let streams = symbols
            .iter()
            .map(|s| s.binance_stream())
            .collect::<Vec<_>>()
            .join("/");
        let url = format!("{WS_BASE}?streams={streams}");
        Self { symbols, url }
    }
}

impl WsAdapter for BinanceAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    fn ws_url(&self) -> String {
        self.url.clone()
    }

    fn subscribe_message(&self) -> Option<String> {
        None
    }

    fn parse_message(&self, text: &str) -> Result<Vec<(Symbol, Decimal, Option<DateTime<Utc>>)>, String> {
        let msg: BinanceCombinedMessage = serde_json::from_str(text)
            .map_err(|e| format!("deserialize: {e}"))?;

        let price: Decimal = msg.data.last_price.parse()
            .map_err(|e| format!("price parse: {e}"))?;

        let exchange_ts: Option<DateTime<Utc>> = if msg.data.event_time > 0 {
            Utc.timestamp_millis_opt(msg.data.event_time as i64).single()
        } else {
            None
        };

        let symbol = self
            .symbols
            .iter()
            .find(|&&s| s.binance_stream() == msg.stream.as_str())
            .copied()
            .ok_or_else(|| format!("unknown stream: {}", msg.stream))?;

        Ok(vec![(symbol, price, exchange_ts)])
    }
}

pub async fn run(
    store: SharedSnapshotStore,
    symbols: Vec<Symbol>,
    reconnect_backoff_ms: u64,
    reconnect_max_attempts: u32,
) {
    run_ws_feed(
        BinanceAdapter::new(symbols),
        store,
        reconnect_backoff_ms,
        reconnect_max_attempts,
    )
    .await;
}
