use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{exchange::Exchange, symbol::Symbol};
use crate::infra::ws_feed::{WsAdapter, run_ws_feed};
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
}

#[derive(Debug, Deserialize)]
struct KrakenEnvelope {
    channel: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct KrakenTickerEntry {
    symbol: String,
    last: f64, // Kraken sends a JSON number, not a string
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
            params: SubscribeParams { channel: "ticker", symbol: symbol_strs },
        };
        let subscribe_json = serde_json::to_string(&sub).expect("subscribe serialization is infallible");
        Self { symbols, subscribe_json }
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

    fn parse_message(&self, text: &str) -> Result<Vec<(Symbol, Decimal, Option<DateTime<Utc>>)>, String> {
        let envelope: KrakenEnvelope = serde_json::from_str(text)
            .map_err(|e| format!("deserialize: {e}"))?;

        if envelope.channel.as_deref() != Some("ticker") {
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
            let entry: KrakenTickerEntry = serde_json::from_value(raw.clone())
                .map_err(|e| format!("ticker entry: {e}"))?;

            // Kraken sends JSON numbers; convert via string to preserve display precision
            let price: Decimal = entry.last.to_string().parse()
                .map_err(|e| format!("price parse: {e}"))?;

            let symbol = self
                .symbols
                .iter()
                .find(|&&s| s.kraken_symbol() == entry.symbol.as_str())
                .copied()
                .ok_or_else(|| format!("unknown kraken symbol: {}", entry.symbol))?;

            updates.push((symbol, price, None));
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
