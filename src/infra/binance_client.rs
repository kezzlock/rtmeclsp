use serde::Deserialize;

use crate::domain::{
    exchange::Exchange,
    order_book::{OrderBook, OrderBookLevel},
    symbol::Symbol,
};
use crate::infra::ws_feed::{WsAdapter, WsUpdate, run_ws_feed};
use crate::state::snapshot_store::SharedSnapshotStore;

const WS_BASE: &str = "wss://stream.binance.com:9443/stream";

#[derive(Debug, Deserialize)]
struct BinanceCombinedMessage {
    stream: String,
    data: BinanceDepthData,
}

#[derive(Debug, Deserialize)]
struct BinanceDepthData {
    #[serde(rename = "lastUpdateId")]
    _last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

pub struct BinanceAdapter {
    symbols: Vec<Symbol>,
    url: String,
}

impl BinanceAdapter {
    fn new(symbols: Vec<Symbol>) -> Self {
        let streams = symbols
            .iter()
            .map(|s| format!("{}@depth5@100ms", s.as_str().to_lowercase()))
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

    fn parse_message(&self, text: &str) -> Result<Vec<WsUpdate>, String> {
        let msg: BinanceCombinedMessage =
            serde_json::from_str(text).map_err(|e| format!("deserialize: {e}"))?;

        let symbol_str = msg.stream.split('@').next().unwrap_or_default();
        let symbol = self
            .symbols
            .iter()
            .find(|&&s| s.as_str().to_lowercase() == symbol_str)
            .copied()
            .ok_or_else(|| format!("unknown stream: {}", msg.stream))?;

        let bids = parse_levels(&msg.data.bids)?;
        let asks = parse_levels(&msg.data.asks)?;

        let ob = OrderBook::new(Exchange::Binance, symbol, bids, asks);
        Ok(vec![WsUpdate::OrderBook(ob)])
    }
}

fn parse_levels(raw: &[[String; 2]]) -> Result<Vec<OrderBookLevel>, String> {
    raw.iter()
        .map(|[p, q]| {
            Ok(OrderBookLevel {
                price: p.parse().map_err(|e| format!("price parse: {e}"))?,
                quantity: q.parse().map_err(|e| format!("qty parse: {e}"))?,
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
        BinanceAdapter::new(symbols),
        store,
        reconnect_backoff_ms,
        reconnect_max_attempts,
    )
    .await;
}
