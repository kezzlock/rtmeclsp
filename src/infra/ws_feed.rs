use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::time::{Duration, sleep};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    symbol::Symbol,
};
use crate::state::snapshot_store::SharedSnapshotStore;

/// Implemented by each WS exchange adapter. All methods are sync — only the runner is async.
pub trait WsAdapter: Send {
    fn exchange(&self) -> Exchange;
    /// Full WebSocket URL, including any query params encoding symbol subscriptions.
    fn ws_url(&self) -> String;
    /// JSON subscribe message to send after connecting. None = subscription baked into URL.
    fn subscribe_message(&self) -> Option<String>;
    /// Parse one text frame. Returns price updates or Err on unrecoverable parse failure.
    /// Return Ok(empty vec) for expected non-price messages (heartbeats, status, etc.).
    fn parse_message(&self, text: &str) -> Result<Vec<(Symbol, Decimal, Option<DateTime<Utc>>)>, String>;
}

pub async fn run_ws_feed(
    adapter: impl WsAdapter,
    store: SharedSnapshotStore,
    backoff_ms: u64,
    max_attempts: u32,
) {
    let exchange = adapter.exchange();
    let name = exchange.as_str();
    let url = adapter.ws_url();
    let mut attempt = 0u32;
    let mut current_backoff = backoff_ms;

    loop {
        attempt += 1;
        info!(attempt, "{name}: connecting to {url}");

        store.update_exchange_status(
            exchange,
            ExchangeStatus::Reconnecting { attempt, next_retry_at: Utc::now() },
        );

        match connect_async(&url).await {
            Ok((mut ws_stream, _)) => {
                info!("{name}: connected");
                attempt = 0;
                current_backoff = backoff_ms;

                if let Some(sub) = adapter.subscribe_message() {
                    if let Err(e) = ws_stream.send(Message::Text(sub.into())).await {
                        error!("{name}: failed to send subscription — {e}");
                        continue;
                    }
                    info!("{name}: subscribed");
                }

                store.update_exchange_status(
                    exchange,
                    ExchangeStatus::Connected { since: Utc::now() },
                );

                let reason = handle_stream(&mut ws_stream, &store, &adapter).await;

                warn!("{name}: disconnected — {reason}");
                store.update_exchange_status(
                    exchange,
                    ExchangeStatus::Disconnected { since: Utc::now(), reason },
                );
            }
            Err(e) => {
                error!("{name}: connection failed — {e}");
            }
        }

        if max_attempts > 0 && attempt >= max_attempts {
            error!("{name}: max reconnect attempts reached, giving up");
            store.update_exchange_status(
                exchange,
                ExchangeStatus::Disconnected {
                    since: Utc::now(),
                    reason: "max reconnect attempts reached".into(),
                },
            );
            return;
        }

        let next_retry_at =
            Utc::now() + chrono::Duration::milliseconds(current_backoff as i64);
        store.update_exchange_status(
            exchange,
            ExchangeStatus::Reconnecting { attempt, next_retry_at },
        );

        info!("{name}: retrying in {current_backoff}ms");
        sleep(Duration::from_millis(current_backoff)).await;
        current_backoff = (current_backoff * 2).min(30_000);
    }
}

async fn handle_stream<A: WsAdapter>(
    ws_stream: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    store: &SharedSnapshotStore,
    adapter: &A,
) -> String {
    let exchange = adapter.exchange();
    let name = exchange.as_str();

    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(text)) => match adapter.parse_message(&text) {
                Ok(updates) => {
                    for (symbol, price, exchange_ts) in updates {
                        store.update_snapshot(exchange, symbol, price, exchange_ts);
                    }
                }
                Err(e) => warn!("{name}: parse error — {e}"),
            },
            Ok(Message::Ping(data)) => {
                if let Err(e) = ws_stream.send(Message::Pong(data)).await {
                    return format!("failed to send pong: {e}");
                }
            }
            Ok(Message::Close(frame)) => {
                return format!("server closed connection: {frame:?}");
            }
            Ok(_) => {}
            Err(e) => return format!("stream error: {e}"),
        }
    }
    "stream ended".into()
}
