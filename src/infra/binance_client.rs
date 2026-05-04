use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream, WebSocketStream,
};
use tracing::{error, info, warn};

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    symbol::Symbol,
};
use crate::state::snapshot_store::SharedSnapshotStore;

const WS_URL: &str = "wss://stream.binance.com:9443/stream";

// Payload z Binance individual symbol ticker stream (@ticker)
// https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-ticker-streams
#[derive(Debug, Deserialize)]
struct BinanceCombinedMessage {
    data: BinanceTickerData,
}

#[derive(Debug, Deserialize)]
struct BinanceTickerData {
    #[serde(rename = "c")]
    last_price: String,
    #[serde(rename = "T")]
    close_time: u64,
}

pub async fn run(
    store: SharedSnapshotStore,
    symbols: Vec<Symbol>,
    reconnect_backoff_ms: u64,
    reconnect_max_attempts: u32,
) {
    let streams = symbols
        .iter()
        .map(|s| s.binance_stream())
        .collect::<Vec<_>>()
        .join("/");
    let url = format!("{WS_URL}?streams={streams}");

    let mut attempt = 0u32;
    let mut backoff_ms = reconnect_backoff_ms;

    loop {
        attempt += 1;
        info!(attempt, "binance: connecting to {url}");

        store.update_exchange_status(
            Exchange::Binance,
            ExchangeStatus::Reconnecting {
                attempt,
                next_retry_at: Utc::now(),
            },
        );

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!("binance: connected");
                attempt = 0;
                backoff_ms = reconnect_backoff_ms;

                store.update_exchange_status(
                    Exchange::Binance,
                    ExchangeStatus::Connected { since: Utc::now() },
                );

                let disconnect_reason = handle_stream(ws_stream, &store, &symbols).await;

                warn!("binance: disconnected — {disconnect_reason}");
                store.update_exchange_status(
                    Exchange::Binance,
                    ExchangeStatus::Disconnected {
                        since: Utc::now(),
                        reason: disconnect_reason,
                    },
                );
            }
            Err(e) => {
                error!("binance: connection failed — {e}");
            }
        }

        if reconnect_max_attempts > 0 && attempt >= reconnect_max_attempts {
            error!("binance: max reconnect attempts reached, giving up");
            store.update_exchange_status(
                Exchange::Binance,
                ExchangeStatus::Disconnected {
                    since: Utc::now(),
                    reason: "max reconnect attempts reached".into(),
                },
            );
            return;
        }

        let next_retry_at = Utc::now() + chrono::Duration::milliseconds(backoff_ms as i64);
        store.update_exchange_status(
            Exchange::Binance,
            ExchangeStatus::Reconnecting {
                attempt,
                next_retry_at,
            },
        );

        info!("binance: retrying in {backoff_ms}ms");
        sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(30_000);
    }
}

async fn handle_stream(
    mut ws_stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    store: &SharedSnapshotStore,
    symbols: &[Symbol],
) -> String {
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Err(e) = process_message(&text, store, symbols) {
                    warn!("binance: failed to process message — {e}: {text}");
                }
            }
            Ok(Message::Ping(data)) => {
                if let Err(e) = ws_stream.send(Message::Pong(data)).await {
                    return format!("failed to send pong: {e}");
                }
            }
            Ok(Message::Close(frame)) => {
                return format!("server closed connection: {frame:?}");
            }
            Ok(_) => {}
            Err(e) => {
                return format!("stream error: {e}");
            }
        }
    }
    "stream ended".into()
}

fn process_message(
    text: &str,
    store: &SharedSnapshotStore,
    symbols: &[Symbol],
) -> Result<(), Box<dyn std::error::Error>> {
    let msg: BinanceCombinedMessage = serde_json::from_str(text)?;
    let price: f64 = msg.data.last_price.parse()?;

    let exchange_ts: Option<DateTime<Utc>> = if msg.data.close_time > 0 {
        Utc.timestamp_millis_opt(msg.data.close_time as i64).single()
    } else {
        None
    };

    // Dopasowanie symbolu na podstawie streamu — Binance zwraca dane per stream,
    // ale w combined stream nie ma jawnego pola z nazwą symbolu w BinanceTickerData,
    // więc identyfikujemy po cenie względem subskrybowanych symboli.
    // W praktyce Binance combined stream zawiera pole "stream" na poziomie root.
    let symbol = match_symbol_from_combined(text, symbols)?;

    store.update_snapshot(Exchange::Binance, symbol, price, exchange_ts);
    Ok(())
}

fn match_symbol_from_combined(
    text: &str,
    symbols: &[Symbol],
) -> Result<Symbol, Box<dyn std::error::Error>> {
    // Parsujemy tylko pole "stream" żeby uniknąć podwójnego parsowania całego JSON
    #[derive(Deserialize)]
    struct StreamWrapper {
        stream: String,
    }
    let wrapper: StreamWrapper = serde_json::from_str(text)?;

    for &symbol in symbols {
        if wrapper.stream.starts_with(&symbol.binance_stream()[..wrapper.stream.find('@').unwrap_or(wrapper.stream.len())]) {
            return Ok(symbol);
        }
    }

    // Dopasowanie przez pełną nazwę streamu
    for &symbol in symbols {
        if wrapper.stream == symbol.binance_stream() {
            return Ok(symbol);
        }
    }

    Err(format!("unknown stream: {}", wrapper.stream).into())
}
