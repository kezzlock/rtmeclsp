use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, sleep};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    symbol::Symbol,
};
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

// Najpierw parsujemy tylko channel żeby nie próbować deserializować
// danych statusu jako ticker entries (różna struktura data[])
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
    last: f64,
}

pub async fn run(
    store: SharedSnapshotStore,
    symbols: Vec<Symbol>,
    reconnect_backoff_ms: u64,
    reconnect_max_attempts: u32,
) {
    let mut attempt = 0u32;
    let mut backoff_ms = reconnect_backoff_ms;

    loop {
        attempt += 1;
        info!(attempt, "kraken: connecting to {WS_URL}");

        store.update_exchange_status(
            Exchange::Kraken,
            ExchangeStatus::Reconnecting {
                attempt,
                next_retry_at: Utc::now(),
            },
        );

        match connect_async(WS_URL).await {
            Ok((mut ws_stream, _)) => {
                info!("kraken: connected, subscribing");
                attempt = 0;
                backoff_ms = reconnect_backoff_ms;

                let symbol_strs: Vec<&'static str> =
                    symbols.iter().map(|s| s.kraken_symbol()).collect();

                let sub = SubscribeRequest {
                    method: "subscribe",
                    params: SubscribeParams {
                        channel: "ticker",
                        symbol: symbol_strs,
                    },
                };

                let sub_json = match serde_json::to_string(&sub) {
                    Ok(j) => j,
                    Err(e) => {
                        error!("kraken: failed to serialize subscription — {e}");
                        continue;
                    }
                };

                if let Err(e) = ws_stream.send(Message::Text(sub_json.into())).await {
                    error!("kraken: failed to send subscription — {e}");
                    continue;
                }

                store.update_exchange_status(
                    Exchange::Kraken,
                    ExchangeStatus::Connected { since: Utc::now() },
                );

                let disconnect_reason = handle_stream(ws_stream, &store, &symbols).await;

                warn!("kraken: disconnected — {disconnect_reason}");
                store.update_exchange_status(
                    Exchange::Kraken,
                    ExchangeStatus::Disconnected {
                        since: Utc::now(),
                        reason: disconnect_reason,
                    },
                );
            }
            Err(e) => {
                error!("kraken: connection failed — {e}");
            }
        }

        if reconnect_max_attempts > 0 && attempt >= reconnect_max_attempts {
            error!("kraken: max reconnect attempts reached, giving up");
            store.update_exchange_status(
                Exchange::Kraken,
                ExchangeStatus::Disconnected {
                    since: Utc::now(),
                    reason: "max reconnect attempts reached".into(),
                },
            );
            return;
        }

        let next_retry_at = Utc::now() + chrono::Duration::milliseconds(backoff_ms as i64);
        store.update_exchange_status(
            Exchange::Kraken,
            ExchangeStatus::Reconnecting {
                attempt,
                next_retry_at,
            },
        );

        info!("kraken: retrying in {backoff_ms}ms");
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
                    warn!("kraken: failed to process message — {e}: {text}");
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
    let envelope: KrakenEnvelope = serde_json::from_str(text)?;

    // ignorujemy heartbeat, status, potwierdzenia subskrypcji — tylko ticker
    if envelope.channel.as_deref() != Some("ticker") {
        return Ok(());
    }
    match envelope.msg_type.as_deref() {
        Some("snapshot") | Some("update") => {}
        _ => return Ok(()),
    }

    let raw_entries = match &envelope.data {
        Some(d) if !d.is_empty() => d,
        _ => return Ok(()),
    };

    for raw in raw_entries {
        let entry: KrakenTickerEntry = serde_json::from_value(raw.clone())?;

        let symbol = symbols
            .iter()
            .find(|&&s| s.kraken_symbol() == entry.symbol.as_str())
            .copied()
            .ok_or_else(|| format!("unknown kraken symbol: {}", entry.symbol))?;

        store.update_snapshot(Exchange::Kraken, symbol, entry.last, None);
    }

    Ok(())
}
