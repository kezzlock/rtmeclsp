use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
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

const WS_URL: &str = "wss://wbs.mexc.com/ws";

// https://mexcdevelop.github.io/apidocs/spot_v3_en/#individual-symbol-book-ticker-streams
#[derive(Debug, Serialize)]
struct SubscribeRequest {
    method: &'static str,
    params: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct MexcMessage {
    #[serde(rename = "c")]
    channel: Option<String>,
    #[serde(rename = "d")]
    data: Option<MexcTickerData>,
    #[serde(rename = "t")]
    timestamp: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct MexcTickerData {
    // last price — pole "p" w miniTicker v3
    #[serde(rename = "p")]
    last_price: Option<String>,
    // symbol
    #[serde(rename = "s")]
    symbol: Option<String>,
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
        info!(attempt, "mexc: connecting to {WS_URL}");

        store.update_exchange_status(
            Exchange::Mexc,
            ExchangeStatus::Reconnecting {
                attempt,
                next_retry_at: Utc::now(),
            },
        );

        match connect_async(WS_URL).await {
            Ok((mut ws_stream, _)) => {
                info!("mexc: connected, subscribing");
                attempt = 0;
                backoff_ms = reconnect_backoff_ms;

                let params = symbols
                    .iter()
                    .map(|s| format!("spot@public.miniTicker.v3.api@{}", s.mexc_symbol()))
                    .collect();

                let sub = SubscribeRequest {
                    method: "SUBSCRIPTION",
                    params,
                };

                let sub_json = match serde_json::to_string(&sub) {
                    Ok(j) => j,
                    Err(e) => {
                        error!("mexc: failed to serialize subscription — {e}");
                        continue;
                    }
                };

                if let Err(e) = ws_stream.send(Message::Text(sub_json.into())).await {
                    error!("mexc: failed to send subscription — {e}");
                    continue;
                }

                store.update_exchange_status(
                    Exchange::Mexc,
                    ExchangeStatus::Connected { since: Utc::now() },
                );

                let disconnect_reason = handle_stream(ws_stream, &store, &symbols).await;

                warn!("mexc: disconnected — {disconnect_reason}");
                store.update_exchange_status(
                    Exchange::Mexc,
                    ExchangeStatus::Disconnected {
                        since: Utc::now(),
                        reason: disconnect_reason,
                    },
                );
            }
            Err(e) => {
                error!("mexc: connection failed — {e}");
            }
        }

        if reconnect_max_attempts > 0 && attempt >= reconnect_max_attempts {
            error!("mexc: max reconnect attempts reached, giving up");
            store.update_exchange_status(
                Exchange::Mexc,
                ExchangeStatus::Disconnected {
                    since: Utc::now(),
                    reason: "max reconnect attempts reached".into(),
                },
            );
            return;
        }

        let next_retry_at = Utc::now() + chrono::Duration::milliseconds(backoff_ms as i64);
        store.update_exchange_status(
            Exchange::Mexc,
            ExchangeStatus::Reconnecting {
                attempt,
                next_retry_at,
            },
        );

        info!("mexc: retrying in {backoff_ms}ms");
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
                    warn!("mexc: failed to process message — {e}: {text}");
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
    let msg: MexcMessage = serde_json::from_str(text)?;

    let channel = match &msg.channel {
        Some(c) => c,
        None => return Ok(()), // wiadomość systemowa (np. potwierdzenie subskrypcji)
    };

    let data = match &msg.data {
        Some(d) => d,
        None => return Ok(()),
    };

    let price_str = match &data.last_price {
        Some(p) => p,
        None => return Ok(()),
    };

    let price: f64 = price_str.parse()?;

    let exchange_ts = msg
        .timestamp
        .and_then(|ts| Utc.timestamp_millis_opt(ts as i64).single());

    let symbol = match_symbol(channel, symbols)?;

    store.update_snapshot(Exchange::Mexc, symbol, price, exchange_ts);
    Ok(())
}

fn match_symbol(channel: &str, symbols: &[Symbol]) -> Result<Symbol, Box<dyn std::error::Error>> {
    for &symbol in symbols {
        if channel.ends_with(symbol.mexc_symbol()) {
            return Ok(symbol);
        }
    }
    Err(format!("unknown channel: {channel}").into())
}
