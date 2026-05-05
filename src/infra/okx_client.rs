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
    data: Option<Vec<OkxTickerData>>,
}

#[derive(Debug, Deserialize)]
struct OkxArg {
    #[serde(rename = "instId")]
    inst_id: String,
}

#[derive(Debug, Deserialize)]
struct OkxTickerData {
    last: String,
    ts: String,
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
        info!(attempt, "okx: connecting to {WS_URL}");

        store.update_exchange_status(
            Exchange::Okx,
            ExchangeStatus::Reconnecting {
                attempt,
                next_retry_at: Utc::now(),
            },
        );

        match connect_async(WS_URL).await {
            Ok((mut ws_stream, _)) => {
                info!("okx: connected, subscribing");
                attempt = 0;
                backoff_ms = reconnect_backoff_ms;

                let args: Vec<SubscribeArg> = symbols
                    .iter()
                    .map(|s| SubscribeArg {
                        channel: "tickers",
                        inst_id: s.okx_symbol(),
                    })
                    .collect();

                let sub = SubscribeRequest {
                    op: "subscribe",
                    args,
                };

                let sub_json = match serde_json::to_string(&sub) {
                    Ok(j) => j,
                    Err(e) => {
                        error!("okx: failed to serialize subscription — {e}");
                        continue;
                    }
                };

                if let Err(e) = ws_stream.send(Message::Text(sub_json.into())).await {
                    error!("okx: failed to send subscription — {e}");
                    continue;
                }

                store.update_exchange_status(
                    Exchange::Okx,
                    ExchangeStatus::Connected { since: Utc::now() },
                );

                let disconnect_reason = handle_stream(ws_stream, &store, &symbols).await;

                warn!("okx: disconnected — {disconnect_reason}");
                store.update_exchange_status(
                    Exchange::Okx,
                    ExchangeStatus::Disconnected {
                        since: Utc::now(),
                        reason: disconnect_reason,
                    },
                );
            }
            Err(e) => {
                error!("okx: connection failed — {e}");
            }
        }

        if reconnect_max_attempts > 0 && attempt >= reconnect_max_attempts {
            error!("okx: max reconnect attempts reached, giving up");
            store.update_exchange_status(
                Exchange::Okx,
                ExchangeStatus::Disconnected {
                    since: Utc::now(),
                    reason: "max reconnect attempts reached".into(),
                },
            );
            return;
        }

        let next_retry_at = Utc::now() + chrono::Duration::milliseconds(backoff_ms as i64);
        store.update_exchange_status(
            Exchange::Okx,
            ExchangeStatus::Reconnecting {
                attempt,
                next_retry_at,
            },
        );

        info!("okx: retrying in {backoff_ms}ms");
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
                    warn!("okx: failed to process message — {e}: {text}");
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
    if text == "ping" {
        return Ok(());
    }

    let msg: OkxMessage = serde_json::from_str(text)?;

    let inst_id = match &msg.arg {
        Some(a) => a.inst_id.as_str(),
        None => return Ok(()),
    };

    let data = match &msg.data {
        Some(d) if !d.is_empty() => d,
        _ => return Ok(()),
    };

    let symbol = symbols
        .iter()
        .find(|&&s| s.okx_symbol() == inst_id)
        .copied()
        .ok_or_else(|| format!("unknown okx instId: {inst_id}"))?;

    let entry = &data[0];
    let price: f64 = entry.last.parse()?;

    let exchange_ts = entry
        .ts
        .parse::<i64>()
        .ok()
        .and_then(|ts| Utc.timestamp_millis_opt(ts).single());

    store.update_snapshot(Exchange::Okx, symbol, price, exchange_ts);
    Ok(())
}
