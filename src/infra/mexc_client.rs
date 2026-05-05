use chrono::Utc;
use serde::Deserialize;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    symbol::Symbol,
};
use crate::state::snapshot_store::SharedSnapshotStore;

const REST_BASE: &str = "https://api.mexc.com/api/v3/ticker/price";
const POLL_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Deserialize)]
struct MexcTickerPrice {
    // symbol w formacie BTCUSDT
    #[allow(dead_code)]
    symbol: String,
    price: String,
}

pub async fn run(
    store: SharedSnapshotStore,
    symbols: Vec<Symbol>,
    _reconnect_backoff_ms: u64,
    _reconnect_max_attempts: u32,
) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            error!("mexc: failed to build HTTP client — {e}");
            return;
        }
    };

    info!("mexc: starting REST polling every {POLL_INTERVAL_MS}ms");
    store.update_exchange_status(
        Exchange::Mexc,
        ExchangeStatus::Connected { since: Utc::now() },
    );

    loop {
        for &symbol in &symbols {
            let url = format!("{REST_BASE}?symbol={}", symbol.mexc_rest_symbol());

            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<MexcTickerPrice>().await {
                        Ok(ticker) => match ticker.price.parse::<f64>() {
                            Ok(price) => {
                                store.update_snapshot(Exchange::Mexc, symbol, price, None);
                            }
                            Err(e) => {
                                warn!("mexc: failed to parse price '{}' — {e}", ticker.price);
                            }
                        },
                        Err(e) => {
                            warn!(
                                "mexc: failed to deserialize response for {} — {e}",
                                symbol.mexc_rest_symbol()
                            );
                        }
                    }
                }
                Ok(resp) => {
                    warn!(
                        "mexc: HTTP {} for {}",
                        resp.status(),
                        symbol.mexc_rest_symbol()
                    );
                    store.update_exchange_status(
                        Exchange::Mexc,
                        ExchangeStatus::Disconnected {
                            since: Utc::now(),
                            reason: format!("HTTP {}", resp.status()),
                        },
                    );
                }
                Err(e) => {
                    warn!(
                        "mexc: request failed for {} — {e}",
                        symbol.mexc_rest_symbol()
                    );
                    store.update_exchange_status(
                        Exchange::Mexc,
                        ExchangeStatus::Disconnected {
                            since: Utc::now(),
                            reason: e.to_string(),
                        },
                    );
                }
            }
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
}
