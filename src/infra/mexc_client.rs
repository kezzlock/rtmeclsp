use chrono::Utc;
use serde::Deserialize;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    symbol::Symbol,
};
use crate::state::snapshot_store::SharedSnapshotStore;

// Bulk endpoint — 1 request/s zamiast N×symbols/s, nie grozi rate limitem
const REST_BULK_URL: &str = "https://api.mexc.com/api/v3/ticker/price";
const POLL_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Deserialize)]
struct MexcTickerPrice {
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

    // Zbuduj zbiór symboli których szukamy (jako &str dla O(1) lookup)
    let wanted: std::collections::HashSet<&'static str> =
        symbols.iter().map(|s| s.mexc_rest_symbol()).collect();

    info!("mexc: starting bulk REST polling every {POLL_INTERVAL_MS}ms for {} symbols", symbols.len());
    store.update_exchange_status(
        Exchange::Mexc,
        ExchangeStatus::Connected { since: Utc::now() },
    );

    loop {
        match client.get(REST_BULK_URL).send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<Vec<MexcTickerPrice>>().await {
                    Ok(tickers) => {
                        for ticker in &tickers {
                            if !wanted.contains(ticker.symbol.as_str()) {
                                continue;
                            }
                            if let Ok(price) = ticker.price.parse::<f64>() {
                                if let Some(&symbol) = symbols
                                    .iter()
                                    .find(|&&s| s.mexc_rest_symbol() == ticker.symbol.as_str())
                                {
                                    store.update_snapshot(Exchange::Mexc, symbol, price, None);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("mexc: failed to deserialize bulk response — {e}");
                    }
                }
            }
            Ok(resp) => {
                warn!("mexc: HTTP {} from bulk endpoint", resp.status());
                store.update_exchange_status(
                    Exchange::Mexc,
                    ExchangeStatus::Disconnected {
                        since: Utc::now(),
                        reason: format!("HTTP {}", resp.status()),
                    },
                );
            }
            Err(e) => {
                warn!("mexc: request failed — {e}");
                store.update_exchange_status(
                    Exchange::Mexc,
                    ExchangeStatus::Disconnected {
                        since: Utc::now(),
                        reason: e.to_string(),
                    },
                );
            }
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
}
