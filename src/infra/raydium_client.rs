use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::json;
use base64::{engine::general_purpose, Engine as _};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    order_book::{OrderBook, OrderBookLevel},
    symbol::Symbol,
};
use crate::state::snapshot_store::SharedSnapshotStore;

pub async fn run(
    store: SharedSnapshotStore,
    symbols: Vec<Symbol>,
    rpc_url: String,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    info!("raydium: starting JSON-RPC polling every 5s for {} symbols", symbols.len());
    store.update_exchange_status(
        Exchange::Raydium,
        ExchangeStatus::Connected { since: Utc::now() },
    );

    loop {
        for &symbol in &symbols {
            let pool_addr = match symbol {
                Symbol::SolUsdt => "7XawhbbxtsRcQA8QoDCRrjULne6fAbRwoScy26h56w9F", // SOL/USDT Raydium
                _ => continue,
            };

            let payload = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [
                    pool_addr,
                    { "encoding": "base64", "commitment": "finalized" }
                ]
            });

            tracing::debug!("raydium: polling {symbol:?} at {pool_addr}");
            match client.post(&rpc_url).json(&payload).send().await {
                Ok(resp) => {
                    tracing::debug!("raydium: received response for {symbol:?}");
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        tracing::debug!("raydium: json response: {json}");
                        if let Some(data_array) = json["result"]["value"]["data"].as_array() {
                            if let Some(base64_str) = data_array.get(0).and_then(|v| v.as_str()) {
                                if let Ok(data) = general_purpose::STANDARD.decode(base64_str) {
                                    if data.len() >= 400 {
                                        let reserve_a = read_u64(&data, 320);
                                        let reserve_b = read_u64(&data, 384);
                                        
                                        let price = calculate_raydium_price(reserve_a, reserve_b, symbol);
                                        
                                        if let Some(p) = price {
                                             let spread_multiplier = Decimal::from_str_radix("0.001", 10).unwrap();
                                             let bid_price = p * (Decimal::ONE - spread_multiplier);
                                             let ask_price = p * (Decimal::ONE + spread_multiplier);
 
                                             let ob = OrderBook::new(
                                                 Exchange::Raydium,
                                                 symbol,
                                                 vec![OrderBookLevel { price: bid_price, quantity: Decimal::from(1000) }],
                                                 vec![OrderBookLevel { price: ask_price, quantity: Decimal::from(1000) }],
                                             );
                                             store.update_order_book(ob);
                                        }
                                    }
                                }
                            }
                        } else if let Some(err) = json.get("error") {
                            warn!("raydium: RPC error for {symbol:?} — {err}");
                        }
                    }
                }
                Err(e) => {
                    warn!("raydium: HTTP request failed for {symbol:?} — {e}");
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        sleep(Duration::from_secs(5)).await;
    }
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}

fn calculate_raydium_price(reserve_a: u64, reserve_b: u64, symbol: Symbol) -> Option<Decimal> {
    if reserve_a == 0 || reserve_b == 0 {
        return None;
    }

    let (dec_a, dec_b) = match symbol {
        Symbol::SolUsdt => (9, 6), // SOL (9) / USDC (6)
        _ => return None,
    };

    let p = (reserve_b as f64 / 10f64.powi(dec_b)) / (reserve_a as f64 / 10f64.powi(dec_a));
    Decimal::from_f64_retain(p)
}
