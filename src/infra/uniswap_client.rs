use std::sync::Arc;
use chrono::Utc;
use ethers::prelude::*;
use rust_decimal::Decimal;
use tokio::time::{sleep, Duration, timeout};
use tracing::{error, info, warn};

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    order_book::{OrderBook, OrderBookLevel},
    symbol::Symbol,
};
use crate::state::snapshot_store::SharedSnapshotStore;

const QUOTER_ADDR: &str = "0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6";

abigen!(
    IQuoter,
    r#"[
        function quoteExactInputSingle(address tokenIn, address tokenOut, uint24 fee, uint256 amountIn, uint160 sqrtPriceLimitX96) external returns (uint256 amountOut)
    ]"#
);

pub async fn run(
    store: SharedSnapshotStore,
    symbols: Vec<Symbol>,
    rpc_url: String,
) {
    let provider = match Provider::<Http>::try_from(rpc_url) {
        Ok(p) => Arc::new(p),
        Err(e) => {
            error!("uniswap: failed to create provider — {e}");
            store.update_exchange_status(Exchange::Uniswap, ExchangeStatus::Disconnected { since: Utc::now(), reason: e.to_string() });
            return;
        }
    };

    let quoter_addr: Address = QUOTER_ADDR.parse().unwrap();
    let quoter = IQuoter::new(quoter_addr, provider.clone());

    info!("uniswap: starting polling every 15s for {} symbols", symbols.len());
    store.update_exchange_status(Exchange::Uniswap, ExchangeStatus::Connected { since: Utc::now() });

    loop {
        for &symbol in &symbols {
            let (token_in_str, token_out_str, fee) = match symbol.uniswap_v3_info() {
                Some(info) => info,
                None => continue,
            };

            let token_in: Address = token_in_str.parse().unwrap();
            let token_out: Address = token_out_str.parse().unwrap();

            let (amount_in, decimals_in) = match symbol {
                Symbol::BtcUsdt => (U256::from(10_000_000u64), 8), // 0.1 WBTC
                _ => (U256::from(100_000_000_000_000_000u128), 18), // 0.1 ETH/UNI
            };

            // Wrap the call in a timeout to prevent hanging
            tracing::debug!("uniswap: requesting quote for {symbol:?}");
            let call = quoter.quote_exact_input_single(token_in, token_out, fee, amount_in, U256::zero());
            match timeout(Duration::from_secs(10), call.call()).await {
                Ok(Ok(amount_out)) => {
                    let decimals_out = 6;
                    let amount_out_f = amount_out.as_u128() as f64 / 10f64.powi(decimals_out);
                    let amount_in_f = amount_in.as_u128() as f64 / 10f64.powi(decimals_in);
                    let price_f = amount_out_f / amount_in_f;
                    
                    if let Some(p) = Decimal::from_f64_retain(price_f) {
                         let spread_multiplier = Decimal::from_str_radix("0.002", 10).unwrap();
                         let bid_price = p * (Decimal::ONE - spread_multiplier);
                         let ask_price = p * (Decimal::ONE + spread_multiplier);

                         let ob = OrderBook::new(
                             Exchange::Uniswap,
                             symbol,
                             vec![OrderBookLevel { price: bid_price, quantity: Decimal::from(100) }],
                             vec![OrderBookLevel { price: ask_price, quantity: Decimal::from(100) }],
                         );
                         store.update_order_book(ob);
                    }
                }
                Ok(Err(e)) => {
                    warn!("uniswap: quote failed for {symbol:?} — {e}");
                }
                Err(_) => {
                    error!("uniswap: quote timed out for {symbol:?}");
                }
            }
            sleep(Duration::from_millis(1000)).await;
        }
        sleep(Duration::from_secs(15)).await;
    }
}
