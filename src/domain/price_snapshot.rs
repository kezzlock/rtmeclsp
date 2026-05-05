use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;

use super::{exchange::Exchange, symbol::Symbol};

#[derive(Debug, Clone, Serialize)]
pub struct PriceSnapshot {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub price: Decimal,
    pub exchange_ts: Option<DateTime<Utc>>,
    pub received_ts: DateTime<Utc>,
    pub is_stale: bool,
}

impl PriceSnapshot {
    pub fn new(
        exchange: Exchange,
        symbol: Symbol,
        price: Decimal,
        exchange_ts: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            exchange,
            symbol,
            price,
            exchange_ts,
            received_ts: Utc::now(),
            is_stale: false,
        }
    }

    pub fn latency_ms(&self) -> Option<f64> {
        let exchange_ts = self.exchange_ts?;
        let diff = self.received_ts - exchange_ts;
        Some(diff.num_microseconds()? as f64 / 1000.0)
    }
}
