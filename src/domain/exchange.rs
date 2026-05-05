use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum Exchange {
    Binance,
    Mexc,
    Kraken,
    Coinbase,
    Okx,
}

impl Exchange {
    pub fn as_str(&self) -> &'static str {
        match self {
            Exchange::Binance => "binance",
            Exchange::Mexc => "mexc",
            Exchange::Kraken => "kraken",
            Exchange::Coinbase => "coinbase",
            Exchange::Okx => "okx",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ExchangeStatus {
    Connected {
        since: DateTime<Utc>,
    },
    Disconnected {
        since: DateTime<Utc>,
        reason: String,
    },
    Reconnecting {
        attempt: u32,
        next_retry_at: DateTime<Utc>,
    },
}
