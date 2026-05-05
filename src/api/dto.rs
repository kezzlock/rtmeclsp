use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::domain::exchange::ExchangeStatus;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub has_data: bool,
}

#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    pub symbols: Vec<SymbolView>,
}

#[derive(Debug, Serialize)]
pub struct SymbolView {
    pub symbol: String,
    pub entries: Vec<ExchangeEntry>,
    pub median_price: f64,
}

#[derive(Debug, Serialize)]
pub struct ExchangeEntry {
    pub exchange: String,
    pub price: f64,
    pub latency_ms: Option<f64>,
    pub diff_from_median: f64,
    pub is_stale: bool,
}

#[derive(Debug, Serialize)]
pub struct ExchangesResponse {
    pub exchanges: Vec<ExchangeStatusView>,
}

#[derive(Debug, Serialize)]
pub struct ExchangeStatusView {
    pub exchange: String,
    pub status: ExchangeStatus,
}

#[derive(Debug, Serialize)]
pub struct HistoryResponse {
    pub symbol: String,
    pub entries: Vec<HistoryEntryDto>,
}

#[derive(Debug, Serialize)]
pub struct HistoryEntryDto {
    pub exchange: String,
    pub price: f64,
    pub received_ts: DateTime<Utc>,
    pub exchange_ts: Option<DateTime<Utc>>,
    pub latency_ms: Option<f64>,
}
