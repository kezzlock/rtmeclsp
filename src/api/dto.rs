use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::domain::exchange::ExchangeStatus;

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    pub status: &'static str,
    pub has_data: bool,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SnapshotResponse {
    pub symbols: Vec<SymbolView>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SymbolView {
    pub symbol: String,
    pub entries: Vec<ExchangeEntry>,
    pub median_price: Decimal,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ExchangeEntry {
    pub exchange: String,
    pub price: Decimal,
    pub latency_ms: Option<f64>,
    pub diff_from_median: Decimal,
    pub is_stale: bool,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ExchangesResponse {
    pub exchanges: Vec<ExchangeStatusView>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ExchangeStatusView {
    pub exchange: String,
    pub status: ExchangeStatus,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HistoryResponse {
    pub symbol: String,
    pub entries: Vec<HistoryEntryDto>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HistoryEntryDto {
    pub exchange: String,
    pub price: Decimal,
    pub received_ts: DateTime<Utc>,
    pub exchange_ts: Option<DateTime<Utc>>,
    pub latency_ms: Option<f64>,
}
