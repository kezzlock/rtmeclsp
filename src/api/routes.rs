use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{FromRef, Query, State},
    http::{StatusCode, header},
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use tera::Tera;
use tokio_stream::StreamExt as _;

use crate::{
    api::dto::{
        ExchangeEntry, ExchangeStatusView, ExchangesResponse, HealthResponse, HistoryEntryDto,
        HistoryResponse, SnapshotResponse, SymbolView,
    },
    domain::exchange::ExchangeStatus,
    domain::symbol::{ALL_SYMBOLS, Symbol},
    state::snapshot_store::SharedSnapshotStore,
};

#[derive(Clone)]
pub struct AppState {
    pub store: SharedSnapshotStore,
    pub tera: Arc<Tera>,
}

impl FromRef<AppState> for SharedSnapshotStore {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.store)
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/", get(index))
        .route("/health", get(health))
        .route("/snapshot", get(snapshot))
        .route("/exchanges", get(exchanges))
        .route("/history", get(history))
        .route("/events", get(events))
        .with_state(state)
}

#[derive(OpenApi)]
#[openapi(
    paths(health, snapshot, exchanges, history),
    components(schemas(
        HealthResponse, 
        SnapshotResponse, 
        SymbolView, 
        ExchangeEntry, 
        ExchangesResponse, 
        ExchangeStatusView, 
        ExchangeStatus,
        HistoryResponse,
        HistoryEntryDto
    ))
)]
struct ApiDoc;

// ── Tera context types (f64 — Tera can't render Decimal natively) ─────────────

#[derive(Serialize)]
struct PivotContext {
    exchanges: Vec<String>,
    rows: Vec<PivotRow>,
}

#[derive(Serialize)]
struct PivotRow {
    symbol: String,
    median_price: f64,
    spread: f64,
    cells: Vec<PivotCell>,
}

#[derive(Serialize)]
struct PivotCell {
    has_data: bool,
    price: f64,
    diff: f64,
    latency_ms: Option<f64>,
    is_stale: bool,
}

fn to_f64(d: Decimal) -> f64 {
    d.to_f64().unwrap_or(0.0)
}

fn build_pivot(snapshot: &SnapshotResponse) -> PivotContext {
    let mut exchanges: Vec<String> = snapshot
        .symbols
        .iter()
        .flat_map(|s| s.entries.iter().map(|e| e.exchange.clone()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    exchanges.sort();

    let rows = snapshot
        .symbols
        .iter()
        .map(|sym| {
            let cells = exchanges
                .iter()
                .map(|exch| {
                    if let Some(e) = sym.entries.iter().find(|e| &e.exchange == exch) {
                        PivotCell {
                            has_data: true,
                            price: to_f64(e.price),
                            diff: to_f64(e.diff_from_median),
                            latency_ms: e.latency_ms,
                            is_stale: e.is_stale,
                        }
                    } else {
                        PivotCell {
                            has_data: false,
                            price: 0.0,
                            diff: 0.0,
                            latency_ms: None,
                            is_stale: false,
                        }
                    }
                })
                .collect();

            let prices: Vec<Decimal> = sym.entries.iter().map(|e| e.price).collect();
            let spread = if prices.len() > 1 {
                prices.iter().copied().max().unwrap_or(Decimal::ZERO)
                    - prices.iter().copied().min().unwrap_or(Decimal::ZERO)
            } else {
                Decimal::ZERO
            };

            PivotRow {
                symbol: sym.symbol.clone(),
                median_price: to_f64(sym.median_price),
                spread: to_f64(spread),
                cells,
            }
        })
        .collect();

    PivotContext { exchanges, rows }
}

async fn index(State(state): State<AppState>) -> impl IntoResponse {
    let data = build_snapshot_response(&state.store, ALL_SYMBOLS);
    let pivot = build_pivot(&data);
    let mut ctx = tera::Context::new();
    ctx.insert("exchanges", &pivot.exchanges);
    ctx.insert("rows", &pivot.rows);

    match state.tera.render("index.html", &ctx) {
        Ok(html) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            html,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("template error: {e}"),
        )
            .into_response(),
    }
}

async fn events(
    State(store): State<SharedSnapshotStore>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let interval = tokio::time::interval(Duration::from_millis(500));
    let stream = tokio_stream::wrappers::IntervalStream::new(interval).map(move |_| {
        let data = build_snapshot_response(&store, ALL_SYMBOLS);
        let json = serde_json::to_string(&data).unwrap_or_default();
        Ok::<Event, Infallible>(Event::default().event("snapshot").data(json))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Server is healthy", body = HealthResponse)
    )
)]
async fn health(State(store): State<SharedSnapshotStore>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        has_data: store.has_any_data(),
    })
}

#[derive(Deserialize, utoipa::IntoParams)]
struct SnapshotQuery {
    /// Comma-separated list of symbols (e.g. BTCUSDT,ETHUSDT). Empty = all symbols.
    symbols: Option<String>,
}

/// Get current price snapshots for requested symbols
#[utoipa::path(
    get,
    path = "/snapshot",
    params(SnapshotQuery),
    responses(
        (status = 200, description = "Current snapshots", body = SnapshotResponse)
    )
)]
async fn snapshot(
    State(store): State<SharedSnapshotStore>,
    Query(query): Query<SnapshotQuery>,
) -> impl IntoResponse {
    let requested: Vec<Symbol> = query
        .symbols
        .unwrap_or_default()
        .split(',')
        .filter_map(|s| Symbol::from_str(s.trim()))
        .collect();

    let symbols = if requested.is_empty() {
        ALL_SYMBOLS.to_vec()
    } else {
        requested
    };

    let data = build_snapshot_response(&store, &symbols);
    (StatusCode::OK, Json(data))
}

/// Get status of all exchange connections
#[utoipa::path(
    get,
    path = "/exchanges",
    responses(
        (status = 200, description = "Exchange statuses", body = ExchangesResponse)
    )
)]
async fn exchanges(State(store): State<SharedSnapshotStore>) -> impl IntoResponse {
    let mut views: Vec<ExchangeStatusView> = store
        .get_all_exchange_statuses()
        .into_iter()
        .map(|(exchange, status)| ExchangeStatusView {
            exchange: exchange.as_str().to_string(),
            status,
        })
        .collect();
    views.sort_by(|a, b| a.exchange.cmp(&b.exchange));
    Json(ExchangesResponse { exchanges: views })
}

#[derive(Deserialize, utoipa::IntoParams)]
struct HistoryQuery {
    /// Symbol to get history for (e.g. BTCUSDT). Empty = all symbols.
    symbol: Option<String>,
    /// Max number of entries to return (default 100, max 10000)
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    100
}

/// Get historical price entries
#[utoipa::path(
    get,
    path = "/history",
    params(HistoryQuery),
    responses(
        (status = 200, description = "Price history", body = HistoryResponse),
        (status = 400, description = "Unknown symbol")
    )
)]
async fn history(
    State(store): State<SharedSnapshotStore>,
    Query(query): Query<HistoryQuery>,
) -> impl IntoResponse {
    let symbols: Vec<Symbol> = match &query.symbol {
        Some(s) => match Symbol::from_str(s) {
            Some(sym) => vec![sym],
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("unknown symbol: {s}")})),
                )
                    .into_response();
            }
        },
        None => ALL_SYMBOLS.to_vec(),
    };

    let limit = query.limit.clamp(1, 10_000);

    let mut all_entries: Vec<HistoryEntryDto> = symbols
        .iter()
        .flat_map(|&sym| store.get_history_for_symbol(sym, limit))
        .map(|e| {
            let latency_ms = e.latency_ms();
            HistoryEntryDto {
                exchange: e.exchange.as_str().to_string(),
                price: e.price,
                received_ts: e.received_ts,
                exchange_ts: e.exchange_ts,
                latency_ms,
            }
        })
        .collect();

    all_entries.sort_by(|a, b| b.received_ts.cmp(&a.received_ts));
    all_entries.truncate(limit);

    let symbol_label = query.symbol.unwrap_or_else(|| "ALL".to_string());

    (
        StatusCode::OK,
        Json(HistoryResponse {
            symbol: symbol_label,
            entries: all_entries,
        }),
    )
        .into_response()
}

pub fn build_snapshot_response(
    store: &SharedSnapshotStore,
    symbols: &[Symbol],
) -> SnapshotResponse {
    let snapshots = store.get_snapshots_for_symbols(symbols);

    let mut by_symbol: HashMap<String, Vec<_>> = HashMap::new();
    for snap in &snapshots {
        by_symbol
            .entry(snap.symbol.as_str().to_string())
            .or_default()
            .push(snap);
    }

    let mut symbol_views: Vec<SymbolView> = by_symbol
        .into_iter()
        .map(|(sym_name, snaps)| {
            let median_price = median(snaps.iter().map(|s| s.price).collect());
            let entries = snaps
                .into_iter()
                .map(|s| ExchangeEntry {
                    exchange: s.exchange.as_str().to_string(),
                    price: s.price,
                    latency_ms: s.latency_ms(),
                    diff_from_median: s.price - median_price,
                    is_stale: s.is_stale,
                })
                .collect();
            SymbolView {
                symbol: sym_name,
                entries,
                median_price,
            }
        })
        .collect();

    symbol_views.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    SnapshotResponse {
        symbols: symbol_views,
    }
}

fn median(mut prices: Vec<Decimal>) -> Decimal {
    if prices.is_empty() {
        return Decimal::ZERO;
    }
    prices.sort(); // Decimal implements Ord — no unwrap needed
    let mid = prices.len() / 2;
    if prices.len() % 2 == 0 {
        (prices[mid - 1] + prices[mid]) / Decimal::TWO
    } else {
        prices[mid]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use serde_json::Value;
    use tower::ServiceExt;

    use crate::domain::exchange::Exchange;
    use crate::domain::symbol::Symbol;
    use crate::state::snapshot_store::SnapshotStore;

    fn d(n: u64) -> Decimal {
        Decimal::from(n)
    }

    fn test_state() -> AppState {
        AppState {
            store: Arc::new(SnapshotStore::new(10_000, 100)),
            tera: Arc::new(Tera::default()),
        }
    }

    async fn call(app: Router, uri: &str) -> (axum::http::StatusCode, Value) {
        let resp = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn health_returns_ok_with_no_data() {
        let state = test_state();
        let app = router(state);
        let (status, json) = call(app, "/health").await;
        assert_eq!(status, 200);
        assert_eq!(json["status"], "ok");
        assert_eq!(json["has_data"], false);
    }

    #[tokio::test]
    async fn health_returns_has_data_true_when_store_has_snapshots() {
        let state = test_state();
        state
            .store
            .update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(50000), None);
        let app = router(state);
        let (status, json) = call(app, "/health").await;
        assert_eq!(status, 200);
        assert_eq!(json["has_data"], true);
    }

    #[tokio::test]
    async fn snapshot_returns_entries_for_requested_symbol() {
        let state = test_state();
        state
            .store
            .update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(50000), None);
        state
            .store
            .update_snapshot(Exchange::Mexc, Symbol::BtcUsdt, d(50100), None);
        let app = router(state);
        let (status, json) = call(app, "/snapshot?symbols=BTCUSDT").await;
        assert_eq!(status, 200);
        let symbols = json["symbols"].as_array().unwrap();
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0]["symbol"], "BTCUSDT");
        assert_eq!(symbols[0]["entries"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn snapshot_median_and_diff_are_correct() {
        let state = test_state();
        state
            .store
            .update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(50000), None);
        state
            .store
            .update_snapshot(Exchange::Mexc, Symbol::BtcUsdt, d(50100), None);
        let app = router(state);
        let (_, json) = call(app, "/snapshot?symbols=BTCUSDT").await;
        assert_eq!(json["symbols"][0]["median_price"], 50050.0);
        let diffs: Vec<f64> = json["symbols"][0]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["diff_from_median"].as_f64().unwrap())
            .collect();
        assert!(diffs.contains(&-50.0) || diffs.contains(&50.0));
    }

    #[tokio::test]
    async fn exchanges_returns_empty_when_no_status() {
        let state = test_state();
        let app = router(state);
        let (status, json) = call(app, "/exchanges").await;
        assert_eq!(status, 200);
        assert_eq!(json["exchanges"].as_array().unwrap().len(), 0);
    }
}
