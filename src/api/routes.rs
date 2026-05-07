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
use metrics_exporter_prometheus::PrometheusHandle;
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
    domain::symbol::{ALL_SYMBOLS, Symbol},
    state::snapshot_store::SharedSnapshotStore,
};

#[derive(Clone)]
pub struct AppState {
    pub store: SharedSnapshotStore,
    pub tera: Arc<Tera>,
    pub prometheus_handle: PrometheusHandle,
    pub enabled_exchanges: Vec<String>,
}

impl FromRef<AppState> for SharedSnapshotStore {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.store)
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/snapshot", get(snapshot))
        .route("/exchanges", get(exchanges))
        .route("/history", get(history))
        .route("/metrics", get(metrics_handler))
        .route("/events", get(events))
        .with_state(state)
}

#[derive(Serialize)]
struct PivotContext {
    exchanges: Vec<String>,
    rows: Vec<PivotRow>,
    volume: f64,
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
    est_buy: Option<f64>,
    est_sell: Option<f64>,
    diff: f64,
    latency_ms: Option<f64>,
    is_stale: bool,
}

fn to_f64(d: Decimal) -> f64 {
    d.to_f64().unwrap_or(0.0)
}

fn build_pivot(snapshot: &SnapshotResponse, enabled: Option<&[String]>) -> PivotContext {
    let mut exchanges: Vec<String> = if let Some(e) = enabled {
        e.to_vec()
    } else {
        snapshot
            .symbols
            .iter()
            .flat_map(|s| s.entries.iter().map(|e| e.exchange.clone()))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    };
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
                            price: e.price,
                            est_buy: e.est_buy_price,
                            est_sell: e.est_sell_price,
                            diff: e.diff_from_median,
                            latency_ms: e.latency_ms,
                            is_stale: e.is_stale,
                        }
                    } else {
                        PivotCell {
                            has_data: false,
                            price: 0.0,
                            est_buy: None,
                            est_sell: None,
                            diff: 0.0,
                            latency_ms: None,
                            is_stale: false,
                        }
                    }
                })
                .collect();

            let prices: Vec<f64> = sym.entries.iter().map(|e| e.price).collect();
            let spread = if prices.len() > 1 {
                prices.iter().copied().fold(f64::MIN, f64::max)
                    - prices.iter().copied().fold(f64::MAX, f64::min)
            } else {
                0.0
            };

            PivotRow {
                symbol: sym.symbol.clone(),
                median_price: sym.median_price,
                spread,
                cells,
            }
        })
        .collect();

    PivotContext {
        exchanges,
        rows,
        volume: snapshot.volume,
    }
}

async fn index(State(state): State<AppState>) -> impl IntoResponse {
    let data = build_snapshot_response(&state.store, ALL_SYMBOLS, Decimal::ZERO);
    let pivot = build_pivot(&data, Some(&state.enabled_exchanges));
    let mut ctx = tera::Context::new();
    ctx.insert("exchanges", &pivot.exchanges);
    ctx.insert("rows", &pivot.rows);
    ctx.insert("volume", &pivot.volume);

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
    Query(query): Query<SnapshotQuery>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let interval = tokio::time::interval(Duration::from_millis(500));
    let volume = Decimal::from_f64_retain(query.volume.unwrap_or(0.0)).unwrap_or(Decimal::ZERO);

    let stream = tokio_stream::wrappers::IntervalStream::new(interval).map(move |_| {
        let data = build_snapshot_response(&store, ALL_SYMBOLS, volume);
        let json = serde_json::to_string(&data).unwrap_or_default();
        Ok::<Event, Infallible>(Event::default().event("snapshot").data(json))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn health(State(store): State<SharedSnapshotStore>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        has_data: store.has_any_data(),
    })
}

#[derive(Deserialize)]
struct SnapshotQuery {
    symbols: Option<String>,
    volume: Option<f64>,
}

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

    let volume = Decimal::from_f64_retain(query.volume.unwrap_or(0.0)).unwrap_or(Decimal::ZERO);

    let data = build_snapshot_response(&store, &symbols, volume);
    (StatusCode::OK, Json(data))
}

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

#[derive(Deserialize)]
struct HistoryQuery {
    symbol: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    100
}

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
        .map(|e| HistoryEntryDto {
            exchange: e.exchange.as_str().to_string(),
            price: to_f64(e.price),
            received_ts: e.received_ts,
            exchange_ts: e.exchange_ts,
            latency_ms: e.latency_ms(),
        })
        .collect();

    all_entries.sort_by_key(|b| std::cmp::Reverse(b.received_ts));
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

async fn metrics_handler(State(state): State<AppState>) -> String {
    state.prometheus_handle.render()
}

pub fn build_snapshot_response(
    store: &SharedSnapshotStore,
    symbols: &[Symbol],
    volume: Decimal,
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
            let symbol_enum = Symbol::from_str(&sym_name).unwrap();
            let median_price_dec = median(snaps.iter().map(|s| s.price).collect());
            let entries = snaps
                .into_iter()
                .map(|s| {
                    let mut est_buy = None;
                    let mut est_sell = None;

                    // ZAWSZE próbujemy pobrać Bid/Ask z arkusza, nawet dla wolumenu 0
                    if let Some(ob) = store.get_order_book(s.exchange, symbol_enum) {
                        est_buy = ob.estimate_buy_price(volume).map(to_f64);
                        est_sell = ob.estimate_sell_price(volume).map(to_f64);
                    }

                    ExchangeEntry {
                        exchange: s.exchange.as_str().to_string(),
                        price: to_f64(s.price),
                        est_buy_price: est_buy,
                        est_sell_price: est_sell,
                        latency_ms: s.latency_ms(),
                        diff_from_median: to_f64(s.price - median_price_dec),
                        is_stale: s.is_stale,
                    }
                })
                .collect();
            SymbolView {
                symbol: sym_name,
                entries,
                median_price: to_f64(median_price_dec),
            }
        })
        .collect();

    symbol_views.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    SnapshotResponse {
        volume: to_f64(volume),
        symbols: symbol_views,
    }
}

fn median(mut prices: Vec<Decimal>) -> Decimal {
    if prices.is_empty() {
        return Decimal::ZERO;
    }
    prices.sort();
    let mid = prices.len() / 2;
    if prices.len().is_multiple_of(2) {
        (prices[mid - 1] + prices[mid]) / Decimal::from(2)
    } else {
        prices[mid]
    }
}
