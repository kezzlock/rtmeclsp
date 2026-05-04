use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use std::collections::HashMap;

use crate::{
    api::dto::{
        ExchangeEntry, ExchangeStatusView, ExchangesResponse, HealthResponse, SnapshotResponse,
        SymbolView,
    },
    domain::symbol::Symbol,
    state::snapshot_store::SharedSnapshotStore,
};

pub fn router(store: SharedSnapshotStore) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/snapshot", get(snapshot))
        .route("/exchanges", get(exchanges))
        .with_state(store)
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

    let symbols_to_fetch = if requested.is_empty() {
        crate::domain::symbol::ALL_SYMBOLS.to_vec()
    } else {
        requested
    };

    let snapshots = store.get_snapshots_for_symbols(&symbols_to_fetch);

    // Grupowanie per symbol
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

    (StatusCode::OK, Json(SnapshotResponse { symbols: symbol_views }))
}

async fn exchanges(State(store): State<SharedSnapshotStore>) -> impl IntoResponse {
    let statuses = store.get_all_exchange_statuses();
    let mut views: Vec<ExchangeStatusView> = statuses
        .into_iter()
        .map(|(exchange, status)| ExchangeStatusView {
            exchange: exchange.as_str().to_string(),
            status,
        })
        .collect();

    views.sort_by(|a, b| a.exchange.cmp(&b.exchange));

    Json(ExchangesResponse { exchanges: views })
}

fn median(mut prices: Vec<f64>) -> f64 {
    if prices.is_empty() {
        return 0.0;
    }
    prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = prices.len() / 2;
    if prices.len() % 2 == 0 {
        (prices[mid - 1] + prices[mid]) / 2.0
    } else {
        prices[mid]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use serde_json::Value;
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::state::snapshot_store::SnapshotStore;
    use crate::domain::exchange::Exchange;
    use crate::domain::symbol::Symbol;

    fn test_store() -> SharedSnapshotStore {
        Arc::new(SnapshotStore::new(10_000))
    }

    async fn call(app: Router, uri: &str) -> (axum::http::StatusCode, Value) {
        let response = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        (status, json)
    }

    #[tokio::test]
    async fn health_returns_ok_with_no_data() {
        let store = test_store();
        let app = router(store);
        let (status, json) = call(app, "/health").await;
        assert_eq!(status, 200);
        assert_eq!(json["status"], "ok");
        assert_eq!(json["has_data"], false);
    }

    #[tokio::test]
    async fn health_returns_has_data_true_when_store_has_snapshots() {
        let store = test_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 50000.0, None);
        let app = router(store);
        let (status, json) = call(app, "/health").await;
        assert_eq!(status, 200);
        assert_eq!(json["has_data"], true);
    }

    #[tokio::test]
    async fn snapshot_returns_entries_for_requested_symbol() {
        let store = test_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 50000.0, None);
        store.update_snapshot(Exchange::Mexc, Symbol::BtcUsdt, 50100.0, None);
        let app = router(store);
        let (status, json) = call(app, "/snapshot?symbols=BTCUSDT").await;
        assert_eq!(status, 200);
        let symbols = json["symbols"].as_array().unwrap();
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0]["symbol"], "BTCUSDT");
        let entries = symbols[0]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn snapshot_median_and_diff_are_correct() {
        let store = test_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 50000.0, None);
        store.update_snapshot(Exchange::Mexc, Symbol::BtcUsdt, 50100.0, None);
        let app = router(store);
        let (_, json) = call(app, "/snapshot?symbols=BTCUSDT").await;
        let sym = &json["symbols"][0];
        // mediana z [50000, 50100] = 50050
        assert_eq!(sym["median_price"], 50050.0);
        // diff_from_median dla każdej giełdy
        let entries = sym["entries"].as_array().unwrap();
        let diffs: Vec<f64> = entries
            .iter()
            .map(|e| e["diff_from_median"].as_f64().unwrap())
            .collect();
        assert!(diffs.contains(&-50.0) || diffs.contains(&50.0));
    }

    #[tokio::test]
    async fn exchanges_returns_empty_when_no_status() {
        let store = test_store();
        let app = router(store);
        let (status, json) = call(app, "/exchanges").await;
        assert_eq!(status, 200);
        assert_eq!(json["exchanges"].as_array().unwrap().len(), 0);
    }
}
