mod api;
mod config;
mod domain;
mod infra;
mod state;

use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    api::routes::AppState, config::config::AppConfig, domain::symbol::Symbol,
    state::snapshot_store::SnapshotStore,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let prometheus_builder = PrometheusBuilder::new();
    let prometheus_handle = prometheus_builder
        .install_recorder()
        .expect("failed to install prometheus recorder");

    let cfg = AppConfig::load().expect("failed to load config");
    info!(
        "config loaded: port={}, exchanges={:?}",
        cfg.http.port, cfg.exchanges.enabled
    );

    let store = Arc::new(SnapshotStore::new(
        cfg.store.stale_threshold_ms,
        cfg.store.history_capacity,
    ));

    let symbols: Vec<Symbol> = cfg
        .symbols
        .iter()
        .filter_map(|s| Symbol::from_str(s))
        .collect();

    if symbols.is_empty() {
        tracing::error!("no valid symbols in config — check the 'symbols' list in config.yaml");
        std::process::exit(1);
    }

    for exchange_id in &cfg.exchanges.enabled {
        let store_clone = Arc::clone(&store);
        let symbols_clone = symbols.clone();
        let backoff = cfg.websocket.reconnect_backoff_ms;
        let max_attempts = cfg.websocket.reconnect_max_attempts;

        match exchange_id.as_str() {
            "binance" => {
                tokio::spawn(async move {
                    infra::binance_client::run(store_clone, symbols_clone, backoff, max_attempts)
                        .await;
                });
            }
            "mexc" => {
                tokio::spawn(async move {
                    infra::mexc_client::run(store_clone, symbols_clone, backoff, max_attempts)
                        .await;
                });
            }
            "kraken" => {
                tokio::spawn(async move {
                    infra::kraken_client::run(store_clone, symbols_clone, backoff, max_attempts)
                        .await;
                });
            }
            "okx" => {
                tokio::spawn(async move {
                    infra::okx_client::run(store_clone, symbols_clone, backoff, max_attempts).await;
                });
            }
            "uniswap" => {
                let rpc_url = cfg.rpc.ethereum_url.clone();
                tokio::spawn(async move {
                    infra::uniswap_client::run(store_clone, symbols_clone, rpc_url).await;
                });
            }
            "raydium" => {
                let rpc_url = cfg.rpc.solana_url.clone();
                tokio::spawn(async move {
                    infra::raydium_client::run(store_clone, symbols_clone, rpc_url).await;
                });
            }
            other => {
                tracing::warn!("unknown exchange in config: {other}, skipping");
            }
        }
    }

    let tera = tera::Tera::new("templates/**/*.html").expect("failed to load templates");

    let app_state = AppState {
        store: Arc::clone(&store),
        tera: Arc::new(tera),
        prometheus_handle,
    };

    let router = api::routes::router(app_state);
    let addr = format!("0.0.0.0:{}", cfg.http.port);
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");
    info!("listening on {addr}");

    axum::serve(listener, router).await.expect("server error");
}
