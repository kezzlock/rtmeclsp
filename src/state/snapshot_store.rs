use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    price_snapshot::PriceSnapshot,
    symbol::Symbol,
};

// DashMap zamiast RwLock<HashMap> — patrz SPEC sekcja 10.1
pub type SharedSnapshotStore = Arc<SnapshotStore>;

pub struct SnapshotStore {
    snapshots: DashMap<(Exchange, Symbol), PriceSnapshot>,
    exchange_status: DashMap<Exchange, ExchangeStatus>,
    stale_threshold_ms: u64,
}

impl SnapshotStore {
    pub fn new(stale_threshold_ms: u64) -> Self {
        Self {
            snapshots: DashMap::new(),
            exchange_status: DashMap::new(),
            stale_threshold_ms,
        }
    }

    pub fn update_snapshot(
        &self,
        exchange: Exchange,
        symbol: Symbol,
        price: f64,
        exchange_ts: Option<DateTime<Utc>>,
    ) {
        let snapshot = PriceSnapshot::new(exchange, symbol, price, exchange_ts);
        self.snapshots.insert((exchange, symbol), snapshot);
    }

    pub fn get_snapshots_for_symbols(&self, symbols: &[Symbol]) -> Vec<PriceSnapshot> {
        let now = Utc::now();
        let threshold = chrono::Duration::milliseconds(self.stale_threshold_ms as i64);

        self.snapshots
            .iter()
            .filter(|entry| symbols.contains(&entry.key().1))
            .map(|entry| {
                let mut snapshot = entry.value().clone();
                if now - snapshot.received_ts > threshold {
                    snapshot.is_stale = true;
                }
                snapshot
            })
            .collect()
    }

    pub fn get_all_snapshots(&self) -> Vec<PriceSnapshot> {
        let now = Utc::now();
        let threshold = chrono::Duration::milliseconds(self.stale_threshold_ms as i64);

        self.snapshots
            .iter()
            .map(|entry| {
                let mut snapshot = entry.value().clone();
                if now - snapshot.received_ts > threshold {
                    snapshot.is_stale = true;
                }
                snapshot
            })
            .collect()
    }

    pub fn update_exchange_status(&self, exchange: Exchange, status: ExchangeStatus) {
        self.exchange_status.insert(exchange, status);
    }

    pub fn get_all_exchange_statuses(&self) -> Vec<(Exchange, ExchangeStatus)> {
        self.exchange_status
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    pub fn has_any_data(&self) -> bool {
        !self.snapshots.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> SnapshotStore {
        SnapshotStore::new(5000)
    }

    #[test]
    fn update_and_get_snapshot() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 50000.0, None);

        let snapshots = store.get_snapshots_for_symbols(&[Symbol::BtcUsdt]);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].price, 50000.0);
        assert_eq!(snapshots[0].exchange, Exchange::Binance);
    }

    #[test]
    fn newer_snapshot_overwrites_older() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 50000.0, None);
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 51000.0, None);

        let snapshots = store.get_snapshots_for_symbols(&[Symbol::BtcUsdt]);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].price, 51000.0);
    }

    #[test]
    fn get_snapshots_filters_by_symbol() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 50000.0, None);
        store.update_snapshot(Exchange::Binance, Symbol::EthUsdt, 3000.0, None);

        let snapshots = store.get_snapshots_for_symbols(&[Symbol::EthUsdt]);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].symbol, Symbol::EthUsdt);
    }

    #[test]
    fn multiple_exchanges_same_symbol() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 50000.0, None);
        store.update_snapshot(Exchange::Mexc, Symbol::BtcUsdt, 50100.0, None);

        let snapshots = store.get_snapshots_for_symbols(&[Symbol::BtcUsdt]);
        assert_eq!(snapshots.len(), 2);
    }
}
