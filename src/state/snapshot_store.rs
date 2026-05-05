use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    price_snapshot::PriceSnapshot,
    symbol::Symbol,
};

pub type SharedSnapshotStore = Arc<SnapshotStore>;

#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub price: Decimal,
    pub received_ts: DateTime<Utc>,
    pub exchange_ts: Option<DateTime<Utc>>,
}

impl HistoryEntry {
    pub fn latency_ms(&self) -> Option<f64> {
        let exchange_ts = self.exchange_ts?;
        let diff = self.received_ts - exchange_ts;
        Some(diff.num_microseconds()? as f64 / 1000.0)
    }
}

pub struct SnapshotStore {
    snapshots: DashMap<(Exchange, Symbol), PriceSnapshot>,
    exchange_status: DashMap<Exchange, ExchangeStatus>,
    history: DashMap<(Exchange, Symbol), VecDeque<HistoryEntry>>,
    history_capacity: usize,
    stale_threshold_ms: u64,
}

impl SnapshotStore {
    pub fn new(stale_threshold_ms: u64, history_capacity: usize) -> Self {
        Self {
            snapshots: DashMap::new(),
            exchange_status: DashMap::new(),
            history: DashMap::new(),
            history_capacity,
            stale_threshold_ms,
        }
    }

    pub fn update_snapshot(
        &self,
        exchange: Exchange,
        symbol: Symbol,
        price: Decimal,
        exchange_ts: Option<DateTime<Utc>>,
    ) {
        let snapshot = PriceSnapshot::new(exchange, symbol, price, exchange_ts);

        let entry = HistoryEntry {
            exchange,
            symbol,
            price: snapshot.price,
            received_ts: snapshot.received_ts,
            exchange_ts: snapshot.exchange_ts,
        };
        let mut ring = self
            .history
            .entry((exchange, symbol))
            .or_insert_with(VecDeque::new);
        if ring.len() >= self.history_capacity {
            ring.pop_front();
        }
        ring.push_back(entry);
        drop(ring);

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

    pub fn get_history_for_symbol(&self, symbol: Symbol, limit: usize) -> Vec<HistoryEntry> {
        let mut entries: Vec<HistoryEntry> = self
            .history
            .iter()
            .filter(|e| e.key().1 == symbol)
            .flat_map(|e| e.value().iter().cloned().collect::<Vec<_>>())
            .collect();

        entries.sort_by(|a, b| b.received_ts.cmp(&a.received_ts));
        entries.truncate(limit);
        entries
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

    fn d(n: u64) -> Decimal {
        Decimal::from(n)
    }

    fn make_store() -> SnapshotStore {
        SnapshotStore::new(5000, 100)
    }

    #[test]
    fn update_and_get_snapshot() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(50000), None);

        let snapshots = store.get_snapshots_for_symbols(&[Symbol::BtcUsdt]);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].price, d(50000));
        assert_eq!(snapshots[0].exchange, Exchange::Binance);
    }

    #[test]
    fn newer_snapshot_overwrites_older() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(50000), None);
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(51000), None);

        let snapshots = store.get_snapshots_for_symbols(&[Symbol::BtcUsdt]);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].price, d(51000));
    }

    #[test]
    fn get_snapshots_filters_by_symbol() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(50000), None);
        store.update_snapshot(Exchange::Binance, Symbol::EthUsdt, d(3000), None);

        let snapshots = store.get_snapshots_for_symbols(&[Symbol::EthUsdt]);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].symbol, Symbol::EthUsdt);
    }

    #[test]
    fn multiple_exchanges_same_symbol() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(50000), None);
        store.update_snapshot(Exchange::Mexc, Symbol::BtcUsdt, d(50100), None);

        let snapshots = store.get_snapshots_for_symbols(&[Symbol::BtcUsdt]);
        assert_eq!(snapshots.len(), 2);
    }

    #[test]
    fn history_accumulates_entries() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(50000), None);
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(51000), None);
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(52000), None);

        let history = store.get_history_for_symbol(Symbol::BtcUsdt, 100);
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].price, d(52000));
        assert_eq!(history[2].price, d(50000));
    }

    #[test]
    fn history_respects_capacity_limit() {
        let store = SnapshotStore::new(5000, 3);
        for price in [1u64, 2, 3, 4, 5] {
            store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(price), None);
        }
        let history = store.get_history_for_symbol(Symbol::BtcUsdt, 100);
        assert_eq!(history.len(), 3);
        let prices: Vec<Decimal> = history.iter().map(|e| e.price).collect();
        assert!(prices.contains(&d(5)));
        assert!(prices.contains(&d(4)));
        assert!(prices.contains(&d(3)));
        assert!(!prices.contains(&d(1)));
    }

    #[test]
    fn history_limit_param_truncates_results() {
        let store = make_store();
        for price in [1u64, 2, 3, 4, 5] {
            store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(price), None);
        }
        let history = store.get_history_for_symbol(Symbol::BtcUsdt, 2);
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn history_merges_multiple_exchanges() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, d(50000), None);
        store.update_snapshot(Exchange::Mexc, Symbol::BtcUsdt, d(50100), None);
        store.update_snapshot(Exchange::Kraken, Symbol::BtcUsdt, d(50200), None);

        let history = store.get_history_for_symbol(Symbol::BtcUsdt, 100);
        assert_eq!(history.len(), 3);
    }
}
