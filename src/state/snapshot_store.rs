use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    price_snapshot::PriceSnapshot,
    symbol::Symbol,
};

pub type SharedSnapshotStore = Arc<SnapshotStore>;

// Pojedynczy wpis w historii — lżejszy niż PriceSnapshot (bez is_stale)
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub price: f64,
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
    // ring buffer per (Exchange, Symbol) — patrz SPEC roadmapa post-MVP
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
        price: f64,
        exchange_ts: Option<DateTime<Utc>>,
    ) {
        let snapshot = PriceSnapshot::new(exchange, symbol, price, exchange_ts);

        // zapis do ring buffer przed insert (klonujemy received_ts ze snapshotu)
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

    // Zwraca ostatnie `limit` wpisów dla danego symbolu, posortowane od najnowszego
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

    fn make_store() -> SnapshotStore {
        SnapshotStore::new(5000, 100)
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

    #[test]
    fn history_accumulates_entries() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 50000.0, None);
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 51000.0, None);
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 52000.0, None);

        let history = store.get_history_for_symbol(Symbol::BtcUsdt, 100);
        assert_eq!(history.len(), 3);
        // posortowane od najnowszego
        assert_eq!(history[0].price, 52000.0);
        assert_eq!(history[2].price, 50000.0);
    }

    #[test]
    fn history_respects_capacity_limit() {
        let store = SnapshotStore::new(5000, 3);
        for price in [1.0, 2.0, 3.0, 4.0, 5.0] {
            store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, price, None);
        }
        let history = store.get_history_for_symbol(Symbol::BtcUsdt, 100);
        assert_eq!(history.len(), 3);
        // najstarsze wypadły — zostały 3.0, 4.0, 5.0
        let prices: Vec<f64> = history.iter().map(|e| e.price).collect();
        assert!(prices.contains(&5.0));
        assert!(prices.contains(&4.0));
        assert!(prices.contains(&3.0));
        assert!(!prices.contains(&1.0));
    }

    #[test]
    fn history_limit_param_truncates_results() {
        let store = make_store();
        for price in [1.0, 2.0, 3.0, 4.0, 5.0] {
            store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, price, None);
        }
        let history = store.get_history_for_symbol(Symbol::BtcUsdt, 2);
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn history_merges_multiple_exchanges() {
        let store = make_store();
        store.update_snapshot(Exchange::Binance, Symbol::BtcUsdt, 50000.0, None);
        store.update_snapshot(Exchange::Mexc, Symbol::BtcUsdt, 50100.0, None);
        store.update_snapshot(Exchange::Kraken, Symbol::BtcUsdt, 50200.0, None);

        let history = store.get_history_for_symbol(Symbol::BtcUsdt, 100);
        assert_eq!(history.len(), 3);
    }
}
