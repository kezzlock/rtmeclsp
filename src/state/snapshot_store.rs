use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use metrics::{gauge, histogram};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::domain::{
    exchange::{Exchange, ExchangeStatus},
    order_book::OrderBook,
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
    order_books: DashMap<(Exchange, Symbol), OrderBook>,
    exchange_status: DashMap<Exchange, ExchangeStatus>,
    history: DashMap<(Exchange, Symbol), VecDeque<HistoryEntry>>,
    history_capacity: usize,
    stale_threshold_ms: u64,
}

impl SnapshotStore {
    pub fn new(stale_threshold_ms: u64, history_capacity: usize) -> Self {
        Self {
            snapshots: DashMap::new(),
            order_books: DashMap::new(),
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

        let price_f64 = price.to_f64().unwrap_or(0.0);
        gauge!("rtme_price", "exchange" => exchange.as_str(), "symbol" => symbol.as_str())
            .set(price_f64);

        if let Some(lat) = snapshot.latency_ms() {
            gauge!("rtme_latency_ms", "exchange" => exchange.as_str(), "symbol" => symbol.as_str())
                .set(lat);
            histogram!("rtme_latency_hist_ms", "exchange" => exchange.as_str(), "symbol" => symbol.as_str()).record(lat);
        }

        self.update_spread_metrics(symbol);

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

    pub fn update_order_book(&self, ob: OrderBook) {
        if let Some(best_bid) = ob.bids.first() {
            gauge!("rtme_best_bid", "exchange" => ob.exchange.as_str(), "symbol" => ob.symbol.as_str()).set(best_bid.price.to_f64().unwrap_or(0.0));
        }
        if let Some(best_ask) = ob.asks.first() {
            gauge!("rtme_best_ask", "exchange" => ob.exchange.as_str(), "symbol" => ob.symbol.as_str()).set(best_ask.price.to_f64().unwrap_or(0.0));
        }

        // Merge logic
        let mut entry = self.order_books.entry((ob.exchange, ob.symbol)).or_insert_with(|| ob.clone());
        if entry.timestamp < ob.timestamp {
            entry.merge(ob.clone());
        }
        let merged_ob = entry.value().clone();
        drop(entry);

        if let (Some(b), Some(a)) = (merged_ob.bids.first(), merged_ob.asks.first()) {
            let mid = (b.price + a.price) / Decimal::from(2);
            self.update_snapshot(merged_ob.exchange, merged_ob.symbol, mid, Some(merged_ob.timestamp));
        }

        self.order_books.insert((ob.exchange, ob.symbol), merged_ob);
    }

    pub fn get_order_book(&self, exchange: Exchange, symbol: Symbol) -> Option<OrderBook> {
        self.order_books
            .get(&(exchange, symbol))
            .map(|e| e.value().clone())
    }

    fn update_spread_metrics(&self, symbol: Symbol) {
        let snapshots: Vec<_> = self
            .snapshots
            .iter()
            .filter(|e| e.key().1 == symbol)
            .map(|e| e.value().clone())
            .collect();

        if snapshots.len() > 1 {
            let prices: Vec<Decimal> = snapshots.iter().map(|s| s.price).collect();
            let min = prices.iter().min().unwrap();
            let max = prices.iter().max().unwrap();
            let spread = (*max - *min).to_f64().unwrap_or(0.0);
            gauge!("rtme_spread", "symbol" => symbol.as_str()).set(spread);
        }
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
