use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{exchange::Exchange, symbol::Symbol};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

impl OrderBook {
    pub fn new(
        exchange: Exchange,
        symbol: Symbol,
        bids: Vec<OrderBookLevel>,
        asks: Vec<OrderBookLevel>,
    ) -> Self {
        Self {
            exchange,
            symbol,
            bids,
            asks,
            timestamp: Utc::now(),
        }
    }

    pub fn mid_price(&self) -> Option<Decimal> {
        let best_bid = self.bids.first()?.price;
        let best_ask = self.asks.first()?.price;
        Some((best_bid + best_ask) / Decimal::from(2))
    }

    pub fn merge(&mut self, update: OrderBook) {
        self.timestamp = update.timestamp;

        if !update.bids.is_empty() {
            merge_levels(&mut self.bids, update.bids, true);
        }
        if !update.asks.is_empty() {
            merge_levels(&mut self.asks, update.asks, false);
        }

        self.bids.truncate(20);
        self.asks.truncate(20);
    }

    pub fn estimate_buy_price(&self, volume: Decimal) -> Option<Decimal> {
        if volume.is_zero() {
            return self.asks.first().map(|l| l.price);
        }

        let mut remaining = volume;
        let mut total_cost = Decimal::ZERO;

        for level in &self.asks {
            let take = remaining.min(level.quantity);
            total_cost += take * level.price;
            remaining -= take;
            if remaining.is_zero() {
                break;
            }
        }

        if remaining.is_zero() {
            Some(total_cost / volume)
        } else {
            None
        }
    }

    pub fn estimate_sell_price(&self, volume: Decimal) -> Option<Decimal> {
        if volume.is_zero() {
            return self.bids.first().map(|l| l.price);
        }

        let mut remaining = volume;
        let mut total_cost = Decimal::ZERO;

        for level in &self.bids {
            let take = remaining.min(level.quantity);
            total_cost += take * level.price;
            remaining -= take;
            if remaining.is_zero() {
                break;
            }
        }

        if remaining.is_zero() {
            Some(total_cost / volume)
        } else {
            None
        }
    }
}

fn merge_levels(current: &mut Vec<OrderBookLevel>, updates: Vec<OrderBookLevel>, reverse: bool) {
    for up in updates {
        if let Some(existing) = current.iter_mut().find(|l| l.price == up.price) {
            existing.quantity = up.quantity;
        } else {
            current.push(up);
        }
    }

    // Remove zero quantity levels
    current.retain(|l| !l.quantity.is_zero());

    // Sort
    if reverse {
        current.sort_by(|a, b| b.price.cmp(&a.price));
    } else {
        current.sort_by(|a, b| a.price.cmp(&b.price));
    }
}
