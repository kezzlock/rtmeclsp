use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum Symbol {
    BtcUsdt,
    EthUsdt,
}

impl Symbol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "BTCUSDT",
            Symbol::EthUsdt => "ETHUSDT",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "BTCUSDT" => Some(Symbol::BtcUsdt),
            "ETHUSDT" => Some(Symbol::EthUsdt),
            _ => None,
        }
    }

    pub fn binance_stream(&self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "btcusdt@ticker",
            Symbol::EthUsdt => "ethusdt@ticker",
        }
    }

    // MEXC używa podkreślnika zamiast sklejonych liter
    pub fn mexc_symbol(&self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "BTC_USDT",
            Symbol::EthUsdt => "ETH_USDT",
        }
    }
}

pub const ALL_SYMBOLS: &[Symbol] = &[Symbol::BtcUsdt, Symbol::EthUsdt];
