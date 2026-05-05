use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum Symbol {
    BtcUsdt,
    EthUsdt,
    BnbUsdt,
    SolUsdt,
    XrpUsdt,
    AdaUsdt,
    DogeUsdt,
    AvaxUsdt,
    LinkUsdt,
    DotUsdt,
    LtcUsdt,
    UniUsdt,
    AtomUsdt,
    TrxUsdt,
    NearUsdt,
}

impl Symbol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "BTCUSDT",
            Symbol::EthUsdt => "ETHUSDT",
            Symbol::BnbUsdt => "BNBUSDT",
            Symbol::SolUsdt => "SOLUSDT",
            Symbol::XrpUsdt => "XRPUSDT",
            Symbol::AdaUsdt => "ADAUSDT",
            Symbol::DogeUsdt => "DOGEUSDT",
            Symbol::AvaxUsdt => "AVAXUSDT",
            Symbol::LinkUsdt => "LINKUSDT",
            Symbol::DotUsdt => "DOTUSDT",
            Symbol::LtcUsdt => "LTCUSDT",
            Symbol::UniUsdt => "UNIUSDT",
            Symbol::AtomUsdt => "ATOMUSDT",
            Symbol::TrxUsdt => "TRXUSDT",
            Symbol::NearUsdt => "NEARUSDT",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        let upper = s.to_uppercase();
        ALL_SYMBOLS
            .iter()
            .copied()
            .find(|sym| sym.as_str() == upper.as_str())
    }

    pub fn binance_stream(&self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "btcusdt@ticker",
            Symbol::EthUsdt => "ethusdt@ticker",
            Symbol::BnbUsdt => "bnbusdt@ticker",
            Symbol::SolUsdt => "solusdt@ticker",
            Symbol::XrpUsdt => "xrpusdt@ticker",
            Symbol::AdaUsdt => "adausdt@ticker",
            Symbol::DogeUsdt => "dogeusdt@ticker",
            Symbol::AvaxUsdt => "avaxusdt@ticker",
            Symbol::LinkUsdt => "linkusdt@ticker",
            Symbol::DotUsdt => "dotusdt@ticker",
            Symbol::LtcUsdt => "ltcusdt@ticker",
            Symbol::UniUsdt => "uniusdt@ticker",
            Symbol::AtomUsdt => "atomusdt@ticker",
            Symbol::TrxUsdt => "trxusdt@ticker",
            Symbol::NearUsdt => "nearusdt@ticker",
        }
    }

    pub fn mexc_rest_symbol(&self) -> &'static str {
        self.as_str()
    }

    pub fn kraken_symbol(&self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "BTC/USD",
            Symbol::EthUsdt => "ETH/USD",
            Symbol::BnbUsdt => "BNB/USD",
            Symbol::SolUsdt => "SOL/USD",
            Symbol::XrpUsdt => "XRP/USD",
            Symbol::AdaUsdt => "ADA/USD",
            Symbol::DogeUsdt => "DOGE/USD",
            Symbol::AvaxUsdt => "AVAX/USD",
            Symbol::LinkUsdt => "LINK/USD",
            Symbol::DotUsdt => "DOT/USD",
            Symbol::LtcUsdt => "LTC/USD",
            Symbol::UniUsdt => "UNI/USD",
            Symbol::AtomUsdt => "ATOM/USD",
            Symbol::TrxUsdt => "TRX/USD",
            Symbol::NearUsdt => "NEAR/USD",
        }
    }

    pub fn okx_symbol(&self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "BTC-USDT",
            Symbol::EthUsdt => "ETH-USDT",
            Symbol::BnbUsdt => "BNB-USDT",
            Symbol::SolUsdt => "SOL-USDT",
            Symbol::XrpUsdt => "XRP-USDT",
            Symbol::AdaUsdt => "ADA-USDT",
            Symbol::DogeUsdt => "DOGE-USDT",
            Symbol::AvaxUsdt => "AVAX-USDT",
            Symbol::LinkUsdt => "LINK-USDT",
            Symbol::DotUsdt => "DOT-USDT",
            Symbol::LtcUsdt => "LTC-USDT",
            Symbol::UniUsdt => "UNI-USDT",
            Symbol::AtomUsdt => "ATOM-USDT",
            Symbol::TrxUsdt => "TRX-USDT",
            Symbol::NearUsdt => "NEAR-USDT",
        }
    }
}

pub const ALL_SYMBOLS: &[Symbol] = &[
    Symbol::BtcUsdt,
    Symbol::EthUsdt,
    Symbol::BnbUsdt,
    Symbol::SolUsdt,
    Symbol::XrpUsdt,
    Symbol::AdaUsdt,
    Symbol::DogeUsdt,
    Symbol::AvaxUsdt,
    Symbol::LinkUsdt,
    Symbol::DotUsdt,
    Symbol::LtcUsdt,
    Symbol::UniUsdt,
    Symbol::AtomUsdt,
    Symbol::TrxUsdt,
    Symbol::NearUsdt,
];
