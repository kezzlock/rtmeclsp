use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

    pub fn mexc_symbol(&self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "BTC_USDT",
            Symbol::EthUsdt => "ETH_USDT",
            Symbol::BnbUsdt => "BNB_USDT",
            Symbol::SolUsdt => "SOL_USDT",
            Symbol::XrpUsdt => "XRP_USDT",
            Symbol::AdaUsdt => "ADA_USDT",
            Symbol::DogeUsdt => "DOGE_USDT",
            Symbol::AvaxUsdt => "AVAX_USDT",
            Symbol::LinkUsdt => "LINK_USDT",
            Symbol::DotUsdt => "DOT_USDT",
            Symbol::LtcUsdt => "LTC_USDT",
            Symbol::UniUsdt => "UNI_USDT",
            Symbol::AtomUsdt => "ATOM_USDT",
            Symbol::TrxUsdt => "TRX_USDT",
            Symbol::NearUsdt => "NEAR_USDT",
        }
    }

    pub fn uniswap_v3_info(&self) -> Option<(&'static str, &'static str, u32)> {
        match self {
            // WBTC / USDT - 0.3% fee
            Symbol::BtcUsdt => Some(("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", "0xdAC17F958D2ee523a2206206994597C13D831ec7", 3000)),
            // WETH / USDT - 0.05% fee (most liquid)
            Symbol::EthUsdt => Some(("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "0xdAC17F958D2ee523a2206206994597C13D831ec7", 500)),
            // UNI / USDT - 0.3% fee
            Symbol::UniUsdt => Some(("0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984", "0xdAC17F958D2ee523a2206206994597C13D831ec7", 3000)),
            _ => None,
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
