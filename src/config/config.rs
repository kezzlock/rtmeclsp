use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub http: HttpConfig,
    pub exchanges: ExchangesConfig,
    pub symbols: Vec<String>,
    pub websocket: WebSocketConfig,
    pub store: StoreConfig,
    pub rpc: RpcConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExchangesConfig {
    pub enabled: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WebSocketConfig {
    pub reconnect_backoff_ms: u64,
    pub reconnect_max_attempts: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StoreConfig {
    pub stale_threshold_ms: u64,
    pub history_capacity: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RpcConfig {
    pub ethereum_url: String,
    pub solana_url: String,
}

impl AppConfig {
    pub fn load() -> Result<Self, config::ConfigError> {
        config::Config::builder()
            .add_source(config::File::with_name("config").required(false))
            .add_source(config::Environment::with_prefix("APP").separator("__"))
            .set_default("http.port", 8080)?
            .set_default("exchanges.enabled", vec!["binance", "mexc"])?
            .set_default("symbols", vec!["BTCUSDT", "ETHUSDT"])?
            .set_default("websocket.reconnect_backoff_ms", 1000)?
            .set_default("websocket.reconnect_max_attempts", 10)?
            .set_default("store.stale_threshold_ms", 10000)?
            .set_default("store.history_capacity", 1000)?
            .set_default("rpc.ethereum_url", "https://cloudflare-eth.com")?
            .set_default("rpc.solana_url", "https://api.mainnet-beta.solana.com")?
            .build()?
            .try_deserialize()
    }
}
