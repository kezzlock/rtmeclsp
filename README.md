# rtmeclsp

**Real-Time Multi-Exchange Crypto Latency & Spread Monitor**

Live price dashboard that aggregates tickers from Binance, Kraken, MEXC and OKX, computes cross-exchange spread and median, and streams updates to the browser every 500 ms via SSE.

![Dashboard screenshot](docs/dashboard.png)

---

## Features

- **Live pivot table** — symbols as rows, exchanges as columns; price, diff from median, and latency per cell
- **Flash animations** — cells flash green/red on price change
- **Spread & median** — computed per symbol across all connected exchanges
- **Staleness detection** — cells fade when a feed goes silent (configurable threshold)
- **Exchange status** — tracks Connected / Disconnected / Reconnecting per exchange
- **Price history** — ring-buffer per (exchange, symbol), queryable via REST
- **Configurable** — symbols and exchanges chosen in `config.yaml`; no recompile needed

---

## Quick start

### Docker (recommended)

```bash
git clone https://github.com/kezzlock/rtmeclsp
cd rtmeclsp
docker compose up
```

Open **http://localhost:8080**

To change symbols or exchanges, edit `config.yaml` and restart the container.

### Build from source

```bash
cargo build --release
./target/release/rtmeclsp
```

Requires Rust 1.82+.

---

## Configuration

```yaml
# config.yaml
http:
  port: 8080

exchanges:
  enabled:
    - binance
    - mexc
    - kraken
    - okx

symbols:
  - BTCUSDT
  - ETHUSDT
  - SOLUSDT
  # … add any symbol supported by all enabled exchanges

websocket:
  reconnect_backoff_ms: 1000   # initial backoff; doubles on each retry, capped at 30 s
  reconnect_max_attempts: 10   # 0 = retry forever

store:
  stale_threshold_ms: 10000    # mark cell stale after this many ms of silence
  history_capacity: 1000       # ring-buffer depth per (exchange, symbol)
```

---

## REST API

| Endpoint | Description |
|---|---|
| `GET /` | Live dashboard (HTML) |
| `GET /health` | `{"status":"ok","has_data":bool}` |
| `GET /snapshot` | Current prices for all symbols (JSON) |
| `GET /snapshot?symbols=BTCUSDT,ETHUSDT` | Filtered snapshot |
| `GET /exchanges` | Exchange connection statuses |
| `GET /history?symbol=BTCUSDT&limit=100` | Price history (newest first) |
| `GET /events` | SSE stream — emits `snapshot` event every 500 ms |

### Snapshot response

```json
{
  "symbols": [
    {
      "symbol": "BTCUSDT",
      "median_price": 80845.26,
      "entries": [
        {
          "exchange": "binance",
          "price": 80848.21,
          "diff_from_median": 2.95,
          "latency_ms": 0.2,
          "is_stale": false
        }
      ]
    }
  ]
}
```

---

## Architecture

```
config.yaml
    │
    ▼
main.rs ──► tokio::spawn × N exchanges
                │
                ├── BinanceAdapter  ─┐
                ├── KrakenAdapter   ─┤─ WsAdapter trait ──► run_ws_feed()
                ├── OkxAdapter      ─┘
                └── mexc_client (REST polling, 1 req/s)
                         │
                         ▼
                  SharedSnapshotStore (DashMap)
                  + history ring-buffers (VecDeque)
                         │
                         ▼
                  axum HTTP server
                  ├── GET /snapshot, /history, /exchanges
                  └── GET /events  (SSE, 500 ms interval)
                             │
                             ▼
                       Browser (SSE + JS pivot table)
```

**Key design decisions:**

- `DashMap` instead of `RwLock<HashMap>` — lock-free concurrent reads from many WS tasks
- `WsAdapter` trait — adding a new exchange requires only a parser struct; reconnect/backoff logic is shared
- MEXC uses REST polling (their WebSocket requires authentication and is geographically blocked for public data)
- `f64` for prices — sufficient for display; `rust_decimal` is in `Cargo.toml` ready for trading logic migration

---

## Adding an exchange

1. Add a variant to `src/domain/exchange.rs`
2. Add symbol mappings to `src/domain/symbol.rs`
3. Create `src/infra/yourexchange_client.rs` implementing `WsAdapter`:

```rust
pub struct YourAdapter { symbols: Vec<Symbol>, subscribe_json: String }

impl WsAdapter for YourAdapter {
    fn exchange(&self) -> Exchange { Exchange::YourExchange }
    fn ws_url(&self) -> String { "wss://...".into() }
    fn subscribe_message(&self) -> Option<String> { Some(self.subscribe_json.clone()) }
    fn parse_message(&self, text: &str)
        -> Result<Vec<(Symbol, f64, Option<DateTime<Utc>>)>, String>
    {
        // parse exchange-specific JSON
    }
}

pub async fn run(store: SharedSnapshotStore, symbols: Vec<Symbol>, backoff: u64, max: u32) {
    run_ws_feed(YourAdapter::new(symbols), store, backoff, max).await;
}
```

4. Wire it up in `main.rs` and add to `config.yaml`.

---

## License

MIT
