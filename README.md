# flow-trades

Self-hosted Solana swap API. Direct AMM execution across 21 pool types (20 unique DEX programs + Pumpup AMM/bonding split) powered by Yellowstone Geyser gRPC. **40µs quotes, zero RPC on hot path, all swaps routed through on-chain fee program.**

```
GET  /quote              Best swap route + expected output
POST /swap               Unsigned versioned transaction (base64)
GET  /health             Server status, pool counts, stream stats
GET  /metrics            Prometheus metrics
GET  /program-id-to-label  DEX program ID → human label
WS   /quote-ws           Streaming quote updates
WS   /swap-stream        Live swaps (every confirmed DEX swap, with native + USD prices)
```

---

## Architecture

```
                      ┌──────────────────────────────────────────────────────────────┐
                      │                      flow-trades                              │
                      │                    (single binary)                            │
                      │                                                              │
  GET /quote ────────►│  ┌──────────┐  ┌──────────┐  ┌────────────────┐            │
  POST /swap ────────►│  │  Axum    │  │  Quote   │  │  TX Builder    │            │
  GET /health ───────►│  │  API     │─►│  Engine  │─►│  + Router CPI  │            │
  WS /quote-ws ──────►│  │  :8080   │  │ (40µs)   │  │  + ALTs        │            │
                      │  └──────────┘  └────┬─────┘  └────────────────┘            │
                      │                     │                                        │
                      │        ┌────────────┴────────────┐                          │
                      │        ▼                         ▼                          │
                      │  ┌──────────┐    ┌──────────────────────────────┐           │
                      │  │ Registry │    │       Account Mirror         │           │
                      │  │ (DashMap) │    │                              │           │
                      │  │ mint-pair│    │  Pool State    Vault Balances │           │
                      │  │  index   │    │  (DashMap)     (DashMap)      │           │
                      │  │          │    │                              │           │
                      │  └─────┬────┘    │  Companion   Mint Program   │           │
                      │        │         │  Cache       Cache          │           │
                      │  ┌─────┴──────┐  │                              │           │
                      │  │  SQLite    │  └────────────┬───────────────┘           │
                      │  │ (pools.db) │               │                            │
                      │  └────────────┘               │                            │
                      │                               │                            │
                      │  ┌────────────────────────────┴────────────────────────┐   │
                      │  │            Geyser gRPC (Yellowstone)                 │   │
                      │  │           Bidirectional subscribe                    │   │
                      │  │                                                     │   │
                      │  │  Filter 1: 21 DEX program owners                    │   │
                      │  │    → pool state updates + inline discovery           │   │
                      │  │                                                     │   │
                      │  │  Filter 2: vault + companion + pool accounts        │   │
                      │  │    → balance updates, dynamically subscribed        │   │
                      │  │                                                     │   │
                      │  │  Filter 3: blocks with DEX transactions             │   │
                      │  │    → pool discovery from inner instructions          │   │
                      │  │    → vault balances from post_token_balances         │   │
                      │  │                                                     │   │
                      │  │  Blockhash: get_latest_blockhash() every 400ms      │   │
                      │  └─────────────────────────────┬───────────────────────┘   │
                      └────────────────────────────────┼───────────────────────────┘
                                                       │
                                            ┌──────────▼──────────┐
                                            │  Yellowstone Geyser  │
                                            │  (gRPC, port 10000)  │
                                            └──────────┬──────────┘
                                                       │
                                            ┌──────────▼──────────┐
                                            │   Solana Validator   │
                                            └─────────────────────┘
```

### Data Flow

1. **Account stream** (Filter 1) pushes pool state changes for 21 DEX programs (incl. OnChain Labs DEX V2 for inner-instruction discovery). 14 sync-parseable types are deserialized inline from raw bytes. Async types (Raydium V4, PumpFun, PumpFun AMM, Meteora Standard, Meteora DBC, Pumpup bonding) use cached companion data from the mirror or a separate RPC for first-time mint discovery.
2. **Block stream** (Filter 3) delivers full blocks with all DEX transactions. Pool candidates extracted from top-level + inner instructions (CPI routes). Pools registered immediately from block data. `post_token_balances` parsed for vault balance updates — same-slot freshness.
3. **Vault balances** fed by three channels: (a) Geyser account subscription, (b) block-stream `post_token_balances`, (c) RPC seed on first cache miss. After first access, all subsequent reads are in-memory.
4. **Companion accounts** (Serum markets, PumpFun global, Meteora vault configs) are fetched once via RPC on first encounter, then subscribed in Geyser for real-time updates.
5. **Quote engine** evaluates all matching pools in parallel. Reads pool state + vault balances from the Account Mirror. Zero RPC after initial seeding.
6. **TX builder** assembles instructions, wraps in flow-router CPI (on-chain fee enforcement), builds v0 versioned transaction. Blockhash from Geyser gRPC cache.

### RPC Call Profile

In steady state, **zero RPC calls** for `/quote` and `/swap`. The only RPC calls are:

| Operation | When | Frequency |
|-----------|------|-----------|
| Mint token program lookup | First `/swap` per unique mint | Once, cached forever |
| Companion data fetch | First encounter per async pool | Once, then Geyser-streamed |
| Transaction simulation | `/swap` with `simulate: true` | User opt-in only |

---

## Supported DEXes (21 pool types)

| # | DEX | Type |
|---|-----|------|
| 1 | Raydium V4 | Legacy AMM |
| 2 | Raydium CPMM | Constant Product |
| 3 | Raydium CLMM | Concentrated Liquidity |
| 4 | Raydium LP | StableSwap |
| 5 | PumpFun | Bonding Curve |
| 6 | PumpFun AMM | Graduated AMM |
| 7 | Orca Whirlpool | Concentrated Liquidity |
| 8 | Meteora Standard | Constant Product |
| 9 | Meteora DLMM | Dynamic LMM |
| 10 | Meteora DAMM v2 | Dynamic AMM |
| 11 | Meteora DBC | Dynamic Bonding Curve |
| 12 | FluxBeam | SPL Token Swap fork |
| 13 | DefiTuna Fusion | CLMM wrapper |
| 14 | DefiTuna Pools | Position manager |
| 15 | Saros | SPL Token Swap fork |
| 16 | Dooar | SPL Token Swap fork |
| 17 | PancakeSwap | CLMM (Raydium fork) |
| 18 | FlashTrade | Custom |
| 19 | Byreal | CLMM |
| 20 | Pumpup AMM | Constant Product (post-graduation) |
| 21 | Pumpup Bonding | Bonding Curve (pre-graduation, native SOL) |

Plus **OnChain Labs DEX V2** (`proVF4...`) — aggregator router into 80+
private MM venues. Discovery-only: pool addresses behind it are picked
up by the block scanner and registered against their underlying
DEXes; we do not quote or execute against the aggregator program
itself.

---

## Quick Start

```toml
# config.toml
rpc_url = "https://your-solana-rpc.com"

[streaming]
geyser_endpoint = "http://your-geyser-node:10000"
# geyser_token = "your-auth-token"  # if remote provider requires it
```

```bash
export OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu
export OPENSSL_INCLUDE_DIR=/usr/include
cargo build --release
./target/release/flow-trades
```

Server starts on `127.0.0.1:8080`. Pools discovered automatically from Geyser, persisted to SQLite.

```bash
# Get a quote
curl "http://localhost:8080/quote?input=So11111111111111111111111111111111111111112&output=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&amount=1000000000"

# Check health
curl http://localhost:8080/health
```

---

## API

### `GET /quote`

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `input` | string | Yes | -- | Input token mint (base58) |
| `output` | string | Yes | -- | Output token mint (base58) |
| `amount` | string | Yes | -- | Raw amount in smallest units |
| `slippage` | u16 | No | `50` | Slippage tolerance in bps |
| `direct_only` | bool | No | `true` | `false` enables multi-hop routing |
| `exclude` | string | No | -- | DEX labels to exclude (comma-separated) |
| `dexes` | string | No | -- | DEX labels to whitelist (comma-separated) |
| `mode` | string | No | `ExactIn` | `ExactIn` or `ExactOut` |

### `POST /swap`

All swaps are routed through the on-chain flow-router program for fee enforcement.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `wallet` | string | Yes | -- | Signer public key (base58) |
| `quote` | object | Yes | -- | Full quote response from `/quote` |
| `priority_fee` | u64 | No | `5000` | Priority fee in lamports |
| `compute_limit` | u32 | No | auto | Compute unit limit (auto: 400K/600K/800K by hops) |
| `simulate` | bool | No | `false` | Simulate first for CU estimation (1 RPC call) |
| `dynamic_cu` | bool | No | `false` | Same as simulate — estimate CU from simulation |
| `tip` | object | No | -- | `{ "address": "base58", "lamports": u64 }` for Jito tips |

All swaps routed through the on-chain router via generic N-hop `wrap_swap()`. Supports 1 to 5 hops — account layout scales automatically with intermediate token accounts.

### `GET /health`

Returns JSON with server status, pool counts, stream stats, blockhash age.

### `GET /metrics`

Prometheus exposition format. Gauges for cache/registry/SQLite sizes, counters for quotes served, stream updates, stream errors, scanner discoveries.

### `WS /quote-ws`

WebSocket endpoint for push-based quote streaming. Client sends a subscription message with input/output/amount, server pushes quote updates at a configurable interval (min 100ms).

### `WS /swap-stream`

Live broadcast of every confirmed DEX swap parsed off the block stream, enriched with native price and USD price. Distinct from `/swap` (which builds an unsigned tx) and `/quote-ws` (which streams projected prices for a chosen pair).

Protocol:

1. Client connects to `/swap-stream`.
2. (Optional) Client sends a JSON filter on the first frame:
   ```json
   {
     "type": "subscribe",
     "filter": {
       "dex": ["Raydium V4", "Pumpup Bonding"],   // optional
       "mint": "9U3F…",                            // optional, matches input or output
       "pool": "AQxK…",                            // optional, exact pool address
       "min_amount_usd": 10.0                       // optional, filters dust
     }
   }
   ```
   Skipping the subscribe message defaults to "match all" after a 5 s grace.
3. Server replies once with `{"type":"subscribed"}`, then streams `{"type":"swap", …}` JSON frames.
4. Server emits `{"type":"ping"}` every 30 s as a keep-alive.
5. If the broadcast channel lags this subscriber (slow consumer), the server emits `{"type":"lagged","skipped":N}` — connection stays open.

Wire format per swap:

```json
{
  "type": "swap",
  "signature": "5xY…",
  "slot": 412341234,
  "block_time": 1745000000,
  "dex": "Pumpup Bonding",
  "pool": "AQxK…",
  "user": "7AakHWVQ…",
  "input":  {"mint":"…","amount":"1000000","decimals":9,"ui_amount":"0.001"},
  "output": {"mint":"…","amount":"24637903819","decimals":6,"ui_amount":"24637.9"},
  "price_native":          "24637903.82",   // output ui per input ui
  "price_native_inverted": "0.0000000406",  // input ui per output ui
  "price_usd":             "0.00000647",    // USD per output token
  "amount_usd":            "0.16"           // total trade size in USD
}
```

USD enrichment: USDC/USDT/PYUSD are treated as $1.00. SOL is priced via the in-process oracle (Binance public ticker primary, DexScreener fallback, refreshed every 10 s by default). When neither side is SOL nor a stable, `price_usd` and `amount_usd` are `null` (no external lookup).

Disable with `--swap-stream-enabled false` if you don't need the broadcast hub.

---

## Configuration

| Option | CLI Flag | Env Var | Default |
|--------|----------|---------|---------|
| `rpc_url` | `--rpc-url` | `RPC_URL` | **(required)** |
| `listen` | `--listen` | `LISTEN_ADDR` | `127.0.0.1:8080` |
| `geyser_endpoint` | `--geyser-endpoint` | `GEYSER_ENDPOINT` | -- |
| `geyser_token` | `--geyser-token` | `GEYSER_TOKEN` | -- |
| `referral_account` | `--referral-account` | `REFERRAL_ACCOUNT` | -- |
| `pool_db` | `--pool-db-path` | `POOL_DB_PATH` | `./pools.db` |
| `pool_cache_ttl` | `--pool-cache-ttl` | `POOL_CACHE_TTL_MS` | `2000` (disabled with Geyser) |
| `alt_addresses` | `--alt-addresses` | `ALT_ADDRESSES` | -- |
| `discovery_mode` | `--discovery-mode` | `DISCOVERY_MODE` | `auto` |
| `block_scan_enabled` | `--block-scan-enabled` | `BLOCK_SCAN_ENABLED` | `true` |
| `swap_stream_enabled` | `--swap-stream-enabled` | `SWAP_STREAM_ENABLED` | `true` |
| `swap_stream_buffer_size` | `--swap-stream-buffer-size` | `SWAP_STREAM_BUFFER_SIZE` | `8192` |
| `sol_price_refresh_secs` | `--sol-price-refresh-secs` | `SOL_PRICE_REFRESH_SECS` | `10` |
| `log_level` | `--log-level` | `LOG_LEVEL` | `warn` |

Config can also be set via `config.toml` (see `config.toml` for full example). Priority: CLI flags > env vars > TOML file > defaults.

---

## Fee Structure

All swaps are routed through the on-chain flow-router program. No swap can bypass the fee wrapper. The program is **immutable** — upgrade authority permanently burned.

| Property | Value |
|----------|-------|
| Router Program ID | `FLoWxxKoBrZtNj5NTPuy1tZcSU6Nnjtz7v5snrrUsNqm` |
| Config PDA | `HKkiUSLkmE2CvpAa3E4fk7ATivuc1MNqmnzhNfnf7VVY` |
| Treasury | `2yL7tWs2TULhicDtdDV7A8P8Agh79EwCFLKeyKL5fMr3` |
| Platform Fee | 0.5% (50 bps) on output token |
| Integrator Share | 70% of fee |
| Protocol Share | 30% of fee |
| Upgrade Authority | `none` (immutable) |

Fee ATAs (treasury + referral) are auto-created idempotently on the first swap for each output mint (~0.002 SOL rent, paid by signer). Cached after creation.

---

## Pool Discovery

Geyser-native dual-channel discovery — zero RPC:

1. **Account stream**: catches every pool whose on-chain state changes. 14/19 types parsed inline from raw bytes. 5 async types use cached companion data.
2. **Block stream**: processes all DEX transactions (top-level + inner CPI instructions). Pools registered immediately from block data. PumpFun mints extracted from instruction accounts.

### Live Stats (sample 10-minute mainnet run, single Geyser endpoint)

| Metric | Value |
|--------|-------|
| Pools discovered | 5,007 |
| Discovery rate | ~500 pools/min |
| Stream updates | 19,754 (~33/sec) |
| Stream errors | 0 |
| Blockhash freshness | <400ms |
| RPC calls | 0 |

### SQLite Persistence

- **WAL mode** — crash-safe, concurrent reads
- **Full load**: 163ms for 250K pools
- **Auto-pruning**: pools not seen in 7 days removed
- **Bootstrap chain**: SQLite → warm storage (bincode) → JSON snapshot → empty

---

## Performance

### Quote Latency

| Scenario | Latency |
|----------|---------|
| Single hot pool (PumpFunAmm, inline) | **22µs** |
| All matching pools (parallel) | **40µs** |
| 1000 sequential quotes (p99) | **47µs** |
| 10 concurrent quotes (throughput) | **29,359/sec** |
| First quote (cold, seeds mirror) | ~300ms |

### Full Pipeline (quote → IX → TX → base64)

| Step | Avg | P95 |
|------|-----|-----|
| Quote (cache hit) | 39µs | 54µs |
| Build IX | 736µs | 3.6ms |
| Build TX | 38µs | 68µs |
| **Total** | **816µs** | **3.7ms** |

### Stress Test Results

| Test | Duration | Result |
|------|----------|--------|
| Sustained quoting (10 workers) | 60s | 5,131 quotes, 114/s |
| Cache stampede (100 tasks × 10K reads) | 340ms | 2.9M reads/s, 344ns/read |
| Quote→TX pipeline (50 workers) | 30s | 406K pipelines, 2,689/s |
| 2-hop routing (20 workers) | 30s | 1,348 routes, 44/s, 0 errors |
| Error fuzzing (10K invalid requests) | 254ms | 0 panics |
| Mixed workload (10 workers) | 120s | 329K ops, 2,727/s |

### Steady State Resource Usage (sample mainnet run)

| Metric | Value |
|--------|-------|
| CPU | <1% |
| RSS | ~35 MB |
| Network | ~140 KB/s in, ~13 KB/s out |
| Threads | 5 |
| Stream errors | 0 |

---

## Geyser Node Configuration

Recommended Yellowstone Geyser settings:

```json
{
  "channel_capacity": "10_000_000",
  "max_decoding_message_size": "67_108_864",
  "filter_limits": {
    "accounts": { "account_max": 100000 }
  }
}
```

`channel_capacity` 10M prevents dropped updates. `account_max` 100K supports dynamic vault + companion subscriptions. `max_decoding_message_size` 64MB handles large subscription requests.

---

## Testing

```bash
export OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu
export OPENSSL_INCLUDE_DIR=/usr/include

cargo test --lib     # 561 unit tests, no RPC needed
cargo test           # + E2E tests (needs RPC_URL + SIM_PRIVATE_KEY)
```

### Test Suites

| Suite | Tests | Description |
|-------|-------|-------------|
| Unit tests (`--lib`) | 561 | All modules, no external deps |
| `e2e_hot_path` | 5 | Full pipeline showcase with mainnet pools |
| `e2e_exhaustive` | 28 | All AMMs, vault fetching, math validation |
| `e2e_streaming` | 4 | Cache benchmarks (cold/hot, pipeline, concurrent) |
| `e2e_stress` | 6 | Sustained load, cache stampede, error fuzzing |
| `e2e_rpc_profile` | 6 | RPC call counting, latency buckets, cache hit rates |
| `e2e_quote` | 10 | Quote API E2E |
| `e2e_router` | 7 | Router wrapping + filtering |
| `e2e_router_mainnet` | 4 | Live: config init + 1/2/3-hop with ALTs |
| `e2e_alt` | 8 | Address Lookup Table versioned TX |
| `e2e_mainnet_swaps` | 8 | Live mainnet swap building |
| `e2e_live_swaps` | 1 | Fetch + build + simulate across 15 hardcoded AMMs |
| `e2e_fresh_markets` | 1 | Discovers a fresh active pool per AMM at run-time, then fetch → build → sim. 18/20 pipeline-pass typical (FlashTrade/Saros skip on no recent activity). |
| `e2e_fresh_roundtrips` | 1 | Live mainnet buy + sell on every SOL-paired AMM using fresh-discovered pools. Falls back to verified-deep pools on thin-bin errors. |
| `e2e_pumpup_bonding` | 1 | Live mainnet round-trip on Pumpup pre-graduation bonding curve |
| `e2e_pumpup_latency` | 1 | Cold fetch / hot cache / build / quote latency probe |
| `e2e_swap_stream` | 2 | `/swap-stream` WebSocket: disabled-mode error frame, enabled-mode subscribe → ping protocol round-trip |

---

## Contributing & Maintainers

**We're open to new maintainers.** If you've shipped Solana code, know your way around AMMs, gRPC, or Rust async, and want to help shape where flow-trades goes next, we'd love to hear from you.

The fastest way to get involved is to **[join our Telegram](https://t.me/+3BPRvJoUvlViMzg1)** and chat with the team directly. Pull requests, issues, and ideas all welcome on this repo too.

---

## License

MIT — see [LICENSE](./LICENSE). Free to use, fork, modify, and ship commercially.
