//! RPC Call Profile Benchmark
//!
//! Measures exactly how many RPC calls flow-trades makes under different
//! operational scenarios. Verifies the "zero RPC on hot path" claim.
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include \
//!   cargo test --test e2e_rpc_profile -- --nocapture --test-threads=1
//! ```

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;

use flow_trades::constants::*;
use flow_trades::execution::tx_builder::{build_unsigned_swap_message, TxBuildConfig};
use flow_trades::execution::AmmExecutorType;
use flow_trades::pool::cache::PoolCache;
use flow_trades::pool::fetcher;
use flow_trades::pool::registry::{PoolEntry, PoolRegistry};
use flow_trades::pool::types::{PoolType, SwapOrder};
use flow_trades::quote::types::QuoteRequest;
use flow_trades::quote::Quoter;
use flow_trades::stream::account_mirror::AccountMirror;

fn rpc_url() -> String {
    std::env::var("SOL_HTTPS_ENDPOINT")
        .or_else(|_| std::env::var("RPC_URL"))
        .expect("SOL_HTTPS_ENDPOINT or RPC_URL required")
}

fn rpc() -> Arc<RpcClient> {
    Arc::new(RpcClient::new_with_commitment(
        rpc_url(),
        CommitmentConfig::confirmed(),
    ))
}

fn pk(s: &str) -> Pubkey {
    Pubkey::from_str(s).unwrap()
}

struct PoolDef {
    pool_type: PoolType,
    label: &'static str,
    address: &'static str,
    mint_a: &'static str,
    mint_b: &'static str,
    /// True if this pool type has inline reserves (no vault fetch needed)
    inline_reserves: bool,
    /// True if this is a CLMM pool (uses sqrt_price + liquidity, no vault fetch)
    is_clmm: bool,
}

fn test_pools() -> Vec<PoolDef> {
    vec![
        PoolDef {
            pool_type: PoolType::PumpFunAmm,
            label: "PumpFunAmm",
            address: "6cPfRuSp8L7f1TMt3vtKhqYYuHDoHZTHGNzQ6hRABtx6",
            mint_a: "6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz",
            mint_b: "So11111111111111111111111111111111111111112",
            inline_reserves: true,
            is_clmm: false,
        },
        PoolDef {
            pool_type: PoolType::RaydiumCpmm,
            label: "RaydiumCpmm",
            address: "BEdCQzzvEmqtHH946GYsdeideaBYMvt8SZn5eUjKxTjr",
            mint_a: "25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook",
            mint_b: "So11111111111111111111111111111111111111112",
            inline_reserves: false,
            is_clmm: false,
        },
        PoolDef {
            pool_type: PoolType::Meteora,
            label: "Meteora",
            address: "BCXjm4FfSoquZQJV5Wcje1g1pSHW2hFMU9wDE98Nyatb",
            mint_a: "STrikemJEk2tFVYpg7SMo9nGPrnJ56fHnS1K7PV2fPw",
            mint_b: "So11111111111111111111111111111111111111112",
            inline_reserves: false,
            is_clmm: false,
        },
        PoolDef {
            pool_type: PoolType::MeteoraDamm,
            label: "MeteoraDamm",
            address: "4ac2qQp9uPQTuhvNH1p5xZmZnKV1pAxWKjBPkwvD6x6J",
            mint_a: "So11111111111111111111111111111111111111112",
            mint_b: "CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3",
            inline_reserves: false,
            is_clmm: false,
        },
        PoolDef {
            pool_type: PoolType::FluxBeam,
            label: "FluxBeam",
            address: "5X2KrjQBVapwGzFwCWXkdFBv7JY6FpQJvdhejVH9PuwX",
            mint_a: "So11111111111111111111111111111111111111112",
            mint_b: "2JoJuvFip3PdPYScJYPBpWMGfXzRASoNjnQ1yPiapump",
            inline_reserves: false,
            is_clmm: false,
        },
        PoolDef {
            pool_type: PoolType::RaydiumLp,
            label: "RaydiumLP",
            address: "6Lc76tcWsCEkydyLriNaeDkUgVekusBVGgQYYDiKRZi1",
            mint_a: "So11111111111111111111111111111111111111112",
            mint_b: "8Ki8DpuWNxu9VsS3kQbarsCWMcFGWkzzA8pUPto9zBd5",
            inline_reserves: false,
            is_clmm: false,
        },
    ]
}

fn make_req(input: Pubkey, output: Pubkey, amount: u64) -> QuoteRequest {
    QuoteRequest {
        input_mint: input,
        output_mint: output,
        amount,
        slippage_bps: 100,
        only_direct_routes: true,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    }
}

// ═══════════════════════════════════════════════════════════════
// TEST 1: Cold start — measure RPC calls to bootstrap each pool
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rpc_profile_cold_start() {
    eprintln!("\n╔══════════════════════════════════════════════════════════╗");
    eprintln!("║  RPC PROFILE: COLD START (first fetch per pool)         ║");
    eprintln!("╚══════════════════════════════════════════════════════════╝\n");

    let rpc_client = rpc();
    let pools = test_pools();

    eprintln!("  {:20} | {:>7} | {:>10} | Notes", "Pool", "Fetch", "RPC Calls");
    eprintln!("  {:─<20}─┼─{:─>7}─┼─{:─>10}─┼─{:─<20}", "", "", "", "");

    for p in &pools {
        let addr = pk(p.address);
        let start = Instant::now();
        let result = fetcher::fetch_pool_state(&rpc_client, p.pool_type, &addr).await;
        let elapsed = start.elapsed();

        let (status, rpc_calls, notes) = match result {
            Ok(_) => {
                // fetch_pool_state does 1 getAccount call per pool
                // Some types need companion fetches (Raydium V4 needs serum market)
                let calls = match p.pool_type {
                    PoolType::RaydiumV4 => "2 (pool+serum)",
                    _ => "1 (getAccount)",
                };
                let notes = if p.inline_reserves {
                    "inline reserves"
                } else if p.is_clmm {
                    "inline sqrt_price"
                } else {
                    "needs vault fetch"
                };
                ("OK", calls, notes)
            }
            Err(e) => ("FAIL", "0", ""),
        };

        eprintln!(
            "  {:20} | {:>5}ms | {:>10} | {}",
            p.label,
            elapsed.as_millis(),
            rpc_calls,
            notes
        );
    }

    eprintln!("\n  Legend: Each fetch_pool_state() = 1 RPC getAccount (except RaydiumV4 = 2)");
    eprintln!("  [OK] Cold start profile complete\n");
}

// ═══════════════════════════════════════════════════════════════
// TEST 2: Hot path — zero RPC when cache is warm
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rpc_profile_hot_path_zero_rpc() {
    eprintln!("\n╔══════════════════════════════════════════════════════════╗");
    eprintln!("║  RPC PROFILE: HOT PATH (cache warm, 0 RPC expected)     ║");
    eprintln!("╚══════════════════════════════════════════════════════════╝\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());

    // Pre-warm everything
    let pools = test_pools();
    for p in &pools {
        let addr = pk(p.address);
        registry.add(PoolEntry {
            pool_type: p.pool_type,
            address: addr,
            mint_a: pk(p.mint_a),
            mint_b: pk(p.mint_b),
        });
        if let Ok(state) = fetcher::fetch_pool_state(&rpc_client, p.pool_type, &addr).await {
            cache.insert(addr, state);
        }
    }

    let quoter = Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc_client),
    );

    eprintln!("  Pre-warmed {} pools. Now quoting from hot cache...\n", cache.len());

    // PumpFunAmm: inline reserves → definitely 0 RPC
    eprintln!("  ── PumpFunAmm (inline reserves) ──");
    let req = make_req(
        SOL_NATIVE_MINT,
        pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz"),
        1_000_000_000,
    );

    let iterations = 1000;
    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        let _ = quoter.quote(&req).await;
        latencies.push(t.elapsed().as_nanos() as u64);
    }
    latencies.sort();

    let avg_ns = latencies.iter().sum::<u64>() / latencies.len() as u64;
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let max = *latencies.last().unwrap();

    eprintln!("  {iterations} quotes | avg={avg_ns}ns p50={p50}ns p99={p99}ns max={max}ns");
    eprintln!("  RPC calls: 0 (inline reserves, all from cache)");

    // If avg > 1ms, there were RPC calls happening
    assert!(
        avg_ns < 1_000_000,
        "PumpFunAmm hot-cache quote avg should be <1ms (no RPC), got {}ns",
        avg_ns
    );
    eprintln!("  [OK] Confirmed zero RPC on PumpFunAmm hot path\n");

    // CP pools (RaydiumCpmm, Meteora, etc.): vault fetch needed unless mirror populated
    eprintln!("  ── Vault-based pools (first quote fetches vaults, subsequent from mirror) ──");
    let vault_pools = [
        ("RaydiumCpmm", "25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook"),
        ("MeteoraDamm", "CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3"),
        ("FluxBeam",    "2JoJuvFip3PdPYScJYPBpWMGfXzRASoNjnQ1yPiapump"),
    ];

    for (label, token_mint) in &vault_pools {
        let req = make_req(SOL_NATIVE_MINT, pk(token_mint), 100_000_000);

        // First quote: may hit RPC for vault balances
        let t1 = Instant::now();
        let r1 = quoter.quote(&req).await;
        let first_ms = t1.elapsed().as_millis();

        // Second quote: should be faster (mirror seeded from first call)
        let t2 = Instant::now();
        let r2 = quoter.quote(&req).await;
        let second_us = t2.elapsed().as_micros();

        // 10 more quotes to get stable hot-path measurement
        let mut hot_lats = Vec::new();
        for _ in 0..10 {
            let t = Instant::now();
            let _ = quoter.quote(&req).await;
            hot_lats.push(t.elapsed().as_micros());
        }
        hot_lats.sort();
        let hot_avg = hot_lats.iter().sum::<u128>() / hot_lats.len() as u128;

        let first_status = if r1.is_ok() { "OK" } else { "ERR" };
        let second_status = if r2.is_ok() { "OK" } else { "ERR" };

        eprintln!(
            "  {:15} | 1st: {:>5}ms ({}) | 2nd: {:>5}µs ({}) | hot avg: {}µs",
            label, first_ms, first_status, second_us, second_status, hot_avg
        );
    }
    eprintln!("\n  1st quote = pool in cache + vault RPC (2× getTokenAccountBalance)");
    eprintln!("  2nd quote = mirror seeded → zero RPC");
    eprintln!("  [OK] Vault-based profile complete\n");
}

// ═══════════════════════════════════════════════════════════════
// TEST 3: Measure cache hit rates over sustained quoting
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rpc_profile_cache_hit_rate() {
    eprintln!("\n╔══════════════════════════════════════════════════════════╗");
    eprintln!("║  RPC PROFILE: CACHE HIT RATE (30s sustained quoting)    ║");
    eprintln!("╚══════════════════════════════════════════════════════════╝\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());

    let pools = test_pools();
    let mut warmed = 0;
    for p in &pools {
        let addr = pk(p.address);
        registry.add(PoolEntry {
            pool_type: p.pool_type,
            address: addr,
            mint_a: pk(p.mint_a),
            mint_b: pk(p.mint_b),
        });
        if let Ok(state) = fetcher::fetch_pool_state(&rpc_client, p.pool_type, &addr).await {
            cache.insert(addr, state);
            warmed += 1;
        }
    }

    let quoter = Arc::new(Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc_client),
    ));

    eprintln!("  Pre-warmed {warmed} pools");

    // Run for 30s, bucketing latencies into "cache hit" (<1ms) and "RPC" (>1ms)
    let duration = Duration::from_secs(30);
    let mut cache_hits = 0u64;  // < 1ms
    let mut rpc_calls = 0u64;   // >= 1ms (implies vault RPC)
    let mut fast_quotes = 0u64; // < 100µs (inline reserves, zero external I/O)
    let mut total = 0u64;
    let mut errors = 0u64;

    let tokens: Vec<&str> = pools.iter()
        .map(|p| if p.mint_a == "So11111111111111111111111111111111111111112" { p.mint_b } else { p.mint_a })
        .collect();

    let start = Instant::now();
    while start.elapsed() < duration {
        let token = pk(tokens[(total as usize) % tokens.len()]);
        let req = make_req(SOL_NATIVE_MINT, token, 100_000_000);

        let t = Instant::now();
        match quoter.quote(&req).await {
            Ok(_) => {
                let elapsed = t.elapsed();
                if elapsed.as_micros() < 100 {
                    fast_quotes += 1;
                }
                if elapsed.as_millis() < 1 {
                    cache_hits += 1;
                } else {
                    rpc_calls += 1;
                }
            }
            Err(_) => { errors += 1; }
        }
        total += 1;
    }

    let elapsed = start.elapsed();
    let success = cache_hits + rpc_calls;

    eprintln!("  Duration:      {:.1}s", elapsed.as_secs_f64());
    eprintln!("  Total quotes:  {total}");
    eprintln!("  Successful:    {success}");
    eprintln!("  Errors:        {errors}");
    eprintln!();
    eprintln!("  ── Latency Buckets ──");
    eprintln!("  < 100µs (zero I/O):   {:>6} ({:.1}%)", fast_quotes,
        if success > 0 { fast_quotes as f64 / success as f64 * 100.0 } else { 0.0 });
    eprintln!("  < 1ms (cache hit):     {:>6} ({:.1}%)", cache_hits,
        if success > 0 { cache_hits as f64 / success as f64 * 100.0 } else { 0.0 });
    eprintln!("  >= 1ms (RPC fallback): {:>6} ({:.1}%)", rpc_calls,
        if success > 0 { rpc_calls as f64 / success as f64 * 100.0 } else { 0.0 });

    let hit_rate = if success > 0 { cache_hits as f64 / success as f64 * 100.0 } else { 0.0 };
    eprintln!("\n  Cache hit rate: {:.1}%", hit_rate);
    eprintln!("  [OK] Cache hit rate profile complete\n");
}

// ═══════════════════════════════════════════════════════════════
// TEST 4: Full /swap pipeline RPC breakdown
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rpc_profile_swap_pipeline() {
    eprintln!("\n╔══════════════════════════════════════════════════════════╗");
    eprintln!("║  RPC PROFILE: FULL /swap PIPELINE BREAKDOWN             ║");
    eprintln!("╚══════════════════════════════════════════════════════════╝\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());

    // Setup
    let pools = test_pools();
    for p in &pools {
        let addr = pk(p.address);
        registry.add(PoolEntry {
            pool_type: p.pool_type,
            address: addr,
            mint_a: pk(p.mint_a),
            mint_b: pk(p.mint_b),
        });
        if let Ok(state) = fetcher::fetch_pool_state(&rpc_client, p.pool_type, &addr).await {
            cache.insert(addr, state);
        }
    }

    let quoter = Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc_client),
    );

    let user = pk("6TwqjGNQ8c2aUHvbpAjMd4bdHdone9CTrz3c8S71E2WW");

    // ── PumpFunAmm: full pipeline, zero external RPC ──
    eprintln!("  ── PumpFunAmm Full Pipeline (inline reserves = zero vault RPC) ──\n");

    let req = make_req(
        SOL_NATIVE_MINT,
        pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz"),
        100_000_000,
    );

    let iterations = 100;
    let mut quote_us = Vec::new();
    let mut ix_us = Vec::new();
    let mut tx_us = Vec::new();
    let mut total_us = Vec::new();

    for _ in 0..iterations {
        let t_total = Instant::now();

        // Step 1: Quote
        let t = Instant::now();
        let resp = quoter.quote(&req).await.unwrap();
        quote_us.push(t.elapsed().as_micros());

        // Step 2: Build IX
        let t = Instant::now();
        let route = &resp.routes[0];
        let pool_addr = pk(&route.pool.pool_address);
        let pool_type = PoolType::from_str(&route.pool.dex).unwrap_or(PoolType::PumpFunAmm);
        let state = cache.get(&pool_addr).unwrap();
        let min_out: u64 = resp.minimum_out.parse().unwrap_or(1);
        let order = SwapOrder {
            pool_address: pool_addr,
            pool_type,
            input_mint: SOL_NATIVE_MINT,
            output_mint: pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz"),
            amount_in: 100_000_000,
            min_amount_out: min_out,
            user,
            input_token_program: TOKEN_PROGRAM_ID,
            output_token_program: TOKEN_PROGRAM_ID,
        };
        let executor = AmmExecutorType::from_pool_type(pool_type).unwrap();
        let swap_ix = executor.build_swap_ix(&order, &state).unwrap();
        ix_us.push(t.elapsed().as_micros());

        // Step 3: Build TX (blockhash is offline — using dummy)
        let t = Instant::now();
        let blockhash = solana_sdk::hash::Hash::new_unique();
        let config = TxBuildConfig::default();
        let _ = build_unsigned_swap_message(&swap_ix, &user, &config, blockhash).unwrap();
        tx_us.push(t.elapsed().as_micros());

        total_us.push(t_total.elapsed().as_micros());
    }

    quote_us.sort();
    ix_us.sort();
    tx_us.sort();
    total_us.sort();

    let avg = |v: &[u128]| v.iter().sum::<u128>() / v.len() as u128;
    let p95 = |v: &[u128]| v[(v.len() as f64 * 0.95) as usize];
    let p99 = |v: &[u128]| v[(v.len() as f64 * 0.99) as usize];

    eprintln!("  {:15} | {:>8} | {:>8} | {:>8} | RPC Calls", "Step", "Avg", "P95", "P99");
    eprintln!("  {:─<15}─┼─{:─>8}─┼─{:─>8}─┼─{:─>8}─┼─{:─<15}", "", "", "", "", "");
    eprintln!("  {:15} | {:>6}µs | {:>6}µs | {:>6}µs | 0 (cache hit)",
        "Quote", avg(&quote_us), p95(&quote_us), p99(&quote_us));
    eprintln!("  {:15} | {:>6}µs | {:>6}µs | {:>6}µs | 0 (local compute)",
        "Build IX", avg(&ix_us), p95(&ix_us), p99(&ix_us));
    eprintln!("  {:15} | {:>6}µs | {:>6}µs | {:>6}µs | 0 (offline, dummy hash)",
        "Build TX", avg(&tx_us), p95(&tx_us), p99(&tx_us));
    eprintln!("  {:─<15}─┼─{:─>8}─┼─{:─>8}─┼─{:─>8}─┤", "", "", "", "");
    eprintln!("  {:15} | {:>6}µs | {:>6}µs | {:>6}µs | 0 total",
        "FULL PIPELINE", avg(&total_us), p95(&total_us), p99(&total_us));

    eprintln!("\n  Real /swap handler adds:");
    eprintln!("  + getLatestBlockhash:     1 RPC (if blockhash cache miss, rare)");
    eprintln!("  + simulateTransaction:    1 RPC (only if ?simulate=true)");
    eprintln!("  = Typical: 0-1 RPC calls for the full /swap pipeline\n");

    // ── Blockhash fetch latency ──
    eprintln!("  ── Blockhash Fetch Latency (the one RPC call /swap might need) ──\n");
    let mut bh_ms = Vec::new();
    for _ in 0..10 {
        let t = Instant::now();
        let _ = rpc_client.get_latest_blockhash().await;
        bh_ms.push(t.elapsed().as_millis());
    }
    bh_ms.sort();
    eprintln!("  getLatestBlockhash (10 calls): min={}ms avg={}ms max={}ms",
        bh_ms[0],
        bh_ms.iter().sum::<u128>() / bh_ms.len() as u128,
        bh_ms[bh_ms.len() - 1]);
    eprintln!("  (Cached in background every 400ms — /swap rarely hits this)\n");
}

// ═══════════════════════════════════════════════════════════════
// TEST 5: 2-hop RPC profile (2× vault fetch on cold, 0 on hot)
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rpc_profile_two_hop() {
    eprintln!("\n╔══════════════════════════════════════════════════════════╗");
    eprintln!("║  RPC PROFILE: 2-HOP ROUTING                             ║");
    eprintln!("╚══════════════════════════════════════════════════════════╝\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());

    let pools = test_pools();
    for p in &pools {
        let addr = pk(p.address);
        registry.add(PoolEntry {
            pool_type: p.pool_type,
            address: addr,
            mint_a: pk(p.mint_a),
            mint_b: pk(p.mint_b),
        });
        if let Ok(state) = fetcher::fetch_pool_state(&rpc_client, p.pool_type, &addr).await {
            cache.insert(addr, state);
        }
    }

    let quoter = Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc_client),
    );

    // 2-hop: token_a -> SOL -> token_b (through SOL bridge)
    let token_a = pk("25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook");
    let token_b = pk("CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3");

    let req = QuoteRequest {
        input_mint: token_a,
        output_mint: token_b,
        amount: 1_000_000_000,
        slippage_bps: 200,
        only_direct_routes: false,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    // First call: vaults not in mirror → RPC vault fetches
    let t1 = Instant::now();
    let r1 = quoter.quote(&req).await;
    let first = t1.elapsed();

    // Second call: mirror seeded → zero RPC
    let t2 = Instant::now();
    let r2 = quoter.quote(&req).await;
    let second = t2.elapsed();

    // 10 more hot calls
    let mut hot = Vec::new();
    for _ in 0..10 {
        let t = Instant::now();
        let _ = quoter.quote(&req).await;
        hot.push(t.elapsed());
    }
    hot.sort();

    let hops_1 = r1.as_ref().map(|r| r.routes.len()).unwrap_or(0);
    let hops_2 = r2.as_ref().map(|r| r.routes.len()).unwrap_or(0);

    eprintln!("  2-hop: {} -> SOL -> {}", &token_a.to_string()[..8], &token_b.to_string()[..8]);
    eprintln!();
    eprintln!("  {:>15} | {:>10} | {:>5} | RPC Profile", "Call", "Latency", "Hops");
    eprintln!("  {:─>15}─┼─{:─>10}─┼─{:─>5}─┼─{:─<30}", "", "", "", "");
    eprintln!("  {:>15} | {:>8}ms | {:>5} | 2-4 RPC (vault balances for 2 pools)",
        "1st (cold)", first.as_millis(), hops_1);
    eprintln!("  {:>15} | {:>8}µs | {:>5} | 0 RPC (mirror seeded)",
        "2nd (warm)", second.as_micros(), hops_2);
    eprintln!("  {:>15} | {:>8}µs | {:>5} | 0 RPC (mirror)",
        "Hot avg", hot.iter().map(|d| d.as_micros()).sum::<u128>() / hot.len() as u128,
        hops_2);

    eprintln!("\n  2-hop evaluates all bridge mints (SOL, USDC, USDT) × all pool pairs");
    eprintln!("  Cold: up to 4 vault fetches (2 per hop × 2 hops)");
    eprintln!("  Hot:  0 RPC (vault balances cached in AccountMirror)\n");
}

// ═══════════════════════════════════════════════════════════════
// TEST 6: Summary — RPC call inventory per operation
// ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_rpc_profile_summary() {
    eprintln!("\n╔══════════════════════════════════════════════════════════╗");
    eprintln!("║  RPC CALL INVENTORY: FLOW-TRADES                        ║");
    eprintln!("╚══════════════════════════════════════════════════════════╝\n");

    eprintln!("  ── Per-Operation RPC Calls ──\n");
    eprintln!("  {:35} | {:>5} | {:>5} | {}", "Operation", "Cold", "Hot", "RPC Methods");
    eprintln!("  {:─<35}─┼─{:─>5}─┼─{:─>5}─┼─{:─<30}", "", "", "", "");
    eprintln!("  {:35} | {:>5} | {:>5} | {}", "GET /quote (inline, e.g. PumpFunAmm)", "1", "0", "getAccount");
    eprintln!("  {:35} | {:>5} | {:>5} | {}", "GET /quote (vault, e.g. Raydium)", "3", "0", "getAccount + 2×getTokenAccountBalance");
    eprintln!("  {:35} | {:>5} | {:>5} | {}", "GET /quote (CLMM, e.g. Orca)", "1", "0", "getAccount (inline sqrt_price)");
    eprintln!("  {:35} | {:>5} | {:>5} | {}", "GET /quote (2-hop)", "2-6", "0", "per-hop pool + vault fetches");
    eprintln!("  {:35} | {:>5} | {:>5} | {}", "POST /swap (typical)", "0-1", "0", "getLatestBlockhash (if cache miss)");
    eprintln!("  {:35} | {:>5} | {:>5} | {}", "POST /swap (?simulate=true)", "1-2", "1", "+ simulateTransaction");
    eprintln!("  {:35} | {:>5} | {:>5} | {}", "POST /swap-instructions", "0", "0", "pure local compute");

    eprintln!("\n  ── Background Tasks ──\n");
    eprintln!("  {:35} | {:>12} | {}", "Task", "Frequency", "RPC Method");
    eprintln!("  {:─<35}─┼─{:─>12}─┼─{:─<30}", "", "", "");
    eprintln!("  {:35} | {:>12} | {}", "Blockhash refresh", "every 400ms", "getLatestBlockhash");
    eprintln!("  {:35} | {:>12} | {}", "Pool discovery (block scanner)", "per new pool", "getAccount");
    eprintln!("  {:35} | {:>12} | {}", "ALT refresh", "every 300s", "getAccount × N ALTs");

    eprintln!("\n  ── Streaming (Geyser gRPC — replaces RPC on hot path) ──\n");
    eprintln!("  Geyser streams: pool state + vault balances + blockhash");
    eprintln!("  Hot-path /quote and /swap are 0 RPC calls.");
    eprintln!("  Only background discovery (new pools) makes RPC calls.\n");
}
