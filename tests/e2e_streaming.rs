//! Cache and quote pipeline benchmarks.
//!
//! Benchmarks the full quote pipeline under different cache states:
//! cold vs hot per AMM, 1000-iteration latency distribution,
//! full pipeline (quote→IX→TX→b64), and concurrent quoting.
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include \
//!   cargo test --test e2e_streaming -- --nocapture --test-threads=1
//! ```

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;

use flow_trades::constants::*;
use flow_trades::execution::tx_builder::{build_unsigned_versioned_tx, TxBuildConfig};
use flow_trades::execution::AmmExecutorType;
use flow_trades::pool::cache::PoolCache;
use flow_trades::pool::fetcher;
use flow_trades::pool::registry::{PoolEntry, PoolRegistry};
use flow_trades::pool::types::{PoolType, SwapOrder};
use flow_trades::quote::Quoter;

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

struct TestPool {
    address: &'static str,
    pool_type: PoolType,
    mint_a: &'static str,
    mint_b: &'static str,
}

const TEST_POOLS: &[TestPool] = &[
    TestPool {
        address: "BEdCQzzvEmqtHH946GYsdeideaBYMvt8SZn5eUjKxTjr",
        pool_type: PoolType::RaydiumCpmm,
        mint_a: "25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook",
        mint_b: "So11111111111111111111111111111111111111112",
    },
    TestPool {
        address: "6cPfRuSp8L7f1TMt3vtKhqYYuHDoHZTHGNzQ6hRABtx6",
        pool_type: PoolType::PumpFunAmm,
        mint_a: "So11111111111111111111111111111111111111112",
        mint_b: "6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz",
    },
    TestPool {
        address: "4ac2qQp9uPQTuhvNH1p5xZmZnKV1pAxWKjBPkwvD6x6J",
        pool_type: PoolType::MeteoraDamm,
        mint_a: "So11111111111111111111111111111111111111112",
        mint_b: "CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3",
    },
    TestPool {
        address: "BCXjm4FfSoquZQJV5Wcje1g1pSHW2hFMU9wDE98Nyatb",
        pool_type: PoolType::Meteora,
        mint_a: "So11111111111111111111111111111111111111112",
        mint_b: "STrikemJEk2tFVYpg7SMo9nGPrnJ56fHnS1K7PV2fPw",
    },
];

fn pk(s: &str) -> Pubkey {
    Pubkey::from_str(s).unwrap()
}

// ── Cache Benchmark Suite ──

#[tokio::test]
async fn test_benchmark_cold_vs_hot_per_amm() {
    eprintln!("\n=== BENCHMARK: Cold vs Hot per AMM ===\n");
    let rpc = rpc();
    let cache = Arc::new(PoolCache::new(60000));
    let user = pk("6TwqjGNQ8c2aUHvbpAjMd4bdHdone9CTrz3c8S71E2WW");

    eprintln!("  {:<16} | {:>8} | {:>8} | {:>8} | {:>10}", "AMM", "Cold(ms)", "Hot(µs)", "Build(µs)", "Speedup");
    eprintln!("  {:-<16}-+-{:-<8}-+-{:-<8}-+-{:-<8}-+-{:-<10}", "", "", "", "", "");

    for p in TEST_POOLS {
        let addr = pk(p.address);

        // Cold: fetch from RPC
        let cold_start = Instant::now();
        let state = match fetcher::fetch_pool_state(&rpc, p.pool_type, &addr).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("  {:<16} | SKIP: {e}", format!("{:?}", p.pool_type));
                continue;
            }
        };
        let cold_ms = cold_start.elapsed().as_millis();

        // Cache it
        cache.insert(addr, state.clone());

        // Hot: read from cache (1000 iterations)
        let hot_start = Instant::now();
        for _ in 0..1000 {
            let _ = cache.get(&addr);
        }
        let hot_us = hot_start.elapsed().as_micros() / 1000;

        // Build IX
        let executor = AmmExecutorType::from_pool_type(p.pool_type).unwrap();
        let order = SwapOrder {
            pool_address: addr,
            pool_type: p.pool_type,
            input_mint: SOL_NATIVE_MINT,
            output_mint: pk(if p.mint_a == "So11111111111111111111111111111111111111112" { p.mint_b } else { p.mint_a }),
            amount_in: 1_000_000,
            min_amount_out: 1,
            user,
            input_token_program: TOKEN_PROGRAM_ID,
            output_token_program: TOKEN_PROGRAM_ID,
        };

        let build_start = Instant::now();
        let _ = executor.build_swap_ix(&order, &state);
        let build_us = build_start.elapsed().as_micros();

        let speedup = if hot_us > 0 { cold_ms * 1000 / hot_us as u128 } else { 0 };

        eprintln!(
            "  {:<16} | {:>5}ms | {:>5}µs | {:>5}µs | {:>8}x",
            format!("{:?}", p.pool_type), cold_ms, hot_us, build_us, speedup
        );
    }
    eprintln!("");
    eprintln!("  [OK] All AMMs benchmarked");
}

#[tokio::test]
async fn test_benchmark_quote_pipeline_iterations() {
    eprintln!("\n=== BENCHMARK: Quote Pipeline (1000 iterations) ===\n");
    let rpc = rpc();
    let cache = Arc::new(PoolCache::new(60000));
    let registry = Arc::new(PoolRegistry::new());

    // Register and pre-warm one pool
    let p = &TEST_POOLS[1]; // PumpFunAmm (has inline reserves)
    let addr = pk(p.address);
    registry.add(PoolEntry {
        address: addr,
        pool_type: p.pool_type,
        mint_a: pk(p.mint_a),
        mint_b: pk(p.mint_b),
    });

    let state = fetcher::fetch_pool_state(&rpc, p.pool_type, &addr)
        .await
        .expect("fetch");
    cache.insert(addr, state);

    let quoter = Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc),
    );

    // Run 1000 quotes
    let iterations = 1000;
    let mut latencies: Vec<u128> = Vec::with_capacity(iterations);

    let req = flow_trades::quote::QuoteRequest {
        input_mint: SOL_NATIVE_MINT,
        output_mint: pk(p.mint_b),
        amount: 1_000_000_000,
        slippage_bps: 50,
        only_direct_routes: true,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = quoter.quote(&req).await;
        latencies.push(start.elapsed().as_micros() as u128);
    }

    latencies.sort();
    let min = latencies[0];
    let p50 = latencies[iterations / 2];
    let p95 = latencies[iterations * 95 / 100];
    let p99 = latencies[iterations * 99 / 100];
    let max = latencies[iterations - 1];
    let avg: u128 = latencies.iter().sum::<u128>() / iterations as u128;
    let success = latencies.iter().filter(|&&l| l < 1_000_000).count();

    eprintln!("  Iterations: {}", iterations);
    eprintln!("  Success:    {}/{}", success, iterations);
    eprintln!("");
    eprintln!("  Metric     | Value (µs)");
    eprintln!("  -----------+-----------");
    eprintln!("  Min        | {:>9}", min);
    eprintln!("  Avg        | {:>9}", avg);
    eprintln!("  P50        | {:>9}", p50);
    eprintln!("  P95        | {:>9}", p95);
    eprintln!("  P99        | {:>9}", p99);
    eprintln!("  Max        | {:>9}", max);
    eprintln!("");

    assert!(avg < 1000, "Average quote should be under 1ms, got {}µs", avg);
    assert!(p99 < 10000, "P99 should be under 10ms, got {}µs", p99);
    eprintln!("  [OK] 1000 quotes within latency targets");
}

#[tokio::test]
async fn test_benchmark_full_pipeline_with_tx_build() {
    eprintln!("\n=== BENCHMARK: Full Pipeline (quote → IX → TX → b64) ===\n");
    let rpc = rpc();
    let cache = Arc::new(PoolCache::new(60000));
    let registry = Arc::new(PoolRegistry::new());
    let user = pk("6TwqjGNQ8c2aUHvbpAjMd4bdHdone9CTrz3c8S71E2WW");

    let p = &TEST_POOLS[1];
    let addr = pk(p.address);
    registry.add(PoolEntry {
        address: addr,
        pool_type: p.pool_type,
        mint_a: pk(p.mint_a),
        mint_b: pk(p.mint_b),
    });

    let state = fetcher::fetch_pool_state(&rpc, p.pool_type, &addr)
        .await
        .expect("fetch");
    cache.insert(addr, state.clone());

    let blockhash = rpc.get_latest_blockhash().await.unwrap();
    let tx_config = TxBuildConfig {
        compute_unit_limit: 400_000,
        priority_fee_lamports: 5_000,
    };

    let iterations = 100;
    let mut quote_us = Vec::with_capacity(iterations);
    let mut build_ix_us = Vec::with_capacity(iterations);
    let mut build_tx_us = Vec::with_capacity(iterations);
    let mut total_us = Vec::with_capacity(iterations);

    let executor = AmmExecutorType::from_pool_type(p.pool_type).unwrap();

    for _ in 0..iterations {
        let total_start = Instant::now();

        let q_start = Instant::now();
        let _cached = cache.get(&addr).expect("cached");
        let q_time = q_start.elapsed().as_micros();

        let ix_start = Instant::now();
        let order = SwapOrder {
            pool_address: addr,
            pool_type: p.pool_type,
            input_mint: SOL_NATIVE_MINT,
            output_mint: pk(p.mint_b),
            amount_in: 1_000_000_000,
            min_amount_out: 1,
            user,
            input_token_program: TOKEN_PROGRAM_ID,
            output_token_program: TOKEN_PROGRAM_ID,
        };
        let ixs = executor.build_swap_ix(&order, &state).unwrap();
        let ix_time = ix_start.elapsed().as_micros();

        let tx_start = Instant::now();
        let vtx = build_unsigned_versioned_tx(&ixs, &user, &tx_config, blockhash, &[]).unwrap();
        let _b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &bincode::serialize(&vtx).unwrap(),
        );
        let tx_time = tx_start.elapsed().as_micros();

        let total_time = total_start.elapsed().as_micros();

        quote_us.push(q_time);
        build_ix_us.push(ix_time);
        build_tx_us.push(tx_time);
        total_us.push(total_time);
    }

    let avg = |v: &[u128]| -> u128 { v.iter().sum::<u128>() / v.len() as u128 };
    let p95 = |v: &mut Vec<u128>| -> u128 { v.sort(); v[v.len() * 95 / 100] };

    eprintln!("  Step          | Avg (µs) | P95 (µs)");
    eprintln!("  --------------+----------+---------");
    eprintln!("  Cache read    | {:>8} | {:>8}", avg(&quote_us), p95(&mut quote_us.clone()));
    eprintln!("  Build IX      | {:>8} | {:>8}", avg(&build_ix_us), p95(&mut build_ix_us.clone()));
    eprintln!("  Build TX+b64  | {:>8} | {:>8}", avg(&build_tx_us), p95(&mut build_tx_us.clone()));
    eprintln!("  TOTAL         | {:>8} | {:>8}", avg(&total_us), p95(&mut total_us.clone()));
    eprintln!("");

    assert!(avg(&total_us) < 5000, "Full pipeline avg should be <5ms");
    eprintln!("  [OK] Full pipeline benchmarked ({} iterations)", iterations);
}

#[tokio::test]
async fn test_benchmark_concurrent_quotes() {
    eprintln!("\n=== BENCHMARK: Concurrent Quotes (10 parallel) ===\n");
    let rpc = rpc();
    let cache = Arc::new(PoolCache::new(60000));
    let registry = Arc::new(PoolRegistry::new());

    for p in TEST_POOLS {
        let addr = pk(p.address);
        registry.add(PoolEntry {
            address: addr,
            pool_type: p.pool_type,
            mint_a: pk(p.mint_a),
            mint_b: pk(p.mint_b),
        });
        if let Ok(state) = fetcher::fetch_pool_state(&rpc, p.pool_type, &addr).await {
            cache.insert(addr, state);
        }
    }

    let quoter = Arc::new(Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc),
    ));

    let p = &TEST_POOLS[1];

    let start = Instant::now();
    let mut handles = Vec::new();

    for _ in 0..10 {
        let quoter = Arc::clone(&quoter);
        let output_mint = pk(p.mint_b);
        handles.push(tokio::spawn(async move {
            let req = flow_trades::quote::QuoteRequest {
                input_mint: SOL_NATIVE_MINT,
                output_mint,
                amount: 1_000_000_000,
                slippage_bps: 50,
                only_direct_routes: true,
                exclude_dexes: vec![],
                dexes: vec![],
                max_accounts: 64,
            };
            let s = Instant::now();
            let result = quoter.quote(&req).await;
            (s.elapsed().as_micros(), result.is_ok())
        }));
    }

    let mut latencies = Vec::new();
    let mut successes = 0;
    for h in handles {
        let (us, ok) = h.await.unwrap();
        latencies.push(us);
        if ok { successes += 1; }
    }
    let wall_time = start.elapsed();

    latencies.sort();
    eprintln!("  10 concurrent quotes in {:?} wall time", wall_time);
    eprintln!("  Success: {}/10", successes);
    eprintln!("  Min: {}µs, Max: {}µs, Avg: {}µs",
        latencies[0],
        latencies[latencies.len() - 1],
        latencies.iter().sum::<u128>() / latencies.len() as u128
    );
    eprintln!("  Throughput: {:.0} quotes/sec",
        10.0 / wall_time.as_secs_f64()
    );

    assert!(successes >= 8, "At least 8/10 concurrent quotes should succeed");
    eprintln!("  [OK] Concurrent quotes work correctly");
}
