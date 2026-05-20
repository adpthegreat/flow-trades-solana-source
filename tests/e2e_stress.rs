//! Stress test for flow-trades: hammers the quote engine, cache, and TX build
//! pipeline under sustained concurrent load for several minutes.
//!
//! Tests:
//! 1. Sustained concurrent quoting (10 workers, 60s, mixed pools)
//! 2. Cache stampede (100 concurrent tasks × 10K reads)
//! 3. Quote → TX build pipeline under load (50 workers, 30s)
//! 4. 2-hop routing under pressure (20 workers, 30s)
//! 5. Error path stress (10K invalid requests, zero panics)
//! 6. Mixed workload (quotes + TX builds + RPC fetches, 120s)
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include \
//!   cargo test --test e2e_stress -- --nocapture --test-threads=1
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
}

fn stress_pools() -> Vec<PoolDef> {
    vec![
        PoolDef {
            pool_type: PoolType::RaydiumCpmm,
            label: "RaydiumCpmm",
            address: "BEdCQzzvEmqtHH946GYsdeideaBYMvt8SZn5eUjKxTjr",
            mint_a: "25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook",
            mint_b: "So11111111111111111111111111111111111111112",
        },
        PoolDef {
            pool_type: PoolType::RaydiumCpmm,
            label: "RaydiumCpmm#2",
            address: "FbGTGvgmDLdegYEbWpKUGfNnSt7DaoRHN4SrvxmsDGpj",
            mint_a: "So11111111111111111111111111111111111111112",
            mint_b: "2oGTdmVgZQpnNVDTjJEPThM21QR1jZBuSCeLhHPmNBR7",
        },
        PoolDef {
            pool_type: PoolType::PumpFunAmm,
            label: "PumpFunAmm",
            address: "6cPfRuSp8L7f1TMt3vtKhqYYuHDoHZTHGNzQ6hRABtx6",
            mint_a: "6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz",
            mint_b: "So11111111111111111111111111111111111111112",
        },
        PoolDef {
            pool_type: PoolType::Meteora,
            label: "Meteora",
            address: "BCXjm4FfSoquZQJV5Wcje1g1pSHW2hFMU9wDE98Nyatb",
            mint_a: "STrikemJEk2tFVYpg7SMo9nGPrnJ56fHnS1K7PV2fPw",
            mint_b: "So11111111111111111111111111111111111111112",
        },
        PoolDef {
            pool_type: PoolType::MeteoraDamm,
            label: "MeteoraDamm",
            address: "4ac2qQp9uPQTuhvNH1p5xZmZnKV1pAxWKjBPkwvD6x6J",
            mint_a: "So11111111111111111111111111111111111111112",
            mint_b: "CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3",
        },
        PoolDef {
            pool_type: PoolType::FluxBeam,
            label: "FluxBeam",
            address: "5X2KrjQBVapwGzFwCWXkdFBv7JY6FpQJvdhejVH9PuwX",
            mint_a: "So11111111111111111111111111111111111111112",
            mint_b: "2JoJuvFip3PdPYScJYPBpWMGfXzRASoNjnQ1yPiapump",
        },
        PoolDef {
            pool_type: PoolType::RaydiumLp,
            label: "RaydiumLP",
            address: "6Lc76tcWsCEkydyLriNaeDkUgVekusBVGgQYYDiKRZi1",
            mint_a: "So11111111111111111111111111111111111111112",
            mint_b: "8Ki8DpuWNxu9VsS3kQbarsCWMcFGWkzzA8pUPto9zBd5",
        },
        PoolDef {
            pool_type: PoolType::Dooar,
            label: "Dooar",
            address: "5GGvf4rQ3yvA3iaQKHD8ZYm6TQjGEG3SoNm2H7kBEufT",
            mint_a: "So11111111111111111111111111111111111111112",
            mint_b: "85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ",
        },
    ]
}

/// Pre-warm cache: fetch all pool states from RPC and insert into cache.
async fn prewarm(rpc: &RpcClient, cache: &PoolCache, registry: &PoolRegistry) -> usize {
    let pools = stress_pools();
    let mut ok = 0;
    for p in &pools {
        let addr = pk(p.address);
        registry.add(PoolEntry {
            pool_type: p.pool_type,
            address: addr,
            mint_a: pk(p.mint_a),
            mint_b: pk(p.mint_b),
        });
        match fetcher::fetch_pool_state(rpc, p.pool_type, &addr).await {
            Ok(state) => {
                cache.insert(addr, state);
                ok += 1;
            }
            Err(e) => eprintln!("  WARN: failed to fetch {}: {}", p.label, e),
        }
    }
    ok
}

fn make_quote_req(input: Pubkey, output: Pubkey, amount: u64) -> QuoteRequest {
    QuoteRequest {
        input_mint: input,
        output_mint: output,
        amount,
        slippage_bps: 100,
        only_direct_routes: false,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    }
}

fn make_quote_req_direct(input: Pubkey, output: Pubkey, amount: u64) -> QuoteRequest {
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

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((sorted.len() as f64 * pct) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Token mints (non-SOL) for each pool, used for round-robin quoting.
fn non_sol_mints() -> Vec<&'static str> {
    vec![
        "25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook",
        "2oGTdmVgZQpnNVDTjJEPThM21QR1jZBuSCeLhHPmNBR7",
        "6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz",
        "STrikemJEk2tFVYpg7SMo9nGPrnJ56fHnS1K7PV2fPw",
        "CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3",
        "2JoJuvFip3PdPYScJYPBpWMGfXzRASoNjnQ1yPiapump",
        "8Ki8DpuWNxu9VsS3kQbarsCWMcFGWkzzA8pUPto9zBd5",
        "85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ",
    ]
}

// ─── TEST 1: Sustained concurrent quoting (60s) ───

#[tokio::test]
async fn test_stress_sustained_concurrent_quotes_60s() {
    eprintln!("\n=== STRESS TEST 1: SUSTAINED CONCURRENT QUOTING (60s) ===\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());
    let warmed = prewarm(&rpc_client, &cache, &registry).await;
    eprintln!("  Pre-warmed {warmed} pools into cache");
    assert!(warmed >= 6, "Need at least 6 pools");

    let quoter = Arc::new(Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc_client),
    ));

    let total_ok = Arc::new(AtomicU64::new(0));
    let total_err = Arc::new(AtomicU64::new(0));
    let total_latency_us = Arc::new(AtomicU64::new(0));

    let mints = non_sol_mints();
    let num_workers = 10;
    let duration = Duration::from_secs(60);
    let start = Instant::now();

    eprintln!("  Spawning {num_workers} workers for {duration:?}...\n");

    let mut handles = Vec::new();
    for worker_id in 0..num_workers {
        let q = Arc::clone(&quoter);
        let ok = Arc::clone(&total_ok);
        let err = Arc::clone(&total_err);
        let lat = Arc::clone(&total_latency_us);
        let mints = mints.clone();

        handles.push(tokio::spawn(async move {
            let mut local_ok = 0u64;
            let mut local_err = 0u64;
            let mut iter = 0u64;
            let worker_start = Instant::now();
            let amounts = [1_000_000u64, 10_000_000, 100_000_000, 1_000_000_000, 10_000_000_000];

            while worker_start.elapsed() < duration {
                let mint_idx = (iter as usize) % mints.len();
                let amount = amounts[(iter as usize) % amounts.len()];
                let output = pk(mints[mint_idx]);

                let req = make_quote_req(SOL_NATIVE_MINT, output, amount);

                let t = Instant::now();
                match q.quote(&req).await {
                    Ok(_) => {
                        local_ok += 1;
                        lat.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
                    }
                    Err(_) => local_err += 1,
                }
                iter += 1;
            }

            ok.fetch_add(local_ok, Ordering::Relaxed);
            err.fetch_add(local_err, Ordering::Relaxed);

            eprintln!(
                "    Worker {worker_id}: {local_ok} ok, {local_err} err in {:.1}s ({:.0} quotes/s)",
                worker_start.elapsed().as_secs_f64(),
                local_ok as f64 / worker_start.elapsed().as_secs_f64()
            );
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let ok_count = total_ok.load(Ordering::Relaxed);
    let err_count = total_err.load(Ordering::Relaxed);
    let total = ok_count + err_count;
    let avg_us = if ok_count > 0 {
        total_latency_us.load(Ordering::Relaxed) / ok_count
    } else {
        0
    };

    eprintln!("\n  ── RESULTS ──");
    eprintln!("  Duration:    {:.1}s", elapsed.as_secs_f64());
    eprintln!("  Total:       {total} quotes ({ok_count} ok, {err_count} err)");
    eprintln!("  Throughput:  {:.0} quotes/s", total as f64 / elapsed.as_secs_f64());
    eprintln!("  Avg latency: {avg_us}µs");
    eprintln!("  Error rate:  {:.2}%",
        if total > 0 { err_count as f64 / total as f64 * 100.0 } else { 0.0 });

    assert!(ok_count > 1000, "Expected 1000+ successful quotes, got {ok_count}");
    eprintln!("  [OK] Sustained load test passed\n");
}

// ─── TEST 2: Cache stampede — 100 concurrent tasks × 10K reads ───

#[tokio::test]
async fn test_stress_cache_stampede() {
    eprintln!("\n=== STRESS TEST 2: CACHE STAMPEDE (100 × 10K) ===\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());
    let warmed = prewarm(&rpc_client, &cache, &registry).await;
    eprintln!("  Pre-warmed {warmed} pools");

    // Only use pools that actually got cached (some may be closed)
    let pool_addrs: Vec<Pubkey> = stress_pools()
        .iter()
        .map(|p| pk(p.address))
        .filter(|addr| cache.get(addr).is_some())
        .collect();
    let cached_count = pool_addrs.len();
    eprintln!("  Using {cached_count} cached pools for stampede");

    let num_tasks = 100;
    let reads_per_task = 10_000;

    let start = Instant::now();
    let mut handles = Vec::new();

    for _task_id in 0..num_tasks {
        let c = Arc::clone(&cache);
        let addrs = pool_addrs.clone();

        handles.push(tokio::spawn(async move {
            let mut hits = 0u64;
            let mut misses = 0u64;
            for i in 0..reads_per_task {
                let addr = addrs[i % addrs.len()];
                if c.get(&addr).is_some() {
                    hits += 1;
                } else {
                    misses += 1;
                }
            }
            (hits, misses)
        }));
    }

    let mut total_hits = 0u64;
    let mut total_misses = 0u64;
    for h in handles {
        let (hits, misses) = h.await.unwrap();
        total_hits += hits;
        total_misses += misses;
    }

    let elapsed = start.elapsed();
    let total_ops = (num_tasks * reads_per_task) as u64;

    eprintln!("  {num_tasks} tasks × {reads_per_task} reads = {total_ops} total ops");
    eprintln!("  Hits: {total_hits}, Misses: {total_misses}");
    eprintln!("  Time: {:.1}ms", elapsed.as_secs_f64() * 1000.0);
    eprintln!("  Throughput: {:.1}M reads/s", total_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0);
    eprintln!("  Avg: {:.0}ns/read", elapsed.as_nanos() as f64 / total_ops as f64);

    assert_eq!(total_misses, 0, "No cache misses expected on pre-warmed data");
    assert!(elapsed.as_millis() < 5000, "1M reads should complete in <5s");
    eprintln!("  [OK] Cache stampede passed\n");
}

// ─── TEST 3: Quote → TX build pipeline under load ───

#[tokio::test]
async fn test_stress_quote_to_tx_pipeline() {
    eprintln!("\n=== STRESS TEST 3: QUOTE → TX BUILD PIPELINE (50 workers, 30s) ===\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());
    let warmed = prewarm(&rpc_client, &cache, &registry).await;
    eprintln!("  Pre-warmed {warmed} pools");

    let quoter = Arc::new(Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc_client),
    ));

    let user = pk("6TwqjGNQ8c2aUHvbpAjMd4bdHdone9CTrz3c8S71E2WW");
    let num_workers = 50;
    let duration = Duration::from_secs(30);

    let total_ok = Arc::new(AtomicU64::new(0));
    let total_err = Arc::new(AtomicU64::new(0));
    let all_latencies = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));

    let start = Instant::now();
    let mut handles = Vec::new();

    // PumpFunAmm has inline reserves — perfect for hot-cache testing
    let output_mint = pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz");

    for _worker in 0..num_workers {
        let q = Arc::clone(&quoter);
        let c = Arc::clone(&cache);
        let ok = Arc::clone(&total_ok);
        let err = Arc::clone(&total_err);
        let lats = Arc::clone(&all_latencies);

        handles.push(tokio::spawn(async move {
            let mut local_lats = Vec::new();
            let worker_start = Instant::now();
            let blockhash = solana_sdk::hash::Hash::new_unique();

            while worker_start.elapsed() < duration {
                let t = Instant::now();

                let req = make_quote_req_direct(SOL_NATIVE_MINT, output_mint, 100_000_000);

                match q.quote(&req).await {
                    Ok(resp) => {
                        let route = &resp.routes[0];
                        let pool_addr = pk(&route.pool.pool_address);
                        let pool_type = PoolType::from_str(&route.pool.dex)
                            .unwrap_or(PoolType::PumpFunAmm);

                        if let Some(state) = c.get(&pool_addr) {
                            let min_out: u64 = resp.minimum_out.parse().unwrap_or(1);
                            let order = SwapOrder {
                                pool_address: pool_addr,
                                pool_type,
                                input_mint: SOL_NATIVE_MINT,
                                output_mint,
                                amount_in: 100_000_000,
                                min_amount_out: min_out,
                                user,
                                input_token_program: TOKEN_PROGRAM_ID,
                                output_token_program: TOKEN_PROGRAM_ID,
                            };

                            let executor = AmmExecutorType::from_pool_type(pool_type);
                            if let Ok(executor) = executor {
                                if let Ok(swap_ix) = executor.build_swap_ix(&order, &state) {
                                    let config = TxBuildConfig::default();
                                    match build_unsigned_swap_message(&swap_ix, &user, &config, blockhash) {
                                        Ok(_) => {
                                            local_lats.push(t.elapsed().as_micros() as u64);
                                            ok.fetch_add(1, Ordering::Relaxed);
                                            continue;
                                        }
                                        Err(_) => {}
                                    }
                                }
                            }
                        }
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            lats.lock().await.extend(local_lats);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let ok_count = total_ok.load(Ordering::Relaxed);
    let err_count = total_err.load(Ordering::Relaxed);
    let total = ok_count + err_count;

    let mut lats = all_latencies.lock().await;
    lats.sort();

    eprintln!("  Duration:    {:.1}s", elapsed.as_secs_f64());
    eprintln!("  Workers:     {num_workers}");
    eprintln!("  Total:       {total} ({ok_count} ok, {err_count} err)");
    eprintln!("  Throughput:  {:.0} pipelines/s", total as f64 / elapsed.as_secs_f64());

    if !lats.is_empty() {
        eprintln!(
            "  Latency:     min={}µs avg={}µs p50={}µs p95={}µs p99={}µs max={}µs",
            lats[0],
            lats.iter().sum::<u64>() / lats.len() as u64,
            percentile(&lats, 0.50),
            percentile(&lats, 0.95),
            percentile(&lats, 0.99),
            lats[lats.len() - 1],
        );
    }

    assert!(ok_count > 500, "Expected 500+ full pipeline completions, got {ok_count}");
    eprintln!("  [OK] Pipeline stress test passed\n");
}

// ─── TEST 4: 2-hop routing under pressure ───

#[tokio::test]
async fn test_stress_two_hop_routing() {
    eprintln!("\n=== STRESS TEST 4: 2-HOP ROUTING UNDER PRESSURE (20 workers, 30s) ===\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());
    let warmed = prewarm(&rpc_client, &cache, &registry).await;
    eprintln!("  Pre-warmed {warmed} pools");

    let quoter = Arc::new(Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc_client),
    ));

    let total_ok = Arc::new(AtomicU64::new(0));
    let total_no_route = Arc::new(AtomicU64::new(0));
    let total_err = Arc::new(AtomicU64::new(0));
    let duration = Duration::from_secs(30);
    let num_workers = 20;

    let start = Instant::now();
    let mut handles = Vec::new();

    // Cross-pair: token_a -> token_b through SOL bridge
    let token_a = pk("25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook");
    let token_b = pk("CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3");

    for _w in 0..num_workers {
        let q = Arc::clone(&quoter);
        let ok = Arc::clone(&total_ok);
        let nr = Arc::clone(&total_no_route);
        let err = Arc::clone(&total_err);

        handles.push(tokio::spawn(async move {
            let worker_start = Instant::now();
            let amounts = [100_000_000u64, 1_000_000_000, 5_000_000_000];
            let mut iter = 0u64;

            while worker_start.elapsed() < duration {
                let req = make_quote_req(token_a, token_b, amounts[(iter as usize) % amounts.len()]);

                match q.quote(&req).await {
                    Ok(_) => { ok.fetch_add(1, Ordering::Relaxed); }
                    Err(e) => {
                        let msg = format!("{e}");
                        if msg.contains("NoRoute") || msg.contains("no route") {
                            nr.fetch_add(1, Ordering::Relaxed);
                        } else {
                            err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                iter += 1;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let ok_count = total_ok.load(Ordering::Relaxed);
    let nr_count = total_no_route.load(Ordering::Relaxed);
    let err_count = total_err.load(Ordering::Relaxed);
    let total = ok_count + nr_count + err_count;

    eprintln!("  Duration:    {:.1}s", elapsed.as_secs_f64());
    eprintln!("  Workers:     {num_workers}");
    eprintln!("  Total:       {total} ({ok_count} ok, {nr_count} no-route, {err_count} err)");
    eprintln!("  Throughput:  {:.0} 2-hop quotes/s", total as f64 / elapsed.as_secs_f64());

    assert!(ok_count > 100, "Expected 100+ 2-hop routes, got {ok_count}");
    assert_eq!(err_count, 0, "Expected 0 unexpected errors, got {err_count}");
    eprintln!("  [OK] 2-hop stress test passed\n");
}

// ─── TEST 5: Error path stress (10K bad requests, zero panics) ───

#[tokio::test]
async fn test_stress_error_paths() {
    eprintln!("\n=== STRESS TEST 5: ERROR PATH STRESS (10K invalid requests) ===\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());
    let _ = prewarm(&rpc_client, &cache, &registry).await;

    let quoter = Arc::new(Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc_client),
    ));

    let num_tasks = 20;
    let iters_per_task = 500;
    let start = Instant::now();
    let handled = Arc::new(AtomicU64::new(0));
    let panics = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    for _t in 0..num_tasks {
        let q = Arc::clone(&quoter);
        let h = Arc::clone(&handled);

        handles.push(tokio::spawn(async move {
            for i in 0..iters_per_task {
                let _result = match i % 5 {
                    0 => {
                        // Zero amount
                        q.quote(&QuoteRequest {
                            input_mint: SOL_NATIVE_MINT,
                            output_mint: pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz"),
                            amount: 0,
                            slippage_bps: 100,
                            only_direct_routes: false,
                            exclude_dexes: vec![],
                            dexes: vec![],
                            max_accounts: 64,
                        }).await
                    }
                    1 => {
                        // Same mint (input == output)
                        q.quote(&QuoteRequest {
                            input_mint: SOL_NATIVE_MINT,
                            output_mint: SOL_NATIVE_MINT,
                            amount: 1_000_000,
                            slippage_bps: 100,
                            only_direct_routes: false,
                            exclude_dexes: vec![],
                            dexes: vec![],
                            max_accounts: 64,
                        }).await
                    }
                    2 => {
                        // Nonexistent pair (valid pubkeys, no pools)
                        q.quote(&QuoteRequest {
                            input_mint: Pubkey::new_unique(),
                            output_mint: Pubkey::new_unique(),
                            amount: 1_000_000,
                            slippage_bps: 100,
                            only_direct_routes: false,
                            exclude_dexes: vec![],
                            dexes: vec![],
                            max_accounts: 64,
                        }).await
                    }
                    3 => {
                        // u64::MAX amount (overflow edge)
                        q.quote(&QuoteRequest {
                            input_mint: SOL_NATIVE_MINT,
                            output_mint: pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz"),
                            amount: u64::MAX,
                            slippage_bps: 100,
                            only_direct_routes: false,
                            exclude_dexes: vec![],
                            dexes: vec![],
                            max_accounts: 64,
                        }).await
                    }
                    _ => {
                        // Very large amount
                        q.quote(&QuoteRequest {
                            input_mint: SOL_NATIVE_MINT,
                            output_mint: pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz"),
                            amount: u64::MAX / 2,
                            slippage_bps: 100,
                            only_direct_routes: false,
                            exclude_dexes: vec![],
                            dexes: vec![],
                            max_accounts: 64,
                        }).await
                    }
                };
                h.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        match handle.await {
            Ok(()) => {}
            Err(_) => { panics.fetch_add(1, Ordering::Relaxed); }
        }
    }

    let elapsed = start.elapsed();
    let total_handled = handled.load(Ordering::Relaxed);
    let total_panics = panics.load(Ordering::Relaxed);

    eprintln!("  {num_tasks} tasks × {iters_per_task} invalid requests = {} total", num_tasks * iters_per_task);
    eprintln!("  Handled:  {total_handled}");
    eprintln!("  Panics:   {total_panics}");
    eprintln!("  Time:     {:.1}ms", elapsed.as_secs_f64() * 1000.0);

    assert_eq!(total_panics, 0, "No panics allowed on invalid input");
    assert_eq!(total_handled, (num_tasks * iters_per_task) as u64, "All requests must be handled");
    eprintln!("  [OK] Error path stress test passed — zero panics\n");
}

// ─── TEST 6: Mixed workload (quotes + TX builds + RPC fetches, 120s) ───

#[tokio::test]
async fn test_stress_mixed_workload_120s() {
    eprintln!("\n=== STRESS TEST 6: MIXED WORKLOAD (120s) ===\n");

    let rpc_client = rpc();
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());
    let warmed = prewarm(&rpc_client, &cache, &registry).await;
    eprintln!("  Pre-warmed {warmed} pools");

    let quoter = Arc::new(Quoter::new(
        Arc::clone(&registry),
        Arc::clone(&cache),
        Arc::clone(&rpc_client),
    ));

    let user = pk("6TwqjGNQ8c2aUHvbpAjMd4bdHdone9CTrz3c8S71E2WW");
    let duration = Duration::from_secs(120);

    let quote_count = Arc::new(AtomicU64::new(0));
    let build_count = Arc::new(AtomicU64::new(0));
    let fetch_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let output_mint = pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz");

    let start = Instant::now();
    let mut handles = Vec::new();

    // 5 quote workers (yield periodically so TX builders don't starve them)
    for _ in 0..5 {
        let q = Arc::clone(&quoter);
        let qc = Arc::clone(&quote_count);
        let ec = Arc::clone(&error_count);
        let mints = non_sol_mints();

        handles.push(tokio::spawn(async move {
            let ws = Instant::now();
            let amounts = [10_000_000u64, 100_000_000, 1_000_000_000];
            let mut iter = 0u64;
            while ws.elapsed() < duration {
                let mint = pk(mints[(iter as usize) % mints.len()]);
                let req = make_quote_req_direct(SOL_NATIVE_MINT, mint, amounts[(iter as usize) % amounts.len()]);
                match q.quote(&req).await {
                    Ok(_) => { qc.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => { ec.fetch_add(1, Ordering::Relaxed); }
                }
                iter += 1;
                if iter % 100 == 0 { tokio::task::yield_now().await; }
            }
        }));
    }

    // 3 TX build workers (yield every 100 iterations for fairness)
    for _ in 0..3 {
        let q = Arc::clone(&quoter);
        let c = Arc::clone(&cache);
        let bc = Arc::clone(&build_count);
        let ec = Arc::clone(&error_count);

        handles.push(tokio::spawn(async move {
            let ws = Instant::now();
            let blockhash = solana_sdk::hash::Hash::new_unique();
            let mut iter = 0u64;

            while ws.elapsed() < duration {
                let req = make_quote_req_direct(SOL_NATIVE_MINT, output_mint, 100_000_000);
                if let Ok(resp) = q.quote(&req).await {
                    let route = &resp.routes[0];
                    let pool_addr = pk(&route.pool.pool_address);
                    let pool_type = PoolType::from_str(&route.pool.dex).unwrap_or(PoolType::PumpFunAmm);
                    if let Some(state) = c.get(&pool_addr) {
                        let min_out: u64 = resp.minimum_out.parse().unwrap_or(1);
                        let order = SwapOrder {
                            pool_address: pool_addr,
                            pool_type,
                            input_mint: SOL_NATIVE_MINT,
                            output_mint,
                            amount_in: 100_000_000,
                            min_amount_out: min_out,
                            user,
                            input_token_program: TOKEN_PROGRAM_ID,
                            output_token_program: TOKEN_PROGRAM_ID,
                        };
                        if let Ok(executor) = AmmExecutorType::from_pool_type(pool_type) {
                            if let Ok(swap_ix) = executor.build_swap_ix(&order, &state) {
                                let config = TxBuildConfig::default();
                                if build_unsigned_swap_message(&swap_ix, &user, &config, blockhash).is_ok() {
                                    bc.fetch_add(1, Ordering::Relaxed);
                                    iter += 1;
                                    if iter % 100 == 0 { tokio::task::yield_now().await; }
                                    continue;
                                }
                            }
                        }
                    }
                }
                ec.fetch_add(1, Ordering::Relaxed);
                iter += 1;
                if iter % 100 == 0 { tokio::task::yield_now().await; }
            }
        }));
    }

    // 2 cache refresh workers (re-fetch from RPC periodically)
    for _ in 0..2 {
        let r = Arc::clone(&rpc_client);
        let c = Arc::clone(&cache);
        let fc = Arc::clone(&fetch_count);
        let ec = Arc::clone(&error_count);

        handles.push(tokio::spawn(async move {
            let ws = Instant::now();
            let pools = stress_pools();
            let mut iter = 0usize;
            while ws.elapsed() < duration {
                let p = &pools[iter % pools.len()];
                let addr = pk(p.address);
                match fetcher::fetch_pool_state(&r, p.pool_type, &addr).await {
                    Ok(state) => {
                        c.insert(addr, state);
                        fc.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => { ec.fetch_add(1, Ordering::Relaxed); }
                }
                iter += 1;
                // Small delay to avoid RPC hammering
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let quotes = quote_count.load(Ordering::Relaxed);
    let builds = build_count.load(Ordering::Relaxed);
    let fetches = fetch_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);
    let total = quotes + builds + fetches + errors;

    eprintln!("  ── RESULTS ({:.0}s) ──", elapsed.as_secs_f64());
    eprintln!("  Quotes:     {quotes:>8} ({:.0}/s)", quotes as f64 / elapsed.as_secs_f64());
    eprintln!("  TX Builds:  {builds:>8} ({:.0}/s)", builds as f64 / elapsed.as_secs_f64());
    eprintln!("  RPC Fetches:{fetches:>8} ({:.0}/s)", fetches as f64 / elapsed.as_secs_f64());
    eprintln!("  Errors:     {errors:>8}");
    eprintln!("  Total ops:  {total:>8} ({:.0}/s)", total as f64 / elapsed.as_secs_f64());

    let total_work = quotes + builds + fetches;
    assert!(total_work > 10_000, "Expected 10K+ total ops, got {total_work}");
    assert!(builds > 1_000, "Expected 1K+ TX builds, got {builds}");
    eprintln!("  [OK] Mixed workload stress test passed\n");
}
