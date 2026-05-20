//! Pumpup-specific latency probe.
//!
//! Measures:
//!  1. Cold fetch (RPC) for the verified mainnet Pumpup pool
//!  2. Hot cache lookup
//!  3. Build swap instruction (just the IX builder, no RPC)
//!  4. Quote evaluation through the router (using inline reserves — zero RPC)
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! export SOL_HTTPS_ENDPOINT="$RPC_URL"
//! cargo test --test e2e_pumpup_latency --release -- --nocapture --test-threads=1
//! ```

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

use flow_trades::constants::{PUMPUP_PROG_ID, TOKEN_PROGRAM_ID};
use flow_trades::execution::amms::{AmmExecutor, pumpup::PumpupExecutor};
use flow_trades::pool::{
    cache::PoolCache,
    fetcher,
    registry::{PoolEntry, PoolRegistry},
    types::{PoolType, SwapOrder},
};
use flow_trades::quote::{QuoteRequest, Quoter};

const PUMPUP_POOL: &str = "7Q9RYYbijphbAXBV527Jz2QmgY4BXdaAzfXhJ3wT8hv1";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
// token_a_mint of pool 7Q9RYY... per on-chain Pool data
const ANNCZ_MINT: &str = "AnncZ1M8BbE8GVPrqJvecff4G7FzQvzpfMt4JvWddGai";

fn rpc_url() -> String {
    std::env::var("SOL_HTTPS_ENDPOINT")
        .or_else(|_| std::env::var("RPC_URL"))
        .expect("SOL_HTTPS_ENDPOINT or RPC_URL must be set")
}

#[tokio::test]
async fn test_pumpup_latency_probe() {
    let rpc = Arc::new(RpcClient::new(rpc_url()));
    let pool_addr = Pubkey::from_str(PUMPUP_POOL).unwrap();
    let usdt = Pubkey::from_str(USDT_MINT).unwrap();
    let anncz = Pubkey::from_str(ANNCZ_MINT).unwrap();

    eprintln!("\n+-{:-<68}-+", "- PUMPUP LATENCY PROBE ");
    eprintln!("| pool: {PUMPUP_POOL}");

    // ── 1. Cold fetch ──
    let cold_start = Instant::now();
    let pool_state = fetcher::fetch_pool_state(&rpc, PoolType::Pumpup, &pool_addr)
        .await
        .expect("cold fetch failed");
    let cold_us = cold_start.elapsed().as_micros();
    eprintln!("| cold fetch (RPC):   {:>6}us  ({:.1} ms)", cold_us, cold_us as f64 / 1000.0);

    // ── 2. Hot cache lookup ──
    let cache = Arc::new(PoolCache::new(120_000));
    cache.insert(pool_addr, pool_state.clone());
    // Take 100 samples to get a real distribution
    let mut hot_us: Vec<u128> = Vec::with_capacity(100);
    for _ in 0..100 {
        let t = Instant::now();
        let _ = cache.get(&pool_addr);
        hot_us.push(t.elapsed().as_nanos());
    }
    hot_us.sort();
    let hot_median_ns = hot_us[50];
    let hot_p99_ns = hot_us[99];
    eprintln!("| hot cache (median): {:>6}ns  (p99 {}ns)", hot_median_ns, hot_p99_ns);

    // ── 3. Build swap instruction (no RPC) ──
    let user = Pubkey::new_unique();
    let order = SwapOrder {
        pool_address: pool_addr,
        pool_type: PoolType::Pumpup,
        input_mint: usdt,
        output_mint: anncz,
        amount_in: 50_000,    // 0.05 USDT
        min_amount_out: 1,
        user,
        input_token_program: TOKEN_PROGRAM_ID,
        output_token_program: TOKEN_PROGRAM_ID,
    };
    // Take 1000 samples — the IX builder is pure compute, sub-µs each
    let mut build_ns: Vec<u128> = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let t = Instant::now();
        let _ = PumpupExecutor.build_swap_ix(&order, &pool_state).expect("build_swap_ix");
        build_ns.push(t.elapsed().as_nanos());
    }
    build_ns.sort();
    let build_median_ns = build_ns[500];
    let build_p99_ns = build_ns[990];
    eprintln!("| build_swap_ix:      {:>6}ns  (p99 {}ns) — {:.2}µs median",
        build_median_ns, build_p99_ns, build_median_ns as f64 / 1000.0);

    // ── 4. End-to-end quote through Quoter (uses inline reserves, no extra RPC) ──
    let registry = Arc::new(PoolRegistry::new());
    registry.add(PoolEntry {
        address: pool_addr,
        pool_type: PoolType::Pumpup,
        mint_a: anncz,
        mint_b: usdt,
    });
    let quoter = Quoter::new(registry, cache.clone(), rpc.clone());
    let req = QuoteRequest {
        input_mint: usdt,
        output_mint: anncz,
        amount: 50_000,
        slippage_bps: 50,
        only_direct_routes: true,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };
    // Warm up
    let _ = quoter.quote(&req).await;
    // 100-sample distribution
    let mut quote_ns: Vec<u128> = Vec::with_capacity(100);
    let mut ok_count = 0u32;
    for _ in 0..100 {
        let t = Instant::now();
        if quoter.quote(&req).await.is_ok() {
            ok_count += 1;
        }
        quote_ns.push(t.elapsed().as_nanos());
    }
    quote_ns.sort();
    let quote_median_us = quote_ns[50] as f64 / 1000.0;
    let quote_p99_us = quote_ns[99] as f64 / 1000.0;
    eprintln!("| quoter.quote() hot: {:.2}µs median, {:.2}µs p99 ({} ok)",
        quote_median_us, quote_p99_us, ok_count);

    eprintln!("+-{:-<68}-+", "");
    eprintln!("\nSummary:");
    eprintln!("  Cold fetch:    {} us ({:.1} ms)", cold_us, cold_us as f64 / 1000.0);
    eprintln!("  Hot cache:     {} ns (sub-microsecond)", hot_median_ns);
    eprintln!("  Build IX:      {} ns ({:.2} µs)", build_median_ns, build_median_ns as f64 / 1000.0);
    eprintln!("  Quote pipeline:{:.2} µs", quote_median_us);

    // Sanity assertions
    assert!(cold_us < 1_000_000, "cold fetch should be < 1s (got {} us)", cold_us);
    assert!(build_median_ns < 50_000, "build_swap_ix should be < 50µs (got {} ns)", build_median_ns);
    assert!(quote_median_us < 1000.0, "quoter.quote() should be < 1ms hot (got {:.2} µs)", quote_median_us);
    assert!(ok_count >= 95, "expected >= 95/100 quotes to succeed, got {}", ok_count);
}
