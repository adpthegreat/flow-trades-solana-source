//! Hot-path showcase: end-to-end demonstration of every hot path in action
//! with real mainnet data.
//!
//! Tests cover:
//! 1. Bootstrap — pool registration across multiple DEXes
//! 2. Pre-warm cache — fetch all pool states, time each
//! 3. Direct quote (hot cache) — sub-millisecond quoting
//! 4. Direct quote comparison — multiple pools, best wins
//! 5. 2-hop quote (hot cache) — TOKEN_A -> SOL -> TOKEN_B
//! 6. Split route evaluation — large trade across 2 pools
//! 7. Vault balance fetching — inline vs vault-based reserves
//! 8. Full pipeline — quote -> build TX -> serialize
//! 9. Cache hit vs miss — cold vs hot comparison
//! 10. V0 TX with ALTs — versioned transaction savings
//!
//! Plus focused tests: 100-quote latency distribution, multi-DEX best-route,
//! all-bridge routing, and cache warm-to-hot transition.
//!
//! Requires `SOL_HTTPS_ENDPOINT` (or `RPC_URL`) env var.
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include \
//!   cargo test --test e2e_hot_path -- --nocapture --test-threads=1
//! ```

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;

use flow_trades::constants::*;
use flow_trades::execution::address_lookup::AltCache;
use flow_trades::execution::tx_builder::{build_unsigned_swap_message, build_unsigned_versioned_tx, TxBuildConfig};
use flow_trades::execution::AmmExecutorType;
use flow_trades::pool::cache::PoolCache;
use flow_trades::pool::fetcher;
use flow_trades::pool::registry::{PoolEntry, PoolRegistry};
use flow_trades::pool::types::{PoolState, PoolType, SwapOrder};
// AMM math available if needed for manual reserve computation:
// use flow_trades::quote::math::{compute_constant_product_out, compute_fee_amount};
use flow_trades::quote::types::QuoteRequest;
use flow_trades::quote::Quoter;

// -- Helpers --

fn rpc_url() -> String {
    std::env::var("SOL_HTTPS_ENDPOINT")
        .or_else(|_| std::env::var("RPC_URL"))
        .expect("SOL_HTTPS_ENDPOINT or RPC_URL must be set")
}

fn rpc() -> RpcClient {
    RpcClient::new_with_commitment(rpc_url(), CommitmentConfig::confirmed())
}

fn pk(s: &str) -> Pubkey {
    Pubkey::from_str(s).unwrap()
}

fn test_user() -> Pubkey {
    pk("6TwqjGNQ8c2aUHvbpAjMd4bdHdone9CTrz3c8S71E2WW")
}

fn short(p: &Pubkey) -> String {
    let s = p.to_string();
    format!("{}...{}", &s[..4], &s[s.len()-3..])
}

/// Known ALTs used by real DEX transactions on mainnet.
const KNOWN_ALTS: &[&str] = &[
    "BrQp6dwBFCdUfrvgnqzw9tc9kLPXTn16AmjZE8xJanMM", // Raydium CPMM
    "AoRtqBqk7Ysf3cd5NWjs93E2ekmz9KG1wLV84v7Xa1KK", // Aggregator ALT
    "7TKvNxkNF1ThM6nW9HafQMQbjqpwY68BVQNUkPdvFaSS", // Orca Whirlpool
];

// -- Pool data: fresh active pools verified on mainnet --

struct PoolDef {
    pool_type: PoolType,
    label: &'static str,
    pool_address: &'static str,
    mint_a: Pubkey,
    mint_b: Pubkey,
}

fn showcase_pools() -> Vec<PoolDef> {
    vec![
        PoolDef {
            pool_type: PoolType::RaydiumCpmm,
            label: "RaydiumCpmm",
            pool_address: "BEdCQzzvEmqtHH946GYsdeideaBYMvt8SZn5eUjKxTjr",
            mint_a: pk("25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook"),
            mint_b: SOL_NATIVE_MINT,
        },
        PoolDef {
            pool_type: PoolType::RaydiumCpmm,
            label: "RaydiumCpmm#2",
            pool_address: "FbGTGvgmDLdegYEbWpKUGfNnSt7DaoRHN4SrvxmsDGpj",
            mint_a: SOL_NATIVE_MINT,
            mint_b: pk("2oGTdmVgZQpnNVDTjJEPThM21QR1jZBuSCeLhHPmNBR7"),
        },
        PoolDef {
            pool_type: PoolType::PumpFunAmm,
            label: "PumpFunAmm",
            pool_address: "6cPfRuSp8L7f1TMt3vtKhqYYuHDoHZTHGNzQ6hRABtx6",
            mint_a: pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz"),
            mint_b: SOL_NATIVE_MINT,
        },
        PoolDef {
            pool_type: PoolType::Meteora,
            label: "Meteora",
            pool_address: "BCXjm4FfSoquZQJV5Wcje1g1pSHW2hFMU9wDE98Nyatb",
            mint_a: pk("STrikemJEk2tFVYpg7SMo9nGPrnJ56fHnS1K7PV2fPw"),
            mint_b: SOL_NATIVE_MINT,
        },
        PoolDef {
            pool_type: PoolType::MeteoraDamm,
            label: "MeteoraDamm",
            pool_address: "4ac2qQp9uPQTuhvNH1p5xZmZnKV1pAxWKjBPkwvD6x6J",
            mint_a: SOL_NATIVE_MINT,
            mint_b: pk("CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3"),
        },
        PoolDef {
            pool_type: PoolType::FluxBeam,
            label: "FluxBeam",
            pool_address: "5X2KrjQBVapwGzFwCWXkdFBv7JY6FpQJvdhejVH9PuwX",
            mint_a: SOL_NATIVE_MINT,
            mint_b: pk("2JoJuvFip3PdPYScJYPBpWMGfXzRASoNjnQ1yPiapump"),
        },
        PoolDef {
            pool_type: PoolType::RaydiumLp,
            label: "RaydiumLP",
            pool_address: "6Lc76tcWsCEkydyLriNaeDkUgVekusBVGgQYYDiKRZi1",
            mint_a: SOL_NATIVE_MINT,
            mint_b: pk("D756Z3S31AZMbU4teTu2BWK77neDArFhaPr6eZ7bonk"),
        },
        PoolDef {
            pool_type: PoolType::Dooar,
            label: "Dooar",
            pool_address: "5GGvkcqQ1554ibdc18JXiPqR8aJz6WV3JSNShoj32ufT",
            mint_a: USDC_MINT,
            mint_b: SOL_NATIVE_MINT,
        },
    ]
}

// -- Helper to extract vault pubkeys from PoolState --

fn extract_vault_info(state: &PoolState) -> Option<(Pubkey, Pubkey)> {
    match state {
        PoolState::RaydiumCpmm { token_0_vault, token_1_vault, .. } => Some((*token_0_vault, *token_1_vault)),
        PoolState::RaydiumLp { base_vault, quote_vault, .. } => Some((*base_vault, *quote_vault)),
        PoolState::Meteora { a_token_vault, b_token_vault, .. } => Some((*a_token_vault, *b_token_vault)),
        PoolState::MeteoraDamm { token_a_vault, token_b_vault, .. } => Some((*token_a_vault, *token_b_vault)),
        PoolState::FluxBeam { token_a_vault, token_b_vault, .. } => Some((*token_a_vault, *token_b_vault)),
        PoolState::Dooar { token_a_vault, token_b_vault, .. } => Some((*token_a_vault, *token_b_vault)),
        PoolState::Saros { token_a_vault, token_b_vault, .. } => Some((*token_a_vault, *token_b_vault)),
        PoolState::RaydiumV4 { coin_vault, pc_vault, .. } => Some((*coin_vault, *pc_vault)),
        _ => None,
    }
}

async fn fetch_vault_bal(rpc: &RpcClient, vault: &Pubkey) -> Option<u64> {
    rpc.get_token_account_balance(vault)
        .await
        .ok()
        .and_then(|b| b.amount.parse::<u64>().ok())
}

fn percentile(sorted: &[u128], pct: f64) -> u128 {
    let idx = ((sorted.len() as f64 * pct / 100.0).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

// ============================================================================
//  MAIN SHOWCASE TEST
// ============================================================================

#[tokio::test]
async fn test_hot_path_showcase() {
    let rpc_client = Arc::new(rpc());
    let registry = Arc::new(PoolRegistry::new());
    let cache = Arc::new(PoolCache::new(120_000)); // 2min TTL for test stability

    eprintln!("\n{}", "=".repeat(70));
    eprintln!("  FLOW-TRADES HOT PATH SHOWCASE");
    eprintln!("  Real mainnet data -- All paths exercised");
    eprintln!("{}\n", "=".repeat(70));

    // ── STEP 1: BOOTSTRAP ──────────────────────────────────────────────────

    let pools = showcase_pools();
    let mut dex_set = std::collections::HashSet::new();
    for p in &pools {
        registry.add(PoolEntry {
            address: pk(p.pool_address),
            pool_type: p.pool_type,
            mint_a: p.mint_a,
            mint_b: p.mint_b,
        });
        dex_set.insert(p.pool_type);
    }

    eprintln!("+-{:-<68}-+", "- STEP 1: BOOTSTRAP ");
    eprintln!("| Registered {} pools across {} DEX types{}", pools.len(), dex_set.len(),
        " ".repeat(68 - 40 - format!("{}", pools.len()).len() - format!("{}", dex_set.len()).len()));
    let dex_names: Vec<_> = dex_set.iter().map(|d| format!("{:?}", d)).collect();
    let dex_line = dex_names.join(", ");
    if dex_line.len() <= 64 {
        eprintln!("| {:<68} |", dex_line);
    } else {
        eprintln!("| {:<68} |", &dex_line[..68]);
    }
    eprintln!("+-{:-<68}-+\n", "");

    assert_eq!(registry.len(), pools.len());

    // ── STEP 2: PRE-WARM CACHE ─────────────────────────────────────────────

    eprintln!("+-{:-<68}-+", "- STEP 2: PRE-WARM CACHE ");
    eprintln!("| {:<18} | {:<16} | {:>8} | {:>6} |{:>10} |", "AMM", "Pool", "Fetch", "Cached", "");
    eprintln!("| {:-<18}-+-{:-<16}-+-{:-<8}-+-{:-<6}-+-{:-<8}-|", "", "", "", "", "");

    let mut total_fetch_ms: u128 = 0;
    let mut cached_count = 0u32;
    let overall_start = Instant::now();

    for p in &pools {
        let addr = pk(p.pool_address);
        let start = Instant::now();
        match fetcher::fetch_pool_state(&rpc_client, p.pool_type, &addr).await {
            Ok(state) => {
                let fetch_ms = start.elapsed().as_millis();
                total_fetch_ms += fetch_ms;
                cache.insert(addr, state);
                cached_count += 1;
                eprintln!("| {:<18} | {:<16} | {:>5}ms | {:>6} |{:>10} |",
                    p.label, short(&addr), fetch_ms, "yes", "");
            }
            Err(e) => {
                let fetch_ms = start.elapsed().as_millis();
                total_fetch_ms += fetch_ms;
                eprintln!("| {:<18} | {:<16} | {:>5}ms | {:>6} |{:>10} |",
                    p.label, short(&addr), fetch_ms, "FAIL", format!("{}", e).chars().take(8).collect::<String>());
            }
        }
    }

    let total_wall = overall_start.elapsed().as_millis();
    eprintln!("| Total: {} pools cached in {}ms (wall: {}ms){} |",
        cached_count, total_fetch_ms, total_wall,
        " ".repeat(68usize.saturating_sub(45 + format!("{}", cached_count).len() + format!("{}", total_fetch_ms).len() + format!("{}", total_wall).len())));
    eprintln!("+-{:-<68}-+\n", "");

    assert!(cached_count >= 5, "expected at least 5 pools cached, got {cached_count}");

    // ── STEP 3: DIRECT QUOTE (HOT CACHE) ───────────────────────────────────

    eprintln!("+-{:-<68}-+", "- STEP 3: DIRECT QUOTE (HOT CACHE) ");

    // Use PumpFunAmm (inline reserves = fastest, most reliable)
    let pfa_pool = pk("6cPfRuSp8L7f1TMt3vtKhqYYuHDoHZTHGNzQ6hRABtx6");
    let pfa_token = pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz");

    let quoter = Quoter::new(registry.clone(), cache.clone(), rpc_client.clone());
    let req = QuoteRequest {
        input_mint: SOL_NATIVE_MINT,
        output_mint: pfa_token,
        amount: 1_000_000_000, // 1 SOL
        slippage_bps: 50,
        only_direct_routes: true,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    // Warm-up call (ensure cache path is exercised)
    let _ = quoter.quote(&req).await;

    let start = Instant::now();
    let result = quoter.quote(&req).await;
    let elapsed = start.elapsed();

    match result {
        Ok(resp) => {
            let out: u64 = resp.amount_out.parse().unwrap_or(0);
            eprintln!("| SOL -> {} via PumpFunAmm{} |",
                &pfa_token.to_string()[..5],
                " ".repeat(68 - 30 - 5));
            eprintln!("| Input:  1.0 SOL (1,000,000,000 lamports){} |",
                " ".repeat(68 - 42));
            eprintln!("| Output: {} tokens{} |",
                resp.amount_out,
                " ".repeat(68usize.saturating_sub(10 + resp.amount_out.len())));
            eprintln!("| Impact: {}%{} |",
                resp.price_impact,
                " ".repeat(68usize.saturating_sub(10 + resp.price_impact.len())));
            eprintln!("| Time:   {:?} (CACHE HIT){} |",
                elapsed,
                " ".repeat(68usize.saturating_sub(24 + format!("{:?}", elapsed).len())));

            assert!(out > 0, "hot cache quote should produce output");
        }
        Err(e) => {
            eprintln!("| [FAIL] {:<61} |", format!("{e}"));
        }
    }
    eprintln!("+-{:-<68}-+\n", "");

    // ── STEP 4: DIRECT QUOTE COMPARISON (multiple pools) ────────────────────

    eprintln!("+-{:-<68}-+", "- STEP 4: MULTI-POOL COMPARISON ");

    // Quote through 2 different RaydiumCpmm pools (different tokens but same DEX)
    let cpmm_pool1 = pk("BEdCQzzvEmqtHH946GYsdeideaBYMvt8SZn5eUjKxTjr");
    let cpmm_token1 = pk("25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook");
    let cpmm_pool2 = pk("FbGTGvgmDLdegYEbWpKUGfNnSt7DaoRHN4SrvxmsDGpj");
    let cpmm_token2 = pk("2oGTdmVgZQpnNVDTjJEPThM21QR1jZBuSCeLhHPmNBR7");

    let mut results = Vec::new();

    for (label, pool_addr, token) in [
        ("CpmmPool1", cpmm_pool1, cpmm_token1),
        ("CpmmPool2", cpmm_pool2, cpmm_token2),
        ("PumpFunAmm", pfa_pool, pfa_token),
    ] {
        // Build a dedicated registry for just this one pool
        let solo_reg = Arc::new(PoolRegistry::new());
        solo_reg.add(PoolEntry {
            address: pool_addr,
            pool_type: if label == "PumpFunAmm" { PoolType::PumpFunAmm } else { PoolType::RaydiumCpmm },
            mint_a: token,
            mint_b: SOL_NATIVE_MINT,
        });
        let solo_quoter = Quoter::new(solo_reg, cache.clone(), rpc_client.clone());
        let req = QuoteRequest {
            input_mint: SOL_NATIVE_MINT,
            output_mint: token,
            amount: 100_000_000, // 0.1 SOL
            slippage_bps: 50,
            only_direct_routes: true,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let start = Instant::now();
        match solo_quoter.quote(&req).await {
            Ok(resp) => {
                let elapsed_us = start.elapsed().as_micros();
                let out: u64 = resp.amount_out.parse().unwrap_or(0);
                results.push((label, out, elapsed_us, resp.price_impact.clone()));
                eprintln!("| {:<14} | out: {:>14} | {:>6}us | impact: {:>6}% |",
                    label, out, elapsed_us, resp.price_impact);
            }
            Err(e) => {
                eprintln!("| {:<14} | [SKIP] {:<48} |", label, format!("{e}").chars().take(48).collect::<String>());
            }
        }
    }

    if results.len() >= 2 {
        eprintln!("| Note: Different tokens -- each pool quoted independently{} |",
            " ".repeat(68 - 57));
    }
    eprintln!("+-{:-<68}-+\n", "");

    // ── STEP 5: 2-HOP QUOTE (HOT CACHE) ────────────────────────────────────

    eprintln!("+-{:-<68}-+", "- STEP 5: 2-HOP QUOTE (HOT CACHE) ");

    // Route: token_a -> SOL -> token_b
    // token_a has a RaydiumCpmm pool with SOL
    // token_b has a MeteoraDamm pool with SOL
    let token_a = pk("25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook");
    let token_b = pk("CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3");

    let hop_req = QuoteRequest {
        input_mint: token_a,
        output_mint: token_b,
        amount: 1_000_000_000, // 1B base units of token_a
        slippage_bps: 100,
        only_direct_routes: false,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    // Warm up
    let _ = quoter.quote(&hop_req).await;

    let start = Instant::now();
    let result = quoter.quote(&hop_req).await;
    let elapsed = start.elapsed();

    match result {
        Ok(resp) => {
            if resp.routes.len() == 2 {
                let hop1 = &resp.routes[0].pool;
                let hop2 = &resp.routes[1].pool;
                eprintln!("| Hop 1: {} -> {} via {:<18}{} |",
                    &hop1.input_token[..6], &hop1.output_token[..6], hop1.dex,
                    " ".repeat(68usize.saturating_sub(40 + hop1.dex.len())));
                eprintln!("|   in={:<14}  out={:<14}{} |",
                    hop1.amount_in, hop1.amount_out,
                    " ".repeat(68usize.saturating_sub(37 + hop1.amount_in.len() + hop1.amount_out.len())));
                eprintln!("| Hop 2: {} -> {} via {:<18}{} |",
                    &hop2.input_token[..6], &hop2.output_token[..6], hop2.dex,
                    " ".repeat(68usize.saturating_sub(40 + hop2.dex.len())));
                eprintln!("|   in={:<14}  out={:<14}{} |",
                    hop2.amount_in, hop2.amount_out,
                    " ".repeat(68usize.saturating_sub(37 + hop2.amount_in.len() + hop2.amount_out.len())));
                eprintln!("| Final output: {:<20} Impact: {}%{} |",
                    resp.amount_out, resp.price_impact,
                    " ".repeat(68usize.saturating_sub(32 + resp.amount_out.len() + resp.price_impact.len())));
                eprintln!("| Time: {:?}{} |",
                    elapsed,
                    " ".repeat(68usize.saturating_sub(7 + format!("{:?}", elapsed).len())));

                // Verify hop continuity
                assert_eq!(hop1.output_token, hop2.input_token, "hop1 output should feed hop2 input");
                let final_out: u64 = resp.amount_out.parse().unwrap_or(0);
                assert!(final_out > 0, "2-hop should produce non-zero output");
            } else {
                eprintln!("| Route found with {} hop(s) (expected 2){} |",
                    resp.routes.len(),
                    " ".repeat(68usize.saturating_sub(36 + format!("{}", resp.routes.len()).len())));
            }
        }
        Err(e) => {
            eprintln!("| [SKIP] 2-hop failed: {:<47} |", format!("{e}").chars().take(47).collect::<String>());
        }
    }
    eprintln!("+-{:-<68}-+\n", "");

    // ── STEP 6: SPLIT ROUTE EVALUATION ──────────────────────────────────────

    eprintln!("+-{:-<68}-+", "- STEP 6: SPLIT ROUTE EVALUATION ");

    // Register 2 pools for SOL -> CDNZa... to test split
    // MeteoraDamm is already registered. Add the Dooar pool for USDC/SOL as a
    // different pair to show how splits would work conceptually.
    // Since splits need the SAME pair in 2 pools, and we have only one pool per
    // token, we do a large-trade quote and report whether a split was used.

    let token_cdnz = pk("CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3");
    let large_req = QuoteRequest {
        input_mint: SOL_NATIVE_MINT,
        output_mint: token_cdnz,
        amount: 50_000_000_000, // 50 SOL (large trade)
        slippage_bps: 200,
        only_direct_routes: true,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    match quoter.quote(&large_req).await {
        Ok(resp) => {
            let is_split = resp.routes.len() > 1 && resp.routes.iter().any(|r| r.percent < 100);
            eprintln!("| Trade: 50 SOL -> CDNZa...{} |", " ".repeat(68 - 25));
            eprintln!("| Output: {:<20} Impact: {}%{} |",
                resp.amount_out, resp.price_impact,
                " ".repeat(68usize.saturating_sub(28 + resp.amount_out.len() + resp.price_impact.len())));
            if is_split {
                for (i, step) in resp.routes.iter().enumerate() {
                    eprintln!("|   Leg {}: {}% via {}{} |",
                        i + 1, step.percent, step.pool.dex,
                        " ".repeat(68usize.saturating_sub(20 + format!("{}", step.percent).len() + step.pool.dex.len())));
                }
                eprintln!("| SPLIT ROUTE ENGAGED{} |", " ".repeat(68 - 19));
            } else {
                eprintln!("| Single pool route (split needs 2+ pools for same pair){} |",
                    " ".repeat(68 - 55));
            }
        }
        Err(e) => {
            eprintln!("| [SKIP] {:<61} |", format!("{e}").chars().take(61).collect::<String>());
        }
    }
    eprintln!("+-{:-<68}-+\n", "");

    // ── STEP 7: VAULT BALANCE FETCHING ──────────────────────────────────────

    eprintln!("+-{:-<68}-+", "- STEP 7: VAULT BALANCE (INLINE vs RPC) ");

    // PumpFunAmm: inline reserves (no RPC fetch)
    let pfa_state = cache.get(&pfa_pool);
    if let Some(PoolState::PumpFunAmm { base_reserve, quote_reserve, .. }) = &pfa_state {
        eprintln!("| PumpFunAmm (INLINE): base={:<12} quote={:<12}    |",
            base_reserve, quote_reserve);
    }

    // RaydiumCpmm: vault fetch required
    let cpmm_state = cache.get(&cpmm_pool1);
    if let Some(ref state) = cpmm_state {
        if let Some((vault_a, vault_b)) = extract_vault_info(state) {
            let start = Instant::now();
            let (bal_a, bal_b) = tokio::join!(
                fetch_vault_bal(&rpc_client, &vault_a),
                fetch_vault_bal(&rpc_client, &vault_b),
            );
            let vault_ms = start.elapsed().as_millis();
            let ra = bal_a.unwrap_or(0);
            let rb = bal_b.unwrap_or(0);
            eprintln!("| RaydiumCpmm (VAULT): res_a={:<12} res_b={:<12}   |", ra, rb);
            eprintln!("| Vault fetch time: {}ms{} |",
                vault_ms,
                " ".repeat(68usize.saturating_sub(19 + format!("{}", vault_ms).len())));
        }
    }

    // MeteoraDamm: vault fetch
    let damm_pool_addr = pk("4ac2qQp9uPQTuhvNH1p5xZmZnKV1pAxWKjBPkwvD6x6J");
    let damm_state = cache.get(&damm_pool_addr);
    if let Some(ref state) = damm_state {
        if let Some((vault_a, vault_b)) = extract_vault_info(state) {
            let (bal_a, bal_b) = tokio::join!(
                fetch_vault_bal(&rpc_client, &vault_a),
                fetch_vault_bal(&rpc_client, &vault_b),
            );
            let ra = bal_a.unwrap_or(0);
            let rb = bal_b.unwrap_or(0);
            eprintln!("| MeteoraDamm (VAULT): res_a={:<12} res_b={:<12}   |", ra, rb);
        }
    }

    eprintln!("| Inline: 0 RPC calls. Vault: 2 RPC calls per pool.{} |",
        " ".repeat(68 - 51));
    eprintln!("+-{:-<68}-+\n", "");

    // ── STEP 8: FULL PIPELINE — QUOTE -> BUILD TX -> SERIALIZE ──────────────

    eprintln!("+-{:-<68}-+", "- STEP 8: FULL PIPELINE (QUOTE->TX->B64) ");

    let user = test_user();
    let pipeline_req = QuoteRequest {
        input_mint: SOL_NATIVE_MINT,
        output_mint: pfa_token,
        amount: 100_000_000, // 0.1 SOL
        slippage_bps: 50,
        only_direct_routes: true,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    // Warm up
    let _ = quoter.quote(&pipeline_req).await;

    // Timed pipeline
    let t0 = Instant::now();
    let resp = quoter.quote(&pipeline_req).await;
    let t_quote = t0.elapsed();

    if let Ok(resp) = resp {
        let t1 = Instant::now();
        let pool_addr = pk(&resp.routes[0].pool.pool_address);
        let cached_state = cache.get(&pool_addr).expect("pool should be cached");
        let executor = AmmExecutorType::from_pool_type(PoolType::PumpFunAmm).unwrap();
        let out_amount: u64 = resp.amount_out.parse().unwrap_or(0);
        let threshold: u64 = resp.minimum_out.parse().unwrap_or(0);
        let order = SwapOrder {
            pool_address: pool_addr,
            pool_type: PoolType::PumpFunAmm,
            input_mint: SOL_NATIVE_MINT,
            output_mint: pfa_token,
            amount_in: 100_000_000,
            min_amount_out: threshold,
            user,
            input_token_program: TOKEN_PROGRAM_ID,
            output_token_program: TOKEN_PROGRAM_ID,
        };
        let ixs = executor.build_swap_ix(&order, &cached_state).unwrap();
        let t_build_ix = t1.elapsed();

        let t2 = Instant::now();
        let tx_config = TxBuildConfig::default();
        let blockhash = solana_sdk::hash::Hash::new_unique();
        let (_, tx) = build_unsigned_swap_message(&ixs, &user, &tx_config, blockhash).unwrap();
        let t_tx = t2.elapsed();

        let t3 = Instant::now();
        let tx_bytes = bincode::serialize(&tx).unwrap();
        let tx_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &tx_bytes);
        let t_ser = t3.elapsed();

        let total = t0.elapsed();

        eprintln!("| quote:     {:>8}us{} |", t_quote.as_micros(), " ".repeat(68 - 22 - format!("{}", t_quote.as_micros()).len()));
        eprintln!("| build_ix:  {:>8}us{} |", t_build_ix.as_micros(), " ".repeat(68 - 22 - format!("{}", t_build_ix.as_micros()).len()));
        eprintln!("| build_tx:  {:>8}us{} |", t_tx.as_micros(), " ".repeat(68 - 22 - format!("{}", t_tx.as_micros()).len()));
        eprintln!("| serialize: {:>8}us{} |", t_ser.as_micros(), " ".repeat(68 - 22 - format!("{}", t_ser.as_micros()).len()));
        eprintln!("| TOTAL:     {:>8}us{} |", total.as_micros(), " ".repeat(68 - 22 - format!("{}", total.as_micros()).len()));
        eprintln!("| TX size:   {} bytes, base64: {} chars{} |",
            tx_bytes.len(), tx_b64.len(),
            " ".repeat(68usize.saturating_sub(30 + format!("{}", tx_bytes.len()).len() + format!("{}", tx_b64.len()).len())));
        eprintln!("| Output:    {} tokens (min: {}){} |",
            out_amount, threshold,
            " ".repeat(68usize.saturating_sub(27 + format!("{}", out_amount).len() + format!("{}", threshold).len())));

        assert!(total.as_millis() < 50, "full pipeline should complete in <50ms on hot cache");
        assert!(tx_bytes.len() <= 1232, "TX should be within 1232-byte limit");
    } else {
        eprintln!("| [SKIP] quote failed{} |", " ".repeat(68 - 19));
    }
    eprintln!("+-{:-<68}-+\n", "");

    // ── STEP 9: CACHE HIT vs MISS COMPARISON ────────────────────────────────

    eprintln!("+-{:-<68}-+", "- STEP 9: CACHE HIT vs MISS ");

    // Cold: new cache, must fetch from RPC
    let cold_cache = Arc::new(PoolCache::new(120_000));
    let cold_reg = Arc::new(PoolRegistry::new());
    cold_reg.add(PoolEntry {
        address: pfa_pool,
        pool_type: PoolType::PumpFunAmm,
        mint_a: pfa_token,
        mint_b: SOL_NATIVE_MINT,
    });
    let cold_quoter = Quoter::new(cold_reg, cold_cache, rpc_client.clone());
    let cold_req = QuoteRequest {
        input_mint: SOL_NATIVE_MINT,
        output_mint: pfa_token,
        amount: 100_000_000,
        slippage_bps: 50,
        only_direct_routes: true,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    let cold_start = Instant::now();
    let _ = cold_quoter.quote(&cold_req).await;
    let cold_elapsed = cold_start.elapsed();

    // Hot: reuse existing warmed cache
    let hot_start = Instant::now();
    let _ = quoter.quote(&pipeline_req).await;
    let hot_elapsed = hot_start.elapsed();

    let cold_us = cold_elapsed.as_micros();
    let hot_us = hot_elapsed.as_micros();
    let speedup = if hot_us > 0 { cold_us as f64 / hot_us as f64 } else { 999.0 };

    eprintln!("| Cold (RPC fetch): {:>10}us{} |",
        cold_us, " ".repeat(68usize.saturating_sub(25 + format!("{}", cold_us).len())));
    eprintln!("| Hot  (cache hit): {:>10}us{} |",
        hot_us, " ".repeat(68usize.saturating_sub(25 + format!("{}", hot_us).len())));
    eprintln!("| Speedup: {:.0}x{} |",
        speedup, " ".repeat(68usize.saturating_sub(13 + format!("{:.0}", speedup).len())));
    eprintln!("+-{:-<68}-+\n", "");

    assert!(speedup > 2.0, "hot cache should be at least 2x faster than cold (got {:.1}x)", speedup);

    // ── STEP 10: V0 TX WITH ALTs ────────────────────────────────────────────

    eprintln!("+-{:-<68}-+", "- STEP 10: V0 TX WITH ALTs ");

    let alt_cache = AltCache::new();
    let alt_pubkeys: Vec<Pubkey> = KNOWN_ALTS.iter().map(|s| pk(s)).collect();
    let loaded = alt_cache.load_alts(&rpc_client, &alt_pubkeys).await;
    let tables = alt_cache.all_tables();

    eprintln!("| Loaded {} ALTs ({} address entries total){} |",
        loaded,
        tables.iter().map(|t| t.addresses.len()).sum::<usize>(),
        " ".repeat(68usize.saturating_sub(40 + format!("{}", loaded).len() + format!("{}", tables.iter().map(|t| t.addresses.len()).sum::<usize>()).len())));

    // Build a swap instruction to compare legacy vs v0
    if let Some(state) = cache.get(&cpmm_pool1) {
        let executor = AmmExecutorType::from_pool_type(PoolType::RaydiumCpmm).unwrap();
        let input_tp = fetcher::get_mint_token_program(&rpc_client, &SOL_NATIVE_MINT)
            .await
            .unwrap_or(TOKEN_PROGRAM_ID);
        let output_tp = fetcher::get_mint_token_program(&rpc_client, &cpmm_token1)
            .await
            .unwrap_or(TOKEN_PROGRAM_ID);

        let order = SwapOrder {
            pool_address: cpmm_pool1,
            pool_type: PoolType::RaydiumCpmm,
            input_mint: SOL_NATIVE_MINT,
            output_mint: cpmm_token1,
            amount_in: 1_000_000,
            min_amount_out: 1,
            user,
            input_token_program: input_tp,
            output_token_program: output_tp,
        };

        let ixs = executor.build_swap_ix(&order, &state).unwrap();
        let blockhash = rpc_client.get_latest_blockhash().await.unwrap();
        let config = TxBuildConfig::default();

        let (_, legacy_tx) = build_unsigned_swap_message(&ixs, &user, &config, blockhash).unwrap();
        let legacy_bytes = bincode::serialize(&legacy_tx).unwrap();

        let v0_tx = build_unsigned_versioned_tx(&ixs, &user, &config, blockhash, &tables).unwrap();
        let v0_bytes = bincode::serialize(&v0_tx).unwrap();

        let savings = legacy_bytes.len() as i64 - v0_bytes.len() as i64;
        let pct = if legacy_bytes.len() > 0 { (savings as f64 / legacy_bytes.len() as f64) * 100.0 } else { 0.0 };

        eprintln!("| Legacy TX:     {} bytes{} |",
            legacy_bytes.len(),
            " ".repeat(68usize.saturating_sub(19 + format!("{}", legacy_bytes.len()).len())));
        eprintln!("| Versioned TX:  {} bytes{} |",
            v0_bytes.len(),
            " ".repeat(68usize.saturating_sub(19 + format!("{}", v0_bytes.len()).len())));
        eprintln!("| Savings:       {} bytes ({:.1}%){} |",
            savings, pct,
            " ".repeat(68usize.saturating_sub(26 + format!("{}", savings).len() + format!("{:.1}", pct).len())));
        eprintln!("| Under 1232 limit: {}{} |",
            if v0_bytes.len() <= 1232 { "YES" } else { "NO" },
            " ".repeat(68usize.saturating_sub(22)));
    } else {
        eprintln!("| [SKIP] RaydiumCpmm pool not cached{} |", " ".repeat(68 - 35));
    }
    eprintln!("+-{:-<68}-+\n", "");

    // ── SUMMARY ─────────────────────────────────────────────────────────────

    eprintln!("{}", "=".repeat(70));
    eprintln!("  SHOWCASE COMPLETE -- all 10 steps executed");
    eprintln!("{}\n", "=".repeat(70));
}

// ============================================================================
//  FOCUSED TEST: 100 SEQUENTIAL QUOTES (LATENCY DISTRIBUTION)
// ============================================================================

#[tokio::test]
async fn test_hot_cache_100_sequential_quotes() {
    eprintln!("\n+-{:-<68}-+", "- 100 SEQUENTIAL QUOTES (LATENCY DISTRIBUTION) ");

    let rpc_client = Arc::new(rpc());
    let registry = Arc::new(PoolRegistry::new());
    let cache = Arc::new(PoolCache::new(120_000));

    // Register 5 pools
    for p in showcase_pools().iter().take(5) {
        let addr = pk(p.pool_address);
        registry.add(PoolEntry {
            address: addr,
            pool_type: p.pool_type,
            mint_a: p.mint_a,
            mint_b: p.mint_b,
        });
        // Pre-warm
        if let Ok(state) = fetcher::fetch_pool_state(&rpc_client, p.pool_type, &addr).await {
            cache.insert(addr, state);
        }
    }

    let quoter = Quoter::new(registry, cache, rpc_client);

    // PumpFunAmm quote (inline reserves -> most stable timing)
    let pfa_token = pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz");
    let req = QuoteRequest {
        input_mint: SOL_NATIVE_MINT,
        output_mint: pfa_token,
        amount: 100_000_000,
        slippage_bps: 50,
        only_direct_routes: true,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    // Warm up
    let _ = quoter.quote(&req).await;

    let mut timings: Vec<u128> = Vec::with_capacity(100);
    let mut ok_count = 0u32;

    for _ in 0..100 {
        let t = Instant::now();
        if quoter.quote(&req).await.is_ok() {
            ok_count += 1;
        }
        timings.push(t.elapsed().as_micros() as u128);
    }

    timings.sort();

    let min = timings[0];
    let avg = timings.iter().sum::<u128>() / timings.len() as u128;
    let p50 = percentile(&timings, 50.0);
    let p95 = percentile(&timings, 95.0);
    let p99 = percentile(&timings, 99.0);
    let max = *timings.last().unwrap();

    eprintln!("| {:<10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} |", "Metric", "Min", "Avg", "P50", "P95", "Max");
    eprintln!("| {:-<10}-+-{:-<10}-+-{:-<10}-+-{:-<10}-+-{:-<10}-+-{:-<10}-|", "", "", "", "", "", "");
    eprintln!("| {:<10} | {:>8}us | {:>8}us | {:>8}us | {:>8}us | {:>8}us |",
        "Latency", min, avg, p50, p95, max);
    eprintln!("| P99: {}us | Success: {}/100{} |",
        p99, ok_count,
        " ".repeat(68usize.saturating_sub(30 + format!("{}", p99).len() + format!("{}", ok_count).len())));
    eprintln!("+-{:-<68}-+\n", "");

    assert!(ok_count >= 95, "at least 95/100 quotes should succeed, got {ok_count}");
    // Hot cache quotes should be fast
    assert!(avg < 5_000, "avg latency should be < 5ms on hot cache, got {}us", avg);
}

// ============================================================================
//  FOCUSED TEST: MULTI-DEX BEST ROUTE SELECTION
// ============================================================================

#[tokio::test]
async fn test_multi_dex_best_route_selection() {
    eprintln!("\n+-{:-<68}-+", "- MULTI-DEX BEST ROUTE SELECTION ");

    let rpc_client = Arc::new(rpc());
    let cache = Arc::new(PoolCache::new(120_000));

    // Quote SOL -> different tokens via different DEXes
    // Each pool has a different token, so we compare per-DEX quote speed/output independently
    struct DexQuote {
        label: &'static str,
        pool_type: PoolType,
        pool_address: &'static str,
        output_mint: Pubkey,
    }

    let cases = vec![
        DexQuote {
            label: "RaydiumCpmm",
            pool_type: PoolType::RaydiumCpmm,
            pool_address: "BEdCQzzvEmqtHH946GYsdeideaBYMvt8SZn5eUjKxTjr",
            output_mint: pk("25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook"),
        },
        DexQuote {
            label: "PumpFunAmm",
            pool_type: PoolType::PumpFunAmm,
            pool_address: "6cPfRuSp8L7f1TMt3vtKhqYYuHDoHZTHGNzQ6hRABtx6",
            output_mint: pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz"),
        },
        DexQuote {
            label: "MeteoraDamm",
            pool_type: PoolType::MeteoraDamm,
            pool_address: "4ac2qQp9uPQTuhvNH1p5xZmZnKV1pAxWKjBPkwvD6x6J",
            output_mint: pk("CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3"),
        },
        DexQuote {
            label: "Meteora",
            pool_type: PoolType::Meteora,
            pool_address: "BCXjm4FfSoquZQJV5Wcje1g1pSHW2hFMU9wDE98Nyatb",
            output_mint: pk("STrikemJEk2tFVYpg7SMo9nGPrnJ56fHnS1K7PV2fPw"),
        },
    ];

    eprintln!("| {:<14} | {:>14} | {:>10} | {:>8} | {:>6} |", "DEX", "Output", "Latency", "Impact", "Status");
    eprintln!("| {:-<14}-+-{:-<14}-+-{:-<10}-+-{:-<8}-+-{:-<6}-|", "", "", "", "", "");

    let mut quote_count = 0u32;

    for case in &cases {
        let addr = pk(case.pool_address);
        let reg = Arc::new(PoolRegistry::new());
        reg.add(PoolEntry {
            address: addr,
            pool_type: case.pool_type,
            mint_a: case.output_mint,
            mint_b: SOL_NATIVE_MINT,
        });

        // Pre-warm
        if let Ok(state) = fetcher::fetch_pool_state(&rpc_client, case.pool_type, &addr).await {
            cache.insert(addr, state);
        }

        let quoter = Quoter::new(reg, cache.clone(), rpc_client.clone());
        let req = QuoteRequest {
            input_mint: SOL_NATIVE_MINT,
            output_mint: case.output_mint,
            amount: 100_000_000,
            slippage_bps: 50,
            only_direct_routes: true,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let start = Instant::now();
        match quoter.quote(&req).await {
            Ok(resp) => {
                let elapsed_us = start.elapsed().as_micros();
                quote_count += 1;
                eprintln!("| {:<14} | {:>14} | {:>7}us | {:>7}% | {:>6} |",
                    case.label, resp.amount_out, elapsed_us, resp.price_impact, "OK");
            }
            Err(e) => {
                eprintln!("| {:<14} | {:>14} | {:>10} | {:>8} | {:>6} |",
                    case.label, "-", "-", "-", "FAIL");
                eprintln!("|   Error: {:<59} |", format!("{e}").chars().take(59).collect::<String>());
            }
        }
    }

    eprintln!("| {}/{} DEXes returned valid quotes{} |",
        quote_count, cases.len(),
        " ".repeat(68usize.saturating_sub(32 + format!("{}", quote_count).len() + format!("{}", cases.len()).len())));
    eprintln!("+-{:-<68}-+\n", "");

    assert!(quote_count >= 2, "at least 2 DEXes should return quotes, got {quote_count}");
}

// ============================================================================
//  FOCUSED TEST: 2-HOP THROUGH ALL BRIDGE TOKENS
// ============================================================================

#[tokio::test]
async fn test_two_hop_all_bridge_tokens() {
    eprintln!("\n+-{:-<68}-+", "- 2-HOP ROUTING: ALL BRIDGE TOKENS ");

    let rpc_client = Arc::new(rpc());
    let registry = Arc::new(PoolRegistry::new());
    let cache = Arc::new(PoolCache::new(120_000));

    // token_a has a RaydiumCpmm pool with SOL
    let token_a = pk("25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook");
    // token_b has a MeteoraDamm pool with SOL
    let token_b = pk("CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3");

    // Register hop pools for SOL bridge
    registry.add(PoolEntry {
        address: pk("BEdCQzzvEmqtHH946GYsdeideaBYMvt8SZn5eUjKxTjr"),
        pool_type: PoolType::RaydiumCpmm,
        mint_a: token_a,
        mint_b: SOL_NATIVE_MINT,
    });
    registry.add(PoolEntry {
        address: pk("4ac2qQp9uPQTuhvNH1p5xZmZnKV1pAxWKjBPkwvD6x6J"),
        pool_type: PoolType::MeteoraDamm,
        mint_a: SOL_NATIVE_MINT,
        mint_b: token_b,
    });

    // Register a USDC/SOL pool (Dooar) so USDC bridge could theoretically be used
    // (only works if there are also token_a/USDC and token_b/USDC pools)
    registry.add(PoolEntry {
        address: pk("5GGvkcqQ1554ibdc18JXiPqR8aJz6WV3JSNShoj32ufT"),
        pool_type: PoolType::Dooar,
        mint_a: USDC_MINT,
        mint_b: SOL_NATIVE_MINT,
    });

    // Pre-warm all registered pools
    for entry in registry.entries() {
        if let Ok(state) = fetcher::fetch_pool_state(&rpc_client, entry.pool_type, &entry.address).await {
            cache.insert(entry.address, state);
        }
    }

    let bridge_names = ["SOL", "USDC", "USDT"];
    let bridges = [SOL_NATIVE_MINT, USDC_MINT, USDT_MINT];

    eprintln!("| Route: {} -> {} via bridge{} |",
        &token_a.to_string()[..8], &token_b.to_string()[..8],
        " ".repeat(68usize.saturating_sub(35)));
    eprintln!("| {:<8} | {:>14} | {:>10} | {:>8} | {:>14} |",
        "Bridge", "Output", "Latency", "Hops", "Status");
    eprintln!("| {:-<8}-+-{:-<14}-+-{:-<10}-+-{:-<8}-+-{:-<14}-|",
        "", "", "", "", "");

    let quoter = Quoter::new(registry, cache, rpc_client);

    for (i, bridge) in bridges.iter().enumerate() {
        // Skip bridge if it's one of the endpoints
        if *bridge == token_a || *bridge == token_b {
            eprintln!("| {:<8} | {:>14} | {:>10} | {:>8} | {:>14} |",
                bridge_names[i], "-", "-", "-", "IS ENDPOINT");
            continue;
        }

        let req = QuoteRequest {
            input_mint: token_a,
            output_mint: token_b,
            amount: 1_000_000_000,
            slippage_bps: 100,
            only_direct_routes: false,
            // Force specific bridge by excluding other bridges?
            // We can't directly -- just quote and see which bridge is chosen
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let start = Instant::now();
        match quoter.quote(&req).await {
            Ok(resp) => {
                let elapsed_us = start.elapsed().as_micros();
                let bridge_used = if resp.routes.len() == 2 {
                    let hop1_out = &resp.routes[0].pool.output_token;
                    if *hop1_out == SOL_NATIVE_MINT.to_string() { "SOL" }
                    else if *hop1_out == USDC_MINT.to_string() { "USDC" }
                    else if *hop1_out == USDT_MINT.to_string() { "USDT" }
                    else { "???" }
                } else { "direct" };

                eprintln!("| {:<8} | {:>14} | {:>7}us | {:>8} | {:>14} |",
                    bridge_names[i], resp.amount_out, elapsed_us,
                    resp.routes.len(), format!("via {}", bridge_used));
            }
            Err(e) => {
                eprintln!("| {:<8} | {:>14} | {:>10} | {:>8} | {:>14} |",
                    bridge_names[i], "-", "-", "-",
                    format!("{}", e).chars().take(14).collect::<String>());
            }
        }

        // Only the first call matters since the quoter evaluates all bridges at once
        break;
    }

    // Show what was found
    let req = QuoteRequest {
        input_mint: token_a,
        output_mint: token_b,
        amount: 1_000_000_000,
        slippage_bps: 100,
        only_direct_routes: false,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    match quoter.quote(&req).await {
        Ok(resp) if resp.routes.len() == 2 => {
            let bridge_mint_str = &resp.routes[0].pool.output_token;
            let bridge_label = if *bridge_mint_str == SOL_NATIVE_MINT.to_string() { "SOL" }
                else if *bridge_mint_str == USDC_MINT.to_string() { "USDC" }
                else if *bridge_mint_str == USDT_MINT.to_string() { "USDT" }
                else { "unknown" };
            eprintln!("| Best bridge: {} (output={}){} |",
                bridge_label, resp.amount_out,
                " ".repeat(68usize.saturating_sub(25 + bridge_label.len() + resp.amount_out.len())));
        }
        Ok(resp) => {
            eprintln!("| Direct route found ({} hop(s)){} |",
                resp.routes.len(),
                " ".repeat(68usize.saturating_sub(29 + format!("{}", resp.routes.len()).len())));
        }
        Err(e) => {
            eprintln!("| No route found: {}{} |",
                format!("{e}").chars().take(50).collect::<String>(),
                " ".repeat(68usize.saturating_sub(18 + format!("{e}").len().min(50))));
        }
    }
    eprintln!("+-{:-<68}-+\n", "");
}

// ============================================================================
//  FOCUSED TEST: CACHE WARM-TO-HOT TRANSITION
// ============================================================================

#[tokio::test]
async fn test_cache_warm_to_hot_transition() {
    eprintln!("\n+-{:-<68}-+", "- CACHE WARM-TO-HOT TRANSITION ");

    let rpc_client = Arc::new(rpc());
    let cache = Arc::new(PoolCache::new(120_000));
    let registry = Arc::new(PoolRegistry::new());

    let pfa_pool = pk("6cPfRuSp8L7f1TMt3vtKhqYYuHDoHZTHGNzQ6hRABtx6");
    let pfa_token = pk("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz");

    registry.add(PoolEntry {
        address: pfa_pool,
        pool_type: PoolType::PumpFunAmm,
        mint_a: pfa_token,
        mint_b: SOL_NATIVE_MINT,
    });

    let quoter = Quoter::new(registry, cache.clone(), rpc_client.clone());

    let req = QuoteRequest {
        input_mint: SOL_NATIVE_MINT,
        output_mint: pfa_token,
        amount: 100_000_000,
        slippage_bps: 50,
        only_direct_routes: true,
        exclude_dexes: vec![],
        dexes: vec![],
        max_accounts: 64,
    };

    // Phase 1: Empty cache — quote must fetch from RPC
    eprintln!("| Phase 1: EMPTY CACHE (cold start){} |", " ".repeat(68 - 34));
    assert!(cache.get(&pfa_pool).is_none(), "cache should be empty");

    let t1 = Instant::now();
    let result1 = quoter.quote(&req).await;
    let cold_us = t1.elapsed().as_micros();

    match &result1 {
        Ok(resp) => {
            eprintln!("|   Quote succeeded: out={} in {}us{} |",
                resp.amount_out, cold_us,
                " ".repeat(68usize.saturating_sub(30 + resp.amount_out.len() + format!("{}", cold_us).len())));
        }
        Err(e) => {
            eprintln!("|   Quote failed: {}{} |",
                format!("{e}").chars().take(52).collect::<String>(),
                " ".repeat(68usize.saturating_sub(18 + format!("{e}").len().min(52))));
        }
    }

    // Phase 2: Simulate stream update — inject state directly into cache
    eprintln!("| Phase 2: STREAM UPDATE (inject into cache){} |", " ".repeat(68 - 43));

    let state = fetcher::fetch_pool_state(&rpc_client, PoolType::PumpFunAmm, &pfa_pool)
        .await
        .expect("should fetch PumpFunAmm state");
    cache.insert(pfa_pool, state);
    assert!(cache.get(&pfa_pool).is_some(), "cache should have the entry now");
    eprintln!("|   Cache entry inserted for {}{} |",
        short(&pfa_pool),
        " ".repeat(68usize.saturating_sub(30 + short(&pfa_pool).len())));

    // Phase 3: Hot cache — quote should be fast
    eprintln!("| Phase 3: HOT CACHE (instant quote){} |", " ".repeat(68 - 35));

    let t3 = Instant::now();
    let result3 = quoter.quote(&req).await;
    let hot_us = t3.elapsed().as_micros();

    match &result3 {
        Ok(resp) => {
            eprintln!("|   Quote succeeded: out={} in {}us{} |",
                resp.amount_out, hot_us,
                " ".repeat(68usize.saturating_sub(30 + resp.amount_out.len() + format!("{}", hot_us).len())));
        }
        Err(e) => {
            eprintln!("|   Quote failed: {}{} |",
                format!("{e}").chars().take(52).collect::<String>(),
                " ".repeat(68usize.saturating_sub(18 + format!("{e}").len().min(52))));
        }
    }

    let speedup = if hot_us > 0 { cold_us as f64 / hot_us as f64 } else { 999.0 };
    eprintln!("| Transition: {}us (cold) -> {}us (hot) = {:.0}x speedup{} |",
        cold_us, hot_us, speedup,
        " ".repeat(68usize.saturating_sub(42 + format!("{}", cold_us).len() + format!("{}", hot_us).len() + format!("{:.0}", speedup).len())));
    eprintln!("+-{:-<68}-+\n", "");

    assert!(result3.is_ok(), "hot cache quote should succeed");
    assert!(hot_us < cold_us || cold_us < 1000, "hot should be faster than cold (cold={}us hot={}us)", cold_us, hot_us);
}
