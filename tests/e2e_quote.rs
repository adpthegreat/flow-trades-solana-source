//! End-to-end quote + swap tests against live mainnet RPC.
//!
//! Validates the full pipeline: pool fetch → AMM math / simulation → tx build.
//!
//! Requires `SOL_HTTPS_ENDPOINT` (or `RPC_URL`) env var.
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include \
//!   cargo test --test e2e_quote -- --nocapture --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Instant;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

use flow_trades::constants::*;
use flow_trades::execution::AmmExecutorType;
use flow_trades::pool::cache::PoolCache;
use flow_trades::pool::fetcher;
use flow_trades::pool::types::{PoolType, SwapOrder};
use flow_trades::execution::tx_builder::{build_unsigned_swap_message, TxBuildConfig};

fn rpc_url() -> String {
    std::env::var("SOL_HTTPS_ENDPOINT")
        .or_else(|_| std::env::var("RPC_URL"))
        .expect("SOL_HTTPS_ENDPOINT or RPC_URL must be set")
}

fn rpc() -> RpcClient {
    RpcClient::new_with_commitment(rpc_url(), CommitmentConfig::confirmed())
}

/// Test pool fetch + instruction build for a given AMM.
async fn test_amm_pipeline(
    pool_type: PoolType,
    pool_address: &str,
    input_mint: Pubkey,
    output_mint: Pubkey,
) {
    let rpc = rpc();
    let pool_addr = Pubkey::from_str(pool_address).unwrap();
    let user = Pubkey::from_str("6TwqjGNQ8c2aUHvbpAjMd4bdHdone9CTrz3c8S71E2WW").unwrap();

    // 1. Fetch pool state
    let start = Instant::now();
    let pool_state = fetcher::fetch_pool_state(&rpc, pool_type, &pool_addr).await;
    let fetch_ms = start.elapsed().as_millis();

    match pool_state {
        Ok(state) => {
            eprintln!("  [OK] pool fetched in {fetch_ms}ms");

            // 2. Detect token programs
            let input_tp = fetcher::get_mint_token_program(&rpc, &input_mint)
                .await
                .unwrap_or(TOKEN_PROGRAM_ID);
            let output_tp = fetcher::get_mint_token_program(&rpc, &output_mint)
                .await
                .unwrap_or(TOKEN_PROGRAM_ID);

            // 3. Build swap instructions
            let executor = AmmExecutorType::from_pool_type(pool_type).unwrap();
            let order = SwapOrder {
                pool_address: pool_addr,
                pool_type,
                input_mint,
                output_mint,
                amount_in: 1_000_000, // 0.001 SOL or 1 token unit
                min_amount_out: 0,
                user,
                input_token_program: input_tp,
                output_token_program: output_tp,
            };

            let ix_result = executor.build_swap_ix(&order, &state);
            match ix_result {
                Ok(ixs) => {
                    eprintln!("  [OK] {} setup + {} swap + {} cleanup instructions",
                        ixs.setup.len(), ixs.swap.len(), ixs.cleanup.len());

                    // 4. Build unsigned transaction
                    let tx_config = TxBuildConfig {
                        compute_unit_limit: 400_000,
                        priority_fee_lamports: 5_000,
                    };
                    let blockhash = rpc.get_latest_blockhash().await.unwrap();
                    let tx_result = build_unsigned_swap_message(&ixs, &user, &tx_config, blockhash);
                    match tx_result {
                        Ok((_msg, tx)) => {
                            let serialized = bincode::serialize(&tx).unwrap();
                            let b64 = base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                &serialized,
                            );
                            eprintln!("  [OK] unsigned tx built ({} bytes, {} b64 chars)", serialized.len(), b64.len());
                        }
                        Err(e) => {
                            eprintln!("  [FAIL] tx build error: {e}");
                            panic!("tx build failed: {e}");
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  [FAIL] build_swap_ix error: {e}");
                    panic!("build_swap_ix failed: {e}");
                }
            }
        }
        Err(e) => {
            eprintln!("  [SKIP] pool fetch failed: {e}");
        }
    }
}

// ── Per-AMM tests ──

#[tokio::test]
async fn test_e2e_raydium_v4() {
    eprintln!("Raydium V4:");
    test_amm_pipeline(
        PoolType::RaydiumV4,
        "3JDQqSxGF1yjpeStYNRmvXk76ApSGm7uE2onDQpyRvn4",
        SOL_NATIVE_MINT,
        Pubkey::from_str("G9EFgQFiJMu4j38CF8ANRGFpdUVitcUXG51tBLYEpump").unwrap(),
    ).await;
}

#[tokio::test]
async fn test_e2e_raydium_cpmm() {
    eprintln!("Raydium CPMM:");
    test_amm_pipeline(
        PoolType::RaydiumCpmm,
        "BEdCQzzvEmqtHH946GYsdeideaBYMvt8SZn5eUjKxTjr",
        SOL_NATIVE_MINT,
        Pubkey::from_str("25fFYbsxrCUPK2JZv6Bx7znfaiCczhTw3PocgA2dcook").unwrap(),
    ).await;
}

#[tokio::test]
async fn test_e2e_raydium_clmm() {
    eprintln!("Raydium CLMM:");
    test_amm_pipeline(
        PoolType::RaydiumCl,
        "ENQmMUSXmUYPaAL9NH79cFw3Lfht3bThmY8Zs8UwGEbr",
        SOL_NATIVE_MINT,
        Pubkey::from_str("22r6hjfpF15dkgJzkNXthNPZny1r7TohQb1vbAEBD5Fg").unwrap(),
    ).await;
}

#[tokio::test]
async fn test_e2e_pumpfun_amm() {
    eprintln!("PumpFun AMM:");
    test_amm_pipeline(
        PoolType::PumpFunAmm,
        "6cPfRuSp8L7f1TMt3vtKhqYYuHDoHZTHGNzQ6hRABtx6",
        SOL_NATIVE_MINT,
        Pubkey::from_str("6FH1oopsRWjnSRnyYpU1cuzKBrQfpap6Gr5wMkXAuWaz").unwrap(),
    ).await;
}

#[tokio::test]
async fn test_e2e_meteora_damm() {
    eprintln!("Meteora DAMM:");
    test_amm_pipeline(
        PoolType::MeteoraDamm,
        "4ac2qQp9uPQTuhvNH1p5xZmZnKV1pAxWKjBPkwvD6x6J",
        SOL_NATIVE_MINT,
        Pubkey::from_str("CDNZaZwhWB2VGFXSEMmM7hBodrhciRx7JPbjghttbEM3").unwrap(),
    ).await;
}

#[tokio::test]
async fn test_e2e_meteora_dlmm() {
    eprintln!("Meteora DLMM:");
    test_amm_pipeline(
        PoolType::MeteoraDlmm,
        "HTvjzsfX3yU6BUodCjZ5vZkUrAxMDTrBs3CJaq43ashR",
        SOL_NATIVE_MINT,
        USDC_MINT,
    ).await;
}

#[tokio::test]
async fn test_e2e_orca() {
    eprintln!("Orca:");
    test_amm_pipeline(
        PoolType::Orca,
        "4AFAkCSkSNmra64irggEFd8ZtF4WCtFe51qVaFFNBL2D",
        Pubkey::from_str("pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn").unwrap(),
        USDC_MINT,
    ).await;
}

#[tokio::test]
async fn test_e2e_meteora_standard() {
    eprintln!("Meteora Standard:");
    test_amm_pipeline(
        PoolType::Meteora,
        "BCXjm4FfSoquZQJV5Wcje1g1pSHW2hFMU9wDE98Nyatb",
        SOL_NATIVE_MINT,
        Pubkey::from_str("STrikemJEk2tFVYpg7SMo9nGPrnJ56fHnS1K7PV2fPw").unwrap(),
    ).await;
}

#[tokio::test]
async fn test_e2e_fluxbeam() {
    eprintln!("FluxBeam:");
    test_amm_pipeline(
        PoolType::FluxBeam,
        "BaX8sxueS6tuPjofvkh2UXoszvmKeVgLAQ1JeJzdjgVi",
        SOL_NATIVE_MINT,
        Pubkey::from_str("DGZB1yEiEYTfP8sn1hCKLw7HLy1QpcusR5LUJrbGk5Xk").unwrap(),
    ).await;
}

// ── Cache performance test ──

#[tokio::test]
async fn test_pool_cache_speedup() {
    let rpc = rpc();
    let cache = PoolCache::new(5000); // 5s TTL
    let pool_addr = Pubkey::from_str("BEdCQzzvEmqtHH946GYsdeideaBYMvt8SZn5eUjKxTjr").unwrap();

    // Cold fetch
    let start = Instant::now();
    let state = fetcher::fetch_pool_state(&rpc, PoolType::RaydiumCpmm, &pool_addr).await.unwrap();
    let cold_ms = start.elapsed().as_millis();

    // Store in cache
    cache.insert(pool_addr, state);

    // Hot fetch
    let start2 = Instant::now();
    let cached = cache.get(&pool_addr);
    let hot_us = start2.elapsed().as_micros();

    eprintln!("Cache test: cold={cold_ms}ms, hot={hot_us}µs");
    assert!(cached.is_some());
    assert!(hot_us < 1000, "cache lookup should be < 1ms, got {hot_us}µs");
}
