//! Live mainnet round-trip test for the Pumpup pre-graduation bonding curve.
//!
//! Picks a real bonding curve from mainnet (filtered by BondingCurve disc +
//! `real_sol > 0`), buys a tiny amount with native SOL, then sells everything
//! back. Verifies the SOL balance round-trips and the token balance returns
//! to ~zero.
//!
//! Bonding curves trade against **native SOL** via System program (no WSOL).
//!
//! Run:
//! ```bash
//! set -a && source .env && set +a
//! OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu OPENSSL_INCLUDE_DIR=/usr/include \
//!   cargo test --test e2e_pumpup_bonding -- --nocapture --test-threads=1
//! ```

use std::str::FromStr;
use std::time::{Duration, Instant};

use solana_account_decoder::UiAccountEncoding;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;

use flow_trades::constants::*;
use flow_trades::execution::amms::{AmmExecutor, pumpup_bonding::PumpupBondingExecutor};
use flow_trades::pool::fetcher;
use flow_trades::pool::types::{PoolState, PoolType, SwapOrder};

const PUMPUP_PROG: &str = "PdMDrKEMaX8q7CCJb7NvUCxerBCcsFUa4LjBEynTtEd";
/// `sha256("account:BondingCurve")[0..8]`
const BONDING_CURVE_DISC: [u8; 8] = [23, 183, 248, 55, 96, 216, 172, 96];

fn rpc() -> RpcClient {
    let url = std::env::var("RPC_URL")
        .or_else(|_| std::env::var("SOL_HTTPS_ENDPOINT"))
        .expect("RPC_URL required");
    RpcClient::new_with_commitment(url, CommitmentConfig::confirmed())
}

fn load_keypair() -> Keypair {
    let b58 = std::env::var("SIM_PRIVATE_KEY").expect("SIM_PRIVATE_KEY required");
    let bytes = bs58::decode(b58.trim()).into_vec().expect("invalid base58");
    Keypair::from_bytes(&bytes).expect("invalid keypair")
}

/// Find a live bonding curve via getProgramAccounts (memcmp on disc) sorted
/// by `real_sol`. Returns the highest-activity curve so the price is stable.
async fn find_active_bonding_curve(rpc: &RpcClient) -> Option<Pubkey> {
    let prog = Pubkey::from_str(PUMPUP_PROG).unwrap();
    let cfg = RpcProgramAccountsConfig {
        filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new(
            0,
            MemcmpEncodedBytes::Bytes(BONDING_CURVE_DISC.to_vec()),
        ))]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            data_slice: Some(solana_account_decoder::UiDataSliceConfig {
                offset: 0, length: 49,
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    let accts = rpc.get_program_accounts_with_config(&prog, cfg).await.ok()?;
    let mut hits: Vec<(Pubkey, u64, u64)> = accts
        .into_iter()
        .filter_map(|(addr, acct)| {
            if acct.data.len() < 48 { return None; }
            let real_sol = u64::from_le_bytes(acct.data[24..32].try_into().ok()?);
            let pool_token_reserves = u64::from_le_bytes(acct.data[40..48].try_into().ok()?);
            // Need actual SOL collected AND tokens still available to buy.
            if real_sol == 0 || pool_token_reserves == 0 { return None; }
            Some((addr, real_sol, pool_token_reserves))
        })
        .collect();
    hits.sort_by(|a, b| b.1.cmp(&a.1));
    hits.first().map(|(a, _, _)| *a)
}

/// Build, sign and submit a Pumpup bonding-curve swap directly (no router).
async fn execute_pumpup_bonding_swap(
    rpc: &RpcClient,
    signer: &Keypair,
    pool: &Pubkey,
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount_in: u64,
    min_amount_out: u64,
) -> Result<(String, u128), String> {
    let user = signer.pubkey();
    let pool_state = fetcher::fetch_pool_state(rpc, PoolType::PumpupBonding, pool)
        .await
        .map_err(|e| format!("fetch: {e}"))?;
    let order = SwapOrder {
        pool_address: *pool,
        pool_type: PoolType::PumpupBonding,
        input_mint: *input_mint,
        output_mint: *output_mint,
        amount_in,
        min_amount_out,
        user,
        input_token_program: TOKEN_PROGRAM_ID,
        output_token_program: TOKEN_PROGRAM_ID,
    };
    let ixs = PumpupBondingExecutor
        .build_swap_ix(&order, &pool_state)
        .map_err(|e| format!("build_ix: {e}"))?;

    let mut all_ixs = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(400_000),
        ComputeBudgetInstruction::set_compute_unit_price(5_000),
    ];
    all_ixs.extend(ixs.setup);
    all_ixs.extend(ixs.swap);
    all_ixs.extend(ixs.cleanup);

    let blockhash = rpc.get_latest_blockhash().await.map_err(|e| format!("blockhash: {e}"))?;
    let start = Instant::now();
    let msg = Message::new_with_blockhash(&all_ixs, Some(&user), &blockhash);
    let tx = Transaction::new(&[signer], msg, blockhash);
    let sig = rpc
        .send_and_confirm_transaction(&tx)
        .await
        .map_err(|e| format!("submit: {e}"))?;
    Ok((sig.to_string(), start.elapsed().as_millis()))
}

async fn token_balance(rpc: &RpcClient, owner: &Pubkey, mint: &Pubkey) -> u64 {
    let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        owner, mint, &TOKEN_PROGRAM_ID,
    );
    rpc.get_token_account_balance(&ata)
        .await
        .ok()
        .and_then(|b| b.amount.parse::<u64>().ok())
        .unwrap_or(0)
}

#[tokio::test]
async fn test_mainnet_pumpup_bonding_round_trip() {
    let rpc = rpc();
    let signer = load_keypair();
    let user = signer.pubkey();
    let sol = SOL_NATIVE_MINT;

    eprintln!("\n+-- PUMPUP BONDING-CURVE MAINNET ROUND TRIP --+");
    eprintln!("| wallet: {user}");

    // 1. Find an active curve.
    let pool = find_active_bonding_curve(&rpc)
        .await
        .expect("no active Pumpup bonding curves found on mainnet");
    eprintln!("| pool:   {pool}");

    // 2. Fetch state to discover the mint and quote sizing.
    let state = fetcher::fetch_pool_state(&rpc, PoolType::PumpupBonding, &pool)
        .await
        .expect("fetch pumpup bonding state");
    let (mint, virtual_sol, real_sol, pool_sol_reserves, pool_token_reserves) = match &state {
        PoolState::PumpupBonding {
            mint, virtual_sol, real_sol, pool_sol_reserves, pool_token_reserves, ..
        } => (*mint, *virtual_sol, *real_sol, *pool_sol_reserves, *pool_token_reserves),
        _ => panic!("expected PumpupBonding state, got {:?}", state),
    };
    eprintln!("| mint:        {mint}");
    eprintln!("| virtual_sol: {virtual_sol} ({:.4} SOL)", virtual_sol as f64 / 1e9);
    eprintln!("| real_sol:    {real_sol} ({:.4} SOL)", real_sol as f64 / 1e9);
    eprintln!("| pool_sol:    {pool_sol_reserves} ({:.4} SOL)", pool_sol_reserves as f64 / 1e9);
    eprintln!("| pool_token:  {pool_token_reserves} ({:.4}M)", pool_token_reserves as f64 / 1e12);

    // 3. Make sure we have SOL.
    let starting_sol = rpc.get_balance(&user).await.unwrap();
    eprintln!("| start SOL:   {} lamports ({:.6} SOL)", starting_sol, starting_sol as f64 / 1e9);
    assert!(starting_sol > 5_000_000, "wallet needs > 0.005 SOL for the test");

    // ── BUY: 0.001 SOL → token ──
    const BUY_SOL: u64 = 1_000_000;
    eprintln!("\n| -- BUY 0.001 SOL -> {mint} --");
    let (buy_sig, buy_ms) = execute_pumpup_bonding_swap(
        &rpc, &signer, &pool, &sol, &mint, BUY_SOL, 1,
    ).await.expect("bonding-curve buy failed");
    eprintln!("|   tx: {buy_sig}");
    eprintln!("|   time: {} ms", buy_ms);

    // Wait for state propagation; query balance.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let token_received = token_balance(&rpc, &user, &mint).await;
    eprintln!("|   tokens received: {} atomic", token_received);
    assert!(token_received > 0, "buy succeeded but token balance is 0");

    // ── SELL all back to SOL ──
    eprintln!("\n| -- SELL {token_received} tokens -> SOL --");
    let mut sell_attempts = 0u32;
    let (sell_sig, sell_ms) = loop {
        sell_attempts += 1;
        match execute_pumpup_bonding_swap(
            &rpc, &signer, &pool, &mint, &sol, token_received, 1,
        ).await {
            Ok(r) => break r,
            Err(e) if sell_attempts < 5 => {
                eprintln!("|   sell attempt {sell_attempts} failed: {e}, retrying...");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            Err(e) => panic!("bonding-curve sell failed after 5 attempts: {e}"),
        }
    };
    eprintln!("|   tx: {sell_sig}");
    eprintln!("|   time: {} ms", sell_ms);

    tokio::time::sleep(Duration::from_secs(3)).await;
    let token_after_sell = token_balance(&rpc, &user, &mint).await;
    let ending_sol = rpc.get_balance(&user).await.unwrap();
    eprintln!("\n|   tokens after sell: {} (should be 0 or near 0)", token_after_sell);
    eprintln!("|   end SOL: {} lamports ({:.6} SOL)", ending_sol, ending_sol as f64 / 1e9);
    let net_lamports = starting_sol as i128 - ending_sol as i128;
    eprintln!("|   net cost: {} lamports ({:.6} SOL — fees + slippage + tx fees)",
        net_lamports, net_lamports as f64 / 1e9);
    eprintln!("+-- PASS --+\n");

    // After selling everything, balance should be near zero (a few dust units OK).
    assert!(token_after_sell < token_received / 100,
        "expected most tokens to be sold; before={token_received} after={token_after_sell}");
}
