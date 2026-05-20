//! Lightweight block scanner for autonomous pool discovery.
//!
//! Polls recent Solana blocks via `getBlock`, scans every transaction for
//! instructions targeting known DEX program IDs, extracts candidate pool
//! addresses, and attempts to fetch their on-chain state. New pools are
//! automatically added to the registry and cache.
//!
//! No Geyser, no external dependencies needed — just a Solana RPC endpoint.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status_client_types::{
    EncodedTransaction, TransactionDetails, UiConfirmedBlock, UiMessage,
    UiTransactionEncoding,
};
use tracing::{info, warn};

use crate::constants::*;
use crate::enrichment::PriceOracle;
use crate::pool::cache::PoolCache;
use crate::pool::registry::{PoolEntry, PoolRegistry};
use crate::pool::types::PoolType;
use crate::storage::sqlite::PoolDb;
use crate::stream::swap_stream::{parse_swaps_from_block, Swap};
use crate::stream::types::StreamStats;

/// Optional swap-stream context for the scanner. When `Some`, every block
/// is also fed into the swap parser and resulting swaps go into the
/// broadcast channel.
#[derive(Clone)]
pub struct SwapStreamCtx {
    pub tx: tokio::sync::broadcast::Sender<std::sync::Arc<Swap>>,
    pub oracle: Arc<PriceOracle>,
}

/// Maximum number of concurrent pool-fetch tasks when discovering new pools.
const MAX_CONCURRENT_DISCOVERIES: usize = 50;

/// Convert an HTTP(S) URL to a WS(S) URL for blockSubscribe.
fn http_to_ws(url: &str) -> String {
    if url.starts_with("https://") {
        format!("wss://{}", &url["https://".len()..])
    } else if url.starts_with("http://") {
        format!("ws://{}", &url["http://".len()..])
    } else {
        url.to_string()
    }
}

/// Build the program ID -> PoolType lookup map.
fn build_program_map() -> HashMap<Pubkey, PoolType> {
    HashMap::from([
        (RAYDIUM_V4_PROG_ID, PoolType::RaydiumV4),
        (RAYDIUM_CPMM_PROG_ID, PoolType::RaydiumCpmm),
        (RAYDIUM_CL_PROG_ID, PoolType::RaydiumCl),
        (RAYDIUM_LP_PROG_ID, PoolType::RaydiumLp),
        (PUMP_FUN_PROG_ID, PoolType::PumpFun),
        (PUMP_FUN_AMM_PROG_ID, PoolType::PumpFunAmm),
        (METEORA_PROG_ID, PoolType::Meteora),
        (METEORA_DLMM_PROG_ID, PoolType::MeteoraDlmm),
        (METEORA_DAMM_PROG_ID, PoolType::MeteoraDamm),
        (METEORA_DBC_PROG_ID, PoolType::MeteoraDbc),
        (ORCA_PROG_ID, PoolType::Orca),
        (FLUXBEAM_PROG_ID, PoolType::FluxBeam),
        (SAROS_PROG_ID, PoolType::Saros),
        (DOOAR_PROG_ID, PoolType::Dooar),
        (PANCAKESWAP_PROG_ID, PoolType::PancakeSwap),
        (FLASH_TRADE_PROG_ID, PoolType::FlashTrade),
        (BYREAL_PROG_ID, PoolType::Byreal),
        (DEFITUNA_FUSION_PROG_ID, PoolType::DefiTunaFusion),
        (DEFITUNA_POOLS_PROG_ID, PoolType::DefiTunaPools),
        (PUMPUP_PROG_ID, PoolType::Pumpup),
        // OnChain Labs DEX V2 is intentionally NOT mapped to a PoolType: it's
        // an aggregator router, not a directly-quotable DEX. Including it in
        // the Geyser owner filter (see geyser.rs::all_dex_programs) means we
        // receive blocks containing OnChain Labs txs; the existing block
        // scanner walks inner instructions and registers any KNOWN-DEX pools
        // it routes through (Raydium, Orca, Meteora, etc.) — the discovery
        // benefit. We just don't try to register OnChain Labs program accounts
        // themselves as pools.
    ])
}

/// Map a DEX program ID to its PoolType.
pub fn dex_program_to_type(program_id: &Pubkey) -> Option<PoolType> {
    // Use a static map for O(1) lookups in hot path
    static PROGRAM_MAP: std::sync::OnceLock<HashMap<Pubkey, PoolType>> = std::sync::OnceLock::new();
    let map = PROGRAM_MAP.get_or_init(build_program_map);
    map.get(program_id).copied()
}

/// Extract the pool address from an instruction's account list based on the DEX type.
///
/// Each DEX puts the pool account at a specific index:
/// - accounts[1]: RaydiumV4, RaydiumCpmm, RaydiumLp, Meteora, MeteoraDlmm, FluxBeam, Saros, Dooar
/// - accounts[2]: RaydiumCl, PumpFun, MeteoraDamm, MeteoraDbc, Orca, PancakeSwap
/// - accounts[3]: PumpFunAmm, PumpupBonding (pool_sol_account)
pub fn extract_pool_index(pool_type: PoolType) -> usize {
    match pool_type {
        PoolType::RaydiumV4 => 1,
        PoolType::RaydiumCpmm => 1,
        PoolType::RaydiumLp => 1,
        PoolType::Meteora => 1,
        PoolType::MeteoraDlmm => 1,
        PoolType::FluxBeam => 1,
        PoolType::Saros => 1,
        PoolType::Dooar => 1,
        PoolType::RaydiumCl => 2,
        PoolType::PumpFun => 2,
        PoolType::MeteoraDamm => 2,
        PoolType::MeteoraDbc => 2,
        PoolType::Orca => 2,
        PoolType::PancakeSwap => 2,
        PoolType::PumpFunAmm => 3,
        // Pumpup `swap` ix puts pool at accounts[0] per IDL.
        PoolType::Pumpup => 0,
        // Pumpup `buy`/`sell` put pool_sol_account at accounts[3] per IDL.
        PoolType::PumpupBonding => 3,
        // Remaining DEXes: try accounts[1] as a reasonable default
        PoolType::FlashTrade
        | PoolType::Byreal
        | PoolType::DefiTunaFusion
        | PoolType::DefiTunaPools => 1,
        PoolType::Unknown => 1,
    }
}

/// First 8 bytes of `sha256("global:swap")` — Pumpup AMM swap.
const PUMPUP_SWAP_DISC: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
/// First 8 bytes of `sha256("global:buy")` — Pumpup bonding-curve buy.
const PUMPUP_BUY_DISC: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
/// First 8 bytes of `sha256("global:sell")` — Pumpup bonding-curve sell.
const PUMPUP_SELL_DISC: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

/// Disambiguate a Pumpup instruction by inspecting its first 8 data bytes.
/// Returns `PoolType::Pumpup` for AMM `swap`, `PoolType::PumpupBonding` for
/// `buy`/`sell`, and `None` for any other Pumpup ix (init/migrate/etc.) so
/// the scanner skips it instead of registering an unrelated account as a pool.
pub(crate) fn pumpup_pool_type_from_ix_data(ix_data_b58: &str) -> Option<PoolType> {
    let bytes = bs58::decode(ix_data_b58).into_vec().ok()?;
    if bytes.len() < 8 {
        return None;
    }
    let disc: [u8; 8] = bytes[..8].try_into().ok()?;
    if disc == PUMPUP_SWAP_DISC {
        Some(PoolType::Pumpup)
    } else if disc == PUMPUP_BUY_DISC || disc == PUMPUP_SELL_DISC {
        Some(PoolType::PumpupBonding)
    } else {
        None
    }
}

/// Extract (mint_a, mint_b) from a parsed PoolState.
pub fn extract_mints_from_state(state: &crate::pool::types::PoolState) -> Option<(Pubkey, Pubkey)> {
    use crate::pool::types::PoolState;
    match state {
        PoolState::RaydiumV4 { .. } => {
            // RaydiumV4 doesn't store mints in PoolState — skip
            None
        }
        PoolState::RaydiumCpmm {
            token_0_mint,
            token_1_mint,
            ..
        } => Some((*token_0_mint, *token_1_mint)),
        PoolState::RaydiumClmm {
            token_mint_0,
            token_mint_1,
            ..
        } => Some((*token_mint_0, *token_mint_1)),
        PoolState::RaydiumLp {
            base_mint,
            quote_mint,
            ..
        } => Some((*base_mint, *quote_mint)),
        PoolState::PumpFun { mint, .. } => Some((*mint, SOL_NATIVE_MINT)),
        PoolState::PumpFunAmm {
            base_mint,
            quote_mint,
            ..
        } => Some((*base_mint, *quote_mint)),
        PoolState::Meteora {
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_mint, *token_b_mint)),
        PoolState::MeteoraDlmm {
            token_x_mint,
            token_y_mint,
            ..
        } => Some((*token_x_mint, *token_y_mint)),
        PoolState::MeteoraDamm {
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_mint, *token_b_mint)),
        PoolState::MeteoraDbc {
            base_mint,
            quote_mint,
            ..
        } => Some((*base_mint, *quote_mint)),
        PoolState::Orca {
            token_mint_a,
            token_mint_b,
            ..
        } => Some((*token_mint_a, *token_mint_b)),
        PoolState::FluxBeam {
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_mint, *token_b_mint)),
        PoolState::FlashTrade { token_mint, .. } => Some((*token_mint, SOL_NATIVE_MINT)),
        PoolState::Byreal {
            token_mint_a,
            token_mint_b,
            ..
        } => Some((*token_mint_a, *token_mint_b)),
        PoolState::DefiTunaFusion {
            token_mint_a,
            token_mint_b,
            ..
        } => Some((*token_mint_a, *token_mint_b)),
        PoolState::DefiTunaPools {
            token_mint_a,
            token_mint_b,
            ..
        } => Some((*token_mint_a, *token_mint_b)),
        PoolState::Saros {
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_mint, *token_b_mint)),
        PoolState::PancakeSwap {
            token_mint_a,
            token_mint_b,
            ..
        } => Some((*token_mint_a, *token_mint_b)),
        PoolState::Dooar {
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_mint, *token_b_mint)),
        PoolState::Pumpup {
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_mint, *token_b_mint)),
        PoolState::PumpupBonding { mint, .. } => Some((*mint, SOL_NATIVE_MINT)),
    }
}

/// Scan a block for new pool addresses and register any that aren't already known.
///
/// Returns the number of newly discovered pools.
/// Extract new pool candidates from a block — sync, no RPC, no async.
/// Returns (pool_address, pool_type) pairs for pools not yet in the registry.
fn extract_candidates_from_block(
    block: &UiConfirmedBlock,
    registry: &PoolRegistry,
) -> Vec<(Pubkey, PoolType)> {
    let transactions = match &block.transactions {
        Some(txs) => txs,
        None => return Vec::new(),
    };

    let mut candidates: HashSet<(Pubkey, PoolType)> = HashSet::new();

    for encoded_tx in transactions {
        if let Some(ref meta) = encoded_tx.meta {
            if meta.err.is_some() { continue; }
        }

        let (account_keys, instructions) = match &encoded_tx.transaction {
            EncodedTransaction::Json(ui_tx) => match &ui_tx.message {
                UiMessage::Raw(raw) => (&raw.account_keys, &raw.instructions),
                UiMessage::Parsed(_) => continue,
            },
            _ => continue,
        };

        for ix in instructions {
            let prog_idx = ix.program_id_index as usize;
            if prog_idx >= account_keys.len() { continue; }

            let program_id = match Pubkey::from_str(&account_keys[prog_idx]) {
                Ok(pk) => pk,
                Err(_) => continue,
            };

            let pool_type = match dex_program_to_type(&program_id) {
                Some(pt) => pt,
                None => continue,
            };

            // Pumpup shares one program ID across two distinct pool types
            // (AMM vs bonding curve). Disambiguate via the ix discriminator.
            let pool_type = if program_id == PUMPUP_PROG_ID {
                match pumpup_pool_type_from_ix_data(&ix.data) {
                    Some(pt) => pt,
                    None => continue,
                }
            } else {
                pool_type
            };

            let pool_idx = extract_pool_index(pool_type);
            if pool_idx >= ix.accounts.len() { continue; }

            let account_idx = ix.accounts[pool_idx] as usize;
            if account_idx >= account_keys.len() { continue; }

            let pool_address = match Pubkey::from_str(&account_keys[account_idx]) {
                Ok(pk) => pk,
                Err(_) => continue,
            };

            if registry.contains(&pool_address) { continue; }
            candidates.insert((pool_address, pool_type));
        }
    }

    candidates.into_iter().collect()
}

/// Summary log interval in seconds.
const SUMMARY_INTERVAL_SECS: u64 = 30;

/// Run the block scanner loop. Polls recent blocks and discovers new pools.
///
/// This function runs forever (or until the task is cancelled).
///
/// Features:
/// - Exponential backoff on errors (1s -> 2s -> 4s -> ... -> 30s max, reset on success)
/// - Skip-ahead when falling behind by >100 slots (jumps to current-5)
/// - Summary logging every 30s instead of per-slot
pub async fn run_block_scanner(
    rpc: Arc<RpcClient>,
    registry: Arc<PoolRegistry>,
    cache: Arc<PoolCache>,
    stats: Arc<StreamStats>,
    pool_db: Arc<PoolDb>,
    _scan_interval_ms: u64, // unused — blockSubscribe is push-based
    swap_stream: Option<SwapStreamCtx>,
) {
    let ws_url = http_to_ws(&rpc.url());

    // Summary counters (reset every SUMMARY_INTERVAL_SECS)
    let mut summary_slots_scanned: u64 = 0;
    let mut summary_pools_discovered: u64 = 0;
    let mut summary_errors: u64 = 0;
    let mut last_summary = std::time::Instant::now();

    info!(
        swap_stream_enabled = swap_stream.is_some(),
        "block scanner started (blockSubscribe — real-time, every block)"
    );

    loop {
        match run_block_subscribe(
            &ws_url, &rpc, &registry, &cache, &stats, &pool_db,
            &mut summary_slots_scanned, &mut summary_pools_discovered, &mut summary_errors,
            &mut last_summary, swap_stream.as_ref(),
        ).await {
            Ok(()) => info!("blockSubscribe ended, reconnecting"),
            Err(e) => warn!(error = %e, "blockSubscribe error, reconnecting in 5s"),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Run a single blockSubscribe session. Returns on disconnect.
#[allow(clippy::too_many_arguments)]
async fn run_block_subscribe(
    ws_url: &str,
    rpc: &Arc<RpcClient>,
    registry: &Arc<PoolRegistry>,
    cache: &Arc<PoolCache>,
    stats: &Arc<StreamStats>,
    pool_db: &Arc<PoolDb>,
    summary_slots: &mut u64,
    summary_pools: &mut u64,
    summary_errors: &mut u64,
    last_summary: &mut std::time::Instant,
    swap_stream: Option<&SwapStreamCtx>,
) -> crate::error::TradeResult<()> {
    use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
    use solana_client::rpc_config::RpcBlockSubscribeFilter;
    use solana_client::rpc_config::RpcBlockSubscribeConfig;
    use futures::StreamExt;

    let pubsub = PubsubClient::new(ws_url)
        .await
        .map_err(|e| crate::error::TradeError::Rpc(format!("WS connect: {e}")))?;

    let config = RpcBlockSubscribeConfig {
        commitment: Some(CommitmentConfig::confirmed()),
        encoding: Some(UiTransactionEncoding::Json),
        transaction_details: Some(TransactionDetails::Full),
        show_rewards: Some(false),
        max_supported_transaction_version: Some(0),
    };

    // Subscribe to ALL blocks. We process each block synchronously (extract pool
    // candidates = fast, no RPC), then spawn async RPC fetches for new pools in
    // the background.
    let (mut stream, _unsub) = pubsub
        .block_subscribe(RpcBlockSubscribeFilter::All, Some(config))
        .await
        .map_err(|e| crate::error::TradeError::Rpc(format!("blockSubscribe: {e}")))?;

    info!("blockSubscribe active — receiving all blocks");

    // Semaphore to cap concurrent pool-fetch tasks
    let fetch_sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_DISCOVERIES));

    while let Some(notification) = stream.next().await {
        let update = notification.value;
        if let Some(block) = update.block {
            *summary_slots += 1;

            // Swap stream: parse + broadcast every confirmed DEX swap.
            // Lossy by design — slow consumers drop, never block.
            if let Some(ctx) = swap_stream {
                let emitted = parse_swaps_from_block(&block, &ctx.oracle, &ctx.tx);
                if emitted > 0 {
                    stats.record_swap_emitted(emitted as u64);
                }
            }

            // Extract pool candidates synchronously (fast — no RPC, no async)
            let candidates = extract_candidates_from_block(&block, registry);

            if !candidates.is_empty() {
                let placeholder = Pubkey::default();
                for (pool_address, pool_type) in candidates {
                    // Register immediately with placeholder mints.
                    // contains() will return true — prevents re-discovery.
                    // lookup() won't find it until background fetch updates mints.
                    registry.add(PoolEntry {
                        address: pool_address,
                        pool_type,
                        mint_a: placeholder,
                        mint_b: placeholder,
                    });

                    // Background: fetch real state + mints and update registry
                    let rpc = Arc::clone(rpc);
                    let registry = Arc::clone(registry);
                    let cache = Arc::clone(cache);
                    let stats = Arc::clone(stats);
                    let pool_db = Arc::clone(pool_db);
                    let sem = Arc::clone(&fetch_sem);

                    tokio::spawn(async move {
                        let _permit = sem.acquire().await;
                        match crate::pool::fetcher::fetch_pool_state(&rpc, pool_type, &pool_address).await {
                            Ok(state) => {
                                if let Some((mint_a, mint_b)) = extract_mints_from_state(&state) {
                                    let entry = PoolEntry {
                                        address: pool_address,
                                        pool_type,
                                        mint_a,
                                        mint_b,
                                    };
                                    let _ = pool_db.insert_pool(&entry);
                                    registry.add(entry); // updates mints from placeholder
                                    cache.insert(pool_address, state);
                                    stats.record_update();
                                }
                            }
                            Err(_) => {
                                // Not a valid pool — remove placeholder
                                registry.remove(&pool_address);
                            }
                        }
                    });
                    *summary_pools += 1;
                }
            }
        }

        // Periodic summary
        if last_summary.elapsed().as_secs() >= SUMMARY_INTERVAL_SECS {
            if *summary_slots > 0 || *summary_pools > 0 {
                info!(
                    slots = *summary_slots,
                    discovered = *summary_pools,
                    errors = *summary_errors,
                    registry = registry.len(),
                    "block scanner summary (last {}s)",
                    SUMMARY_INTERVAL_SECS
                );
            }
            *summary_slots = 0;
            *summary_pools = 0;
            *summary_errors = 0;
            *last_summary = std::time::Instant::now();
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dex_program_to_type_all_programs() {
        // Verify all 19 DEX programs are mapped
        assert_eq!(dex_program_to_type(&RAYDIUM_V4_PROG_ID), Some(PoolType::RaydiumV4));
        assert_eq!(dex_program_to_type(&RAYDIUM_CPMM_PROG_ID), Some(PoolType::RaydiumCpmm));
        assert_eq!(dex_program_to_type(&RAYDIUM_CL_PROG_ID), Some(PoolType::RaydiumCl));
        assert_eq!(dex_program_to_type(&RAYDIUM_LP_PROG_ID), Some(PoolType::RaydiumLp));
        assert_eq!(dex_program_to_type(&PUMP_FUN_PROG_ID), Some(PoolType::PumpFun));
        assert_eq!(dex_program_to_type(&PUMP_FUN_AMM_PROG_ID), Some(PoolType::PumpFunAmm));
        assert_eq!(dex_program_to_type(&METEORA_PROG_ID), Some(PoolType::Meteora));
        assert_eq!(dex_program_to_type(&METEORA_DLMM_PROG_ID), Some(PoolType::MeteoraDlmm));
        assert_eq!(dex_program_to_type(&METEORA_DAMM_PROG_ID), Some(PoolType::MeteoraDamm));
        assert_eq!(dex_program_to_type(&METEORA_DBC_PROG_ID), Some(PoolType::MeteoraDbc));
        assert_eq!(dex_program_to_type(&ORCA_PROG_ID), Some(PoolType::Orca));
        assert_eq!(dex_program_to_type(&FLUXBEAM_PROG_ID), Some(PoolType::FluxBeam));
        assert_eq!(dex_program_to_type(&SAROS_PROG_ID), Some(PoolType::Saros));
        assert_eq!(dex_program_to_type(&DOOAR_PROG_ID), Some(PoolType::Dooar));
        assert_eq!(dex_program_to_type(&PANCAKESWAP_PROG_ID), Some(PoolType::PancakeSwap));
        assert_eq!(dex_program_to_type(&FLASH_TRADE_PROG_ID), Some(PoolType::FlashTrade));
        assert_eq!(dex_program_to_type(&BYREAL_PROG_ID), Some(PoolType::Byreal));
        assert_eq!(dex_program_to_type(&DEFITUNA_FUSION_PROG_ID), Some(PoolType::DefiTunaFusion));
        assert_eq!(dex_program_to_type(&DEFITUNA_POOLS_PROG_ID), Some(PoolType::DefiTunaPools));
    }

    #[test]
    fn test_dex_program_to_type_unknown() {
        let unknown = Pubkey::new_unique();
        assert_eq!(dex_program_to_type(&unknown), None);
    }

    #[test]
    fn test_dex_program_to_type_count() {
        // 19 base DEX programs + Pumpup = 20.
        // OnChain Labs DEX V2 is intentionally not mapped here (aggregator router,
        // not a directly-quotable DEX — see comment in build_program_map).
        let map = build_program_map();
        assert_eq!(map.len(), 20);
    }

    #[test]
    fn test_extract_pool_index_accounts_1() {
        // DEXes where pool address is at accounts[1]
        assert_eq!(extract_pool_index(PoolType::RaydiumV4), 1);
        assert_eq!(extract_pool_index(PoolType::RaydiumCpmm), 1);
        assert_eq!(extract_pool_index(PoolType::RaydiumLp), 1);
        assert_eq!(extract_pool_index(PoolType::Meteora), 1);
        assert_eq!(extract_pool_index(PoolType::MeteoraDlmm), 1);
        assert_eq!(extract_pool_index(PoolType::FluxBeam), 1);
        assert_eq!(extract_pool_index(PoolType::Saros), 1);
        assert_eq!(extract_pool_index(PoolType::Dooar), 1);
    }

    #[test]
    fn test_extract_pool_index_accounts_2() {
        // DEXes where pool address is at accounts[2]
        assert_eq!(extract_pool_index(PoolType::RaydiumCl), 2);
        assert_eq!(extract_pool_index(PoolType::PumpFun), 2);
        assert_eq!(extract_pool_index(PoolType::MeteoraDamm), 2);
        assert_eq!(extract_pool_index(PoolType::MeteoraDbc), 2);
        assert_eq!(extract_pool_index(PoolType::Orca), 2);
        assert_eq!(extract_pool_index(PoolType::PancakeSwap), 2);
    }

    #[test]
    fn test_extract_pool_index_accounts_3() {
        // DEXes where pool address is at accounts[3]
        assert_eq!(extract_pool_index(PoolType::PumpFunAmm), 3);
    }

    #[test]
    fn test_extract_pool_index_all_variants_handled() {
        // Ensure every PoolType variant returns a valid index (no panics)
        let variants = [
            PoolType::Unknown,
            PoolType::RaydiumV4,
            PoolType::RaydiumCpmm,
            PoolType::RaydiumCl,
            PoolType::RaydiumLp,
            PoolType::PumpFun,
            PoolType::PumpFunAmm,
            PoolType::Meteora,
            PoolType::MeteoraDlmm,
            PoolType::MeteoraDamm,
            PoolType::MeteoraDbc,
            PoolType::Orca,
            PoolType::FluxBeam,
            PoolType::FlashTrade,
            PoolType::Byreal,
            PoolType::DefiTunaFusion,
            PoolType::DefiTunaPools,
            PoolType::Saros,
            PoolType::PancakeSwap,
            PoolType::Dooar,
        ];
        for v in &variants {
            let idx = extract_pool_index(*v);
            assert!(idx <= 3, "unexpected pool index {} for {:?}", idx, v);
        }
    }

    #[test]
    fn test_extract_mints_from_state_raydium_cpmm() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::RaydiumCpmm {
            pool: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            config: Pubkey::new_unique(),
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            token_0_mint: mint_a,
            token_1_mint: mint_b,
            observation: Pubkey::new_unique(),
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_a, mint_b)));
    }

    #[test]
    fn test_extract_mints_from_state_pumpfun_amm() {
        let base = Pubkey::new_unique();
        let quote = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::PumpFunAmm {
            pool: Pubkey::new_unique(),
            base_mint: base,
            quote_mint: quote,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 1000,
            quote_reserve: 2000,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((base, quote)));
    }

    #[test]
    fn test_extract_mints_from_state_orca() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::Orca {
            whirlpool: Pubkey::new_unique(),
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            oracle: Pubkey::new_unique(),
            token_mint_a: mint_a,
            token_mint_b: mint_b,
            tick_current: 0,
            tick_spacing: 64,
            sqrt_price_x64: 0,
            liquidity: 0,
            fee_rate: 0,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_a, mint_b)));
    }

    #[test]
    fn test_extract_mints_from_state_meteora_damm() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::MeteoraDamm {
            pool: Pubkey::new_unique(),
            token_a_vault: Pubkey::new_unique(),
            token_b_vault: Pubkey::new_unique(),
            token_a_mint: mint_a,
            token_b_mint: mint_b,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_a, mint_b)));
    }

    #[test]
    fn test_extract_mints_from_state_raydium_v4_returns_none() {
        // RaydiumV4 doesn't store mints in PoolState
        let state = crate::pool::types::PoolState::RaydiumV4 {
            amm_id: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            open_orders: Pubkey::new_unique(),
            target_orders: Pubkey::new_unique(),
            coin_vault: Pubkey::new_unique(),
            pc_vault: Pubkey::new_unique(),
            serum_program: Pubkey::new_unique(),
            serum_market: Pubkey::new_unique(),
            serum_bids: Pubkey::new_unique(),
            serum_asks: Pubkey::new_unique(),
            serum_event_queue: Pubkey::new_unique(),
            serum_coin_vault: Pubkey::new_unique(),
            serum_pc_vault: Pubkey::new_unique(),
            serum_vault_signer: Pubkey::new_unique(),
        };
        let result = extract_mints_from_state(&state);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_mints_from_state_pumpfun_bonding() {
        let mint = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::PumpFun {
            global: Pubkey::new_unique(),
            fee_account: Pubkey::new_unique(),
            mint,
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            event_authority: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint, SOL_NATIVE_MINT)));
    }

    #[test]
    fn test_extract_mints_from_state_meteora_dlmm() {
        let mint_x = Pubkey::new_unique();
        let mint_y = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::MeteoraDlmm {
            lb_pair: Pubkey::new_unique(),
            bin_array_bitmap_extension: Pubkey::new_unique(),
            reserve_x: Pubkey::new_unique(),
            reserve_y: Pubkey::new_unique(),
            token_x_mint: mint_x,
            token_y_mint: mint_y,
            oracle: Pubkey::new_unique(),
            host_fee_in: Pubkey::new_unique(),
            event_authority: Pubkey::new_unique(),
            bin_arrays: vec![],
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_x, mint_y)));
    }

    #[test]
    fn test_extract_mints_from_state_all_variants() {
        // Verify that every PoolState variant is handled (no panics)
        // Build a representative state for each variant and verify it returns Some or None
        let pk = || Pubkey::new_unique();

        let states = vec![
            crate::pool::types::PoolState::RaydiumV4 {
                amm_id: pk(), authority: pk(), open_orders: pk(), target_orders: pk(),
                coin_vault: pk(), pc_vault: pk(), serum_program: pk(), serum_market: pk(),
                serum_bids: pk(), serum_asks: pk(), serum_event_queue: pk(),
                serum_coin_vault: pk(), serum_pc_vault: pk(), serum_vault_signer: pk(),
            },
            crate::pool::types::PoolState::RaydiumCpmm {
                pool: pk(), authority: pk(), config: pk(),
                token_0_vault: pk(), token_1_vault: pk(),
                token_0_mint: pk(), token_1_mint: pk(), observation: pk(),
            },
            crate::pool::types::PoolState::RaydiumClmm {
                pool: pk(), amm_config: pk(), observation: pk(),
                token_vault_0: pk(), token_vault_1: pk(),
                tick_array_0: pk(), tick_array_1: pk(), tick_array_2: pk(),
                token_mint_0: pk(), token_mint_1: pk(),
                tick_current: 0, tick_spacing: 1,
                sqrt_price_x64: 0, liquidity: 0, fee_rate: 0,
            },
            crate::pool::types::PoolState::Orca {
                whirlpool: pk(), token_vault_a: pk(), token_vault_b: pk(),
                oracle: pk(), token_mint_a: pk(), token_mint_b: pk(),
                tick_current: 0, tick_spacing: 64,
                sqrt_price_x64: 0, liquidity: 0, fee_rate: 0,
            },
            crate::pool::types::PoolState::MeteoraDamm {
                pool: pk(), token_a_vault: pk(), token_b_vault: pk(),
                token_a_mint: pk(), token_b_mint: pk(),
            },
        ];

        for state in &states {
            // Just verify no panic
            let _ = extract_mints_from_state(state);
        }
    }

    #[test]
    fn test_registry_contains() {
        let registry = PoolRegistry::new();
        let addr = Pubkey::new_unique();
        assert!(!registry.contains(&addr));

        registry.add(PoolEntry {
            address: addr,
            pool_type: PoolType::Orca,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        });
        assert!(registry.contains(&addr));
    }

    #[test]
    fn test_registry_contains_after_remove() {
        let registry = PoolRegistry::new();
        let addr = Pubkey::new_unique();

        registry.add(PoolEntry {
            address: addr,
            pool_type: PoolType::Orca,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        });
        assert!(registry.contains(&addr));

        registry.remove(&addr);
        assert!(!registry.contains(&addr));
    }

    #[test]
    fn test_build_program_map_unique_keys() {
        let map = build_program_map();
        // All keys must be unique (HashMap guarantees this, but let's verify count).
        // 19 base + Pumpup = 20. OnChain Labs is excluded by design (aggregator).
        assert_eq!(map.len(), 20);
        // All values should be non-Unknown
        for (_, pt) in &map {
            assert_ne!(*pt, PoolType::Unknown);
        }
    }

    #[test]
    fn test_extract_mints_from_state_raydium_lp() {
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::RaydiumLp {
            pool_state: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            base_vault: Pubkey::new_unique(),
            quote_vault: Pubkey::new_unique(),
            base_mint,
            quote_mint,
            config_id: Pubkey::new_unique(),
            platform_id: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((base_mint, quote_mint)));
    }

    #[test]
    fn test_extract_mints_from_state_meteora_dbc() {
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::MeteoraDbc {
            pool: Pubkey::new_unique(),
            config: Pubkey::new_unique(),
            pool_authority: Pubkey::new_unique(),
            base_vault: Pubkey::new_unique(),
            quote_vault: Pubkey::new_unique(),
            base_mint,
            quote_mint,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((base_mint, quote_mint)));
    }

    #[test]
    fn test_extract_mints_from_state_fluxbeam() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::FluxBeam {
            pool: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            token_a_vault: Pubkey::new_unique(),
            token_b_vault: Pubkey::new_unique(),
            pool_mint: Pubkey::new_unique(),
            fee_account: Pubkey::new_unique(),
            token_a_mint: mint_a,
            token_b_mint: mint_b,
            pool_token_program: Pubkey::new_unique(),
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_a, mint_b)));
    }

    #[test]
    fn test_extract_mints_from_state_saros() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::Saros {
            pool: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            token_a_vault: Pubkey::new_unique(),
            token_b_vault: Pubkey::new_unique(),
            pool_mint: Pubkey::new_unique(),
            fee_account: Pubkey::new_unique(),
            token_a_mint: mint_a,
            token_b_mint: mint_b,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_a, mint_b)));
    }

    #[test]
    fn test_extract_mints_from_state_dooar() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::Dooar {
            pool: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            token_a_vault: Pubkey::new_unique(),
            token_b_vault: Pubkey::new_unique(),
            pool_mint: Pubkey::new_unique(),
            fee_account: Pubkey::new_unique(),
            token_a_mint: mint_a,
            token_b_mint: mint_b,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_a, mint_b)));
    }

    #[test]
    fn test_extract_mints_from_state_pancakeswap() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::PancakeSwap {
            pool: Pubkey::new_unique(),
            amm_config: Pubkey::new_unique(),
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            observation: Pubkey::new_unique(),
            token_mint_a: mint_a,
            token_mint_b: mint_b,
            tick_current: 0,
            tick_spacing: 1,
            sqrt_price_x64: 0, liquidity: 0, fee_rate: 0,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_a, mint_b)));
    }

    #[test]
    fn test_extract_mints_from_state_defituna_fusion() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::DefiTunaFusion {
            pool: Pubkey::new_unique(),
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            token_mint_a: mint_a,
            token_mint_b: mint_b,
            tick_spacing: 1,
            tick_current_index: 0,
            sqrt_price_x64: 0, liquidity: 0, fee_rate: 0,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_a, mint_b)));
    }

    #[test]
    fn test_extract_mints_from_state_byreal() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::Byreal {
            pool: Pubkey::new_unique(),
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            oracle: Pubkey::new_unique(),
            token_mint_a: mint_a,
            token_mint_b: mint_b,
            tick_current: 0,
            tick_spacing: 1,
            sqrt_price_x64: 0, liquidity: 0,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_a, mint_b)));
    }

    #[test]
    fn test_extract_mints_from_state_raydium_clmm() {
        let mint_0 = Pubkey::new_unique();
        let mint_1 = Pubkey::new_unique();
        let state = crate::pool::types::PoolState::RaydiumClmm {
            pool: Pubkey::new_unique(),
            amm_config: Pubkey::new_unique(),
            observation: Pubkey::new_unique(),
            token_vault_0: Pubkey::new_unique(),
            token_vault_1: Pubkey::new_unique(),
            tick_array_0: Pubkey::new_unique(),
            tick_array_1: Pubkey::new_unique(),
            tick_array_2: Pubkey::new_unique(),
            token_mint_0: mint_0,
            token_mint_1: mint_1,
            tick_current: 0,
            tick_spacing: 1,
            sqrt_price_x64: 0, liquidity: 0, fee_rate: 0,
        };
        let result = extract_mints_from_state(&state);
        assert_eq!(result, Some((mint_0, mint_1)));
    }
}
