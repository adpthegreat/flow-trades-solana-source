use std::sync::Arc;
use std::time::Instant;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tracing::debug;

use crate::constants::BRIDGE_MINTS;
use crate::error::{TradeError, TradeResult};
use crate::pool::cache::PoolCache;
use crate::pool::registry::{PoolEntry, PoolRegistry};
use crate::pool::types::{PoolState, PoolType};

use super::math::{compute_constant_product_out, compute_clmm_output_multi_tick, compute_fee_amount, compute_price_impact_for_type, estimate_price_impact, extract_clmm_params};
use super::types::{
    PlatformFee, QuoteRequest, QuoteResponse, RouteStep, PoolRoute, compute_threshold,
};

/// Default fee in basis points for constant-product AMMs that don't expose their fee on-chain.
const DEFAULT_FEE_BPS: u16 = 25;

/// Platform fee in basis points (0.5%).
const PLATFORM_FEE_BPS: u16 = 50;

/// Compute the platform fee for a quote. Always taken from the output token.
fn compute_platform_fee(
    amount_out: u64,
    output_mint: &Pubkey,
) -> PlatformFee {
    let fee_amount = (amount_out as u128 * PLATFORM_FEE_BPS as u128 / 10_000) as u64;
    PlatformFee {
        amount: fee_amount.to_string(),
        fee_bps: PLATFORM_FEE_BPS,
        fee_token: output_mint.to_string(),
        side: "output".to_string(),
    }
}

/// Pool types that use constant-product math (x * y = k).
fn is_constant_product(pool_type: PoolType) -> bool {
    matches!(
        pool_type,
        PoolType::RaydiumV4
            | PoolType::RaydiumCpmm
            | PoolType::RaydiumLp
            | PoolType::PumpFunAmm
            | PoolType::Meteora
            | PoolType::MeteoraDamm
            | PoolType::FluxBeam
            | PoolType::Saros
            | PoolType::Dooar
            | PoolType::Pumpup
            | PoolType::PumpupBonding
    )
}

/// Pool types that use CLMM (concentrated liquidity) math.
fn is_clmm(pool_type: PoolType) -> bool {
    matches!(
        pool_type,
        PoolType::RaydiumCl
            | PoolType::Orca
            | PoolType::PancakeSwap
            | PoolType::Byreal
            | PoolType::DefiTunaFusion
    )
}

/// Check if a pool type is quotable (constant product or CLMM).
fn is_quotable(pool_type: PoolType) -> bool {
    is_constant_product(pool_type) || is_clmm(pool_type)
}

/// Get the label for a pool type from the program_id_to_label mapping.
pub(crate) fn label_for_pool_type(pool_type: PoolType) -> &'static str {
    match pool_type {
        PoolType::RaydiumV4 => "Raydium V4",
        PoolType::RaydiumCpmm => "Raydium CPMM",
        PoolType::RaydiumCl => "Raydium CLMM",
        PoolType::RaydiumLp => "Raydium LP",
        PoolType::PumpFun => "PumpFun",
        PoolType::PumpFunAmm => "PumpFun AMM",
        PoolType::Meteora => "Meteora",
        PoolType::MeteoraDlmm => "Meteora DLMM",
        PoolType::MeteoraDamm => "Meteora DAMM",
        PoolType::MeteoraDbc => "Meteora DBC",
        PoolType::Orca => "Orca",
        PoolType::FluxBeam => "FluxBeam",
        PoolType::FlashTrade => "FlashTrade",
        PoolType::Byreal => "Byreal",
        PoolType::DefiTunaFusion => "DefiTuna Fusion",
        PoolType::DefiTunaPools => "DefiTuna Pools",
        PoolType::Saros => "Saros",
        PoolType::PancakeSwap => "PancakeSwap",
        PoolType::Dooar => "Dooar",
        PoolType::Pumpup => "Pumpup",
        PoolType::PumpupBonding => "Pumpup Bonding",
        _ => "Unknown",
    }
}

/// A computed direct route candidate with output amount and metadata.
#[derive(Debug, Clone)]
struct DirectRoute {
    pool_address: Pubkey,
    pool_type: PoolType,
    out_amount: u64,
    fee_amount: u64,
    reserve_in: u128,
    reserve_out: u128,
}

/// A computed 2-hop route candidate.
#[derive(Debug, Clone)]
struct TwoHopRoute {
    hop1_entry: PoolEntry,
    hop2_entry: PoolEntry,
    bridge_mint: Pubkey,
    hop1_amount_out: u64,
    hop1_fee_amount: u64,
    hop1_reserve_in: u128,
    hop1_reserve_out: u128,
    final_amount_out: u64,
    hop2_fee_amount: u64,
    hop2_reserve_in: u128,
    hop2_reserve_out: u128,
}

/// A computed 3-hop route candidate.
#[derive(Debug, Clone)]
struct ThreeHopRoute {
    hop1_entry: PoolEntry,
    hop2_entry: PoolEntry,
    hop3_entry: PoolEntry,
    bridge1_mint: Pubkey,
    bridge2_mint: Pubkey,
    hop1_amount_out: u64,
    hop1_fee_amount: u64,
    hop1_reserve_in: u128,
    hop1_reserve_out: u128,
    hop2_amount_out: u64,
    hop2_fee_amount: u64,
    hop2_reserve_in: u128,
    hop2_reserve_out: u128,
    final_amount_out: u64,
    hop3_fee_amount: u64,
    hop3_reserve_in: u128,
    hop3_reserve_out: u128,
}

/// A computed split route: divide input across 2 pools for better output on large trades.
#[derive(Debug, Clone)]
struct SplitRoute {
    pool_a: DirectRoute,
    pool_b: DirectRoute,
    pct_a: u8,
    pct_b: u8,
    amount_a: u64,
    amount_b: u64,
    out_a: u64,
    out_b: u64,
    total_out: u64,
    fee_a: u64,
    fee_b: u64,
}

/// The Quoter finds the best swap route for a given token pair.
pub struct Quoter {
    registry: Arc<PoolRegistry>,
    cache: Arc<PoolCache>,
    rpc: Arc<RpcClient>,
    mirror: Option<Arc<crate::stream::account_mirror::AccountMirror>>,
}

impl Quoter {
    pub fn new(
        registry: Arc<PoolRegistry>,
        cache: Arc<PoolCache>,
        rpc: Arc<RpcClient>,
    ) -> Self {
        Self {
            registry,
            cache,
            rpc,
            mirror: None,
        }
    }

    /// Create a Quoter with an AccountMirror for zero-RPC vault balance lookups.
    pub fn with_mirror(
        registry: Arc<PoolRegistry>,
        cache: Arc<PoolCache>,
        rpc: Arc<RpcClient>,
        mirror: Arc<crate::stream::account_mirror::AccountMirror>,
    ) -> Self {
        Self {
            registry,
            cache,
            rpc,
            mirror: Some(mirror),
        }
    }

    /// Find the best quote for a swap request.
    pub async fn quote(&self, req: &QuoteRequest) -> TradeResult<QuoteResponse> {
        let start = Instant::now();

        // 1. Evaluate direct routes
        let direct_routes = self.evaluate_direct_routes(req).await;

        // Split route quoting disabled — the swap builder cannot execute splits yet.
        // When split execution is implemented, re-enable this line:
        // let best_split = evaluate_split_routes(&direct_routes, req.amount);
        let best_split: Option<SplitRoute> = None;

        // 3. Evaluate 2-hop routes (unless only_direct_routes is true)
        let two_hop_routes = if req.only_direct_routes {
            Vec::new()
        } else {
            self.evaluate_two_hop_routes(req).await
        };

        // 4. Evaluate 3-hop routes (unless only_direct_routes is true)
        let three_hop_routes = if req.only_direct_routes {
            Vec::new()
        } else {
            self.evaluate_three_hop_routes(req).await
        };

        // 5. Pick the best route (direct, split, 2-hop, or 3-hop)
        let best_direct = direct_routes.iter().max_by_key(|r| r.out_amount);
        let best_two_hop = two_hop_routes.iter().max_by_key(|r| r.final_amount_out);
        let best_three_hop = three_hop_routes.iter().max_by_key(|r| r.final_amount_out);

        let context_slot: u64 = 0;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0; // milliseconds

        // Find the overall best output amount
        let direct_out = best_direct.map(|r| r.out_amount).unwrap_or(0);
        let split_out = best_split.as_ref().map(|r| r.total_out).unwrap_or(0);
        let two_hop_out = best_two_hop.map(|r| r.final_amount_out).unwrap_or(0);
        let three_hop_out = best_three_hop.map(|r| r.final_amount_out).unwrap_or(0);

        let max_out = direct_out.max(split_out).max(two_hop_out).max(three_hop_out);

        if max_out == 0 {
            return Err(TradeError::NoRoute {
                input_mint: req.input_mint.to_string(),
                output_mint: req.output_mint.to_string(),
            });
        }

        if max_out == split_out {
            if let Some(ref split) = best_split {
                return Ok(self.build_split_response(req, split, context_slot, elapsed));
            }
        }

        if max_out == three_hop_out {
            if let Some(three_hop) = best_three_hop {
                return Ok(self.build_three_hop_response(req, three_hop, context_slot, elapsed));
            }
        }

        if max_out == two_hop_out {
            if let Some(two_hop) = best_two_hop {
                return Ok(self.build_two_hop_response(req, two_hop, context_slot, elapsed));
            }
        }

        if let Some(direct) = best_direct {
            return Ok(self.build_direct_response(req, direct, context_slot, elapsed));
        }

        Err(TradeError::NoRoute {
            input_mint: req.input_mint.to_string(),
            output_mint: req.output_mint.to_string(),
        })
    }

    /// Evaluate all direct routes for the given token pair.
    async fn evaluate_direct_routes(&self, req: &QuoteRequest) -> Vec<DirectRoute> {
        let entries = self.filter_entries(
            &req.input_mint,
            &req.output_mint,
            &req.dexes,
            &req.exclude_dexes,
        );

        // Evaluate all pools in parallel — Geyser-fed cache returns in microseconds.
        let futs: Vec<_> = entries.iter().map(|entry| {
            let input = req.input_mint;
            let output = req.output_mint;
            let amount = req.amount;
            async move {
                self.evaluate_single_pool(entry, &input, &output, amount)
                    .await
                    .map(|(out_amount, fee_amount, reserve_in, reserve_out)| DirectRoute {
                        pool_address: entry.address,
                        pool_type: entry.pool_type,
                        out_amount,
                        fee_amount,
                        reserve_in,
                        reserve_out,
                    })
            }
        }).collect();

        let results = futures::future::join_all(futs).await;
        let mut candidates: Vec<DirectRoute> = results.into_iter().flatten().collect();

        // Sort by best output descending
        candidates.sort_by(|a, b| b.out_amount.cmp(&a.out_amount));
        candidates
    }

    /// Evaluate 2-hop routes through bridge mints (SOL, USDC, USDT).
    async fn evaluate_two_hop_routes(&self, req: &QuoteRequest) -> Vec<TwoHopRoute> {
        let mut routes = Vec::new();

        for bridge in &BRIDGE_MINTS {
            // Skip if bridge is already the input or output mint
            if *bridge == req.input_mint || *bridge == req.output_mint {
                continue;
            }

            // Find pools for hop1 (input -> bridge) and hop2 (bridge -> output)
            let hop1_entries = self.filter_entries(
                &req.input_mint,
                bridge,
                &req.dexes,
                &req.exclude_dexes,
            );
            let hop2_entries = self.filter_entries(
                bridge,
                &req.output_mint,
                &req.dexes,
                &req.exclude_dexes,
            );

            if hop1_entries.is_empty() || hop2_entries.is_empty() {
                continue;
            }

            // Evaluate hop1 routes
            for h1 in &hop1_entries {
                let h1_result = self.evaluate_single_pool(
                    h1, &req.input_mint, bridge, req.amount,
                ).await;

                if let Some((h1_out, h1_fee, h1_res_in, h1_res_out)) = h1_result {
                    // Evaluate hop2 routes using hop1 output
                    for h2 in &hop2_entries {
                        let h2_result = self.evaluate_single_pool(
                            h2, bridge, &req.output_mint, h1_out,
                        ).await;

                        if let Some((h2_out, h2_fee, h2_res_in, h2_res_out)) = h2_result {
                            routes.push(TwoHopRoute {
                                hop1_entry: h1.clone(),
                                hop2_entry: h2.clone(),
                                bridge_mint: *bridge,
                                hop1_amount_out: h1_out,
                                hop1_fee_amount: h1_fee,
                                hop1_reserve_in: h1_res_in,
                                hop1_reserve_out: h1_res_out,
                                final_amount_out: h2_out,
                                hop2_fee_amount: h2_fee,
                                hop2_reserve_in: h2_res_in,
                                hop2_reserve_out: h2_res_out,
                            });
                        }
                    }
                }
            }
        }

        routes
    }

    /// Evaluate 3-hop routes through pairs of bridge mints.
    ///
    /// For each (bridge1, bridge2) pair from BRIDGE_MINTS where bridge1 != bridge2
    /// and neither equals input or output:
    ///   hop1: input -> bridge1
    ///   hop2: bridge1 -> bridge2
    ///   hop3: bridge2 -> output
    ///
    /// To avoid combinatorial explosion, we only evaluate the best hop1 pool (by output)
    /// per bridge1, and the best hop2 pool per bridge pair. This keeps it O(bridges^2).
    async fn evaluate_three_hop_routes(&self, req: &QuoteRequest) -> Vec<ThreeHopRoute> {
        let mut routes = Vec::new();

        for bridge1 in &BRIDGE_MINTS {
            // Skip if bridge1 is already the input or output mint
            if *bridge1 == req.input_mint || *bridge1 == req.output_mint {
                continue;
            }

            // Find the best hop1 pool: input -> bridge1
            let hop1_entries = self.filter_entries(
                &req.input_mint,
                bridge1,
                &req.dexes,
                &req.exclude_dexes,
            );

            if hop1_entries.is_empty() {
                continue;
            }

            // Evaluate all hop1 pools and pick the best by output
            let mut best_h1: Option<(&PoolEntry, u64, u64, u128, u128)> = None;
            for h1 in &hop1_entries {
                if let Some((h1_out, h1_fee, h1_res_in, h1_res_out)) =
                    self.evaluate_single_pool(h1, &req.input_mint, bridge1, req.amount).await
                {
                    if best_h1.as_ref().map_or(true, |(_, best_out, _, _, _)| h1_out > *best_out) {
                        best_h1 = Some((h1, h1_out, h1_fee, h1_res_in, h1_res_out));
                    }
                }
            }

            let (h1_entry, h1_out, h1_fee, h1_res_in, h1_res_out) = match best_h1 {
                Some(v) => v,
                None => continue,
            };

            for bridge2 in &BRIDGE_MINTS {
                // Skip if bridge2 equals bridge1, input, or output
                if *bridge2 == *bridge1 || *bridge2 == req.input_mint || *bridge2 == req.output_mint {
                    continue;
                }

                // Find the best hop2 pool: bridge1 -> bridge2
                let hop2_entries = self.filter_entries(
                    bridge1,
                    bridge2,
                    &req.dexes,
                    &req.exclude_dexes,
                );

                if hop2_entries.is_empty() {
                    continue;
                }

                let mut best_h2: Option<(&PoolEntry, u64, u64, u128, u128)> = None;
                for h2 in &hop2_entries {
                    if let Some((h2_out, h2_fee, h2_res_in, h2_res_out)) =
                        self.evaluate_single_pool(h2, bridge1, bridge2, h1_out).await
                    {
                        if best_h2.as_ref().map_or(true, |(_, best_out, _, _, _)| h2_out > *best_out) {
                            best_h2 = Some((h2, h2_out, h2_fee, h2_res_in, h2_res_out));
                        }
                    }
                }

                let (h2_entry, h2_out, h2_fee, h2_res_in, h2_res_out) = match best_h2 {
                    Some(v) => v,
                    None => continue,
                };

                // Find hop3 pools: bridge2 -> output
                let hop3_entries = self.filter_entries(
                    bridge2,
                    &req.output_mint,
                    &req.dexes,
                    &req.exclude_dexes,
                );

                for h3 in &hop3_entries {
                    if let Some((h3_out, h3_fee, h3_res_in, h3_res_out)) =
                        self.evaluate_single_pool(h3, bridge2, &req.output_mint, h2_out).await
                    {
                        routes.push(ThreeHopRoute {
                            hop1_entry: h1_entry.clone(),
                            hop2_entry: h2_entry.clone(),
                            hop3_entry: h3.clone(),
                            bridge1_mint: *bridge1,
                            bridge2_mint: *bridge2,
                            hop1_amount_out: h1_out,
                            hop1_fee_amount: h1_fee,
                            hop1_reserve_in: h1_res_in,
                            hop1_reserve_out: h1_res_out,
                            hop2_amount_out: h2_out,
                            hop2_fee_amount: h2_fee,
                            hop2_reserve_in: h2_res_in,
                            hop2_reserve_out: h2_res_out,
                            final_amount_out: h3_out,
                            hop3_fee_amount: h3_fee,
                            hop3_reserve_in: h3_res_in,
                            hop3_reserve_out: h3_res_out,
                        });
                    }
                }
            }
        }

        routes
    }

    /// Evaluate a single pool for a given input/output/amount.
    /// Returns Some((out_amount, fee_amount, reserve_in, reserve_out)) or None.
    async fn evaluate_single_pool(
        &self,
        entry: &PoolEntry,
        input_mint: &Pubkey,
        _output_mint: &Pubkey,
        amount: u64,
    ) -> Option<(u64, u64, u128, u128)> {
        // Try cache first (nanoseconds). In Geyser mode the cache never expires,
        // so this almost always hits. On miss, fall back to RPC fetch + cache.
        let state = match self.cache.get(&entry.address) {
            Some(s) => s,
            None => {
                // Cache miss — fetch from RPC and cache the result.
                // This path is only hit for pools discovered from SQLite bootstrap
                // that haven't received a Geyser update yet.
                match crate::pool::fetcher::fetch_pool_state(&self.rpc, entry.pool_type, &entry.address).await {
                    Ok(s) => {
                        self.cache.insert(entry.address, s.clone());
                        s
                    }
                    Err(_) => return None,
                }
            }
        };

        // Branch: CLMM pools use tick-based math, constant-product pools use reserve math
        if is_clmm(entry.pool_type) {
            // Extract CLMM parameters from pool state
            let params = match extract_clmm_params(&state, input_mint) {
                Some(p) if p.sqrt_price_x64 > 0 && p.liquidity > 0 => p,
                _ => {
                    debug!(pool = %entry.address, "CLMM pool has no sqrt_price/liquidity data");
                    return None;
                }
            };

            let out = match compute_clmm_output_multi_tick(
                params.sqrt_price_x64,
                params.liquidity,
                amount,
                params.fee_bps,
                params.a_to_b,
                &params.tick_liquidities,
            ) {
                Some(o) if o > 0 => o,
                _ => {
                    debug!(pool = %entry.address, "CLMM zero output");
                    return None;
                }
            };

            let fee_amount = compute_fee_amount(amount, params.fee_bps);
            // Derive virtual reserves for price impact computation
            let q64: u128 = 1u128 << 64;
            let reserve_a = params.liquidity.saturating_mul(q64) / params.sqrt_price_x64.max(1);
            let reserve_b = params.liquidity.saturating_mul(params.sqrt_price_x64) / q64.max(1);
            let (reserve_in, reserve_out) = if params.a_to_b {
                (reserve_a, reserve_b)
            } else {
                (reserve_b, reserve_a)
            };

            return Some((out, fee_amount, reserve_in, reserve_out));
        }

        // Constant-product pools: get reserves from inline data, mirror, or vault RPC
        let (reserve_in, reserve_out) = match extract_reserves_inline(&state, input_mint) {
            Some(r) => r,
            None => {
                // Try mirror vault balances (zero RPC, nanosecond latency)
                match self.get_reserves_from_mirror(&state, input_mint) {
                    Some(r) => r,
                    None => {
                        // Fall back to RPC vault balance fetch.
                        // On success, seed the mirror so next quote is instant.
                        match fetch_reserves(&self.rpc, &state, input_mint).await {
                            Some(r) => {
                                if let Some(mirror) = self.mirror.as_ref() {
                                    if let Some((va, vb, ma, _mb)) = extract_vault_mints(&state) {
                                        let (bal_in, bal_out) = r;
                                        let (bal_a, bal_b) = if *input_mint == ma {
                                            (bal_in as u64, bal_out as u64)
                                        } else {
                                            (bal_out as u64, bal_in as u64)
                                        };
                                        mirror.update_vault_balance(va, bal_a);
                                        mirror.update_vault_balance(vb, bal_b);
                                        if !mirror.is_vault(&va) {
                                            mirror.register_vault(va, entry.address);
                                        }
                                        if !mirror.is_vault(&vb) {
                                            mirror.register_vault(vb, entry.address);
                                        }
                                    }
                                }
                                r
                            }
                            None => {
                                debug!(pool = %entry.address, "could not extract or fetch reserves");
                                return None;
                            }
                        }
                    }
                }
            }
        };

        // Compute output amount
        let fee_bps = fee_for_pool_type(entry.pool_type);
        let out = match compute_constant_product_out(reserve_in, reserve_out, amount, fee_bps) {
            Some(o) if o > 0 => o,
            _ => {
                debug!(pool = %entry.address, "zero output");
                return None;
            }
        };

        let fee_amount = compute_fee_amount(amount, fee_bps);
        Some((out, fee_amount, reserve_in, reserve_out))
    }

    /// Try to get reserves from the AccountMirror's vault balance cache.
    /// Returns (reserve_in, reserve_out) if both vault balances are available.
    /// Zero RPC — reads from the in-memory mirror fed by Geyser.
    fn get_reserves_from_mirror(
        &self,
        state: &PoolState,
        input_mint: &Pubkey,
    ) -> Option<(u128, u128)> {
        let mirror = self.mirror.as_ref()?;
        let (vault_a, vault_b, mint_a, mint_b) = extract_vault_mints(state)?;

        // Can't determine direction without mints (RaydiumV4)
        if mint_a == Pubkey::default() && mint_b == Pubkey::default() {
            return None;
        }

        let bal_a = match mirror.get_vault_balance(&vault_a) {
            Some(b) => b as u128,
            None => {
                debug!(vault = %vault_a, "mirror miss: vault A balance not cached");
                return None;
            }
        };
        let bal_b = match mirror.get_vault_balance(&vault_b) {
            Some(b) => b as u128,
            None => {
                debug!(vault = %vault_b, "mirror miss: vault B balance not cached");
                return None;
            }
        };

        if *input_mint == mint_a {
            Some((bal_a, bal_b))
        } else {
            Some((bal_b, bal_a))
        }
    }

    /// Filter pool entries by mint pair + dex whitelist/blacklist.
    fn filter_entries(
        &self,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        dexes: &[String],
        exclude_dexes: &[String],
    ) -> Vec<PoolEntry> {
        self.registry
            .lookup(input_mint, output_mint)
            .into_iter()
            .filter(|e| {
                // Skip pools we can't quote (non-CP and non-CLMM)
                if !is_quotable(e.pool_type) {
                    return false;
                }

                let label = label_for_pool_type(e.pool_type);

                // Apply dex whitelist
                if !dexes.is_empty() && !dexes.iter().any(|d| d == label) {
                    return false;
                }

                // Apply dex blacklist
                if exclude_dexes.iter().any(|d| d == label) {
                    return false;
                }

                true
            })
            .collect()
    }

    /// Build a QuoteResponse from a direct route.
    fn build_direct_response(
        &self,
        req: &QuoteRequest,
        route: &DirectRoute,
        context_slot: u64,
        elapsed: f64,
    ) -> QuoteResponse {
        let threshold = compute_threshold(route.out_amount, req.slippage_bps);
        let price_impact = compute_price_impact_for_type(
            route.pool_type,
            req.amount,
            route.out_amount,
            route.reserve_in,
            route.reserve_out,
        );
        let platform_fee = compute_platform_fee(route.out_amount, &req.output_mint);

        QuoteResponse {
            input_token: req.input_mint.to_string(),
            amount_in: req.amount.to_string(),
            output_token: req.output_mint.to_string(),
            amount_out: route.out_amount.to_string(),
            minimum_out: threshold.to_string(),
            mode: "ExactIn".to_string(),
            slippage_bps: req.slippage_bps,
            price_impact: price_impact,
            routes: vec![RouteStep {
                pool: PoolRoute {
                    pool_address: route.pool_address.to_string(),
                    dex: label_for_pool_type(route.pool_type).to_string(),
                    input_token: req.input_mint.to_string(),
                    output_token: req.output_mint.to_string(),
                    amount_in: req.amount.to_string(),
                    amount_out: route.out_amount.to_string(),
                    fee: route.fee_amount.to_string(),
                    fee_token: req.input_mint.to_string(),
                },
                percent: 100,
            }],
            slot: context_slot,
            quote_time_ms: elapsed,
            platform_fee: Some(platform_fee),
        }
    }

    /// Build a QuoteResponse from a 2-hop route.
    fn build_two_hop_response(
        &self,
        req: &QuoteRequest,
        route: &TwoHopRoute,
        context_slot: u64,
        elapsed: f64,
    ) -> QuoteResponse {
        let threshold = compute_threshold(route.final_amount_out, req.slippage_bps);

        // Price impact for multi-hop: use combined impact
        // Approximate: use the product of (1-impact) for each hop
        let impact1 = estimate_price_impact(
            route.hop1_reserve_in,
            route.hop1_reserve_out,
            req.amount,
            route.hop1_amount_out,
        );
        let impact2 = estimate_price_impact(
            route.hop2_reserve_in,
            route.hop2_reserve_out,
            route.hop1_amount_out,
            route.final_amount_out,
        );

        // Combine impacts: total_impact = 1 - (1 - impact1) * (1 - impact2)
        let i1: f64 = impact1.parse().unwrap_or(0.0);
        let i2: f64 = impact2.parse().unwrap_or(0.0);
        let combined = 1.0 - (1.0 - i1 / 100.0) * (1.0 - i2 / 100.0);
        let combined_pct = (combined * 100.0).max(0.0);
        let price_impact = format!("{:.2}", combined_pct);

        let platform_fee = compute_platform_fee(route.final_amount_out, &req.output_mint);

        QuoteResponse {
            input_token: req.input_mint.to_string(),
            amount_in: req.amount.to_string(),
            output_token: req.output_mint.to_string(),
            amount_out: route.final_amount_out.to_string(),
            minimum_out: threshold.to_string(),
            mode: "ExactIn".to_string(),
            slippage_bps: req.slippage_bps,
            price_impact: price_impact,
            routes: vec![
                RouteStep {
                    pool: PoolRoute {
                        pool_address: route.hop1_entry.address.to_string(),
                        dex: label_for_pool_type(route.hop1_entry.pool_type).to_string(),
                        input_token: req.input_mint.to_string(),
                        output_token: route.bridge_mint.to_string(),
                        amount_in: req.amount.to_string(),
                        amount_out: route.hop1_amount_out.to_string(),
                        fee: route.hop1_fee_amount.to_string(),
                        fee_token: req.input_mint.to_string(),
                    },
                    percent: 100,
                },
                RouteStep {
                    pool: PoolRoute {
                        pool_address: route.hop2_entry.address.to_string(),
                        dex: label_for_pool_type(route.hop2_entry.pool_type).to_string(),
                        input_token: route.bridge_mint.to_string(),
                        output_token: req.output_mint.to_string(),
                        amount_in: route.hop1_amount_out.to_string(),
                        amount_out: route.final_amount_out.to_string(),
                        fee: route.hop2_fee_amount.to_string(),
                        fee_token: route.bridge_mint.to_string(),
                    },
                    percent: 100,
                },
            ],
            slot: context_slot,
            quote_time_ms: elapsed,
            platform_fee: Some(platform_fee),
        }
    }

    /// Build a QuoteResponse from a 3-hop route.
    fn build_three_hop_response(
        &self,
        req: &QuoteRequest,
        route: &ThreeHopRoute,
        context_slot: u64,
        elapsed: f64,
    ) -> QuoteResponse {
        let threshold = compute_threshold(route.final_amount_out, req.slippage_bps);

        // Price impact for 3-hop: product of (1-impact) for each hop
        let impact1 = estimate_price_impact(
            route.hop1_reserve_in,
            route.hop1_reserve_out,
            req.amount,
            route.hop1_amount_out,
        );
        let impact2 = estimate_price_impact(
            route.hop2_reserve_in,
            route.hop2_reserve_out,
            route.hop1_amount_out,
            route.hop2_amount_out,
        );
        let impact3 = estimate_price_impact(
            route.hop3_reserve_in,
            route.hop3_reserve_out,
            route.hop2_amount_out,
            route.final_amount_out,
        );

        // Combine impacts: total = 1 - (1-i1)(1-i2)(1-i3)
        let i1: f64 = impact1.parse().unwrap_or(0.0);
        let i2: f64 = impact2.parse().unwrap_or(0.0);
        let i3: f64 = impact3.parse().unwrap_or(0.0);
        let combined = 1.0 - (1.0 - i1 / 100.0) * (1.0 - i2 / 100.0) * (1.0 - i3 / 100.0);
        let combined_pct = (combined * 100.0).max(0.0);
        let price_impact = format!("{:.2}", combined_pct);

        let platform_fee = compute_platform_fee(route.final_amount_out, &req.output_mint);

        QuoteResponse {
            input_token: req.input_mint.to_string(),
            amount_in: req.amount.to_string(),
            output_token: req.output_mint.to_string(),
            amount_out: route.final_amount_out.to_string(),
            minimum_out: threshold.to_string(),
            mode: "ExactIn".to_string(),
            slippage_bps: req.slippage_bps,
            price_impact,
            routes: vec![
                RouteStep {
                    pool: PoolRoute {
                        pool_address: route.hop1_entry.address.to_string(),
                        dex: label_for_pool_type(route.hop1_entry.pool_type).to_string(),
                        input_token: req.input_mint.to_string(),
                        output_token: route.bridge1_mint.to_string(),
                        amount_in: req.amount.to_string(),
                        amount_out: route.hop1_amount_out.to_string(),
                        fee: route.hop1_fee_amount.to_string(),
                        fee_token: req.input_mint.to_string(),
                    },
                    percent: 100,
                },
                RouteStep {
                    pool: PoolRoute {
                        pool_address: route.hop2_entry.address.to_string(),
                        dex: label_for_pool_type(route.hop2_entry.pool_type).to_string(),
                        input_token: route.bridge1_mint.to_string(),
                        output_token: route.bridge2_mint.to_string(),
                        amount_in: route.hop1_amount_out.to_string(),
                        amount_out: route.hop2_amount_out.to_string(),
                        fee: route.hop2_fee_amount.to_string(),
                        fee_token: route.bridge1_mint.to_string(),
                    },
                    percent: 100,
                },
                RouteStep {
                    pool: PoolRoute {
                        pool_address: route.hop3_entry.address.to_string(),
                        dex: label_for_pool_type(route.hop3_entry.pool_type).to_string(),
                        input_token: route.bridge2_mint.to_string(),
                        output_token: req.output_mint.to_string(),
                        amount_in: route.hop2_amount_out.to_string(),
                        amount_out: route.final_amount_out.to_string(),
                        fee: route.hop3_fee_amount.to_string(),
                        fee_token: route.bridge2_mint.to_string(),
                    },
                    percent: 100,
                },
            ],
            slot: context_slot,
            quote_time_ms: elapsed,
            platform_fee: Some(platform_fee),
        }
    }

    /// Build a QuoteResponse from a split route.
    fn build_split_response(
        &self,
        req: &QuoteRequest,
        split: &SplitRoute,
        context_slot: u64,
        elapsed: f64,
    ) -> QuoteResponse {
        let threshold = compute_threshold(split.total_out, req.slippage_bps);

        // Combined price impact from both legs (weighted)
        let impact_a = estimate_price_impact(
            split.pool_a.reserve_in,
            split.pool_a.reserve_out,
            split.amount_a,
            split.out_a,
        );
        let impact_b = estimate_price_impact(
            split.pool_b.reserve_in,
            split.pool_b.reserve_out,
            split.amount_b,
            split.out_b,
        );

        // Weighted average of the two impacts
        let ia: f64 = impact_a.parse().unwrap_or(0.0);
        let ib: f64 = impact_b.parse().unwrap_or(0.0);
        let weighted = (ia * split.pct_a as f64 + ib * split.pct_b as f64) / 100.0;
        let price_impact = format!("{:.2}", weighted);

        let platform_fee = compute_platform_fee(split.total_out, &req.output_mint);

        QuoteResponse {
            input_token: req.input_mint.to_string(),
            amount_in: req.amount.to_string(),
            output_token: req.output_mint.to_string(),
            amount_out: split.total_out.to_string(),
            minimum_out: threshold.to_string(),
            mode: "ExactIn".to_string(),
            slippage_bps: req.slippage_bps,
            price_impact: price_impact,
            routes: vec![
                RouteStep {
                    pool: PoolRoute {
                        pool_address: split.pool_a.pool_address.to_string(),
                        dex: label_for_pool_type(split.pool_a.pool_type).to_string(),
                        input_token: req.input_mint.to_string(),
                        output_token: req.output_mint.to_string(),
                        amount_in: split.amount_a.to_string(),
                        amount_out: split.out_a.to_string(),
                        fee: split.fee_a.to_string(),
                        fee_token: req.input_mint.to_string(),
                    },
                    percent: split.pct_a,
                },
                RouteStep {
                    pool: PoolRoute {
                        pool_address: split.pool_b.pool_address.to_string(),
                        dex: label_for_pool_type(split.pool_b.pool_type).to_string(),
                        input_token: req.input_mint.to_string(),
                        output_token: req.output_mint.to_string(),
                        amount_in: split.amount_b.to_string(),
                        amount_out: split.out_b.to_string(),
                        fee: split.fee_b.to_string(),
                        fee_token: req.input_mint.to_string(),
                    },
                    percent: split.pct_b,
                },
            ],
            slot: context_slot,
            quote_time_ms: elapsed,
            platform_fee: Some(platform_fee),
        }
    }
}

/// Evaluate split routes across pairs of direct-route pools.
///
/// When multiple pools exist for the same pair, splitting input across two pools
/// can yield more output because each pool absorbs less price impact.
///
/// Tries splits: 100/0, 90/10, 80/20, ..., 50/50 for each pool pair.
/// Returns the best split if it beats the best single-pool route, otherwise None.
#[cfg(test)]
fn evaluate_split_routes(
    direct_routes: &[DirectRoute],
    amount: u64,
) -> Option<SplitRoute> {
    if direct_routes.len() < 2 {
        return None;
    }

    // Best single-pool output
    let best_single = direct_routes.iter().map(|r| r.out_amount).max().unwrap_or(0);

    let mut best_split: Option<SplitRoute> = None;

    // Try all pairs of pools
    for (i, pool_a) in direct_routes.iter().enumerate() {
        for pool_b in direct_routes.iter().skip(i + 1) {
            // Try split percentages: 90/10, 80/20, ..., 50/50, 40/60, ..., 10/90
            for pct_a in (10u8..=90).step_by(10) {
                let pct_b = 100 - pct_a;
                let amount_a = (amount as u128 * pct_a as u128 / 100) as u64;
                let amount_b = amount.saturating_sub(amount_a);

                if amount_a == 0 || amount_b == 0 {
                    continue;
                }

                let fee_bps_a = fee_for_pool_type(pool_a.pool_type);
                let fee_bps_b = fee_for_pool_type(pool_b.pool_type);

                let out_a = match compute_constant_product_out(
                    pool_a.reserve_in,
                    pool_a.reserve_out,
                    amount_a,
                    fee_bps_a,
                ) {
                    Some(o) if o > 0 => o,
                    _ => continue,
                };
                let out_b = match compute_constant_product_out(
                    pool_b.reserve_in,
                    pool_b.reserve_out,
                    amount_b,
                    fee_bps_b,
                ) {
                    Some(o) if o > 0 => o,
                    _ => continue,
                };

                let total_out = out_a + out_b;

                // Only keep if it beats best single and current best split
                if total_out > best_single {
                    let current_best = best_split.as_ref().map(|s| s.total_out).unwrap_or(0);
                    if total_out > current_best {
                        best_split = Some(SplitRoute {
                            pool_a: pool_a.clone(),
                            pool_b: pool_b.clone(),
                            pct_a,
                            pct_b,
                            amount_a,
                            amount_b,
                            out_a,
                            out_b,
                            total_out,
                            fee_a: compute_fee_amount(amount_a, fee_bps_a),
                            fee_b: compute_fee_amount(amount_b, fee_bps_b),
                        });
                    }
                }
            }
        }
    }

    best_split
}

// ── Vault Balance Fetching ──

/// Fetch the token balance of an SPL token account.
async fn fetch_vault_balance(rpc: &RpcClient, vault: &Pubkey) -> Option<u64> {
    rpc.get_token_account_balance(vault)
        .await
        .ok()
        .and_then(|b| b.amount.parse::<u64>().ok())
}

/// Extract (vault_a, vault_b, mint_a, mint_b) from a PoolState for vault balance fetching.
/// Returns None for unsupported variants (PumpFun bonding, FlashTrade, CLMM, etc.)
fn extract_vault_mints(pool_state: &PoolState) -> Option<(Pubkey, Pubkey, Pubkey, Pubkey)> {
    match pool_state {
        PoolState::RaydiumCpmm {
            token_0_vault,
            token_1_vault,
            token_0_mint,
            token_1_mint,
            ..
        } => Some((*token_0_vault, *token_1_vault, *token_0_mint, *token_1_mint)),

        PoolState::RaydiumLp {
            base_vault,
            quote_vault,
            base_mint,
            quote_mint,
            ..
        } => Some((*base_vault, *quote_vault, *base_mint, *quote_mint)),

        PoolState::RaydiumV4 {
            coin_vault,
            pc_vault,
            ..
        } => {
            // RaydiumV4 doesn't store mints directly in PoolState.
            // We return the vaults, and use Pubkey::default() as placeholder mints.
            // The caller must use the registry's mint_a/mint_b for direction detection.
            // We return (coin_vault, pc_vault, default, default) — handled specially in fetch_reserves.
            Some((*coin_vault, *pc_vault, Pubkey::default(), Pubkey::default()))
        }

        PoolState::Meteora {
            a_token_vault,
            b_token_vault,
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*a_token_vault, *b_token_vault, *token_a_mint, *token_b_mint)),

        PoolState::MeteoraDamm {
            token_a_vault,
            token_b_vault,
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_vault, *token_b_vault, *token_a_mint, *token_b_mint)),

        PoolState::MeteoraDbc {
            base_vault,
            quote_vault,
            base_mint,
            quote_mint,
            ..
        } => Some((*base_vault, *quote_vault, *base_mint, *quote_mint)),

        PoolState::FluxBeam {
            token_a_vault,
            token_b_vault,
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_vault, *token_b_vault, *token_a_mint, *token_b_mint)),

        PoolState::Saros {
            token_a_vault,
            token_b_vault,
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_vault, *token_b_vault, *token_a_mint, *token_b_mint)),

        PoolState::Dooar {
            token_a_vault,
            token_b_vault,
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_vault, *token_b_vault, *token_a_mint, *token_b_mint)),

        // Pumpup has inline reserves — vault fetch not needed for quoting
        // (handled by extract_reserves_inline). We still expose vaults+mints
        // here so cold paths or 2-hop routing that bypasses inline can fall
        // back to vault RPC fetch.
        PoolState::Pumpup {
            token_a_vault,
            token_b_vault,
            token_a_mint,
            token_b_mint,
            ..
        } => Some((*token_a_vault, *token_b_vault, *token_a_mint, *token_b_mint)),

        // PumpFunAmm has inline reserves — no vault fetch needed (handled by extract_reserves_inline)
        // PumpFun bonding, FlashTrade, CLMM pools — not supported for vault balance fetching
        _ => None,
    }
}

/// Extract (reserve_in, reserve_out) from inline pool data (no RPC needed).
/// Only PumpFunAmm stores reserves directly in PoolState.
fn extract_reserves_inline(
    state: &PoolState,
    input_mint: &Pubkey,
) -> Option<(u128, u128)> {
    match state {
        PoolState::PumpFunAmm {
            base_reserve,
            quote_reserve,
            base_mint,
            quote_mint,
            ..
        } => {
            let (res_in, res_out) = if *input_mint == *base_mint {
                (*base_reserve as u128, *quote_reserve as u128)
            } else if *input_mint == *quote_mint {
                (*quote_reserve as u128, *base_reserve as u128)
            } else {
                return None;
            };
            Some((res_in, res_out))
        }
        PoolState::Pumpup {
            token_a_mint,
            token_b_mint,
            token_a_reserve,
            token_b_reserve,
            ..
        } => {
            let (res_in, res_out) = if *input_mint == *token_a_mint {
                (*token_a_reserve as u128, *token_b_reserve as u128)
            } else if *input_mint == *token_b_mint {
                (*token_b_reserve as u128, *token_a_reserve as u128)
            } else {
                return None;
            };
            Some((res_in, res_out))
        }
        // Pumpup pre-graduation bonding curve. SOL side uses
        // `virtual_sol + real_sol` to mirror the program's internal
        // constant-product math (matches PumpFun bonding pattern).
        PoolState::PumpupBonding {
            mint,
            virtual_sol,
            real_sol,
            pool_token_reserves,
            ..
        } => {
            use crate::constants::SOL_NATIVE_MINT;
            let sol_side = (*virtual_sol as u128).saturating_add(*real_sol as u128);
            let token_side = *pool_token_reserves as u128;
            let (res_in, res_out) = if *input_mint == SOL_NATIVE_MINT {
                (sol_side, token_side)
            } else if *input_mint == *mint {
                (token_side, sol_side)
            } else {
                return None;
            };
            Some((res_in, res_out))
        }
        _ => None,
    }
}

/// Fetch (reserve_in, reserve_out) for a pool by fetching vault balances.
/// For PumpFunAmm, returns inline reserves directly (no RPC).
/// Uses tokio::join! for parallel vault balance fetches.
async fn fetch_reserves(
    rpc: &RpcClient,
    pool_state: &PoolState,
    input_mint: &Pubkey,
) -> Option<(u128, u128)> {
    let (vault_a, vault_b, mint_a, mint_b) = extract_vault_mints(pool_state)?;

    let (bal_a, bal_b) = tokio::join!(
        fetch_vault_balance(rpc, &vault_a),
        fetch_vault_balance(rpc, &vault_b),
    );

    let (ra, rb) = (bal_a? as u128, bal_b? as u128);

    // For RaydiumV4 where mints are default (not stored in state),
    // we use vault order: coin_vault = vault_a (first), pc_vault = vault_b (second).
    // The caller's input_mint won't match Pubkey::default(), so we need to infer
    // direction from which vault the input lands in. However, without mints we
    // can't determine direction here — we return (ra, rb) and let the caller
    // use the registry mint_a/mint_b. For simplicity, treat vault_a as "A" side.
    // Direction detection for V4: not applicable since V4 doesn't have mints in state.
    // The registry mint_a corresponds to coin, mint_b to pc.
    if mint_a == Pubkey::default() && mint_b == Pubkey::default() {
        // RaydiumV4: can't determine direction from mints in state.
        // Return (ra, rb) as (reserve_a, reserve_b) — the caller needs to map.
        // For now, return in order and let the outer code figure direction.
        // Since we don't know which vault corresponds to which mint,
        // return None to skip RaydiumV4 vault-based quoting for now.
        // V4 pools are mostly closed (Serum) anyway.
        return None;
    }

    // Return in (input_reserve, output_reserve) order
    if *input_mint == mint_a {
        Some((ra, rb))
    } else {
        Some((rb, ra))
    }
}

/// Fee in basis points for each pool type.
/// These are the typical trading fees charged by each DEX protocol.
fn fee_for_pool_type(pool_type: PoolType) -> u16 {
    match pool_type {
        PoolType::RaydiumV4 => 25,      // 0.25%
        PoolType::RaydiumCpmm => 25,    // varies by config, default 0.25%
        PoolType::RaydiumLp => 25,      // 0.25%
        PoolType::PumpFunAmm => 25,     // ~0.25%
        PoolType::Meteora => 25,        // varies
        PoolType::MeteoraDamm => 25,    // varies
        PoolType::FluxBeam => 25,       // 0.25%
        PoolType::Saros => 25,          // 0.25%
        PoolType::Dooar => 25,          // 0.25%
        _ => DEFAULT_FEE_BPS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::{SOL_NATIVE_MINT, USDC_MINT};

    #[test]
    fn test_is_constant_product() {
        assert!(is_constant_product(PoolType::RaydiumV4));
        assert!(is_constant_product(PoolType::RaydiumCpmm));
        assert!(is_constant_product(PoolType::PumpFunAmm));
        assert!(is_constant_product(PoolType::Meteora));
        assert!(is_constant_product(PoolType::FluxBeam));
        assert!(is_constant_product(PoolType::Dooar));

        // CLMM pools are NOT constant product
        assert!(!is_constant_product(PoolType::Orca));
        assert!(!is_constant_product(PoolType::RaydiumCl));
        assert!(!is_constant_product(PoolType::MeteoraDlmm));
        assert!(!is_constant_product(PoolType::PancakeSwap));
    }

    #[test]
    fn test_label_for_pool_type() {
        assert_eq!(label_for_pool_type(PoolType::RaydiumCpmm), "Raydium CPMM");
        assert_eq!(label_for_pool_type(PoolType::Orca), "Orca");
        assert_eq!(label_for_pool_type(PoolType::PumpFunAmm), "PumpFun AMM");
        assert_eq!(label_for_pool_type(PoolType::MeteoraDlmm), "Meteora DLMM");
    }

    #[test]
    fn test_fee_for_pool_type() {
        assert_eq!(fee_for_pool_type(PoolType::RaydiumV4), 25);
        assert_eq!(fee_for_pool_type(PoolType::PumpFunAmm), 25);
        assert_eq!(fee_for_pool_type(PoolType::Orca), DEFAULT_FEE_BPS);
    }

    #[test]
    fn test_extract_reserves_inline_pumpfun_amm_forward() {
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let state = PoolState::PumpFunAmm {
            pool: Pubkey::new_unique(),
            base_mint,
            quote_mint,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 1_000_000,
            quote_reserve: 500_000,
        };

        let result = extract_reserves_inline(&state, &base_mint);
        assert_eq!(result, Some((1_000_000, 500_000)));
    }

    #[test]
    fn test_extract_reserves_inline_pumpfun_amm_reverse() {
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let state = PoolState::PumpFunAmm {
            pool: Pubkey::new_unique(),
            base_mint,
            quote_mint,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 1_000_000,
            quote_reserve: 500_000,
        };

        let result = extract_reserves_inline(&state, &quote_mint);
        assert_eq!(result, Some((500_000, 1_000_000)));
    }

    #[test]
    fn test_extract_reserves_inline_non_pumpfun_returns_none() {
        let state = PoolState::MeteoraDamm {
            pool: Pubkey::new_unique(),
            token_a_vault: Pubkey::new_unique(),
            token_b_vault: Pubkey::new_unique(),
            token_a_mint: Pubkey::new_unique(),
            token_b_mint: Pubkey::new_unique(),
        };
        let input_mint = Pubkey::new_unique();
        let result = extract_reserves_inline(&state, &input_mint);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_vault_mints_raydium_cpmm() {
        let vault0 = Pubkey::new_unique();
        let vault1 = Pubkey::new_unique();
        let mint0 = Pubkey::new_unique();
        let mint1 = Pubkey::new_unique();
        let state = PoolState::RaydiumCpmm {
            pool: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            config: Pubkey::new_unique(),
            token_0_vault: vault0,
            token_1_vault: vault1,
            token_0_mint: mint0,
            token_1_mint: mint1,
            observation: Pubkey::new_unique(),
        };

        let (va, vb, ma, mb) = extract_vault_mints(&state).unwrap();
        assert_eq!(va, vault0);
        assert_eq!(vb, vault1);
        assert_eq!(ma, mint0);
        assert_eq!(mb, mint1);
    }

    #[test]
    fn test_extract_vault_mints_raydium_lp() {
        let bv = Pubkey::new_unique();
        let qv = Pubkey::new_unique();
        let bm = Pubkey::new_unique();
        let qm = Pubkey::new_unique();
        let state = PoolState::RaydiumLp {
            pool_state: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            base_vault: bv,
            quote_vault: qv,
            base_mint: bm,
            quote_mint: qm,
            config_id: Pubkey::new_unique(),
            platform_id: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
        };

        let (va, vb, ma, mb) = extract_vault_mints(&state).unwrap();
        assert_eq!(va, bv);
        assert_eq!(vb, qv);
        assert_eq!(ma, bm);
        assert_eq!(mb, qm);
    }

    #[test]
    fn test_extract_vault_mints_meteora() {
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();
        let ma = Pubkey::new_unique();
        let mb = Pubkey::new_unique();
        let state = PoolState::Meteora {
            pool: Pubkey::new_unique(),
            token_a_mint: ma,
            token_b_mint: mb,
            a_vault: Pubkey::new_unique(),
            b_vault: Pubkey::new_unique(),
            a_token_vault: va,
            b_token_vault: vb,
            a_vault_lp_mint: Pubkey::new_unique(),
            b_vault_lp_mint: Pubkey::new_unique(),
            a_vault_lp: Pubkey::new_unique(),
            b_vault_lp: Pubkey::new_unique(),
            admin_token_a_fee: Pubkey::new_unique(),
            admin_token_b_fee: Pubkey::new_unique(),
            vault_program: Pubkey::new_unique(),
        };

        let (v_a, v_b, m_a, m_b) = extract_vault_mints(&state).unwrap();
        assert_eq!(v_a, va);
        assert_eq!(v_b, vb);
        assert_eq!(m_a, ma);
        assert_eq!(m_b, mb);
    }

    #[test]
    fn test_extract_vault_mints_meteora_damm() {
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();
        let ma = Pubkey::new_unique();
        let mb = Pubkey::new_unique();
        let state = PoolState::MeteoraDamm {
            pool: Pubkey::new_unique(),
            token_a_vault: va,
            token_b_vault: vb,
            token_a_mint: ma,
            token_b_mint: mb,
        };

        let (v_a, v_b, m_a, m_b) = extract_vault_mints(&state).unwrap();
        assert_eq!(v_a, va);
        assert_eq!(v_b, vb);
        assert_eq!(m_a, ma);
        assert_eq!(m_b, mb);
    }

    #[test]
    fn test_extract_vault_mints_meteora_dbc() {
        let bv = Pubkey::new_unique();
        let qv = Pubkey::new_unique();
        let bm = Pubkey::new_unique();
        let qm = Pubkey::new_unique();
        let state = PoolState::MeteoraDbc {
            pool: Pubkey::new_unique(),
            config: Pubkey::new_unique(),
            pool_authority: Pubkey::new_unique(),
            base_vault: bv,
            quote_vault: qv,
            base_mint: bm,
            quote_mint: qm,
        };

        let (va, vb, ma, mb) = extract_vault_mints(&state).unwrap();
        assert_eq!(va, bv);
        assert_eq!(vb, qv);
        assert_eq!(ma, bm);
        assert_eq!(mb, qm);
    }

    #[test]
    fn test_extract_vault_mints_fluxbeam() {
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();
        let ma = Pubkey::new_unique();
        let mb = Pubkey::new_unique();
        let state = PoolState::FluxBeam {
            pool: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            token_a_vault: va,
            token_b_vault: vb,
            pool_mint: Pubkey::new_unique(),
            fee_account: Pubkey::new_unique(),
            token_a_mint: ma,
            token_b_mint: mb,
            pool_token_program: Pubkey::new_unique(),
        };

        let (v_a, v_b, m_a, m_b) = extract_vault_mints(&state).unwrap();
        assert_eq!(v_a, va);
        assert_eq!(v_b, vb);
        assert_eq!(m_a, ma);
        assert_eq!(m_b, mb);
    }

    #[test]
    fn test_extract_vault_mints_saros() {
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();
        let ma = Pubkey::new_unique();
        let mb = Pubkey::new_unique();
        let state = PoolState::Saros {
            pool: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            token_a_vault: va,
            token_b_vault: vb,
            pool_mint: Pubkey::new_unique(),
            fee_account: Pubkey::new_unique(),
            token_a_mint: ma,
            token_b_mint: mb,
        };

        let (v_a, v_b, m_a, m_b) = extract_vault_mints(&state).unwrap();
        assert_eq!(v_a, va);
        assert_eq!(v_b, vb);
        assert_eq!(m_a, ma);
        assert_eq!(m_b, mb);
    }

    #[test]
    fn test_extract_vault_mints_dooar() {
        let va = Pubkey::new_unique();
        let vb = Pubkey::new_unique();
        let ma = Pubkey::new_unique();
        let mb = Pubkey::new_unique();
        let state = PoolState::Dooar {
            pool: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            token_a_vault: va,
            token_b_vault: vb,
            pool_mint: Pubkey::new_unique(),
            fee_account: Pubkey::new_unique(),
            token_a_mint: ma,
            token_b_mint: mb,
        };

        let (v_a, v_b, m_a, m_b) = extract_vault_mints(&state).unwrap();
        assert_eq!(v_a, va);
        assert_eq!(v_b, vb);
        assert_eq!(m_a, ma);
        assert_eq!(m_b, mb);
    }

    #[test]
    fn test_extract_vault_mints_raydium_v4() {
        let cv = Pubkey::new_unique();
        let pv = Pubkey::new_unique();
        let state = PoolState::RaydiumV4 {
            amm_id: Pubkey::new_unique(),
            authority: Pubkey::new_unique(),
            open_orders: Pubkey::new_unique(),
            target_orders: Pubkey::new_unique(),
            coin_vault: cv,
            pc_vault: pv,
            serum_program: Pubkey::new_unique(),
            serum_market: Pubkey::new_unique(),
            serum_bids: Pubkey::new_unique(),
            serum_asks: Pubkey::new_unique(),
            serum_event_queue: Pubkey::new_unique(),
            serum_coin_vault: Pubkey::new_unique(),
            serum_pc_vault: Pubkey::new_unique(),
            serum_vault_signer: Pubkey::new_unique(),
        };

        let result = extract_vault_mints(&state);
        assert!(result.is_some());
        let (va, vb, ma, mb) = result.unwrap();
        assert_eq!(va, cv);
        assert_eq!(vb, pv);
        // Mints are Pubkey::default() for V4 (not stored in state)
        assert_eq!(ma, Pubkey::default());
        assert_eq!(mb, Pubkey::default());
    }

    #[test]
    fn test_extract_vault_mints_unsupported_pumpfun() {
        let state = PoolState::PumpFun {
            global: Pubkey::new_unique(),
            fee_account: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            event_authority: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
        };
        assert!(extract_vault_mints(&state).is_none());
    }

    #[test]
    fn test_extract_vault_mints_unsupported_flash_trade() {
        let state = PoolState::FlashTrade {
            pool: Pubkey::new_unique(),
            oracle: Pubkey::new_unique(),
            custody: Pubkey::new_unique(),
            token_mint: Pubkey::new_unique(),
        };
        assert!(extract_vault_mints(&state).is_none());
    }

    #[test]
    fn test_extract_vault_mints_unsupported_orca_clmm() {
        let state = PoolState::Orca {
            whirlpool: Pubkey::new_unique(),
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            oracle: Pubkey::new_unique(),
            token_mint_a: Pubkey::new_unique(),
            token_mint_b: Pubkey::new_unique(),
            tick_current: 0,
            tick_spacing: 64,
            sqrt_price_x64: 0,
            liquidity: 0,
            fee_rate: 0,
        };
        assert!(extract_vault_mints(&state).is_none());
    }

    #[test]
    fn test_extract_vault_mints_unsupported_pumpfun_amm() {
        // PumpFunAmm uses inline reserves, not vault fetching
        let state = PoolState::PumpFunAmm {
            pool: Pubkey::new_unique(),
            base_mint: Pubkey::new_unique(),
            quote_mint: Pubkey::new_unique(),
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 1000,
            quote_reserve: 2000,
        };
        assert!(extract_vault_mints(&state).is_none());
    }

    #[test]
    fn test_route_candidate_sorting() {
        let mut candidates = vec![
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::RaydiumCpmm,
                out_amount: 100,
                fee_amount: 1,
                reserve_in: 1000,
                reserve_out: 1000,
            },
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::PumpFunAmm,
                out_amount: 200,
                fee_amount: 2,
                reserve_in: 2000,
                reserve_out: 2000,
            },
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::Meteora,
                out_amount: 150,
                fee_amount: 1,
                reserve_in: 1500,
                reserve_out: 1500,
            },
        ];

        candidates.sort_by(|a, b| b.out_amount.cmp(&a.out_amount));
        assert_eq!(candidates[0].out_amount, 200);
        assert_eq!(candidates[1].out_amount, 150);
        assert_eq!(candidates[2].out_amount, 100);
    }

    // ── Multi-hop routing unit tests ──

    #[test]
    fn test_bridge_mints_skip_input() {
        use crate::constants::{SOL_NATIVE_MINT, USDT_MINT, PYUSD_MINT};
        // When input_mint is SOL, SOL should be skipped as bridge
        let input_mint = SOL_NATIVE_MINT;
        let bridges: Vec<&Pubkey> = BRIDGE_MINTS
            .iter()
            .filter(|b| **b != input_mint)
            .collect();
        // SOL filtered out, USDC, USDT, and PYUSD remain
        assert_eq!(bridges.len(), 3);
        // USDT should be the second one
        assert_eq!(*bridges[1], USDT_MINT);
        // PYUSD should be the third one
        assert_eq!(*bridges[2], PYUSD_MINT);
    }

    #[test]
    fn test_bridge_mints_skip_output() {
        use crate::constants::USDT_MINT;
        let output_mint = USDT_MINT;
        let bridges: Vec<&Pubkey> = BRIDGE_MINTS
            .iter()
            .filter(|b| **b != output_mint)
            .collect();
        // USDT filtered out, SOL, USDC, and PYUSD remain
        assert_eq!(bridges.len(), 3);
    }

    #[test]
    fn test_two_hop_route_struct() {
        let h1 = PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        };
        let h2 = PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::Meteora,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        };
        let route = TwoHopRoute {
            hop1_entry: h1,
            hop2_entry: h2,
            bridge_mint: Pubkey::new_unique(),
            hop1_amount_out: 1000,
            hop1_fee_amount: 3,
            hop1_reserve_in: 100_000,
            hop1_reserve_out: 200_000,
            final_amount_out: 950,
            hop2_fee_amount: 3,
            hop2_reserve_in: 150_000,
            hop2_reserve_out: 300_000,
        };

        assert_eq!(route.final_amount_out, 950);
        assert_eq!(route.hop1_amount_out, 1000);
    }

    #[tokio::test]
    async fn test_quoter_no_route_error() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(registry, cache, rpc);

        let req = QuoteRequest {
            input_mint: Pubkey::new_unique(),
            output_mint: Pubkey::new_unique(),
            amount: 1000,
            slippage_bps: 50,
            only_direct_routes: true,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let result = quoter.quote(&req).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            TradeError::NoRoute { .. } => {}
            other => panic!("expected NoRoute, got: {other}"),
        }
    }

    #[test]
    fn test_filter_entries_dex_whitelist() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(Arc::clone(&registry), cache, rpc);

        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        registry.add(PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            mint_a,
            mint_b,
        });
        registry.add(PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::Meteora,
            mint_a,
            mint_b,
        });

        // Whitelist only Meteora
        let entries = quoter.filter_entries(
            &mint_a,
            &mint_b,
            &["Meteora".to_string()],
            &[],
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].pool_type, PoolType::Meteora);
    }

    #[test]
    fn test_filter_entries_dex_blacklist() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(Arc::clone(&registry), cache, rpc);

        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        registry.add(PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            mint_a,
            mint_b,
        });
        registry.add(PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::Meteora,
            mint_a,
            mint_b,
        });

        // Blacklist Meteora
        let entries = quoter.filter_entries(
            &mint_a,
            &mint_b,
            &[],
            &["Meteora".to_string()],
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].pool_type, PoolType::RaydiumCpmm);
    }

    #[test]
    fn test_filter_entries_includes_clmm() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(Arc::clone(&registry), cache, rpc);

        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        registry.add(PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::Orca, // CLMM — now quotable
            mint_a,
            mint_b,
        });
        registry.add(PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            mint_a,
            mint_b,
        });

        let entries = quoter.filter_entries(&mint_a, &mint_b, &[], &[]);
        assert_eq!(entries.len(), 2, "both CP and CLMM pools should be included");
        let types: Vec<PoolType> = entries.iter().map(|e| e.pool_type).collect();
        assert!(types.contains(&PoolType::Orca));
        assert!(types.contains(&PoolType::RaydiumCpmm));
    }

    #[test]
    fn test_filter_entries_skips_unsupported() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(Arc::clone(&registry), cache, rpc);

        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        // MeteoraDlmm is not supported for quoting
        registry.add(PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::MeteoraDlmm,
            mint_a,
            mint_b,
        });
        registry.add(PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            mint_a,
            mint_b,
        });

        let entries = quoter.filter_entries(&mint_a, &mint_b, &[], &[]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].pool_type, PoolType::RaydiumCpmm);
    }

    #[test]
    fn test_build_direct_response_structure() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(registry, cache, rpc);

        let input_mint = Pubkey::new_unique();
        let output_mint = Pubkey::new_unique();
        let req = QuoteRequest {
            input_mint,
            output_mint,
            amount: 1_000_000,
            slippage_bps: 50,
            only_direct_routes: true,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let route = DirectRoute {
            pool_address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            out_amount: 990_000,
            fee_amount: 2_500,
            reserve_in: 10_000_000,
            reserve_out: 10_000_000,
        };

        let resp = quoter.build_direct_response(&req, &route, 0, 0.01);
        assert_eq!(resp.routes.len(), 1);
        assert_eq!(resp.amount_out, "990000");
        assert_eq!(resp.routes[0].percent, 100);
        assert_eq!(resp.routes[0].pool.dex, "Raydium CPMM");
    }

    #[test]
    fn test_build_two_hop_response_structure() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(registry, cache, rpc);

        let input_mint = Pubkey::new_unique();
        let output_mint = Pubkey::new_unique();
        let bridge_mint = Pubkey::new_unique();
        let req = QuoteRequest {
            input_mint,
            output_mint,
            amount: 1_000_000,
            slippage_bps: 50,
            only_direct_routes: false,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let route = TwoHopRoute {
            hop1_entry: PoolEntry {
                address: Pubkey::new_unique(),
                pool_type: PoolType::RaydiumCpmm,
                mint_a: input_mint,
                mint_b: bridge_mint,
            },
            hop2_entry: PoolEntry {
                address: Pubkey::new_unique(),
                pool_type: PoolType::Meteora,
                mint_a: bridge_mint,
                mint_b: output_mint,
            },
            bridge_mint,
            hop1_amount_out: 500_000,
            hop1_fee_amount: 1_250,
            hop1_reserve_in: 10_000_000,
            hop1_reserve_out: 5_000_000,
            final_amount_out: 490_000,
            hop2_fee_amount: 1_250,
            hop2_reserve_in: 5_000_000,
            hop2_reserve_out: 10_000_000,
        };

        let resp = quoter.build_two_hop_response(&req, &route, 0, 0.01);
        assert_eq!(resp.routes.len(), 2);
        assert_eq!(resp.amount_out, "490000");

        // Hop 1: input -> bridge
        assert_eq!(resp.routes[0].pool.input_token, input_mint.to_string());
        assert_eq!(resp.routes[0].pool.output_token, bridge_mint.to_string());
        assert_eq!(resp.routes[0].pool.dex, "Raydium CPMM");

        // Hop 2: bridge -> output
        assert_eq!(resp.routes[1].pool.input_token, bridge_mint.to_string());
        assert_eq!(resp.routes[1].pool.output_token, output_mint.to_string());
        assert_eq!(resp.routes[1].pool.dex, "Meteora");
    }

    #[tokio::test]
    async fn test_quoter_direct_route_pumpfun_amm() {
        // PumpFunAmm has inline reserves, so we can test the full path
        // without real RPC vault balance fetches.
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(60_000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(Arc::clone(&registry), Arc::clone(&cache), Arc::clone(&rpc));

        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_addr = Pubkey::new_unique();

        // Register pool
        registry.add(PoolEntry {
            address: pool_addr,
            pool_type: PoolType::PumpFunAmm,
            mint_a: base_mint,
            mint_b: quote_mint,
        });

        // Pre-populate cache with pool state (inline reserves)
        cache.insert(pool_addr, PoolState::PumpFunAmm {
            pool: pool_addr,
            base_mint,
            quote_mint,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 10_000_000,
            quote_reserve: 5_000_000,
        });

        let req = QuoteRequest {
            input_mint: base_mint,
            output_mint: quote_mint,
            amount: 100_000,
            slippage_bps: 50,
            only_direct_routes: true,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let result = quoter.quote(&req).await;
        assert!(result.is_ok(), "quote should succeed with cached PumpFunAmm: {:?}", result.err());
        let resp = result.unwrap();
        assert_eq!(resp.routes.len(), 1);
        let out: u64 = resp.amount_out.parse().unwrap();
        assert!(out > 0, "out_amount should be > 0");
        assert!(out < 100_000, "out_amount should be less than input (different reserves)");
    }

    #[tokio::test]
    async fn test_quoter_two_hop_route_pumpfun_amm() {
        // Test 2-hop routing: TOKEN_A -> SOL -> TOKEN_B
        // using PumpFunAmm pools (inline reserves, no RPC needed)
        use crate::constants::SOL_NATIVE_MINT;

        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(60_000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(Arc::clone(&registry), Arc::clone(&cache), Arc::clone(&rpc));

        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();
        let pool1_addr = Pubkey::new_unique();
        let pool2_addr = Pubkey::new_unique();

        // Pool 1: TOKEN_A / SOL
        registry.add(PoolEntry {
            address: pool1_addr,
            pool_type: PoolType::PumpFunAmm,
            mint_a: token_a,
            mint_b: SOL_NATIVE_MINT,
        });
        cache.insert(pool1_addr, PoolState::PumpFunAmm {
            pool: pool1_addr,
            base_mint: token_a,
            quote_mint: SOL_NATIVE_MINT,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 10_000_000_000, // 10B token
            quote_reserve: 50_000_000_000, // 50 SOL (in lamports)
        });

        // Pool 2: TOKEN_B / SOL
        registry.add(PoolEntry {
            address: pool2_addr,
            pool_type: PoolType::PumpFunAmm,
            mint_a: token_b,
            mint_b: SOL_NATIVE_MINT,
        });
        cache.insert(pool2_addr, PoolState::PumpFunAmm {
            pool: pool2_addr,
            base_mint: token_b,
            quote_mint: SOL_NATIVE_MINT,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 20_000_000_000, // 20B token
            quote_reserve: 100_000_000_000, // 100 SOL (in lamports)
        });

        // Quote: TOKEN_A -> TOKEN_B (no direct pool, must route through SOL)
        let req = QuoteRequest {
            input_mint: token_a,
            output_mint: token_b,
            amount: 1_000_000, // 1M token_a
            slippage_bps: 50,
            only_direct_routes: false,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let result = quoter.quote(&req).await;
        assert!(result.is_ok(), "2-hop quote should succeed: {:?}", result.err());
        let resp = result.unwrap();

        // Should be a 2-hop route
        assert_eq!(resp.routes.len(), 2, "should be a 2-hop route");

        // Hop 1: TOKEN_A -> SOL
        assert_eq!(resp.routes[0].pool.input_token, token_a.to_string());
        assert_eq!(resp.routes[0].pool.output_token, SOL_NATIVE_MINT.to_string());

        // Hop 2: SOL -> TOKEN_B
        assert_eq!(resp.routes[1].pool.input_token, SOL_NATIVE_MINT.to_string());
        assert_eq!(resp.routes[1].pool.output_token, token_b.to_string());

        let out: u64 = resp.amount_out.parse().unwrap();
        assert!(out > 0, "final output should be > 0");
    }

    #[tokio::test]
    async fn test_quoter_only_direct_routes_skips_two_hop() {
        use crate::constants::SOL_NATIVE_MINT;

        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(60_000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(Arc::clone(&registry), Arc::clone(&cache), Arc::clone(&rpc));

        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();
        let pool1_addr = Pubkey::new_unique();
        let pool2_addr = Pubkey::new_unique();

        // Same pools as above but only_direct_routes = true
        registry.add(PoolEntry {
            address: pool1_addr,
            pool_type: PoolType::PumpFunAmm,
            mint_a: token_a,
            mint_b: SOL_NATIVE_MINT,
        });
        cache.insert(pool1_addr, PoolState::PumpFunAmm {
            pool: pool1_addr,
            base_mint: token_a,
            quote_mint: SOL_NATIVE_MINT,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 10_000_000_000,
            quote_reserve: 50_000_000_000,
        });

        registry.add(PoolEntry {
            address: pool2_addr,
            pool_type: PoolType::PumpFunAmm,
            mint_a: token_b,
            mint_b: SOL_NATIVE_MINT,
        });
        cache.insert(pool2_addr, PoolState::PumpFunAmm {
            pool: pool2_addr,
            base_mint: token_b,
            quote_mint: SOL_NATIVE_MINT,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 20_000_000_000,
            quote_reserve: 100_000_000_000,
        });

        let req = QuoteRequest {
            input_mint: token_a,
            output_mint: token_b,
            amount: 1_000_000,
            slippage_bps: 50,
            only_direct_routes: true, // This should skip 2-hop
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let result = quoter.quote(&req).await;
        // Should get NoRoute since there's no direct TOKEN_A -> TOKEN_B pool
        assert!(result.is_err());
        match result.unwrap_err() {
            TradeError::NoRoute { .. } => {}
            other => panic!("expected NoRoute, got: {other}"),
        }
    }

    #[tokio::test]
    async fn test_quoter_direct_beats_two_hop() {
        use crate::constants::SOL_NATIVE_MINT;

        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(60_000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(Arc::clone(&registry), Arc::clone(&cache), Arc::clone(&rpc));

        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();

        // Direct pool: TOKEN_A / TOKEN_B with excellent reserves
        let direct_pool = Pubkey::new_unique();
        registry.add(PoolEntry {
            address: direct_pool,
            pool_type: PoolType::PumpFunAmm,
            mint_a: token_a,
            mint_b: token_b,
        });
        cache.insert(direct_pool, PoolState::PumpFunAmm {
            pool: direct_pool,
            base_mint: token_a,
            quote_mint: token_b,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 1_000_000_000,
            quote_reserve: 1_000_000_000,
        });

        // Indirect pools: TOKEN_A / SOL and TOKEN_B / SOL (worse total output due to double fees)
        let pool1 = Pubkey::new_unique();
        registry.add(PoolEntry {
            address: pool1,
            pool_type: PoolType::PumpFunAmm,
            mint_a: token_a,
            mint_b: SOL_NATIVE_MINT,
        });
        cache.insert(pool1, PoolState::PumpFunAmm {
            pool: pool1,
            base_mint: token_a,
            quote_mint: SOL_NATIVE_MINT,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 100_000_000,
            quote_reserve: 100_000_000,
        });

        let pool2 = Pubkey::new_unique();
        registry.add(PoolEntry {
            address: pool2,
            pool_type: PoolType::PumpFunAmm,
            mint_a: token_b,
            mint_b: SOL_NATIVE_MINT,
        });
        cache.insert(pool2, PoolState::PumpFunAmm {
            pool: pool2,
            base_mint: token_b,
            quote_mint: SOL_NATIVE_MINT,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            coin_creator: Pubkey::new_unique(),
            base_reserve: 100_000_000,
            quote_reserve: 100_000_000,
        });

        let req = QuoteRequest {
            input_mint: token_a,
            output_mint: token_b,
            amount: 1_000,
            slippage_bps: 50,
            only_direct_routes: false,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let result = quoter.quote(&req).await;
        assert!(result.is_ok());
        let resp = result.unwrap();

        // Direct route should win (1 hop, better for small amounts with large reserves)
        assert_eq!(resp.routes.len(), 1, "direct route should be preferred");
    }

    // ── Split Route Tests ──

    #[test]
    fn test_split_no_routes_returns_none() {
        let routes: Vec<DirectRoute> = Vec::new();
        assert!(evaluate_split_routes(&routes, 1000).is_none());
    }

    #[test]
    fn test_split_single_route_returns_none() {
        let routes = vec![DirectRoute {
            pool_address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            out_amount: 9000,
            fee_amount: 25,
            reserve_in: 1_000_000,
            reserve_out: 1_000_000,
        }];
        assert!(evaluate_split_routes(&routes, 10000).is_none());
    }

    #[test]
    fn test_split_large_trade_benefits_from_split() {
        // Two equal pools with 1M reserves each.
        // A large trade (100K = 10% of reserves) will have high impact in one pool.
        // Splitting across both pools should give more output.
        let pool_a = DirectRoute {
            pool_address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            out_amount: 0, // will be recomputed
            fee_amount: 0,
            reserve_in: 1_000_000,
            reserve_out: 1_000_000,
        };
        let pool_b = DirectRoute {
            pool_address: Pubkey::new_unique(),
            pool_type: PoolType::Orca,
            out_amount: 0,
            fee_amount: 0,
            reserve_in: 1_000_000,
            reserve_out: 1_000_000,
        };

        // Compute single-pool outputs first
        let amount = 100_000u64;
        let single_a = compute_constant_product_out(1_000_000, 1_000_000, amount, 25).unwrap();
        let single_b = compute_constant_product_out(1_000_000, 1_000_000, amount, 25).unwrap();
        let best_single = single_a.max(single_b);

        let routes = vec![
            DirectRoute { out_amount: single_a, fee_amount: compute_fee_amount(amount, 25), ..pool_a },
            DirectRoute { out_amount: single_b, fee_amount: compute_fee_amount(amount, 25), ..pool_b },
        ];

        let split = evaluate_split_routes(&routes, amount);
        assert!(split.is_some(), "split should be found for large trade");
        let split = split.unwrap();
        assert!(
            split.total_out > best_single,
            "split output {} should beat single pool {}",
            split.total_out,
            best_single
        );
        assert_eq!(split.pct_a + split.pct_b, 100);
    }

    #[test]
    fn test_split_small_trade_no_benefit() {
        // Two pools with huge reserves. A tiny trade has negligible impact.
        // Splitting provides no benefit (and evaluate_split_routes returns None).
        let amount = 100u64;
        let reserve = 1_000_000_000u128; // huge reserves

        let single_out = compute_constant_product_out(reserve, reserve, amount, 25).unwrap();
        let routes = vec![
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::RaydiumCpmm,
                out_amount: single_out,
                fee_amount: compute_fee_amount(amount, 25),
                reserve_in: reserve,
                reserve_out: reserve,
            },
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::Orca,
                out_amount: single_out,
                fee_amount: compute_fee_amount(amount, 25),
                reserve_in: reserve,
                reserve_out: reserve,
            },
        ];

        // For very small amounts relative to reserves, split should not beat single
        let split = evaluate_split_routes(&routes, amount);
        // With equal reserves, 50/50 split output = same as single pool (no improvement)
        assert!(split.is_none(), "tiny trade should not benefit from split");
    }

    #[test]
    fn test_split_50_50_not_always_best() {
        // Two pools with different reserve sizes.
        // 50/50 split is not optimal when pools are unequal.
        let amount = 100_000u64;

        // Pool A: large reserves (less impact)
        let large_reserve = 10_000_000u128;
        let single_a = compute_constant_product_out(large_reserve, large_reserve, amount, 25).unwrap();

        // Pool B: small reserves (more impact)
        let small_reserve = 500_000u128;
        let single_b = compute_constant_product_out(small_reserve, small_reserve, amount, 25).unwrap();

        let routes = vec![
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::RaydiumCpmm,
                out_amount: single_a,
                fee_amount: compute_fee_amount(amount, 25),
                reserve_in: large_reserve,
                reserve_out: large_reserve,
            },
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::Meteora,
                out_amount: single_b,
                fee_amount: compute_fee_amount(amount, 25),
                reserve_in: small_reserve,
                reserve_out: small_reserve,
            },
        ];

        let split = evaluate_split_routes(&routes, amount);
        if let Some(ref s) = split {
            // For unequal pools, the bigger pool should get more volume
            assert!(s.pct_a > s.pct_b || s.pct_b > s.pct_a, "unequal pools should have unequal split");
        }
        // Whether or not split is found, the best single (pool A) should be good
        assert!(single_a > single_b, "large pool should give better single output");
    }

    #[test]
    fn test_split_percents_sum_to_100() {
        let amount = 50_000u64;
        let reserve = 500_000u128;
        let single_out = compute_constant_product_out(reserve, reserve, amount, 25).unwrap();

        let routes = vec![
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::RaydiumCpmm,
                out_amount: single_out,
                fee_amount: compute_fee_amount(amount, 25),
                reserve_in: reserve,
                reserve_out: reserve,
            },
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::Meteora,
                out_amount: single_out,
                fee_amount: compute_fee_amount(amount, 25),
                reserve_in: reserve,
                reserve_out: reserve,
            },
        ];

        if let Some(split) = evaluate_split_routes(&routes, amount) {
            assert_eq!(split.pct_a + split.pct_b, 100, "split percentages must sum to 100");
            assert_eq!(split.amount_a + split.amount_b, amount, "split amounts must sum to total");
            assert!(split.total_out > 0, "total output must be positive");
        }
    }

    #[test]
    fn test_split_route_amounts_are_correct() {
        // Verify that split amounts are recomputed correctly (not just reusing pool_a/pool_b out_amount)
        let amount = 100_000u64;
        let reserve = 1_000_000u128;
        let single_out = compute_constant_product_out(reserve, reserve, amount, 25).unwrap();

        let routes = vec![
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::RaydiumCpmm,
                out_amount: single_out,
                fee_amount: compute_fee_amount(amount, 25),
                reserve_in: reserve,
                reserve_out: reserve,
            },
            DirectRoute {
                pool_address: Pubkey::new_unique(),
                pool_type: PoolType::Meteora,
                out_amount: single_out,
                fee_amount: compute_fee_amount(amount, 25),
                reserve_in: reserve,
                reserve_out: reserve,
            },
        ];

        if let Some(split) = evaluate_split_routes(&routes, amount) {
            // Verify out_a and out_b are individually correct
            let check_a = compute_constant_product_out(reserve, reserve, split.amount_a, 25).unwrap();
            let check_b = compute_constant_product_out(reserve, reserve, split.amount_b, 25).unwrap();
            assert_eq!(split.out_a, check_a, "out_a should match recomputed value");
            assert_eq!(split.out_b, check_b, "out_b should match recomputed value");
            assert_eq!(split.total_out, check_a + check_b, "total should be sum of parts");
        }
    }

    // ── Three-Hop Route Tests ──

    #[test]
    fn test_three_hop_route_struct() {
        let h1 = PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        };
        let h2 = PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::Meteora,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        };
        let h3 = PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::PumpFunAmm,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        };
        let bridge1 = Pubkey::new_unique();
        let bridge2 = Pubkey::new_unique();

        let route = ThreeHopRoute {
            hop1_entry: h1,
            hop2_entry: h2,
            hop3_entry: h3,
            bridge1_mint: bridge1,
            bridge2_mint: bridge2,
            hop1_amount_out: 5000,
            hop1_fee_amount: 13,
            hop1_reserve_in: 100_000,
            hop1_reserve_out: 200_000,
            hop2_amount_out: 4800,
            hop2_fee_amount: 12,
            hop2_reserve_in: 150_000,
            hop2_reserve_out: 250_000,
            final_amount_out: 4600,
            hop3_fee_amount: 12,
            hop3_reserve_in: 200_000,
            hop3_reserve_out: 300_000,
        };

        assert_eq!(route.hop1_amount_out, 5000);
        assert_eq!(route.hop2_amount_out, 4800);
        assert_eq!(route.final_amount_out, 4600);
        assert_eq!(route.bridge1_mint, bridge1);
        assert_eq!(route.bridge2_mint, bridge2);
        assert_eq!(route.hop1_entry.pool_type, PoolType::RaydiumCpmm);
        assert_eq!(route.hop2_entry.pool_type, PoolType::Meteora);
        assert_eq!(route.hop3_entry.pool_type, PoolType::PumpFunAmm);
    }

    #[test]
    fn test_three_hop_bridge_pair_filtering() {
        use crate::constants::{SOL_NATIVE_MINT, USDC_MINT, USDT_MINT, PYUSD_MINT};

        let input_mint = SOL_NATIVE_MINT;
        let output_mint = USDC_MINT;

        // Collect valid (bridge1, bridge2) pairs
        let mut pairs: Vec<(Pubkey, Pubkey)> = Vec::new();
        for bridge1 in &BRIDGE_MINTS {
            if *bridge1 == input_mint || *bridge1 == output_mint {
                continue;
            }
            for bridge2 in &BRIDGE_MINTS {
                if *bridge2 == *bridge1 || *bridge2 == input_mint || *bridge2 == output_mint {
                    continue;
                }
                pairs.push((*bridge1, *bridge2));
            }
        }

        // With input=SOL and output=USDC, valid bridges are USDT and PYUSD
        // Valid pairs: (USDT, PYUSD), (PYUSD, USDT)
        assert_eq!(pairs.len(), 2);
        assert!(pairs.contains(&(USDT_MINT, PYUSD_MINT)));
        assert!(pairs.contains(&(PYUSD_MINT, USDT_MINT)));

        // Verify no pair has bridge == input or output
        for (b1, b2) in &pairs {
            assert_ne!(*b1, input_mint, "bridge1 must not equal input");
            assert_ne!(*b1, output_mint, "bridge1 must not equal output");
            assert_ne!(*b2, input_mint, "bridge2 must not equal input");
            assert_ne!(*b2, output_mint, "bridge2 must not equal output");
            assert_ne!(*b1, *b2, "bridge1 must not equal bridge2");
        }
    }

    #[test]
    fn test_build_three_hop_response_structure() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(registry, cache, rpc);

        let input_mint = Pubkey::new_unique();
        let output_mint = Pubkey::new_unique();
        let bridge1_mint = Pubkey::new_unique();
        let bridge2_mint = Pubkey::new_unique();
        let req = QuoteRequest {
            input_mint,
            output_mint,
            amount: 1_000_000,
            slippage_bps: 50,
            only_direct_routes: false,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let route = ThreeHopRoute {
            hop1_entry: PoolEntry {
                address: Pubkey::new_unique(),
                pool_type: PoolType::RaydiumCpmm,
                mint_a: input_mint,
                mint_b: bridge1_mint,
            },
            hop2_entry: PoolEntry {
                address: Pubkey::new_unique(),
                pool_type: PoolType::Meteora,
                mint_a: bridge1_mint,
                mint_b: bridge2_mint,
            },
            hop3_entry: PoolEntry {
                address: Pubkey::new_unique(),
                pool_type: PoolType::PumpFunAmm,
                mint_a: bridge2_mint,
                mint_b: output_mint,
            },
            bridge1_mint,
            bridge2_mint,
            hop1_amount_out: 500_000,
            hop1_fee_amount: 1_250,
            hop1_reserve_in: 10_000_000,
            hop1_reserve_out: 5_000_000,
            hop2_amount_out: 480_000,
            hop2_fee_amount: 1_200,
            hop2_reserve_in: 5_000_000,
            hop2_reserve_out: 5_000_000,
            final_amount_out: 460_000,
            hop3_fee_amount: 1_200,
            hop3_reserve_in: 5_000_000,
            hop3_reserve_out: 10_000_000,
        };

        let resp = quoter.build_three_hop_response(&req, &route, 0, 0.01);

        // Must have exactly 3 route steps
        assert_eq!(resp.routes.len(), 3);
        assert_eq!(resp.amount_out, "460000");

        // Hop 1: input -> bridge1
        assert_eq!(resp.routes[0].pool.input_token, input_mint.to_string());
        assert_eq!(resp.routes[0].pool.output_token, bridge1_mint.to_string());
        assert_eq!(resp.routes[0].pool.dex, "Raydium CPMM");
        assert_eq!(resp.routes[0].pool.amount_in, "1000000");
        assert_eq!(resp.routes[0].pool.amount_out, "500000");
        assert_eq!(resp.routes[0].percent, 100);

        // Hop 2: bridge1 -> bridge2
        assert_eq!(resp.routes[1].pool.input_token, bridge1_mint.to_string());
        assert_eq!(resp.routes[1].pool.output_token, bridge2_mint.to_string());
        assert_eq!(resp.routes[1].pool.dex, "Meteora");
        assert_eq!(resp.routes[1].pool.amount_in, "500000");
        assert_eq!(resp.routes[1].pool.amount_out, "480000");
        assert_eq!(resp.routes[1].percent, 100);

        // Hop 3: bridge2 -> output
        assert_eq!(resp.routes[2].pool.input_token, bridge2_mint.to_string());
        assert_eq!(resp.routes[2].pool.output_token, output_mint.to_string());
        assert_eq!(resp.routes[2].pool.dex, "PumpFun AMM");
        assert_eq!(resp.routes[2].pool.amount_in, "480000");
        assert_eq!(resp.routes[2].pool.amount_out, "460000");
        assert_eq!(resp.routes[2].percent, 100);

        // Price impact should be a parseable positive number
        let pi: f64 = resp.price_impact.parse().unwrap();
        assert!(pi >= 0.0, "price impact must be non-negative");
    }

    // ── Platform Fee Tests (always output side) ──

    #[test]
    fn test_compute_platform_fee_sol_output() {
        let output = SOL_NATIVE_MINT;
        let pf = compute_platform_fee(1_000_000_000, &output);
        // 1B * 50 / 10000 = 5_000_000
        assert_eq!(pf.amount, "5000000");
        assert_eq!(pf.fee_bps, 50);
        assert_eq!(pf.fee_token, SOL_NATIVE_MINT.to_string());
        assert_eq!(pf.side, "output");
    }

    #[test]
    fn test_compute_platform_fee_usdc_output() {
        let output = USDC_MINT;
        let pf = compute_platform_fee(100_000_000, &output);
        // 100M * 50 / 10000 = 500_000
        assert_eq!(pf.amount, "500000");
        assert_eq!(pf.fee_token, USDC_MINT.to_string());
        assert_eq!(pf.side, "output");
    }

    #[test]
    fn test_compute_platform_fee_random_token_output() {
        let output = Pubkey::new_unique();
        let pf = compute_platform_fee(5_000_000, &output);
        // 5M * 50 / 10000 = 25_000
        assert_eq!(pf.amount, "25000");
        assert_eq!(pf.fee_token, output.to_string());
        assert_eq!(pf.side, "output");
    }

    #[test]
    fn test_compute_platform_fee_zero_amount() {
        let output = USDC_MINT;
        let pf = compute_platform_fee(0, &output);
        assert_eq!(pf.amount, "0");
        assert_eq!(pf.fee_token, USDC_MINT.to_string());
    }

    #[test]
    fn test_compute_platform_fee_small_amount_rounds_down() {
        let output = USDC_MINT;
        // 99 * 50 / 10000 = 0 (rounds down)
        let pf = compute_platform_fee(99, &output);
        assert_eq!(pf.amount, "0");
    }

    #[test]
    fn test_compute_platform_fee_exact_boundary() {
        let output = USDC_MINT;
        // 200 * 50 / 10000 = 1 (exact)
        let pf = compute_platform_fee(200, &output);
        assert_eq!(pf.amount, "1");
    }

    #[test]
    fn test_compute_platform_fee_large_amount_no_overflow() {
        let output = USDC_MINT;
        // u64::MAX * 50 / 10000 — should not overflow due to u128 intermediate
        let pf = compute_platform_fee(u64::MAX, &output);
        let expected = (u64::MAX as u128 * 50 / 10_000) as u64;
        assert_eq!(pf.amount, expected.to_string());
    }

    #[test]
    fn test_platform_fee_bps_is_constant() {
        let pf = compute_platform_fee(1000, &SOL_NATIVE_MINT);
        assert_eq!(pf.fee_bps, 50);
        assert_eq!(pf.fee_bps, PLATFORM_FEE_BPS);
    }

    // ── Slippage enforcement: minimum_out in build_direct_response ──

    #[test]
    fn test_direct_response_minimum_out_matches_threshold() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(registry, cache, rpc);

        let input_mint = Pubkey::new_unique();
        let output_mint = Pubkey::new_unique();
        let req = QuoteRequest {
            input_mint,
            output_mint,
            amount: 1_000_000,
            slippage_bps: 100, // 1%
            only_direct_routes: true,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let route = DirectRoute {
            pool_address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            out_amount: 500_000,
            fee_amount: 2_500,
            reserve_in: 10_000_000,
            reserve_out: 10_000_000,
        };

        let resp = quoter.build_direct_response(&req, &route, 0, 0.01);

        let expected_threshold = compute_threshold(500_000, 100);
        assert_eq!(expected_threshold, 495_000); // 500000 * 9900 / 10000
        assert_eq!(resp.minimum_out, expected_threshold.to_string());
    }

    #[test]
    fn test_direct_response_zero_slippage_minimum_equals_output() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(registry, cache, rpc);

        let req = QuoteRequest {
            input_mint: Pubkey::new_unique(),
            output_mint: Pubkey::new_unique(),
            amount: 1_000_000,
            slippage_bps: 0, // zero slippage
            only_direct_routes: true,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let route = DirectRoute {
            pool_address: Pubkey::new_unique(),
            pool_type: PoolType::Meteora,
            out_amount: 750_000,
            fee_amount: 1_875,
            reserve_in: 5_000_000,
            reserve_out: 5_000_000,
        };

        let resp = quoter.build_direct_response(&req, &route, 0, 0.01);

        // With zero slippage, minimum_out should equal amount_out
        assert_eq!(resp.minimum_out, resp.amount_out);
        assert_eq!(resp.minimum_out, "750000");
    }

    #[test]
    fn test_direct_response_high_slippage_minimum_near_zero() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(registry, cache, rpc);

        let req = QuoteRequest {
            input_mint: Pubkey::new_unique(),
            output_mint: Pubkey::new_unique(),
            amount: 1_000_000,
            slippage_bps: 9999, // 99.99% slippage
            only_direct_routes: true,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let route = DirectRoute {
            pool_address: Pubkey::new_unique(),
            pool_type: PoolType::PumpFunAmm,
            out_amount: 1_000_000,
            fee_amount: 2_500,
            reserve_in: 10_000_000,
            reserve_out: 10_000_000,
        };

        let resp = quoter.build_direct_response(&req, &route, 0, 0.01);

        // 1_000_000 * (10000 - 9999) / 10000 = 1_000_000 * 1 / 10000 = 100
        let expected = compute_threshold(1_000_000, 9999);
        assert_eq!(expected, 100);
        assert_eq!(resp.minimum_out, "100");
    }

    #[test]
    fn test_two_hop_response_minimum_out_matches_threshold() {
        let registry = Arc::new(PoolRegistry::new());
        let cache = Arc::new(crate::pool::cache::PoolCache::new(2000));
        let rpc = Arc::new(RpcClient::new("http://localhost:8899".to_string()));
        let quoter = Quoter::new(registry, cache, rpc);

        let input_mint = Pubkey::new_unique();
        let output_mint = Pubkey::new_unique();
        let bridge_mint = Pubkey::new_unique();
        let req = QuoteRequest {
            input_mint,
            output_mint,
            amount: 1_000_000,
            slippage_bps: 200, // 2%
            only_direct_routes: false,
            exclude_dexes: vec![],
            dexes: vec![],
            max_accounts: 64,
        };

        let route = TwoHopRoute {
            hop1_entry: PoolEntry {
                address: Pubkey::new_unique(),
                pool_type: PoolType::RaydiumCpmm,
                mint_a: input_mint,
                mint_b: bridge_mint,
            },
            hop2_entry: PoolEntry {
                address: Pubkey::new_unique(),
                pool_type: PoolType::Meteora,
                mint_a: bridge_mint,
                mint_b: output_mint,
            },
            bridge_mint,
            hop1_amount_out: 500_000,
            hop1_fee_amount: 1_250,
            hop1_reserve_in: 10_000_000,
            hop1_reserve_out: 5_000_000,
            final_amount_out: 490_000,
            hop2_fee_amount: 1_250,
            hop2_reserve_in: 5_000_000,
            hop2_reserve_out: 10_000_000,
        };

        let resp = quoter.build_two_hop_response(&req, &route, 0, 0.01);

        // threshold = 490000 * (10000 - 200) / 10000 = 490000 * 9800 / 10000 = 480200
        let expected_threshold = compute_threshold(490_000, 200);
        assert_eq!(expected_threshold, 480_200);
        assert_eq!(resp.minimum_out, expected_threshold.to_string());
    }
}
