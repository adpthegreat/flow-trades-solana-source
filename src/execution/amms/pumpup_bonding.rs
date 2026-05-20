//! Pumpup pre-graduation bonding-curve executor.
//!
//! Builds the `buy` and `sell` instructions for the Pumpup bonding curve
//! (program `PdMDrKEMaX8q7CCJb7NvUCxerBCcsFUa4LjBEynTtEd`). Source-of-truth:
//! on-chain Anchor IDL fetched from `BzBmXJiz9H88PAZomvWn8UvmdmeucWZg7N1cygN5po61`.
//!
//! # Native SOL
//! Pre-graduation bonding curves trade against **native SOL** via the System
//! program (`pool_sol_account` is itself the lamport holder). There is **no
//! WSOL wrap/unwrap**, mirroring the PumpFun bonding pattern.
//!
//! # Instruction layout (per IDL)
//! ## buy(token_amount: u64, max_sol_amount: u64) — disc `66063d1201daebea`
//! ## sell(token_amount: u64, min_sol_amount: u64) — disc `33e685a4017f83ad`
//!
//! Accounts (13, identical for buy and sell, IDL order):
//!   [0]  config                       PDA(["pumpup.config"]) — readonly
//!   [1]  mint                         readonly
//!   [2]  pool_token_account           writable, ATA(pool_sol_account, mint, TOKEN_PROG)
//!   [3]  pool_sol_account             writable, PDA(["pumpup.pool", mint])
//!   [4]  ai_token_account             writable, ATA(config, mint, TOKEN_PROG)
//!   [5]  pumpup_fee                   writable, fee recipient from PumpupConfiguration
//!   [6]  user_token_account           writable, ATA(user, mint, TOKEN_PROG)
//!   [7]  user                         writable, signer
//!   [8]  system_program               readonly = 11111111111111111111111111111111
//!   [9]  token_program                readonly = TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
//!   [10] associated_token_program     readonly = ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL
//!   [11] event_authority              readonly = PDA(["__event_authority"])
//!   [12] program                      readonly = Pumpup program itself

use std::sync::LazyLock;

use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_program;
use spl_associated_token_account::ID as ASSOCIATED_TOKEN_PROGRAM_ID;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;

use crate::constants::*;
use crate::error::{TradeError, TradeResult};
use crate::pool::types::{PoolState, SwapInstructions, SwapOrder};
use super::AmmExecutor;

/// `sha256("global:buy")[0..8]`
pub(crate) const PUMPUP_BUY_DISC: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
/// `sha256("global:sell")[0..8]`
pub(crate) const PUMPUP_SELL_DISC: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

/// Cached event authority PDA — same for all Pumpup pools.
static PUMPUP_EVENT_AUTHORITY: LazyLock<Pubkey> = LazyLock::new(|| {
    Pubkey::find_program_address(&[b"__event_authority"], &PUMPUP_PROG_ID).0
});

/// Cached `["pumpup.config"]` PDA — same for every bonding curve.
static PUMPUP_CONFIG_PDA: LazyLock<Pubkey> = LazyLock::new(|| {
    Pubkey::find_program_address(&[b"pumpup.config"], &PUMPUP_PROG_ID).0
});

pub struct PumpupBondingExecutor;

impl AmmExecutor for PumpupBondingExecutor {
    fn build_swap_ix(
        &self,
        order: &SwapOrder,
        pool_state: &PoolState,
    ) -> TradeResult<SwapInstructions> {
        let (pool_sol_account, mint, pool_token_account, pumpup_fee,
             virtual_sol, real_sol, pool_token_reserves) = match pool_state {
            PoolState::PumpupBonding {
                pool, mint, pool_token_account, pumpup_fee,
                virtual_sol, real_sol, pool_token_reserves, ..
            } => (*pool, *mint, *pool_token_account, *pumpup_fee,
                  *virtual_sol, *real_sol, *pool_token_reserves),
            _ => return Err(TradeError::Execution("expected PumpupBonding pool state".into())),
        };

        // Direction: buy = SOL → token, sell = token → SOL.
        let is_buy = order.input_mint == SOL_NATIVE_MINT && order.output_mint == mint;
        let is_sell = order.input_mint == mint && order.output_mint == SOL_NATIVE_MINT;
        if !is_buy && !is_sell {
            return Err(TradeError::Execution(format!(
                "Pumpup bonding {pool_sol_account} only trades SOL <-> {mint} \
                 (got input={} output={})",
                order.input_mint, order.output_mint
            )));
        }

        // ai_token_account = ATA(config, mint, TOKEN_PROGRAM)
        let ai_token_account = get_associated_token_address_with_program_id(
            &*PUMPUP_CONFIG_PDA, &mint, &TOKEN_PROGRAM_ID,
        );
        // user_token_account = ATA(user, mint, TOKEN_PROGRAM)
        let user_token_account = get_associated_token_address_with_program_id(
            &order.user, &mint, &TOKEN_PROGRAM_ID,
        );

        // Setup: ensure user has the token ATA — required for both buy (token
        // dest) and sell (token source — the program won't create it).
        let setup = vec![create_associated_token_account_idempotent(
            &order.user,
            &order.user,
            &mint,
            &TOKEN_PROGRAM_ID,
        )];
        let cleanup = Vec::new();

        // ── Instruction data ──
        // Pumpup `buy` is "exact tokens out" — `token_amount` is the desired
        // token output and `max_sol_amount` caps what the user will pay.
        // Pumpup `sell` is "exact tokens in" — `token_amount` is the input and
        // `min_sol_amount` is the user's slippage floor on the SOL received.
        //
        // Our `SwapOrder` interface is "exact SOL in" for buys, so we estimate
        // the expected token output via constant-product math against
        // `(virtual_sol + real_sol, pool_token_reserves)` (matches the
        // program's internal curve), apply a small safety factor to keep
        // total cost under `max_sol_amount`, then enforce slippage against
        // `min_amount_out` before passing it through.
        let mut data = Vec::with_capacity(8 + 16);
        if is_buy {
            let sol_side = (virtual_sol as u128).saturating_add(real_sol as u128);
            let token_side = pool_token_reserves as u128;
            if sol_side == 0 || token_side == 0 {
                return Err(TradeError::Execution(format!(
                    "Pumpup bonding {pool_sol_account}: empty curve reserves \
                     (sol={sol_side} token={token_side})"
                )));
            }
            let amt_in = order.amount_in as u128;
            // x*y=k constant product (gross — fee shaved off via safety factor below)
            let denominator = sol_side
                .checked_add(amt_in)
                .ok_or_else(|| TradeError::Execution("Pumpup bonding sol_in overflow".into()))?;
            let gross = amt_in
                .checked_mul(token_side)
                .ok_or_else(|| TradeError::Execution("Pumpup bonding mul overflow".into()))?
                / denominator;
            // Safety factor: under-estimate by 1% so the program's per-side
            // fee (~1 bp, configurable in PumpupConfiguration) never pushes
            // required SOL above `max_sol_amount = amount_in`.
            let token_amount = gross.saturating_mul(99) / 100;
            if token_amount == 0 {
                return Err(TradeError::Execution(format!(
                    "Pumpup bonding {pool_sol_account}: amount_in {amt_in} too small \
                     to buy a single token (sol_side={sol_side} token_side={token_side})"
                )));
            }
            if token_amount < order.min_amount_out as u128 {
                return Err(TradeError::Execution(format!(
                    "Pumpup bonding {pool_sol_account}: expected output {token_amount} \
                     below min_amount_out {} (slippage)",
                    order.min_amount_out
                )));
            }
            let token_amount: u64 = token_amount.try_into().map_err(|_| {
                TradeError::Execution("Pumpup bonding token_amount overflow u64".into())
            })?;
            data.extend_from_slice(&PUMPUP_BUY_DISC);
            data.extend_from_slice(&token_amount.to_le_bytes());      // token_amount
            data.extend_from_slice(&order.amount_in.to_le_bytes());    // max_sol_amount
        } else {
            data.extend_from_slice(&PUMPUP_SELL_DISC);
            data.extend_from_slice(&order.amount_in.to_le_bytes());      // token_amount
            data.extend_from_slice(&order.min_amount_out.to_le_bytes()); // min_sol_amount
        }

        let accounts = vec![
            AccountMeta::new_readonly(*PUMPUP_CONFIG_PDA, false),               // 0
            AccountMeta::new_readonly(mint, false),                             // 1
            AccountMeta::new(pool_token_account, false),                        // 2
            AccountMeta::new(pool_sol_account, false),                          // 3
            AccountMeta::new(ai_token_account, false),                          // 4
            AccountMeta::new(pumpup_fee, false),                                // 5
            AccountMeta::new(user_token_account, false),                        // 6
            AccountMeta::new(order.user, true),                                 // 7
            AccountMeta::new_readonly(system_program::ID, false),               // 8
            AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false),                 // 9
            AccountMeta::new_readonly(ASSOCIATED_TOKEN_PROGRAM_ID, false),      // 10
            AccountMeta::new_readonly(*PUMPUP_EVENT_AUTHORITY, false),          // 11
            AccountMeta::new_readonly(PUMPUP_PROG_ID, false),                   // 12
        ];

        let swap_ix = Instruction {
            program_id: PUMPUP_PROG_ID,
            accounts,
            data,
        };

        Ok(SwapInstructions {
            setup,
            swap: vec![swap_ix],
            cleanup,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::types::PoolType;
    use sha2::{Digest, Sha256};

    fn make_state(mint: Pubkey, pool: Pubkey) -> PoolState {
        PoolState::PumpupBonding {
            pool,
            mint,
            pool_token_account: Pubkey::new_unique(),
            pumpup_fee: Pubkey::new_unique(),
            virtual_sol: 30_000_000_000,    // 30 SOL virtual
            real_sol: 1_000_000_000,        // 1 SOL real
            pool_sol_reserves: 31_000_000_000,
            pool_token_reserves: 800_000_000_000,
        }
    }

    fn make_order(input_mint: Pubkey, output_mint: Pubkey, pool: Pubkey, amount_in: u64) -> SwapOrder {
        SwapOrder {
            pool_address: pool,
            pool_type: PoolType::PumpupBonding,
            input_mint,
            output_mint,
            amount_in,
            min_amount_out: 1_000_000,
            user: Pubkey::new_unique(),
            input_token_program: TOKEN_PROGRAM_ID,
            output_token_program: TOKEN_PROGRAM_ID,
        }
    }

    #[test]
    fn test_disc_matches_sha256() {
        let mut h = Sha256::new();
        h.update(b"global:buy");
        let buy: [u8; 8] = h.finalize()[..8].try_into().unwrap();
        assert_eq!(buy, PUMPUP_BUY_DISC, "buy discriminator mismatch");

        let mut h = Sha256::new();
        h.update(b"global:sell");
        let sell: [u8; 8] = h.finalize()[..8].try_into().unwrap();
        assert_eq!(sell, PUMPUP_SELL_DISC, "sell discriminator mismatch");
    }

    #[test]
    fn test_buy_builds_correct_disc_and_args() {
        let mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let state = make_state(mint, pool);

        // BUY: input=SOL, output=mint. amount_in=0.05 SOL, min_out=1_000_000.
        // Curve: virtual+real = 31 SOL = 31_000_000_000, tokens = 800_000_000_000.
        // expected = (50_000_000 * 800_000_000_000) / (31_000_000_000 + 50_000_000)
        //          = 4e19 / 31_050_000_000 ≈ 1_288_245_572
        // After 99% safety factor: ~1_275_363_116
        let order = make_order(SOL_NATIVE_MINT, mint, pool, 50_000_000);
        let result = PumpupBondingExecutor.build_swap_ix(&order, &state).unwrap();

        assert_eq!(result.swap.len(), 1);
        let ix = &result.swap[0];
        assert_eq!(ix.program_id, PUMPUP_PROG_ID);
        assert_eq!(&ix.data[..8], &PUMPUP_BUY_DISC);
        // token_amount: computed from curve, must be > min_amount_out and reasonable
        let token_amount = u64::from_le_bytes(ix.data[8..16].try_into().unwrap());
        assert!(token_amount > 1_000_000, "computed token_amount should exceed min_amount_out");
        assert!(token_amount < 1_400_000_000, "computed token_amount sanity bound");
        assert!(token_amount > 1_270_000_000, "computed token_amount should be near 99% of curve estimate");
        // max_sol_amount = amount_in
        assert_eq!(u64::from_le_bytes(ix.data[16..24].try_into().unwrap()), 50_000_000);
        assert_eq!(ix.accounts.len(), 13);
    }

    #[test]
    fn test_buy_rejects_dust_input() {
        let mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let state = make_state(mint, pool);
        // amount_in=1 lamport on a 31 SOL curve → expected tokens < 1 → reject.
        let order = make_order(SOL_NATIVE_MINT, mint, pool, 1);
        assert!(PumpupBondingExecutor.build_swap_ix(&order, &state).is_err());
    }

    #[test]
    fn test_buy_rejects_slippage() {
        let mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let state = make_state(mint, pool);
        // Demand min_amount_out way above what the curve can deliver.
        let mut order = make_order(SOL_NATIVE_MINT, mint, pool, 50_000_000);
        order.min_amount_out = 10_000_000_000; // 10B tokens vs curve ~1.28B
        assert!(PumpupBondingExecutor.build_swap_ix(&order, &state).is_err());
    }

    #[test]
    fn test_sell_builds_correct_disc_and_args() {
        let mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let state = make_state(mint, pool);

        // SELL: input=mint, output=SOL. amount_in=500 tokens, min_out=0.001 SOL.
        let order = make_order(mint, SOL_NATIVE_MINT, pool, 500_000_000);
        let result = PumpupBondingExecutor.build_swap_ix(&order, &state).unwrap();

        let ix = &result.swap[0];
        assert_eq!(&ix.data[..8], &PUMPUP_SELL_DISC);
        // token_amount = amount_in
        assert_eq!(u64::from_le_bytes(ix.data[8..16].try_into().unwrap()), 500_000_000);
        // min_sol_amount = min_amount_out
        assert_eq!(u64::from_le_bytes(ix.data[16..24].try_into().unwrap()), 1_000_000);
    }

    #[test]
    fn test_account_ordering_per_idl() {
        let mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let state = make_state(mint, pool);
        let order = make_order(SOL_NATIVE_MINT, mint, pool, 50_000_000);
        let (pool_token_account, pumpup_fee) = match &state {
            PoolState::PumpupBonding { pool_token_account, pumpup_fee, .. } => (*pool_token_account, *pumpup_fee),
            _ => unreachable!(),
        };
        let result = PumpupBondingExecutor.build_swap_ix(&order, &state).unwrap();
        let ix = &result.swap[0];

        assert_eq!(ix.accounts[0].pubkey, *PUMPUP_CONFIG_PDA, "[0] config");
        assert_eq!(ix.accounts[1].pubkey, mint, "[1] mint");
        assert_eq!(ix.accounts[2].pubkey, pool_token_account, "[2] pool_token_account");
        assert_eq!(ix.accounts[3].pubkey, pool, "[3] pool_sol_account");
        assert_eq!(ix.accounts[5].pubkey, pumpup_fee, "[5] pumpup_fee");
        assert_eq!(ix.accounts[7].pubkey, order.user, "[7] user");
        assert!(ix.accounts[7].is_signer, "[7] user must be signer");
        assert_eq!(ix.accounts[8].pubkey, system_program::ID, "[8] system_program");
        assert_eq!(ix.accounts[9].pubkey, TOKEN_PROGRAM_ID, "[9] token_program");
        assert_eq!(ix.accounts[10].pubkey, ASSOCIATED_TOKEN_PROGRAM_ID, "[10] associated_token_program");
        assert_eq!(ix.accounts[11].pubkey, *PUMPUP_EVENT_AUTHORITY, "[11] event_authority");
        assert_eq!(ix.accounts[12].pubkey, PUMPUP_PROG_ID, "[12] program (self)");
    }

    #[test]
    fn test_writability_flags_per_idl() {
        let mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let state = make_state(mint, pool);
        let order = make_order(SOL_NATIVE_MINT, mint, pool, 50_000_000);
        let result = PumpupBondingExecutor.build_swap_ix(&order, &state).unwrap();
        let ix = &result.swap[0];

        // [0] config readonly, [1] mint readonly, [2..7] writable, [7] signer+writable, [8..12] readonly.
        assert!(!ix.accounts[0].is_writable, "[0] config readonly");
        assert!(!ix.accounts[1].is_writable, "[1] mint readonly");
        for i in 2..=7 {
            assert!(ix.accounts[i].is_writable, "[{i}] should be writable");
        }
        for i in 8..=12 {
            assert!(!ix.accounts[i].is_writable, "[{i}] should be readonly");
        }
    }

    #[test]
    fn test_setup_includes_user_ata_creation() {
        let mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let state = make_state(mint, pool);
        let order = make_order(SOL_NATIVE_MINT, mint, pool, 50_000_000);
        let result = PumpupBondingExecutor.build_swap_ix(&order, &state).unwrap();
        // create_associated_token_account_idempotent ix
        assert_eq!(result.setup.len(), 1);
        assert_eq!(result.setup[0].program_id, ASSOCIATED_TOKEN_PROGRAM_ID);
        assert!(result.cleanup.is_empty());
    }

    #[test]
    fn test_rejects_wrong_pool_type() {
        let pool = Pubkey::new_unique();
        let order = make_order(SOL_NATIVE_MINT, Pubkey::new_unique(), pool, 1_000);
        let bad = PoolState::Pumpup {
            pool,
            token_a_mint: Pubkey::new_unique(),
            token_b_mint: Pubkey::new_unique(),
            token_a_vault: Pubkey::new_unique(),
            token_b_vault: Pubkey::new_unique(),
            fee_recipient: Pubkey::new_unique(),
            fee_recipient2: Pubkey::new_unique(),
            token_a_reserve: 1,
            token_b_reserve: 1,
        };
        assert!(PumpupBondingExecutor.build_swap_ix(&order, &bad).is_err());
    }

    #[test]
    fn test_rejects_non_sol_pair() {
        let mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let state = make_state(mint, pool);
        // input=mint, output=USDC (not SOL) — must reject.
        let order = make_order(mint, USDC_MINT, pool, 1_000_000);
        assert!(PumpupBondingExecutor.build_swap_ix(&order, &state).is_err());
    }
}
