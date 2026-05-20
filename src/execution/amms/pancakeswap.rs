use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;

use crate::error::{TradeError, TradeResult};
use crate::pool::types::{PoolState, SwapInstructions, SwapOrder};
use crate::constants::*;
use super::{AmmExecutor, DISC_SWAP};

pub struct PancakeSwapExecutor;

fn derive_tick_array(pool: &Pubkey, tick_current: i32, tick_spacing: i32, offset: i32) -> Pubkey {
    let ticks_per_array = 88 * tick_spacing;
    let start_index = if ticks_per_array == 0 { 0 } else {
        (tick_current.div_euclid(ticks_per_array) + offset) * ticks_per_array
    };
    let start_str = start_index.to_string();
    let (pda, _) = Pubkey::find_program_address(
        &[b"tick_array", pool.as_ref(), start_str.as_bytes()],
        &PANCAKESWAP_PROG_ID,
    );
    pda
}

impl AmmExecutor for PancakeSwapExecutor {
    fn build_swap_ix(
        &self,
        order: &SwapOrder,
        pool_state: &PoolState,
    ) -> TradeResult<SwapInstructions> {
        let (pool, amm_config, token_vault_a, token_vault_b, observation,
             token_mint_a, token_mint_b, tick_current, tick_spacing) =
            match pool_state {
                PoolState::PancakeSwap {
                    pool, amm_config, token_vault_a, token_vault_b, observation,
                    token_mint_a, token_mint_b, tick_current, tick_spacing, ..
                } => (pool, amm_config, token_vault_a, token_vault_b, observation,
                      token_mint_a, token_mint_b, *tick_current, *tick_spacing),
                _ => return Err(TradeError::Execution("expected PancakeSwap pool state".into())),
            };

        let a_to_b = order.input_mint == *token_mint_a;

        // Derive tick arrays for the swap direction
        let tick_array = derive_tick_array(pool, tick_current, tick_spacing, 0);

        let (prog_a, prog_b) = if a_to_b {
            (order.input_token_program, order.output_token_program)
        } else {
            (order.output_token_program, order.input_token_program)
        };
        let user_token_a = get_associated_token_address_with_program_id(&order.user, token_mint_a, &prog_a);
        let user_token_b = get_associated_token_address_with_program_id(&order.user, token_mint_b, &prog_b);

        let mut setup = vec![
            create_associated_token_account_idempotent(
                &order.user, &order.user, &order.output_mint, &order.output_token_program,
            ),
        ];
        let mut cleanup = Vec::new();

        if order.input_mint == SOL_NATIVE_MINT {
            let ui = get_associated_token_address_with_program_id(&order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID);
            setup.insert(0, create_associated_token_account_idempotent(
                &order.user, &order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID,
            ));
            setup.push(solana_sdk::system_instruction::transfer(&order.user, &ui, order.amount_in));
            setup.push(spl_token::instruction::sync_native(&TOKEN_PROGRAM_ID, &ui).unwrap());
            cleanup.push(spl_token::instruction::close_account(
                &TOKEN_PROGRAM_ID, &ui, &order.user, &order.user, &[],
            ).unwrap());
        }
        if order.output_mint == SOL_NATIVE_MINT {
            let uo = get_associated_token_address_with_program_id(&order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID);
            cleanup.push(spl_token::instruction::close_account(
                &TOKEN_PROGRAM_ID, &uo, &order.user, &order.user, &[],
            ).unwrap());
        }

        // sqrt_price_limit: boundary values
        let sqrt_price_limit: u128 = if a_to_b { 4295048017 } else { 79226673515401279992447579054 };

        let disc = DISC_SWAP;
        let mut data = Vec::with_capacity(41);
        data.extend_from_slice(&disc);
        data.extend_from_slice(&order.amount_in.to_le_bytes());
        data.extend_from_slice(&order.min_amount_out.to_le_bytes());
        data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
        data.push(1u8); // is_base_input = true

        // Determine input/output vaults based on direction
        let (input_vault, output_vault) = if a_to_b {
            (*token_vault_a, *token_vault_b)
        } else {
            (*token_vault_b, *token_vault_a)
        };

        let (input_ata, output_ata) = if a_to_b {
            (user_token_a, user_token_b)
        } else {
            (user_token_b, user_token_a)
        };

        // Accounts (10) -- PancakeSwap CLMM Swap (Raydium CLMM fork)
        let accounts = vec![
            AccountMeta::new(order.user, true),                     // [0] payer
            AccountMeta::new_readonly(*amm_config, false),          // [1] amm_config
            AccountMeta::new(*pool, false),                         // [2] pool_state
            AccountMeta::new(input_ata, false),                     // [3] input_token_account
            AccountMeta::new(output_ata, false),                    // [4] output_token_account
            AccountMeta::new(input_vault, false),                   // [5] input_vault
            AccountMeta::new(output_vault, false),                  // [6] output_vault
            AccountMeta::new(*observation, false),                  // [7] observation_state
            AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false),     // [8] token_program
            AccountMeta::new(tick_array, false),                    // [9] tick_array
        ];

        let swap_ix = Instruction {
            program_id: PANCAKESWAP_PROG_ID,
            accounts,
            data,
        };

        Ok(SwapInstructions { setup, swap: vec![swap_ix], cleanup })
    }
}
