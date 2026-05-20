use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;

use crate::error::{TradeError, TradeResult};
use crate::pool::types::{PoolState, SwapInstructions, SwapOrder};
use crate::constants::*;
use super::{AmmExecutor, DISC_SWAP};

pub struct ByrealExecutor;

fn derive_tick_array(pool: &Pubkey, tick_current: i32, tick_spacing: i32, offset: i32) -> Pubkey {
    let ticks_per_array = 88 * tick_spacing;
    let start_index = if ticks_per_array == 0 { 0 } else {
        (tick_current.div_euclid(ticks_per_array) + offset) * ticks_per_array
    };
    let start_str = start_index.to_string();
    let (pda, _) = Pubkey::find_program_address(
        &[b"tick_array", pool.as_ref(), start_str.as_bytes()],
        &BYREAL_PROG_ID,
    );
    pda
}

impl AmmExecutor for ByrealExecutor {
    fn build_swap_ix(
        &self,
        order: &SwapOrder,
        pool_state: &PoolState,
    ) -> TradeResult<SwapInstructions> {
        let (pool, token_vault_a, token_vault_b, oracle,
             token_mint_a, token_mint_b, tick_current, tick_spacing) =
            match pool_state {
                PoolState::Byreal {
                    pool, token_vault_a, token_vault_b, oracle,
                    token_mint_a, token_mint_b, tick_current, tick_spacing, ..
                } => (pool, token_vault_a, token_vault_b, oracle,
                      token_mint_a, token_mint_b, *tick_current, *tick_spacing),
                _ => return Err(TradeError::Execution("expected Byreal pool state".into())),
            };

        let a_to_b = order.input_mint == *token_mint_a;

        let (ta0, ta1, ta2) = if a_to_b {
            (derive_tick_array(pool, tick_current, tick_spacing, 0),
             derive_tick_array(pool, tick_current, tick_spacing, -1),
             derive_tick_array(pool, tick_current, tick_spacing, -2))
        } else {
            (derive_tick_array(pool, tick_current, tick_spacing, 0),
             derive_tick_array(pool, tick_current, tick_spacing, 1),
             derive_tick_array(pool, tick_current, tick_spacing, 2))
        };

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
            let user_input = get_associated_token_address_with_program_id(&order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID);
            setup.insert(0, create_associated_token_account_idempotent(
                &order.user, &order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID,
            ));
            setup.push(solana_sdk::system_instruction::transfer(&order.user, &user_input, order.amount_in));
            setup.push(spl_token::instruction::sync_native(&TOKEN_PROGRAM_ID, &user_input).unwrap());
            cleanup.push(spl_token::instruction::close_account(
                &TOKEN_PROGRAM_ID, &user_input, &order.user, &order.user, &[],
            ).unwrap());
        }
        if order.output_mint == SOL_NATIVE_MINT {
            let user_output = get_associated_token_address_with_program_id(&order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID);
            cleanup.push(spl_token::instruction::close_account(
                &TOKEN_PROGRAM_ID, &user_output, &order.user, &order.user, &[],
            ).unwrap());
        }

        let sqrt_price_limit: u128 = if a_to_b { 4295048017 } else { 79226673515401279992447579054 };

        let disc = DISC_SWAP;
        let mut data = Vec::with_capacity(42);
        data.extend_from_slice(&disc);
        data.extend_from_slice(&order.amount_in.to_le_bytes());
        data.extend_from_slice(&order.min_amount_out.to_le_bytes());
        data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
        data.push(1u8);
        data.push(if a_to_b { 1u8 } else { 0u8 });

        let accounts = vec![
            AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false),
            AccountMeta::new(order.user, true),
            AccountMeta::new(*pool, false),
            AccountMeta::new(user_token_a, false),
            AccountMeta::new(*token_vault_a, false),
            AccountMeta::new(user_token_b, false),
            AccountMeta::new(*token_vault_b, false),
            AccountMeta::new(ta0, false),
            AccountMeta::new(ta1, false),
            AccountMeta::new(ta2, false),
            AccountMeta::new_readonly(*oracle, false),
        ];

        let swap_ix = Instruction {
            program_id: BYREAL_PROG_ID,
            accounts,
            data,
        };

        Ok(SwapInstructions { setup, swap: vec![swap_ix], cleanup })
    }
}
