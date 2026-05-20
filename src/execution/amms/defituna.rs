use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;

use crate::error::{TradeError, TradeResult};
use crate::pool::types::{PoolState, SwapInstructions, SwapOrder};
use crate::constants::*;
use super::{AmmExecutor, DISC_SWAP};

pub struct DefiTunaFusionExecutor;
pub struct DefiTunaPoolsExecutor;

const MEMO_PROGRAM: Pubkey = Pubkey::from_str_const("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

/// Derive a DefiTuna Fusion tick array PDA.
fn derive_tick_array(pool: &Pubkey, tick_current: i32, tick_spacing: u16, offset: i32) -> Pubkey {
    let ticks_per_array: i32 = 88 * (tick_spacing as i32);
    let start_index = if ticks_per_array == 0 {
        0
    } else {
        let array_idx = tick_current.div_euclid(ticks_per_array) + offset;
        array_idx * ticks_per_array
    };
    let start_str = start_index.to_string();
    let (pda, _) = Pubkey::find_program_address(
        &[b"tick_array", pool.as_ref(), start_str.as_bytes()],
        &DEFITUNA_FUSION_PROG_ID,
    );
    pda
}

impl AmmExecutor for DefiTunaFusionExecutor {
    fn build_swap_ix(
        &self,
        order: &SwapOrder,
        pool_state: &PoolState,
    ) -> TradeResult<SwapInstructions> {
        let (pool, token_vault_a, token_vault_b, token_mint_a, token_mint_b,
             tick_spacing, tick_current_index) =
            match pool_state {
                PoolState::DefiTunaFusion {
                    pool, token_vault_a, token_vault_b, token_mint_a, token_mint_b,
                    tick_spacing, tick_current_index, ..
                } => (pool, token_vault_a, token_vault_b, token_mint_a, token_mint_b,
                      *tick_spacing, *tick_current_index),
                _ => return Err(TradeError::Execution("expected DefiTunaFusion pool state".into())),
            };

        let a_to_b = order.input_mint == *token_mint_a;
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
        add_wsol_handling(&mut setup, &mut cleanup, order);

        let disc = DISC_SWAP;
        let mut data = Vec::with_capacity(43);
        data.extend_from_slice(&disc);
        data.extend_from_slice(&order.amount_in.to_le_bytes());
        data.extend_from_slice(&order.min_amount_out.to_le_bytes());
        data.extend_from_slice(&0u128.to_le_bytes()); // sqrt_price_limit = 0 (no limit)
        data.push(1u8); // amount_specified_is_input = true
        data.push(if a_to_b { 1u8 } else { 0u8 }); // a_to_b
        data.push(0u8); // remaining_accounts_info = None

        // Derive tick array PDAs
        let tick_array_0 = derive_tick_array(pool, tick_current_index, tick_spacing, 0);
        let tick_array_1 = derive_tick_array(pool, tick_current_index, tick_spacing, 1);
        let tick_array_2 = derive_tick_array(pool, tick_current_index, tick_spacing, -1);

        // 14 accounts
        let accounts = vec![
            AccountMeta::new_readonly(prog_a, false),           // [0]  token_program_a
            AccountMeta::new_readonly(prog_b, false),           // [1]  token_program_b
            AccountMeta::new_readonly(MEMO_PROGRAM, false),     // [2]  memo_program
            AccountMeta::new_readonly(order.user, true),        // [3]  token_authority (signer)
            AccountMeta::new(*pool, false),                     // [4]  fusion_pool
            AccountMeta::new_readonly(*token_mint_a, false),    // [5]  token_mint_a
            AccountMeta::new_readonly(*token_mint_b, false),    // [6]  token_mint_b
            AccountMeta::new(user_token_a, false),              // [7]  token_owner_account_a
            AccountMeta::new(user_token_b, false),              // [8]  token_owner_account_b
            AccountMeta::new(*token_vault_a, false),            // [9]  token_vault_a
            AccountMeta::new(*token_vault_b, false),            // [10] token_vault_b
            AccountMeta::new(tick_array_0, false),              // [11] tick_array0
            AccountMeta::new(tick_array_1, false),              // [12] tick_array1
            AccountMeta::new(tick_array_2, false),              // [13] tick_array2
        ];

        let swap_ix = Instruction {
            program_id: DEFITUNA_FUSION_PROG_ID,
            accounts,
            data,
        };

        Ok(SwapInstructions { setup, swap: vec![swap_ix], cleanup })
    }
}

impl AmmExecutor for DefiTunaPoolsExecutor {
    fn build_swap_ix(
        &self,
        order: &SwapOrder,
        pool_state: &PoolState,
    ) -> TradeResult<SwapInstructions> {
        let (pool, token_vault_a, token_vault_b, token_mint_a, token_mint_b) = match pool_state {
            PoolState::DefiTunaPools {
                pool, token_vault_a, token_vault_b, token_mint_a, token_mint_b,
            } => (pool, token_vault_a, token_vault_b, token_mint_a, token_mint_b),
            _ => return Err(TradeError::Execution("expected DefiTunaPools pool state".into())),
        };

        let a_to_b = order.input_mint == *token_mint_a;
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
        add_wsol_handling(&mut setup, &mut cleanup, order);

        let disc = DISC_SWAP;
        let mut data = Vec::with_capacity(25);
        data.extend_from_slice(&disc);
        data.extend_from_slice(&order.amount_in.to_le_bytes());
        data.extend_from_slice(&order.min_amount_out.to_le_bytes());
        data.push(if a_to_b { 0u8 } else { 1u8 });

        let accounts = vec![
            AccountMeta::new(order.user, true),
            AccountMeta::new(user_token_a, false),
            AccountMeta::new(user_token_b, false),
            AccountMeta::new(*pool, false),
            AccountMeta::new(*token_vault_a, false),
            AccountMeta::new(*token_vault_b, false),
            AccountMeta::new_readonly(*token_mint_a, false),
            AccountMeta::new_readonly(*token_mint_b, false),
            AccountMeta::new_readonly(order.input_token_program, false),
        ];

        let swap_ix = Instruction {
            program_id: DEFITUNA_POOLS_PROG_ID,
            accounts,
            data,
        };

        Ok(SwapInstructions { setup, swap: vec![swap_ix], cleanup })
    }
}

fn add_wsol_handling(
    setup: &mut Vec<solana_sdk::instruction::Instruction>,
    cleanup: &mut Vec<solana_sdk::instruction::Instruction>,
    order: &SwapOrder,
) {
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
}
