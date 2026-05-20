use solana_sdk::instruction::{AccountMeta, Instruction};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;

use crate::error::{TradeError, TradeResult};
use crate::pool::types::{PoolState, SwapInstructions, SwapOrder};
use crate::constants::*;
use super::{AmmExecutor, DISC_SWAP};

pub struct MeteoraDlmmExecutor;

impl AmmExecutor for MeteoraDlmmExecutor {
    fn build_swap_ix(
        &self,
        order: &SwapOrder,
        pool_state: &PoolState,
    ) -> TradeResult<SwapInstructions> {
        let (lb_pair, bin_array_bitmap_extension, reserve_x, reserve_y,
             token_x_mint, token_y_mint, oracle, host_fee_in,
             event_authority, bin_arrays) =
            match pool_state {
                PoolState::MeteoraDlmm {
                    lb_pair, bin_array_bitmap_extension, reserve_x, reserve_y,
                    token_x_mint, token_y_mint, oracle, host_fee_in,
                    event_authority, bin_arrays,
                } => (lb_pair, bin_array_bitmap_extension, reserve_x, reserve_y,
                      token_x_mint, token_y_mint, oracle, host_fee_in,
                      event_authority, bin_arrays),
                _ => return Err(TradeError::Execution("expected MeteoraDlmm pool state".into())),
            };

        // Determine token programs for X and Y
        let (x_prog, y_prog) = if order.input_mint == *token_x_mint {
            (order.input_token_program, order.output_token_program)
        } else {
            (order.output_token_program, order.input_token_program)
        };
        let user_token_x = get_associated_token_address_with_program_id(&order.user, token_x_mint, &x_prog);
        let user_token_y = get_associated_token_address_with_program_id(&order.user, token_y_mint, &y_prog);

        let mut setup = vec![
            create_associated_token_account_idempotent(
                &order.user, &order.user, &order.output_mint, &order.output_token_program,
            ),
        ];
        let mut cleanup = Vec::new();

        // WSOL handling
        if order.input_mint == SOL_NATIVE_MINT {
            let user_input_ata = get_associated_token_address_with_program_id(&order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID);
            setup.insert(0, create_associated_token_account_idempotent(
                &order.user, &order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID,
            ));
            setup.push(solana_sdk::system_instruction::transfer(
                &order.user, &user_input_ata, order.amount_in,
            ));
            setup.push(spl_token::instruction::sync_native(&TOKEN_PROGRAM_ID, &user_input_ata).unwrap());
            cleanup.push(spl_token::instruction::close_account(
                &TOKEN_PROGRAM_ID, &user_input_ata, &order.user, &order.user, &[],
            ).unwrap());
        }
        if order.output_mint == SOL_NATIVE_MINT {
            let user_output_ata = get_associated_token_address_with_program_id(&order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID);
            cleanup.push(spl_token::instruction::close_account(
                &TOKEN_PROGRAM_ID, &user_output_ata, &order.user, &order.user, &[],
            ).unwrap());
        }

        // Data: discriminator + amount_in(u64) + min_amount_out(u64)
        let disc = DISC_SWAP;
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&disc);
        data.extend_from_slice(&order.amount_in.to_le_bytes());
        data.extend_from_slice(&order.min_amount_out.to_le_bytes());

        let swap_x_to_y = order.input_mint == *token_x_mint;
        let (user_token_in, user_token_out) = if swap_x_to_y {
            (user_token_x, user_token_y)
        } else {
            (user_token_y, user_token_x)
        };

        let mut accounts = vec![
            AccountMeta::new(*lb_pair, false),
            AccountMeta::new_readonly(*bin_array_bitmap_extension, false),
            AccountMeta::new(*reserve_x, false),
            AccountMeta::new(*reserve_y, false),
            AccountMeta::new(user_token_in, false),
            AccountMeta::new(user_token_out, false),
            AccountMeta::new_readonly(*token_x_mint, false),
            AccountMeta::new_readonly(*token_y_mint, false),
            AccountMeta::new(*oracle, false),
            AccountMeta::new_readonly(*host_fee_in, false),
            AccountMeta::new(order.user, true),
            AccountMeta::new_readonly(x_prog, false),
            AccountMeta::new_readonly(y_prog, false),
            AccountMeta::new_readonly(*event_authority, false),
            AccountMeta::new_readonly(METEORA_DLMM_PROG_ID, false),
        ];

        // Remaining accounts: bin arrays
        for ba in bin_arrays {
            accounts.push(AccountMeta::new(*ba, false));
        }

        let swap_ix = Instruction {
            program_id: METEORA_DLMM_PROG_ID,
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
