use solana_sdk::instruction::{AccountMeta, Instruction};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;

use crate::error::{TradeError, TradeResult};
use crate::pool::types::{PoolState, SwapInstructions, SwapOrder};
use crate::constants::*;
use super::{AmmExecutor, DISC_SWAP};

pub struct FlashTradeExecutor;

impl AmmExecutor for FlashTradeExecutor {
    fn build_swap_ix(
        &self,
        order: &SwapOrder,
        pool_state: &PoolState,
    ) -> TradeResult<SwapInstructions> {
        let (pool, oracle, custody, _token_mint) = match pool_state {
            PoolState::FlashTrade { pool, oracle, custody, token_mint } => {
                (pool, oracle, custody, token_mint)
            }
            _ => return Err(TradeError::Execution("expected FlashTrade pool state".into())),
        };

        let user_input_ata = get_associated_token_address_with_program_id(&order.user, &order.input_mint, &order.input_token_program);
        let user_output_ata = get_associated_token_address_with_program_id(&order.user, &order.output_mint, &order.output_token_program);

        let mut setup = vec![
            create_associated_token_account_idempotent(
                &order.user, &order.user, &order.output_mint, &order.output_token_program,
            ),
        ];
        let mut cleanup = Vec::new();

        if order.input_mint == SOL_NATIVE_MINT {
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
            cleanup.push(spl_token::instruction::close_account(
                &TOKEN_PROGRAM_ID, &user_output_ata, &order.user, &order.user, &[],
            ).unwrap());
        }

        let disc = DISC_SWAP;
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&disc);
        data.extend_from_slice(&order.amount_in.to_le_bytes());
        data.extend_from_slice(&order.min_amount_out.to_le_bytes());

        let accounts = vec![
            AccountMeta::new(order.user, true),
            AccountMeta::new(user_input_ata, false),
            AccountMeta::new(user_output_ata, false),
            AccountMeta::new(*pool, false),
            AccountMeta::new_readonly(*oracle, false),
            AccountMeta::new(*custody, false),
            AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false),
        ];

        let swap_ix = Instruction {
            program_id: FLASH_TRADE_PROG_ID,
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
