use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;

use crate::error::{TradeError, TradeResult};
use crate::pool::types::{PoolState, SwapInstructions, SwapOrder};
use crate::constants::*;
use super::AmmExecutor;

pub struct PumpFunExecutor;

// PumpFun instruction discriminators
// buy_exact_sol_in: user specifies SOL to spend, accepts any tokens out
const BUY_EXACT_SOL_IN_DISC: [u8; 8] = [0x38, 0xfc, 0x74, 0x08, 0x9e, 0xdf, 0xcd, 0x5f];
const SELL_DISC: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];

impl AmmExecutor for PumpFunExecutor {
    fn build_swap_ix(
        &self,
        order: &SwapOrder,
        pool_state: &PoolState,
    ) -> TradeResult<SwapInstructions> {
        let (global, fee_account, mint, bonding_curve, associated_bonding_curve, event_authority, creator) =
            match pool_state {
                PoolState::PumpFun {
                    global, fee_account, mint, bonding_curve,
                    associated_bonding_curve, event_authority, creator,
                } => (global, fee_account, mint, bonding_curve, associated_bonding_curve, event_authority, creator),
                _ => return Err(TradeError::Execution("expected PumpFun pool state".into())),
            };

        // Direction: buy = SOL -> token, sell = token -> SOL
        let is_buy = order.input_mint == SOL_NATIVE_MINT;
        let token_prog = if is_buy { order.output_token_program } else { order.input_token_program };
        let user_token_ata = get_associated_token_address_with_program_id(&order.user, mint, &token_prog);

        let setup = vec![
            create_associated_token_account_idempotent(
                &order.user, &order.user, mint, &token_prog,
            ),
        ];
        let cleanup = Vec::new();

        // Build instruction data
        let data = if is_buy {
            // buy_exact_sol_in(spendable_sol_in, min_tokens_out, track_volume: Option<bool>)
            let mut buf = Vec::with_capacity(25);
            buf.extend_from_slice(&BUY_EXACT_SOL_IN_DISC);
            buf.extend_from_slice(&order.amount_in.to_le_bytes());      // SOL to spend
            buf.extend_from_slice(&order.min_amount_out.to_le_bytes()); // min tokens out
            buf.push(0x00); // track_volume = None
            buf
        } else {
            // sell(amount, min_sol_output)
            let mut buf = Vec::with_capacity(24);
            buf.extend_from_slice(&SELL_DISC);
            buf.extend_from_slice(&order.amount_in.to_le_bytes());      // token amount
            buf.extend_from_slice(&order.min_amount_out.to_le_bytes()); // min SOL output
            buf
        };

        // V2 PumpFun constants
        let global_volume_accumulator = pubkey_from_str(
            "Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y",
        );
        let fee_config = pubkey_from_str(
            "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt",
        );
        let fee_program = pubkey_from_str(
            "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ",
        );

        // Creator vault PDA: seeds = ["creator-vault", bonding_curve_data.creator]
        let (creator_vault, _) = Pubkey::find_program_address(
            &[b"creator-vault", creator.as_ref()],
            &PUMP_FUN_PROG_ID,
        );

        // User volume accumulator PDA: per-user trade tracking
        let (user_volume_accumulator, _) = Pubkey::find_program_address(
            &[b"user_volume_accumulator", order.user.as_ref()],
            &PUMP_FUN_PROG_ID,
        );

        let accounts = if is_buy {
            // BUY V2 (16 accounts):
            vec![
                AccountMeta::new_readonly(*global, false),                             // 0
                AccountMeta::new(*fee_account, false),                                 // 1
                AccountMeta::new_readonly(*mint, false),                               // 2
                AccountMeta::new(*bonding_curve, false),                               // 3
                AccountMeta::new(*associated_bonding_curve, false),                    // 4
                AccountMeta::new(user_token_ata, false),                               // 5
                AccountMeta::new(order.user, true),                                    // 6
                AccountMeta::new_readonly(solana_sdk::system_program::ID, false),      // 7
                AccountMeta::new_readonly(token_prog, false),                          // 8
                AccountMeta::new(creator_vault, false),                                // 9
                AccountMeta::new_readonly(*event_authority, false),                    // 10
                AccountMeta::new_readonly(PUMP_FUN_PROG_ID, false),                   // 11
                AccountMeta::new_readonly(global_volume_accumulator, false),             // 12
                AccountMeta::new(user_volume_accumulator, false),                      // 13
                AccountMeta::new_readonly(fee_config, false),                          // 14
                AccountMeta::new_readonly(fee_program, false),                         // 15
            ]
        } else {
            // SELL V2 (14 accounts):
            vec![
                AccountMeta::new_readonly(*global, false),                             // 0
                AccountMeta::new(*fee_account, false),                                 // 1
                AccountMeta::new_readonly(*mint, false),                               // 2
                AccountMeta::new(*bonding_curve, false),                               // 3
                AccountMeta::new(*associated_bonding_curve, false),                    // 4
                AccountMeta::new(user_token_ata, false),                               // 5
                AccountMeta::new(order.user, true),                                    // 6
                AccountMeta::new_readonly(solana_sdk::system_program::ID, false),      // 7
                AccountMeta::new(creator_vault, false),                                // 8
                AccountMeta::new_readonly(token_prog, false),                          // 9
                AccountMeta::new_readonly(*event_authority, false),                    // 10
                AccountMeta::new_readonly(PUMP_FUN_PROG_ID, false),                   // 11
                AccountMeta::new_readonly(fee_config, false),                          // 12
                AccountMeta::new_readonly(fee_program, false),                         // 13
            ]
        };

        let swap_ix = Instruction {
            program_id: PUMP_FUN_PROG_ID,
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

fn pubkey_from_str(s: &str) -> Pubkey {
    s.parse::<Pubkey>().expect("invalid hardcoded pubkey")
}
