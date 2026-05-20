use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;

use crate::error::{TradeError, TradeResult};
use crate::pool::types::{PoolState, SwapInstructions, SwapOrder};
use crate::constants::*;
use super::AmmExecutor;

// Anchor discriminators: SHA256("global:<name>")[0..8]
const BUY_DISC: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
const SELL_DISC: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];

// Constant addresses from official PumpSwap IDL / mainnet
const GLOBAL_CONFIG: Pubkey = Pubkey::from_str_const("ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw");
const PROTOCOL_FEE_RECIPIENT: Pubkey = Pubkey::from_str_const("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV");
const EVENT_AUTHORITY: Pubkey = Pubkey::from_str_const("GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR");
const FEE_PROGRAM: Pubkey = Pubkey::from_str_const("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
const SYSTEM_PROGRAM: Pubkey = Pubkey::from_str_const("11111111111111111111111111111111");
const ASSOC_TOKEN_PROGRAM: Pubkey = Pubkey::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

pub struct PumpFunAmmExecutor;

impl AmmExecutor for PumpFunAmmExecutor {
    fn build_swap_ix(
        &self,
        order: &SwapOrder,
        pool_state: &PoolState,
    ) -> TradeResult<SwapInstructions> {
        let (pool, base_mint, quote_mint,
             pool_base_vault, pool_quote_vault, coin_creator,
             base_reserve, quote_reserve) =
            match pool_state {
                PoolState::PumpFunAmm {
                    pool, base_mint, quote_mint,
                    pool_base_vault, pool_quote_vault, coin_creator,
                    base_reserve, quote_reserve,
                } => (pool, base_mint, quote_mint,
                      pool_base_vault, pool_quote_vault, coin_creator,
                      *base_reserve, *quote_reserve),
                _ => return Err(TradeError::Execution("expected PumpFunAmm pool state".into())),
            };

        // PumpSwap IDL:
        //   Buy  = receive base tokens (base_amount_out), spend quote (max_quote_amount_in)
        //   Sell = send base tokens (base_amount_in), receive quote (min_quote_amount_out)
        let use_buy_ix = order.output_mint == *base_mint;

        // Token program for each pool-side mint
        let (base_prog, quote_prog) = if order.input_mint == *base_mint {
            (order.input_token_program, order.output_token_program)
        } else {
            (order.output_token_program, order.input_token_program)
        };
        let user_base_ata = get_associated_token_address_with_program_id(&order.user, base_mint, &base_prog);
        let user_quote_ata = get_associated_token_address_with_program_id(&order.user, quote_mint, &quote_prog);

        let mut setup = vec![
            create_associated_token_account_idempotent(
                &order.user, &order.user, base_mint, &base_prog,
            ),
            create_associated_token_account_idempotent(
                &order.user, &order.user, quote_mint, &quote_prog,
            ),
        ];
        let mut cleanup = Vec::new();

        // WSOL wrapping when spending SOL
        if order.input_mint == SOL_NATIVE_MINT {
            let user_wsol = get_associated_token_address_with_program_id(
                &order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID,
            );
            setup.push(solana_sdk::system_instruction::transfer(
                &order.user, &user_wsol, order.amount_in,
            ));
            setup.push(spl_token::instruction::sync_native(&TOKEN_PROGRAM_ID, &user_wsol).unwrap());
            cleanup.push(spl_token::instruction::close_account(
                &TOKEN_PROGRAM_ID, &user_wsol, &order.user, &order.user, &[],
            ).unwrap());
        }
        // WSOL unwrapping when receiving SOL
        if order.output_mint == SOL_NATIVE_MINT {
            let user_wsol = get_associated_token_address_with_program_id(
                &order.user, &SOL_NATIVE_MINT, &TOKEN_PROGRAM_ID,
            );
            cleanup.push(spl_token::instruction::close_account(
                &TOKEN_PROGRAM_ID, &user_wsol, &order.user, &order.user, &[],
            ).unwrap());
        }

        // Derive PDAs
        let protocol_fee_recipient_ta = get_associated_token_address_with_program_id(
            &PROTOCOL_FEE_RECIPIENT, quote_mint, &quote_prog,
        );

        let (coin_creator_vault_authority, _) = Pubkey::find_program_address(
            &[b"creator_vault", coin_creator.as_ref()],
            &PUMP_FUN_AMM_PROG_ID,
        );

        let coin_creator_vault_ata = get_associated_token_address_with_program_id(
            &coin_creator_vault_authority, quote_mint, &quote_prog,
        );

        let (fee_config, _) = Pubkey::find_program_address(
            &[b"fee_config", PUMP_FUN_AMM_PROG_ID.as_ref()],
            &FEE_PROGRAM,
        );

        // Build instruction data
        let mut data = Vec::with_capacity(24);
        if use_buy_ix {
            data.extend_from_slice(&BUY_DISC);
            let base_amount_out = if base_reserve > 0 && quote_reserve > 0 {
                let out = (base_reserve as u128)
                    .checked_mul(order.amount_in as u128)
                    .unwrap_or(0)
                    / (quote_reserve as u128 + order.amount_in as u128);
                if out == 0 {
                    return Err(TradeError::Execution(format!(
                        "PumpFun AMM pool has insufficient liquidity (base reserve: {base_reserve})"
                    )));
                }
                let result = (out * 95 / 100) as u64;
                if result == 0 { 1u64 } else { result }
            } else {
                return Err(TradeError::Execution(
                    "PumpFun AMM pool reserves unavailable -- cannot compute swap output".into()
                ));
            };
            data.extend_from_slice(&base_amount_out.to_le_bytes());
            data.extend_from_slice(&order.amount_in.to_le_bytes());
        } else {
            data.extend_from_slice(&SELL_DISC);
            data.extend_from_slice(&order.amount_in.to_le_bytes());
            data.extend_from_slice(&order.min_amount_out.to_le_bytes());
        }

        // pool_v2: PDA = ["pool-v2", base_mint] under pAMM
        let (pool_v2, _) = Pubkey::find_program_address(
            &[b"pool-v2", base_mint.as_ref()],
            &PUMP_FUN_AMM_PROG_ID,
        );

        let mut accounts = vec![
            AccountMeta::new(*pool, false),                                    // [0]
            AccountMeta::new(order.user, true),                                // [1]
            AccountMeta::new_readonly(GLOBAL_CONFIG, false),                   // [2]
            AccountMeta::new_readonly(*base_mint, false),                      // [3]
            AccountMeta::new_readonly(*quote_mint, false),                     // [4]
            AccountMeta::new(user_base_ata, false),                            // [5]
            AccountMeta::new(user_quote_ata, false),                           // [6]
            AccountMeta::new(*pool_base_vault, false),                         // [7]
            AccountMeta::new(*pool_quote_vault, false),                        // [8]
            AccountMeta::new_readonly(PROTOCOL_FEE_RECIPIENT, false),          // [9]
            AccountMeta::new(protocol_fee_recipient_ta, false),                // [10]
            AccountMeta::new_readonly(base_prog, false),                       // [11]
            AccountMeta::new_readonly(quote_prog, false),                      // [12]
            AccountMeta::new_readonly(SYSTEM_PROGRAM, false),                  // [13]
            AccountMeta::new_readonly(ASSOC_TOKEN_PROGRAM, false),             // [14]
            AccountMeta::new_readonly(EVENT_AUTHORITY, false),                 // [15]
            AccountMeta::new_readonly(PUMP_FUN_AMM_PROG_ID, false),            // [16]
            AccountMeta::new(coin_creator_vault_ata, false),                   // [17]
            AccountMeta::new_readonly(coin_creator_vault_authority, false),     // [18]
        ];

        if use_buy_ix {
            let (global_vol, _) = Pubkey::find_program_address(
                &[b"global_volume_accumulator"],
                &PUMP_FUN_AMM_PROG_ID,
            );
            let (user_vol, _) = Pubkey::find_program_address(
                &[b"user_volume_accumulator", order.user.as_ref()],
                &PUMP_FUN_AMM_PROG_ID,
            );
            accounts.push(AccountMeta::new_readonly(global_vol, false));       // [19]
            accounts.push(AccountMeta::new(user_vol, false));                  // [20]
        }

        // Common tail: fee_config + fee_program + pool_v2
        accounts.push(AccountMeta::new_readonly(fee_config, false));
        accounts.push(AccountMeta::new_readonly(FEE_PROGRAM, false));
        accounts.push(AccountMeta::new_readonly(pool_v2, false));

        let swap_ix = Instruction {
            program_id: PUMP_FUN_AMM_PROG_ID,
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
