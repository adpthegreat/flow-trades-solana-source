use serde::{Deserialize, Serialize};
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use std::fmt;

/// Supported DEX pool types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PoolType {
    #[default]
    Unknown,
    RaydiumV4,
    RaydiumCpmm,
    RaydiumCl,
    RaydiumLp,
    PumpFun,
    PumpFunAmm,
    Meteora,
    MeteoraDlmm,
    MeteoraDamm,
    MeteoraDbc,
    Orca,
    FluxBeam,
    FlashTrade,
    Byreal,
    DefiTunaFusion,
    DefiTunaPools,
    Saros,
    PancakeSwap,
    Dooar,
    Pumpup,
    /// Pumpup pre-graduation bonding curve (native SOL pair). Pool address =
    /// `pool_sol_account` PDA (`["pumpup.pool", mint]`). State stored inline
    /// as the BondingCurve struct.
    PumpupBonding,
}

impl PoolType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PoolType::Unknown => "UNKNOWN",
            PoolType::RaydiumV4 => "RAYDIUM_V4",
            PoolType::RaydiumCpmm => "RAYDIUM_CPMM",
            PoolType::RaydiumCl => "RAYDIUM_CL",
            PoolType::RaydiumLp => "RAYDIUM_LP",
            PoolType::PumpFun => "PUMP_FUN",
            PoolType::PumpFunAmm => "PUMP_FUN_AMM",
            PoolType::Meteora => "METEORA",
            PoolType::MeteoraDlmm => "METEORA_DLMM",
            PoolType::MeteoraDamm => "METEORA_DAMM",
            PoolType::MeteoraDbc => "METEORA_DBC",
            PoolType::Orca => "ORCA",

            PoolType::FluxBeam => "FLUXBEAM",
            PoolType::FlashTrade => "FLASH_TRADE",
            PoolType::Byreal => "BYREAL",
            PoolType::DefiTunaFusion => "DEFITUNA_FUSION",
            PoolType::DefiTunaPools => "DEFITUNA_POOLS",
            PoolType::Saros => "SAROS",
            PoolType::PancakeSwap => "PANCAKESWAP",
            PoolType::Dooar => "DOOAR",
            PoolType::Pumpup => "PUMPUP",
            PoolType::PumpupBonding => "PUMPUP_BONDING",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s {
            "UNKNOWN" => Some(PoolType::Unknown),
            "RAYDIUM_V4" => Some(PoolType::RaydiumV4),
            "RAYDIUM_CPMM" => Some(PoolType::RaydiumCpmm),
            "RAYDIUM_CL" => Some(PoolType::RaydiumCl),
            "RAYDIUM_LP" => Some(PoolType::RaydiumLp),
            "PUMP_FUN" => Some(PoolType::PumpFun),
            "PUMP_FUN_AMM" => Some(PoolType::PumpFunAmm),
            "METEORA" => Some(PoolType::Meteora),
            "METEORA_DLMM" => Some(PoolType::MeteoraDlmm),
            "METEORA_DAMM" => Some(PoolType::MeteoraDamm),
            "METEORA_DBC" => Some(PoolType::MeteoraDbc),
            "ORCA" => Some(PoolType::Orca),

            "FLUXBEAM" => Some(PoolType::FluxBeam),
            "FLASH_TRADE" => Some(PoolType::FlashTrade),
            "BYREAL" => Some(PoolType::Byreal),
            "DEFITUNA_FUSION" => Some(PoolType::DefiTunaFusion),
            "DEFITUNA_POOLS" => Some(PoolType::DefiTunaPools),
            "SAROS" => Some(PoolType::Saros),
            "PANCAKESWAP" => Some(PoolType::PancakeSwap),
            "DOOAR" => Some(PoolType::Dooar),
            "PUMPUP" => Some(PoolType::Pumpup),
            "PUMPUP_BONDING" => Some(PoolType::PumpupBonding),
            _ => None,
        }
    }
}

impl fmt::Display for PoolType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for PoolType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_opt(s).ok_or_else(|| format!("Unknown pool type: {s}"))
    }
}

impl Serialize for PoolType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PoolType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        PoolType::from_str_opt(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("Unknown pool type: {s}")))
    }
}

/// On-chain pool state fetched before building instructions.
/// One variant per AMM -- contains all account addresses needed for the swap instruction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PoolState {
    RaydiumV4 {
        amm_id: Pubkey,
        authority: Pubkey,
        open_orders: Pubkey,
        target_orders: Pubkey,
        coin_vault: Pubkey,
        pc_vault: Pubkey,
        serum_program: Pubkey,
        serum_market: Pubkey,
        serum_bids: Pubkey,
        serum_asks: Pubkey,
        serum_event_queue: Pubkey,
        serum_coin_vault: Pubkey,
        serum_pc_vault: Pubkey,
        serum_vault_signer: Pubkey,
    },
    RaydiumCpmm {
        pool: Pubkey,
        authority: Pubkey,
        config: Pubkey,
        token_0_vault: Pubkey,
        token_1_vault: Pubkey,
        token_0_mint: Pubkey,
        token_1_mint: Pubkey,
        observation: Pubkey,
    },
    RaydiumClmm {
        pool: Pubkey,
        amm_config: Pubkey,
        observation: Pubkey,
        token_vault_0: Pubkey,
        token_vault_1: Pubkey,
        tick_array_0: Pubkey,
        tick_array_1: Pubkey,
        tick_array_2: Pubkey,
        token_mint_0: Pubkey,
        token_mint_1: Pubkey,
        tick_current: i32,
        tick_spacing: i32,
        /// Current sqrt price as Q64.64 fixed-point (u128)
        sqrt_price_x64: u128,
        /// Current tick range liquidity (u128)
        liquidity: u128,
        /// Fee rate in hundredths of a basis point (e.g., 2500 = 25 bps)
        fee_rate: u16,
    },
    RaydiumLp {
        pool_state: Pubkey,
        authority: Pubkey,
        base_vault: Pubkey,
        quote_vault: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
        config_id: Pubkey,
        platform_id: Pubkey,
        creator: Pubkey,
    },
    PumpFun {
        global: Pubkey,
        fee_account: Pubkey,
        mint: Pubkey,
        bonding_curve: Pubkey,
        associated_bonding_curve: Pubkey,
        event_authority: Pubkey,
        /// Creator pubkey from bonding curve data (offset +49). Used for creator_vault PDA.
        creator: Pubkey,
    },
    PumpFunAmm {
        pool: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
        pool_base_vault: Pubkey,
        pool_quote_vault: Pubkey,
        coin_creator: Pubkey,
        /// Token balance in pool_base_vault (for swap amount computation)
        base_reserve: u64,
        /// Token balance in pool_quote_vault (for swap amount computation)
        quote_reserve: u64,
    },
    Meteora {
        pool: Pubkey,
        token_a_mint: Pubkey,
        token_b_mint: Pubkey,
        a_vault: Pubkey,
        b_vault: Pubkey,
        a_token_vault: Pubkey,
        b_token_vault: Pubkey,
        a_vault_lp_mint: Pubkey,
        b_vault_lp_mint: Pubkey,
        a_vault_lp: Pubkey,
        b_vault_lp: Pubkey,
        admin_token_a_fee: Pubkey,
        admin_token_b_fee: Pubkey,
        vault_program: Pubkey,
    },
    MeteoraDlmm {
        lb_pair: Pubkey,
        bin_array_bitmap_extension: Pubkey,
        reserve_x: Pubkey,
        reserve_y: Pubkey,
        token_x_mint: Pubkey,
        token_y_mint: Pubkey,
        oracle: Pubkey,
        host_fee_in: Pubkey,
        event_authority: Pubkey,
        /// Bin array PDAs derived from active_id
        bin_arrays: Vec<Pubkey>,
    },
    MeteoraDamm {
        pool: Pubkey,
        token_a_vault: Pubkey,
        token_b_vault: Pubkey,
        token_a_mint: Pubkey,
        token_b_mint: Pubkey,
    },
    MeteoraDbc {
        pool: Pubkey,
        config: Pubkey,
        pool_authority: Pubkey,
        base_vault: Pubkey,
        quote_vault: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
    },
    Orca {
        whirlpool: Pubkey,
        token_vault_a: Pubkey,
        token_vault_b: Pubkey,
        oracle: Pubkey,
        token_mint_a: Pubkey,
        token_mint_b: Pubkey,
        tick_current: i32,
        tick_spacing: i32,
        /// Current sqrt price as Q64.64 fixed-point (u128)
        sqrt_price_x64: u128,
        /// Current tick range liquidity (u128)
        liquidity: u128,
        /// Fee rate in hundredths of a basis point (e.g., 2500 = 25 bps)
        fee_rate: u16,
    },
    FluxBeam {
        pool: Pubkey,
        authority: Pubkey,
        token_a_vault: Pubkey,
        token_b_vault: Pubkey,
        pool_mint: Pubkey,
        fee_account: Pubkey,
        token_a_mint: Pubkey,
        token_b_mint: Pubkey,
        pool_token_program: Pubkey,
    },
    FlashTrade {
        pool: Pubkey,
        oracle: Pubkey,
        custody: Pubkey,
        token_mint: Pubkey,
    },
    Byreal {
        pool: Pubkey,
        token_vault_a: Pubkey,
        token_vault_b: Pubkey,
        oracle: Pubkey,
        token_mint_a: Pubkey,
        token_mint_b: Pubkey,
        tick_current: i32,
        tick_spacing: i32,
        /// Current sqrt price as Q64.64 fixed-point (u128)
        sqrt_price_x64: u128,
        /// Current tick range liquidity (u128)
        liquidity: u128,
    },
    DefiTunaFusion {
        pool: Pubkey,
        token_vault_a: Pubkey,
        token_vault_b: Pubkey,
        token_mint_a: Pubkey,
        token_mint_b: Pubkey,
        tick_spacing: u16,
        tick_current_index: i32,
        /// Current sqrt price as Q64.64 fixed-point (u128)
        sqrt_price_x64: u128,
        /// Current tick range liquidity (u128)
        liquidity: u128,
        /// Fee rate in hundredths of a basis point
        fee_rate: u16,
    },
    DefiTunaPools {
        pool: Pubkey,
        token_vault_a: Pubkey,
        token_vault_b: Pubkey,
        token_mint_a: Pubkey,
        token_mint_b: Pubkey,
    },
    Saros {
        pool: Pubkey,
        authority: Pubkey,
        token_a_vault: Pubkey,
        token_b_vault: Pubkey,
        pool_mint: Pubkey,
        fee_account: Pubkey,
        token_a_mint: Pubkey,
        token_b_mint: Pubkey,
    },
    PancakeSwap {
        pool: Pubkey,
        amm_config: Pubkey,
        token_vault_a: Pubkey,
        token_vault_b: Pubkey,
        observation: Pubkey,
        token_mint_a: Pubkey,
        token_mint_b: Pubkey,
        tick_current: i32,
        tick_spacing: i32,
        /// Current sqrt price as Q64.64 fixed-point (u128)
        sqrt_price_x64: u128,
        /// Current tick range liquidity (u128)
        liquidity: u128,
        /// Fee rate in hundredths of a basis point
        fee_rate: u16,
    },
    Dooar {
        pool: Pubkey,
        authority: Pubkey,
        token_a_vault: Pubkey,
        token_b_vault: Pubkey,
        pool_mint: Pubkey,
        fee_account: Pubkey,
        token_a_mint: Pubkey,
        token_b_mint: Pubkey,
    },
    /// Pumpup post-graduation AMM pool. Layout sourced from on-chain Anchor IDL
    /// at `BzBmXJiz9H88PAZomvWn8UvmdmeucWZg7N1cygN5po61`. Reserves are stored
    /// inline in the Pool account — no vault RPC fetch required for quoting.
    Pumpup {
        pool: Pubkey,
        token_a_mint: Pubkey,
        token_b_mint: Pubkey,
        token_a_vault: Pubkey,
        token_b_vault: Pubkey,
        fee_recipient: Pubkey,
        fee_recipient2: Pubkey,
        token_a_reserve: u64,
        token_b_reserve: u64,
    },
    /// Pumpup pre-graduation bonding curve. The BondingCurve struct lives
    /// inside `pool_sol_account` (PDA `["pumpup.pool", mint]`) — that account
    /// holds both lamports (real SOL) and the curve data. `pool` is therefore
    /// `pool_sol_account` itself. Native SOL pair (no WSOL).
    ///
    /// Quoting uses constant-product math against `(virtual_sol + real_sol,
    /// pool_token_reserves)` — matches the PumpFun bonding pattern.
    PumpupBonding {
        /// `pool_sol_account` — also the pool address.
        pool: Pubkey,
        /// Token mint paired against SOL.
        mint: Pubkey,
        /// ATA(pool_sol_account, mint, TOKEN_PROGRAM_ID) — token vault.
        pool_token_account: Pubkey,
        /// pumpup_fee recipient — read from PumpupConfiguration singleton.
        pumpup_fee: Pubkey,
        /// Virtual SOL reserve for constant-product math.
        virtual_sol: u64,
        /// Real SOL collected (subset of pool_sol_reserves).
        real_sol: u64,
        /// Live SOL reserve in pool_sol_account.
        pool_sol_reserves: u64,
        /// Live token reserve in pool_token_account.
        pool_token_reserves: u64,
    },
}

/// What the caller submits to execute a swap.
#[derive(Debug, Clone)]
pub struct SwapOrder {
    pub pool_address: Pubkey,
    pub pool_type: PoolType,
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub amount_in: u64,
    pub min_amount_out: u64,
    pub user: Pubkey,
    /// Token program owning input_mint (Token or Token-2022).
    pub input_token_program: Pubkey,
    /// Token program owning output_mint (Token or Token-2022).
    pub output_token_program: Pubkey,
}

/// Instructions built by an AMM executor, ready for transaction assembly.
#[derive(Debug)]
pub struct SwapInstructions {
    pub setup: Vec<Instruction>,
    pub swap: Vec<Instruction>,
    pub cleanup: Vec<Instruction>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_type_display() {
        assert_eq!(PoolType::RaydiumV4.as_str(), "RAYDIUM_V4");
        assert_eq!(PoolType::PumpFun.as_str(), "PUMP_FUN");
        assert_eq!(PoolType::MeteoraDlmm.as_str(), "METEORA_DLMM");
        assert_eq!(PoolType::Unknown.as_str(), "UNKNOWN");
    }

    #[test]
    fn test_pool_type_default() {
        let pt: PoolType = Default::default();
        assert_eq!(pt, PoolType::Unknown);
    }

    #[test]
    fn test_pool_type_from_str_roundtrip() {
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
            PoolType::Pumpup,
            PoolType::PumpupBonding,
        ];
        for variant in &variants {
            let s = variant.as_str();
            let parsed: PoolType = s.parse().unwrap();
            assert_eq!(*variant, parsed, "round-trip failed for {s}");
        }
    }

    #[test]
    fn test_pool_type_from_str_invalid() {
        let result: Result<PoolType, _> = "INVALID_POOL".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_type_serialize_roundtrip() {
        let variants = [
            PoolType::RaydiumV4,
            PoolType::PumpFun,
            PoolType::Orca,
            PoolType::MeteoraDlmm,
        ];
        for variant in &variants {
            let json = serde_json::to_string(variant).unwrap();
            let parsed: PoolType = serde_json::from_str(&json).unwrap();
            assert_eq!(*variant, parsed);
        }
    }

    #[test]
    fn test_pool_type_deserialize_invalid() {
        let result: Result<PoolType, _> = serde_json::from_str("\"NOT_A_POOL\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_type_all_variants_count() {
        // 22 variants total (including Unknown, Pumpup, PumpupBonding)
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
            PoolType::Pumpup,
            PoolType::PumpupBonding,
        ];
        assert_eq!(variants.len(), 22);
        // Verify all have unique string representations
        let strs: std::collections::HashSet<&str> = variants.iter().map(|v| v.as_str()).collect();
        assert_eq!(strs.len(), 22);
    }

    #[test]
    fn test_pool_type_display_matches_as_str() {
        let variants = [
            PoolType::RaydiumCpmm,
            PoolType::Orca,
            PoolType::PumpFunAmm,
        ];
        for v in &variants {
            assert_eq!(format!("{v}"), v.as_str());
        }
    }

    #[test]
    fn test_pool_state_json_roundtrip() {
        // Test a simple variant
        let state = PoolState::MeteoraDamm {
            pool: Pubkey::new_unique(),
            token_a_vault: Pubkey::new_unique(),
            token_b_vault: Pubkey::new_unique(),
            token_a_mint: Pubkey::new_unique(),
            token_b_mint: Pubkey::new_unique(),
        };
        let json = serde_json::to_string(&state).unwrap();
        let parsed: PoolState = serde_json::from_str(&json).unwrap();
        // Compare by re-serializing (PoolState doesn't derive PartialEq)
        let json2 = serde_json::to_string(&parsed).unwrap();
        assert_eq!(json, json2);
    }

    #[test]
    fn test_pool_state_json_roundtrip_with_vec() {
        // Test a variant with Vec<Pubkey> (MeteoraDlmm.bin_arrays)
        let state = PoolState::MeteoraDlmm {
            lb_pair: Pubkey::new_unique(),
            bin_array_bitmap_extension: Pubkey::new_unique(),
            reserve_x: Pubkey::new_unique(),
            reserve_y: Pubkey::new_unique(),
            token_x_mint: Pubkey::new_unique(),
            token_y_mint: Pubkey::new_unique(),
            oracle: Pubkey::new_unique(),
            host_fee_in: Pubkey::new_unique(),
            event_authority: Pubkey::new_unique(),
            bin_arrays: vec![Pubkey::new_unique(), Pubkey::new_unique()],
        };
        let json = serde_json::to_string(&state).unwrap();
        let parsed: PoolState = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string(&parsed).unwrap();
        assert_eq!(json, json2);
    }

    #[test]
    fn test_pool_state_json_roundtrip_with_numerics() {
        // Test a variant with u64, i32, u16 fields
        let state = PoolState::RaydiumClmm {
            pool: Pubkey::new_unique(),
            amm_config: Pubkey::new_unique(),
            observation: Pubkey::new_unique(),
            token_vault_0: Pubkey::new_unique(),
            token_vault_1: Pubkey::new_unique(),
            tick_array_0: Pubkey::new_unique(),
            tick_array_1: Pubkey::new_unique(),
            tick_array_2: Pubkey::new_unique(),
            token_mint_0: Pubkey::new_unique(),
            token_mint_1: Pubkey::new_unique(),
            tick_current: -42,
            tick_spacing: 10,
            sqrt_price_x64: 1u128 << 64,
            liquidity: 1_000_000,
            fee_rate: 25,
        };
        let json = serde_json::to_string(&state).unwrap();
        let parsed: PoolState = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string(&parsed).unwrap();
        assert_eq!(json, json2);
    }
}
