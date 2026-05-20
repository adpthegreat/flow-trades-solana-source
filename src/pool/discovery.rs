//! Pool auto-discovery via RPC getProgramAccounts and block scanner.
//!
//! Discovery sources:
//! - **Block scanner** (primary): Discovers new pools from live block data (~24/min).
//! - **RPC scan**: `getProgramAccounts` with data-size filter per DEX program (slow, ~30-60s).

use std::str::FromStr;

use solana_sdk::pubkey::Pubkey;
use tracing::{info, warn};

use crate::constants::*;
use crate::pool::registry::PoolEntry;
use crate::pool::types::PoolType;

use super::registry::PoolRegistry;

/// Discovery mode for pool auto-discovery at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryMode {
    /// No automatic discovery — manual pools-file only.
    None,
    /// Block scanner enabled, auto-discover from live blocks (default).
    Auto,
}

impl DiscoveryMode {
    pub fn from_str_config(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "auto" => DiscoveryMode::Auto,
            _ => DiscoveryMode::None,
        }
    }
}

impl std::fmt::Display for DiscoveryMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiscoveryMode::None => write!(f, "none"),
            DiscoveryMode::Auto => write!(f, "auto"),
        }
    }
}

/// Parse a single JSON row into a PoolEntry.
/// Expected fields: pool_address, pool_type, mint_a, mint_b.
fn parse_pool_row(row: &serde_json::Value) -> Option<PoolEntry> {
    let addr_str = row.get("pool_address").and_then(|v| v.as_str()).unwrap_or("");
    let pt_str = row.get("pool_type").and_then(|v| v.as_str()).unwrap_or("");
    let ma_str = row.get("mint_a").and_then(|v| v.as_str()).unwrap_or("");
    let mb_str = row.get("mint_b").and_then(|v| v.as_str()).unwrap_or("");

    let address = Pubkey::from_str(addr_str).ok()?;
    let pool_type = PoolType::from_str_opt(pt_str)?;
    let mint_a = Pubkey::from_str(ma_str).ok()?;
    let mint_b = Pubkey::from_str(mb_str).ok()?;

    Some(PoolEntry {
        address,
        pool_type,
        mint_a,
        mint_b,
    })
}

/// Parse a JSON array of pool entries.
/// Expected format: `[ { "pool_address": "...", "pool_type": "...", "mint_a": "...", "mint_b": "..." }, ... ]`
pub fn parse_pool_json_array(rows: &[serde_json::Value]) -> Vec<PoolEntry> {
    rows.iter().filter_map(parse_pool_row).collect()
}

/// Known data sizes per DEX program for getProgramAccounts filtering.
/// Returns (program_id, pool_type, data_size).
pub fn rpc_scan_programs() -> Vec<(Pubkey, PoolType, usize)> {
    vec![
        (RAYDIUM_CPMM_PROG_ID, PoolType::RaydiumCpmm, 637),
        (PUMP_FUN_AMM_PROG_ID, PoolType::PumpFunAmm, 211),
        // Pumpup AMM Pool account: 261 bytes (verified vs mainnet pool
        // 7Q9RYYbijphbAXBV527Jz2QmgY4BXdaAzfXhJ3wT8hv1).
        (PUMPUP_PROG_ID, PoolType::Pumpup, 261),
        // More programs can be added as their data sizes are confirmed.
        // Orca whirlpools: 653 bytes
        // Meteora DAMM: variable, needs memcmp filter
    ]
}

/// Extract (mint_a, mint_b) from raw account data using known byte offsets.
/// Each DEX has different layouts — offsets are from pool_fetcher.rs.
///
/// Returns None if the data is too short or pubkeys can't be read.
pub fn extract_mints_from_data(pool_type: PoolType, data: &[u8]) -> Option<(Pubkey, Pubkey)> {
    match pool_type {
        PoolType::RaydiumCpmm => {
            // After 8-byte discriminator:
            // token_0_mint at offset 72, token_1_mint at offset 104
            if data.len() < 136 {
                return None;
            }
            let mint_a = Pubkey::try_from(&data[72..104]).ok()?;
            let mint_b = Pubkey::try_from(&data[104..136]).ok()?;
            Some((mint_a, mint_b))
        }
        PoolType::PumpFunAmm => {
            // pool(32) + lp_mint(32) + base_mint at offset 72, quote_mint at offset 104
            // Actually: pool_bumps(2) + pool_status(1) + lp_mint(32) = 35...
            // From pool_fetcher.rs: base_mint at offset 72, quote_mint at offset 104
            if data.len() < 136 {
                return None;
            }
            let mint_a = Pubkey::try_from(&data[72..104]).ok()?;
            let mint_b = Pubkey::try_from(&data[104..136]).ok()?;
            Some((mint_a, mint_b))
        }
        PoolType::Pumpup => {
            // After 8-byte Anchor disc:
            // token_a_mint at offset 8, token_b_mint at offset 40 (per IDL).
            if data.len() < 72 {
                return None;
            }
            let mint_a = Pubkey::try_from(&data[8..40]).ok()?;
            let mint_b = Pubkey::try_from(&data[40..72]).ok()?;
            Some((mint_a, mint_b))
        }
        _ => None,
    }
}

/// Discover pools via RPC getProgramAccounts for known DEX programs.
/// This is slow (30-60s per program) but doesn't require external dependencies.
/// Returns the number of pools added to the registry.
pub async fn discover_via_rpc(
    rpc: &solana_client::nonblocking::rpc_client::RpcClient,
    registry: &PoolRegistry,
) -> usize {
    use solana_account_decoder::UiAccountEncoding;
    use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
    use solana_client::rpc_filter::RpcFilterType;

    let programs = rpc_scan_programs();
    let mut total = 0;

    for (prog_id, pool_type, data_size) in &programs {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(*data_size as u64)]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                ..Default::default()
            },
            ..Default::default()
        };

        match rpc
            .get_program_accounts_with_config(prog_id, config)
            .await
        {
            Ok(accounts) => {
                let mut count = 0;
                for (address, account) in &accounts {
                    if let Some((mint_a, mint_b)) =
                        extract_mints_from_data(*pool_type, &account.data)
                    {
                        registry.add(PoolEntry {
                            address: *address,
                            pool_type: *pool_type,
                            mint_a,
                            mint_b,
                        });
                        count += 1;
                    }
                }
                info!(
                    program = %prog_id,
                    pool_type = %pool_type,
                    discovered = count,
                    total_accounts = accounts.len(),
                    "RPC pool discovery"
                );
                total += count;
            }
            Err(e) => {
                warn!(
                    program = %prog_id,
                    pool_type = %pool_type,
                    error = %e,
                    "RPC pool discovery failed"
                );
            }
        }
    }

    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_mode_from_str() {
        assert_eq!(
            DiscoveryMode::from_str_config("none"),
            DiscoveryMode::None
        );
        assert_eq!(
            DiscoveryMode::from_str_config("auto"),
            DiscoveryMode::Auto
        );
        assert_eq!(
            DiscoveryMode::from_str_config("invalid"),
            DiscoveryMode::None
        );
        assert_eq!(
            DiscoveryMode::from_str_config(""),
            DiscoveryMode::None
        );
    }

    #[test]
    fn test_discovery_mode_display() {
        assert_eq!(DiscoveryMode::None.to_string(), "none");
        assert_eq!(DiscoveryMode::Auto.to_string(), "auto");
    }

    #[test]
    fn test_discovery_mode_case_insensitive() {
        assert_eq!(
            DiscoveryMode::from_str_config("Auto"),
            DiscoveryMode::Auto
        );
        assert_eq!(
            DiscoveryMode::from_str_config("AUTO"),
            DiscoveryMode::Auto
        );
        assert_eq!(
            DiscoveryMode::from_str_config("NONE"),
            DiscoveryMode::None
        );
    }

    #[test]
    fn test_parse_pool_json_array_valid() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let pool_addr = Pubkey::new_unique();

        let rows = vec![serde_json::json!({
            "pool_address": pool_addr.to_string(),
            "pool_type": "RAYDIUM_CPMM",
            "mint_a": mint_a.to_string(),
            "mint_b": mint_b.to_string(),
        })];

        let entries = parse_pool_json_array(&rows);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].address, pool_addr);
        assert_eq!(entries[0].pool_type, PoolType::RaydiumCpmm);
        assert_eq!(entries[0].mint_a, mint_a);
        assert_eq!(entries[0].mint_b, mint_b);
    }

    #[test]
    fn test_parse_pool_json_array_multiple() {
        let rows = vec![
            serde_json::json!({
                "pool_address": Pubkey::new_unique().to_string(),
                "pool_type": "ORCA",
                "mint_a": Pubkey::new_unique().to_string(),
                "mint_b": Pubkey::new_unique().to_string(),
            }),
            serde_json::json!({
                "pool_address": Pubkey::new_unique().to_string(),
                "pool_type": "PUMP_FUN_AMM",
                "mint_a": Pubkey::new_unique().to_string(),
                "mint_b": Pubkey::new_unique().to_string(),
            }),
        ];

        let entries = parse_pool_json_array(&rows);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].pool_type, PoolType::Orca);
        assert_eq!(entries[1].pool_type, PoolType::PumpFunAmm);
    }

    #[test]
    fn test_parse_pool_json_array_empty() {
        assert!(parse_pool_json_array(&[]).is_empty());
    }

    #[test]
    fn test_parse_pool_json_array_invalid_address() {
        let rows = vec![serde_json::json!({
            "pool_address": "not-a-pubkey",
            "pool_type": "ORCA",
            "mint_a": Pubkey::new_unique().to_string(),
            "mint_b": Pubkey::new_unique().to_string(),
        })];

        let entries = parse_pool_json_array(&rows);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_parse_pool_json_array_invalid_pool_type() {
        let rows = vec![serde_json::json!({
            "pool_address": Pubkey::new_unique().to_string(),
            "pool_type": "INVALID_DEX",
            "mint_a": Pubkey::new_unique().to_string(),
            "mint_b": Pubkey::new_unique().to_string(),
        })];

        let entries = parse_pool_json_array(&rows);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_parse_pool_json_array_invalid_mint() {
        let rows = vec![serde_json::json!({
            "pool_address": Pubkey::new_unique().to_string(),
            "pool_type": "ORCA",
            "mint_a": "bad-mint",
            "mint_b": Pubkey::new_unique().to_string(),
        })];

        let entries = parse_pool_json_array(&rows);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_parse_pool_json_array_mixed_valid_invalid() {
        let rows = vec![
            serde_json::json!({
                "pool_address": "invalid",
                "pool_type": "ORCA",
                "mint_a": Pubkey::new_unique().to_string(),
                "mint_b": Pubkey::new_unique().to_string(),
            }),
            serde_json::json!({
                "pool_address": Pubkey::new_unique().to_string(),
                "pool_type": "METEORA",
                "mint_a": Pubkey::new_unique().to_string(),
                "mint_b": Pubkey::new_unique().to_string(),
            }),
        ];

        let entries = parse_pool_json_array(&rows);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].pool_type, PoolType::Meteora);
    }

    #[test]
    fn test_parse_pool_json_array_dedup_via_registry() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let pool_addr = Pubkey::new_unique();

        let rows = vec![
            serde_json::json!({
                "pool_address": pool_addr.to_string(),
                "pool_type": "ORCA",
                "mint_a": mint_a.to_string(),
                "mint_b": mint_b.to_string(),
            }),
            serde_json::json!({
                "pool_address": pool_addr.to_string(),
                "pool_type": "ORCA",
                "mint_a": mint_a.to_string(),
                "mint_b": mint_b.to_string(),
            }),
        ];

        let entries = parse_pool_json_array(&rows);
        // parse returns both — dedup happens in registry.add()
        assert_eq!(entries.len(), 2);

        let registry = PoolRegistry::new();
        for e in entries {
            registry.add(e);
        }
        // Registry deduplicates by address
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_parse_pool_json_array_missing_fields() {
        // Row with missing mint_b
        let rows = vec![serde_json::json!({
            "pool_address": Pubkey::new_unique().to_string(),
            "pool_type": "ORCA",
            "mint_a": Pubkey::new_unique().to_string()
            // missing mint_b
        })];

        let entries = parse_pool_json_array(&rows);
        // mint_b is empty string, which won't parse as Pubkey
        assert!(entries.is_empty());
    }

    #[test]
    fn test_rpc_scan_programs_non_empty() {
        let programs = rpc_scan_programs();
        assert!(!programs.is_empty());
        // Should include at least RaydiumCpmm
        assert!(programs
            .iter()
            .any(|(_, pt, _)| *pt == PoolType::RaydiumCpmm));
    }

    #[test]
    fn test_extract_mints_raydium_cpmm() {
        // Build fake account data with mints at offsets 72 and 104
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let mut data = vec![0u8; 637];
        data[72..104].copy_from_slice(mint_a.as_ref());
        data[104..136].copy_from_slice(mint_b.as_ref());

        let result = extract_mints_from_data(PoolType::RaydiumCpmm, &data);
        assert!(result.is_some());
        let (ma, mb) = result.unwrap();
        assert_eq!(ma, mint_a);
        assert_eq!(mb, mint_b);
    }

    #[test]
    fn test_extract_mints_pumpfun_amm() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let mut data = vec![0u8; 211];
        data[72..104].copy_from_slice(mint_a.as_ref());
        data[104..136].copy_from_slice(mint_b.as_ref());

        let result = extract_mints_from_data(PoolType::PumpFunAmm, &data);
        assert!(result.is_some());
        let (ma, mb) = result.unwrap();
        assert_eq!(ma, mint_a);
        assert_eq!(mb, mint_b);
    }

    #[test]
    fn test_extract_mints_data_too_short() {
        let data = vec![0u8; 100]; // Too short
        assert!(extract_mints_from_data(PoolType::RaydiumCpmm, &data).is_none());
        assert!(extract_mints_from_data(PoolType::PumpFunAmm, &data).is_none());
    }

    #[test]
    fn test_extract_mints_unsupported_pool_type() {
        let data = vec![0u8; 700];
        assert!(extract_mints_from_data(PoolType::Orca, &data).is_none());
        assert!(extract_mints_from_data(PoolType::MeteoraDlmm, &data).is_none());
    }
}
