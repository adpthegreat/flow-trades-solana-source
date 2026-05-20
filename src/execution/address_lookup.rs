use dashmap::DashMap;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

use crate::error::{TradeError, TradeResult};

/// Metadata size for on-chain Address Lookup Table accounts (56 bytes).
const LOOKUP_TABLE_META_SIZE: usize = 56;

/// Cache of resolved Address Lookup Tables.
/// Maps ALT address -> resolved account data (key + list of addresses).
pub struct AltCache {
    tables: DashMap<Pubkey, AddressLookupTableAccount>,
}

impl AltCache {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }

    /// Load ALTs from on-chain by fetching their account data.
    /// Returns the number of successfully loaded tables.
    pub async fn load_alts(&self, rpc: &RpcClient, alt_addresses: &[Pubkey]) -> usize {
        let mut loaded = 0;
        for addr in alt_addresses {
            match Self::fetch_alt(rpc, addr).await {
                Ok(alt) => {
                    tracing::info!(
                        alt = %addr,
                        addresses = alt.addresses.len(),
                        "loaded ALT"
                    );
                    self.tables.insert(*addr, alt);
                    loaded += 1;
                }
                Err(e) => {
                    tracing::warn!(alt = %addr, error = %e, "failed to load ALT");
                }
            }
        }
        loaded
    }

    /// Fetch and deserialize a single ALT from on-chain.
    async fn fetch_alt(
        rpc: &RpcClient,
        address: &Pubkey,
    ) -> TradeResult<AddressLookupTableAccount> {
        let account = rpc
            .get_account(address)
            .await
            .map_err(|e| TradeError::Rpc(format!("fetch ALT {address}: {e}")))?;

        let addresses = parse_alt_addresses(&account.data)?;

        Ok(AddressLookupTableAccount {
            key: *address,
            addresses,
        })
    }

    /// Get all loaded ALTs as a Vec (needed for v0 message creation).
    pub fn all_tables(&self) -> Vec<AddressLookupTableAccount> {
        self.tables
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get the list of loaded ALT addresses (for /swap-instructions response).
    pub fn alt_addresses(&self) -> Vec<Pubkey> {
        self.tables.iter().map(|entry| *entry.key()).collect()
    }

    /// Check if any ALTs are loaded.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Number of loaded ALTs.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Total number of addresses across all loaded ALTs.
    pub fn total_addresses(&self) -> usize {
        self.tables
            .iter()
            .map(|entry| entry.value().addresses.len())
            .sum()
    }

    /// Spawn a background task that periodically refreshes all ALTs.
    pub fn spawn_refresh(
        self: Arc<Self>,
        rpc: Arc<RpcClient>,
        alt_addresses: Vec<Pubkey>,
        interval: std::time::Duration,
    ) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let loaded = self.load_alts(&rpc, &alt_addresses).await;
                tracing::debug!(loaded, total = alt_addresses.len(), "ALT refresh complete");
            }
        });
    }
}

/// Parse address list from raw ALT account data.
/// The on-chain format is: 56 bytes of metadata, then N * 32 bytes of pubkeys.
pub fn parse_alt_addresses(data: &[u8]) -> TradeResult<Vec<Pubkey>> {
    if data.len() < LOOKUP_TABLE_META_SIZE {
        return Err(TradeError::Execution(format!(
            "ALT account data too short: {} bytes (need at least {})",
            data.len(),
            LOOKUP_TABLE_META_SIZE
        )));
    }

    let address_data = &data[LOOKUP_TABLE_META_SIZE..];
    if address_data.len() % 32 != 0 {
        return Err(TradeError::Execution(format!(
            "ALT address data not aligned: {} bytes (must be multiple of 32)",
            address_data.len()
        )));
    }

    let num_addresses = address_data.len() / 32;
    let mut addresses = Vec::with_capacity(num_addresses);

    for i in 0..num_addresses {
        let offset = i * 32;
        let bytes: [u8; 32] = address_data[offset..offset + 32]
            .try_into()
            .map_err(|_| TradeError::Execution("ALT address slice conversion failed".into()))?;
        addresses.push(Pubkey::new_from_array(bytes));
    }

    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alt_cache_new_is_empty() {
        let cache = AltCache::new();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_addresses(), 0);
        assert!(cache.all_tables().is_empty());
        assert!(cache.alt_addresses().is_empty());
    }

    #[test]
    fn test_alt_cache_insert_and_retrieve() {
        let cache = AltCache::new();
        let alt_key = Pubkey::new_unique();
        let addr1 = Pubkey::new_unique();
        let addr2 = Pubkey::new_unique();

        cache.tables.insert(
            alt_key,
            AddressLookupTableAccount {
                key: alt_key,
                addresses: vec![addr1, addr2],
            },
        );

        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.total_addresses(), 2);

        let tables = cache.all_tables();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].key, alt_key);
        assert_eq!(tables[0].addresses.len(), 2);
    }

    #[test]
    fn test_alt_cache_multiple_tables() {
        let cache = AltCache::new();
        let alt_key1 = Pubkey::new_unique();
        let alt_key2 = Pubkey::new_unique();

        cache.tables.insert(
            alt_key1,
            AddressLookupTableAccount {
                key: alt_key1,
                addresses: vec![Pubkey::new_unique(); 3],
            },
        );
        cache.tables.insert(
            alt_key2,
            AddressLookupTableAccount {
                key: alt_key2,
                addresses: vec![Pubkey::new_unique(); 5],
            },
        );

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.total_addresses(), 8);
        assert_eq!(cache.alt_addresses().len(), 2);
    }

    #[test]
    fn test_parse_alt_addresses_empty_table() {
        // 56 bytes of metadata, 0 addresses
        let data = vec![0u8; LOOKUP_TABLE_META_SIZE];
        let addresses = parse_alt_addresses(&data).unwrap();
        assert!(addresses.is_empty());
    }

    #[test]
    fn test_parse_alt_addresses_single_address() {
        let mut data = vec![0u8; LOOKUP_TABLE_META_SIZE + 32];
        // Write a known pubkey at offset 56
        let pubkey = Pubkey::new_unique();
        data[LOOKUP_TABLE_META_SIZE..LOOKUP_TABLE_META_SIZE + 32]
            .copy_from_slice(pubkey.as_ref());

        let addresses = parse_alt_addresses(&data).unwrap();
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], pubkey);
    }

    #[test]
    fn test_parse_alt_addresses_multiple_addresses() {
        let keys: Vec<Pubkey> = (0..10).map(|_| Pubkey::new_unique()).collect();
        let mut data = vec![0u8; LOOKUP_TABLE_META_SIZE + 32 * keys.len()];
        for (i, key) in keys.iter().enumerate() {
            let offset = LOOKUP_TABLE_META_SIZE + i * 32;
            data[offset..offset + 32].copy_from_slice(key.as_ref());
        }

        let addresses = parse_alt_addresses(&data).unwrap();
        assert_eq!(addresses.len(), 10);
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(addresses[i], *key);
        }
    }

    #[test]
    fn test_parse_alt_addresses_too_short() {
        let data = vec![0u8; 10]; // less than LOOKUP_TABLE_META_SIZE
        let result = parse_alt_addresses(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_parse_alt_addresses_unaligned() {
        // 56 + 15 bytes = not aligned to 32
        let data = vec![0u8; LOOKUP_TABLE_META_SIZE + 15];
        let result = parse_alt_addresses(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not aligned"));
    }

    #[test]
    fn test_parse_alt_addresses_max_256() {
        // ALTs can hold up to 256 addresses
        let keys: Vec<Pubkey> = (0..256).map(|_| Pubkey::new_unique()).collect();
        let mut data = vec![0u8; LOOKUP_TABLE_META_SIZE + 32 * keys.len()];
        for (i, key) in keys.iter().enumerate() {
            let offset = LOOKUP_TABLE_META_SIZE + i * 32;
            data[offset..offset + 32].copy_from_slice(key.as_ref());
        }

        let addresses = parse_alt_addresses(&data).unwrap();
        assert_eq!(addresses.len(), 256);
    }
}
