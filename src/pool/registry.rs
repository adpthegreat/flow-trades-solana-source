use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

use crate::error::{TradeError, TradeResult};

use super::types::PoolType;

/// A known pool entry in the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolEntry {
    pub address: Pubkey,
    pub pool_type: PoolType,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
}

/// Returns a canonical (sorted) pair so (A,B) and (B,A) map to the same key.
fn sorted_pair(a: Pubkey, b: Pubkey) -> (Pubkey, Pubkey) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

/// Concurrent pool registry for looking up pools by token pair.
/// Uses DashMap for lock-free concurrent reads and writes.
pub struct PoolRegistry {
    /// Primary index: pool address -> PoolEntry
    pools: DashMap<Pubkey, PoolEntry>,
    /// Secondary index: sorted (mint_a, mint_b) -> list of pool addresses
    mint_index: DashMap<(Pubkey, Pubkey), Vec<Pubkey>>,
}

impl PoolRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            pools: DashMap::new(),
            mint_index: DashMap::new(),
        }
    }

    /// Create a registry from a list of pool entries.
    pub fn from_entries(entries: Vec<PoolEntry>) -> Self {
        let reg = Self::new();
        for entry in entries {
            reg.add(entry);
        }
        reg
    }

    /// Add a pool entry to the registry. Thread-safe, no &mut self needed.
    pub fn add(&self, entry: PoolEntry) {
        let address = entry.address;
        let pair = sorted_pair(entry.mint_a, entry.mint_b);

        // If replacing an existing entry, remove old mint_index reference first
        if let Some(old) = self.pools.get(&address) {
            let old_pair = sorted_pair(old.mint_a, old.mint_b);
            if old_pair != pair {
                // Mint pair changed — remove from old index
                if let Some(mut addrs) = self.mint_index.get_mut(&old_pair) {
                    addrs.retain(|a| *a != address);
                }
            }
        }

        self.pools.insert(address, entry);

        // Update mint index
        self.mint_index
            .entry(pair)
            .and_modify(|addrs| {
                if !addrs.contains(&address) {
                    addrs.push(address);
                }
            })
            .or_insert_with(|| vec![address]);
    }

    /// Look up all pools that contain both input_mint and output_mint.
    /// Returns owned PoolEntry values (cloned from the DashMap).
    pub fn lookup(&self, input_mint: &Pubkey, output_mint: &Pubkey) -> Vec<PoolEntry> {
        let pair = sorted_pair(*input_mint, *output_mint);
        match self.mint_index.get(&pair) {
            Some(addrs) => addrs
                .iter()
                .filter_map(|addr| self.pools.get(addr).map(|e| e.clone()))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Get a pool entry by address. Returns None if not in the registry.
    pub fn get(&self, address: &Pubkey) -> Option<PoolEntry> {
        self.pools.get(address).map(|e| e.clone())
    }

    /// Remove a pool entry by address.
    pub fn remove(&self, address: &Pubkey) {
        if let Some((_, entry)) = self.pools.remove(address) {
            let pair = sorted_pair(entry.mint_a, entry.mint_b);
            if let Some(mut addrs) = self.mint_index.get_mut(&pair) {
                addrs.retain(|a| a != address);
            }
        }
    }

    /// Collect all entries (for persistence).
    pub fn entries(&self) -> Vec<PoolEntry> {
        self.pools.iter().map(|r| r.value().clone()).collect()
    }

    /// Collect all pool addresses in the registry.
    pub fn addresses(&self) -> Vec<Pubkey> {
        self.pools.iter().map(|r| *r.key()).collect()
    }

    /// Number of pools in the registry.
    pub fn len(&self) -> usize {
        self.pools.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }

    /// Check if a pool address is already in the registry. O(1) via DashMap.
    pub fn contains(&self, address: &Pubkey) -> bool {
        self.pools.contains_key(address)
    }

    /// Load pool entries from a JSON file.
    pub fn load_from_json(path: &str) -> TradeResult<Self> {
        let data = std::fs::read_to_string(path).map_err(|e| {
            TradeError::Internal(format!("failed to read pools file {path}: {e}"))
        })?;
        let entries: Vec<PoolEntry> = serde_json::from_str(&data).map_err(|e| {
            TradeError::Internal(format!("failed to parse pools file {path}: {e}"))
        })?;
        Ok(Self::from_entries(entries))
    }
}

impl Default for PoolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(pool_type: PoolType, mint_a: Pubkey, mint_b: Pubkey) -> PoolEntry {
        PoolEntry {
            address: Pubkey::new_unique(),
            pool_type,
            mint_a,
            mint_b,
        }
    }

    #[test]
    fn test_registry_empty() {
        let reg = PoolRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_registry_add_and_len() {
        let reg = PoolRegistry::new();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        reg.add(make_entry(PoolType::Orca, mint_a, mint_b));
        assert_eq!(reg.len(), 1);
        assert!(!reg.is_empty());
    }

    #[test]
    fn test_registry_lookup_forward() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let entry = make_entry(PoolType::RaydiumCpmm, mint_a, mint_b);
        let reg = PoolRegistry::from_entries(vec![entry]);

        let results = reg.lookup(&mint_a, &mint_b);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].pool_type, PoolType::RaydiumCpmm);
    }

    #[test]
    fn test_registry_lookup_reverse() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let entry = make_entry(PoolType::Orca, mint_a, mint_b);
        let reg = PoolRegistry::from_entries(vec![entry]);

        // Lookup in reverse order should also match
        let results = reg.lookup(&mint_b, &mint_a);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].pool_type, PoolType::Orca);
    }

    #[test]
    fn test_registry_lookup_no_match() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let mint_c = Pubkey::new_unique();
        let entry = make_entry(PoolType::Orca, mint_a, mint_b);
        let reg = PoolRegistry::from_entries(vec![entry]);

        let results = reg.lookup(&mint_a, &mint_c);
        assert!(results.is_empty());
    }

    #[test]
    fn test_registry_lookup_multiple_pools() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let entries = vec![
            make_entry(PoolType::RaydiumCpmm, mint_a, mint_b),
            make_entry(PoolType::Orca, mint_a, mint_b),
            make_entry(PoolType::MeteoraDlmm, mint_a, mint_b),
        ];
        let reg = PoolRegistry::from_entries(entries);

        let results = reg.lookup(&mint_a, &mint_b);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_registry_from_entries() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let entries = vec![make_entry(PoolType::PumpFunAmm, mint_a, mint_b)];
        let reg = PoolRegistry::from_entries(entries);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_registry_default() {
        let reg = PoolRegistry::default();
        assert!(reg.is_empty());
    }

    #[test]
    fn test_registry_remove() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let entry = make_entry(PoolType::Orca, mint_a, mint_b);
        let addr = entry.address;
        let reg = PoolRegistry::from_entries(vec![entry]);

        assert_eq!(reg.len(), 1);
        assert_eq!(reg.lookup(&mint_a, &mint_b).len(), 1);

        reg.remove(&addr);
        assert_eq!(reg.len(), 0);
        assert!(reg.lookup(&mint_a, &mint_b).is_empty());
    }

    #[test]
    fn test_registry_remove_nonexistent() {
        let reg = PoolRegistry::new();
        let fake = Pubkey::new_unique();
        reg.remove(&fake); // should not panic
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_registry_entries_collects_all() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let entries = vec![
            make_entry(PoolType::Orca, mint_a, mint_b),
            make_entry(PoolType::RaydiumCpmm, mint_a, mint_b),
        ];
        let reg = PoolRegistry::from_entries(entries);

        let all = reg.entries();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_registry_concurrent_add() {
        use std::sync::Arc;

        let reg = Arc::new(PoolRegistry::new());
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let reg = Arc::clone(&reg);
                std::thread::spawn(move || {
                    reg.add(make_entry(PoolType::Orca, mint_a, mint_b));
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(reg.len(), 10);
        assert_eq!(reg.lookup(&mint_a, &mint_b).len(), 10);
    }

    #[test]
    fn test_registry_addresses() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let entry1 = make_entry(PoolType::Orca, mint_a, mint_b);
        let entry2 = make_entry(PoolType::RaydiumCpmm, mint_a, mint_b);
        let addr1 = entry1.address;
        let addr2 = entry2.address;
        let reg = PoolRegistry::from_entries(vec![entry1, entry2]);

        let addresses = reg.addresses();
        assert_eq!(addresses.len(), 2);
        let set: std::collections::HashSet<Pubkey> = addresses.into_iter().collect();
        assert!(set.contains(&addr1));
        assert!(set.contains(&addr2));
    }

    #[test]
    fn test_registry_addresses_empty() {
        let reg = PoolRegistry::new();
        assert!(reg.addresses().is_empty());
    }

    #[test]
    fn test_registry_from_entries_roundtrip() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let mint_c = Pubkey::new_unique();
        let entries = vec![
            make_entry(PoolType::Orca, mint_a, mint_b),
            make_entry(PoolType::RaydiumCpmm, mint_b, mint_c),
            make_entry(PoolType::Meteora, mint_a, mint_c),
        ];
        let reg = PoolRegistry::from_entries(entries.clone());

        let collected = reg.entries();
        assert_eq!(collected.len(), 3);

        // Rebuild from collected entries
        let reg2 = PoolRegistry::from_entries(collected);
        assert_eq!(reg2.len(), 3);
        assert_eq!(reg2.lookup(&mint_a, &mint_b).len(), 1);
        assert_eq!(reg2.lookup(&mint_b, &mint_c).len(), 1);
        assert_eq!(reg2.lookup(&mint_a, &mint_c).len(), 1);
    }

    #[test]
    fn test_registry_load_from_json_with_temp_file() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let entries = vec![PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            mint_a,
            mint_b,
        }];

        let json = serde_json::to_string_pretty(&entries).unwrap();
        let path = "/tmp/flow_trades_test_registry.json";
        std::fs::write(path, &json).unwrap();

        let reg = PoolRegistry::load_from_json(path).unwrap();
        assert_eq!(reg.len(), 1);
        let found = reg.lookup(&mint_a, &mint_b);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].pool_type, PoolType::RaydiumCpmm);

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_registry_load_from_json_nonexistent() {
        let result = PoolRegistry::load_from_json("/nonexistent/path.json");
        assert!(result.is_err());
    }

    #[test]
    fn test_sorted_pair_canonical() {
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let (x1, y1) = sorted_pair(a, b);
        let (x2, y2) = sorted_pair(b, a);
        assert_eq!(x1, x2);
        assert_eq!(y1, y2);
    }

    #[test]
    fn test_registry_get_by_address() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let entry = make_entry(PoolType::Orca, mint_a, mint_b);
        let addr = entry.address;
        let reg = PoolRegistry::from_entries(vec![entry]);

        let found = reg.get(&addr);
        assert!(found.is_some());
        assert_eq!(found.unwrap().pool_type, PoolType::Orca);

        let not_found = reg.get(&Pubkey::new_unique());
        assert!(not_found.is_none());
    }

    #[test]
    fn test_pool_entry_serde_roundtrip() {
        let entry = PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::PumpFunAmm,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: PoolEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.address, entry.address);
        assert_eq!(parsed.pool_type, entry.pool_type);
        assert_eq!(parsed.mint_a, entry.mint_a);
        assert_eq!(parsed.mint_b, entry.mint_b);
    }
}
