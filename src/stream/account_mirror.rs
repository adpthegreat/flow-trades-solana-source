//! Local account mirror for eliminating RPC dependency.
//!
//! Stores two categories of data:
//! 1. **Static companion bytes** — fetched once on pool discovery, cached forever.
//!    Used by sync companion parsers to build PoolState for the 5 async pool types.
//! 2. **Dynamic vault balances** — streamed via Geyser in real-time.
//!    Used by the quote engine instead of RPC `get_token_account_balance()`.

use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;

/// SPL Token account data: amount is u64 LE at byte offset 64.
/// Layout: mint(32) + owner(32) + amount(8) + ...
const TOKEN_AMOUNT_OFFSET: usize = 64;

/// Local mirror of account data needed for pool state and quoting.
pub struct AccountMirror {
    /// Static companion account data — fetched once on pool discovery.
    /// Key: companion account pubkey. Value: raw account bytes.
    /// Used by sync companion parsers (RaydiumV4 serum market, Meteora vaults,
    /// MeteoraDbc config, PumpFun global config).
    companion_bytes: DashMap<Pubkey, Vec<u8>>,

    /// Dynamic vault token balances — updated from Geyser in real-time.
    /// Key: vault token account pubkey. Value: token balance (u64).
    vault_balances: DashMap<Pubkey, u64>,

    /// Reverse index: vault pubkey → pool pubkeys that depend on it.
    /// Multiple pools can share a vault (rare but possible).
    vault_to_pools: DashMap<Pubkey, Vec<Pubkey>>,
}

impl AccountMirror {
    pub fn new() -> Self {
        Self {
            companion_bytes: DashMap::new(),
            vault_balances: DashMap::new(),
            vault_to_pools: DashMap::new(),
        }
    }

    // ── Companion bytes (static) ──

    /// Store static companion account data. Called once on pool discovery.
    pub fn insert_companion(&self, address: Pubkey, data: Vec<u8>) {
        self.companion_bytes.insert(address, data);
    }

    /// Retrieve companion account bytes. Returns None if not yet fetched.
    pub fn get_companion(&self, address: &Pubkey) -> Option<Vec<u8>> {
        self.companion_bytes.get(address).map(|r| r.clone())
    }

    /// Check if companion data is cached.
    pub fn has_companion(&self, address: &Pubkey) -> bool {
        self.companion_bytes.contains_key(address)
    }

    /// Number of cached companion accounts.
    pub fn companion_count(&self) -> usize {
        self.companion_bytes.len()
    }

    /// All companion pubkeys we're tracking (for building Geyser subscription).
    pub fn all_companion_pubkeys(&self) -> Vec<Pubkey> {
        self.companion_bytes.iter().map(|r| *r.key()).collect()
    }

    // ── Vault balances (dynamic) ──

    /// Update a vault token balance from Geyser account data.
    /// Parses the SPL token amount from raw account bytes.
    /// Returns the parsed balance, or None if data is too short.
    pub fn update_vault_from_bytes(&self, vault: Pubkey, data: &[u8]) -> Option<u64> {
        let balance = parse_token_balance(data)?;
        self.vault_balances.insert(vault, balance);
        Some(balance)
    }

    /// Update a vault token balance directly (e.g., from RPC fallback).
    pub fn update_vault_balance(&self, vault: Pubkey, balance: u64) {
        self.vault_balances.insert(vault, balance);
    }

    /// Get the cached balance for a vault token account.
    pub fn get_vault_balance(&self, vault: &Pubkey) -> Option<u64> {
        self.vault_balances.get(vault).map(|r| *r)
    }

    /// Number of tracked vault balances.
    pub fn vault_balance_count(&self) -> usize {
        self.vault_balances.len()
    }

    // ── Vault ↔ Pool reverse index ──

    /// Register a vault as belonging to a pool. Builds the reverse index
    /// so we can invalidate pool cache when vault balances change.
    pub fn register_vault(&self, vault: Pubkey, pool: Pubkey) {
        self.vault_to_pools
            .entry(vault)
            .and_modify(|pools| {
                if !pools.contains(&pool) {
                    pools.push(pool);
                }
            })
            .or_insert_with(|| vec![pool]);
    }

    /// Look up which pools depend on a given vault.
    pub fn pools_for_vault(&self, vault: &Pubkey) -> Vec<Pubkey> {
        self.vault_to_pools
            .get(vault)
            .map(|r| r.clone())
            .unwrap_or_default()
    }

    /// Check if a pubkey is a registered vault (for Geyser dispatch).
    pub fn is_vault(&self, address: &Pubkey) -> bool {
        self.vault_to_pools.contains_key(address)
    }

    /// All vault pubkeys we're tracking (for building Geyser subscription).
    pub fn all_vault_pubkeys(&self) -> Vec<Pubkey> {
        self.vault_to_pools.iter().map(|r| *r.key()).collect()
    }

    /// Number of tracked vaults.
    pub fn vault_count(&self) -> usize {
        self.vault_to_pools.len()
    }
}

impl Default for AccountMirror {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse SPL token balance from raw account bytes.
/// Token account layout: mint(32) + owner(32) + amount(8 LE) + ...
/// Returns None if data is too short.
pub fn parse_token_balance(data: &[u8]) -> Option<u64> {
    if data.len() < TOKEN_AMOUNT_OFFSET + 8 {
        return None;
    }
    let bytes: [u8; 8] = data[TOKEN_AMOUNT_OFFSET..TOKEN_AMOUNT_OFFSET + 8]
        .try_into()
        .ok()?;
    Some(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_mirror() -> AccountMirror {
        AccountMirror::new()
    }

    // ── Companion tests ──

    #[test]
    fn test_companion_insert_and_get() {
        let mirror = make_mirror();
        let addr = Pubkey::new_unique();
        let data = vec![1, 2, 3, 4];
        mirror.insert_companion(addr, data.clone());
        assert_eq!(mirror.get_companion(&addr), Some(data));
    }

    #[test]
    fn test_companion_missing() {
        let mirror = make_mirror();
        assert_eq!(mirror.get_companion(&Pubkey::new_unique()), None);
    }

    #[test]
    fn test_companion_has() {
        let mirror = make_mirror();
        let addr = Pubkey::new_unique();
        assert!(!mirror.has_companion(&addr));
        mirror.insert_companion(addr, vec![0]);
        assert!(mirror.has_companion(&addr));
    }

    #[test]
    fn test_companion_count() {
        let mirror = make_mirror();
        assert_eq!(mirror.companion_count(), 0);
        mirror.insert_companion(Pubkey::new_unique(), vec![]);
        mirror.insert_companion(Pubkey::new_unique(), vec![]);
        assert_eq!(mirror.companion_count(), 2);
    }

    #[test]
    fn test_companion_overwrite() {
        let mirror = make_mirror();
        let addr = Pubkey::new_unique();
        mirror.insert_companion(addr, vec![1]);
        mirror.insert_companion(addr, vec![2, 3]);
        assert_eq!(mirror.get_companion(&addr), Some(vec![2, 3]));
        assert_eq!(mirror.companion_count(), 1);
    }

    // ── Vault balance tests ──

    #[test]
    fn test_vault_balance_update_and_get() {
        let mirror = make_mirror();
        let vault = Pubkey::new_unique();
        mirror.update_vault_balance(vault, 1_000_000);
        assert_eq!(mirror.get_vault_balance(&vault), Some(1_000_000));
    }

    #[test]
    fn test_vault_balance_missing() {
        let mirror = make_mirror();
        assert_eq!(mirror.get_vault_balance(&Pubkey::new_unique()), None);
    }

    #[test]
    fn test_vault_balance_from_bytes() {
        let mirror = make_mirror();
        let vault = Pubkey::new_unique();
        // Build a minimal SPL token account: 72 bytes minimum (mint + owner + amount)
        let mut data = vec![0u8; 165]; // standard token account size
        let amount: u64 = 123_456_789;
        data[64..72].copy_from_slice(&amount.to_le_bytes());
        let result = mirror.update_vault_from_bytes(vault, &data);
        assert_eq!(result, Some(123_456_789));
        assert_eq!(mirror.get_vault_balance(&vault), Some(123_456_789));
    }

    #[test]
    fn test_vault_balance_from_bytes_too_short() {
        let mirror = make_mirror();
        let result = mirror.update_vault_from_bytes(Pubkey::new_unique(), &[0u8; 50]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_vault_balance_count() {
        let mirror = make_mirror();
        assert_eq!(mirror.vault_balance_count(), 0);
        mirror.update_vault_balance(Pubkey::new_unique(), 100);
        mirror.update_vault_balance(Pubkey::new_unique(), 200);
        assert_eq!(mirror.vault_balance_count(), 2);
    }

    // ── Reverse index tests ──

    #[test]
    fn test_register_vault_single_pool() {
        let mirror = make_mirror();
        let vault = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        mirror.register_vault(vault, pool);
        assert_eq!(mirror.pools_for_vault(&vault), vec![pool]);
    }

    #[test]
    fn test_register_vault_multiple_pools() {
        let mirror = make_mirror();
        let vault = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        mirror.register_vault(vault, pool_a);
        mirror.register_vault(vault, pool_b);
        let pools = mirror.pools_for_vault(&vault);
        assert_eq!(pools.len(), 2);
        assert!(pools.contains(&pool_a));
        assert!(pools.contains(&pool_b));
    }

    #[test]
    fn test_register_vault_dedup() {
        let mirror = make_mirror();
        let vault = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        mirror.register_vault(vault, pool);
        mirror.register_vault(vault, pool); // duplicate
        assert_eq!(mirror.pools_for_vault(&vault), vec![pool]);
    }

    #[test]
    fn test_is_vault() {
        let mirror = make_mirror();
        let vault = Pubkey::new_unique();
        assert!(!mirror.is_vault(&vault));
        mirror.register_vault(vault, Pubkey::new_unique());
        assert!(mirror.is_vault(&vault));
    }

    #[test]
    fn test_all_vault_pubkeys() {
        let mirror = make_mirror();
        let v1 = Pubkey::new_unique();
        let v2 = Pubkey::new_unique();
        mirror.register_vault(v1, Pubkey::new_unique());
        mirror.register_vault(v2, Pubkey::new_unique());
        let all = mirror.all_vault_pubkeys();
        assert_eq!(all.len(), 2);
        assert!(all.contains(&v1));
        assert!(all.contains(&v2));
    }

    #[test]
    fn test_vault_count() {
        let mirror = make_mirror();
        assert_eq!(mirror.vault_count(), 0);
        mirror.register_vault(Pubkey::new_unique(), Pubkey::new_unique());
        mirror.register_vault(Pubkey::new_unique(), Pubkey::new_unique());
        assert_eq!(mirror.vault_count(), 2);
    }

    #[test]
    fn test_pools_for_unknown_vault() {
        let mirror = make_mirror();
        assert!(mirror.pools_for_vault(&Pubkey::new_unique()).is_empty());
    }

    // ── parse_token_balance tests ──

    #[test]
    fn test_parse_token_balance_valid() {
        let mut data = vec![0u8; 165];
        let amount: u64 = 999_999_999_999;
        data[64..72].copy_from_slice(&amount.to_le_bytes());
        assert_eq!(parse_token_balance(&data), Some(999_999_999_999));
    }

    #[test]
    fn test_parse_token_balance_zero() {
        let data = vec![0u8; 165];
        assert_eq!(parse_token_balance(&data), Some(0));
    }

    #[test]
    fn test_parse_token_balance_too_short() {
        assert_eq!(parse_token_balance(&[0u8; 71]), None);
    }

    #[test]
    fn test_parse_token_balance_exact_minimum() {
        let mut data = vec![0u8; 72]; // exactly mint(32) + owner(32) + amount(8)
        data[64..72].copy_from_slice(&42u64.to_le_bytes());
        assert_eq!(parse_token_balance(&data), Some(42));
    }

    #[test]
    fn test_parse_token_balance_max() {
        let mut data = vec![0u8; 165];
        data[64..72].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(parse_token_balance(&data), Some(u64::MAX));
    }

    #[test]
    fn test_all_companion_pubkeys() {
        let mirror = make_mirror();
        let c1 = Pubkey::new_unique();
        let c2 = Pubkey::new_unique();
        mirror.insert_companion(c1, vec![1]);
        mirror.insert_companion(c2, vec![2]);
        let all = mirror.all_companion_pubkeys();
        assert_eq!(all.len(), 2);
        assert!(all.contains(&c1));
        assert!(all.contains(&c2));
    }

    #[test]
    fn test_all_companion_pubkeys_empty() {
        let mirror = make_mirror();
        assert!(mirror.all_companion_pubkeys().is_empty());
    }

    #[test]
    fn test_companion_update_via_geyser() {
        // Simulates: companion fetched via RPC, then updated via Geyser stream
        let mirror = make_mirror();
        let addr = Pubkey::new_unique();
        mirror.insert_companion(addr, vec![1, 2, 3]); // initial RPC fetch
        assert!(mirror.has_companion(&addr));
        mirror.insert_companion(addr, vec![4, 5, 6]); // Geyser update
        assert_eq!(mirror.get_companion(&addr), Some(vec![4, 5, 6]));
    }
}
