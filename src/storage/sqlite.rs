use rusqlite::{params, Connection};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Mutex;

use crate::error::{TradeError, TradeResult};
use crate::pool::registry::PoolEntry;
use crate::pool::types::PoolType;

/// SQLite-backed durable persistence for pool data.
///
/// Replaces bincode/JSON file persistence with a crash-safe, single-file
/// embedded database. Uses WAL mode for concurrent reads during writes.
pub struct PoolDb {
    conn: Mutex<Connection>,
}

impl PoolDb {
    /// Open or create the SQLite database at the given path.
    pub fn open(path: &str) -> TradeResult<Self> {
        let conn = Connection::open(path)
            .map_err(|e| TradeError::Internal(format!("sqlite open: {e}")))?;

        // Enable WAL mode for concurrent reads during writes
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            .map_err(|e| TradeError::Internal(format!("sqlite pragma: {e}")))?;

        // Create table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pools (
                address TEXT PRIMARY KEY,
                pool_type TEXT NOT NULL,
                mint_a TEXT NOT NULL,
                mint_b TEXT NOT NULL,
                discovered_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                last_seen INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                trade_count INTEGER NOT NULL DEFAULT 0
            )",
            [],
        )
        .map_err(|e| TradeError::Internal(format!("sqlite create: {e}")))?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Load all pools from the database.
    pub fn load_all(&self) -> TradeResult<Vec<PoolEntry>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare("SELECT address, pool_type, mint_a, mint_b FROM pools")
            .map_err(|e| TradeError::Internal(format!("sqlite prepare: {e}")))?;

        let rows = stmt
            .query_map([], |row| {
                let addr: String = row.get(0)?;
                let pt: String = row.get(1)?;
                let ma: String = row.get(2)?;
                let mb: String = row.get(3)?;
                Ok((addr, pt, ma, mb))
            })
            .map_err(|e| TradeError::Internal(format!("sqlite query: {e}")))?;

        let mut entries = Vec::new();
        for row in rows {
            if let Ok((addr, pt, ma, mb)) = row {
                if let (Ok(address), Some(pool_type), Ok(mint_a), Ok(mint_b)) = (
                    Pubkey::from_str(&addr),
                    PoolType::from_str_opt(&pt),
                    Pubkey::from_str(&ma),
                    Pubkey::from_str(&mb),
                ) {
                    entries.push(PoolEntry {
                        address,
                        pool_type,
                        mint_a,
                        mint_b,
                    });
                }
            }
        }
        Ok(entries)
    }

    /// Insert a new pool (or ignore if already exists).
    pub fn insert_pool(&self, entry: &PoolEntry) -> TradeResult<bool> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let affected = conn
            .execute(
                "INSERT OR IGNORE INTO pools (address, pool_type, mint_a, mint_b) VALUES (?1, ?2, ?3, ?4)",
                params![
                    entry.address.to_string(),
                    entry.pool_type.as_str(),
                    entry.mint_a.to_string(),
                    entry.mint_b.to_string(),
                ],
            )
            .map_err(|e| TradeError::Internal(format!("sqlite insert: {e}")))?;
        Ok(affected > 0)
    }

    /// Batch insert pools (inside a transaction for speed).
    pub fn insert_pools(&self, entries: &[PoolEntry]) -> TradeResult<usize> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| TradeError::Internal(format!("sqlite tx: {e}")))?;

        let mut inserted = 0;
        for entry in entries {
            let affected = tx
                .execute(
                    "INSERT OR IGNORE INTO pools (address, pool_type, mint_a, mint_b) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        entry.address.to_string(),
                        entry.pool_type.as_str(),
                        entry.mint_a.to_string(),
                        entry.mint_b.to_string(),
                    ],
                )
                .map_err(|e| TradeError::Internal(format!("sqlite batch insert: {e}")))?;
            inserted += affected;
        }

        tx.commit()
            .map_err(|e| TradeError::Internal(format!("sqlite commit: {e}")))?;
        Ok(inserted)
    }

    /// Update last_seen timestamp and increment trade_count for a pool.
    pub fn touch_pool(&self, address: &Pubkey) -> TradeResult<()> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "UPDATE pools SET last_seen = strftime('%s','now'), trade_count = trade_count + 1 WHERE address = ?1",
            params![address.to_string()],
        )
        .map_err(|e| TradeError::Internal(format!("sqlite touch: {e}")))?;
        Ok(())
    }

    /// Get pool count.
    pub fn count(&self) -> usize {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.query_row("SELECT COUNT(*) FROM pools", [], |row| {
            row.get::<_, usize>(0)
        })
        .unwrap_or(0)
    }

    /// Remove pools not seen in N seconds.
    pub fn prune_stale(&self, max_age_secs: u64) -> TradeResult<usize> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let removed = conn
            .execute(
                "DELETE FROM pools WHERE last_seen < strftime('%s','now') - ?1",
                params![max_age_secs as i64],
            )
            .map_err(|e| TradeError::Internal(format!("sqlite prune: {e}")))?;
        Ok(removed)
    }

    /// Remove pools not seen in N seconds and return the pruned addresses.
    pub fn prune_stale_and_get_addresses(&self, max_age_secs: u64) -> TradeResult<Vec<Pubkey>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        // First, collect addresses to prune
        let mut stmt = conn
            .prepare("SELECT address FROM pools WHERE last_seen < strftime('%s','now') - ?1")
            .map_err(|e| TradeError::Internal(format!("sqlite prune select: {e}")))?;

        let addresses: Vec<Pubkey> = stmt
            .query_map(params![max_age_secs as i64], |row| {
                let addr: String = row.get(0)?;
                Ok(addr)
            })
            .map_err(|e| TradeError::Internal(format!("sqlite prune query: {e}")))?
            .filter_map(|r| r.ok())
            .filter_map(|addr_str| Pubkey::from_str(&addr_str).ok())
            .collect();

        // Then delete them
        if !addresses.is_empty() {
            conn.execute(
                "DELETE FROM pools WHERE last_seen < strftime('%s','now') - ?1",
                params![max_age_secs as i64],
            )
            .map_err(|e| TradeError::Internal(format!("sqlite prune delete: {e}")))?;
        }

        Ok(addresses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_test_entry() -> PoolEntry {
        PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::Orca,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        }
    }

    fn temp_db_path() -> String {
        format!(
            "/tmp/flow_trades_sqlite_test_{}.db",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    fn cleanup(path: &str) {
        std::fs::remove_file(path).ok();
        std::fs::remove_file(format!("{path}-wal")).ok();
        std::fs::remove_file(format!("{path}-shm")).ok();
    }

    #[test]
    fn test_open_and_create_table() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();
        assert_eq!(db.count(), 0);
        cleanup(&path);
    }

    #[test]
    fn test_insert_and_load_roundtrip() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let entry = make_test_entry();
        let addr = entry.address;
        let pt = entry.pool_type;
        let ma = entry.mint_a;
        let mb = entry.mint_b;

        assert!(db.insert_pool(&entry).unwrap());
        assert_eq!(db.count(), 1);

        let loaded = db.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].address, addr);
        assert_eq!(loaded[0].pool_type, pt);
        assert_eq!(loaded[0].mint_a, ma);
        assert_eq!(loaded[0].mint_b, mb);

        cleanup(&path);
    }

    #[test]
    fn test_insert_duplicate_ignored() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let entry = make_test_entry();
        assert!(db.insert_pool(&entry).unwrap()); // first insert -> true
        assert!(!db.insert_pool(&entry).unwrap()); // duplicate -> false
        assert_eq!(db.count(), 1);

        cleanup(&path);
    }

    #[test]
    fn test_batch_insert() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let entries: Vec<PoolEntry> = (0..10).map(|_| make_test_entry()).collect();
        let inserted = db.insert_pools(&entries).unwrap();
        assert_eq!(inserted, 10);
        assert_eq!(db.count(), 10);

        let loaded = db.load_all().unwrap();
        assert_eq!(loaded.len(), 10);

        cleanup(&path);
    }

    #[test]
    fn test_batch_insert_with_duplicates() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let entry = make_test_entry();
        db.insert_pool(&entry).unwrap();

        // Batch contains the duplicate + 2 new ones
        let entries = vec![entry, make_test_entry(), make_test_entry()];
        let inserted = db.insert_pools(&entries).unwrap();
        assert_eq!(inserted, 2); // only 2 new
        assert_eq!(db.count(), 3);

        cleanup(&path);
    }

    #[test]
    fn test_touch_pool() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let entry = make_test_entry();
        let addr = entry.address;
        db.insert_pool(&entry).unwrap();

        // Touch should not error
        db.touch_pool(&addr).unwrap();

        // Verify trade_count incremented
        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT trade_count FROM pools WHERE address = ?1",
                params![addr.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        cleanup(&path);
    }

    #[test]
    fn test_touch_nonexistent_pool() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        // Touching a nonexistent pool should not error (just 0 rows affected)
        db.touch_pool(&Pubkey::new_unique()).unwrap();
        assert_eq!(db.count(), 0);

        cleanup(&path);
    }

    #[test]
    fn test_count_empty() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();
        assert_eq!(db.count(), 0);
        cleanup(&path);
    }

    #[test]
    fn test_prune_stale() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let entry = make_test_entry();
        db.insert_pool(&entry).unwrap();

        // Set last_seen to 1000 seconds ago
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE pools SET last_seen = strftime('%s','now') - 1000 WHERE address = ?1",
                params![entry.address.to_string()],
            )
            .unwrap();
        }

        // Prune with max_age 500 seconds -> should remove it
        let removed = db.prune_stale(500).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(db.count(), 0);

        cleanup(&path);
    }

    #[test]
    fn test_prune_keeps_recent() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let entry = make_test_entry();
        db.insert_pool(&entry).unwrap();

        // Prune with a very large max_age -> should keep it
        let removed = db.prune_stale(999_999).unwrap();
        assert_eq!(removed, 0);
        assert_eq!(db.count(), 1);

        cleanup(&path);
    }

    #[test]
    fn test_concurrent_reads() {
        let path = temp_db_path();
        let db = Arc::new(PoolDb::open(&path).unwrap());

        // Insert some data
        let entries: Vec<PoolEntry> = (0..20).map(|_| make_test_entry()).collect();
        db.insert_pools(&entries).unwrap();

        // Spawn multiple threads that all read
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    let loaded = db.load_all().unwrap();
                    assert_eq!(loaded.len(), 20);
                    let count = db.count();
                    assert_eq!(count, 20);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        cleanup(&path);
    }

    #[test]
    fn test_all_pool_types_roundtrip() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let pool_types = [
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
        ];

        let mut entries = Vec::new();
        for pt in &pool_types {
            entries.push(PoolEntry {
                address: Pubkey::new_unique(),
                pool_type: *pt,
                mint_a: Pubkey::new_unique(),
                mint_b: Pubkey::new_unique(),
            });
        }

        db.insert_pools(&entries).unwrap();
        let loaded = db.load_all().unwrap();
        assert_eq!(loaded.len(), pool_types.len());

        // Verify all pool types survived the round-trip
        let loaded_types: std::collections::HashSet<PoolType> =
            loaded.iter().map(|e| e.pool_type).collect();
        for pt in &pool_types {
            assert!(loaded_types.contains(pt), "missing pool type: {pt}");
        }

        cleanup(&path);
    }

    #[test]
    fn test_load_all_empty() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let loaded = db.load_all().unwrap();
        assert!(loaded.is_empty());

        cleanup(&path);
    }

    #[test]
    fn test_reopen_persistence() {
        let path = temp_db_path();

        // Create and populate
        {
            let db = PoolDb::open(&path).unwrap();
            let entries: Vec<PoolEntry> = (0..5).map(|_| make_test_entry()).collect();
            db.insert_pools(&entries).unwrap();
            assert_eq!(db.count(), 5);
        }

        // Reopen and verify data persists
        {
            let db = PoolDb::open(&path).unwrap();
            assert_eq!(db.count(), 5);
            let loaded = db.load_all().unwrap();
            assert_eq!(loaded.len(), 5);
        }

        cleanup(&path);
    }

    #[test]
    fn test_prune_stale_and_get_addresses() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let entry1 = make_test_entry();
        let entry2 = make_test_entry();
        let addr1 = entry1.address;
        db.insert_pool(&entry1).unwrap();
        db.insert_pool(&entry2).unwrap();

        // Make entry1 stale (1000 seconds ago)
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE pools SET last_seen = strftime('%s','now') - 1000 WHERE address = ?1",
                params![addr1.to_string()],
            )
            .unwrap();
        }

        // Prune with max_age 500 -> should return entry1's address
        let pruned = db.prune_stale_and_get_addresses(500).unwrap();
        assert_eq!(pruned.len(), 1);
        assert_eq!(pruned[0], addr1);
        assert_eq!(db.count(), 1);

        cleanup(&path);
    }

    #[test]
    fn test_prune_stale_and_get_addresses_empty() {
        let path = temp_db_path();
        let db = PoolDb::open(&path).unwrap();

        let entry = make_test_entry();
        db.insert_pool(&entry).unwrap();

        // Prune with very large max_age -> nothing pruned
        let pruned = db.prune_stale_and_get_addresses(999_999).unwrap();
        assert!(pruned.is_empty());
        assert_eq!(db.count(), 1);

        cleanup(&path);
    }
}
