use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::time::{self, Duration};
use tracing::{debug, error, info};

use crate::error::{TradeError, TradeResult};
use crate::pool::registry::{PoolEntry, PoolRegistry};

/// JSON snapshot of the pool registry (L3 persistence).
/// Human-readable, portable, used as a fallback or for manual inspection.
#[derive(Serialize, Deserialize)]
struct PoolSnapshot {
    version: u8,
    saved_at: String,
    pools: Vec<PoolEntry>,
}

const SNAPSHOT_VERSION: u8 = 1;

/// Save a JSON snapshot of the registry to disk.
pub fn save(path: &str, registry: &PoolRegistry) -> TradeResult<()> {
    let snapshot = PoolSnapshot {
        version: SNAPSHOT_VERSION,
        saved_at: chrono::Utc::now().to_rfc3339(),
        pools: registry.entries(),
    };

    let json = serde_json::to_string_pretty(&snapshot)
        .map_err(|e| TradeError::Internal(format!("json snapshot serialize: {e}")))?;

    let tmp_path = format!("{path}.tmp");
    std::fs::write(&tmp_path, &json)
        .map_err(|e| TradeError::Internal(format!("json snapshot write {tmp_path}: {e}")))?;

    std::fs::rename(&tmp_path, path)
        .map_err(|e| TradeError::Internal(format!("json snapshot rename to {path}: {e}")))?;

    debug!(
        path,
        pools = snapshot.pools.len(),
        bytes = json.len(),
        "json snapshot saved"
    );

    Ok(())
}

/// Load a JSON snapshot from disk. Returns pool entries.
pub fn load(path: &str) -> TradeResult<Vec<PoolEntry>> {
    let data = std::fs::read_to_string(path)
        .map_err(|e| TradeError::Internal(format!("json snapshot read {path}: {e}")))?;

    let snapshot: PoolSnapshot = serde_json::from_str(&data)
        .map_err(|e| TradeError::Internal(format!("json snapshot parse: {e}")))?;

    if snapshot.version != SNAPSHOT_VERSION {
        return Err(TradeError::Internal(format!(
            "json snapshot version mismatch: expected {SNAPSHOT_VERSION}, got {}",
            snapshot.version
        )));
    }

    info!(
        path,
        pools = snapshot.pools.len(),
        saved_at = %snapshot.saved_at,
        "json snapshot loaded"
    );

    Ok(snapshot.pools)
}

/// Spawn a background task that periodically saves a JSON snapshot.
pub fn spawn_periodic_save(
    registry: Arc<PoolRegistry>,
    path: String,
    interval_secs: u64,
) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            if let Err(e) = save(&path, &registry) {
                error!(error = %e, "periodic json snapshot save failed");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::types::PoolType;
    use solana_sdk::pubkey::Pubkey;

    fn make_test_entry() -> PoolEntry {
        PoolEntry {
            address: Pubkey::new_unique(),
            pool_type: PoolType::RaydiumCpmm,
            mint_a: Pubkey::new_unique(),
            mint_b: Pubkey::new_unique(),
        }
    }

    #[test]
    fn test_json_snapshot_roundtrip() {
        let registry = PoolRegistry::new();
        let entry = make_test_entry();
        let entry_addr = entry.address;
        registry.add(entry);

        let path = "/tmp/flow_trades_test_snapshot.json";
        save(path, &registry).unwrap();

        let entries = load(path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].address, entry_addr);

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_json_snapshot_pretty_output() {
        let registry = PoolRegistry::new();
        registry.add(make_test_entry());

        let path = "/tmp/flow_trades_test_snapshot_pretty.json";
        save(path, &registry).unwrap();

        let data = std::fs::read_to_string(path).unwrap();
        // Pretty JSON should contain newlines and indentation
        assert!(data.contains('\n'));
        assert!(data.contains("  "));
        // Should contain version and saved_at fields
        assert!(data.contains("\"version\""));
        assert!(data.contains("\"saved_at\""));
        assert!(data.contains("\"pools\""));

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_json_snapshot_empty_registry() {
        let registry = PoolRegistry::new();

        let path = "/tmp/flow_trades_test_snapshot_empty.json";
        save(path, &registry).unwrap();

        let entries = load(path).unwrap();
        assert!(entries.is_empty());

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_json_snapshot_load_nonexistent() {
        let result = load("/tmp/flow_trades_nonexistent_snapshot.json");
        assert!(result.is_err());
    }

    #[test]
    fn test_json_snapshot_multiple_pools() {
        let registry = PoolRegistry::new();
        for _ in 0..5 {
            registry.add(make_test_entry());
        }

        let path = "/tmp/flow_trades_test_snapshot_multi.json";
        save(path, &registry).unwrap();

        let entries = load(path).unwrap();
        assert_eq!(entries.len(), 5);

        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_json_snapshot_corrupt_data() {
        let path = "/tmp/flow_trades_test_snapshot_corrupt.json";
        std::fs::write(path, "not valid json {{{").unwrap();

        let result = load(path);
        assert!(result.is_err());

        std::fs::remove_file(path).ok();
    }
}
