//! TOML config file support.
//!
//! Loads settings from `config.toml` and merges with CLI args / env vars.
//! Priority: CLI flags > env vars > config.toml > defaults.

use serde::Deserialize;
use std::path::Path;

/// Top-level config file structure.
#[derive(Debug, Deserialize, Default)]
pub struct ConfigFile {
    pub rpc_url: Option<String>,
    pub listen: Option<String>,
    pub log_level: Option<String>,

    #[serde(default)]
    pub router: RouterSection,

    #[serde(default)]
    pub streaming: StreamingSection,

    #[serde(default)]
    pub discovery: DiscoverySection,

    #[serde(default)]
    pub storage: StorageSection,

    #[serde(default)]
    pub alt: AltSection,
}

#[derive(Debug, Deserialize, Default)]
pub struct RouterSection {
    // program_id, fee_bps, and protocol_fee_account are enforced by the on-chain config PDA.
    pub referral_account: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct StreamingSection {
    pub geyser_endpoint: Option<String>,
    pub geyser_token: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct DiscoverySection {
    pub mode: Option<String>,
    pub block_scan_enabled: Option<bool>,
    pub block_scan_interval_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct StorageSection {
    pub pool_db: Option<String>,
    pub cache_ttl_ms: Option<u64>,
    pub prune_interval_secs: Option<u64>,
    pub prune_max_age_days: Option<u64>,
    pub warm_storage: Option<String>,
    pub snapshot: Option<String>,
    pub snapshot_interval_secs: Option<u64>,
    pub warm_save_interval_secs: Option<u64>,
    pub pools_file: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct AltSection {
    pub addresses: Option<String>,
    pub refresh_interval_secs: Option<u64>,
}

impl ConfigFile {
    /// Load from a TOML file. Returns default if file doesn't exist.
    pub fn load(path: &str) -> Self {
        if !Path::new(path).exists() {
            return Self::default();
        }
        match std::fs::read_to_string(path) {
            Ok(contents) => match toml::from_str(&contents) {
                Ok(config) => {
                    tracing::info!(path, "loaded config file");
                    config
                }
                Err(e) => {
                    tracing::warn!(path, error = %e, "failed to parse config file, using defaults");
                    Self::default()
                }
            },
            Err(e) => {
                tracing::debug!(path, error = %e, "config file not found, using defaults");
                Self::default()
            }
        }
    }
}

/// Apply config file values to the CLI config (only for fields not already set by CLI/env).
pub fn apply_config_file(cli: &mut super::config::Config, file: &ConfigFile) {
    // Only apply file values when the CLI value is still at its default
    // (meaning neither CLI flag nor env var was used).
    // This is a best-effort merge — CLI/env always wins.

    if let Some(ref v) = file.rpc_url {
        if cli.rpc_url.is_empty() || cli.rpc_url == "https://your-solana-rpc.com" {
            cli.rpc_url = v.clone();
        }
    }
    if let Some(ref v) = file.listen {
        if cli.listen == "127.0.0.1:8080" {
            cli.listen = v.clone();
        }
    }
    if let Some(ref v) = file.log_level {
        if cli.log_level == "info" {
            cli.log_level = v.clone();
        }
    }

    // Router (fee rate and protocol account enforced on-chain — only referral is configurable)
    if cli.referral_account.is_none() {
        cli.referral_account = file.router.referral_account.clone();
    }

    // Streaming (Geyser)
    if cli.geyser_endpoint.is_none() {
        cli.geyser_endpoint = file.streaming.geyser_endpoint.clone();
    }
    if cli.geyser_token.is_none() {
        cli.geyser_token = file.streaming.geyser_token.clone();
    }

    // Discovery
    if cli.discovery_mode == "auto" {
        if let Some(ref v) = file.discovery.mode {
            cli.discovery_mode = v.clone();
        }
    }
    if cli.block_scan_enabled {
        if let Some(v) = file.discovery.block_scan_enabled {
            cli.block_scan_enabled = v;
        }
    }
    if cli.block_scan_interval_ms == 2000 {
        if let Some(v) = file.discovery.block_scan_interval_ms {
            cli.block_scan_interval_ms = v;
        }
    }

    // Storage
    if cli.pool_db_path == "./pools.db" {
        if let Some(ref v) = file.storage.pool_db {
            cli.pool_db_path = v.clone();
        }
    }
    if cli.pool_cache_ttl == 2000 {
        if let Some(v) = file.storage.cache_ttl_ms {
            cli.pool_cache_ttl = v;
        }
    }
    if cli.prune_interval_secs == 3600 {
        if let Some(v) = file.storage.prune_interval_secs {
            cli.prune_interval_secs = v;
        }
    }
    if cli.prune_max_age_days == 7 {
        if let Some(v) = file.storage.prune_max_age_days {
            cli.prune_max_age_days = v;
        }
    }
    if cli.pools_file.is_none() {
        cli.pools_file = file.storage.pools_file.clone();
    }

    // ALT
    if cli.alt_addresses.is_none() {
        cli.alt_addresses = file.alt.addresses.clone();
    }
    if cli.alt_refresh_interval_secs == 300 {
        if let Some(v) = file.alt.refresh_interval_secs {
            cli.alt_refresh_interval_secs = v;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_toml() {
        let toml_str = r#"rpc_url = "https://example.com""#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(config.rpc_url.unwrap(), "https://example.com");
        assert!(config.router.referral_account.is_none());
    }

    #[test]
    fn test_parse_full_toml() {
        let toml_str = r#"
rpc_url = "https://rpc.example.com"
listen = "0.0.0.0:9090"
log_level = "debug"

[router]
referral_account = "IntegratorAccount..."

[streaming]
geyser_endpoint = "http://geyser:10000"

[discovery]
mode = "auto"
block_scan_enabled = false

[storage]
pool_db = "/data/pools.db"
cache_ttl_ms = 5000
prune_max_age_days = 14

[alt]
addresses = "ABC123,DEF456"
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(config.rpc_url.unwrap(), "https://rpc.example.com");
        assert_eq!(config.listen.unwrap(), "0.0.0.0:9090");
        assert_eq!(config.router.referral_account.unwrap(), "IntegratorAccount...");
        assert_eq!(config.streaming.geyser_endpoint.unwrap(), "http://geyser:10000");
        assert_eq!(config.discovery.block_scan_enabled.unwrap(), false);
        assert_eq!(config.storage.pool_db.unwrap(), "/data/pools.db");
        assert_eq!(config.alt.addresses.unwrap(), "ABC123,DEF456");
    }

    #[test]
    fn test_missing_file_returns_default() {
        let config = ConfigFile::load("/nonexistent/path.toml");
        assert!(config.rpc_url.is_none());
    }

    #[test]
    fn test_empty_sections_default() {
        let toml_str = r#"rpc_url = "https://rpc.com""#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert!(config.router.referral_account.is_none());
        assert!(config.streaming.geyser_endpoint.is_none());
        assert!(config.discovery.block_scan_enabled.is_none());
    }
}
