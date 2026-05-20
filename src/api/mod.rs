pub mod health;
pub mod metrics;
pub mod quote;
pub mod swap;
pub mod swap_stream_ws;
pub mod ws;

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::execution::AltCache;
use crate::pool::cache::PoolCache;
use crate::pool::registry::PoolRegistry;
use crate::quote::Quoter;
use crate::storage::sqlite::PoolDb;
use crate::stream::blockhash::BlockhashCache;
use crate::execution::router::RouterConfig;
use crate::stream::StreamStats;

pub use metrics::Metrics;

/// Shared application state for all handlers.
pub struct AppState {
    pub quoter: Arc<Quoter>,
    pub rpc: Arc<RpcClient>,
    pub cache: Arc<PoolCache>,
    pub registry: Arc<PoolRegistry>,
    pub blockhash_cache: Arc<BlockhashCache>,
    pub stream_stats: Arc<StreamStats>,
    pub alt_cache: Arc<AltCache>,
    /// Router config for on-chain fee collection.
    pub router_config: Option<RouterConfig>,
    /// SQLite pool database for durable persistence.
    pub pool_db: Arc<PoolDb>,
    /// Prometheus metrics.
    pub metrics: Arc<Metrics>,
    /// Known-existing fee ATAs (treasury + referral). Avoids redundant
    /// create_associated_token_account_idempotent instructions after first swap.
    pub known_fee_atas: dashmap::DashSet<solana_sdk::pubkey::Pubkey>,
    /// Account mirror — cached companion data + vault balances from Geyser.
    pub account_mirror: Arc<crate::stream::account_mirror::AccountMirror>,
    /// Mint → token program cache. Mint owner is immutable — cache forever.
    /// Eliminates 1-4 RPC getAccount calls per /swap.
    pub mint_program_cache: Arc<dashmap::DashMap<solana_sdk::pubkey::Pubkey, solana_sdk::pubkey::Pubkey>>,
    /// Live swap broadcast hub. `None` if `--swap-stream-enabled false`.
    /// Subscribers connect via `GET /swap-stream` (WebSocket).
    pub swap_broadcast: Option<tokio::sync::broadcast::Sender<Arc<crate::stream::swap_stream::Swap>>>,
    /// SOL/USD oracle (lock-free). Used by the swap-stream USD enrichment.
    /// `None` when the swap stream is disabled.
    pub price_oracle: Option<Arc<crate::enrichment::PriceOracle>>,
}

/// Maximum request body size (64 KB — swap requests are typically <10KB).
/// Protects against memory exhaustion from oversized payloads.
const MAX_BODY_SIZE: usize = 64 * 1024;

/// Build the Axum router with all endpoints.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/quote", get(quote::handle_quote))
        .route("/swap", post(swap::handle_swap))
        .route("/health", get(health::handle_health))
        .route("/metrics", get(metrics::handle_metrics))
        .route("/program-id-to-label", get(health::handle_labels))
        .route("/quote-ws", get(ws::handle_ws_upgrade))
        .route("/swap-stream", get(swap_stream_ws::handle_swap_stream_upgrade))
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::commitment_config::CommitmentConfig;

    fn make_test_state() -> Arc<AppState> {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            "http://localhost:8899".to_string(),
            CommitmentConfig::confirmed(),
        ));
        let cache = Arc::new(PoolCache::new(2000));
        let registry = Arc::new(PoolRegistry::new());
        let quoter = Arc::new(Quoter::new(
            Arc::clone(&registry),
            Arc::clone(&cache),
            Arc::clone(&rpc),
        ));
        let blockhash_cache = Arc::new(BlockhashCache::new(2000));
        let stream_stats = Arc::new(StreamStats::new());
        let alt_cache = Arc::new(AltCache::new());
        let pool_db = Arc::new(PoolDb::open(":memory:").unwrap());

        let metrics = Arc::new(Metrics::new());

        Arc::new(AppState {
            quoter,
            rpc,
            cache,
            registry,
            blockhash_cache,
            stream_stats,
            alt_cache,
            router_config: None,
            pool_db,
            metrics,
            known_fee_atas: dashmap::DashSet::new(),
            account_mirror: Arc::new(crate::stream::account_mirror::AccountMirror::new()),
            mint_program_cache: Arc::new(dashmap::DashMap::new()),
            swap_broadcast: None,
            price_oracle: None,
        })
    }

    #[test]
    fn test_app_state_creation() {
        let state = make_test_state();
        assert_eq!(state.cache.len(), 0);
        assert_eq!(state.registry.len(), 0);
        assert!(state.alt_cache.is_empty());
    }

    #[test]
    fn test_router_builds_without_error() {
        let state = make_test_state();
        // This should not panic
        let _router = router(state);
    }
}
