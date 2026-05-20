use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::sync::RwLock;

/// Aggregated statistics for the streaming subsystem.
pub struct StreamStats {
    pub updates_received: AtomicU64,
    pub errors: AtomicU64,
    pub swaps_emitted: AtomicU64,
    pub last_update: RwLock<Option<Instant>>,
    pub last_swap: RwLock<Option<Instant>>,
}

impl StreamStats {
    pub fn new() -> Self {
        Self {
            updates_received: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            swaps_emitted: AtomicU64::new(0),
            last_update: RwLock::new(None),
            last_swap: RwLock::new(None),
        }
    }

    /// Record a successful pool state update.
    pub fn record_update(&self) {
        self.updates_received.fetch_add(1, Ordering::Relaxed);
        // Best-effort: try_write avoids blocking the hot path if another
        // update is concurrently writing. Worst case we skip one timestamp update.
        if let Ok(mut guard) = self.last_update.try_write() {
            *guard = Some(Instant::now());
        }
    }

    /// Record an error during streaming.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one or more parsed swaps emitted to the broadcast channel.
    pub fn record_swap_emitted(&self, count: u64) {
        self.swaps_emitted.fetch_add(count, Ordering::Relaxed);
        if let Ok(mut guard) = self.last_swap.try_write() {
            *guard = Some(Instant::now());
        }
    }

    /// Take a snapshot of the current stats.
    /// Returns (updates_received, errors, last_update).
    pub async fn snapshot(&self) -> (u64, u64, Option<Instant>) {
        let updates = self.updates_received.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let last = *self.last_update.read().await;
        (updates, errors, last)
    }

    /// Snapshot the swap-stream counters: (swaps_emitted, last_swap_at).
    pub async fn swap_snapshot(&self) -> (u64, Option<Instant>) {
        let count = self.swaps_emitted.load(Ordering::Relaxed);
        let last = *self.last_swap.read().await;
        (count, last)
    }
}

impl Default for StreamStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for the Geyser stream manager.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub geyser_endpoint: Option<String>,
    pub geyser_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_stats_default() {
        let stats = StreamStats::new();
        assert_eq!(stats.updates_received.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_stream_stats_record_update() {
        let stats = StreamStats::new();
        stats.record_update();
        stats.record_update();
        stats.record_update();
        assert_eq!(stats.updates_received.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_stream_stats_record_error() {
        let stats = StreamStats::new();
        stats.record_error();
        stats.record_error();
        assert_eq!(stats.errors.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_stream_stats_snapshot() {
        let stats = StreamStats::new();
        stats.record_update();
        stats.record_update();
        stats.record_error();

        let (updates, errors, last) = stats.snapshot().await;
        assert_eq!(updates, 2);
        assert_eq!(errors, 1);
        assert!(last.is_some());
    }

    #[tokio::test]
    async fn test_stream_stats_snapshot_empty() {
        let stats = StreamStats::new();
        let (updates, errors, last) = stats.snapshot().await;
        assert_eq!(updates, 0);
        assert_eq!(errors, 0);
        assert!(last.is_none());
    }

    #[test]
    fn test_stream_config_clone() {
        let config = StreamConfig {
            geyser_endpoint: Some("http://geyser:10000".into()),
            geyser_token: Some("tok".into()),
        };
        let cloned = config.clone();
        assert_eq!(cloned.geyser_endpoint.unwrap(), "http://geyser:10000");
        assert_eq!(cloned.geyser_token.unwrap(), "tok");
    }
}
