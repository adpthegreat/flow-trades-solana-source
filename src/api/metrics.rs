//! Prometheus metrics endpoint (`GET /metrics`).
//!
//! Exposes application metrics in Prometheus text exposition format.
//! No authentication required — intended for internal monitoring.

use std::sync::Arc;

use axum::extract::State;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder,
};

use super::AppState;

/// Application-level metrics collected throughout the system.
pub struct Metrics {
    pub registry: Registry,
    pub quote_total: IntCounter,
    pub quote_latency: Histogram,
    pub cache_size: IntGauge,
    pub registry_size: IntGauge,
    pub sqlite_size: IntGauge,
    pub stream_updates: IntCounter,
    pub stream_errors: IntCounter,
    pub scanner_pools_discovered: IntCounter,
    pub scanner_slots_scanned: IntCounter,
}

impl Metrics {
    /// Create and register all metrics.
    pub fn new() -> Self {
        let registry = Registry::new();

        let quote_total =
            IntCounter::new("flow_trades_quote_total", "Total quotes served").unwrap();

        let quote_latency = Histogram::with_opts(
            HistogramOpts::new(
                "flow_trades_quote_latency_seconds",
                "Quote handler latency in seconds",
            )
            .buckets(vec![0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0]),
        )
        .unwrap();

        let cache_size =
            IntGauge::new("flow_trades_cache_size", "Pool cache entry count").unwrap();

        let registry_size =
            IntGauge::new("flow_trades_registry_size", "Pool registry entry count").unwrap();

        let sqlite_size =
            IntGauge::new("flow_trades_sqlite_size", "SQLite pool row count").unwrap();

        let stream_updates = IntCounter::new(
            "flow_trades_stream_updates_total",
            "Total stream updates received",
        )
        .unwrap();

        let stream_errors =
            IntCounter::new("flow_trades_stream_errors_total", "Total stream errors").unwrap();

        let scanner_pools_discovered = IntCounter::new(
            "flow_trades_scanner_pools_discovered",
            "Pools discovered by block scanner",
        )
        .unwrap();

        let scanner_slots_scanned = IntCounter::new(
            "flow_trades_scanner_slots_scanned",
            "Slots scanned by block scanner",
        )
        .unwrap();

        // Register all metrics
        registry
            .register(Box::new(quote_total.clone()))
            .ok();
        registry
            .register(Box::new(quote_latency.clone()))
            .ok();
        registry
            .register(Box::new(cache_size.clone()))
            .ok();
        registry
            .register(Box::new(registry_size.clone()))
            .ok();
        registry
            .register(Box::new(sqlite_size.clone()))
            .ok();
        registry
            .register(Box::new(stream_updates.clone()))
            .ok();
        registry
            .register(Box::new(stream_errors.clone()))
            .ok();
        registry
            .register(Box::new(scanner_pools_discovered.clone()))
            .ok();
        registry
            .register(Box::new(scanner_slots_scanned.clone()))
            .ok();

        Self {
            registry,
            quote_total,
            quote_latency,
            cache_size,
            registry_size,
            sqlite_size,
            stream_updates,
            stream_errors,
            scanner_pools_discovered,
            scanner_slots_scanned,
        }
    }

    /// Update gauge metrics from current application state.
    pub fn refresh_gauges(&self, cache_len: usize, registry_len: usize, sqlite_count: usize) {
        self.cache_size.set(cache_len as i64);
        self.registry_size.set(registry_len as i64);
        self.sqlite_size.set(sqlite_count as i64);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// GET /metrics — returns Prometheus text exposition format.
pub async fn handle_metrics(State(state): State<Arc<AppState>>) -> String {
    // Refresh gauges from live state
    state.metrics.refresh_gauges(
        state.cache.len(),
        state.registry.len(),
        state.pool_db.count(),
    );

    let encoder = TextEncoder::new();
    let metric_families = state.metrics.registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap_or_default();
    String::from_utf8(buffer).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let m = Metrics::new();
        assert_eq!(m.quote_total.get(), 0);
        assert_eq!(m.cache_size.get(), 0);
        assert_eq!(m.registry_size.get(), 0);
        assert_eq!(m.sqlite_size.get(), 0);
        assert_eq!(m.stream_updates.get(), 0);
        assert_eq!(m.stream_errors.get(), 0);
        assert_eq!(m.scanner_pools_discovered.get(), 0);
        assert_eq!(m.scanner_slots_scanned.get(), 0);
    }

    #[test]
    fn test_metrics_quote_counter() {
        let m = Metrics::new();
        m.quote_total.inc();
        m.quote_total.inc();
        assert_eq!(m.quote_total.get(), 2);
    }

    #[test]
    fn test_metrics_quote_latency_histogram() {
        let m = Metrics::new();
        m.quote_latency.observe(0.001);
        m.quote_latency.observe(0.005);
        assert_eq!(m.quote_latency.get_sample_count(), 2);
    }

    #[test]
    fn test_metrics_refresh_gauges() {
        let m = Metrics::new();
        m.refresh_gauges(100, 200, 300);
        assert_eq!(m.cache_size.get(), 100);
        assert_eq!(m.registry_size.get(), 200);
        assert_eq!(m.sqlite_size.get(), 300);
    }

    #[test]
    fn test_metrics_stream_counters() {
        let m = Metrics::new();
        m.stream_updates.inc_by(5);
        m.stream_errors.inc_by(2);
        assert_eq!(m.stream_updates.get(), 5);
        assert_eq!(m.stream_errors.get(), 2);
    }

    #[test]
    fn test_metrics_scanner_counters() {
        let m = Metrics::new();
        m.scanner_pools_discovered.inc_by(10);
        m.scanner_slots_scanned.inc_by(50);
        assert_eq!(m.scanner_pools_discovered.get(), 10);
        assert_eq!(m.scanner_slots_scanned.get(), 50);
    }

    #[test]
    fn test_metrics_encode_produces_output() {
        let m = Metrics::new();
        m.quote_total.inc();
        m.cache_size.set(42);

        let encoder = TextEncoder::new();
        let families = m.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("flow_trades_quote_total"));
        assert!(output.contains("flow_trades_cache_size"));
    }
}
