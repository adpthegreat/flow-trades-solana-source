//! SOL/USD price oracle. Binance primary + DexScreener fallback, with
//! lock-free `AtomicU64` storage of the f64 bits. Bootstrapping refreshes
//! once at startup, then every 10s on a tokio task — see `main.rs`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, warn};

use crate::error::{TradeError, TradeResult};

/// SOL/USD price oracle (fully lock-free).
///
/// Fetches the current SOL/USD price from multiple free public APIs.
/// Primary: Binance public ticker (no auth needed)
/// Fallback: DexScreener API (no auth needed)
pub struct PriceOracle {
    /// Cached SOL price in USD (stored as f64 bits in AtomicU64)
    cached_price: AtomicU64,
    /// Last refresh time as epoch_ms (AtomicU64 instead of Mutex<Instant>)
    last_refresh_epoch_ms: AtomicU64,
}

const BINANCE_URL: &str = "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT";
const DEXSCREENER_URL: &str =
    "https://api.dexscreener.com/latest/dex/pairs/solana/7qbRF6YsyGuLUVs6Y1sfC93Lg8R9eE6RUzYeznBL6rE6";

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl PriceOracle {
    pub fn new() -> Self {
        Self {
            cached_price: AtomicU64::new(0),
            last_refresh_epoch_ms: AtomicU64::new(0),
        }
    }

    /// Get the cached SOL/USD price. Returns None if never refreshed.
    pub fn get_sol_usd_price(&self) -> Option<f64> {
        let bits = self.cached_price.load(Ordering::Relaxed);
        if bits == 0 {
            None
        } else {
            Some(f64::from_bits(bits))
        }
    }

    /// Refresh the SOL/USD price. Tries Binance first, then DexScreener.
    pub fn refresh(&self) -> TradeResult<f64> {
        match self.fetch_binance_price() {
            Ok(price) => {
                self.store_price(price);
                debug!(price = format!("{:.2}", price), source = "binance", "SOL/USD price refreshed");
                return Ok(price);
            }
            Err(e) => {
                warn!(error = %e, "Binance price fetch failed, trying fallback");
            }
        }

        match self.fetch_dexscreener_price() {
            Ok(price) => {
                self.store_price(price);
                debug!(price = format!("{:.2}", price), source = "dexscreener", "SOL/USD price refreshed");
                Ok(price)
            }
            Err(e) => {
                warn!(error = %e, "DexScreener price fetch also failed");
                Err(TradeError::PriceUnavailable(
                    "All price sources failed".into(),
                ))
            }
        }
    }

    /// Fetch SOL price from Binance public ticker API.
    /// Response format: { "symbol": "SOLUSDT", "price": "170.50000000" }
    fn fetch_binance_price(&self) -> TradeResult<f64> {
        let resp: serde_json::Value = ureq::get(BINANCE_URL)
            .timeout(Duration::from_secs(10))
            .call()
            .map_err(|e| TradeError::PriceUnavailable(format!("Binance HTTP: {}", e)))?
            .into_json()
            .map_err(|e| TradeError::PriceUnavailable(format!("Binance JSON: {}", e)))?;

        let price_str = resp
            .get("price")
            .and_then(|p| p.as_str())
            .ok_or_else(|| {
                TradeError::PriceUnavailable(format!("Binance response missing price: {}", resp))
            })?;

        let price: f64 = price_str
            .parse()
            .map_err(|e| TradeError::PriceUnavailable(format!("Binance price parse: {}", e)))?;

        self.validate_price(price)
    }

    /// Fetch SOL price from DexScreener API (Raydium SOL/USDC pair).
    /// Response format: { "pair": { "priceUsd": "170.50" } }
    fn fetch_dexscreener_price(&self) -> TradeResult<f64> {
        let resp: serde_json::Value = ureq::get(DEXSCREENER_URL)
            .timeout(Duration::from_secs(10))
            .call()
            .map_err(|e| TradeError::PriceUnavailable(format!("DexScreener HTTP: {}", e)))?
            .into_json()
            .map_err(|e| TradeError::PriceUnavailable(format!("DexScreener JSON: {}", e)))?;

        let price_str = resp
            .get("pair")
            .and_then(|p| p.get("priceUsd"))
            .and_then(|p| p.as_str())
            .ok_or_else(|| {
                TradeError::PriceUnavailable(format!("DexScreener response missing price: {}", resp))
            })?;

        let price: f64 = price_str
            .parse()
            .map_err(|e| TradeError::PriceUnavailable(format!("DexScreener price parse: {}", e)))?;

        self.validate_price(price)
    }

    fn validate_price(&self, price: f64) -> TradeResult<f64> {
        if price > 1.0 && price < 100_000.0 {
            Ok(price)
        } else {
            Err(TradeError::PriceUnavailable(format!(
                "Price {} outside sane range (1-100000)",
                price
            )))
        }
    }

    fn store_price(&self, price: f64) {
        self.cached_price.store(price.to_bits(), Ordering::Relaxed);
        self.last_refresh_epoch_ms
            .store(epoch_ms(), Ordering::Relaxed);
    }

    /// Check if the price needs refreshing (older than max_age).
    pub fn needs_refresh(&self, max_age_secs: u64) -> bool {
        let last = self.last_refresh_epoch_ms.load(Ordering::Relaxed);
        if last == 0 {
            return true;
        }
        let elapsed_ms = epoch_ms().saturating_sub(last);
        elapsed_ms >= max_age_secs * 1000
    }

    /// Age of the cached price in milliseconds. Returns None if never refreshed.
    pub fn age_ms(&self) -> Option<u64> {
        let last = self.last_refresh_epoch_ms.load(Ordering::Relaxed);
        if last == 0 {
            None
        } else {
            Some(epoch_ms().saturating_sub(last))
        }
    }

    /// Test-only: set the cached price directly without an HTTP fetch.
    /// Useful when downstream consumers (swap stream, USD enrichment)
    /// need a deterministic SOL/USD value in unit tests.
    #[doc(hidden)]
    pub fn set_for_tests(&self, price: f64) {
        self.store_price(price);
    }
}

impl Default for PriceOracle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_oracle_initial_state() {
        let oracle = PriceOracle::new();
        assert!(oracle.get_sol_usd_price().is_none());
        assert!(oracle.needs_refresh(0));
        assert!(oracle.age_ms().is_none());
    }

    #[test]
    fn test_price_validation() {
        let oracle = PriceOracle::new();
        assert!(oracle.validate_price(170.0).is_ok());
        assert!(oracle.validate_price(0.5).is_err());
        assert!(oracle.validate_price(200_000.0).is_err());
    }

    #[test]
    fn test_store_and_read_price() {
        let oracle = PriceOracle::new();
        oracle.store_price(175.50);
        let price = oracle.get_sol_usd_price().unwrap();
        assert!((price - 175.50).abs() < 0.001);
        assert!(oracle.age_ms().is_some());
    }

    #[test]
    fn test_needs_refresh() {
        let oracle = PriceOracle::new();
        // Never refreshed → needs refresh
        assert!(oracle.needs_refresh(60));
        oracle.store_price(150.0);
        // Just refreshed → does not need refresh for 60s window
        assert!(!oracle.needs_refresh(60));
        // Always needs refresh for 0-second window
        assert!(oracle.needs_refresh(0));
    }
}
