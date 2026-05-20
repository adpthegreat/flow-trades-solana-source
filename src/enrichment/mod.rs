//! Price enrichment.
//!
//! Currently exposes a SOL/USD price oracle: a fully lock-free
//! `AtomicU64`-backed cache with a Binance primary fetch and DexScreener
//! fallback, refreshed at startup and every 10s on a tokio task.

pub mod price;

pub use price::PriceOracle;
