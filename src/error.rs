use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Core error type for flow-trades.
#[derive(Debug, thiserror::Error)]
pub enum TradeError {
    #[error("Validation error: {0}")]
    Validation(String),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Simulation failed: {error}")]
    SimulationFailed {
        error: String,
        logs: Vec<String>,
    },

    #[error("Slippage exceeded: expected {expected}, simulated {simulated}")]
    SlippageExceeded {
        expected: u64,
        simulated: u64,
    },

    #[error("Pool not found: {0}")]
    PoolNotFound(String),

    #[error("No route found for {input_mint} -> {output_mint}")]
    NoRoute {
        input_mint: String,
        output_mint: String,
    },

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Price unavailable: {0}")]
    PriceUnavailable(String),
}

pub type TradeResult<T> = Result<T, TradeError>;

impl IntoResponse for TradeError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            // Safe to expose: user-facing validation errors
            TradeError::Validation(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            TradeError::PoolNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            TradeError::NoRoute { .. } => (StatusCode::NOT_FOUND, self.to_string()),
            TradeError::SlippageExceeded { .. } => (StatusCode::BAD_REQUEST, self.to_string()),
            // Internal errors: sanitize to prevent leaking RPC URLs, stack traces, etc.
            TradeError::Rpc(_) => {
                tracing::warn!(error = %self, "RPC error in request handler");
                (StatusCode::INTERNAL_SERVER_ERROR, "RPC error".to_string())
            }
            TradeError::Execution(_) => {
                tracing::warn!(error = %self, "execution error in request handler");
                (StatusCode::INTERNAL_SERVER_ERROR, "execution error".to_string())
            }
            TradeError::SimulationFailed { .. } => {
                tracing::warn!(error = %self, "simulation failed in request handler");
                (StatusCode::BAD_REQUEST, "simulation failed".to_string())
            }
            TradeError::Internal(_) => {
                tracing::error!(error = %self, "internal error in request handler");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
            }
            TradeError::PriceUnavailable(_) => {
                tracing::warn!(error = %self, "price unavailable");
                (StatusCode::SERVICE_UNAVAILABLE, "price unavailable".to_string())
            }
        };
        (status, axum::Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

impl From<solana_client::client_error::ClientError> for TradeError {
    fn from(e: solana_client::client_error::ClientError) -> Self {
        TradeError::Rpc(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_error_display() {
        let e = TradeError::Validation("bad input".into());
        assert!(e.to_string().contains("bad input"));
    }

    #[test]
    fn test_no_route_display() {
        let e = TradeError::NoRoute {
            input_mint: "SOL".into(),
            output_mint: "USDC".into(),
        };
        assert!(e.to_string().contains("SOL"));
        assert!(e.to_string().contains("USDC"));
    }

    #[test]
    fn test_validation_is_400() {
        let e = TradeError::Validation("test".into());
        let resp = e.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_pool_not_found_is_404() {
        let e = TradeError::PoolNotFound("abc".into());
        let resp = e.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_internal_is_500() {
        let e = TradeError::Internal("crash".into());
        let resp = e.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
