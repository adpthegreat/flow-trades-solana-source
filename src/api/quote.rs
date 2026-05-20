use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Query, State};
use axum::Json;

use crate::error::TradeError;
use crate::quote::types::{QuoteParams, QuoteRequest};
use crate::quote::QuoteResponse;

use super::AppState;

/// GET /quote — find the best swap route for a token pair.
pub async fn handle_quote(
    State(state): State<Arc<AppState>>,
    Query(params): Query<QuoteParams>,
) -> Result<Json<QuoteResponse>, TradeError> {
    let start = Instant::now();
    let req = QuoteRequest::from_params(&params)?;

    let response = state.quoter.quote(&req).await?;

    // Record metrics
    let elapsed = start.elapsed();
    state.metrics.quote_total.inc();
    state.metrics.quote_latency.observe(elapsed.as_secs_f64());

    if elapsed.as_millis() > 10 {
        tracing::warn!(
            handler_ms = elapsed.as_millis(),
            server_ms = response.quote_time_ms,
            "slow quote handler"
        );
    }

    Ok(Json(response))
}
