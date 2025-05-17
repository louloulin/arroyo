use crate::rest::AppState;
use axum::{
    routing::{get, post},
    Router,
};

/// Create Push routes
pub fn create_push_routes() -> Router<AppState> {
    // Add Push routes to the API router
    Router::new()
        .route("/api/v1/push/topics", get(handle_get_topics))
        .route("/api/v1/push/metrics", get(handle_get_metrics))
}

// Simple handler functions for testing
async fn handle_get_topics() -> &'static str {
    "[]"
}

async fn handle_get_metrics() -> &'static str {
    "{}"
}
