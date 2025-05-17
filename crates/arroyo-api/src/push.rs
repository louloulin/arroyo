use axum::{
    routing::{get, post, delete},
    Router,
};
use std::sync::Arc;

use arroyo_connectors::push::{PushConnector, api};

/// Create Push routes
pub fn create_push_routes() -> (Router, Arc<PushConnector>) {
    // Create Push Connector instance
    let connector = Arc::new(PushConnector::new());
    
    // Create routes
    let routes = Router::new()
        .route("/push/:topic", post(api::handle_push))
        .route("/push/topics", get(api::handle_get_topics))
        .route("/push/topics", post(api::handle_create_topic))
        .route("/push/topics/:topic", get(api::handle_get_topic_info))
        .route("/push/topics/:topic", delete(api::handle_delete_topic))
        .route("/push/messages/:topic", get(api::handle_query_messages))
        .route("/push/health", get(api::handle_health_check))
        .with_state(connector.clone());
    
    (routes, connector)
}
