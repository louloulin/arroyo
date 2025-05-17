use axum::{
    routing::{get, post, delete},
};
use std::sync::Arc;

use arroyo_connectors::push::{PushConnector, api};

/// Create Push routes
pub fn create_push_routes() -> Vec<(&'static str, axum::routing::MethodRouter)> {
    // Create Push Connector instance
    let connector = Arc::new(PushConnector::new());

    // Create routes
    vec![
        ("/push/:topic", post(api::handle_push).with_state(connector.clone())),
        ("/push/topics", get(api::handle_get_topics).with_state(connector.clone())),
        ("/push/topics", post(api::handle_create_topic).with_state(connector.clone())),
        ("/push/topics/:topic", get(api::handle_get_topic_info).with_state(connector.clone())),
        ("/push/topics/:topic", delete(api::handle_delete_topic).with_state(connector.clone())),
        ("/push/messages/:topic", get(api::handle_query_messages).with_state(connector.clone())),
        ("/push/metrics", get(api::handle_get_metrics).with_state(connector.clone())),
        ("/push/metrics/:topic", get(api::handle_get_topic_metrics).with_state(connector.clone())),
        ("/push/health", get(api::handle_health_check).with_state(connector)),
    ]
}
