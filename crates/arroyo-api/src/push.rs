use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::{get, post, delete},
    Json, Router,
};
use arroyo_connectors::push::PushConnector;
use crate::rest::AppState;

/// Create Push routes
pub fn create_push_routes() -> Router<AppState> {
    // Create Push Connector instance
    let connector = Arc::new(PushConnector::new());

    // Add Push routes to the API router
    Router::new()
        .route("/:topic", post(handle_push))
        .route("/topics", get(handle_get_topics))
        .route("/topics", post(handle_create_topic))
        .route("/topics/:topic", get(handle_get_topic_info))
        .route("/topics/:topic", delete(handle_delete_topic))
        .route("/health", get(handle_health_check))
        .with_state(connector)
}

/// Handle push requests
#[axum::debug_handler]
pub async fn handle_push(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // Delegate to the Push connector's API handler
    connector.handle_push(&topic, body).await
}

/// Handle get topics request
#[axum::debug_handler]
pub async fn handle_get_topics(
    State(connector): State<Arc<PushConnector>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // Get connection ID from query parameters (optional)
    let _connection_id = params.get("connectionId");

    // Get topics from topic manager
    match connector.topic_manager().get_topics() {
        Ok(topics) => Json(topics).into_response(),
        Err(e) => {
            tracing::error!("Failed to get topics: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to get topics: {}", e)
                })),
            ).into_response()
        }
    }
}

/// Handle create topic request
#[axum::debug_handler]
pub async fn handle_create_topic(
    State(connector): State<Arc<PushConnector>>,
    Json(payload): Json<arroyo_connectors::push::topic::CreateTopicRequest>,
) -> impl IntoResponse {
    // Create topic
    match connector.topic_manager().create_topic(payload) {
        Ok(topic) => (axum::http::StatusCode::CREATED, Json(topic)).into_response(),
        Err(e) => {
            let status = match e {
                arroyo_connectors::push::topic::TopicError::TopicAlreadyExists(_) => axum::http::StatusCode::CONFLICT,
                arroyo_connectors::push::topic::TopicError::InvalidTopicName(_) => axum::http::StatusCode::BAD_REQUEST,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };

            (
                status,
                Json(serde_json::json!({
                    "error": format!("{}", e)
                })),
            ).into_response()
        }
    }
}

/// Handle get topic info request
#[axum::debug_handler]
pub async fn handle_get_topic_info(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    // Get topic info
    match connector.topic_manager().get_topic(&topic) {
        Ok(topic) => Json(topic).into_response(),
        Err(e) => {
            let status = match e {
                arroyo_connectors::push::topic::TopicError::TopicNotFound(_) => axum::http::StatusCode::NOT_FOUND,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };

            (
                status,
                Json(serde_json::json!({
                    "error": format!("{}", e)
                })),
            ).into_response()
        }
    }
}

/// Handle delete topic request
#[axum::debug_handler]
pub async fn handle_delete_topic(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    // Delete topic
    match connector.topic_manager().delete_topic(&topic) {
        Ok(_) => (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Topic {} deleted", topic)
            })),
        ).into_response(),
        Err(e) => {
            let status = match e {
                arroyo_connectors::push::topic::TopicError::TopicNotFound(_) => axum::http::StatusCode::NOT_FOUND,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };

            (
                status,
                Json(serde_json::json!({
                    "error": format!("{}", e)
                })),
            ).into_response()
        }
    }
}

/// Handle health check request
#[axum::debug_handler]
pub async fn handle_health_check(
    State(_connector): State<Arc<PushConnector>>,
) -> impl IntoResponse {
    // Return health status
    (
        axum::http::StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "topics": "available"
        })),
    )
}
