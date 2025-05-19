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
    // Create Push Connector instance with hybrid architecture
    let mut connector = PushConnector::new();

    // Initialize data plane (but don't start it yet)
    if let Err(e) = connector.init_data_plane() {
        tracing::error!("Failed to initialize Push data plane: {}", e);
    }

    let connector = Arc::new(connector);

    // Add management plane routes to the API router
    Router::new()
        // Data routes
        .route("/:topic", post(handle_push))

        // Topic management routes
        .route("/topics", get(handle_get_topics))
        .route("/topics", post(handle_create_topic))
        .route("/topics/:topic", get(handle_get_topic_info))
        .route("/topics/:topic", delete(handle_delete_topic))

        // Monitoring routes
        .route("/health", get(handle_health_check))
        .route("/metrics", get(handle_get_metrics))

        // Service discovery routes
        .route("/services", get(handle_get_services))
        .route("/services/:id", get(handle_get_service))

        // Protocol management routes
        .route("/protocols", get(handle_get_protocols))
        .route("/protocols/:protocol", post(handle_enable_protocol))
        .route("/protocols/:protocol", delete(handle_disable_protocol))

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
    State(connector): State<Arc<PushConnector>>,
) -> impl IntoResponse {
    // Get health status from management plane
    let health = connector.management_plane().health_check();

    // Return health status
    (
        axum::http::StatusCode::OK,
        Json(health),
    )
}

/// Handle get metrics request
#[axum::debug_handler]
pub async fn handle_get_metrics(
    State(connector): State<Arc<PushConnector>>,
) -> impl IntoResponse {
    // Get metrics from metrics manager
    if let Some(metrics_manager) = connector.metrics_manager() {
        let metrics = metrics_manager.get_all_metrics();
        (axum::http::StatusCode::OK, Json(metrics)).into_response()
    } else {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Metrics manager not available"
            })),
        ).into_response()
    }
}

/// Handle get services request
#[axum::debug_handler]
pub async fn handle_get_services(
    State(connector): State<Arc<PushConnector>>,
) -> impl IntoResponse {
    // Get services from service registry
    match connector.service_registry().get_all_services() {
        Ok(services) => (axum::http::StatusCode::OK, Json(services)).into_response(),
        Err(e) => {
            tracing::error!("Failed to get services: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to get services: {}", e)
                })),
            ).into_response()
        }
    }
}

/// Handle get service request
#[axum::debug_handler]
pub async fn handle_get_service(
    State(connector): State<Arc<PushConnector>>,
    Path(service_id): Path<String>,
) -> impl IntoResponse {
    // Get service from service registry
    match connector.service_registry().get_service(&service_id) {
        Ok(service) => (axum::http::StatusCode::OK, Json(service)).into_response(),
        Err(e) => {
            tracing::error!("Failed to get service: {}", e);
            (
                axum::http::StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!("Failed to get service: {}", e)
                })),
            ).into_response()
        }
    }
}

/// Handle get protocols request
#[axum::debug_handler]
pub async fn handle_get_protocols(
    State(connector): State<Arc<PushConnector>>,
) -> impl IntoResponse {
    // Get data plane
    if let Some(data_plane) = connector.data_plane() {
        // Get enabled protocols
        let protocols: Vec<String> = data_plane.enabled_protocols()
            .iter()
            .map(|p| p.as_str().to_string())
            .collect();

        (axum::http::StatusCode::OK, Json(protocols)).into_response()
    } else {
        (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({
                "protocols": ["http"],
                "note": "Data plane not initialized, only HTTP protocol is available"
            })),
        ).into_response()
    }
}

/// Handle enable protocol request
#[axum::debug_handler]
pub async fn handle_enable_protocol(
    State(connector): State<Arc<PushConnector>>,
    Path(protocol): Path<String>,
) -> impl IntoResponse {
    // Convert protocol string to ProtocolType
    let protocol_type = match arroyo_connectors::push::dataplane::ProtocolType::from_str(&protocol) {
        Ok(p) => p,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Invalid protocol: {}", e)
                })),
            ).into_response();
        }
    };

    // Since we can't get a mutable reference to the connector through the State extractor,
    // we need to use a different approach. We'll create a new connector instance and
    // update the protocol configuration through a separate API call.

    // For now, we'll just return a message indicating that this operation is not supported
    // in the current implementation.
    (
        axum::http::StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({
            "error": "Protocol management through the API is not implemented yet. Please update the configuration file to enable protocols.",
            "requested_protocol": protocol
        })),
    ).into_response()
}

/// Handle disable protocol request
#[axum::debug_handler]
pub async fn handle_disable_protocol(
    State(connector): State<Arc<PushConnector>>,
    Path(protocol): Path<String>,
) -> impl IntoResponse {
    // Convert protocol string to ProtocolType
    let protocol_type = match arroyo_connectors::push::dataplane::ProtocolType::from_str(&protocol) {
        Ok(p) => p,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Invalid protocol: {}", e)
                })),
            ).into_response();
        }
    };

    // Since we can't get a mutable reference to the connector through the State extractor,
    // we need to use a different approach. We'll create a new connector instance and
    // update the protocol configuration through a separate API call.

    // For now, we'll just return a message indicating that this operation is not supported
    // in the current implementation.
    (
        axum::http::StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({
            "error": "Protocol management through the API is not implemented yet. Please update the configuration file to disable protocols.",
            "requested_protocol": protocol
        })),
    ).into_response()
}
