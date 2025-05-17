use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use tracing::{debug, error};

use crate::push::source::PushMessage;
use crate::push::topic::{CreateTopicRequest, TopicError};
use crate::push::messages::{MessageQuery};
use crate::push::PushConnector;

/// Handle push requests
pub async fn handle_push(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    debug!("Received push request for topic: {}, size: {}", topic, body.len());

    // Start processing time measurement
    let start_time = std::time::Instant::now();
    let mut success = true;

    // Validate message
    if let Err(e) = connector.message_validator().validate(&topic, &body) {
        let (status, message) = match e {
            crate::push::validator::ValidationError::MessageTooLarge(max_size) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("Message size exceeds maximum allowed size of {} bytes", max_size),
            ),
            crate::push::validator::ValidationError::InvalidTopicName(topic) => (
                StatusCode::BAD_REQUEST,
                format!("Invalid topic name: {}", topic),
            ),
            _ => (
                StatusCode::BAD_REQUEST,
                format!("Validation error: {}", e),
            ),
        };

        error!("Message validation failed: {}", e);

        // Record error in metrics
        if let Some(metrics_manager) = connector.metrics_manager() {
            if let Err(e) = metrics_manager.record_error(&topic) {
                error!("Failed to record error in metrics: {}", e);
            }
        }

        return (
            status,
            Json(serde_json::json!({
                "error": message
            })),
        );
    }

    // Create push message
    let message = PushMessage {
        topic: topic.clone(),
        data: body.to_vec(),
        timestamp: SystemTime::now(),
    };

    // Update topic message count and last activity time
    if let Err(e) = connector.topic_manager().record_message(&topic, body.len()) {
        // If topic doesn't exist, create it with default settings
        if let TopicError::TopicNotFound(_) = e {
            let request = CreateTopicRequest {
                name: topic.clone(),
                retention_period: connector.config().default_retention_period,
                compression: connector.config().default_compression,
            };

            if let Err(e) = connector.topic_manager().create_topic(request) {
                error!("Failed to create topic: {}", e);
                success = false;

                // Record processing time and error
                let processing_time = start_time.elapsed().as_millis() as u64;
                if let Some(metrics_manager) = connector.metrics_manager() {
                    if let Err(e) = metrics_manager.record_message_with_details(&topic, body.len(), processing_time, false) {
                        error!("Failed to update metrics: {}", e);
                    }
                }

                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to create topic: {}", e)
                    })),
                );
            }

            // Try to record message again
            if let Err(e) = connector.topic_manager().record_message(&topic, body.len()) {
                error!("Failed to record message: {}", e);
                success = false;

                // Record processing time and error
                let processing_time = start_time.elapsed().as_millis() as u64;
                if let Some(metrics_manager) = connector.metrics_manager() {
                    if let Err(e) = metrics_manager.record_message_with_details(&topic, body.len(), processing_time, false) {
                        error!("Failed to update metrics: {}", e);
                    }
                }

                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to record message: {}", e)
                    })),
                );
            }
        } else {
            error!("Failed to record message: {}", e);
            success = false;

            // Record processing time and error
            let processing_time = start_time.elapsed().as_millis() as u64;
            if let Some(metrics_manager) = connector.metrics_manager() {
                if let Err(e) = metrics_manager.record_message_with_details(&topic, body.len(), processing_time, false) {
                    error!("Failed to update metrics: {}", e);
                }
            }

            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to record message: {}", e)
                })),
            );
        }
    }

    // Store message for browsing
    if let Some(message_store) = connector.message_store() {
        if let Err(e) = message_store.store_message(&topic, body.to_vec()) {
            error!("Failed to store message: {}", e);
            // Non-fatal error, continue processing
        }
    }

    // Send message to channel
    let result = match connector.send_message(message).await {
        Ok(_) => {
            // Record processing time and success
            let processing_time = start_time.elapsed().as_millis() as u64;
            if let Some(metrics_manager) = connector.metrics_manager() {
                if let Err(e) = metrics_manager.record_message_with_details(&topic, body.len(), processing_time, true) {
                    error!("Failed to update metrics: {}", e);
                }
            }

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "message": "Message received",
                    "timestamp": SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    "processing_time_ms": processing_time
                })),
            )
        }
        Err(e) => {
            error!("Failed to send message: {}", e);
            success = false;

            // Record processing time and error
            let processing_time = start_time.elapsed().as_millis() as u64;
            if let Some(metrics_manager) = connector.metrics_manager() {
                if let Err(e) = metrics_manager.record_message_with_details(&topic, body.len(), processing_time, false) {
                    error!("Failed to update metrics: {}", e);
                }
            }

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to process message: {}", e),
                    "processing_time_ms": processing_time
                })),
            )
        }
    };

    result
}

/// Handle get topics request
pub async fn handle_get_topics(
    State(connector): State<Arc<PushConnector>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // Get connection ID from query parameters (optional)
    let _connection_id = params.get("connectionId");

    // Get topics from topic manager
    match connector.topic_manager().get_topics() {
        Ok(topics) => {
            (StatusCode::OK, Json(topics))
        },
        Err(e) => {
            error!("Failed to get topics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to get topics: {}", e)
                })),
            )
        }
    }
}

/// Handle create topic request
pub async fn handle_create_topic(
    State(connector): State<Arc<PushConnector>>,
    Json(payload): Json<CreateTopicRequest>,
) -> impl IntoResponse {
    // Create topic
    match connector.topic_manager().create_topic(payload) {
        Ok(topic) => {
            (StatusCode::CREATED, Json(topic))
        },
        Err(e) => {
            let status = match e {
                TopicError::TopicAlreadyExists(_) => StatusCode::CONFLICT,
                TopicError::InvalidTopicName(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            (
                status,
                Json(serde_json::json!({
                    "error": format!("{}", e)
                })),
            )
        }
    }
}

/// Handle get topic info request
pub async fn handle_get_topic_info(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    // Get topic info
    match connector.topic_manager().get_topic(&topic) {
        Ok(topic) => {
            (StatusCode::OK, Json(topic))
        },
        Err(e) => {
            let status = match e {
                TopicError::TopicNotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            (
                status,
                Json(serde_json::json!({
                    "error": format!("{}", e)
                })),
            )
        }
    }
}

/// Handle delete topic request
pub async fn handle_delete_topic(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    // Delete topic
    match connector.topic_manager().delete_topic(&topic) {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Topic {} deleted", topic)
            })),
        ),
        Err(e) => {
            let status = match e {
                TopicError::TopicNotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            (
                status,
                Json(serde_json::json!({
                    "error": format!("{}", e)
                })),
            )
        }
    }
}

/// Handle query messages request
pub async fn handle_query_messages(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // Parse query parameters
    let limit = params.get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100);
    let offset = params.get("offset")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let start_time = params.get("start_time")
        .and_then(|s| s.parse::<u64>().ok());
    let end_time = params.get("end_time")
        .and_then(|s| s.parse::<u64>().ok());

    // Create query
    let query = MessageQuery {
        topic: topic.clone(),
        limit,
        offset,
        start_time,
        end_time,
    };

    // Query messages
    if let Some(message_store) = connector.message_store() {
        match message_store.query_messages(&query) {
            Ok(messages) => {
                (StatusCode::OK, Json(messages))
            },
            Err(e) => {
                error!("Failed to query messages: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to query messages: {}", e)
                    })),
                )
            }
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Message store not available"
            })),
        )
    }
}

/// Handle health check request
pub async fn handle_health_check() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION")
        })),
    )
}

/// Handle get metrics request
pub async fn handle_get_metrics(
    State(connector): State<Arc<PushConnector>>,
) -> impl IntoResponse {
    if let Some(metrics_manager) = connector.metrics_manager() {
        match metrics_manager.get_all_metrics() {
            Ok(metrics) => {
                (StatusCode::OK, Json(metrics))
            },
            Err(e) => {
                error!("Failed to get metrics: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to get metrics: {}", e)
                    })),
                )
            }
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Metrics manager not available"
            })),
        )
    }
}

/// Handle get topic metrics request
pub async fn handle_get_topic_metrics(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    if let Some(metrics_manager) = connector.metrics_manager() {
        match metrics_manager.get_topic_metrics(&topic) {
            Ok(Some(metrics)) => {
                (StatusCode::OK, Json(metrics))
            },
            Ok(None) => {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": format!("Topic {} not found", topic)
                    })),
                )
            },
            Err(e) => {
                error!("Failed to get topic metrics: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to get topic metrics: {}", e)
                    })),
                )
            }
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Metrics manager not available"
            })),
        )
    }
}
