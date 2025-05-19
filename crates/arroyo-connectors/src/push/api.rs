use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
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
) -> Response {
    debug!("Received push request for topic: {}, size: {}", topic, body.len());

    // Start processing time measurement
    let start_time = std::time::Instant::now();

    // Validate message
    if let Some(validator) = connector.message_validator() {
        if let Err(e) = validator.validate(&topic, &body) {
        let (status, message) = match e {
            crate::push::validator::ValidationError::MessageTooLarge(max_size) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("Message size exceeds maximum allowed size of {} bytes", max_size),
            ),
            crate::push::validator::ValidationError::InvalidTopicName(ref topic) => (
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
        ).into_response();
        }
    }

    // Create push message
    let message = PushMessage {
        id: 0, // Will be assigned by the message store
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
                ).into_response();
            }

            // Try to record message again
            if let Err(e) = connector.topic_manager().record_message(&topic, body.len()) {
                error!("Failed to record message: {}", e);

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
                ).into_response();
            }
        } else {
            error!("Failed to record message: {}", e);

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
            ).into_response();
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
    match connector.send_message(message).await {
        Ok(_) => {
            // Record processing time and success
            let processing_time = start_time.elapsed().as_millis() as u64;
            if let Some(metrics_manager) = connector.metrics_manager() {
                if let Err(e) = metrics_manager.record_message_with_details(&topic, body.len(), processing_time, true) {
                    error!("Failed to update metrics: {}", e);
                }
            }

            // Get the message ID and status from the message store
            let message_id = if let Some(message_store) = connector.message_store() {
                // Get the latest message for this topic
                let query = MessageQuery {
                    topic: topic.clone(),
                    limit: 1,
                    offset: 0,
                    start_time: None,
                    end_time: None,
                };

                match message_store.query_messages(&query) {
                    Ok(messages) if !messages.is_empty() => {
                        let message = &messages[0];
                        Some(serde_json::json!({
                            "id": message.id,
                            "status": message.status,
                            "timestamp": message.timestamp
                        }))
                    },
                    _ => None,
                }
            } else {
                None
            };

            let mut response = serde_json::json!({
                "success": true,
                "message": "Message received",
                "timestamp": SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                "processing_time_ms": processing_time
            });

            // Add message ID and status if available
            if let Some(message_info) = message_id {
                response["message_id"] = message_info["id"].clone();
                response["message_status"] = message_info["status"].clone();
            }

            (
                StatusCode::OK,
                Json(response),
            ).into_response()
        }
        Err(e) => {
            error!("Failed to send message: {}", e);

            // Record processing time and error
            let processing_time = start_time.elapsed().as_millis() as u64;
            if let Some(metrics_manager) = connector.metrics_manager() {
                if let Err(e) = metrics_manager.record_message_with_details(&topic, body.len(), processing_time, false) {
                    error!("Failed to update metrics: {}", e);
                }
            }

            // Get the message ID and status from the message store
            let message_id = if let Some(message_store) = connector.message_store() {
                // Get the latest message for this topic
                let query = MessageQuery {
                    topic: topic.clone(),
                    limit: 1,
                    offset: 0,
                    start_time: None,
                    end_time: None,
                };

                match message_store.query_messages(&query) {
                    Ok(messages) if !messages.is_empty() => {
                        let message = &messages[0];
                        Some(serde_json::json!({
                            "id": message.id,
                            "status": message.status,
                            "error": message.error
                        }))
                    },
                    _ => None,
                }
            } else {
                None
            };

            let mut response = serde_json::json!({
                "error": format!("Failed to process message: {}", e),
                "processing_time_ms": processing_time
            });

            // Add message ID and status if available
            if let Some(message_info) = message_id {
                response["message_id"] = message_info["id"].clone();
                response["message_status"] = message_info["status"].clone();
                if message_info["error"].is_string() {
                    response["message_error"] = message_info["error"].clone();
                }
            }

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(response),
            ).into_response()
        }
    }
}

/// Handle get topics request
pub async fn handle_get_topics(
    State(connector): State<Arc<PushConnector>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // Get connection ID from query parameters (optional)
    let _connection_id = params.get("connectionId");

    // Get topics from topic manager
    match connector.topic_manager().get_topics() {
        Ok(topics) => {
            (StatusCode::OK, Json(topics)).into_response()
        },
        Err(e) => {
            error!("Failed to get topics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to get topics: {}", e)
                })),
            ).into_response()
        }
    }
}

/// Handle create topic request
pub async fn handle_create_topic(
    State(connector): State<Arc<PushConnector>>,
    Json(payload): Json<CreateTopicRequest>,
) -> Response {
    // Create topic
    match connector.topic_manager().create_topic(payload) {
        Ok(topic) => {
            (StatusCode::CREATED, Json(topic)).into_response()
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
            ).into_response()
        }
    }
}

/// Handle get topic info request
pub async fn handle_get_topic_info(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
) -> Response {
    // Get topic info
    match connector.topic_manager().get_topic(&topic) {
        Ok(topic) => {
            (StatusCode::OK, Json(topic)).into_response()
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
            ).into_response()
        }
    }
}

/// Handle delete topic request
pub async fn handle_delete_topic(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
) -> Response {
    // Delete topic
    match connector.topic_manager().delete_topic(&topic) {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Topic {} deleted", topic)
            })),
        ).into_response(),
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
            ).into_response()
        }
    }
}

/// Handle query messages request
pub async fn handle_query_messages(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
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
                (StatusCode::OK, Json(messages)).into_response()
            },
            Err(e) => {
                error!("Failed to query messages: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to query messages: {}", e)
                    })),
                ).into_response()
            }
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Message store not available"
            })),
        ).into_response()
    }
}

/// Handle health check request
pub async fn handle_health_check() -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION")
        })),
    ).into_response()
}

/// Handle get metrics request
pub async fn handle_get_metrics(
    State(connector): State<Arc<PushConnector>>,
) -> Response {
    if let Some(metrics_manager) = connector.metrics_manager() {
        match metrics_manager.get_all_metrics() {
            Ok(metrics) => {
                (StatusCode::OK, Json(metrics)).into_response()
            },
            Err(e) => {
                error!("Failed to get metrics: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to get metrics: {}", e)
                    })),
                ).into_response()
            }
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Metrics manager not available"
            })),
        ).into_response()
    }
}

/// Handle get topic metrics request
pub async fn handle_get_topic_metrics(
    State(connector): State<Arc<PushConnector>>,
    Path(topic): Path<String>,
) -> Response {
    if let Some(metrics_manager) = connector.metrics_manager() {
        match metrics_manager.get_topic_metrics(&topic) {
            Ok(Some(metrics)) => {
                (StatusCode::OK, Json(metrics)).into_response()
            },
            Ok(None) => {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": format!("Topic {} not found", topic)
                    })),
                ).into_response()
            },
            Err(e) => {
                error!("Failed to get topic metrics: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to get topic metrics: {}", e)
                    })),
                ).into_response()
            }
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Metrics manager not available"
            })),
        ).into_response()
    }
}
