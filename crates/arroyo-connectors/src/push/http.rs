use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use axum::{
    extract::{Path, State, Query},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, delete},
    Json, Router,
};
use axum_server::Server;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::push::source::PushMessage;
use crate::push::topic::{CreateTopicRequest, TopicError};
use crate::push::metrics::TopicMetricsManager;
use crate::push::messages::{MessageStore, MessageQuery, MessageData};

/// HTTP server state
pub struct HttpServerState {
    pub message_tx: mpsc::Sender<PushMessage>,
    pub topic_manager: Arc<crate::push::topic::TopicManager>,
    pub metrics_manager: Arc<crate::push::metrics::TopicMetricsManager>,
    pub message_store: Arc<crate::push::messages::MessageStore>,
}

/// HTTP server configuration
#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub addr: SocketAddr,
    pub timeout: u64,
    pub max_connections: usize,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:8000".parse().unwrap(),
            timeout: 30,
            max_connections: 100,
        }
    }
}

/// Response for push requests
#[derive(Debug, Serialize, Deserialize)]
pub struct PushResponse {
    pub success: bool,
    pub message: String,
    pub timestamp: u64,
}

/// HTTP server for push connector
pub struct HttpServer {
    pub config: HttpServerConfig,
    pub server_handle: Option<JoinHandle<Result<(), anyhow::Error>>>,
}

impl HttpServer {
    /// Create a new HTTP server
    pub fn new(config: HttpServerConfig) -> Self {
        Self {
            config,
            server_handle: None,
        }
    }

    /// Start the HTTP server
    pub async fn start(
        &mut self,
        message_tx: mpsc::Sender<PushMessage>,
    ) -> Result<(), anyhow::Error> {
        let state = Arc::new(HttpServerState {
            message_tx,
            topic_manager: Arc::new(crate::push::topic::TopicManager::new()),
            metrics_manager: Arc::new(crate::push::metrics::TopicMetricsManager::new()),
            message_store: Arc::new(crate::push::messages::MessageStore::new(1000)), // Store up to 1000 messages per topic
        });

        // Create router with routes
        let app = Router::new()
            .route("/api/v1/push/:topic", post(Self::handle_push))
            .route("/api/v1/push/topics", get(Self::handle_get_topics))
            .route("/api/v1/push/topics", post(Self::handle_create_topic))
            .route(
                "/api/v1/push/topics/:topic",
                get(Self::handle_get_topic_info),
            )
            .route("/api/v1/push/topics/:topic", delete(Self::handle_delete_topic))
            .route("/api/v1/push/metrics", get(Self::handle_get_metrics))
            .route("/api/v1/push/metrics/:topic", get(Self::handle_get_topic_metrics))
            .route("/api/v1/push/messages/:topic", get(Self::handle_query_messages))
            .route("/api/v1/push/messages/:topic/:id", get(Self::handle_get_message))
            .route("/api/v1/push/messages/:topic", delete(Self::handle_clear_messages))
            .route("/api/v1/health", get(Self::handle_health_check))
            .with_state(state);

        // Start server
        info!("Starting HTTP server on {}", self.config.addr);
        let server = axum_server::bind(self.config.addr).serve(app.into_make_service());

        // Store server handle
        let handle = tokio::spawn(async move {
            server.await.map_err(|e| anyhow::anyhow!("HTTP server error: {}", e))?;
            Ok(())
        });

        self.server_handle = Some(handle);
        Ok(())
    }

    /// Stop the HTTP server
    pub async fn stop(&mut self) -> Result<(), anyhow::Error> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            info!("HTTP server stopped");
        }
        Ok(())
    }

    /// Handle push requests
    async fn handle_push(
        State(state): State<Arc<HttpServerState>>,
        Path(topic): Path<String>,
        body: axum::body::Bytes,
    ) -> impl IntoResponse {
        debug!("Received push request for topic: {}, size: {}", topic, body.len());

        // Create push message
        let message = PushMessage {
            id: 0, // Will be assigned by the message store
            topic: topic.clone(),
            data: body.to_vec(),
            timestamp: SystemTime::now(),
        };

        // Update topic message count and last activity time
        if let Err(e) = state.topic_manager.record_message(&topic, body.len()) {
            // If topic doesn't exist, create it with default settings
            if let TopicError::TopicNotFound(_) = e {
                let request = CreateTopicRequest {
                    name: topic.clone(),
                    retention_period: 7 * 24 * 60 * 60, // 7 days
                    compression: false,
                };

                if let Err(e) = state.topic_manager.create_topic(request) {
                    error!("Failed to create topic: {}", e);
                }

                // Try to record message again
                if let Err(e) = state.topic_manager.record_message(&topic, body.len()) {
                    error!("Failed to record message: {}", e);
                }
            } else {
                error!("Failed to record message: {}", e);
            }
        }

        // Update metrics
        if let Err(e) = state.metrics_manager.record_message(&topic, body.len()) {
            error!("Failed to update metrics: {}", e);
        }

        // Store message for browsing
        if let Err(e) = state.message_store.store_message(&topic, body.to_vec()) {
            error!("Failed to store message: {}", e);
        }

        // Send message to channel
        match state.message_tx.send(message).await {
            Ok(_) => {
                let response = PushResponse {
                    success: true,
                    message: "Message received".to_string(),
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };
                (StatusCode::OK, Json(response))
            }
            Err(e) => {
                error!("Failed to send message: {}", e);
                let response = PushResponse {
                    success: false,
                    message: format!("Failed to process message: {}", e),
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };
                (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
            }
        }
    }

    /// Handle get topics request
    async fn handle_get_topics(
        State(state): State<Arc<HttpServerState>>,
        Query(params): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        // Get connection ID from query parameters (optional)
        let _connection_id = params.get("connectionId");

        // Get topics from topic manager
        match state.topic_manager.get_topics() {
            Ok(topics) => {
                let topics_json = serde_json::to_value(topics).unwrap_or(serde_json::json!([]));
                (StatusCode::OK, Json(topics_json))
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
    async fn handle_create_topic(
        State(state): State<Arc<HttpServerState>>,
        Json(payload): Json<CreateTopicRequest>,
    ) -> impl IntoResponse {
        // Create topic
        match state.topic_manager.create_topic(payload) {
            Ok(topic) => {
                let topic_json = serde_json::to_value(topic).unwrap_or(serde_json::json!({}));
                (StatusCode::CREATED, Json(topic_json))
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
    async fn handle_get_topic_info(
        State(state): State<Arc<HttpServerState>>,
        Path(topic): Path<String>,
    ) -> impl IntoResponse {
        // Get topic info
        match state.topic_manager.get_topic(&topic) {
            Ok(topic) => {
                let topic_json = serde_json::to_value(topic).unwrap_or(serde_json::json!({}));
                (StatusCode::OK, Json(topic_json))
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
    async fn handle_delete_topic(
        State(state): State<Arc<HttpServerState>>,
        Path(topic): Path<String>,
    ) -> impl IntoResponse {
        // Delete topic
        match state.topic_manager.delete_topic(&topic) {
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

    /// Handle health check request
    async fn handle_health_check() -> impl IntoResponse {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "ok",
                "version": env!("CARGO_PKG_VERSION")
            })),
        )
    }

    /// Handle get metrics request
    async fn handle_get_metrics(
        State(state): State<Arc<HttpServerState>>,
    ) -> impl IntoResponse {
        // Get metrics for all topics
        match state.metrics_manager.get_all_metrics() {
            Ok(metrics) => {
                let metrics_json = serde_json::to_value(metrics).unwrap_or(serde_json::json!([]));
                (StatusCode::OK, Json(metrics_json))
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
    }

    /// Handle get topic metrics request
    async fn handle_get_topic_metrics(
        State(state): State<Arc<HttpServerState>>,
        Path(topic): Path<String>,
    ) -> impl IntoResponse {
        // Get metrics for a specific topic
        match state.metrics_manager.get_topic_metrics(&topic) {
            Ok(Some(metrics)) => {
                let metrics_json = serde_json::to_value(metrics).unwrap_or(serde_json::json!({}));
                (StatusCode::OK, Json(metrics_json))
            },
            Ok(None) => {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": format!("Topic not found: {}", topic)
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
    }

    /// Handle query messages request
    async fn handle_query_messages(
        State(state): State<Arc<HttpServerState>>,
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
        match state.message_store.query_messages(&query) {
            Ok(messages) => {
                let messages_json = serde_json::to_value(messages).unwrap_or(serde_json::json!([]));
                (StatusCode::OK, Json(messages_json))
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
    }

    /// Handle get message request
    async fn handle_get_message(
        State(state): State<Arc<HttpServerState>>,
        Path((topic, id)): Path<(String, String)>,
    ) -> impl IntoResponse {
        // Parse message ID
        let id = match id.parse::<u64>() {
            Ok(id) => id,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("Invalid message ID: {}", id)
                    })),
                );
            }
        };

        // Get message
        match state.message_store.get_message(&topic, id) {
            Ok(Some(message)) => {
                let message_json = serde_json::to_value(message).unwrap_or(serde_json::json!({}));
                (StatusCode::OK, Json(message_json))
            },
            Ok(None) => {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": format!("Message not found: {}", id)
                    })),
                )
            },
            Err(e) => {
                error!("Failed to get message: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to get message: {}", e)
                    })),
                )
            }
        }
    }

    /// Handle clear messages request
    async fn handle_clear_messages(
        State(state): State<Arc<HttpServerState>>,
        Path(topic): Path<String>,
    ) -> impl IntoResponse {
        // Clear messages
        match state.message_store.clear_messages(&topic) {
            Ok(_) => {
                (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "success": true,
                        "message": format!("Messages cleared for topic: {}", topic)
                    })),
                )
            },
            Err(e) => {
                error!("Failed to clear messages: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Failed to clear messages: {}", e)
                    })),
                )
            }
        }
    }
}
