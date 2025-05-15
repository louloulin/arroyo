use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::push::source::PushMessage;

/// HTTP server state
pub struct HttpServerState {
    pub message_tx: mpsc::Sender<PushMessage>,
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
        let state = Arc::new(HttpServerState { message_tx });

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
            .route("/api/v1/push/health", get(Self::handle_health_check))
            .with_state(state);

        // Start server
        info!("Starting HTTP server on {}", self.config.addr);
        let server = axum::Server::bind(&self.config.addr).serve(app.into_make_service());

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
            topic: topic.clone(),
            data: body.to_vec(),
            timestamp: SystemTime::now(),
        };

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
    async fn handle_get_topics(State(_state): State<Arc<HttpServerState>>) -> impl IntoResponse {
        // TODO: Implement get topics functionality
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "topics": []
            })),
        )
    }

    /// Handle create topic request
    async fn handle_create_topic(
        State(_state): State<Arc<HttpServerState>>,
        Json(payload): Json<serde_json::Value>,
    ) -> impl IntoResponse {
        // TODO: Implement create topic functionality
        (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "success": true,
                "message": "Topic created"
            })),
        )
    }

    /// Handle get topic info request
    async fn handle_get_topic_info(
        State(_state): State<Arc<HttpServerState>>,
        Path(topic): Path<String>,
    ) -> impl IntoResponse {
        // TODO: Implement get topic info functionality
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "topic": topic,
                "messages": 0,
                "created_at": SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            })),
        )
    }

    /// Handle delete topic request
    async fn handle_delete_topic(
        State(_state): State<Arc<HttpServerState>>,
        Path(topic): Path<String>,
    ) -> impl IntoResponse {
        // TODO: Implement delete topic functionality
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Topic {} deleted", topic)
            })),
        )
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
}
