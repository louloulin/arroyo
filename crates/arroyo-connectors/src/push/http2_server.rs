use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Json, Router,
};
#[cfg(feature = "http2")]
use hyper::server::conn::AddrIncoming;
#[cfg(feature = "http2")]
use hyper::server::Server;
#[cfg(feature = "http2")]
use hyper::service::{make_service_fn, service_fn};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::push::management::PushManagementPlane;
use crate::push::source::PushMessage;

/// HTTP/2 server state
pub struct Http2ServerState {
    pub message_tx: mpsc::Sender<PushMessage>,
    pub management_plane: Arc<PushManagementPlane>,
}

/// HTTP/2 server configuration
#[derive(Debug, Clone)]
pub struct Http2ServerConfig {
    pub addr: SocketAddr,
    pub timeout: u64,
    pub max_connections: usize,
}

impl Default for Http2ServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:8080".parse().unwrap(),
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

/// HTTP/2 server for push connector
pub struct Http2Server {
    pub config: Http2ServerConfig,
    pub server_handle: Option<JoinHandle<Result<(), anyhow::Error>>>,
}

impl Http2Server {
    /// Create a new HTTP/2 server
    pub fn new(config: Http2ServerConfig) -> Self {
        Self {
            config,
            server_handle: None,
        }
    }

    /// Start the HTTP/2 server
    pub async fn start(
        &mut self,
        message_tx: mpsc::Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        let state = Arc::new(Http2ServerState {
            message_tx,
            management_plane,
        });

        // Create router with routes
        let app = Router::new()
            .route("/api/v1/push/:topic", post(Self::handle_push))
            .with_state(state);

        // Create HTTP/2 server
        info!("Starting HTTP/2 server on {}", self.config.addr);

        #[cfg(feature = "http2")]
        {
            // Create incoming connections
            let incoming = AddrIncoming::bind(&self.config.addr)
                .map_err(|e| anyhow::anyhow!("Failed to bind to address: {}", e))?;

            // Create server with HTTP/2 support
            let server = Server::builder(incoming)
                .http2_only(true) // Enable HTTP/2 only
                .serve(app.into_make_service());

            // Store server handle
            let handle = tokio::spawn(async move {
                server.await.map_err(|e| anyhow::anyhow!("HTTP/2 server error: {}", e))?;
                Ok(())
            });

            self.server_handle = Some(handle);
        }

        #[cfg(not(feature = "http2"))]
        {
            // Use standard Axum server without HTTP/2 support
            let addr = self.config.addr;
            let handle = tokio::spawn(async move {
                let listener = tokio::net::TcpListener::bind(addr).await
                    .map_err(|e| anyhow::anyhow!("Failed to bind to address: {}", e))?;

                axum::serve(listener, app)
                    .await
                    .map_err(|e| anyhow::anyhow!("HTTP server error: {}", e))?;
                Ok(())
            });

            self.server_handle = Some(handle);
        }

        Ok(())
    }

    /// Stop the HTTP/2 server
    pub async fn stop(&mut self) -> Result<(), anyhow::Error> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            info!("HTTP/2 server stopped");
        }
        Ok(())
    }

    /// Handle push requests
    async fn handle_push(
        State(state): State<Arc<Http2ServerState>>,
        Path(topic): Path<String>,
        body: axum::body::Bytes,
    ) -> impl IntoResponse {
        debug!("Received push request for topic: {}, size: {}", topic, body.len());

        // Check if topic exists
        if let Err(e) = state.management_plane.get_topic(&topic) {
            // If topic doesn't exist, create it with default settings
            if let crate::push::topic::TopicError::TopicNotFound(_) = e {
                let request = crate::push::topic::CreateTopicRequest {
                    name: topic.clone(),
                    retention_period: 7 * 24 * 60 * 60, // 7 days
                    compression: false,
                };

                if let Err(e) = state.management_plane.create_topic(request) {
                    error!("Failed to create topic: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(PushResponse {
                            success: false,
                            message: format!("Failed to create topic: {}", e),
                            timestamp: SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        }),
                    );
                }
            } else {
                error!("Failed to get topic: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(PushResponse {
                        success: false,
                        message: format!("Failed to get topic: {}", e),
                        timestamp: SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    }),
                );
            }
        }

        // Validate message
        if let Err(e) = state.management_plane.message_validator().validate(&topic, &body) {
            error!("Message validation failed: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(PushResponse {
                    success: false,
                    message: format!("Message validation failed: {}", e),
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                }),
            );
        }

        // Record message
        if let Err(e) = state.management_plane.record_message(&topic, body.len()) {
            error!("Failed to record message: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PushResponse {
                    success: false,
                    message: format!("Failed to record message: {}", e),
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                }),
            );
        }

        // Create push message
        let message = PushMessage {
            id: 0, // Will be assigned by the message store
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
}
