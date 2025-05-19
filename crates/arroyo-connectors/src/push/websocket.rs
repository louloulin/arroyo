use std::sync::Arc;
use std::collections::HashMap;
use std::time::SystemTime;
use std::net::SocketAddr;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info};

#[cfg(feature = "websocket")]
use futures_util::{SinkExt, StreamExt};
#[cfg(feature = "websocket")]
use tokio_tungstenite::{
    accept_async,
    tungstenite::protocol::Message,
};

use crate::push::source::PushMessage;
use crate::push::management::PushManagementPlane;
use crate::push::protocol::ProtocolAdapter;
use crate::push::dataplane::ProtocolType;

/// WebSocket protocol adapter
pub struct WebSocketAdapter {
    /// Message sender
    message_tx: Sender<PushMessage>,

    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,

    /// Configuration
    config: HashMap<String, String>,

    /// WebSocket server
    server: Option<WebSocketServer>,
}

impl WebSocketAdapter {
    /// Create a new WebSocket adapter
    pub fn new(
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
        config: HashMap<String, String>,
    ) -> Self {
        Self {
            message_tx,
            management_plane,
            config,
            server: None,
        }
    }

    /// Get the server address
    fn server_address(&self) -> Result<SocketAddr> {
        let host = self.config.get("host").cloned().unwrap_or_else(|| "127.0.0.1".to_string());
        let port = self.config.get("port").cloned().unwrap_or_else(|| "8081".to_string());

        let addr = format!("{}:{}", host, port).parse()
            .map_err(|e| anyhow!("Invalid server address: {}", e))?;

        Ok(addr)
    }
}

#[async_trait::async_trait]
impl ProtocolAdapter for WebSocketAdapter {
    fn protocol_type(&self) -> ProtocolType {
        ProtocolType::WebSocket
    }

    async fn start(&mut self) -> Result<()> {
        // Get server address
        let addr = self.server_address()?;

        // Create WebSocket server configuration
        let config = WebSocketServerConfig {
            addr,
            timeout: 30,
            max_connections: 100,
        };

        // Create WebSocket server
        let mut server = WebSocketServer::new(config);

        // Start WebSocket server
        server.start(
            self.message_tx.clone(),
            self.management_plane.clone(),
        ).await?;

        // Store server
        self.server = Some(server);

        info!("WebSocket server started on {}", addr);

        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        // Stop WebSocket server
        if let Some(server) = self.server.as_mut() {
            server.stop().await?;
            info!("WebSocket server stopped");
        }

        // Clear server
        self.server = None;

        Ok(())
    }

    async fn process_message(&self, topic: &str, data: Vec<u8>) -> Result<()> {
        // Validate message
        if let Err(e) = self.management_plane.message_validator().validate(topic, &data) {
            return Err(anyhow!("Message validation failed: {}", e));
        }

        // Record message
        if let Err(e) = self.management_plane.record_message(topic, data.len()) {
            return Err(anyhow!("Failed to record message: {}", e));
        }

        // Create message
        let message = PushMessage {
            id: 0, // Will be assigned by the message store
            topic: topic.to_string(),
            data,
            timestamp: SystemTime::now(),
        };

        // Send message
        self.message_tx.send(message).await.map_err(|e| anyhow!("Failed to send message: {}", e))
    }

    fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    fn update_config(&mut self, config: HashMap<String, String>) -> Result<()> {
        self.config = config;
        Ok(())
    }
}

/// WebSocket server configuration
#[derive(Debug, Clone)]
pub struct WebSocketServerConfig {
    pub addr: SocketAddr,
    pub timeout: u64,
    pub max_connections: usize,
}

impl Default for WebSocketServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:8081".parse().unwrap(),
            timeout: 30,
            max_connections: 100,
        }
    }
}

/// WebSocket server for push connector
pub struct WebSocketServer {
    pub config: WebSocketServerConfig,
    pub server_handle: Option<tokio::task::JoinHandle<Result<(), anyhow::Error>>>,
}

impl WebSocketServer {
    /// Create a new WebSocket server
    pub fn new(config: WebSocketServerConfig) -> Self {
        Self {
            config,
            server_handle: None,
        }
    }

    /// Start the WebSocket server
    pub async fn start(
        &mut self,
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        // Create WebSocket server
        info!("Starting WebSocket server on {}", self.config.addr);

        // Create TCP listener
        let listener = tokio::net::TcpListener::bind(self.config.addr).await
            .map_err(|e| anyhow!("Failed to bind to address: {}", e))?;

        // Clone references for the server task
        let message_tx_clone = message_tx.clone();
        let management_plane_clone = management_plane.clone();

        // Start server in a separate task
        let handle = tokio::spawn(async move {
            Self::run_server(listener, message_tx_clone, management_plane_clone).await
        });

        // Store server handle
        self.server_handle = Some(handle);

        Ok(())
    }

    /// Run the WebSocket server
    async fn run_server(
        listener: tokio::net::TcpListener,
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        info!("WebSocket server running");

        // Accept connections
        loop {
            // Accept a new connection
            let (socket, addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    continue;
                }
            };

            debug!("New WebSocket connection from {}", addr);

            // Clone references for the connection task
            let message_tx_clone = message_tx.clone();
            let management_plane_clone = management_plane.clone();

            // Handle connection in a separate task
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(socket, message_tx_clone, management_plane_clone).await {
                    error!("WebSocket connection error: {}", e);
                }
            });
        }
    }

    /// Handle a WebSocket connection
    async fn handle_connection(
        socket: tokio::net::TcpStream,
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        #[cfg(feature = "websocket")]
        {
            // Accept WebSocket connection
            let ws_stream = accept_async(socket).await
                .map_err(|e| anyhow!("Failed to accept WebSocket connection: {}", e))?;

            debug!("WebSocket connection established");

            // Split the WebSocket stream
            let (mut ws_sender, mut ws_receiver) = ws_stream.split();

            // Handle WebSocket messages
            while let Some(msg) = ws_receiver.next().await {
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                };

                // Handle different message types
                match msg {
                    Message::Text(text) => {
                        debug!("Received text message: {}", text);

                        // Parse message as JSON
                        let parsed: serde_json::Value = match serde_json::from_str(&text) {
                            Ok(parsed) => parsed,
                            Err(e) => {
                                error!("Failed to parse message as JSON: {}", e);

                                // Send error response
                                let error_response = serde_json::json!({
                                    "success": false,
                                    "error": format!("Failed to parse message as JSON: {}", e),
                                });

                                if let Err(e) = ws_sender.send(Message::Text(error_response.to_string())).await {
                                    error!("Failed to send error response: {}", e);
                                }

                                continue;
                            }
                        };

                        // Extract topic and data
                        let topic = match parsed.get("topic").and_then(|t| t.as_str()) {
                            Some(topic) => topic,
                            None => {
                                error!("Missing 'topic' field in message");

                                // Send error response
                                let error_response = serde_json::json!({
                                    "success": false,
                                    "error": "Missing 'topic' field in message",
                                });

                                if let Err(e) = ws_sender.send(Message::Text(error_response.to_string())).await {
                                    error!("Failed to send error response: {}", e);
                                }

                                continue;
                            }
                        };

                        let data = match parsed.get("data") {
                            Some(data) => {
                                // Convert data to bytes
                                match serde_json::to_vec(data) {
                                    Ok(bytes) => bytes,
                                    Err(e) => {
                                        error!("Failed to serialize data: {}", e);

                                        // Send error response
                                        let error_response = serde_json::json!({
                                            "success": false,
                                            "error": format!("Failed to serialize data: {}", e),
                                        });

                                        if let Err(e) = ws_sender.send(Message::Text(error_response.to_string())).await {
                                            error!("Failed to send error response: {}", e);
                                        }

                                        continue;
                                    }
                                }
                            }
                            None => {
                                error!("Missing 'data' field in message");

                                // Send error response
                                let error_response = serde_json::json!({
                                    "success": false,
                                    "error": "Missing 'data' field in message",
                                });

                                if let Err(e) = ws_sender.send(Message::Text(error_response.to_string())).await {
                                    error!("Failed to send error response: {}", e);
                                }

                                continue;
                            }
                        };

                        // Process message
                        if let Err(e) = Self::process_message(topic, data, &message_tx, &management_plane).await {
                            error!("Failed to process message: {}", e);

                            // Send error response
                            let error_response = serde_json::json!({
                                "success": false,
                                "error": format!("Failed to process message: {}", e),
                            });

                            if let Err(e) = ws_sender.send(Message::Text(error_response.to_string())).await {
                                error!("Failed to send error response: {}", e);
                            }

                            continue;
                        }

                        // Send success response
                        let success_response = serde_json::json!({
                            "success": true,
                            "message": "Message received",
                            "timestamp": SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        });

                        if let Err(e) = ws_sender.send(Message::Text(success_response.to_string())).await {
                            error!("Failed to send success response: {}", e);
                        }
                    }
                    Message::Binary(data) => {
                        debug!("Received binary message: {} bytes", data.len());

                        // Extract topic from headers or use default
                        let topic = "default"; // In a real implementation, this would be extracted from headers or connection context

                        // Process message
                        if let Err(e) = Self::process_message(topic, data, &message_tx, &management_plane).await {
                            error!("Failed to process binary message: {}", e);
                            continue;
                        }
                    }
                    Message::Ping(data) => {
                        debug!("Received ping");
                        // Respond with pong
                        if let Err(e) = ws_sender.send(Message::Pong(data)).await {
                            error!("Failed to send pong: {}", e);
                            break;
                        }
                    }
                    Message::Pong(_) => {
                        debug!("Received pong");
                    }
                    Message::Close(_) => {
                        debug!("Received close message");
                        break;
                    }
                    Message::Frame(_) => {
                        // We don't need to handle raw frames
                    }
                }
            }

            debug!("WebSocket connection closed");
        }

        #[cfg(not(feature = "websocket"))]
        {
            error!("WebSocket support is not enabled. Enable the 'websocket' feature to use it.");
        }

        Ok(())
    }

    /// Process a message
    async fn process_message(
        topic: &str,
        data: Vec<u8>,
        message_tx: &Sender<PushMessage>,
        management_plane: &Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        // Check if topic exists
        if let Err(e) = management_plane.get_topic(topic) {
            // If topic doesn't exist, create it with default settings
            if let crate::push::topic::TopicError::TopicNotFound(_) = e {
                let request = crate::push::topic::CreateTopicRequest {
                    name: topic.to_string(),
                    retention_period: 7 * 24 * 60 * 60, // 7 days
                    compression: false,
                };

                if let Err(e) = management_plane.create_topic(request) {
                    return Err(anyhow!("Failed to create topic: {}", e));
                }
            } else {
                return Err(anyhow!("Failed to get topic: {}", e));
            }
        }

        // Validate message
        if let Err(e) = management_plane.message_validator().validate(topic, &data) {
            return Err(anyhow!("Message validation failed: {}", e));
        }

        // Record message
        if let Err(e) = management_plane.record_message(topic, data.len()) {
            return Err(anyhow!("Failed to record message: {}", e));
        }

        // Create message
        let message = PushMessage {
            id: 0, // Will be assigned by the message store
            topic: topic.to_string(),
            data,
            timestamp: SystemTime::now(),
        };

        // Send message
        message_tx.send(message).await.map_err(|e| anyhow!("Failed to send message: {}", e))
    }

    /// Stop the WebSocket server
    pub async fn stop(&mut self) -> Result<(), anyhow::Error> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            info!("WebSocket server stopped");
        }
        Ok(())
    }
}
