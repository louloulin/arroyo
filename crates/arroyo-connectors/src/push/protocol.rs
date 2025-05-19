use std::sync::Arc;
use std::collections::HashMap;
use std::time::SystemTime;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info};

use crate::push::source::PushMessage;
use crate::push::management::PushManagementPlane;
use crate::push::dataplane::ProtocolType;

/// Protocol adapter factory
pub struct ProtocolAdapterFactory;

impl ProtocolAdapterFactory {
    /// Create a new protocol adapter
    pub fn create(
        protocol: ProtocolType,
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
        config: HashMap<String, String>,
    ) -> Result<Box<dyn ProtocolAdapter>> {
        match protocol {
            ProtocolType::Http => Ok(Box::new(HttpAdapter::new(
                message_tx,
                management_plane,
                config,
            ))),
            ProtocolType::Http2 => {
                use crate::push::http2::Http2Adapter;
                Ok(Box::new(Http2Adapter::new(
                    message_tx,
                    management_plane,
                    config,
                )))
            },
            ProtocolType::WebSocket => {
                use crate::push::websocket::WebSocketAdapter;
                Ok(Box::new(WebSocketAdapter::new(
                    message_tx,
                    management_plane,
                    config,
                )))
            },
            ProtocolType::Grpc => {
                use crate::push::grpc::GrpcAdapter;
                Ok(Box::new(GrpcAdapter::new(
                    message_tx,
                    management_plane,
                    config,
                )))
            },
            ProtocolType::Quic => Err(anyhow!("QUIC protocol not implemented yet")),
        }
    }
}

/// Protocol adapter trait
#[async_trait::async_trait]
pub trait ProtocolAdapter: Send + Sync {
    /// Get the protocol type
    fn protocol_type(&self) -> ProtocolType;

    /// Start the adapter
    async fn start(&mut self) -> Result<()>;

    /// Stop the adapter
    async fn stop(&mut self) -> Result<()>;

    /// Process a message
    async fn process_message(&self, topic: &str, data: Vec<u8>) -> Result<()>;

    /// Get adapter configuration
    fn config(&self) -> &HashMap<String, String>;

    /// Update adapter configuration
    fn update_config(&mut self, config: HashMap<String, String>) -> Result<()>;
}

/// HTTP protocol adapter
pub struct HttpAdapter {
    /// Message sender
    message_tx: Sender<PushMessage>,

    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,

    /// Configuration
    config: HashMap<String, String>,
}

impl HttpAdapter {
    /// Create a new HTTP adapter
    pub fn new(
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
        config: HashMap<String, String>,
    ) -> Self {
        Self {
            message_tx,
            management_plane,
            config,
        }
    }
}

#[async_trait::async_trait]
impl ProtocolAdapter for HttpAdapter {
    fn protocol_type(&self) -> ProtocolType {
        ProtocolType::Http
    }

    async fn start(&mut self) -> Result<()> {
        // HTTP adapter doesn't need to start a server as it's integrated with the API service
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        // HTTP adapter doesn't need to stop a server
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

/// HTTP/2 protocol adapter (to be implemented)
pub struct Http2Adapter {
    /// Message sender
    message_tx: Sender<PushMessage>,

    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,

    /// Configuration
    config: HashMap<String, String>,
}

/// WebSocket protocol adapter (to be implemented)
pub struct WebSocketAdapter {
    /// Message sender
    message_tx: Sender<PushMessage>,

    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,

    /// Configuration
    config: HashMap<String, String>,
}

/// gRPC protocol adapter (to be implemented)
pub struct GrpcAdapter {
    /// Message sender
    message_tx: Sender<PushMessage>,

    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,

    /// Configuration
    config: HashMap<String, String>,
}

/// QUIC protocol adapter (to be implemented)
pub struct QuicAdapter {
    /// Message sender
    message_tx: Sender<PushMessage>,

    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,

    /// Configuration
    config: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_http_adapter_process_message() {
        // Create management plane
        let management_plane = Arc::new(PushManagementPlane::new());

        // Create topic
        let _ = management_plane.create_topic(crate::push::topic::CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: false,
        });

        // Create channel
        let (tx, mut rx) = mpsc::channel(100);

        // Create adapter
        let adapter = HttpAdapter::new(
            tx,
            management_plane,
            HashMap::new(),
        );

        // Process message
        let result = adapter.process_message("test-topic", b"test data".to_vec()).await;
        assert!(result.is_ok());

        // Receive message
        let message = rx.recv().await.unwrap();
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.data, b"test data");
    }
}
