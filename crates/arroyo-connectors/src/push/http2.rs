use std::sync::Arc;
use std::collections::HashMap;
use std::time::SystemTime;
use std::net::SocketAddr;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc::Sender;
use tracing::{error, info};

use crate::push::source::PushMessage;
use crate::push::management::PushManagementPlane;
use crate::push::protocol::ProtocolAdapter;
use crate::push::dataplane::ProtocolType;
use crate::push::http2_server::{Http2Server, Http2ServerConfig};

/// HTTP/2 protocol adapter
pub struct Http2Adapter {
    /// Message sender
    message_tx: Sender<PushMessage>,

    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,

    /// Configuration
    config: HashMap<String, String>,

    /// HTTP/2 server
    server: Option<Http2Server>,
}

impl Http2Adapter {
    /// Create a new HTTP/2 adapter
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
        let port = self.config.get("port").cloned().unwrap_or_else(|| "8080".to_string());

        let addr = format!("{}:{}", host, port).parse()
            .map_err(|e| anyhow!("Invalid server address: {}", e))?;

        Ok(addr)
    }
}

#[async_trait::async_trait]
impl ProtocolAdapter for Http2Adapter {
    fn protocol_type(&self) -> ProtocolType {
        ProtocolType::Http2
    }

    async fn start(&mut self) -> Result<()> {
        // Get server address
        let addr = self.server_address()?;

        // Create HTTP/2 server configuration
        let config = Http2ServerConfig {
            addr,
            timeout: 30,
            max_connections: 100,
        };

        // Create HTTP/2 server
        let mut server = Http2Server::new(config);

        // Start HTTP/2 server
        server.start(
            self.message_tx.clone(),
            self.management_plane.clone(),
        ).await?;

        // Store server
        self.server = Some(server);

        info!("HTTP/2 server started on {}", addr);

        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        // Stop HTTP/2 server
        if let Some(server) = self.server.as_mut() {
            server.stop().await?;
            info!("HTTP/2 server stopped");
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
