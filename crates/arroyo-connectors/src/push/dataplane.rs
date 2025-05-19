use std::sync::Arc;
use std::collections::HashMap;
use std::time::SystemTime;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc::{self, Sender, Receiver};
use tracing::{debug, error, info};

use crate::push::source::PushMessage;
use crate::push::buffer::MemoryBuffer;
use crate::push::backpressure::BackpressureController;
use crate::push::batch::BatchProcessor;
use crate::push::PushConnectorConfig;
use crate::push::management::PushManagementPlane;

/// Protocol type supported by the data plane
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolType {
    /// HTTP protocol
    Http,
    /// HTTP/2 protocol
    Http2,
    /// WebSocket protocol
    WebSocket,
    /// gRPC protocol
    Grpc,
    /// QUIC protocol
    Quic,
}

impl ProtocolType {
    /// Convert string to protocol type
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "http" => Ok(ProtocolType::Http),
            "http2" => Ok(ProtocolType::Http2),
            "websocket" | "ws" => Ok(ProtocolType::WebSocket),
            "grpc" => Ok(ProtocolType::Grpc),
            "quic" => Ok(ProtocolType::Quic),
            _ => Err(anyhow!("Unsupported protocol: {}", s)),
        }
    }

    /// Convert protocol type to string
    pub fn as_str(&self) -> &'static str {
        match self {
            ProtocolType::Http => "http",
            ProtocolType::Http2 => "http2",
            ProtocolType::WebSocket => "websocket",
            ProtocolType::Grpc => "grpc",
            ProtocolType::Quic => "quic",
        }
    }
}

/// Data plane for Push connector
/// Responsible for receiving and processing data from external systems
pub struct PushDataPlane {
    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,

    /// Memory buffer for storing messages
    memory_buffer: Arc<MemoryBuffer>,

    /// Backpressure controller
    backpressure_controller: Arc<BackpressureController>,

    /// Batch processor
    batch_processor: BatchProcessor,

    /// Message sender
    message_tx: Option<Sender<PushMessage>>,

    /// Message receiver
    message_rx: Option<Receiver<PushMessage>>,

    /// Configuration
    config: PushConnectorConfig,

    /// Enabled protocols
    enabled_protocols: Vec<ProtocolType>,

    /// Protocol adapters
    protocol_adapters: HashMap<ProtocolType, Box<dyn ProtocolAdapter>>,

    /// Running flag
    running: bool,
}

impl PushDataPlane {
    /// Create a new data plane
    pub fn new(management_plane: Arc<PushManagementPlane>) -> Self {
        Self::with_config(management_plane, PushConnectorConfig::default())
    }

    /// Create a new data plane with the specified configuration
    pub fn with_config(management_plane: Arc<PushManagementPlane>, config: PushConnectorConfig) -> Self {
        // Create channel for message passing
        let (tx, rx) = mpsc::channel(config.buffer_size);

        // Create memory buffer
        let memory_buffer = Arc::new(MemoryBuffer::new(config.buffer_size));

        // Create backpressure controller
        let backpressure_controller = Arc::new(BackpressureController::new(
            config.buffer_size,
        ));

        // Create batch processor
        let batch_processor = BatchProcessor::new(
            config.max_batch_size,
            std::time::Duration::from_millis(100),
        );

        Self {
            management_plane,
            memory_buffer,
            backpressure_controller,
            batch_processor,
            message_tx: Some(tx),
            message_rx: Some(rx),
            config,
            enabled_protocols: vec![ProtocolType::Http], // Default to HTTP only
            protocol_adapters: HashMap::new(),
            running: false,
        }
    }

    /// Enable a protocol
    pub fn enable_protocol(&mut self, protocol: ProtocolType) -> Result<()> {
        if !self.enabled_protocols.contains(&protocol) {
            self.enabled_protocols.push(protocol);
        }
        Ok(())
    }

    /// Disable a protocol
    pub fn disable_protocol(&mut self, protocol: ProtocolType) -> Result<()> {
        self.enabled_protocols.retain(|&p| p != protocol);
        Ok(())
    }

    /// Get enabled protocols
    pub fn enabled_protocols(&self) -> &[ProtocolType] {
        &self.enabled_protocols
    }

    /// Start the data plane
    pub async fn start(&mut self) -> Result<()> {
        if self.running {
            return Ok(());
        }

        // Initialize protocol adapters
        self.init_protocol_adapters().await?;

        // Start protocol adapters
        for protocol in &self.enabled_protocols {
            if let Some(adapter) = self.protocol_adapters.get_mut(protocol) {
                adapter.start().await?;
            }
        }

        self.running = true;
        Ok(())
    }

    /// Stop the data plane
    pub async fn stop(&mut self) -> Result<()> {
        if !self.running {
            return Ok(());
        }

        // Stop protocol adapters
        for protocol in &self.enabled_protocols {
            if let Some(adapter) = self.protocol_adapters.get_mut(protocol) {
                adapter.stop().await?;
            }
        }

        self.running = false;
        Ok(())
    }

    /// Initialize protocol adapters
    async fn init_protocol_adapters(&mut self) -> Result<()> {
        // Clear existing adapters
        self.protocol_adapters.clear();

        // Create adapters for enabled protocols
        for &protocol in &self.enabled_protocols {
            let adapter: Box<dyn ProtocolAdapter> = match protocol {
                ProtocolType::Http => Box::new(HttpAdapter::new(
                    self.message_tx.clone().unwrap(),
                    self.management_plane.clone(),
                )),
                // Other protocols will be implemented in later phases
                _ => {
                    error!("Protocol not implemented yet: {:?}", protocol);
                    continue;
                }
            };

            self.protocol_adapters.insert(protocol, adapter);
        }

        Ok(())
    }

    /// Process a message
    pub async fn process_message(&mut self, message: PushMessage) -> Result<()> {
        // Add message to batch processor
        self.batch_processor.add(message);

        // Check if batch is ready
        if let Some(batch) = self.batch_processor.check() {
            // Process batch
            for (topic, messages) in batch {
                debug!("Processing batch of {} messages for topic {}", messages.len(), topic);

                // TODO: Process messages
            }
        }

        Ok(())
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
}

/// HTTP protocol adapter
pub struct HttpAdapter {
    /// Message sender
    message_tx: Sender<PushMessage>,

    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,
}

impl HttpAdapter {
    /// Create a new HTTP adapter
    pub fn new(message_tx: Sender<PushMessage>, management_plane: Arc<PushManagementPlane>) -> Self {
        Self {
            message_tx,
            management_plane,
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
}
