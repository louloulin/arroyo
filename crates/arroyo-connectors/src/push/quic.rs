use std::sync::Arc;
use std::collections::HashMap;
use std::time::SystemTime;
use std::net::SocketAddr;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info};

use crate::push::source::PushMessage;
use crate::push::management::PushManagementPlane;
use crate::push::protocol::ProtocolAdapter;
use crate::push::dataplane::ProtocolType;

/// QUIC protocol adapter
pub struct QuicAdapter {
    /// Message sender
    message_tx: Sender<PushMessage>,
    
    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,
    
    /// Configuration
    config: HashMap<String, String>,
    
    /// QUIC server
    server: Option<QuicServer>,
}

impl QuicAdapter {
    /// Create a new QUIC adapter
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
        let port = self.config.get("port").cloned().unwrap_or_else(|| "8083".to_string());
        
        let addr = format!("{}:{}", host, port).parse()
            .map_err(|e| anyhow!("Invalid server address: {}", e))?;
        
        Ok(addr)
    }
}

#[async_trait::async_trait]
impl ProtocolAdapter for QuicAdapter {
    fn protocol_type(&self) -> ProtocolType {
        ProtocolType::Quic
    }
    
    async fn start(&mut self) -> Result<()> {
        // Get server address
        let addr = self.server_address()?;
        
        // Create QUIC server configuration
        let config = QuicServerConfig {
            addr,
            max_connections: 100,
            cert_path: self.config.get("cert_path").cloned(),
            key_path: self.config.get("key_path").cloned(),
        };
        
        // Create QUIC server
        let mut server = QuicServer::new(config);
        
        // Start QUIC server
        server.start(
            self.message_tx.clone(),
            self.management_plane.clone(),
        ).await?;
        
        // Store server
        self.server = Some(server);
        
        info!("QUIC server started on {}", addr);
        
        Ok(())
    }
    
    async fn stop(&mut self) -> Result<()> {
        // Stop QUIC server
        if let Some(server) = self.server.as_mut() {
            server.stop().await?;
            info!("QUIC server stopped");
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

/// QUIC server configuration
#[derive(Debug, Clone)]
pub struct QuicServerConfig {
    pub addr: SocketAddr,
    pub max_connections: usize,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl Default for QuicServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:8083".parse().unwrap(),
            max_connections: 100,
            cert_path: None,
            key_path: None,
        }
    }
}

/// QUIC server for push connector
pub struct QuicServer {
    pub config: QuicServerConfig,
    pub server_handle: Option<tokio::task::JoinHandle<Result<(), anyhow::Error>>>,
}

impl QuicServer {
    /// Create a new QUIC server
    pub fn new(config: QuicServerConfig) -> Self {
        Self {
            config,
            server_handle: None,
        }
    }
    
    /// Start the QUIC server
    pub async fn start(
        &mut self,
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        #[cfg(feature = "quic")]
        {
            use std::fs::File;
            use std::io::BufReader;
            use rustls::{Certificate, PrivateKey};
            use quinn::{Endpoint, ServerConfig, TransportConfig};
            
            // Configure QUIC server
            info!("Starting QUIC server on {}", self.config.addr);
            
            // Create TLS configuration
            let (cert, key) = if let (Some(cert_path), Some(key_path)) = (&self.config.cert_path, &self.config.key_path) {
                // Load certificate and key from files
                let cert_file = File::open(cert_path)
                    .map_err(|e| anyhow!("Failed to open certificate file: {}", e))?;
                let key_file = File::open(key_path)
                    .map_err(|e| anyhow!("Failed to open key file: {}", e))?;
                
                let mut cert_reader = BufReader::new(cert_file);
                let mut key_reader = BufReader::new(key_file);
                
                let certs = rustls_pemfile::certs(&mut cert_reader)
                    .map_err(|e| anyhow!("Failed to parse certificate: {}", e))?
                    .into_iter()
                    .map(Certificate)
                    .collect();
                
                let key = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
                    .map_err(|e| anyhow!("Failed to parse key: {}", e))?
                    .into_iter()
                    .map(PrivateKey)
                    .next()
                    .ok_or_else(|| anyhow!("No private key found"))?;
                
                (certs, key)
            } else {
                // Generate self-signed certificate
                let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
                    .map_err(|e| anyhow!("Failed to generate certificate: {}", e))?;
                
                let cert_der = cert.serialize_der()
                    .map_err(|e| anyhow!("Failed to serialize certificate: {}", e))?;
                let key_der = cert.serialize_private_key_der();
                
                let cert = Certificate(cert_der);
                let key = PrivateKey(key_der);
                
                (vec![cert], key)
            };
            
            // Create server configuration
            let mut server_config = ServerConfig::with_single_cert(cert, key)
                .map_err(|e| anyhow!("Failed to create server config: {}", e))?;
            
            // Configure transport
            let mut transport_config = TransportConfig::default();
            transport_config.max_concurrent_uni_streams(10_u8.into());
            server_config.transport = Arc::new(transport_config);
            
            // Create endpoint
            let endpoint = Endpoint::server(server_config, self.config.addr)
                .map_err(|e| anyhow!("Failed to create endpoint: {}", e))?;
            
            // Clone references for the server task
            let message_tx_clone = message_tx.clone();
            let management_plane_clone = management_plane.clone();
            
            // Start server in a separate task
            let server_handle = tokio::spawn(async move {
                Self::run_server(endpoint, message_tx_clone, management_plane_clone).await
            });
            
            // Store server handle
            self.server_handle = Some(server_handle);
        }
        
        #[cfg(not(feature = "quic"))]
        {
            error!("QUIC support is not enabled. Enable the 'quic' feature to use it.");
        }
        
        Ok(())
    }
    
    /// Run the QUIC server
    #[cfg(feature = "quic")]
    async fn run_server(
        endpoint: quinn::Endpoint,
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        info!("QUIC server running");
        
        // Accept connections
        while let Some(conn) = endpoint.accept().await {
            let connection = match conn.await {
                Ok(connection) => connection,
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    continue;
                }
            };
            
            debug!("New QUIC connection from {}", connection.remote_address());
            
            // Clone references for the connection task
            let message_tx_clone = message_tx.clone();
            let management_plane_clone = management_plane.clone();
            
            // Handle connection in a separate task
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(connection, message_tx_clone, management_plane_clone).await {
                    error!("QUIC connection error: {}", e);
                }
            });
        }
        
        Ok(())
    }
    
    /// Handle a QUIC connection
    #[cfg(feature = "quic")]
    async fn handle_connection(
        connection: quinn::Connection,
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        // Accept bi-directional streams
        while let Ok(Some((send, recv))) = connection.accept_bi().await {
            // Handle stream in a separate task
            let message_tx_clone = message_tx.clone();
            let management_plane_clone = management_plane.clone();
            
            tokio::spawn(async move {
                if let Err(e) = Self::handle_stream(send, recv, message_tx_clone, management_plane_clone).await {
                    error!("QUIC stream error: {}", e);
                }
            });
        }
        
        Ok(())
    }
    
    /// Handle a QUIC stream
    #[cfg(feature = "quic")]
    async fn handle_stream(
        mut send: quinn::SendStream,
        mut recv: quinn::RecvStream,
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        // Read topic name
        let topic_bytes = recv.read_to_end(1024).await
            .map_err(|e| anyhow!("Failed to read topic: {}", e))?;
        
        let topic = String::from_utf8(topic_bytes.clone())
            .map_err(|e| anyhow!("Failed to parse topic: {}", e))?;
        
        debug!("Received QUIC stream for topic: {}", topic);
        
        // Check if topic exists
        if let Err(e) = management_plane.get_topic(&topic) {
            // If topic doesn't exist, create it with default settings
            if let crate::push::topic::TopicError::TopicNotFound(_) = e {
                let request = crate::push::topic::CreateTopicRequest {
                    name: topic.clone(),
                    retention_period: 7 * 24 * 60 * 60, // 7 days
                    compression: false,
                };

                if let Err(e) = management_plane.create_topic(request) {
                    error!("Failed to create topic: {}", e);
                    let error_msg = format!("Failed to create topic: {}", e);
                    send.write_all(error_msg.as_bytes()).await
                        .map_err(|e| anyhow!("Failed to send error: {}", e))?;
                    return Err(anyhow!("Failed to create topic: {}", e));
                }
            } else {
                error!("Failed to get topic: {}", e);
                let error_msg = format!("Failed to get topic: {}", e);
                send.write_all(error_msg.as_bytes()).await
                    .map_err(|e| anyhow!("Failed to send error: {}", e))?;
                return Err(anyhow!("Failed to get topic: {}", e));
            }
        }
        
        // Read message data
        let data = recv.read_to_end(10 * 1024 * 1024).await // 10MB limit
            .map_err(|e| anyhow!("Failed to read data: {}", e))?;
        
        // Validate message
        if let Err(e) = management_plane.message_validator().validate(&topic, &data) {
            error!("Message validation failed: {}", e);
            let error_msg = format!("Message validation failed: {}", e);
            send.write_all(error_msg.as_bytes()).await
                .map_err(|e| anyhow!("Failed to send error: {}", e))?;
            return Err(anyhow!("Message validation failed: {}", e));
        }
        
        // Record message
        if let Err(e) = management_plane.record_message(&topic, data.len()) {
            error!("Failed to record message: {}", e);
            let error_msg = format!("Failed to record message: {}", e);
            send.write_all(error_msg.as_bytes()).await
                .map_err(|e| anyhow!("Failed to send error: {}", e))?;
            return Err(anyhow!("Failed to record message: {}", e));
        }
        
        // Create message
        let message = PushMessage {
            id: 0, // Will be assigned by the message store
            topic: topic.clone(),
            data,
            timestamp: SystemTime::now(),
        };
        
        // Send message
        if let Err(e) = message_tx.send(message).await {
            error!("Failed to send message: {}", e);
            let error_msg = format!("Failed to send message: {}", e);
            send.write_all(error_msg.as_bytes()).await
                .map_err(|e| anyhow!("Failed to send error: {}", e))?;
            return Err(anyhow!("Failed to send message: {}", e));
        }
        
        // Send success response
        let success_msg = "Message received";
        send.write_all(success_msg.as_bytes()).await
            .map_err(|e| anyhow!("Failed to send success: {}", e))?;
        
        Ok(())
    }
    
    /// Stop the QUIC server
    pub async fn stop(&mut self) -> Result<(), anyhow::Error> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            info!("QUIC server stopped");
        }
        Ok(())
    }
}
