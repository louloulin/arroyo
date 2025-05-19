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

// Include the generated gRPC code when the grpc feature is enabled
#[cfg(feature = "grpc")]
pub mod proto {
    tonic::include_proto!("arroyo.push");
}

/// gRPC protocol adapter
pub struct GrpcAdapter {
    /// Message sender
    message_tx: Sender<PushMessage>,
    
    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,
    
    /// Configuration
    config: HashMap<String, String>,
    
    /// gRPC server
    server: Option<GrpcServer>,
}

impl GrpcAdapter {
    /// Create a new gRPC adapter
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
        let port = self.config.get("port").cloned().unwrap_or_else(|| "8082".to_string());
        
        let addr = format!("{}:{}", host, port).parse()
            .map_err(|e| anyhow!("Invalid server address: {}", e))?;
        
        Ok(addr)
    }
}

#[async_trait::async_trait]
impl ProtocolAdapter for GrpcAdapter {
    fn protocol_type(&self) -> ProtocolType {
        ProtocolType::Grpc
    }
    
    async fn start(&mut self) -> Result<()> {
        // Get server address
        let addr = self.server_address()?;
        
        // Create gRPC server configuration
        let config = GrpcServerConfig {
            addr,
            max_connections: 100,
        };
        
        // Create gRPC server
        let mut server = GrpcServer::new(config);
        
        // Start gRPC server
        server.start(
            self.message_tx.clone(),
            self.management_plane.clone(),
        ).await?;
        
        // Store server
        self.server = Some(server);
        
        info!("gRPC server started on {}", addr);
        
        Ok(())
    }
    
    async fn stop(&mut self) -> Result<()> {
        // Stop gRPC server
        if let Some(server) = self.server.as_mut() {
            server.stop().await?;
            info!("gRPC server stopped");
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

/// gRPC server configuration
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    pub addr: SocketAddr,
    pub max_connections: usize,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:8082".parse().unwrap(),
            max_connections: 100,
        }
    }
}

/// gRPC server for push connector
pub struct GrpcServer {
    pub config: GrpcServerConfig,
    pub server_handle: Option<tokio::task::JoinHandle<Result<(), anyhow::Error>>>,
}

impl GrpcServer {
    /// Create a new gRPC server
    pub fn new(config: GrpcServerConfig) -> Self {
        Self {
            config,
            server_handle: None,
        }
    }
    
    /// Start the gRPC server
    pub async fn start(
        &mut self,
        message_tx: Sender<PushMessage>,
        management_plane: Arc<PushManagementPlane>,
    ) -> Result<(), anyhow::Error> {
        #[cfg(feature = "grpc")]
        {
            use tonic::transport::Server;
            use proto::push_service_server::{PushServiceServer};
            
            // Create gRPC service
            let service = GrpcService::new(message_tx, management_plane);
            
            // Create gRPC server
            info!("Starting gRPC server on {}", self.config.addr);
            
            // Start server in a separate task
            let addr = self.config.addr;
            let server_handle = tokio::spawn(async move {
                Server::builder()
                    .add_service(PushServiceServer::new(service))
                    .serve(addr)
                    .await
                    .map_err(|e| anyhow::anyhow!("gRPC server error: {}", e))?;
                
                Ok(())
            });
            
            // Store server handle
            self.server_handle = Some(server_handle);
        }
        
        #[cfg(not(feature = "grpc"))]
        {
            error!("gRPC support is not enabled. Enable the 'grpc' feature to use it.");
        }
        
        Ok(())
    }
    
    /// Stop the gRPC server
    pub async fn stop(&mut self) -> Result<(), anyhow::Error> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            info!("gRPC server stopped");
        }
        Ok(())
    }
}

#[cfg(feature = "grpc")]
pub struct GrpcService {
    message_tx: Sender<PushMessage>,
    management_plane: Arc<PushManagementPlane>,
}

#[cfg(feature = "grpc")]
impl GrpcService {
    pub fn new(message_tx: Sender<PushMessage>, management_plane: Arc<PushManagementPlane>) -> Self {
        Self {
            message_tx,
            management_plane,
        }
    }
}

#[cfg(feature = "grpc")]
#[tonic::async_trait]
impl proto::push_service_server::PushService for GrpcService {
    async fn push_message(
        &self,
        request: tonic::Request<proto::PushRequest>,
    ) -> Result<tonic::Response<proto::PushResponse>, tonic::Status> {
        let req = request.into_inner();
        let topic = req.topic;
        let data = req.data;
        
        debug!("Received gRPC push request for topic: {}, size: {}", topic, data.len());
        
        // Check if topic exists
        if let Err(e) = self.management_plane.get_topic(&topic) {
            // If topic doesn't exist, create it with default settings
            if let crate::push::topic::TopicError::TopicNotFound(_) = e {
                let request = crate::push::topic::CreateTopicRequest {
                    name: topic.clone(),
                    retention_period: 7 * 24 * 60 * 60, // 7 days
                    compression: false,
                };

                if let Err(e) = self.management_plane.create_topic(request) {
                    error!("Failed to create topic: {}", e);
                    return Err(tonic::Status::internal(format!("Failed to create topic: {}", e)));
                }
            } else {
                error!("Failed to get topic: {}", e);
                return Err(tonic::Status::internal(format!("Failed to get topic: {}", e)));
            }
        }
        
        // Validate message
        if let Err(e) = self.management_plane.message_validator().validate(&topic, &data) {
            error!("Message validation failed: {}", e);
            return Err(tonic::Status::invalid_argument(format!("Message validation failed: {}", e)));
        }
        
        // Record message
        if let Err(e) = self.management_plane.record_message(&topic, data.len()) {
            error!("Failed to record message: {}", e);
            return Err(tonic::Status::internal(format!("Failed to record message: {}", e)));
        }
        
        // Create message
        let message = PushMessage {
            id: 0, // Will be assigned by the message store
            topic: topic.clone(),
            data,
            timestamp: SystemTime::now(),
        };
        
        // Send message
        if let Err(e) = self.message_tx.send(message).await {
            error!("Failed to send message: {}", e);
            return Err(tonic::Status::internal(format!("Failed to send message: {}", e)));
        }
        
        // Create response
        let now = SystemTime::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs() as i64,
            nanos: 0,
        };
        
        let response = proto::PushResponse {
            success: true,
            message_id: "0".to_string(), // TODO: Generate real message ID
            error: "".to_string(),
            timestamp: Some(timestamp),
        };
        
        Ok(tonic::Response::new(response))
    }
    
    // Implement other methods with placeholder implementations
    // These will be implemented in future updates
    
    async fn push_messages(
        &self,
        _request: tonic::Request<proto::PushBatchRequest>,
    ) -> Result<tonic::Response<proto::PushBatchResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
    
    async fn get_topic(
        &self,
        _request: tonic::Request<proto::GetTopicRequest>,
    ) -> Result<tonic::Response<proto::TopicInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
    
    async fn list_topics(
        &self,
        _request: tonic::Request<proto::ListTopicsRequest>,
    ) -> Result<tonic::Response<proto::ListTopicsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
    
    async fn create_topic(
        &self,
        _request: tonic::Request<proto::CreateTopicRequest>,
    ) -> Result<tonic::Response<proto::TopicInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
    
    async fn delete_topic(
        &self,
        _request: tonic::Request<proto::DeleteTopicRequest>,
    ) -> Result<tonic::Response<prost_types::Empty>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
    
    type SubscribeStream = tokio_stream::wrappers::ReceiverStream<Result<proto::SubscribeResponse, tonic::Status>>;
    
    async fn subscribe(
        &self,
        _request: tonic::Request<proto::SubscribeRequest>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
    
    async fn health_check(
        &self,
        _request: tonic::Request<prost_types::Empty>,
    ) -> Result<tonic::Response<proto::HealthStatus>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented yet"))
    }
}
