use std::sync::Arc;
use std::collections::HashMap;
use std::time::SystemTime;
use std::net::SocketAddr;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc::Sender;
use hyper::{Body, Request, Response, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use hyper::server::Server;
use tracing::{debug, error, info};

use crate::push::source::PushMessage;
use crate::push::management::PushManagementPlane;
use crate::push::protocol::ProtocolAdapter;
use crate::push::dataplane::ProtocolType;

/// HTTP/2 protocol adapter
pub struct Http2Adapter {
    /// Message sender
    message_tx: Sender<PushMessage>,
    
    /// Management plane reference
    management_plane: Arc<PushManagementPlane>,
    
    /// Configuration
    config: HashMap<String, String>,
    
    /// Server handle
    server_handle: Option<tokio::task::JoinHandle<()>>,
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
            server_handle: None,
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
    
    /// Handle HTTP/2 request
    async fn handle_request(
        management_plane: Arc<PushManagementPlane>,
        message_tx: Sender<PushMessage>,
        req: Request<Body>,
    ) -> Result<Response<Body>> {
        // Get topic from path
        let path = req.uri().path();
        let topic = path.trim_start_matches('/');
        
        if topic.is_empty() {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Topic name is required"))
                .unwrap());
        }
        
        // Check if topic exists
        if let Err(e) = management_plane.get_topic(topic) {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(format!("Topic not found: {}", e)))
                .unwrap());
        }
        
        // Get request body
        let body = hyper::body::to_bytes(req.into_body()).await
            .map_err(|e| anyhow!("Failed to read request body: {}", e))?;
        
        // Validate message
        if let Err(e) = management_plane.message_validator().validate(topic, &body) {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("Message validation failed: {}", e)))
                .unwrap());
        }
        
        // Record message
        if let Err(e) = management_plane.record_message(topic, body.len()) {
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to record message: {}", e)))
                .unwrap());
        }
        
        // Create message
        let message = PushMessage {
            id: 0, // Will be assigned by the message store
            topic: topic.to_string(),
            data: body.to_vec(),
            timestamp: SystemTime::now(),
        };
        
        // Send message
        if let Err(e) = message_tx.send(message).await {
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to send message: {}", e)))
                .unwrap());
        }
        
        // Return success response
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from("Message received"))
            .unwrap())
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
        
        // Clone references for the service
        let management_plane = self.management_plane.clone();
        let message_tx = self.message_tx.clone();
        
        // Create service
        let make_svc = make_service_fn(move |_conn| {
            let management_plane = management_plane.clone();
            let message_tx = message_tx.clone();
            
            async move {
                Ok::<_, hyper::Error>(service_fn(move |req| {
                    let management_plane = management_plane.clone();
                    let message_tx = message_tx.clone();
                    
                    async move {
                        match Self::handle_request(management_plane, message_tx, req).await {
                            Ok(response) => Ok::<_, hyper::Error>(response),
                            Err(e) => {
                                error!("Failed to handle request: {}", e);
                                Ok(Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::from(format!("Internal server error: {}", e)))
                                    .unwrap())
                            }
                        }
                    }
                }))
            }
        });
        
        // Create server with HTTP/2 support
        let server = Server::bind(&addr)
            .http2_only(true) // Enable HTTP/2 only
            .serve(make_svc);
        
        info!("HTTP/2 server listening on {}", addr);
        
        // Start server in a separate task
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.await {
                error!("HTTP/2 server error: {}", e);
            }
        });
        
        // Store server handle
        self.server_handle = Some(server_handle);
        
        Ok(())
    }
    
    async fn stop(&mut self) -> Result<()> {
        // Stop server
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            info!("HTTP/2 server stopped");
        }
        
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
