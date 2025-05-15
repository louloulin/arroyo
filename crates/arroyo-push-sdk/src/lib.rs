use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use reqwest::{Client, header};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Transport protocol for push client
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportProtocol {
    /// HTTP protocol
    Http,
    /// QUIC protocol
    Quic,
    /// gRPC protocol
    Grpc,
    /// WebSocket protocol
    WebSocket,
}

/// Compression type for push client
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression
    None,
    /// GZIP compression
    Gzip,
    /// LZ4 compression
    Lz4,
    /// ZSTD compression
    Zstd,
}

/// Retry policy for push client
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retries
    pub max_retries: usize,
    /// Initial backoff duration
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

/// Push client configuration
#[derive(Debug, Clone)]
pub struct PushClientConfig {
    /// Transport protocol
    pub protocol: TransportProtocol,
    /// Base URL
    pub base_url: String,
    /// API key for authentication
    pub api_key: Option<String>,
    /// Connection pool size
    pub connection_pool_size: Option<usize>,
    /// Request timeout
    pub timeout: Option<Duration>,
    /// Retry policy
    pub retry_policy: Option<RetryPolicy>,
    /// Compression type
    pub compression: Option<CompressionType>,
}

impl Default for PushClientConfig {
    fn default() -> Self {
        Self {
            protocol: TransportProtocol::Http,
            base_url: "http://localhost:8000".to_string(),
            api_key: None,
            connection_pool_size: None,
            timeout: Some(Duration::from_secs(30)),
            retry_policy: Some(RetryPolicy::default()),
            compression: None,
        }
    }
}

/// Push response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushResponse {
    /// Whether the push was successful
    pub success: bool,
    /// Response message
    pub message: String,
    /// Timestamp of the response
    pub timestamp: u64,
}

/// Batch push response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchPushResponse {
    /// Whether the push was successful
    pub success: bool,
    /// Number of messages processed
    pub processed_count: usize,
    /// Response message
    pub message: String,
    /// Timestamp of the response
    pub timestamp: u64,
}

/// Push client error
#[derive(Debug, Error)]
pub enum PushClientError {
    /// HTTP error
    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),
    
    /// Server error
    #[error("Server error: {status} - {message}")]
    ServerError {
        /// HTTP status code
        status: u16,
        /// Error message
        message: String,
    },
    
    /// Protocol error
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    
    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

/// Push client
pub struct PushClient {
    /// Client configuration
    config: PushClientConfig,
    /// HTTP client
    http_client: Option<Client>,
    // TODO: Add other protocol clients (QUIC, gRPC, WebSocket)
}

impl PushClient {
    /// Create a new push client
    pub fn new(config: PushClientConfig) -> Result<Self, PushClientError> {
        let mut client = Self {
            config,
            http_client: None,
        };
        
        // Initialize client based on protocol
        match client.config.protocol {
            TransportProtocol::Http => {
                client.init_http_client()?;
            }
            TransportProtocol::Quic => {
                return Err(PushClientError::ProtocolError(
                    "QUIC protocol not implemented yet".to_string(),
                ));
            }
            TransportProtocol::Grpc => {
                return Err(PushClientError::ProtocolError(
                    "gRPC protocol not implemented yet".to_string(),
                ));
            }
            TransportProtocol::WebSocket => {
                return Err(PushClientError::ProtocolError(
                    "WebSocket protocol not implemented yet".to_string(),
                ));
            }
        }
        
        Ok(client)
    }
    
    /// Initialize HTTP client
    fn init_http_client(&mut self) -> Result<(), PushClientError> {
        let mut headers = header::HeaderMap::new();
        
        // Add API key if provided
        if let Some(api_key) = &self.config.api_key {
            headers.insert(
                header::AUTHORIZATION,
                header::HeaderValue::from_str(&format!("Bearer {}", api_key))
                    .map_err(|e| PushClientError::ConfigurationError(format!("Invalid API key: {}", e)))?,
            );
        }
        
        // Add content type
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/octet-stream"),
        );
        
        // Add compression header if needed
        if let Some(compression) = self.config.compression {
            match compression {
                CompressionType::Gzip => {
                    headers.insert(
                        header::CONTENT_ENCODING,
                        header::HeaderValue::from_static("gzip"),
                    );
                }
                CompressionType::Lz4 => {
                    headers.insert(
                        header::CONTENT_ENCODING,
                        header::HeaderValue::from_static("lz4"),
                    );
                }
                CompressionType::Zstd => {
                    headers.insert(
                        header::CONTENT_ENCODING,
                        header::HeaderValue::from_static("zstd"),
                    );
                }
                CompressionType::None => {}
            }
        }
        
        // Build client
        let mut client_builder = Client::builder()
            .default_headers(headers);
        
        // Add timeout if provided
        if let Some(timeout) = self.config.timeout {
            client_builder = client_builder.timeout(timeout);
        }
        
        // Add connection pool size if provided
        if let Some(pool_size) = self.config.connection_pool_size {
            client_builder = client_builder.pool_max_idle_per_host(pool_size);
        }
        
        // Build client
        let client = client_builder.build()
            .map_err(|e| PushClientError::ConfigurationError(format!("Failed to build HTTP client: {}", e)))?;
        
        self.http_client = Some(client);
        Ok(())
    }
    
    /// Push data to a topic
    pub async fn push(&self, topic: &str, data: Vec<u8>, metadata: Option<HashMap<String, String>>) -> Result<PushResponse, PushClientError> {
        match self.config.protocol {
            TransportProtocol::Http => {
                self.push_http(topic, data, metadata).await
            }
            TransportProtocol::Quic => {
                Err(PushClientError::ProtocolError(
                    "QUIC protocol not implemented yet".to_string(),
                ))
            }
            TransportProtocol::Grpc => {
                Err(PushClientError::ProtocolError(
                    "gRPC protocol not implemented yet".to_string(),
                ))
            }
            TransportProtocol::WebSocket => {
                Err(PushClientError::ProtocolError(
                    "WebSocket protocol not implemented yet".to_string(),
                ))
            }
        }
    }
    
    /// Push data using HTTP protocol
    async fn push_http(&self, topic: &str, data: Vec<u8>, _metadata: Option<HashMap<String, String>>) -> Result<PushResponse, PushClientError> {
        let client = self.http_client.as_ref()
            .ok_or_else(|| PushClientError::ConfigurationError("HTTP client not initialized".to_string()))?;
        
        let url = format!("{}/api/v1/push/{}", self.config.base_url, topic);
        
        // TODO: Apply compression if needed
        
        let response = client.post(&url)
            .body(data)
            .send()
            .await?;
        
        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(PushClientError::ServerError {
                status: status.as_u16(),
                message: error_text,
            });
        }
        
        let push_response = response.json::<PushResponse>().await?;
        Ok(push_response)
    }
    
    /// Push batch of data to a topic
    pub async fn push_batch(&self, topic: &str, batch: Vec<Vec<u8>>, metadata: Option<HashMap<String, String>>) -> Result<BatchPushResponse, PushClientError> {
        // TODO: Implement batch push
        Err(PushClientError::ProtocolError(
            "Batch push not implemented yet".to_string(),
        ))
    }
}
