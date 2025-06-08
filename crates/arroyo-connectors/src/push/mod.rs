use anyhow::Result;
use arroyo_operator::connector::{Connector, Connection};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Sender};
use std::sync::RwLock;
use once_cell::sync::Lazy;

// Global registry for message senders
static MESSAGE_SENDERS: Lazy<RwLock<HashMap<String, mpsc::Sender<source::PushMessage>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub mod api;
pub mod auth;
pub mod backpressure;
pub mod batch;
pub mod buffer;
pub mod converter;
pub mod dataplane;
pub mod discovery;
pub mod grpc;
pub mod http;
pub mod http2;
pub mod http2_server;
pub mod management;
pub mod messages;
pub mod metrics;
pub mod protocol;
pub mod quic;
pub mod retry;
pub mod source;
pub mod sql;
pub mod topic;
pub mod validator;
pub mod websocket;

#[cfg(test)]
mod topic_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod metrics_tests;

#[cfg(test)]
mod messages_tests;

#[cfg(test)]
mod http_tests;

#[cfg(test)]
mod buffer_tests;

#[cfg(test)]
mod backpressure_tests;

#[cfg(test)]
mod batch_tests;

#[cfg(test)]
mod converter_tests;

#[cfg(test)]
mod sql_tests;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./push.svg");

// Define PushConfig struct manually
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushConfig {
    pub buffer_size: Option<usize>,
    pub max_batch_size: Option<usize>,
    pub authentication: Option<Authentication>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Authentication {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "api_key")]
    ApiKey { api_key: String },
    #[serde(rename = "oauth")]
    OAuth { client_id: String, client_secret: String, token_url: String },
}

// Define PushTable struct manually
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushTable {
    pub topic: String,
    pub protocol: String,
    pub retention_period: Option<u64>,
    pub http_config: Option<HashMap<String, String>>,
    pub quic_config: Option<HashMap<String, String>>,
    pub grpc_config: Option<HashMap<String, String>>,
    pub websocket_config: Option<HashMap<String, String>>,
    pub compression: Option<String>,
    pub batch_size: Option<u64>,
}

/// Push connector configuration
#[derive(Debug, Clone)]
pub struct PushConnectorConfig {
    pub buffer_size: usize,
    pub max_batch_size: usize,
    pub max_message_size: usize,
    pub default_retention_period: u64,
    pub default_compression: bool,
}

impl Default for PushConnectorConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10 * 1024 * 1024, // 10 MB
            max_batch_size: 1000,
            max_message_size: 1024 * 1024, // 1 MB
            default_retention_period: 7 * 24 * 60 * 60, // 7 days
            default_compression: false,
        }
    }
}

/// Push connector for receiving data pushed from external systems
/// Uses a hybrid architecture with management plane integrated into API service
/// and data plane as a separate service
pub struct PushConnector {
    /// Management plane for topic management, health checks, etc.
    management_plane: Arc<management::PushManagementPlane>,

    /// Data plane for receiving and processing data
    data_plane: Option<dataplane::PushDataPlane>,

    /// Service registry for data plane services
    service_registry: Arc<discovery::PushServiceRegistry>,

    /// Configuration
    config: PushConnectorConfig,

    /// Message sender
    message_tx: Option<mpsc::Sender<source::PushMessage>>,
}

impl PushConnector {
    /// Create a new Push Connector
    pub fn new() -> Self {
        Self::with_config(PushConnectorConfig::default())
    }

    /// Create a new Push Connector with custom configuration
    pub fn with_config(config: PushConnectorConfig) -> Self {
        // Create management plane
        let management_plane = Arc::new(management::PushManagementPlane::with_config(config.clone()));

        // Create service registry
        let service_registry = Arc::new(discovery::PushServiceRegistry::new());

        // Create connector instance
        let connector = Self {
            management_plane,
            data_plane: None,
            service_registry,
            config: config.clone(),
            message_tx: None,
        };

        // Initialize retry manager
        Self::init_retry_manager(connector.management_plane.message_store());

        connector
    }

    /// Initialize the data plane
    pub fn init_data_plane(&mut self) -> Result<()> {
        // Create data plane
        let mut data_plane = dataplane::PushDataPlane::with_config(
            self.management_plane.clone(),
            self.config.clone(),
        );

        // Enable HTTP protocol by default
        data_plane.enable_protocol(dataplane::ProtocolType::Http)?;

        // Store data plane
        self.data_plane = Some(data_plane);

        Ok(())
    }

    /// Start the data plane
    pub async fn start_data_plane(&mut self) -> Result<()> {
        if let Some(data_plane) = &mut self.data_plane {
            data_plane.start().await?;
        } else {
            self.init_data_plane()?;
            if let Some(data_plane) = &mut self.data_plane {
                data_plane.start().await?;
            }
        }

        Ok(())
    }

    /// Stop the data plane
    pub async fn stop_data_plane(&mut self) -> Result<()> {
        if let Some(data_plane) = &mut self.data_plane {
            data_plane.stop().await?;
        }

        Ok(())
    }

    /// Get the topic manager
    pub fn topic_manager(&self) -> Arc<topic::TopicManager> {
        self.management_plane.topic_manager()
    }

    /// Get the metrics manager
    pub fn metrics_manager(&self) -> Option<Arc<metrics::TopicMetricsManager>> {
        Some(self.management_plane.metrics_manager())
    }

    /// Get the message store
    pub fn message_store(&self) -> Option<Arc<messages::MessageStore>> {
        Some(self.management_plane.message_store())
    }

    /// Get the message validator
    pub fn message_validator(&self) -> Option<Arc<validator::MessageValidator>> {
        Some(self.management_plane.message_validator())
    }

    /// Get the management plane
    pub fn management_plane(&self) -> Arc<management::PushManagementPlane> {
        self.management_plane.clone()
    }

    /// Get the service registry
    pub fn service_registry(&self) -> Arc<discovery::PushServiceRegistry> {
        self.service_registry.clone()
    }

    /// Get the data plane
    pub fn data_plane(&self) -> Option<&dataplane::PushDataPlane> {
        self.data_plane.as_ref()
    }

    /// Get mutable reference to the data plane
    pub fn data_plane_mut(&mut self) -> Option<&mut dataplane::PushDataPlane> {
        self.data_plane.as_mut()
    }

    /// Initialize retry manager
    fn init_retry_manager(message_store: Arc<messages::MessageStore>) {
        // Create a channel for retry messages
        let (tx, rx) = mpsc::channel::<source::PushMessage>(100);

        // Create retry manager
        let retry_config = retry::RetryConfig::default();
        let retry_manager = Arc::new(retry::RetryManager::new(message_store, retry_config));

        // Start retry manager
        let retry_manager_clone = retry_manager.clone();

        // Spawn a task to start the retry manager
        tokio::spawn(async move {
            retry_manager_clone.start(tx).await;
        });

        // Spawn a task to process retry messages
        tokio::spawn(async move {
            Self::process_retry_messages(rx).await;
        });
    }

    /// Process retry messages
    async fn process_retry_messages(mut rx: mpsc::Receiver<source::PushMessage>) {
        while let Some(message) = rx.recv().await {
            // Clone message for use in the closure
            let message_clone = message.clone();
            let topic = message.topic.clone();

            // Get the sender from the global registry
            let tx_clone = {
                let senders = MESSAGE_SENDERS.read().unwrap();
                if let Some(tx) = senders.get(&topic) {
                    Some(tx.clone())
                } else {
                    None
                }
            };

            // Now send the message if we have a sender
            if let Some(tx) = tx_clone {
                if let Err(e) = tx.send(message_clone).await {
                    tracing::error!("Failed to send retry message: {}", e);
                } else {
                    tracing::debug!("Successfully sent retry message for topic {}", topic);
                }
            }
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &PushConnectorConfig {
        &self.config
    }

    /// Set the message sender
    pub fn set_message_sender(&mut self, tx: mpsc::Sender<source::PushMessage>) {
        self.message_tx = Some(tx);
    }

    /// Handle push requests
    pub async fn handle_push(&self, topic: &str, body: axum::body::Bytes) -> axum::response::Response {
        use axum::response::IntoResponse;
        use axum::http::StatusCode;
        use axum::Json;

        // Start processing time measurement
        let start_time = std::time::Instant::now();

        // Validate message size
        if body.len() > self.config.max_message_size {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({
                    "error": format!("Message size exceeds maximum allowed size of {} bytes", self.config.max_message_size)
                })),
            ).into_response();
        }

        // Check if topic exists
        if let Err(_) = self.topic_manager().get_topic(topic) {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!("Topic not found: {}", topic)
                })),
            ).into_response();
        }

        // Create push message
        let message = source::PushMessage {
            id: 0, // Will be assigned by the message store
            topic: topic.to_string(),
            data: body.to_vec(),
            timestamp: std::time::SystemTime::now(),
        };

        // Message processing tracking
        let _message_processed = true;

        // Send message to channel if available
        if let Some(tx) = &self.message_tx {
            if let Err(e) = tx.send(message.clone()).await {
                tracing::error!("Failed to send message to channel: {}", e);
            } else {
                tracing::debug!("Message sent to direct channel for topic {}", topic);
            }
        }

        // Try to send message using global registry
        {
            // Get the sender from the global registry without holding the lock across await
            let tx_opt = {
                if let Ok(senders) = MESSAGE_SENDERS.read() {
                    senders.get(topic).cloned()
                } else {
                    None
                }
            };

            // Now send the message if we have a sender
            if let Some(tx) = tx_opt {
                if let Err(e) = tx.send(message.clone()).await {
                    tracing::error!("Failed to send message to global channel: {}", e);
                } else {
                    tracing::debug!("Message sent to global registry channel for topic {}", topic);
                }
            }
        }

        // Store message in message store
        if let Some(store) = self.message_store() {
            if let Err(e) = store.store_message(&message.topic, message.data.clone()) {
                tracing::error!("Failed to store message: {}", e);
            } else {
                tracing::debug!("Message stored in message store for topic {}", topic);
            }
        }

        // Update metrics
        if let Some(metrics) = self.metrics_manager() {
            if let Err(e) = metrics.record_message(topic, body.len()) {
                tracing::error!("Failed to record message metrics: {}", e);
            }
        }

        // Get elapsed time for response
        let elapsed = start_time.elapsed();

        // Return response
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "topic": topic,
                "message_size": body.len(),
                "processing_time_ms": elapsed.as_millis(),
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            })),
        ).into_response()
    }

    /// Send a message
    pub async fn send_message(&self, mut message: source::PushMessage) -> Result<()> {
        let topic = message.topic.clone();
        let data = message.data.clone();

        // Store message in message store
        let message_store = self.message_store().ok_or_else(|| anyhow::anyhow!("Message store not available"))?;
        let message_data = match message_store.store_message(&topic, data) {
            Ok(data) => data,
            Err(e) => {
                tracing::error!("Failed to store message: {}", e);
                return Err(anyhow::anyhow!("Failed to store message: {}", e));
            }
        };

        // Update message ID
        message.id = message_data.id;

        // Update metrics
        if let Some(metrics) = self.metrics_manager() {
            if let Err(e) = metrics.record_message(&topic, message_data.size) {
                tracing::error!("Failed to record message metrics: {}", e);
            }
        }

        // Validate message
        if let Some(validator) = self.message_validator() {
            if let Err(e) = validator.validate(&topic, &message.data) {
                tracing::warn!("Message validation failed: {}", e);
                // Mark message as failed
                if let Err(e) = message_store.mark_message_failed(&topic, message_data.id, e.to_string()) {
                    tracing::error!("Failed to mark message as failed: {}", e);
                }
                return Err(anyhow::anyhow!("Message validation failed"));
            }
        }

        // Mark message as processing
        if let Err(e) = message_store.mark_message_processing(&topic, message_data.id) {
            tracing::error!("Failed to mark message as processing: {}", e);
        }

        // First try to use the local message sender
        if let Some(tx) = &self.message_tx {
            match tx.send(message.clone()).await {
                Ok(_) => {
                    // Mark message as processed
                    if let Err(e) = message_store.mark_message_processed(&topic, message_data.id) {
                        tracing::error!("Failed to mark message as processed: {}", e);
                    }
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!("Failed to send message via local sender: {}", e);
                }
            }
        }

        // If local sender is not available or fails, try to use the global registry
        if let Ok(senders) = MESSAGE_SENDERS.read() {
            if let Some(tx) = senders.get(&topic) {
                match tx.send(message.clone()).await {
                    Ok(_) => {
                        tracing::debug!("Message for topic {} sent to streaming pipeline via global registry", topic);
                        // Mark message as processed
                        if let Err(e) = message_store.mark_message_processed(&topic, message_data.id) {
                            tracing::error!("Failed to mark message as processed: {}", e);
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::error!("Failed to send message to streaming pipeline via global registry: {}", e);
                    }
                }
            }
        }

        // If we get here, no sender was available
        tracing::warn!("Message sender not set, message for topic {} will not be processed by streaming pipeline", topic);

        // Mark message as failed
        if let Err(e) = message_store.mark_message_failed(&topic, message_data.id, "No message sender available".to_string()) {
            tracing::error!("Failed to mark message as failed: {}", e);
        }

        // Return error since we couldn't process the message
        Err(anyhow::anyhow!("No message sender available for topic {}", topic))
    }
}

impl Connector for PushConnector {
    type ProfileT = PushConfig;
    type TableT = PushTable;

    fn name(&self) -> &'static str {
        "push"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "push".to_string(),
            name: "Push".to_string(),
            icon: ICON.to_string(),
            description: "Receive data pushed from external systems".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        let buffer_size = match config.buffer_size {
            Some(size) => size,
            None => 10 * 1024 * 1024,
        };
        format!("Push connector with buffer size: {}", buffer_size)
    }

    fn table_type(&self, _config: Self::ProfileT, _table: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn test(
        &self,
        _name: &str,
        _config: Self::ProfileT,
        _table: Self::TableT,
        _schema: Option<&ConnectionSchema>,
        _tx: Sender<TestSourceMessage>,
    ) {
        // TODO: Implement test functionality
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        // Parse protocol-specific options
        sql::parse_protocol_options(options)?;

        // Validate options (pass immutable reference)
        sql::validate_protocol_options(options)?;

        let topic = options
            .pull_opt_str("topic")?
            .ok_or_else(|| anyhow::anyhow!("topic is required"))?;

        let table = PushTable {
            topic,
            protocol: options
                .pull_opt_str("protocol")?
                .unwrap_or_else(|| "http".to_string()),
            retention_period: options.pull_opt_u64("retention_period")?,
            http_config: options.pull_opt_str("http_config")?.and_then(|s| {
                serde_json::from_str::<HashMap<String, String>>(&s).ok()
            }),
            quic_config: options.pull_opt_str("quic_config")?.and_then(|s| {
                serde_json::from_str::<HashMap<String, String>>(&s).ok()
            }),
            grpc_config: options.pull_opt_str("grpc_config")?.and_then(|s| {
                serde_json::from_str::<HashMap<String, String>>(&s).ok()
            }),
            websocket_config: options.pull_opt_str("websocket_config")?.and_then(|s| {
                serde_json::from_str::<HashMap<String, String>>(&s).ok()
            }),
            compression: options.pull_opt_str("compression")?,
            batch_size: options.pull_opt_u64("batch_size")?,
        };

        let config = if let Some(profile) = profile {
            serde_json::from_value(profile.config.clone()).map_err(|e| {
                anyhow::anyhow!("Failed to parse connection profile: {}", e)
            })?
        } else {
            PushConfig {
                buffer_size: options.pull_opt_u64("buffer_size")?.map(|v| v as usize),
                max_batch_size: options.pull_opt_u64("max_batch_size")?.map(|v| v as usize),
                authentication: None,
            }
        };

        self.from_config(None, name, config, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        // Create a combined config that includes both profile and table
        let table_config = serde_json::json!({
            "topic": table.topic,
            "protocol": table.protocol,
            "retention_period": table.retention_period,
            "http_config": table.http_config,
            "quic_config": table.quic_config,
            "grpc_config": table.grpc_config,
            "websocket_config": table.websocket_config,
            "compression": table.compression,
            "batch_size": table.batch_size
        });

        let combined_config = serde_json::json!({
            "buffer_size": config.buffer_size,
            "max_batch_size": config.max_batch_size,
            "authentication": config.authentication,
            "connection": table_config.clone(),
            "table": table_config
        });

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type: ConnectionType::Source,
            schema: schema.cloned().unwrap_or_else(|| ConnectionSchema {
                format: None,
                bad_data: None,
                framing: None,
                struct_name: None,
                fields: vec![],
                definition: None,
                inferred: None,
                primary_keys: Default::default(),
            }),
            config: serde_json::to_string(&combined_config)?,
            description: format!("Push connector for topic: {}", table.topic),
            partition_fields: None,
        })
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        // Log the profile and table for debugging
        tracing::debug!("Making operator with profile: {:?}", profile);
        tracing::debug!("Making operator with table: {:?}", table);

        source::PushSourceFunc::new_operator(profile, table, config)
    }
}
