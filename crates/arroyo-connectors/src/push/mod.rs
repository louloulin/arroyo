use anyhow::Result;
use arroyo_operator::connector::{Connector, Connection};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Sender};

pub mod api;
pub mod auth;
pub mod backpressure;
pub mod batch;
pub mod buffer;
pub mod converter;
pub mod http;
pub mod messages;
pub mod metrics;
pub mod source;
pub mod sql;
pub mod topic;
pub mod validator;

#[cfg(test)]
mod topic_tests;

#[cfg(test)]
mod metrics_tests;

#[cfg(test)]
mod messages_tests;

#[cfg(test)]
mod tests;

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
pub struct PushConnector {
    topic_manager: Arc<topic::TopicManager>,
    metrics_manager: Arc<metrics::TopicMetricsManager>,
    message_store: Arc<messages::MessageStore>,
    message_validator: Arc<validator::MessageValidator>,
    message_tx: Option<mpsc::Sender<source::PushMessage>>,
    config: PushConnectorConfig,
}

impl PushConnector {
    /// Create a new Push Connector
    pub fn new() -> Self {
        Self::with_config(PushConnectorConfig::default())
    }

    /// Create a new Push Connector with custom configuration
    pub fn with_config(config: PushConnectorConfig) -> Self {
        Self {
            topic_manager: Arc::new(topic::TopicManager::new()),
            metrics_manager: Arc::new(metrics::TopicMetricsManager::new()),
            message_store: Arc::new(messages::MessageStore::new(1000)), // Store up to 1000 messages per topic
            message_validator: Arc::new(validator::MessageValidator::new(
                config.max_message_size,
                100, // max_field_count
                256, // max_field_name_length
                10 * 1024, // max_field_value_length (10 KB)
            )),
            message_tx: None,
            config,
        }
    }

    /// Get the topic manager
    pub fn topic_manager(&self) -> Arc<topic::TopicManager> {
        self.topic_manager.clone()
    }

    /// Get the metrics manager
    pub fn metrics_manager(&self) -> Option<Arc<metrics::TopicMetricsManager>> {
        Some(self.metrics_manager.clone())
    }

    /// Get the message store
    pub fn message_store(&self) -> Option<Arc<messages::MessageStore>> {
        Some(self.message_store.clone())
    }

    /// Get the message validator
    pub fn message_validator(&self) -> Arc<validator::MessageValidator> {
        self.message_validator.clone()
    }

    /// Get the configuration
    pub fn config(&self) -> &PushConnectorConfig {
        &self.config
    }

    /// Set the message sender
    pub fn set_message_sender(&mut self, tx: mpsc::Sender<source::PushMessage>) {
        self.message_tx = Some(tx);
    }

    /// Send a message
    pub async fn send_message(&self, message: source::PushMessage) -> Result<()> {
        if let Some(tx) = &self.message_tx {
            tx.send(message).await.map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Message sender not set"))
        }
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

        // Validate options
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
            config: serde_json::to_string(&config)?,
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
        source::PushSourceFunc::new_operator(profile, table, config)
    }
}
