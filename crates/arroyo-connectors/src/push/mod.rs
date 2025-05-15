use arroyo_operator::connector::{Connector, Connection, LookupConnector, MetadataDef};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use arroyo_rpc::formats::{Format, Framing, BadData};
use datafusion::sql::sqlparser::ast::{Expr, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Receiver;
use typify::import_types;

pub mod auth;
pub mod backpressure;
pub mod batch;
pub mod buffer;
pub mod converter;
pub mod http;
pub mod source;
pub mod sql;

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

// Import types from JSON schema
import_types!(
    schema = "src/push/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

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

/// Push connector for receiving data pushed from external systems
pub struct PushConnector {}

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
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
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
                buffer_size: match options.pull_opt_u64("buffer_size")? {
                    Some(size) => Some(size as usize),
                    None => None,
                },
                max_batch_size: match options.pull_opt_u64("max_batch_size")? {
                    Some(size) => Some(size as usize),
                    None => None,
                },
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
