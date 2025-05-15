use arroyo_operator::connector::{Connector, Connection, LookupConnector, MetadataDef};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use arroyo_types::formats::{Format, Framing, BadData};
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
pub mod http;
pub mod source;

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
        format!("Push connector with buffer size: {}",
                config.buffer_size.unwrap_or(10 * 1024 * 1024))
    }

    fn table_type(&self, _config: Self::ProfileT, table: Self::TableT) -> ConnectionType {
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
        // TODO: Implement from_options
        unimplemented!("from_options not implemented for push connector")
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
            config: serde_json::to_string(&config).unwrap(),
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
