use anyhow::{anyhow, bail};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, TestSourceMessage};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use typify::import_types;

use crate::topic::sink::TopicSinkFunc;
use crate::topic::source::TopicSourceFunc;
use crate::{ConnectionType, EmptyConfig};

mod admin;
mod alert;
mod health;
mod metrics;
mod partition;
mod permission;
mod quota;
mod sink;
mod source;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admin_tests;

#[cfg(test)]
mod alert_tests;

#[cfg(test)]
mod health_tests;

#[cfg(test)]
mod metrics_tests;

#[cfg(test)]
mod quota_tests;

#[cfg(test)]
mod permission_tests;

#[cfg(test)]
mod permission_integration_tests;

#[cfg(test)]
mod partition_tests;

#[cfg(test)]
mod integration_tests;

pub use admin::TopicAdmin;
pub use alert::{AlertNotifier, EmailNotifier, TopicAlertManager, WebhookNotifier};
pub use health::TopicHealthChecker;
pub use metrics::TopicMetricsCollector;
pub use partition::PartitionManager;
pub use permission::TopicPermissionManager;
pub use quota::TopicQuotaManager;

pub struct TopicConnector {}

const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./topic.svg");

import_types!(schema = "src/topic/table.json");

impl Connector for TopicConnector {
    type ProfileT = EmptyConfig;
    type TableT = TopicTable;

    fn name(&self) -> &'static str {
        "topic"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "topic".to_string(),
            name: "Topic".to_string(),
            icon: ICON.to_string(),
            description: "Read or write from Arroyo topics".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, t: Self::TableT) -> ConnectionType {
        match t.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink {} => ConnectionType::Sink,
        }
    }

    fn get_schema(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        s: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        s.cloned()
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            tx.send(message).await.unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let topic = options.pull_str("topic")?;
        let table_type = options.pull_str("type")?;

        let table_type = match table_type.as_str() {
            "source" => {
                let offset = options.pull_opt_str("source.offset")?;
                let offset_str = match offset.as_deref() {
                    Some("earliest") => "earliest",
                    None | Some("latest") => "latest",
                    Some(other) => bail!("invalid value for source.offset '{}'", other),
                };

                TableType::Source {
                    offset: match offset_str {
                        "earliest" => SourceOffset::Earliest,
                        "latest" => SourceOffset::Latest,
                        _ => bail!("Invalid offset mode: {}", offset_str),
                    },
                }
            }
            "sink" => TableType::Sink {},
            _ => {
                bail!("type must be one of 'source' or 'sink");
            }
        };

        let table = TopicTable {
            topic,
            type_: table_type,
        };

        self.from_config(None, name, EmptyConfig {}, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: EmptyConfig,
        table: TopicTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (typ, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                format!("TopicSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => {
                (ConnectionType::Sink, format!("TopicSink<{}>", table.topic))
            }
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for Topic connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Topic connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            typ,
            schema,
            &config,
            desc,
        ))
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        match table.type_ {
            TableType::Source { offset } => {
                // 将 SourceOffset 转换为 source::SourceOffset
                let offset_mode = match offset {
                    SourceOffset::Earliest => source::SourceOffset::Earliest,
                    SourceOffset::Latest => source::SourceOffset::Latest,
                };

                Ok(ConstructedOperator::from_source(Box::new(
                    TopicSourceFunc {
                        topic: table.topic,
                        offset_mode,
                        format: config
                            .format
                            .ok_or_else(|| anyhow!("format required for topic source"))?,
                        framing: config.framing,
                        bad_data: config.bad_data,
                        metadata_fields: config.metadata_fields,
                        // 默认批处理配置
                        batch_size: 100,
                        // 默认预取配置
                        prefetch_count: 1000,
                        // 默认预取超时
                        prefetch_timeout: std::time::Duration::from_millis(500),
                    },
                )))
            },
            TableType::Sink { .. } => {
                let serializer = ArrowSerializer::new(
                    config
                        .format
                        .ok_or_else(|| anyhow!("format required for topic sink"))?,
                );

                // 创建 TopicSinkFunc 实例
                let sink_func = TopicSinkFunc::new(table.topic, serializer)
                    // 默认使用轮询分区策略
                    .with_partition_strategy(sink::PartitionStrategy::RoundRobin)
                    // 默认启用事务
                    .with_transactions()
                    // 默认分区数量为 3
                    .with_partition_count(3)
                    // 默认刷新间隔为 1 秒
                    .with_flush_interval(std::time::Duration::from_secs(1))
                    // 默认缓冲区大小限制为 1000 条消息
                    .with_buffer_size_limit(1000);

                Ok(ConstructedOperator::from_operator(Box::new(sink_func)))
            },
        }
    }
}
