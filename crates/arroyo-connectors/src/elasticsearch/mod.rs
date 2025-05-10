use anyhow::anyhow;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connector, MetadataDef};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::OperatorConfig;
use ::elasticsearch::Elasticsearch;
use ::elasticsearch::cluster::ClusterHealthParts;
use ::elasticsearch::http::transport::Transport;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::oneshot::Receiver;
use typify::import_types;

pub mod sink;
pub mod source;

use crate::elasticsearch::sink::ElasticsearchSinkFunc;
use crate::elasticsearch::source::ElasticsearchSourceFunc;

pub struct ElasticsearchConnector {}

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./elasticsearch.svg");

import_types!(
    schema = "src/elasticsearch/profile.json",
    convert = {
        {type = "string", format = "var-str"} = arroyo_rpc::var_str::VarStr
    }
);

import_types!(schema = "src/elasticsearch/table.json");

pub(crate) struct ElasticsearchClient {
    client: Elasticsearch,
}

impl ElasticsearchClient {
    pub async fn new(config: &ElasticsearchConfig) -> anyhow::Result<Self> {
        // 确保至少有一个主机
        if config.hosts.is_empty() {
            return Err(anyhow!("No Elasticsearch hosts provided"));
        }

        // 获取第一个主机URL
        let url_str = config.hosts[0].sub_env_vars()?;

        // 创建客户端
        let client = if let (Some(username), Some(password)) = (&config.username, &config.password) {
            let username = username.sub_env_vars()?;
            let password = password.sub_env_vars()?;

            // 创建带认证的客户端
            let conn_str = format!("{}://{}:{}@{}",
                if url_str.starts_with("https") { "https" } else { "http" },
                username,
                password,
                url_str.replace("http://", "").replace("https://", "")
            );

            Elasticsearch::new(Transport::single_node(&conn_str)?)
        } else {
            // 创建不带认证的客户端
            Elasticsearch::new(Transport::single_node(&url_str)?)
        };

        Ok(Self { client })
    }

    pub fn get_client(&self) -> &Elasticsearch {
        &self.client
    }
}

#[allow(dependency_on_unit_never_type_fallback)]
async fn test_inner(
    c: ElasticsearchConfig,
    tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
) -> anyhow::Result<String> {
    tx.send(TestSourceMessage::info("Connecting to Elasticsearch"))
        .await
        .unwrap();

    let client = ElasticsearchClient::new(&c).await?;

    tx.send(TestSourceMessage::info(
        "Connected successfully, checking cluster health",
    ))
    .await
    .unwrap();

    // 检查集群健康状态
    let health = client
        .get_client()
        .cluster()
        .health(ClusterHealthParts::None)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to check cluster health: {:?}", e))?;

    let health_response = health.json::<serde_json::Value>().await
        .map_err(|e| anyhow!("Failed to parse health response: {:?}", e))?;

    let status = health_response["status"].as_str().unwrap_or("unknown");

    Ok(format!(
        "Successfully connected to Elasticsearch. Cluster status: {}",
        status
    ))
}

impl Connector for ElasticsearchConnector {
    type ProfileT = ElasticsearchConfig;
    type TableT = ElasticsearchTable;

    fn name(&self) -> &'static str {
        "elasticsearch"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "elasticsearch".to_string(),
            name: "Elasticsearch".to_string(),
            icon: ICON.to_string(),
            description: "Read from or write to Elasticsearch indices".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn metadata_defs(&self) -> &'static [MetadataDef] {
        &[]
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match &table.connector_type {
            ConnectorType::Source { .. } => ConnectionType::Source,
            ConnectorType::Sink { .. } => ConnectionType::Sink,
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

    fn test_profile(&self, profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let (itx, _rx) = tokio::sync::mpsc::channel(8);
            let message = match test_inner(profile, itx).await {
                Ok(msg) => TestSourceMessage::done(msg),
                Err(e) => TestSourceMessage::fail(format!("Failed to connect to Elasticsearch: {:?}", e)),
            };

            tx.send(message).unwrap();
        });

        Some(rx)
    }

    fn test(
        &self,
        _: &str,
        c: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let resp = match test_inner(c, tx.clone()).await {
                Ok(c) => TestSourceMessage::done(c),
                Err(e) => TestSourceMessage::fail(e.to_string()),
            };

            tx.send(resp).await.unwrap();
        });
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        // 创建一个新的任务来异步初始化客户端
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let result = match &table.connector_type {
                ConnectorType::Source { query, batch_size, poll_interval } => {
                    match ElasticsearchClient::new(&profile).await {
                        Ok(client) => {
                            let client = Arc::new(client);
                            let source_func = ElasticsearchSourceFunc {
                                client,
                                index: table.index.clone(),
                                query: query.clone(),
                                batch_size: 100,
                                poll_interval: 1000,
                                format: match config.format {
                                    Some(f) => f,
                                    None => {
                                        let _ = tx.send(Err(anyhow!("format required for Elasticsearch source")));
                                        return;
                                    }
                                },
                                framing: config.framing,
                                bad_data: config.bad_data,
                            };
                            Ok(ConstructedOperator::from_source(Box::new(source_func)))
                        }
                        Err(e) => Err(e),
                    }
                }
                ConnectorType::Sink { write_mode, id_field, .. } => {
                    match ElasticsearchClient::new(&profile).await {
                        Ok(client) => {
                            let client = Arc::new(client);
                            let sink_func = ElasticsearchSinkFunc {
                                client,
                                index: table.index.clone(),
                                write_mode: write_mode.clone().to_string(),
                                id_field: id_field.clone(),
                                serializer: ArrowSerializer::new(
                                    match config.format {
                                        Some(f) => f,
                                        None => {
                                            let _ = tx.send(Err(anyhow!("format required for Elasticsearch sink")));
                                            return;
                                        }
                                    },
                                ),
                            };
                            Ok(ConstructedOperator::from_operator(Box::new(sink_func)))
                        }
                        Err(e) => Err(e),
                    }
                }
            };

            let _ = tx.send(result);
        });

        // 等待异步任务完成
        rx.blocking_recv().unwrap()
    }

    fn from_options(
        &self,
        _: &str,
        _: &mut arroyo_rpc::ConnectorOptions,
        _: Option<&ConnectionSchema>,
        _: Option<&arroyo_rpc::api_types::connections::ConnectionProfile>,
    ) -> Result<arroyo_operator::connector::Connection, anyhow::Error> {
        // 这个方法在当前实现中不需要，但需要满足 Connector trait
        Err(anyhow!("Not implemented"))
    }

    fn from_config(
        &self,
        _: Option<i64>,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> Result<arroyo_operator::connector::Connection, anyhow::Error> {
        // 这个方法在当前实现中不需要，但需要满足 Connector trait
        Err(anyhow!("Not implemented"))
    }
}
