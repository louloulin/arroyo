use anyhow::anyhow;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connector, MetadataDef};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::OperatorConfig;
use ::mongodb::options::{ClientOptions, Credential};
use ::mongodb::Client as MongoClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::oneshot::Receiver;
use typify::import_types;

pub mod sink;
pub mod source;

use crate::mongodb::sink::MongoDBSinkFunc;
use crate::mongodb::source::MongoDBSourceFunc;

pub struct MongoDBConnector {}

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./mongodb.svg");

import_types!(
    schema = "src/mongodb/profile.json",
    convert = {
        {type = "string", format = "var-str"} = arroyo_rpc::var_str::VarStr
    }
);

import_types!(schema = "src/mongodb/table.json");

pub(crate) struct MongoDBClient {
    client: MongoClient,
}

impl MongoDBClient {
    pub async fn new(config: &MongoDbConfig) -> anyhow::Result<Self> {
        let mut client_options = ClientOptions::parse(&config.connection_string.sub_env_vars()?)
            .await
            .map_err(|e| anyhow!("Failed to parse MongoDB connection string: {:?}", e))?;

        // 设置认证信息
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            let credential = Credential::builder()
                .username(username.sub_env_vars()?)
                .password(password.sub_env_vars()?)
                .build();
            client_options.credential = Some(credential);
        }

        // 创建客户端
        let client = MongoClient::with_options(client_options)
            .map_err(|e| anyhow!("Failed to create MongoDB client: {:?}", e))?;

        Ok(Self { client })
    }

    pub fn get_client(&self) -> MongoClient {
        self.client.clone()
    }
}

#[allow(dependency_on_unit_never_type_fallback)]
async fn test_inner(
    c: MongoDbConfig,
    tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
) -> anyhow::Result<String> {
    tx.send(TestSourceMessage::info("Connecting to MongoDB"))
        .await
        .unwrap();

    let client = MongoDBClient::new(&c).await?;

    tx.send(TestSourceMessage::info(
        "Connected successfully, listing databases",
    ))
    .await
    .unwrap();

    // 尝试列出所有数据库，验证连接是否正常
    let db_names = client
        .get_client()
        .list_database_names(None, None)
        .await
        .map_err(|e| anyhow!("Failed to list databases: {:?}", e))?;

    Ok(format!(
        "Successfully connected to MongoDB. Available databases: {}",
        db_names.join(", ")
    ))
}

impl Connector for MongoDBConnector {
    type ProfileT = MongoDbConfig;
    type TableT = MongoDbTable;

    fn name(&self) -> &'static str {
        "mongodb"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "mongodb".to_string(),
            name: "MongoDB".to_string(),
            icon: ICON.to_string(),
            description: "Read from or write to MongoDB collections".to_string(),
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
        match table.connector_type {
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
                Err(e) => TestSourceMessage::fail(format!("Failed to connect to MongoDB: {:?}", e)),
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
                ConnectorType::Source { .. } => {
                    match MongoDBClient::new(&profile).await {
                        Ok(client) => {
                            let client = Arc::new(client);
                            let source_func = MongoDBSourceFunc {
                                client,
                                database: table.database,
                                collection: table.collection,
                                format: match config.format {
                                    Some(f) => f,
                                    None => {
                                        let _ = tx.send(Err(anyhow!("format required for MongoDB source")));
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
                ConnectorType::Sink { .. } => {
                    match MongoDBClient::new(&profile).await {
                        Ok(client) => {
                            let client = Arc::new(client);
                            let sink_func = MongoDBSinkFunc {
                                client,
                                database: table.database,
                                collection: table.collection,
                                serializer: ArrowSerializer::new(
                                    match config.format {
                                        Some(f) => f,
                                        None => {
                                            let _ = tx.send(Err(anyhow!("format required for MongoDB sink")));
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
