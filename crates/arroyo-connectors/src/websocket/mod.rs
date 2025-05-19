use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use arroyo_types::string_to_map;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
// use tokio_tungstenite::tungstenite::handshake::client::generate_key;
// use tokio_tungstenite::tungstenite::http::Uri;
// use tokio_tungstenite::{connect_async, tungstenite};
// use tungstenite::http::Request;
use typify::import_types;

use crate::{header_map, EmptyConfig};

use crate::websocket::operator::{WebsocketSourceFunc, WebsocketSourceState};
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

mod operator;

const TABLE_SCHEMA: &str = include_str!("./table.json");

import_types!(schema = "src/websocket/table.json", convert = { {type = "string", format = "var-str"} = VarStr });
const ICON: &str = include_str!("./websocket.svg");

pub struct WebsocketConnector {}

impl Connector for WebsocketConnector {
    type ProfileT = EmptyConfig;

    type TableT = WebsocketTable;

    fn name(&self) -> &'static str {
        "websocket"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "websocket".to_string(),
            name: "Websocket".to_string(),
            icon: ICON.to_string(),
            description: "Connect to a Websocket server".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        // Temporarily disabled due to missing dependencies
        tokio::task::spawn(async move {
            tx.send(TestSourceMessage {
                error: false,
                done: true,
                message: "WebSocket test is temporarily disabled".to_string(),
            })
            .await
            .unwrap();
        });
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let description = format!("WebsocketSource<{}>", table.endpoint);

        if let Some(headers) = &table.headers {
            string_to_map(&headers.sub_env_vars()?, ':').ok_or_else(|| {
                anyhow!(
                    "Invalid format for headers; should be a \
                    comma-separated list of colon-separated key value pairs"
                )
            })?;
        }

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for WebSocket connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for WebSocket connection"))?;

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
            ConnectionType::Source,
            schema,
            &config,
            description,
        ))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let endpoint = options.pull_str("endpoint")?;
        let headers = options.pull_opt_str("headers")?;
        let mut subscription_messages = vec![];

        // add the single subscription message if it exists
        if let Some(message) = options.pull_opt_str("subscription_message")? {
            subscription_messages.push(SubscriptionMessage(message));

            if options.contains_key("subscription_messages.0") {
                return Err(anyhow!(
                    "Cannot specify both 'subscription_message' and 'subscription_messages.0'"
                ));
            }
        }

        // add the indexed subscription messages if they exist
        let mut message_index = 0;
        while let Some(message) =
            options.pull_opt_str(&format!("subscription_messages.{}", message_index))?
        {
            subscription_messages.push(SubscriptionMessage(message));
            message_index += 1;
        }

        self.from_config(
            None,
            name,
            EmptyConfig {},
            WebsocketTable {
                endpoint,
                headers: headers.map(VarStr::new),
                subscription_message: None,
                subscription_messages,
            },
            schema,
        )
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        // Include subscription_message for backwards compatibility
        let mut subscription_messages = vec![];
        if let Some(message) = table.subscription_message {
            subscription_messages.push(message.to_string());
        };
        subscription_messages.extend(
            table
                .subscription_messages
                .into_iter()
                .map(|m| m.to_string()),
        );

        let headers = header_map(table.headers)
            .into_iter()
            .map(|(k, v)| ((&k).into(), (&v).into()))
            .collect();

        Ok(ConstructedOperator::from_source(Box::new(
            WebsocketSourceFunc {
                url: table.endpoint,
                headers,
                subscription_messages,
                format: config
                    .format
                    .ok_or_else(|| anyhow!("format required for websocket source"))?,
                framing: config.framing,
                bad_data: config.bad_data,
                state: WebsocketSourceState::default(),
            },
        )))
    }
}
