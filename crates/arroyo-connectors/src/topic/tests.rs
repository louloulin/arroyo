#[cfg(test)]
mod tests {
    use arroyo_operator::connector::Connector;
    use arroyo_rpc::api_types::connections::{ConnectionSchema, ConnectionType};
    use arroyo_rpc::formats::{Format, JsonFormat};
    use arroyo_rpc::ConnectorOptions;
    use serde_json::json;

    use crate::topic::{SourceOffset, TableType, TopicTable};

    // 使用 SQL 解析器创建 ConnectorOptions
    fn mock_connector_options() -> ConnectorOptions {
        let mut options = ConnectorOptions::try_from(&vec![]).unwrap();
        options
    }

    use crate::topic::TopicConnector;

    #[test]
    fn test_topic_connector_metadata() {
        let connector = TopicConnector {};
        let metadata = connector.metadata();

        assert_eq!(metadata.id, "topic");
        assert_eq!(metadata.name, "Topic");
        assert!(metadata.source);
        assert!(metadata.sink);
        assert!(!metadata.hidden);
    }

    #[test]
    fn test_topic_source_config() {
        let connector = TopicConnector {};
        let mut options = mock_connector_options();
        options.insert_str("topic", "test-topic").unwrap();
        options.insert_str("type", "source").unwrap();
        options.insert_str("source.offset", "earliest").unwrap();

        let schema = ConnectionSchema {
            fields: vec![],
            format: Some(Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            })),
            framing: None,
            bad_data: None,
            struct_name: None,
            definition: None,
            inferred: None,
            primary_keys: Default::default(),
        };

        let connection = connector
            .from_options("test-connection", &mut options, Some(&schema), None)
            .unwrap();

        assert_eq!(connection.name, "test-connection");
        assert_eq!(connection.connection_type, ConnectionType::Source);
        assert_eq!(connection.description, "TopicSource<test-topic>");
    }

    #[test]
    fn test_topic_sink_config() {
        let connector = TopicConnector {};
        let mut options = mock_connector_options();
        options.insert_str("topic", "test-topic").unwrap();
        options.insert_str("type", "sink").unwrap();

        let schema = ConnectionSchema {
            fields: vec![],
            format: Some(Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            })),
            framing: None,
            bad_data: None,
            struct_name: None,
            definition: None,
            inferred: None,
            primary_keys: Default::default(),
        };

        let connection = connector
            .from_options("test-connection", &mut options, Some(&schema), None)
            .unwrap();

        assert_eq!(connection.name, "test-connection");
        assert_eq!(connection.connection_type, ConnectionType::Sink);
        assert_eq!(connection.description, "TopicSink<test-topic>");
    }

    #[test]
    fn test_topic_make_operator() {
        let connector = TopicConnector {};

        // 创建 Source 操作符
        let table = TopicTable {
            topic: "test-topic".to_string(),
            type_: TableType::Source {
                offset: SourceOffset::Earliest,
            },
        };

        let config = arroyo_rpc::OperatorConfig {
            connection: json!({}),
            table: json!({
                "topic": "test-topic",
                "type": {
                    "Source": {
                        "offset": "earliest"
                    }
                }
            }),
            rate_limit: None,
            format: Some(Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            })),
            framing: None,
            bad_data: None,
            metadata_fields: vec![],
        };

        let operator = connector.make_operator(crate::EmptyConfig {}, table, config);
        assert!(operator.is_ok());
    }
}
