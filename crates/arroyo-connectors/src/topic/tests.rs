#[cfg(test)]
mod tests {
    use arroyo_operator::connector::Connector;
    use arroyo_rpc::api_types::connections::{ConnectionSchema, ConnectionType};
    use arroyo_rpc::formats::{Format, JsonFormat};
    use arroyo_rpc::ConnectorOptions;
    use arroyo_types::Record;
    use serde_json::json;
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::topic::sink::{PartitionStrategy, TopicSinkFunc};
    use crate::topic::source::TopicSourceFunc;
    use crate::topic::{SourceOffset, TableType, TopicTable};

    // 使用 SQL 解析器创建 ConnectorOptions
    fn mock_connector_options() -> ConnectorOptions {
        ConnectorOptions::try_from(&vec![]).unwrap()
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

    #[test]
    fn test_topic_source_batch_prefetch() {
        // 创建 TopicSourceFunc 实例
        let source_func = TopicSourceFunc {
            topic: "test-topic".to_string(),
            offset_mode: crate::topic::source::SourceOffset::Earliest,
            format: Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            }),
            framing: None,
            bad_data: None,
            metadata_fields: vec![],
            batch_size: 10,
            prefetch_count: 20,
            prefetch_timeout: Duration::from_millis(100),
        };

        // 验证批处理和预取配置
        assert_eq!(source_func.batch_size, 10);
        assert_eq!(source_func.prefetch_count, 20);
        assert_eq!(source_func.prefetch_timeout, Duration::from_millis(100));
    }

    #[test]
    fn test_topic_source_prefetch_data() {
        // 创建 TopicSourceFunc 实例
        let source_func = TopicSourceFunc {
            topic: "test-topic".to_string(),
            offset_mode: crate::topic::source::SourceOffset::Earliest,
            format: Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            }),
            framing: None,
            bad_data: None,
            metadata_fields: vec![],
            batch_size: 5,
            prefetch_count: 10,
            prefetch_timeout: Duration::from_millis(100),
        };

        // 创建预取缓冲区
        let mut prefetch_buffer: HashMap<u32, Vec<Record<String>>> = HashMap::new();
        let partitions = vec![0u32, 1u32];
        let mut offsets = HashMap::new();
        offsets.insert(0u32, 0u64);
        offsets.insert(1u32, 0u64);

        // 执行预取
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            source_func.prefetch_data(&mut prefetch_buffer, &partitions, &offsets).await.unwrap();
        });

        // 验证预取结果
        assert_eq!(prefetch_buffer.len(), 2); // 两个分区
        assert_eq!(prefetch_buffer.get(&0).unwrap().len(), 10); // 每个分区预取 10 条记录
        assert_eq!(prefetch_buffer.get(&1).unwrap().len(), 10);
    }

    #[test]
    fn test_topic_sink_partition_strategy() {
        use arroyo_formats::ser::ArrowSerializer;

        // 创建 TopicSinkFunc 实例
        let sink_func = TopicSinkFunc::new(
            "test-topic".to_string(),
            ArrowSerializer::new(Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            })),
        )
        .with_partition_strategy(PartitionStrategy::RoundRobin)
        .with_partition_count(3);

        // 验证分区策略
        assert_eq!(sink_func.partition_strategy, PartitionStrategy::RoundRobin);
        assert_eq!(sink_func.partition_count, 3);
    }

    #[test]
    fn test_topic_sink_transaction() {
        use arroyo_formats::ser::ArrowSerializer;

        // 创建 TopicSinkFunc 实例
        let sink_func = TopicSinkFunc::new(
            "test-topic".to_string(),
            ArrowSerializer::new(Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            })),
        )
        .with_transactions();

        // 验证事务配置
        assert!(sink_func.transactional);
    }
}
