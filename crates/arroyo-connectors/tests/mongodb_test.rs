#[cfg(test)]
mod tests {
    use arroyo_connectors::mongodb::MongoDBConnector;
    use arroyo_operator::connector::Connector;
    use arroyo_rpc::api_types::connections::{ConnectionType, TestSourceMessage};
    use arroyo_rpc::grpc::rpc::Format;
    use arroyo_rpc::OperatorConfig;
    use serde_json::json;
    use std::time::Duration;
    use tokio::time::timeout;

    // 这个测试需要一个运行中的 MongoDB 实例
    // 可以使用 Docker 启动一个测试实例：
    // docker run --name mongodb-test -p 27017:27017 -d mongo:latest
    #[tokio::test]
    #[ignore] // 忽略这个测试，除非明确要运行它
    async fn test_mongodb_connector() {
        // 创建 MongoDB 连接器
        let connector = MongoDBConnector {};

        // 验证连接器元数据
        let metadata = connector.metadata();
        assert_eq!(metadata.id, "mongodb");
        assert_eq!(metadata.name, "MongoDB");
        assert!(metadata.source);
        assert!(metadata.sink);

        // 创建配置
        let profile_json = json!({
            "connection_string": "mongodb://localhost:27017",
        });
        let profile = serde_json::from_value(profile_json).unwrap();

        // 测试连接
        let rx = connector.test_profile(profile).unwrap();
        let result = timeout(Duration::from_secs(5), rx).await.unwrap();

        match result {
            TestSourceMessage::Done { message } => {
                println!("Connection test result: {}", message);
                assert!(message.contains("Successfully connected to MongoDB"));
            }
            TestSourceMessage::Info { message } => {
                println!("Connection test info: {}", message);
            }
            TestSourceMessage::Fail { message } => {
                panic!("Connection test failed: {}", message);
            }
        }

        // 测试源表配置
        let source_table_json = json!({
            "database": "test",
            "collection": "test_collection",
            "connectorType": {
                "filter": null,
                "batchSize": 100,
                "pollInterval": 1000
            }
        });
        let source_table = serde_json::from_value(source_table_json).unwrap();

        // 验证表类型
        let table_type = connector.table_type(profile.clone(), source_table.clone());
        assert_eq!(table_type, ConnectionType::Source);

        // 测试目标表配置
        let sink_table_json = json!({
            "database": "test",
            "collection": "test_collection",
            "connectorType": {
                "writeMode": "Insert",
                "batchSize": 100,
                "idField": null
            }
        });
        let sink_table = serde_json::from_value(sink_table_json).unwrap();

        // 验证表类型
        let table_type = connector.table_type(profile.clone(), sink_table.clone());
        assert_eq!(table_type, ConnectionType::Sink);

        // 测试创建操作符
        let config = OperatorConfig {
            format: Some(Format::Json as i32),
            framing: None,
            bad_data: None,
            ..Default::default()
        };

        // 创建源操作符
        let source_operator = connector.make_operator(
            profile.clone(),
            source_table.clone(),
            config.clone(),
        );
        assert!(source_operator.is_ok());

        // 创建目标操作符
        let sink_operator = connector.make_operator(
            profile.clone(),
            sink_table.clone(),
            config.clone(),
        );
        assert!(sink_operator.is_ok());
    }
}
