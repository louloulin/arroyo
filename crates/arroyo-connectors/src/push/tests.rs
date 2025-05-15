#[cfg(test)]
mod tests {
    use crate::push::{PushConnector, PushConfig, PushTable};
    use arroyo_operator::connector::Connector;
    use arroyo_rpc::api_types::connections::ConnectionType;

    #[test]
    fn test_push_connector_name() {
        let connector = PushConnector {};
        assert_eq!(connector.name(), "push");
    }

    #[test]
    fn test_push_connector_metadata() {
        let connector = PushConnector {};
        let metadata = connector.metadata();
        assert_eq!(metadata.id, "push");
        assert_eq!(metadata.name, "Push");
        assert!(metadata.source);
        assert!(!metadata.sink);
        assert!(metadata.testing);
        assert!(!metadata.hidden);
        assert!(metadata.custom_schemas);
        assert!(metadata.connection_config.is_some());
    }

    #[test]
    fn test_push_connector_table_type() {
        let connector = PushConnector {};
        let config = PushConfig {
            buffer_size: Some(1024),
            max_batch_size: Some(100),
            authentication: None,
        };
        let table = PushTable {
            topic: "test-topic".to_string(),
            protocol: "http".to_string(),
            retention_period: None,
            http_config: None,
            quic_config: None,
            grpc_config: None,
            websocket_config: None,
            compression: None,
            batch_size: None,
        };
        let table_type = connector.table_type(config, table);
        assert_eq!(table_type, ConnectionType::Source);
    }

    #[test]
    fn test_push_connector_config_description() {
        let connector = PushConnector {};
        let config = PushConfig {
            buffer_size: Some(1024),
            max_batch_size: Some(100),
            authentication: None,
        };
        let description = connector.config_description(config);
        assert_eq!(description, "Push connector with buffer size: 1024");
    }
}
