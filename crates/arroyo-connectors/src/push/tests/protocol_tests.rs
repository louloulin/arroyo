#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc;

    use crate::push::dataplane::ProtocolType;
    use crate::push::management::PushManagementPlane;
    use crate::push::protocol::{ProtocolAdapter, ProtocolAdapterFactory};
    use crate::push::topic::CreateTopicRequest;

    #[tokio::test]
    async fn test_http_adapter() {
        // Create management plane
        let management_plane = Arc::new(PushManagementPlane::new());

        // Create topic
        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: false,
        };

        let result = management_plane.create_topic(request);
        assert!(result.is_ok());

        // Create channel
        let (tx, mut rx) = mpsc::channel(100);

        // Create HTTP adapter
        let adapter = ProtocolAdapterFactory::create(
            ProtocolType::Http,
            tx,
            management_plane.clone(),
            HashMap::new(),
        ).unwrap();

        // Process message
        let result = adapter.process_message("test-topic", b"test data".to_vec()).await;
        assert!(result.is_ok());

        // Receive message
        let message = rx.recv().await.unwrap();
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.data, b"test data");
    }

    #[tokio::test]
    async fn test_http2_adapter() {
        // Create management plane
        let management_plane = Arc::new(PushManagementPlane::new());

        // Create topic
        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: false,
        };

        let result = management_plane.create_topic(request);
        assert!(result.is_ok());

        // Create channel
        let (tx, mut rx) = mpsc::channel(100);

        // Create HTTP/2 adapter
        let adapter = ProtocolAdapterFactory::create(
            ProtocolType::Http2,
            tx,
            management_plane.clone(),
            HashMap::new(),
        ).unwrap();

        // Process message
        let result = adapter.process_message("test-topic", b"test data".to_vec()).await;
        assert!(result.is_ok());

        // Receive message
        let message = rx.recv().await.unwrap();
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.data, b"test data");
    }

    #[tokio::test]
    async fn test_websocket_adapter() {
        // Create management plane
        let management_plane = Arc::new(PushManagementPlane::new());

        // Create topic
        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: false,
        };

        let result = management_plane.create_topic(request);
        assert!(result.is_ok());

        // Create channel
        let (tx, mut rx) = mpsc::channel(100);

        // Create WebSocket adapter
        let adapter = ProtocolAdapterFactory::create(
            ProtocolType::WebSocket,
            tx,
            management_plane.clone(),
            HashMap::new(),
        ).unwrap();

        // Process message
        let result = adapter.process_message("test-topic", b"test data".to_vec()).await;
        assert!(result.is_ok());

        // Receive message
        let message = rx.recv().await.unwrap();
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.data, b"test data");
    }

    #[tokio::test]
    async fn test_protocol_type_conversion() {
        // Test from_str
        assert_eq!(ProtocolType::from_str("http").unwrap(), ProtocolType::Http);
        assert_eq!(ProtocolType::from_str("http2").unwrap(), ProtocolType::Http2);
        assert_eq!(ProtocolType::from_str("websocket").unwrap(), ProtocolType::WebSocket);
        assert_eq!(ProtocolType::from_str("ws").unwrap(), ProtocolType::WebSocket);
        assert_eq!(ProtocolType::from_str("grpc").unwrap(), ProtocolType::Grpc);
        assert_eq!(ProtocolType::from_str("quic").unwrap(), ProtocolType::Quic);

        // Test invalid protocol
        assert!(ProtocolType::from_str("invalid").is_err());

        // Test as_str
        assert_eq!(ProtocolType::Http.as_str(), "http");
        assert_eq!(ProtocolType::Http2.as_str(), "http2");
        assert_eq!(ProtocolType::WebSocket.as_str(), "websocket");
        assert_eq!(ProtocolType::Grpc.as_str(), "grpc");
        assert_eq!(ProtocolType::Quic.as_str(), "quic");
    }

    #[tokio::test]
    async fn test_adapter_config() {
        // Create management plane
        let management_plane = Arc::new(PushManagementPlane::new());

        // Create channel
        let (tx, _rx) = mpsc::channel(100);

        // Create config
        let mut config = HashMap::new();
        config.insert("host".to_string(), "localhost".to_string());
        config.insert("port".to_string(), "8080".to_string());

        // Create HTTP adapter
        let mut adapter = ProtocolAdapterFactory::create(
            ProtocolType::Http,
            tx,
            management_plane.clone(),
            config.clone(),
        ).unwrap();

        // Check config
        assert_eq!(adapter.config().get("host").unwrap(), "localhost");
        assert_eq!(adapter.config().get("port").unwrap(), "8080");

        // Update config
        let mut new_config = HashMap::new();
        new_config.insert("host".to_string(), "127.0.0.1".to_string());
        new_config.insert("port".to_string(), "9090".to_string());

        let result = adapter.update_config(new_config);
        assert!(result.is_ok());

        // Check updated config
        assert_eq!(adapter.config().get("host").unwrap(), "127.0.0.1");
        assert_eq!(adapter.config().get("port").unwrap(), "9090");
    }

    #[tokio::test]
    async fn test_grpc_adapter() {
        // Create management plane
        let management_plane = Arc::new(PushManagementPlane::new());

        // Create topic
        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: false,
        };

        let result = management_plane.create_topic(request);
        assert!(result.is_ok());

        // Create channel
        let (tx, mut rx) = mpsc::channel(100);

        // Create gRPC adapter
        let adapter = ProtocolAdapterFactory::create(
            ProtocolType::Grpc,
            tx,
            management_plane.clone(),
            HashMap::new(),
        ).unwrap();

        // Process message
        let result = adapter.process_message("test-topic", b"test data".to_vec()).await;
        assert!(result.is_ok());

        // Receive message
        let message = rx.recv().await.unwrap();
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.data, b"test data");
    }

    #[tokio::test]
    async fn test_adapter_start_stop() {
        // This test is more of an integration test and would require actual network connections
        // For now, we'll just test that the methods don't panic

        // Create management plane
        let management_plane = Arc::new(PushManagementPlane::new());

        // Create channel
        let (tx, _rx) = mpsc::channel(100);

        // Create config with non-conflicting ports
        let mut http_config = HashMap::new();
        http_config.insert("host".to_string(), "127.0.0.1".to_string());
        http_config.insert("port".to_string(), "18080".to_string());

        let mut http2_config = HashMap::new();
        http2_config.insert("host".to_string(), "127.0.0.1".to_string());
        http2_config.insert("port".to_string(), "18081".to_string());

        let mut ws_config = HashMap::new();
        ws_config.insert("host".to_string(), "127.0.0.1".to_string());
        ws_config.insert("port".to_string(), "18082".to_string());

        let mut grpc_config = HashMap::new();
        grpc_config.insert("host".to_string(), "127.0.0.1".to_string());
        grpc_config.insert("port".to_string(), "18083".to_string());

        // Create adapters
        let mut http_adapter = ProtocolAdapterFactory::create(
            ProtocolType::Http,
            tx.clone(),
            management_plane.clone(),
            http_config,
        ).unwrap();

        let mut http2_adapter = ProtocolAdapterFactory::create(
            ProtocolType::Http2,
            tx.clone(),
            management_plane.clone(),
            http2_config,
        ).unwrap();

        let mut ws_adapter = ProtocolAdapterFactory::create(
            ProtocolType::WebSocket,
            tx.clone(),
            management_plane.clone(),
            ws_config,
        ).unwrap();

        let mut grpc_adapter = ProtocolAdapterFactory::create(
            ProtocolType::Grpc,
            tx.clone(),
            management_plane.clone(),
            grpc_config,
        ).unwrap();

        // Start adapters
        // Note: These might fail if the ports are already in use
        // We're just testing that the methods don't panic
        let _ = http_adapter.start().await;
        let _ = http2_adapter.start().await;
        let _ = ws_adapter.start().await;
        let _ = grpc_adapter.start().await;

        // Wait a bit
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Stop adapters
        let _ = http_adapter.stop().await;
        let _ = http2_adapter.stop().await;
        let _ = ws_adapter.stop().await;
        let _ = grpc_adapter.stop().await;
    }
}
