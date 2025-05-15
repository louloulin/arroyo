#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use tokio::sync::mpsc;
    use tokio::time::sleep;

    use crate::push::http::{HttpServer, HttpServerConfig, PushResponse};
    use crate::push::source::PushMessage;

    #[tokio::test]
    async fn test_http_server_start_stop() {
        // Create HTTP server
        let config = HttpServerConfig {
            addr: "127.0.0.1:8001".parse().unwrap(),
            timeout: 30,
            max_connections: 100,
        };
        let mut server = HttpServer::new(config);

        // Create channel
        let (tx, mut rx) = mpsc::channel(100);

        // Start server
        let result = server.start(tx).await;
        assert!(result.is_ok());

        // Wait a bit
        sleep(Duration::from_millis(100)).await;

        // Stop server
        let result = server.stop().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_http_push_endpoint() {
        // Create HTTP server
        let config = HttpServerConfig {
            addr: "127.0.0.1:8002".parse().unwrap(),
            timeout: 30,
            max_connections: 100,
        };
        let mut server = HttpServer::new(config);

        // Create channel
        let (tx, mut rx) = mpsc::channel(100);

        // Start server
        let result = server.start(tx).await;
        assert!(result.is_ok());

        // Wait a bit
        sleep(Duration::from_millis(100)).await;

        // Send request
        let client = reqwest::Client::new();
        let response = client
            .post("http://127.0.0.1:8002/api/v1/push/test-topic")
            .body("test data")
            .send()
            .await;

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.status(), 200);

        // Check response
        let push_response = response.json::<PushResponse>().await;
        assert!(push_response.is_ok());
        let push_response = push_response.unwrap();
        assert!(push_response.success);
        assert_eq!(push_response.message, "Message received");

        // Check received message
        let message = rx.recv().await;
        assert!(message.is_some());
        let message = message.unwrap();
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.data, b"test data");

        // Stop server
        let result = server.stop().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_http_health_endpoint() {
        // Create HTTP server
        let config = HttpServerConfig {
            addr: "127.0.0.1:8003".parse().unwrap(),
            timeout: 30,
            max_connections: 100,
        };
        let mut server = HttpServer::new(config);

        // Create channel
        let (tx, mut rx) = mpsc::channel(100);

        // Start server
        let result = server.start(tx).await;
        assert!(result.is_ok());

        // Wait a bit
        sleep(Duration::from_millis(100)).await;

        // Send request
        let client = reqwest::Client::new();
        let response = client
            .get("http://127.0.0.1:8003/api/v1/push/health")
            .send()
            .await;

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.status(), 200);

        // Check response
        let health_response = response.json::<serde_json::Value>().await;
        assert!(health_response.is_ok());
        let health_response = health_response.unwrap();
        assert_eq!(health_response["status"], "ok");

        // Stop server
        let result = server.stop().await;
        assert!(result.is_ok());
    }
}
