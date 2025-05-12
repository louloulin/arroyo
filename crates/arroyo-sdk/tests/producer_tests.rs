use arroyo_sdk::{ArroyoClient, Producer, ProducerBuilder, Result, SendResult};
use std::collections::HashMap;
use tokio::sync::oneshot;

// 创建模拟客户端
async fn create_mock_client(server: &mockito::Server) -> ArroyoClient {
    ArroyoClient::new(&server.url()).unwrap()
}

#[tokio::test]
async fn test_sync_send() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new();

    // 设置模拟响应
    let _m = server.mock("POST", "/api/topics/test-topic/messages")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create();

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .send_timeout_ms(1000)
            .build(),
    );

    // 发送消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let result = producer.send(key, value).await?;

    // 验证结果
    assert!(result.success);
    assert!(result.error.is_none());
    assert!(result.latency_ms > 0);
    assert_eq!(result.size_bytes, 18); // "test-key" + "test-value" = 8 + 10 = 18

    Ok(())
}

#[tokio::test]
async fn test_sync_send_with_headers() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new();

    // 设置模拟响应
    let _m = server.mock("POST", "/api/topics/test-topic/messages")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create();

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .send_timeout_ms(1000)
            .build(),
    );

    // 发送消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let mut headers = HashMap::new();
    headers.insert("header1".to_string(), b"value1".to_vec());
    headers.insert("header2".to_string(), b"value2".to_vec());
    let result = producer.send_with_headers(key, value, headers).await?;

    // 验证结果
    assert!(result.success);
    assert!(result.error.is_none());
    assert!(result.latency_ms > 0);
    assert_eq!(result.size_bytes, 18); // "test-key" + "test-value" = 8 + 10 = 18

    Ok(())
}

#[tokio::test]
async fn test_async_send() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new();

    // 设置模拟响应
    let _m = server.mock("POST", "/api/topics/test-topic/messages")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create();

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .send_timeout_ms(1000)
            .build(),
    );

    // 创建通道用于接收回调结果
    let (tx, rx) = oneshot::channel::<SendResult>();

    // 发送消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    producer.send_async(key, value, Some(Box::new(move |result| {
        let _ = tx.send(result);
    })));

    // 等待回调结果
    let result = rx.await.unwrap();

    // 验证结果
    assert!(result.success);
    assert!(result.error.is_none());
    assert!(result.latency_ms > 0);
    assert_eq!(result.size_bytes, 18); // "test-key" + "test-value" = 8 + 10 = 18

    Ok(())
}

#[tokio::test]
async fn test_batch_send() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new();

    // 设置模拟响应
    let _m = server.mock("POST", "/api/topics/test-topic/messages/batch")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create();

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .batch_size(1000)
            .build(),
    );

    // 创建消息批次
    let mut messages = Vec::new();
    for i in 0..5 {
        let key = Some(format!("key-{}", i).into_bytes());
        let value = format!("value-{}", i).into_bytes();
        let message = arroyo_sdk::models::Message {
            key,
            value,
            headers: None,
            timestamp: None,
            partition: None,
            offset: None,
        };
        messages.push(message);
    }

    // 发送消息批次
    let results = producer.send_batch(messages).await?;

    // 验证结果
    assert_eq!(results.len(), 5);
    for result in results {
        assert!(result.success);
        assert!(result.error.is_none());
        assert!(result.latency_ms > 0);
    }

    Ok(())
}

#[tokio::test]
async fn test_batch_send_async() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new();

    // 设置模拟响应
    let _m = server.mock("POST", "/api/topics/test-topic/messages/batch")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create();

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .batch_size(1000)
            .build(),
    );

    // 创建消息批次
    let mut messages = Vec::new();
    for i in 0..5 {
        let key = Some(format!("key-{}", i).into_bytes());
        let value = format!("value-{}", i).into_bytes();
        let message = arroyo_sdk::models::Message {
            key,
            value,
            headers: None,
            timestamp: None,
            partition: None,
            offset: None,
        };
        messages.push(message);
    }

    // 创建通道用于接收回调结果
    let (tx, rx) = oneshot::channel::<Vec<SendResult>>();

    // 发送消息批次
    producer.send_batch_async(messages, Some(Box::new(move |results| {
        let _ = tx.send(results);
    })));

    // 等待回调结果
    let results = rx.await.unwrap();

    // 验证结果
    assert_eq!(results.len(), 5);
    for result in results {
        assert!(result.success);
        assert!(result.error.is_none());
        assert!(result.latency_ms > 0);
    }

    Ok(())
}
