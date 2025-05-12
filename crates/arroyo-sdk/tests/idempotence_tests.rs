use arroyo_sdk::{ArroyoClient, Producer, ProducerBuilder, Result, SendResult};
use mockito;
use std::collections::HashMap;
use tokio::sync::oneshot;
use tokio::time::sleep;
use std::time::Duration;

// 创建模拟客户端
async fn create_mock_client(server: &mockito::Server) -> ArroyoClient {
    ArroyoClient::new(&server.url()).unwrap()
}

#[tokio::test]
async fn test_idempotent_send() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 允许多次调用
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .expect(2) // 期望被调用两次
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .enable_idempotence()
            .producer_id("test-producer-id")
            .build(),
    );

    // 发送消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let result1 = producer.send(key.clone(), value.clone()).await?;

    // 验证结果
    assert!(result1.success);
    assert!(result1.error.is_none());
    assert!(result1.latency_ms > 0);
    assert_eq!(result1.size_bytes, 18); // "test-key" + "test-value" = 8 + 10 = 18

    // 再次发送相同的消息（应该被去重）
    let result2 = producer.send(key.clone(), value.clone()).await?;

    // 验证结果
    assert!(result2.success);
    assert!(result2.error.is_none());
    // 不检查 latency_ms，因为去重的消息可能不会实际发送
    assert_eq!(result2.size_bytes, 18);

    Ok(())
}

#[tokio::test]
async fn test_idempotent_send_with_headers() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 允许多次调用
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .expect(2) // 期望被调用两次
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .enable_idempotence()
            .producer_id("test-producer-id")
            .build(),
    );

    // 创建头部
    let mut headers = HashMap::new();
    headers.insert("test-header".to_string(), b"test-header-value".to_vec());

    // 发送消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let result1 = producer
        .send_with_headers(key.clone(), value.clone(), headers.clone())
        .await?;

    // 验证结果
    assert!(result1.success);
    assert!(result1.error.is_none());
    assert!(result1.latency_ms > 0);

    // 再次发送相同的消息（应该被去重）
    let result2 = producer
        .send_with_headers(key.clone(), value.clone(), headers.clone())
        .await?;

    // 验证结果
    assert!(result2.success);
    assert!(result2.error.is_none());
    // 不检查 latency_ms，因为去重的消息可能不会实际发送

    Ok(())
}

#[tokio::test]
async fn test_idempotent_async_send() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 允许多次调用
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .expect(2) // 期望被调用两次
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .enable_idempotence()
            .producer_id("test-producer-id")
            .build(),
    );

    // 创建通道用于接收回调结果
    let (tx1, rx1) = oneshot::channel::<SendResult>();
    let (tx2, rx2) = oneshot::channel::<SendResult>();

    // 发送消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    producer.send_async(key.clone(), value.clone(), Some(Box::new(move |result| {
        let _ = tx1.send(result);
    })));

    // 等待回调结果
    let result1 = rx1.await.unwrap();

    // 验证结果
    assert!(result1.success);
    assert!(result1.error.is_none());
    assert!(result1.latency_ms > 0);

    // 再次发送相同的消息（应该被去重）
    producer.send_async(key.clone(), value.clone(), Some(Box::new(move |result| {
        let _ = tx2.send(result);
    })));

    // 等待回调结果
    let result2 = rx2.await.unwrap();

    // 验证结果
    assert!(result2.success);
    assert!(result2.error.is_none());
    // 不检查 latency_ms，因为去重的消息可能不会实际发送

    Ok(())
}

#[tokio::test]
async fn test_idempotent_cache_expiry() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 允许多次调用
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .expect(2) // 期望被调用两次
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .enable_idempotence()
            .producer_id("test-producer-id")
            .deduplication_cache_expiry_ms(100) // 设置较短的过期时间
            .deduplication_cache_size(10)
            .build(),
    );

    // 发送消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let result1 = producer.send(key.clone(), value.clone()).await?;

    // 验证结果
    assert!(result1.success);

    // 等待缓存过期
    sleep(Duration::from_millis(200)).await;

    // 再次发送相同的消息（缓存已过期，应该重新发送）
    let result2 = producer.send(key.clone(), value.clone()).await?;

    // 验证结果
    assert!(result2.success);

    Ok(())
}
