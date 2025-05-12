use arroyo_sdk::{
    ArroyoClient, CompressionType, Producer, ProducerBuilder, Result, SendResult,
};
use mockito;
use std::collections::HashMap;

// 创建模拟客户端
async fn create_mock_client(server: &mockito::Server) -> ArroyoClient {
    ArroyoClient::new(&server.url()).unwrap()
}

#[tokio::test]
async fn test_compression_none() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages/batch")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .compression_type_enum(CompressionType::None)
            .build(),
    );

    // 创建消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let message = arroyo_sdk::models::Message {
        key: key.clone(),
        value: value.clone(),
        headers: None,
        timestamp: None,
        partition: None,
        offset: None,
    };

    // 发送消息
    let results = producer.send_batch(vec![message]).await?;
    assert_eq!(results.len(), 1);
    assert!(results[0].success);

    Ok(())
}

#[tokio::test]
async fn test_compression_gzip() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages/batch")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .compression_type_enum(CompressionType::Gzip)
            .build(),
    );

    // 创建消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let message = arroyo_sdk::models::Message {
        key: key.clone(),
        value: value.clone(),
        headers: None,
        timestamp: None,
        partition: None,
        offset: None,
    };

    // 发送消息
    let results = producer.send_batch(vec![message]).await?;
    assert_eq!(results.len(), 1);
    assert!(results[0].success);

    Ok(())
}

#[tokio::test]
async fn test_compression_lz4() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages/batch")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .compression_type_enum(CompressionType::Lz4)
            .build(),
    );

    // 创建消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let message = arroyo_sdk::models::Message {
        key: key.clone(),
        value: value.clone(),
        headers: None,
        timestamp: None,
        partition: None,
        offset: None,
    };

    // 发送消息
    let results = producer.send_batch(vec![message]).await?;
    assert_eq!(results.len(), 1);
    assert!(results[0].success);

    Ok(())
}

#[tokio::test]
async fn test_compression_snappy() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages/batch")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .compression_type_enum(CompressionType::Snappy)
            .build(),
    );

    // 创建消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let message = arroyo_sdk::models::Message {
        key: key.clone(),
        value: value.clone(),
        headers: None,
        timestamp: None,
        partition: None,
        offset: None,
    };

    // 发送消息
    let results = producer.send_batch(vec![message]).await?;
    assert_eq!(results.len(), 1);
    assert!(results[0].success);

    Ok(())
}

#[tokio::test]
async fn test_compression_zstd() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages/batch")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .compression_type_enum(CompressionType::Zstd)
            .build(),
    );

    // 创建消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let message = arroyo_sdk::models::Message {
        key: key.clone(),
        value: value.clone(),
        headers: None,
        timestamp: None,
        partition: None,
        offset: None,
    };

    // 发送消息
    let results = producer.send_batch(vec![message]).await?;
    assert_eq!(results.len(), 1);
    assert!(results[0].success);

    Ok(())
}

#[tokio::test]
async fn test_compression_string_api() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages/batch")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body("{}")
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .compression_type("zstd")
            .build(),
    );

    // 创建消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let message = arroyo_sdk::models::Message {
        key: key.clone(),
        value: value.clone(),
        headers: None,
        timestamp: None,
        partition: None,
        offset: None,
    };

    // 发送消息
    let results = producer.send_batch(vec![message]).await?;
    assert_eq!(results.len(), 1);
    assert!(results[0].success);

    Ok(())
}
