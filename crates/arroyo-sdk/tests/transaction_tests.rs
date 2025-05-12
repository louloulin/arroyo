use arroyo_sdk::{
    ArroyoClient, Producer, ProducerBuilder, Result, SendResult, TransactionResult, TransactionState,
};
use mockito;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

// 创建模拟客户端
async fn create_mock_client(server: &mockito::Server) -> ArroyoClient {
    ArroyoClient::new(&server.url()).unwrap()
}

#[tokio::test]
async fn test_transaction_lifecycle() -> Result<()> {
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
            .enable_transactions()
            .transaction_id_prefix("test-tx")
            .build(),
    );

    // 初始化事务
    let init_result = producer.init_transaction().await?;
    assert!(init_result.success);
    assert!(!init_result.transaction_id.is_empty());

    // 开始事务
    let begin_result = producer.begin_transaction().await?;
    assert!(begin_result.success);
    assert_eq!(begin_result.transaction_id, init_result.transaction_id);

    // 在事务中发送消息
    let key = Some(b"test-key".to_vec());
    let value = b"test-value".to_vec();
    let send_result = producer.send_in_transaction(key, value).await?;
    assert!(send_result.success);

    // 在事务中发送带头部的消息
    let mut headers = HashMap::new();
    headers.insert("test-header".to_string(), b"test-header-value".to_vec());
    let send_result2 = producer
        .send_with_headers_in_transaction(Some(b"test-key2".to_vec()), b"test-value2".to_vec(), headers)
        .await?;
    assert!(send_result2.success);

    // 提交事务
    let commit_result = producer.commit_transaction().await?;
    assert!(commit_result.success);
    assert_eq!(commit_result.transaction_id, init_result.transaction_id);

    // 创建新事务
    let new_tx_result = producer.new_transaction().await?;
    assert!(new_tx_result.success);
    assert_ne!(new_tx_result.transaction_id, init_result.transaction_id);

    // 初始化新事务
    let init_result2 = producer.init_transaction().await?;
    assert!(init_result2.success);
    assert_eq!(init_result2.transaction_id, new_tx_result.transaction_id);

    // 开始新事务
    let begin_result2 = producer.begin_transaction().await?;
    assert!(begin_result2.success);

    // 中止事务
    let abort_result = producer.abort_transaction().await?;
    assert!(abort_result.success);

    Ok(())
}

#[tokio::test]
async fn test_transaction_commit_with_messages() -> Result<()> {
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
            .enable_transactions()
            .transaction_timeout_ms(5000)
            .build(),
    );

    // 初始化事务
    producer.init_transaction().await?;
    producer.begin_transaction().await?;

    // 在事务中发送多条消息
    for i in 0..5 {
        let key = Some(format!("key-{}", i).into_bytes());
        let value = format!("value-{}", i).into_bytes();
        producer.send_in_transaction(key, value).await?;
    }

    // 提交事务
    let commit_result = producer.commit_transaction().await?;
    assert!(commit_result.success);

    Ok(())
}

#[tokio::test]
async fn test_transaction_error_handling() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 失败响应
    let _m = server
        .mock("POST", "/api/topics/test-topic/messages/batch")
        .with_status(500)
        .with_header("content-type", "application/json")
        .with_body("{\"error\": \"Internal server error\"}")
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .enable_transactions()
            .build(),
    );

    // 初始化事务
    producer.init_transaction().await?;
    producer.begin_transaction().await?;

    // 在事务中发送消息
    for i in 0..3 {
        let key = Some(format!("key-{}", i).into_bytes());
        let value = format!("value-{}", i).into_bytes();
        producer.send_in_transaction(key, value).await?;
    }

    // 提交事务 - 应该失败
    let commit_result = producer.commit_transaction().await?;
    assert!(!commit_result.success);
    assert!(commit_result.error.is_some());

    Ok(())
}

#[tokio::test]
async fn test_transaction_validation() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;
    let client = create_mock_client(&server).await;
    
    // 创建非事务性生产者
    let non_tx_producer = Producer::new(
        client,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .build(),
    );

    // 尝试初始化事务 - 应该失败
    let init_result = non_tx_producer.init_transaction().await;
    assert!(init_result.is_err());

    // 创建事务性生产者
    let tx_producer = Producer::new(
        create_mock_client(&server).await,
        "test-topic",
        ProducerBuilder::new()
            .client_id("test-producer")
            .enable_transactions()
            .build(),
    );

    // 尝试在初始化前开始事务 - 应该失败
    let begin_result = tx_producer.begin_transaction().await;
    assert!(begin_result.is_err());

    // 初始化事务
    tx_producer.init_transaction().await?;

    // 尝试在开始前提交事务 - 应该失败
    let commit_result = tx_producer.commit_transaction().await;
    assert!(commit_result.is_err());

    Ok(())
}
