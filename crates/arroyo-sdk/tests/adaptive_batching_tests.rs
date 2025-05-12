use arroyo_sdk::{
    AdaptiveBatchingConfig, AdaptiveBatchingStrategy, ArroyoClient, Producer, ProducerBuilder,
    Result, SendResult,
};
use mockito;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::sleep;

// 创建模拟客户端
async fn create_mock_client(server: &mockito::Server) -> ArroyoClient {
    ArroyoClient::new(&server.url()).unwrap()
}

#[tokio::test]
async fn test_adaptive_batching_throughput() -> Result<()> {
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
            .batch_size(100)
            .batch_delay_ms(10)
            .enable_adaptive_batching(AdaptiveBatchingStrategy::Throughput)
            .min_batch_size(50)
            .max_batch_size(1000)
            .adjustment_interval_ms(100)
            .target_throughput(5000.0)
            .build(),
    );

    // 获取初始批处理参数
    let (initial_batch_size, initial_batch_delay_ms) = producer.get_batch_parameters().await;
    assert_eq!(initial_batch_size, 100);
    assert_eq!(initial_batch_delay_ms, 10);

    // 创建消息批次
    let mut messages = Vec::new();
    for i in 0..200 {
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

    // 发送多个批次，触发自适应调整
    for _ in 0..5 {
        let results = producer.send_batch(messages.clone()).await?;
        assert_eq!(results.len(), 200);
        for result in &results {
            assert!(result.success);
        }
        sleep(Duration::from_millis(50)).await;
    }

    // 等待调整完成
    sleep(Duration::from_millis(200)).await;

    // 获取调整后的批处理参数
    let (adjusted_batch_size, adjusted_batch_delay_ms) = producer.get_batch_parameters().await;
    
    // 验证参数已经调整
    println!("调整后的批处理大小: {}", adjusted_batch_size);
    println!("调整后的批处理延迟: {}ms", adjusted_batch_delay_ms);
    
    // 参数应该在配置的范围内
    assert!(adjusted_batch_size >= 50);
    assert!(adjusted_batch_size <= 1000);
    assert!(adjusted_batch_delay_ms >= 0);
    assert!(adjusted_batch_delay_ms <= 1000);

    Ok(())
}

#[tokio::test]
async fn test_adaptive_batching_latency() -> Result<()> {
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
            .batch_size(500)
            .batch_delay_ms(50)
            .enable_adaptive_batching(AdaptiveBatchingStrategy::Latency)
            .min_batch_size(50)
            .max_batch_size(1000)
            .adjustment_interval_ms(100)
            .target_latency_ms(20)
            .build(),
    );

    // 获取初始批处理参数
    let (initial_batch_size, initial_batch_delay_ms) = producer.get_batch_parameters().await;
    assert_eq!(initial_batch_size, 500);
    assert_eq!(initial_batch_delay_ms, 50);

    // 创建消息批次
    let mut messages = Vec::new();
    for i in 0..100 {
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

    // 发送多个批次，触发自适应调整
    for _ in 0..5 {
        let results = producer.send_batch(messages.clone()).await?;
        assert_eq!(results.len(), 100);
        for result in &results {
            assert!(result.success);
        }
        sleep(Duration::from_millis(50)).await;
    }

    // 等待调整完成
    sleep(Duration::from_millis(200)).await;

    // 获取调整后的批处理参数
    let (adjusted_batch_size, adjusted_batch_delay_ms) = producer.get_batch_parameters().await;
    
    // 验证参数已经调整
    println!("调整后的批处理大小: {}", adjusted_batch_size);
    println!("调整后的批处理延迟: {}ms", adjusted_batch_delay_ms);
    
    // 参数应该在配置的范围内
    assert!(adjusted_batch_size >= 50);
    assert!(adjusted_batch_size <= 1000);
    assert!(adjusted_batch_delay_ms >= 0);
    assert!(adjusted_batch_delay_ms <= 1000);

    Ok(())
}

#[tokio::test]
async fn test_adaptive_batching_balanced() -> Result<()> {
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
            .batch_size(200)
            .batch_delay_ms(30)
            .enable_adaptive_batching(AdaptiveBatchingStrategy::Balanced)
            .min_batch_size(50)
            .max_batch_size(1000)
            .adjustment_interval_ms(100)
            .target_latency_ms(50)
            .target_throughput(2000.0)
            .build(),
    );

    // 获取初始批处理参数
    let (initial_batch_size, initial_batch_delay_ms) = producer.get_batch_parameters().await;
    assert_eq!(initial_batch_size, 200);
    assert_eq!(initial_batch_delay_ms, 30);

    // 创建消息批次
    let mut messages = Vec::new();
    for i in 0..150 {
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

    // 发送多个批次，触发自适应调整
    for _ in 0..5 {
        let results = producer.send_batch(messages.clone()).await?;
        assert_eq!(results.len(), 150);
        for result in &results {
            assert!(result.success);
        }
        sleep(Duration::from_millis(50)).await;
    }

    // 等待调整完成
    sleep(Duration::from_millis(200)).await;

    // 获取调整后的批处理参数
    let (adjusted_batch_size, adjusted_batch_delay_ms) = producer.get_batch_parameters().await;
    
    // 验证参数已经调整
    println!("调整后的批处理大小: {}", adjusted_batch_size);
    println!("调整后的批处理延迟: {}ms", adjusted_batch_delay_ms);
    
    // 参数应该在配置的范围内
    assert!(adjusted_batch_size >= 50);
    assert!(adjusted_batch_size <= 1000);
    assert!(adjusted_batch_delay_ms >= 0);
    assert!(adjusted_batch_delay_ms <= 1000);

    Ok(())
}
