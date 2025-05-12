use arroyo_sdk::{
    ArroyoClient, Consumer, ConsumerBuilder, Result, flow_control::{FlowControlConfig, FlowControlStrategy, BackpressureStrategy}
};
use mockito;
use std::time::Duration;

// 创建模拟客户端
async fn create_mock_client(server: &mockito::Server) -> ArroyoClient {
    ArroyoClient::new(&server.url()).unwrap()
}

#[tokio::test]
async fn test_flow_control_config() -> Result<()> {
    // 测试默认配置
    let config = FlowControlConfig::default();
    assert_eq!(config.strategy, FlowControlStrategy::None);
    assert_eq!(config.max_messages_per_second, 1000);
    assert_eq!(config.backpressure_strategy, BackpressureStrategy::Block);
    assert_eq!(config.buffer_size, 10000);
    assert_eq!(config.backpressure_threshold, 0.8);

    // 测试自定义配置
    let custom_config = FlowControlConfig {
        strategy: FlowControlStrategy::FixedRate,
        max_messages_per_second: 500,
        backpressure_strategy: BackpressureStrategy::Drop,
        buffer_size: 5000,
        backpressure_threshold: 0.7,
    };
    assert_eq!(custom_config.strategy, FlowControlStrategy::FixedRate);
    assert_eq!(custom_config.max_messages_per_second, 500);
    assert_eq!(custom_config.backpressure_strategy, BackpressureStrategy::Drop);
    assert_eq!(custom_config.buffer_size, 5000);
    assert_eq!(custom_config.backpressure_threshold, 0.7);

    Ok(())
}

#[tokio::test]
async fn test_consumer_with_flow_control() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 获取消息
    let _m1 = server
        .mock("GET", "/api/topics/test-topic/messages")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[
            {
                "key": [107, 101, 121, 49],
                "value": [118, 97, 108, 117, 101, 49],
                "timestamp": 1625097600000,
                "partition": 0,
                "offset": 1
            }
        ]"#)
        .expect(1)
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    
    // 创建带有固定速率限流的消费者
    let consumer = Consumer::new(
        client,
        "test-topic",
        ConsumerBuilder::new("test-group")
            .enable_rate_limiting(100)
            .build(),
    );

    // 拉取消息
    let messages = consumer.poll(Duration::from_secs(1)).await?;

    // 验证结果
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].key, Some(b"key1".to_vec()));
    assert_eq!(messages[0].value, b"value1".to_vec());
    assert_eq!(messages[0].partition, Some(0));
    assert_eq!(messages[0].offset, Some(1));

    Ok(())
}

#[tokio::test]
async fn test_consumer_with_adaptive_flow_control() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 获取消息
    let _m1 = server
        .mock("GET", "/api/topics/test-topic/messages")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[
            {
                "key": [107, 101, 121, 49],
                "value": [118, 97, 108, 117, 101, 49],
                "timestamp": 1625097600000,
                "partition": 0,
                "offset": 1
            },
            {
                "key": [107, 101, 121, 50],
                "value": [118, 97, 108, 117, 101, 50],
                "timestamp": 1625097600001,
                "partition": 0,
                "offset": 2
            }
        ]"#)
        .expect(1)
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    
    // 创建带有自适应速率限流的消费者
    let consumer = Consumer::new(
        client,
        "test-topic",
        ConsumerBuilder::new("test-group")
            .enable_adaptive_rate_limiting(200)
            .build(),
    );

    // 拉取消息
    let messages = consumer.poll(Duration::from_secs(1)).await?;

    // 验证结果
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].key, Some(b"key1".to_vec()));
    assert_eq!(messages[0].value, b"value1".to_vec());
    assert_eq!(messages[1].key, Some(b"key2".to_vec()));
    assert_eq!(messages[1].value, b"value2".to_vec());

    Ok(())
}

#[tokio::test]
async fn test_consumer_with_backpressure() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 获取大量消息
    let _m1 = server
        .mock("GET", "/api/topics/test-topic/messages")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[
            {
                "key": [107, 101, 121, 49],
                "value": [118, 97, 108, 117, 101, 49],
                "timestamp": 1625097600000,
                "partition": 0,
                "offset": 1
            }
        ]"#)
        .expect(1)
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    
    // 创建带有背压策略的消费者
    let consumer = Consumer::new(
        client,
        "test-topic",
        ConsumerBuilder::new("test-group")
            .flow_control(FlowControlConfig {
                strategy: FlowControlStrategy::FixedRate,
                max_messages_per_second: 100,
                backpressure_strategy: BackpressureStrategy::Block,
                buffer_size: 10,
                backpressure_threshold: 0.5,
            })
            .build(),
    );

    // 拉取消息
    let messages = consumer.poll(Duration::from_secs(1)).await?;

    // 验证结果
    assert_eq!(messages.len(), 1);

    Ok(())
}
