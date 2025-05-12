use arroyo_sdk::{
    ArroyoClient, Consumer, ConsumerBuilder, ConsumerGroupManager, PartitionAssignmentStrategy,
    Result, SubscriptionType,
};
use mockito;
use std::time::Duration;

// 创建模拟客户端
async fn create_mock_client(server: &mockito::Server) -> ArroyoClient {
    ArroyoClient::new(&server.url()).unwrap()
}

#[tokio::test]
async fn test_consumer_group_creation() -> Result<()> {
    // 创建一个消费者组管理器
    let manager = ConsumerGroupManager::new(
        ArroyoClient::new("http://localhost:8000").unwrap(),
        "test-group",
        "test-client",
        30000,
        3000,
        PartitionAssignmentStrategy::RoundRobin,
    );

    // 设置订阅的 Topic
    let mut manager_clone = manager.clone();
    manager_clone.subscribe(vec!["test-topic"]);

    // 验证消费者组管理器已创建
    assert!(manager_clone.is_joined().await == false);

    Ok(())
}

#[tokio::test]
async fn test_consumer_with_subscription_types() -> Result<()> {
    // 测试单个 Topic 订阅
    let consumer = Consumer::new(
        ArroyoClient::new("http://localhost:8000").unwrap(),
        "test-topic",
        ConsumerBuilder::new("test-group")
            .subscribe("test-topic")
            .enable_consumer_group(true)
            .partition_assignment_strategy(PartitionAssignmentStrategy::RoundRobin)
            .build(),
    );

    // 测试多个 Topic 订阅
    let consumer_multi = Consumer::new_with_subscription(
        ArroyoClient::new("http://localhost:8000").unwrap(),
        SubscriptionType::multiple(vec!["topic1", "topic2"]),
        ConsumerBuilder::new("test-group")
            .enable_consumer_group(true)
            .partition_assignment_strategy(PartitionAssignmentStrategy::Range)
            .build(),
    );

    // 测试正则表达式订阅
    let consumer_pattern = Consumer::new_with_subscription(
        ArroyoClient::new("http://localhost:8000").unwrap(),
        SubscriptionType::pattern("test-.*"),
        ConsumerBuilder::new("test-group")
            .enable_consumer_group(true)
            .partition_assignment_strategy(PartitionAssignmentStrategy::ConsistentHash)
            .build(),
    );

    // 验证消费者已创建
    assert!(consumer.close().await.is_ok());
    assert!(consumer_multi.close().await.is_ok());
    assert!(consumer_pattern.close().await.is_ok());

    Ok(())
}
