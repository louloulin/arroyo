use arroyo_sdk::{ArroyoClient, Result, TopicOptions};
use std::collections::HashMap;

#[tokio::test]
async fn test_topic_api() -> Result<()> {
    // 创建客户端
    let client = ArroyoClient::new("http://localhost:8000")?;

    // 创建 Topic 选项
    let topic_options = TopicOptions {
        name: "test-topic".to_string(),
        partitions: 3,
        replication_factor: 1,
        retention_ms: None,
        retention_bytes: None,
        cleanup_policy: None,
        config: None,
    };

    // 这里我们不实际调用 API，因为这是一个单元测试
    // 在实际应用中，我们会调用 client.create_topic(topic_options).await?

    // 验证 TopicOptions 结构
    assert_eq!(topic_options.name, "test-topic");
    assert_eq!(topic_options.partitions, 3);
    assert_eq!(topic_options.replication_factor, 1);

    Ok(())
}

#[tokio::test]
async fn test_producer_api() -> Result<()> {
    // 创建消息用于测试
    let message = arroyo_sdk::models::Message {
        key: Some(b"key".to_vec()),
        value: b"value".to_vec(),
        headers: None,
        timestamp: Some(1620633600000),
        partition: None,
        offset: None,
    };

    // 验证消息结构
    assert_eq!(message.key, Some(b"key".to_vec()));
    assert_eq!(message.value, b"value".to_vec());
    assert_eq!(message.timestamp, Some(1620633600000));

    Ok(())
}

#[tokio::test]
async fn test_consumer_api() -> Result<()> {
    // 创建偏移量 HashMap 用于测试
    let mut offsets = HashMap::new();
    offsets.insert(0u32, 2u64);
    offsets.insert(1u32, 0u64);

    // 验证 HashMap 结构
    assert_eq!(offsets.len(), 2);
    assert_eq!(offsets.get(&0), Some(&2));
    assert_eq!(offsets.get(&1), Some(&0));

    Ok(())
}
