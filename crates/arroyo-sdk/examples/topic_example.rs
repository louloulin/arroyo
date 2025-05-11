use arroyo_sdk::{ArroyoClient, Result, TopicBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    // 创建客户端
    let client = ArroyoClient::new("http://localhost:8000")?;
    
    // 使用构建器创建 Topic
    let topic_options = client
        .topic_builder("example-topic")
        .partitions(3)
        .replication_factor(1)
        .retention_ms(86400000) // 1 天
        .cleanup_policy("delete")
        .config("segment.bytes", "1073741824") // 1 GB
        .build();
    
    // 创建 Topic
    let topic = client.create_topic(topic_options).await?;
    println!("创建了新 Topic: {:?}", topic);
    
    // 获取所有 Topics
    let topics = client.get_topics().await?;
    println!("所有 Topics: {:?}", topics);
    
    // 获取指定 Topic
    let topic = client.get_topic("example-topic").await?;
    println!("Topic 详情: {:?}", topic);
    
    // 更新 Topic 配置
    let mut config = std::collections::HashMap::new();
    config.insert("retention.ms".to_string(), "172800000".to_string()); // 2 天
    let updated_topic = client.update_topic_config("example-topic", config).await?;
    println!("更新后的 Topic: {:?}", updated_topic);
    
    // 删除 Topic
    client.delete_topic("example-topic").await?;
    println!("已删除 Topic");
    
    Ok(())
}
