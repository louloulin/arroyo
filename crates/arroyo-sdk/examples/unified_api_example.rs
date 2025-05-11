use arroyo_sdk::{
    ArroyoClient, Consumer, ConsumerBuilder, Producer, ProducerBuilder, Result, TopicBuilder,
};
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // 创建客户端
    let client = ArroyoClient::new("http://localhost:8000")?;
    
    // 创建 Topic
    let topic_name = "unified-api-example";
    let topic_options = client
        .topic_builder(topic_name)
        .partitions(3)
        .replication_factor(1)
        .build();
    
    let topic = client.create_topic(topic_options).await?;
    println!("创建了新 Topic: {:?}", topic);
    
    // 创建生产者
    let producer_options = ProducerBuilder::new()
        .client_id("example-producer")
        .batch_size(16384)
        .compression_type("lz4")
        .build();
    
    let producer = Producer::new(client.clone(), topic_name, producer_options);
    
    // 发送消息
    for i in 0..10 {
        let key = format!("key-{}", i).into_bytes();
        let value = format!("value-{}", i).into_bytes();
        
        // 添加消息头部
        let mut headers = HashMap::new();
        headers.insert("index".to_string(), i.to_string().into_bytes());
        headers.insert("timestamp".to_string(), chrono::Utc::now().timestamp().to_string().into_bytes());
        
        producer.send_with_headers(Some(key), value, headers).await?;
        println!("发送消息 {}", i);
    }
    
    // 创建消费者
    let consumer_options = ConsumerBuilder::new("example-consumer-group")
        .client_id("example-consumer")
        .auto_offset_reset("earliest")
        .build();
    
    let consumer = Consumer::new(client.clone(), topic_name, consumer_options);
    
    // 消费消息
    println!("开始消费消息...");
    let messages = consumer.poll(Duration::from_secs(5)).await?;
    
    for message in &messages {
        let key = message.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string());
        let value = String::from_utf8_lossy(&message.value).to_string();
        
        println!("收到消息: key={:?}, value={}", key, value);
        
        if let Some(headers) = &message.headers {
            println!("  消息头部:");
            for (name, value) in headers {
                println!("    {}: {}", name, String::from_utf8_lossy(value));
            }
        }
    }
    
    // 提交偏移量
    if !messages.is_empty() {
        let mut offsets = HashMap::new();
        for message in &messages {
            if let (Some(partition), Some(offset)) = (message.partition, message.offset) {
                offsets.insert(partition, offset + 1);
            }
        }
        
        if !offsets.is_empty() {
            consumer.commit_batch(offsets).await?;
            println!("提交了偏移量");
        }
    }
    
    // 创建流处理作业
    println!("创建流处理作业...");
    let sql = format!(
        "CREATE TABLE result AS
         SELECT 
           key,
           COUNT(*) AS message_count,
           MAX(CAST(value AS VARCHAR)) AS max_value
         FROM {}
         GROUP BY key",
        topic_name
    );
    
    let job_id = client.create_sql_job("unified-api-example-job", &sql, None).await?;
    println!("创建了流处理作业，ID: {}", job_id);
    
    // 等待作业运行一段时间
    println!("等待作业运行...");
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // 获取作业状态
    let job = client.get_job(&job_id).await?;
    println!("作业状态: {:?}", job);
    
    // 停止作业
    client.stop_job(&job_id).await?;
    println!("停止了作业");
    
    // 删除 Topic
    client.delete_topic(topic_name).await?;
    println!("删除了 Topic");
    
    Ok(())
}
