#[cfg(test)]
mod tests {
    use crate::topic::TopicHealthChecker;
    use arroyo_rpc::api_types::topics::{TopicConfig, TopicHealthStatus};
    use std::env;
    use std::time::Duration;
    use tokio::time::sleep;

    // 获取测试服务器地址
    fn get_test_server() -> String {
        env::var("KAFKA_TEST_SERVER").unwrap_or_else(|_| "localhost:9092".to_string())
    }

    // 检查是否应该跳过需要 Kafka 服务器的测试
    fn should_skip_kafka_tests() -> bool {
        env::var("SKIP_KAFKA_TESTS").unwrap_or_else(|_| "true".to_string()) == "true"
    }

    // 生成唯一的 Topic 名称
    fn generate_topic_name(prefix: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        format!("{}-{}", prefix, timestamp)
    }

    #[tokio::test]
    async fn test_health_checker_all_topics() {
        if should_skip_kafka_tests() {
            println!("Skipping test_health_checker_all_topics because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicHealthChecker
        let server = get_test_server();
        let mut health_checker = TopicHealthChecker::new(&server, Some(10))
            .expect("Failed to create TopicHealthChecker");

        // 检查所有 Topic 的健康状态
        let health_results = health_checker
            .check_all_topics_health(true)
            .await
            .expect("Failed to check all topics health");

        // 验证结果
        println!("Found {} topics", health_results.len());
        for health in &health_results {
            println!(
                "Topic: {}, Status: {:?}, Partitions: {}",
                health.topic_name, health.status, health.partition_count
            );
        }
    }

    #[tokio::test]
    async fn test_health_checker_specific_topic() {
        if should_skip_kafka_tests() {
            println!("Skipping test_health_checker_specific_topic because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicAdmin 和 TopicHealthChecker
        let server = get_test_server();
        let admin = crate::topic::TopicAdmin::new(&server).expect("Failed to create TopicAdmin");
        let mut health_checker = TopicHealthChecker::new(&server, Some(10))
            .expect("Failed to create TopicHealthChecker");

        // 生成唯一的 Topic 名称
        let topic_name = generate_topic_name("test-health");

        // 创建 Topic
        let config = TopicConfig {
            name: topic_name.clone(),
            partitions: 3,
            replication_factor: 1,
            retention_ms: Some(86400000), // 1 day
            retention_bytes: Some(1073741824), // 1 GB
            cleanup_policy: "delete".to_string(),
            max_message_bytes: Some(1048576), // 1 MB
            description: Some("Test topic for health check".to_string()),
        };

        let _topic_info = admin
            .create_topic(&config)
            .await
            .expect("Failed to create topic");

        // 等待 Topic 创建完成
        sleep(Duration::from_secs(1)).await;

        // 检查特定 Topic 的健康状态
        let health_results = health_checker
            .check_topics_health(&[topic_name.clone()], true)
            .await
            .expect("Failed to check topic health");

        // 验证结果
        assert_eq!(health_results.len(), 1);
        let health = &health_results[0];
        assert_eq!(health.topic_name, topic_name);
        assert_eq!(health.partition_count, 3);
        assert_eq!(health.status, TopicHealthStatus::Healthy);
        assert_eq!(health.partitions.len(), 3);

        // 删除 Topic
        admin
            .delete_topic(&topic_name)
            .await
            .expect("Failed to delete topic");
    }

    #[tokio::test]
    async fn test_health_checker_cache() {
        if should_skip_kafka_tests() {
            println!("Skipping test_health_checker_cache because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicHealthChecker，设置缓存时间为 2 秒
        let server = get_test_server();
        let mut health_checker = TopicHealthChecker::new(&server, Some(2))
            .expect("Failed to create TopicHealthChecker");

        // 第一次检查，强制刷新
        let start = std::time::Instant::now();
        let _health_results1 = health_checker
            .check_all_topics_health(true)
            .await
            .expect("Failed to check all topics health");
        let duration1 = start.elapsed();

        // 第二次检查，使用缓存
        let start = std::time::Instant::now();
        let _health_results2 = health_checker
            .check_all_topics_health(false)
            .await
            .expect("Failed to check all topics health");
        let duration2 = start.elapsed();

        // 验证第二次检查使用了缓存（速度更快）
        println!("First check took {:?}", duration1);
        println!("Second check took {:?}", duration2);
        assert!(duration2 < duration1);

        // 等待缓存过期
        sleep(Duration::from_secs(3)).await;

        // 第三次检查，缓存已过期
        let start = std::time::Instant::now();
        let _health_results3 = health_checker
            .check_all_topics_health(false)
            .await
            .expect("Failed to check all topics health");
        let duration3 = start.elapsed();

        // 验证第三次检查未使用缓存（速度较慢）
        println!("Third check took {:?}", duration3);
        assert!(duration3 > duration2);
    }

    #[tokio::test]
    async fn test_health_checker_nonexistent_topic() {
        if should_skip_kafka_tests() {
            println!("Skipping test_health_checker_nonexistent_topic because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicHealthChecker
        let server = get_test_server();
        let mut health_checker = TopicHealthChecker::new(&server, Some(10))
            .expect("Failed to create TopicHealthChecker");

        // 检查不存在的 Topic
        let nonexistent_topic = "nonexistent-topic-".to_string() + &rand::random::<u64>().to_string();
        let health_results = health_checker
            .check_topics_health(&[nonexistent_topic.clone()], true)
            .await
            .expect("Failed to check topic health");

        // 验证结果
        assert_eq!(health_results.len(), 1);
        let health = &health_results[0];
        assert_eq!(health.topic_name, nonexistent_topic);
        assert_eq!(health.status, TopicHealthStatus::Unhealthy);
        assert!(!health.issues.is_empty());
    }
}
