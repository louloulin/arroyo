use super::{TopicAdmin, TopicQuotaManager};
use arroyo_rpc::api_types::quotas::TopicLimitConfig;
use arroyo_rpc::api_types::topics::TopicConfig;
use std::env;
use std::time::Duration;
use tokio::time::sleep;

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn test_topic_admin_with_quota_manager() {
        if should_skip_kafka_tests() {
            println!("Skipping test_topic_admin_with_quota_manager because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicQuotaManager
        let server = get_test_server();
        let quota_manager = TopicQuotaManager::new(&server, Some(10))
            .expect("Failed to create TopicQuotaManager");

        // 创建 Topic 限制
        let limit_config = TopicLimitConfig {
            topic_name: "*".to_string(), // 全局限制
            max_partitions: Some(5),
            max_replication_factor: Some(2),
            max_retention_ms: Some(86400000), // 1 day
            max_retention_bytes: Some(1073741824), // 1 GB
            max_message_bytes: Some(1048576), // 1 MB
            max_producer_bytes_per_sec: Some(1048576), // 1 MB/s
            max_consumer_bytes_per_sec: Some(1048576), // 1 MB/s
            max_request_rate: Some(100),
            description: Some("Global topic limit".to_string()),
        };

        quota_manager
            .create_topic_limit(&limit_config)
            .await
            .expect("Failed to create topic limit");

        // 创建 TopicAdmin 并设置配额管理器
        let admin = TopicAdmin::new(&server)
            .expect("Failed to create TopicAdmin")
            .with_quota_manager(quota_manager);

        // 测试符合限制的 Topic 创建
        let valid_topic_name = generate_topic_name("test-valid");
        let valid_config = TopicConfig {
            name: valid_topic_name.clone(),
            partitions: 3,
            replication_factor: 1,
            retention_ms: Some(43200000), // 12 hours
            retention_bytes: Some(536870912), // 512 MB
            cleanup_policy: "delete".to_string(),
            max_message_bytes: Some(524288), // 512 KB
            description: Some("Valid test topic".to_string()),
        };

        let result = admin.create_topic(&valid_config).await;
        assert!(result.is_ok(), "Failed to create valid topic: {:?}", result);

        // 等待 Topic 创建完成
        sleep(Duration::from_secs(1)).await;

        // 测试超出限制的 Topic 创建
        let invalid_topic_name = generate_topic_name("test-invalid");
        let invalid_config = TopicConfig {
            name: invalid_topic_name.clone(),
            partitions: 10, // 超出限制
            replication_factor: 1,
            retention_ms: Some(43200000),
            retention_bytes: Some(536870912),
            cleanup_policy: "delete".to_string(),
            max_message_bytes: Some(524288),
            description: Some("Invalid test topic".to_string()),
        };

        let result = admin.create_topic(&invalid_config).await;
        assert!(result.is_err(), "Created topic with invalid config");
        assert!(
            result.unwrap_err().to_string().contains("exceeds limit"),
            "Error message should mention limit exceeded"
        );

        // 测试超出限制的 Topic 更新
        let update_config = TopicConfig {
            name: valid_topic_name.clone(),
            partitions: 3, // 不能更改
            replication_factor: 1, // 不能更改
            retention_ms: Some(172800000), // 2 days，超出限制
            retention_bytes: Some(536870912),
            cleanup_policy: "delete".to_string(),
            max_message_bytes: Some(524288),
            description: Some("Updated test topic".to_string()),
        };

        let result = admin.update_topic(&update_config).await;
        assert!(result.is_err(), "Updated topic with invalid config");
        assert!(
            result.unwrap_err().to_string().contains("exceeds limit"),
            "Error message should mention limit exceeded"
        );

        // 清理
        admin
            .delete_topic(&valid_topic_name)
            .await
            .expect("Failed to delete topic");
    }
}
