use super::quota::TopicQuotaManager;
use arroyo_rpc::api_types::quotas::{
    QuotaConfig, QuotaScope, QuotaType, QuotaUnit, TopicLimitConfig,
};
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

    #[tokio::test]
    async fn test_quota_manager_create_delete_quota() {
        if should_skip_kafka_tests() {
            println!("Skipping test_quota_manager_create_delete_quota because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicQuotaManager
        let server = get_test_server();
        let quota_manager = TopicQuotaManager::new(&server, Some(10))
            .expect("Failed to create TopicQuotaManager");

        // 创建配额
        let quota_config = QuotaConfig {
            quota_type: QuotaType::Producer,
            unit: QuotaUnit::BytesPerSecond,
            scope: QuotaScope::Topic,
            value: 1048576.0, // 1 MB/s
            name: Some("test-topic".to_string()),
            description: Some("Test producer quota".to_string()),
        };

        let created_quota = quota_manager
            .create_quota(&quota_config)
            .await
            .expect("Failed to create quota");

        // 验证配额信息
        assert_eq!(created_quota.quota_type, QuotaType::Producer);
        assert_eq!(created_quota.unit, QuotaUnit::BytesPerSecond);
        assert_eq!(created_quota.scope, QuotaScope::Topic);
        assert_eq!(created_quota.value, 1048576.0);
        assert_eq!(created_quota.name, Some("test-topic".to_string()));

        // 获取所有配额
        let quotas = quota_manager
            .get_quotas()
            .await
            .expect("Failed to get quotas");
        assert_eq!(quotas.len(), 1);

        // 删除配额
        quota_manager
            .delete_quota(&QuotaType::Producer, &QuotaScope::Topic, &Some("test-topic".to_string()))
            .await
            .expect("Failed to delete quota");

        // 验证配额已删除
        let quotas = quota_manager
            .get_quotas()
            .await
            .expect("Failed to get quotas");
        assert_eq!(quotas.len(), 0);
    }

    #[tokio::test]
    async fn test_quota_manager_update_quota() {
        if should_skip_kafka_tests() {
            println!("Skipping test_quota_manager_update_quota because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicQuotaManager
        let server = get_test_server();
        let quota_manager = TopicQuotaManager::new(&server, Some(10))
            .expect("Failed to create TopicQuotaManager");

        // 创建配额
        let quota_config = QuotaConfig {
            quota_type: QuotaType::Consumer,
            unit: QuotaUnit::BytesPerSecond,
            scope: QuotaScope::Topic,
            value: 1048576.0, // 1 MB/s
            name: Some("test-topic".to_string()),
            description: Some("Test consumer quota".to_string()),
        };

        quota_manager
            .create_quota(&quota_config)
            .await
            .expect("Failed to create quota");

        // 更新配额
        let updated_config = QuotaConfig {
            quota_type: QuotaType::Consumer,
            unit: QuotaUnit::BytesPerSecond,
            scope: QuotaScope::Topic,
            value: 2097152.0, // 2 MB/s
            name: Some("test-topic".to_string()),
            description: Some("Updated test consumer quota".to_string()),
        };

        let updated_quota = quota_manager
            .update_quota(&updated_config)
            .await
            .expect("Failed to update quota");

        // 验证更新后的配额信息
        assert_eq!(updated_quota.value, 2097152.0);
        assert_eq!(
            updated_quota.description,
            Some("Updated test consumer quota".to_string())
        );

        // 清理
        quota_manager
            .delete_quota(&QuotaType::Consumer, &QuotaScope::Topic, &Some("test-topic".to_string()))
            .await
            .expect("Failed to delete quota");
    }

    #[tokio::test]
    async fn test_topic_limit_create_delete() {
        if should_skip_kafka_tests() {
            println!("Skipping test_topic_limit_create_delete because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicQuotaManager
        let server = get_test_server();
        let quota_manager = TopicQuotaManager::new(&server, Some(10))
            .expect("Failed to create TopicQuotaManager");

        // 创建 Topic 限制
        let limit_config = TopicLimitConfig {
            topic_name: "test-topic".to_string(),
            max_partitions: Some(10),
            max_replication_factor: Some(3),
            max_retention_ms: Some(86400000), // 1 day
            max_retention_bytes: Some(1073741824), // 1 GB
            max_message_bytes: Some(1048576), // 1 MB
            max_producer_bytes_per_sec: Some(1048576), // 1 MB/s
            max_consumer_bytes_per_sec: Some(1048576), // 1 MB/s
            max_request_rate: Some(100),
            description: Some("Test topic limit".to_string()),
        };

        let created_limit = quota_manager
            .create_topic_limit(&limit_config)
            .await
            .expect("Failed to create topic limit");

        // 验证限制信息
        assert_eq!(created_limit.topic_name, "test-topic");
        assert_eq!(created_limit.max_partitions, Some(10));
        assert_eq!(created_limit.max_replication_factor, Some(3));

        // 获取所有限制
        let limits = quota_manager
            .get_topic_limits()
            .await
            .expect("Failed to get topic limits");
        assert_eq!(limits.len(), 1);

        // 删除限制
        quota_manager
            .delete_topic_limit("test-topic")
            .await
            .expect("Failed to delete topic limit");

        // 验证限制已删除
        let limits = quota_manager
            .get_topic_limits()
            .await
            .expect("Failed to get topic limits");
        assert_eq!(limits.len(), 0);
    }

    #[tokio::test]
    async fn test_validate_topic_config() {
        if should_skip_kafka_tests() {
            println!("Skipping test_validate_topic_config because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicQuotaManager
        let server = get_test_server();
        let quota_manager = TopicQuotaManager::new(&server, Some(10))
            .expect("Failed to create TopicQuotaManager");

        // 创建 Topic 限制
        let limit_config = TopicLimitConfig {
            topic_name: "test-topic".to_string(),
            max_partitions: Some(5),
            max_replication_factor: Some(2),
            max_retention_ms: Some(86400000), // 1 day
            max_retention_bytes: Some(1073741824), // 1 GB
            max_message_bytes: Some(1048576), // 1 MB
            max_producer_bytes_per_sec: Some(1048576), // 1 MB/s
            max_consumer_bytes_per_sec: Some(1048576), // 1 MB/s
            max_request_rate: Some(100),
            description: Some("Test topic limit".to_string()),
        };

        quota_manager
            .create_topic_limit(&limit_config)
            .await
            .expect("Failed to create topic limit");

        // 验证符合限制的配置
        let valid_config = TopicConfig {
            name: "test-topic".to_string(),
            partitions: 3,
            replication_factor: 2,
            retention_ms: Some(43200000), // 12 hours
            retention_bytes: Some(536870912), // 512 MB
            cleanup_policy: "delete".to_string(),
            max_message_bytes: Some(524288), // 512 KB
            description: Some("Valid test topic".to_string()),
        };

        let result = quota_manager.validate_topic_config(&valid_config).await;
        assert!(result.is_ok());

        // 验证超出限制的配置
        let invalid_config = TopicConfig {
            name: "test-topic".to_string(),
            partitions: 10, // 超出限制
            replication_factor: 3, // 超出限制
            retention_ms: Some(172800000), // 2 days，超出限制
            retention_bytes: Some(2147483648), // 2 GB，超出限制
            cleanup_policy: "delete".to_string(),
            max_message_bytes: Some(2097152), // 2 MB，超出限制
            description: Some("Invalid test topic".to_string()),
        };

        let result = quota_manager.validate_topic_config(&invalid_config).await;
        assert!(result.is_err());

        // 清理
        quota_manager
            .delete_topic_limit("test-topic")
            .await
            .expect("Failed to delete topic limit");
    }

    #[tokio::test]
    async fn test_quota_usage() {
        if should_skip_kafka_tests() {
            println!("Skipping test_quota_usage because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicQuotaManager
        let server = get_test_server();
        let quota_manager = TopicQuotaManager::new(&server, Some(1))
            .expect("Failed to create TopicQuotaManager");

        // 创建配额
        let quota_config = QuotaConfig {
            quota_type: QuotaType::Producer,
            unit: QuotaUnit::BytesPerSecond,
            scope: QuotaScope::Topic,
            value: 1048576.0, // 1 MB/s
            name: Some("test-topic".to_string()),
            description: Some("Test producer quota".to_string()),
        };

        quota_manager
            .create_quota(&quota_config)
            .await
            .expect("Failed to create quota");

        // 获取配额使用情况
        let usage = quota_manager
            .get_quota_usage()
            .await
            .expect("Failed to get quota usage");
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].quota_type, QuotaType::Producer);
        assert_eq!(usage[0].unit, QuotaUnit::BytesPerSecond);
        assert_eq!(usage[0].scope, QuotaScope::Topic);
        assert_eq!(usage[0].quota_value, 1048576.0);

        // 等待更新间隔
        sleep(Duration::from_secs(2)).await;

        // 再次获取配额使用情况，应该已更新
        let updated_usage = quota_manager
            .get_quota_usage()
            .await
            .expect("Failed to get quota usage");
        assert_eq!(updated_usage.len(), 1);

        // 清理
        quota_manager
            .delete_quota(&QuotaType::Producer, &QuotaScope::Topic, &Some("test-topic".to_string()))
            .await
            .expect("Failed to delete quota");
    }
}
