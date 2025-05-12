#[cfg(test)]
mod tests {
    use crate::topic::TopicAdmin;
    use arroyo_rpc::api_types::topics::TopicConfig;
    use std::env;
    use std::time::Duration;
    use tokio::time::sleep;

    // 获取测试服务器地址
    fn get_test_server() -> String {
        env::var("KAFKA_TEST_SERVER").unwrap_or_else(|_| "localhost:9092".to_string())
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
    async fn test_topic_admin_create_delete() {
        // 创建 TopicAdmin
        let server = get_test_server();
        let admin = TopicAdmin::new(&server).expect("Failed to create TopicAdmin");

        // 生成唯一的 Topic 名称
        let topic_name = generate_topic_name("test-create-delete");

        // 创建 Topic
        let config = TopicConfig {
            name: topic_name.clone(),
            partitions: 3,
            replication_factor: 1,
            retention_ms: Some(86400000), // 1 day
            retention_bytes: Some(1073741824), // 1 GB
            cleanup_policy: "delete".to_string(),
            max_message_bytes: Some(1048576), // 1 MB
            description: Some("Test topic for create/delete".to_string()),
        };

        let topic_info = admin.create_topic(&config, None).await.expect("Failed to create topic");

        // 验证 Topic 信息
        assert_eq!(topic_info.name, topic_name);
        assert_eq!(topic_info.partitions, 3);
        assert_eq!(topic_info.replication_factor, 1);
        assert_eq!(topic_info.retention_ms, Some(86400000));
        assert_eq!(topic_info.retention_bytes, Some(1073741824));
        assert_eq!(topic_info.cleanup_policy, "delete");
        assert_eq!(topic_info.max_message_bytes, Some(1048576));
        assert_eq!(topic_info.description, Some("Test topic for create/delete".to_string()));

        // 等待 Topic 创建完成
        sleep(Duration::from_secs(1)).await;

        // 获取 Topic 详情
        let topic_details = admin.get_topic_details(&topic_name, None).await.expect("Failed to get topic details");
        assert_eq!(topic_details.info.name, topic_name);
        assert_eq!(topic_details.info.partitions, 3);
        assert_eq!(topic_details.partitions.len(), 3);

        // 删除 Topic
        admin.delete_topic(&topic_name, None).await.expect("Failed to delete topic");

        // 等待 Topic 删除完成
        sleep(Duration::from_secs(1)).await;

        // 验证 Topic 已删除
        let topics = admin.list_topics(None).await.expect("Failed to list topics");
        assert!(!topics.iter().any(|t| t.name == topic_name));
    }

    #[tokio::test]
    async fn test_topic_admin_update() {
        // 创建 TopicAdmin
        let server = get_test_server();
        let admin = TopicAdmin::new(&server).expect("Failed to create TopicAdmin");

        // 生成唯一的 Topic 名称
        let topic_name = generate_topic_name("test-update");

        // 创建 Topic
        let config = TopicConfig {
            name: topic_name.clone(),
            partitions: 2,
            replication_factor: 1,
            retention_ms: Some(86400000), // 1 day
            retention_bytes: None,
            cleanup_policy: "delete".to_string(),
            max_message_bytes: None,
            description: Some("Test topic for update".to_string()),
        };

        let topic_info = admin.create_topic(&config, None).await.expect("Failed to create topic");

        // 验证 Topic 信息
        assert_eq!(topic_info.name, topic_name);
        assert_eq!(topic_info.partitions, 2);

        // 等待 Topic 创建完成
        sleep(Duration::from_secs(1)).await;

        // 更新 Topic 配置
        let updated_config = TopicConfig {
            name: topic_name.clone(),
            partitions: 2, // 分区数不能更改
            replication_factor: 1, // 副本因子不能更改
            retention_ms: Some(172800000), // 2 days
            retention_bytes: Some(2147483648), // 2 GB
            cleanup_policy: "compact".to_string(),
            max_message_bytes: Some(2097152), // 2 MB
            description: Some("Updated test topic".to_string()),
        };

        let updated_info = admin.update_topic(&updated_config, None).await.expect("Failed to update topic");

        // 验证更新后的 Topic 信息
        assert_eq!(updated_info.name, topic_name);
        assert_eq!(updated_info.partitions, 2);
        assert_eq!(updated_info.retention_ms, Some(172800000));
        assert_eq!(updated_info.retention_bytes, Some(2147483648));
        assert_eq!(updated_info.cleanup_policy, "compact");
        assert_eq!(updated_info.max_message_bytes, Some(2097152));
        assert_eq!(updated_info.description, Some("Updated test topic".to_string()));

        // 删除 Topic
        admin.delete_topic(&topic_name, None).await.expect("Failed to delete topic");
    }

    #[tokio::test]
    async fn test_topic_admin_list() {
        // 创建 TopicAdmin
        let server = get_test_server();
        let admin = TopicAdmin::new(&server).expect("Failed to create TopicAdmin");

        // 生成唯一的 Topic 名称
        let topic_name = generate_topic_name("test-list");

        // 创建 Topic
        let config = TopicConfig {
            name: topic_name.clone(),
            partitions: 1,
            replication_factor: 1,
            retention_ms: None,
            retention_bytes: None,
            cleanup_policy: "delete".to_string(),
            max_message_bytes: None,
            description: Some("Test topic for list".to_string()),
        };

        admin.create_topic(&config, None).await.expect("Failed to create topic");

        // 等待 Topic 创建完成
        sleep(Duration::from_secs(1)).await;

        // 获取 Topic 列表
        let topics = admin.list_topics(None).await.expect("Failed to list topics");

        // 验证新创建的 Topic 在列表中
        assert!(topics.iter().any(|t| t.name == topic_name));

        // 删除 Topic
        admin.delete_topic(&topic_name, None).await.expect("Failed to delete topic");
    }
}
