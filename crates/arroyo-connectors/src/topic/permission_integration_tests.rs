#[cfg(test)]
mod tests {
    use crate::topic::{TopicAdmin, TopicPermissionManager};
    use crate::topic::permission::{PermissionLevel, UserRole};
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

    // 检查是否跳过 Kafka 测试
    fn should_skip_kafka_tests() -> bool {
        env::var("SKIP_KAFKA_TESTS").unwrap_or_else(|_| "false".to_string()) == "true"
    }

    #[tokio::test]
    async fn test_topic_permission_integration() {
        if should_skip_kafka_tests() {
            println!("Skipping test_topic_permission_integration because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicAdmin 和 TopicPermissionManager
        let server = get_test_server();
        let mut admin = TopicAdmin::new(&server).expect("Failed to create TopicAdmin");
        let permission_manager = TopicPermissionManager::new();
        
        // 设置权限管理器
        admin = admin.with_permission_manager(permission_manager);
        
        // 生成唯一的 Topic 名称
        let topic_name = generate_topic_name("test-permission");
        
        // 创建 Topic 配置
        let config = TopicConfig {
            name: topic_name.clone(),
            partitions: 3,
            replication_factor: 1,
            retention_ms: Some(86400000), // 1 day
            retention_bytes: Some(1073741824), // 1 GB
            cleanup_policy: "delete".to_string(),
            max_message_bytes: Some(1048576), // 1 MB
            description: Some("Test topic for permission".to_string()),
        };
        
        // 创建用户
        let admin_user = "admin_user";
        let write_user = "write_user";
        let read_user = "read_user";
        let no_permission_user = "no_permission_user";
        
        // 设置用户角色和权限
        if let Some(permission_manager) = admin.permission_manager.as_ref() {
            permission_manager.set_user_role(admin_user, UserRole::Admin).await;
            permission_manager.set_topic_permission(&topic_name, write_user, PermissionLevel::Write).await.unwrap();
            permission_manager.set_topic_permission(&topic_name, read_user, PermissionLevel::Read).await.unwrap();
        }
        
        // 测试 1: 管理员用户可以创建 Topic
        let result = admin.create_topic(&config, Some(admin_user)).await;
        assert!(result.is_ok(), "Admin user should be able to create topic");
        
        // 等待 Topic 创建完成
        sleep(Duration::from_secs(1)).await;
        
        // 测试 2: 读取用户可以获取 Topic 详情
        let result = admin.get_topic_details(&topic_name, Some(read_user)).await;
        assert!(result.is_ok(), "Read user should be able to get topic details");
        
        // 测试 3: 写入用户可以更新 Topic
        let updated_config = TopicConfig {
            name: topic_name.clone(),
            partitions: 3, // 分区数不能更改
            replication_factor: 1, // 副本因子不能更改
            retention_ms: Some(172800000), // 2 days
            retention_bytes: Some(2147483648), // 2 GB
            cleanup_policy: "compact".to_string(),
            max_message_bytes: Some(2097152), // 2 MB
            description: Some("Updated test topic".to_string()),
        };
        
        let result = admin.update_topic(&updated_config, Some(write_user)).await;
        assert!(result.is_ok(), "Write user should be able to update topic");
        
        // 测试 4: 无权限用户不能更新 Topic
        let result = admin.update_topic(&updated_config, Some(no_permission_user)).await;
        assert!(result.is_err(), "No permission user should not be able to update topic");
        assert!(result.unwrap_err().to_string().contains("does not have"), "Error message should mention permission");
        
        // 测试 5: 读取用户不能删除 Topic
        let result = admin.delete_topic(&topic_name, Some(read_user)).await;
        assert!(result.is_err(), "Read user should not be able to delete topic");
        assert!(result.unwrap_err().to_string().contains("does not have"), "Error message should mention permission");
        
        // 测试 6: 管理员用户可以删除 Topic
        let result = admin.delete_topic(&topic_name, Some(admin_user)).await;
        assert!(result.is_ok(), "Admin user should be able to delete topic");
        
        // 等待 Topic 删除完成
        sleep(Duration::from_secs(1)).await;
        
        // 测试 7: 验证 Topic 已删除
        let topics = admin.list_topics(Some(admin_user)).await.expect("Failed to list topics");
        assert!(!topics.iter().any(|t| t.name == topic_name), "Topic should be deleted");
    }
}
