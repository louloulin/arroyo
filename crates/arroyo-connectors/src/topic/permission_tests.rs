#[cfg(test)]
mod tests {
    use crate::topic::permission::{PermissionLevel, TopicPermissionManager, UserRole};

    #[tokio::test]
    async fn test_permission_level_includes() {
        assert!(PermissionLevel::Admin.includes(&PermissionLevel::Read));
        assert!(PermissionLevel::Admin.includes(&PermissionLevel::Write));
        assert!(PermissionLevel::Admin.includes(&PermissionLevel::Admin));

        assert!(PermissionLevel::Write.includes(&PermissionLevel::Read));
        assert!(PermissionLevel::Write.includes(&PermissionLevel::Write));
        assert!(!PermissionLevel::Write.includes(&PermissionLevel::Admin));

        assert!(PermissionLevel::Read.includes(&PermissionLevel::Read));
        assert!(!PermissionLevel::Read.includes(&PermissionLevel::Write));
        assert!(!PermissionLevel::Read.includes(&PermissionLevel::Admin));
    }

    #[tokio::test]
    async fn test_user_role_default_permission() {
        assert_eq!(UserRole::User.default_permission(), PermissionLevel::Read);
        assert_eq!(UserRole::Admin.default_permission(), PermissionLevel::Admin);
    }

    #[tokio::test]
    async fn test_permission_manager_basic() {
        let manager = TopicPermissionManager::new();

        // 设置用户角色
        manager.set_user_role("user1", UserRole::User).await;
        manager.set_user_role("admin1", UserRole::Admin).await;

        // 验证用户角色
        assert_eq!(manager.get_user_role("user1").await, UserRole::User);
        assert_eq!(manager.get_user_role("admin1").await, UserRole::Admin);
        assert_eq!(manager.get_user_role("unknown").await, UserRole::User); // 默认角色
    }

    #[tokio::test]
    async fn test_topic_permission_setting() {
        let manager = TopicPermissionManager::new();

        // 设置 Topic 权限
        manager.set_topic_permission("topic1", "user1", PermissionLevel::Write).await.unwrap();
        manager.set_topic_permission("topic2", "user1", PermissionLevel::Read).await.unwrap();
        manager.set_topic_permission("topic3", "user2", PermissionLevel::Admin).await.unwrap();

        // 验证权限
        assert_eq!(
            manager.get_topic_permission("topic1", "user1").await,
            PermissionLevel::Write
        );
        assert_eq!(
            manager.get_topic_permission("topic2", "user1").await,
            PermissionLevel::Read
        );
        assert_eq!(
            manager.get_topic_permission("topic3", "user2").await,
            PermissionLevel::Admin
        );

        // 未设置权限的用户应该获得默认权限
        assert_eq!(
            manager.get_topic_permission("topic1", "user3").await,
            PermissionLevel::Read
        );

        // 设置用户为管理员后，应该获得管理员权限
        manager.set_user_role("user3", UserRole::Admin).await;
        assert_eq!(
            manager.get_topic_permission("topic1", "user3").await,
            PermissionLevel::Admin
        );
    }

    #[tokio::test]
    async fn test_permission_check() {
        let manager = TopicPermissionManager::new();

        // 设置用户角色和权限
        manager.set_user_role("admin1", UserRole::Admin).await;
        manager.set_topic_permission("topic1", "user1", PermissionLevel::Write).await.unwrap();
        manager.set_topic_permission("topic2", "user1", PermissionLevel::Read).await.unwrap();

        // 检查权限
        assert!(manager.check_permission("topic1", "user1", PermissionLevel::Read).await.is_ok());
        assert!(manager.check_permission("topic1", "user1", PermissionLevel::Write).await.is_ok());
        assert!(manager.check_permission("topic1", "user1", PermissionLevel::Admin).await.is_err());

        assert!(manager.check_permission("topic2", "user1", PermissionLevel::Read).await.is_ok());
        assert!(manager.check_permission("topic2", "user1", PermissionLevel::Write).await.is_err());

        // 管理员应该有所有权限
        assert!(manager.check_permission("topic1", "admin1", PermissionLevel::Admin).await.is_ok());
        assert!(manager.check_permission("topic2", "admin1", PermissionLevel::Admin).await.is_ok());
        assert!(manager.check_permission("topic3", "admin1", PermissionLevel::Admin).await.is_ok());
    }

    #[tokio::test]
    async fn test_accessible_topics() {
        let manager = TopicPermissionManager::new();

        // 设置用户角色和权限
        manager.set_user_role("admin1", UserRole::Admin).await;
        manager.set_topic_permission("topic1", "user1", PermissionLevel::Write).await.unwrap();
        manager.set_topic_permission("topic2", "user1", PermissionLevel::Read).await.unwrap();
        manager.set_topic_permission("topic3", "user2", PermissionLevel::Admin).await.unwrap();

        // 获取可访问的 Topic
        let user1_read_topics = manager.get_accessible_topics("user1", PermissionLevel::Read).await;
        assert_eq!(user1_read_topics.len(), 2);
        assert!(user1_read_topics.contains(&"topic1".to_string()));
        assert!(user1_read_topics.contains(&"topic2".to_string()));

        let user1_write_topics = manager.get_accessible_topics("user1", PermissionLevel::Write).await;
        assert_eq!(user1_write_topics.len(), 1);
        assert!(user1_write_topics.contains(&"topic1".to_string()));

        let user2_admin_topics = manager.get_accessible_topics("user2", PermissionLevel::Admin).await;
        assert_eq!(user2_admin_topics.len(), 1);
        assert!(user2_admin_topics.contains(&"topic3".to_string()));

        // 管理员应该可以访问所有 Topic
        let admin_topics = manager.get_accessible_topics("admin1", PermissionLevel::Admin).await;
        assert_eq!(admin_topics.len(), 3);
        assert!(admin_topics.contains(&"topic1".to_string()));
        assert!(admin_topics.contains(&"topic2".to_string()));
        assert!(admin_topics.contains(&"topic3".to_string()));
    }
}
