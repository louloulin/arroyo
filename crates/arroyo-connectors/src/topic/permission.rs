use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::info;

/// 权限级别
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PermissionLevel {
    /// 只读权限
    Read,
    /// 写入权限
    Write,
    /// 管理权限
    Admin,
}

impl PermissionLevel {
    /// 检查是否包含指定权限
    pub fn includes(&self, other: &PermissionLevel) -> bool {
        match (self, other) {
            (PermissionLevel::Admin, _) => true,
            (PermissionLevel::Write, PermissionLevel::Read) => true,
            (PermissionLevel::Write, PermissionLevel::Write) => true,
            (PermissionLevel::Read, PermissionLevel::Read) => true,
            _ => false,
        }
    }
}

/// 用户角色
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UserRole {
    /// 普通用户
    User,
    /// 管理员
    Admin,
}

impl UserRole {
    /// 获取角色对应的默认权限级别
    pub fn default_permission(&self) -> PermissionLevel {
        match self {
            UserRole::User => PermissionLevel::Read,
            UserRole::Admin => PermissionLevel::Admin,
        }
    }
}

/// Topic 权限配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPermission {
    /// Topic 名称
    pub topic_name: String,
    /// 用户 ID
    pub user_id: String,
    /// 权限级别
    pub permission: PermissionLevel,
}

/// Topic 权限管理器
pub struct TopicPermissionManager {
    /// 用户角色映射
    user_roles: RwLock<HashMap<String, UserRole>>,
    /// Topic 权限映射
    topic_permissions: RwLock<HashMap<String, HashMap<String, PermissionLevel>>>,
}

impl TopicPermissionManager {
    /// 创建 Topic 权限管理器
    pub fn new() -> Self {
        Self {
            user_roles: RwLock::new(HashMap::new()),
            topic_permissions: RwLock::new(HashMap::new()),
        }
    }

    /// 设置用户角色
    pub async fn set_user_role(&self, user_id: &str, role: UserRole) {
        let mut user_roles = self.user_roles.write().await;
        user_roles.insert(user_id.to_string(), role);
        info!("Set user {} role to {:?}", user_id, role);
    }

    /// 获取用户角色
    pub async fn get_user_role(&self, user_id: &str) -> UserRole {
        let user_roles = self.user_roles.read().await;
        user_roles.get(user_id).copied().unwrap_or(UserRole::User)
    }

    /// 设置 Topic 权限
    pub async fn set_topic_permission(
        &self,
        topic_name: &str,
        user_id: &str,
        permission: PermissionLevel,
    ) -> Result<()> {
        let mut topic_permissions = self.topic_permissions.write().await;

        // 获取或创建 Topic 权限映射
        let user_permissions = topic_permissions
            .entry(topic_name.to_string())
            .or_insert_with(HashMap::new);

        // 设置用户权限
        user_permissions.insert(user_id.to_string(), permission);

        info!(
            "Set permission for user {} on topic {} to {:?}",
            user_id, topic_name, permission
        );

        Ok(())
    }

    /// 获取用户对 Topic 的权限
    pub async fn get_topic_permission(
        &self,
        topic_name: &str,
        user_id: &str,
    ) -> PermissionLevel {
        // 获取用户角色
        let role = self.get_user_role(user_id).await;

        // 管理员拥有所有权限
        if role == UserRole::Admin {
            return PermissionLevel::Admin;
        }

        // 检查特定 Topic 权限
        let topic_permissions = self.topic_permissions.read().await;
        if let Some(user_permissions) = topic_permissions.get(topic_name) {
            if let Some(permission) = user_permissions.get(user_id) {
                return *permission;
            }
        }

        // 返回角色默认权限
        role.default_permission()
    }

    /// 检查用户是否有指定权限
    pub async fn check_permission(
        &self,
        topic_name: &str,
        user_id: &str,
        required_permission: PermissionLevel,
    ) -> Result<()> {
        let permission = self.get_topic_permission(topic_name, user_id).await;

        if !permission.includes(&required_permission) {
            bail!(
                "User {} does not have {:?} permission on topic {}",
                user_id,
                required_permission,
                topic_name
            );
        }

        Ok(())
    }

    /// 获取用户可访问的所有 Topic
    pub async fn get_accessible_topics(
        &self,
        user_id: &str,
        required_permission: PermissionLevel,
    ) -> Vec<String> {
        let role = self.get_user_role(user_id).await;
        let topic_permissions = self.topic_permissions.read().await;

        // 管理员可以访问所有 Topic
        if role == UserRole::Admin {
            return topic_permissions.keys().cloned().collect();
        }

        // 筛选用户有权限的 Topic
        let mut accessible_topics = Vec::new();
        for (topic_name, user_permissions) in topic_permissions.iter() {
            if let Some(permission) = user_permissions.get(user_id) {
                if permission.includes(&required_permission) {
                    accessible_topics.push(topic_name.clone());
                }
            } else if role.default_permission().includes(&required_permission) {
                accessible_topics.push(topic_name.clone());
            }
        }

        accessible_topics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn test_topic_permission_manager() {
        let manager = TopicPermissionManager::new();

        // 设置用户角色
        manager.set_user_role("user1", UserRole::User).await;
        manager.set_user_role("admin1", UserRole::Admin).await;

        // 设置 Topic 权限
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

        // 获取可访问的 Topic
        let user1_read_topics = manager.get_accessible_topics("user1", PermissionLevel::Read).await;
        assert!(user1_read_topics.contains(&"topic1".to_string()));
        assert!(user1_read_topics.contains(&"topic2".to_string()));

        let user1_write_topics = manager.get_accessible_topics("user1", PermissionLevel::Write).await;
        assert!(user1_write_topics.contains(&"topic1".to_string()));
        assert!(!user1_write_topics.contains(&"topic2".to_string()));
    }
}
