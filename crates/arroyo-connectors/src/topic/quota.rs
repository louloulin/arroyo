use anyhow::{anyhow, bail, Result};
use arroyo_rpc::api_types::quotas::{
    QuotaConfig, QuotaScope, QuotaType, QuotaUsage, TopicLimitConfig,
};
use arroyo_rpc::api_types::topics::TopicConfig;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::collections::HashMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::info;

/// Topic 配额管理器
pub struct TopicQuotaManager {
    /// 服务器地址
    server: String,
    /// 管理客户端
    admin_client: AdminClient<DefaultClientContext>,
    /// 配额配置
    quotas: RwLock<HashMap<String, QuotaConfig>>,
    /// Topic 限制配置
    topic_limits: RwLock<HashMap<String, TopicLimitConfig>>,
    /// 配额使用情况
    quota_usage: RwLock<HashMap<String, QuotaUsage>>,
    /// 上次更新时间
    last_update: RwLock<Instant>,
    /// 更新间隔（秒）
    update_interval: u64,
}

impl TopicQuotaManager {
    /// 创建 Topic 配额管理器
    pub fn new(server: &str, update_interval: Option<u64>) -> Result<Self> {
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", server)
            .create::<AdminClient<DefaultClientContext>>()
            .map_err(|e| anyhow!("Failed to create admin client: {}", e))?;

        Ok(Self {
            server: server.to_string(),
            admin_client,
            quotas: RwLock::new(HashMap::new()),
            topic_limits: RwLock::new(HashMap::new()),
            quota_usage: RwLock::new(HashMap::new()),
            last_update: RwLock::new(Instant::now()),
            update_interval: update_interval.unwrap_or(60),
        })
    }

    /// 创建配额
    pub async fn create_quota(&self, config: &QuotaConfig) -> Result<QuotaConfig> {
        // 检查配额是否已存在
        let quota_key = self.get_quota_key(config);
        let mut quotas = self.quotas.write().await;
        if quotas.contains_key(&quota_key) {
            bail!("Quota already exists: {}", quota_key);
        }

        // 保存配额配置
        let quota_config = config.clone();
        quotas.insert(quota_key.clone(), quota_config.clone());

        // 初始化配额使用情况
        let mut quota_usage = self.quota_usage.write().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        quota_usage.insert(
            quota_key,
            QuotaUsage {
                quota_type: config.quota_type.clone(),
                unit: config.unit.clone(),
                scope: config.scope.clone(),
                quota_value: config.value,
                current_value: 0.0,
                usage_percentage: 0.0,
                is_exceeded: false,
                last_updated: now,
            },
        );

        info!("Created quota: {:?}", quota_config);
        Ok(quota_config)
    }

    /// 更新配额
    pub async fn update_quota(&self, config: &QuotaConfig) -> Result<QuotaConfig> {
        // 检查配额是否存在
        let quota_key = self.get_quota_key(config);
        let mut quotas = self.quotas.write().await;
        if !quotas.contains_key(&quota_key) {
            bail!("Quota does not exist: {}", quota_key);
        }

        // 更新配额配置
        let quota_config = config.clone();
        quotas.insert(quota_key.clone(), quota_config.clone());

        // 更新配额使用情况
        let mut quota_usage = self.quota_usage.write().await;
        if let Some(usage) = quota_usage.get_mut(&quota_key) {
            usage.quota_value = config.value;
            usage.usage_percentage = (usage.current_value / config.value) * 100.0;
            usage.is_exceeded = usage.current_value > config.value;
        }

        info!("Updated quota: {:?}", quota_config);
        Ok(quota_config)
    }

    /// 删除配额
    pub async fn delete_quota(&self, quota_type: &QuotaType, scope: &QuotaScope, name: &Option<String>) -> Result<()> {
        // 构建配额键
        let quota_key = self.build_quota_key(quota_type, scope, name);

        // 删除配额配置
        let mut quotas = self.quotas.write().await;
        if !quotas.contains_key(&quota_key) {
            bail!("Quota does not exist: {}", quota_key);
        }
        quotas.remove(&quota_key);

        // 删除配额使用情况
        let mut quota_usage = self.quota_usage.write().await;
        quota_usage.remove(&quota_key);

        info!("Deleted quota: {}", quota_key);
        Ok(())
    }

    /// 获取所有配额
    pub async fn get_quotas(&self) -> Result<Vec<QuotaConfig>> {
        let quotas = self.quotas.read().await;
        Ok(quotas.values().cloned().collect())
    }

    /// 获取配额使用情况
    pub async fn get_quota_usage(&self) -> Result<Vec<QuotaUsage>> {
        // 更新配额使用情况
        self.update_quota_usage().await?;

        let quota_usage = self.quota_usage.read().await;
        Ok(quota_usage.values().cloned().collect())
    }

    /// 创建 Topic 限制
    pub async fn create_topic_limit(&self, config: &TopicLimitConfig) -> Result<TopicLimitConfig> {
        // 检查 Topic 限制是否已存在
        let mut topic_limits = self.topic_limits.write().await;
        if topic_limits.contains_key(&config.topic_name) {
            bail!("Topic limit already exists: {}", config.topic_name);
        }

        // 保存 Topic 限制配置
        let limit_config = config.clone();
        topic_limits.insert(config.topic_name.clone(), limit_config.clone());

        info!("Created topic limit: {:?}", limit_config);
        Ok(limit_config)
    }

    /// 更新 Topic 限制
    pub async fn update_topic_limit(&self, config: &TopicLimitConfig) -> Result<TopicLimitConfig> {
        // 检查 Topic 限制是否存在
        let mut topic_limits = self.topic_limits.write().await;
        if !topic_limits.contains_key(&config.topic_name) {
            bail!("Topic limit does not exist: {}", config.topic_name);
        }

        // 更新 Topic 限制配置
        let limit_config = config.clone();
        topic_limits.insert(config.topic_name.clone(), limit_config.clone());

        info!("Updated topic limit: {:?}", limit_config);
        Ok(limit_config)
    }

    /// 删除 Topic 限制
    pub async fn delete_topic_limit(&self, topic_name: &str) -> Result<()> {
        // 删除 Topic 限制配置
        let mut topic_limits = self.topic_limits.write().await;
        if !topic_limits.contains_key(topic_name) {
            bail!("Topic limit does not exist: {}", topic_name);
        }
        topic_limits.remove(topic_name);

        info!("Deleted topic limit: {}", topic_name);
        Ok(())
    }

    /// 获取所有 Topic 限制
    pub async fn get_topic_limits(&self) -> Result<Vec<TopicLimitConfig>> {
        let topic_limits = self.topic_limits.read().await;
        Ok(topic_limits.values().cloned().collect())
    }

    /// 获取特定 Topic 的限制
    pub async fn get_topic_limit(&self, topic_name: &str) -> Result<Option<TopicLimitConfig>> {
        let topic_limits = self.topic_limits.read().await;
        Ok(topic_limits.get(topic_name).cloned())
    }

    /// 验证 Topic 配置是否符合限制
    pub async fn validate_topic_config(&self, config: &TopicConfig) -> Result<()> {
        // 获取 Topic 限制
        let topic_limits = self.topic_limits.read().await;

        // 检查全局限制
        if let Some(global_limit) = topic_limits.get("*") {
            self.check_topic_limit(config, global_limit)?;
        }

        // 检查特定 Topic 限制
        if let Some(topic_limit) = topic_limits.get(&config.name) {
            self.check_topic_limit(config, topic_limit)?;
        }

        Ok(())
    }

    /// 检查 Topic 配置是否符合限制
    fn check_topic_limit(&self, config: &TopicConfig, limit: &TopicLimitConfig) -> Result<()> {
        // 检查分区数
        if let Some(max_partitions) = limit.max_partitions {
            if config.partitions > max_partitions {
                bail!(
                    "Topic partition count exceeds limit: {} > {}",
                    config.partitions,
                    max_partitions
                );
            }
        }

        // 检查副本因子
        if let Some(max_replication_factor) = limit.max_replication_factor {
            if config.replication_factor > max_replication_factor {
                bail!(
                    "Topic replication factor exceeds limit: {} > {}",
                    config.replication_factor,
                    max_replication_factor
                );
            }
        }

        // 检查保留时间
        if let Some(max_retention_ms) = limit.max_retention_ms {
            if let Some(retention_ms) = config.retention_ms {
                if retention_ms > max_retention_ms {
                    bail!(
                        "Topic retention time exceeds limit: {} > {}",
                        retention_ms,
                        max_retention_ms
                    );
                }
            }
        }

        // 检查保留大小
        if let Some(max_retention_bytes) = limit.max_retention_bytes {
            if let Some(retention_bytes) = config.retention_bytes {
                if retention_bytes > max_retention_bytes {
                    bail!(
                        "Topic retention size exceeds limit: {} > {}",
                        retention_bytes,
                        max_retention_bytes
                    );
                }
            }
        }

        // 检查消息大小
        if let Some(max_message_bytes) = limit.max_message_bytes {
            if let Some(message_bytes) = config.max_message_bytes {
                if message_bytes > max_message_bytes {
                    bail!(
                        "Topic message size exceeds limit: {} > {}",
                        message_bytes,
                        max_message_bytes
                    );
                }
            }
        }

        Ok(())
    }

    /// 检查配额限制
    pub async fn check_quota(&self, quota_type: &QuotaType, scope: &QuotaScope, name: &Option<String>, value: f64) -> Result<bool> {
        // 构建配额键
        let quota_key = self.build_quota_key(quota_type, scope, name);

        // 获取配额配置
        let quotas = self.quotas.read().await;
        if let Some(quota) = quotas.get(&quota_key) {
            // 检查是否超过配额
            return Ok(value <= quota.value);
        }

        // 如果没有配额限制，则允许
        Ok(true)
    }

    /// 更新配额使用情况
    async fn update_quota_usage(&self) -> Result<()> {
        // 检查是否需要更新
        let mut last_update = self.last_update.write().await;
        if last_update.elapsed().as_secs() < self.update_interval {
            return Ok(());
        }
        *last_update = Instant::now();

        // 获取 Kafka 指标
        // 注意：这里只是示例，实际实现需要使用 Kafka 的 JMX 指标或其他方式获取使用情况
        // 由于 rdkafka 的 Rust 绑定可能不完全支持所有 AdminClient 功能，
        // 可能需要使用其他方式获取指标

        // 更新配额使用情况
        let mut quota_usage = self.quota_usage.write().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for (_key, usage) in quota_usage.iter_mut() {
            // 这里只是示例，实际实现需要根据实际指标更新
            usage.current_value = usage.current_value * 0.8 + (usage.quota_value * 0.5) * 0.2; // 简单的模拟值
            usage.usage_percentage = (usage.current_value / usage.quota_value) * 100.0;
            usage.is_exceeded = usage.current_value > usage.quota_value;
            usage.last_updated = now;
        }

        Ok(())
    }

    /// 获取配额键
    fn get_quota_key(&self, config: &QuotaConfig) -> String {
        self.build_quota_key(&config.quota_type, &config.scope, &config.name)
    }

    /// 构建配额键
    fn build_quota_key(&self, quota_type: &QuotaType, scope: &QuotaScope, name: &Option<String>) -> String {
        let type_str = match quota_type {
            QuotaType::Producer => "producer",
            QuotaType::Consumer => "consumer",
            QuotaType::Request => "request",
        };

        let scope_str = match scope {
            QuotaScope::Cluster => "cluster",
            QuotaScope::Topic => "topic",
            QuotaScope::User => "user",
            QuotaScope::ClientId => "client_id",
            QuotaScope::UserClientId => "user_client_id",
        };

        let name_str = name.clone().unwrap_or_else(|| "*".to_string());

        format!("{}:{}:{}", type_str, scope_str, name_str)
    }


}
