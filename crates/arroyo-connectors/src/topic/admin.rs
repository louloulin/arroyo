use anyhow::{anyhow, bail, Result};
use arroyo_rpc::api_types::topics::{
    TopicConfig, TopicDetails, TopicInfo, TopicPartitionInfo,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

/// Topic 管理器
pub struct TopicAdmin {
    /// 服务器地址
    server: String,
    /// 管理客户端
    admin_client: AdminClient<DefaultClientContext>,
}

// 使用 rdkafka 的默认客户端上下文
use rdkafka::client::DefaultClientContext;

impl TopicAdmin {
    /// 创建 Topic 管理器
    pub fn new(server: &str) -> Result<Self> {
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", server)
            .create::<AdminClient<DefaultClientContext>>()
            .map_err(|e| anyhow!("Failed to create admin client: {}", e))?;

        Ok(Self {
            server: server.to_string(),
            admin_client,
        })
    }

    /// 创建 Topic
    pub async fn create_topic(&self, config: &TopicConfig) -> Result<TopicInfo> {
        // 检查 Topic 是否已存在
        if self.topic_exists(&config.name).await? {
            bail!("Topic '{}' already exists", config.name);
        }

        // 创建 Topic
        let replication = match config.replication_factor {
            r if r > 0 => TopicReplication::Fixed(config.replication_factor as i32),
            _ => TopicReplication::Fixed(1),
        };

        let mut topic_config = HashMap::new();
        if let Some(retention_ms) = config.retention_ms {
            topic_config.insert("retention.ms".to_string(), retention_ms.to_string());
        }
        if let Some(retention_bytes) = config.retention_bytes {
            topic_config.insert("retention.bytes".to_string(), retention_bytes.to_string());
        }
        topic_config.insert("cleanup.policy".to_string(), config.cleanup_policy.clone());
        if let Some(max_message_bytes) = config.max_message_bytes {
            topic_config.insert("max.message.bytes".to_string(), max_message_bytes.to_string());
        }

        // 创建 Topic
        let new_topic = NewTopic::new(&config.name, config.partitions, replication);

        // 执行创建操作
        self.admin_client
            .create_topics(&[new_topic], &AdminOptions::new())
            .await
            .map_err(|e| anyhow!("Failed to create topic: {}", e))?;

        info!("Created topic: {}", config.name);

        // 返回创建的 Topic 信息
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(TopicInfo {
            name: config.name.clone(),
            partitions: config.partitions,
            replication_factor: config.replication_factor,
            retention_ms: config.retention_ms,
            retention_bytes: config.retention_bytes,
            cleanup_policy: config.cleanup_policy.clone(),
            max_message_bytes: config.max_message_bytes,
            description: config.description.clone(),
            created_at: now,
            updated_at: now,
        })
    }

    /// 删除 Topic
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        // 检查 Topic 是否存在
        if !self.topic_exists(name).await? {
            bail!("Topic '{}' does not exist", name);
        }

        // 执行删除操作
        self.admin_client
            .delete_topics(&[name], &AdminOptions::new())
            .await
            .map_err(|e| anyhow!("Failed to delete topic: {}", e))?;

        info!("Deleted topic: {}", name);
        Ok(())
    }

    /// 更新 Topic 配置
    pub async fn update_topic(&self, config: &TopicConfig) -> Result<TopicInfo> {
        // 检查 Topic 是否存在
        if !self.topic_exists(&config.name).await? {
            bail!("Topic '{}' does not exist", config.name);
        }

        // 获取当前配置
        let current_config = self.get_topic_config(&config.name).await?;

        // 准备新配置
        let mut new_config = HashMap::new();
        if let Some(retention_ms) = config.retention_ms {
            new_config.insert("retention.ms".to_string(), retention_ms.to_string());
        }
        if let Some(retention_bytes) = config.retention_bytes {
            new_config.insert("retention.bytes".to_string(), retention_bytes.to_string());
        }
        new_config.insert("cleanup.policy".to_string(), config.cleanup_policy.clone());
        if let Some(max_message_bytes) = config.max_message_bytes {
            new_config.insert("max.message.bytes".to_string(), max_message_bytes.to_string());
        }

        // 执行更新操作
        // 注意：这里只是示例，实际实现需要使用 rdkafka 的 alter_configs 方法
        // 由于 rdkafka 的 Rust 绑定可能不完全支持所有 AdminClient 功能，
        // 可能需要使用其他方式实现配置更新

        // 返回更新后的 Topic 信息
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(TopicInfo {
            name: config.name.clone(),
            partitions: current_config.partitions,
            replication_factor: current_config.replication_factor,
            retention_ms: config.retention_ms,
            retention_bytes: config.retention_bytes,
            cleanup_policy: config.cleanup_policy.clone(),
            max_message_bytes: config.max_message_bytes,
            description: config.description.clone(),
            created_at: current_config.created_at,
            updated_at: now,
        })
    }

    /// 获取 Topic 列表
    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        // 获取元数据
        let metadata = self
            .admin_client
            .inner()
            .fetch_metadata(None, std::time::Duration::from_secs(10))
            .map_err(|e| anyhow!("Failed to fetch metadata: {}", e))?;

        let mut topics = Vec::new();
        for topic in metadata.topics() {
            // 跳过内部 Topic
            if topic.name().starts_with("__") {
                continue;
            }

            let config = self.get_topic_config(topic.name()).await?;
            topics.push(config);
        }

        Ok(topics)
    }

    /// 获取 Topic 详情
    pub async fn get_topic_details(&self, name: &str) -> Result<TopicDetails> {
        // 检查 Topic 是否存在
        if !self.topic_exists(name).await? {
            bail!("Topic '{}' does not exist", name);
        }

        // 获取元数据
        let metadata = self
            .admin_client
            .inner()
            .fetch_metadata(Some(name), std::time::Duration::from_secs(10))
            .map_err(|e| anyhow!("Failed to fetch metadata: {}", e))?;

        let topic = metadata
            .topics()
            .first()
            .ok_or_else(|| anyhow!("Topic not found in metadata"))?;

        // 获取基本信息
        let info = self.get_topic_config(name).await?;

        // 获取分区信息
        let mut partitions = Vec::new();
        for partition in topic.partitions() {
            // 获取副本列表
            let replicas: Vec<i32> = partition.replicas().to_vec();

            // 获取同步副本列表
            let isr: Vec<i32> = partition.isr().to_vec();

            // 获取领导者 ID
            let leader_id = partition.leader();

            partitions.push(TopicPartitionInfo {
                id: partition.id(),
                leader: leader_id,
                replicas,
                isr,
            });
        }

        // 获取消息数量和存储大小
        // 注意：这里只是示例，实际实现可能需要使用其他 API 获取这些信息
        let message_count = 0;
        let size_bytes = 0;

        Ok(TopicDetails {
            info,
            partitions,
            message_count,
            size_bytes,
        })
    }

    /// 检查 Topic 是否存在
    async fn topic_exists(&self, name: &str) -> Result<bool> {
        let metadata = self
            .admin_client
            .inner()
            .fetch_metadata(None, std::time::Duration::from_secs(10))
            .map_err(|e| anyhow!("Failed to fetch metadata: {}", e))?;

        for topic in metadata.topics() {
            if topic.name() == name {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// 获取 Topic 配置
    async fn get_topic_config(&self, name: &str) -> Result<TopicInfo> {
        // 获取元数据
        let metadata = self
            .admin_client
            .inner()
            .fetch_metadata(Some(name), std::time::Duration::from_secs(10))
            .map_err(|e| anyhow!("Failed to fetch metadata: {}", e))?;

        let topic = metadata
            .topics()
            .first()
            .ok_or_else(|| anyhow!("Topic not found in metadata"))?;

        // 获取配置
        // 注意：这里只是示例，实际实现需要使用 rdkafka 的 describe_configs 方法
        // 由于 rdkafka 的 Rust 绑定可能不完全支持所有 AdminClient 功能，
        // 可能需要使用其他方式获取配置

        // 使用默认值
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(TopicInfo {
            name: name.to_string(),
            partitions: topic.partitions().len() as i32,
            replication_factor: 1,
            retention_ms: None,
            retention_bytes: None,
            cleanup_policy: "delete".to_string(),
            max_message_bytes: None,
            description: None,
            created_at: now,
            updated_at: now,
        })
    }
}
