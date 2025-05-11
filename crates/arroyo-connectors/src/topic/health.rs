use anyhow::{anyhow, Result};
use arroyo_rpc::api_types::topics::{
    TopicHealth, TopicHealthStatus, TopicPartitionHealth, TopicPartitionStatus,
};
use rdkafka::admin::AdminClient;
use rdkafka::config::ClientConfig;
use std::collections::HashMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::error;

// 使用 rdkafka 的默认客户端上下文
use rdkafka::client::DefaultClientContext;

/// Topic 健康检查器
pub struct TopicHealthChecker {
    /// 服务器地址
    server: String,
    /// 管理客户端
    admin_client: AdminClient<DefaultClientContext>,
    /// 健康检查缓存
    cache: HashMap<String, (TopicHealth, Instant)>,
    /// 缓存过期时间（秒）
    cache_ttl: u64,
}

impl TopicHealthChecker {
    /// 创建 Topic 健康检查器
    pub fn new(server: &str, cache_ttl: Option<u64>) -> Result<Self> {
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", server)
            .create::<AdminClient<DefaultClientContext>>()
            .map_err(|e| anyhow!("Failed to create admin client: {}", e))?;

        Ok(Self {
            server: server.to_string(),
            admin_client,
            cache: HashMap::new(),
            cache_ttl: cache_ttl.unwrap_or(60), // 默认缓存 60 秒
        })
    }

    /// 检查 Topic 健康状态
    pub async fn check_topic_health(&mut self, topic_name: &str, force_refresh: bool) -> Result<TopicHealth> {
        // 检查缓存
        if !force_refresh {
            if let Some((health, timestamp)) = self.cache.get(topic_name) {
                if timestamp.elapsed().as_secs() < self.cache_ttl {
                    return Ok(health.clone());
                }
            }
        }

        // 获取元数据
        let metadata = self
            .admin_client
            .inner()
            .fetch_metadata(Some(topic_name), std::time::Duration::from_secs(10))
            .map_err(|e| anyhow!("Failed to fetch metadata: {}", e))?;

        let topic = metadata
            .topics()
            .first()
            .ok_or_else(|| anyhow!("Topic not found in metadata"))?;

        // 检查 Topic 是否存在
        if topic.name() != topic_name {
            return Err(anyhow!("Topic '{}' does not exist", topic_name));
        }

        // 检查分区健康状态
        let mut partitions = Vec::new();
        let mut overall_status = TopicHealthStatus::Healthy;
        let mut issues = Vec::new();

        for partition in topic.partitions() {
            let partition_id = partition.id();
            let leader_id = partition.leader();
            let replicas: Vec<i32> = partition.replicas().to_vec();
            let isr: Vec<i32> = partition.isr().to_vec();

            // 检查分区状态
            let mut partition_status = TopicPartitionStatus::Healthy;
            let mut partition_issues = Vec::new();

            // 检查是否有领导者
            if leader_id == -1 {
                partition_status = TopicPartitionStatus::Unhealthy;
                partition_issues.push(format!("Partition {} has no leader", partition_id));
                overall_status = TopicHealthStatus::Unhealthy;
                issues.push(format!("Partition {} has no leader", partition_id));
            }

            // 检查同步副本数量
            if isr.len() < replicas.len() {
                partition_status = TopicPartitionStatus::Degraded;
                partition_issues.push(format!(
                    "Partition {} has {} out of {} replicas in sync",
                    partition_id,
                    isr.len(),
                    replicas.len()
                ));

                if overall_status != TopicHealthStatus::Unhealthy {
                    overall_status = TopicHealthStatus::Degraded;
                }

                issues.push(format!(
                    "Partition {} has {} out of {} replicas in sync",
                    partition_id,
                    isr.len(),
                    replicas.len()
                ));
            }

            // 添加分区健康状态
            partitions.push(TopicPartitionHealth {
                partition_id,
                status: partition_status,
                leader_id,
                replica_count: replicas.len() as i32,
                isr_count: isr.len() as i32,
                issues: partition_issues,
            });
        }

        // 创建 Topic 健康状态
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let health = TopicHealth {
            topic_name: topic_name.to_string(),
            status: overall_status,
            partition_count: partitions.len() as i32,
            partitions,
            issues,
            checked_at: now,
        };

        // 更新缓存
        self.cache.insert(topic_name.to_string(), (health.clone(), Instant::now()));

        Ok(health)
    }

    /// 检查多个 Topic 的健康状态
    pub async fn check_topics_health(
        &mut self,
        topic_names: &[String],
        force_refresh: bool,
    ) -> Result<Vec<TopicHealth>> {
        let mut results = Vec::new();

        for topic_name in topic_names {
            match self.check_topic_health(topic_name, force_refresh).await {
                Ok(health) => results.push(health),
                Err(e) => {
                    error!("Failed to check health for topic {}: {}", topic_name, e);
                    // 创建一个错误状态的健康报告
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    results.push(TopicHealth {
                        topic_name: topic_name.to_string(),
                        status: TopicHealthStatus::Unhealthy,
                        partition_count: 0,
                        partitions: Vec::new(),
                        issues: vec![format!("Failed to check health: {}", e)],
                        checked_at: now,
                    });
                }
            }
        }

        Ok(results)
    }

    /// 检查所有 Topic 的健康状态
    pub async fn check_all_topics_health(&mut self, force_refresh: bool) -> Result<Vec<TopicHealth>> {
        // 获取所有 Topic 的元数据
        let metadata = self
            .admin_client
            .inner()
            .fetch_metadata(None, std::time::Duration::from_secs(10))
            .map_err(|e| anyhow!("Failed to fetch metadata: {}", e))?;

        let topic_names: Vec<String> = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .filter(|name| !name.starts_with("__")) // 过滤内部 Topic
            .collect();

        self.check_topics_health(&topic_names, force_refresh).await
    }

    /// 清除缓存
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// 设置缓存过期时间
    pub fn set_cache_ttl(&mut self, ttl_seconds: u64) {
        self.cache_ttl = ttl_seconds;
    }
}
