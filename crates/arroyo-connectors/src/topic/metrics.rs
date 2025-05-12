use anyhow::{anyhow, Result};
use arroyo_rpc::api_types::metrics::{
    PartitionMetrics, TopicMetrics, TopicMetricsFilter,
};
use std::collections::HashMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Topic 指标收集器
pub struct TopicMetricsCollector {
    /// 服务器地址
    server: String,
    /// Topic 指标缓存
    topic_metrics_cache: RwLock<HashMap<String, TopicMetrics>>,
    /// 分区指标缓存
    partition_metrics_cache: RwLock<HashMap<String, HashMap<i32, PartitionMetrics>>>,
    /// 缓存过期时间（秒）
    cache_ttl: u64,
    /// 上次缓存更新时间
    last_cache_update: RwLock<Instant>,
    /// 指标历史数据
    metrics_history: RwLock<HashMap<String, Vec<(u64, TopicMetrics)>>>,
    /// 历史数据保留时间（秒）
    history_retention: u64,
}

impl TopicMetricsCollector {
    /// 创建指标收集器
    pub fn new(server: &str, cache_ttl: Option<u64>, history_retention: Option<u64>) -> Result<Self> {
        Ok(Self {
            server: server.to_string(),
            topic_metrics_cache: RwLock::new(HashMap::new()),
            partition_metrics_cache: RwLock::new(HashMap::new()),
            cache_ttl: cache_ttl.unwrap_or(60),
            last_cache_update: RwLock::new(Instant::now()),
            metrics_history: RwLock::new(HashMap::new()),
            history_retention: history_retention.unwrap_or(86400), // 默认保留 24 小时
        })
    }

    /// 收集 Topic 指标
    pub async fn collect_topic_metrics(&self, topic_name: &str) -> Result<TopicMetrics> {
        // 检查缓存是否过期
        let mut last_cache_update = self.last_cache_update.write().await;
        if last_cache_update.elapsed().as_secs() < self.cache_ttl {
            // 如果缓存未过期，尝试从缓存获取
            let topic_metrics_cache = self.topic_metrics_cache.read().await;
            if let Some(metrics) = topic_metrics_cache.get(topic_name) {
                return Ok(metrics.clone());
            }
        }
        *last_cache_update = Instant::now();
        drop(last_cache_update);

        // 收集 Topic 指标
        let metrics = self.collect_topic_metrics_internal(topic_name).await?;

        // 更新缓存
        let mut topic_metrics_cache = self.topic_metrics_cache.write().await;
        topic_metrics_cache.insert(topic_name.to_string(), metrics.clone());

        // 更新历史数据
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut metrics_history = self.metrics_history.write().await;
        let history = metrics_history
            .entry(topic_name.to_string())
            .or_insert_with(Vec::new);
        history.push((now, metrics.clone()));

        // 清理过期的历史数据
        let retention_threshold = now.saturating_sub(self.history_retention);
        history.retain(|(timestamp, _)| *timestamp >= retention_threshold);

        Ok(metrics)
    }

    /// 内部方法：收集 Topic 指标
    async fn collect_topic_metrics_internal(&self, topic_name: &str) -> Result<TopicMetrics> {
        // 在实际实现中，这里应该从监控系统或存储系统获取真实的指标数据
        // 这里简单模拟一下
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 获取分区指标
        let partition_metrics = self.collect_partition_metrics(topic_name).await?;
        let partition_count = partition_metrics.len() as u32;

        // 计算 Topic 级别的聚合指标
        let mut total_size_bytes = 0;
        let mut total_message_count = 0;
        let mut bytes_in_per_sec = 0.0;
        let mut bytes_out_per_sec = 0.0;
        let mut messages_in_per_sec = 0.0;

        for metrics in partition_metrics.values() {
            total_size_bytes += metrics.size_bytes;
            total_message_count += metrics.message_count;
            bytes_in_per_sec += metrics.bytes_in_per_sec;
            bytes_out_per_sec += metrics.bytes_out_per_sec;
            messages_in_per_sec += metrics.messages_in_per_sec;
        }

        Ok(TopicMetrics {
            topic_name: topic_name.to_string(),
            partition_count,
            total_size_bytes,
            total_message_count,
            bytes_in_per_sec,
            bytes_out_per_sec,
            messages_in_per_sec,
            created_at: now.saturating_sub(3600), // 假设创建于 1 小时前
            updated_at: now,
            retention_bytes: 1073741824, // 1GB
            retention_ms: 604800000,     // 7 天
            metrics: HashMap::new(),     // 其他自定义指标
        })
    }

    /// 收集分区指标
    pub async fn collect_partition_metrics(
        &self,
        topic_name: &str,
    ) -> Result<HashMap<i32, PartitionMetrics>> {
        // 检查缓存是否过期
        let last_cache_update = self.last_cache_update.read().await;
        if last_cache_update.elapsed().as_secs() < self.cache_ttl {
            // 如果缓存未过期，尝试从缓存获取
            let partition_metrics_cache = self.partition_metrics_cache.read().await;
            if let Some(metrics) = partition_metrics_cache.get(topic_name) {
                return Ok(metrics.clone());
            }
        }
        drop(last_cache_update);

        // 收集分区指标
        let metrics = self.collect_partition_metrics_internal(topic_name).await?;

        // 更新缓存
        let mut partition_metrics_cache = self.partition_metrics_cache.write().await;
        partition_metrics_cache.insert(topic_name.to_string(), metrics.clone());

        Ok(metrics)
    }

    /// 内部方法：收集分区指标
    async fn collect_partition_metrics_internal(
        &self,
        topic_name: &str,
    ) -> Result<HashMap<i32, PartitionMetrics>> {
        // 在实际实现中，这里应该从监控系统或存储系统获取真实的指标数据
        // 这里简单模拟一下
        let mut metrics = HashMap::new();

        // 假设有 3 个分区
        for partition_id in 0..3 {
            let replica_lag = HashMap::new();
            // 在实际实现中，这里应该获取真实的副本延迟数据

            metrics.insert(
                partition_id,
                PartitionMetrics {
                    partition_id,
                    topic_name: topic_name.to_string(),
                    size_bytes: 1024 * 1024 * (partition_id + 1) as u64, // 模拟不同大小
                    message_count: 1000 * (partition_id + 1) as u64,     // 模拟不同消息数
                    bytes_in_per_sec: 1024.0 * (partition_id + 1) as f64, // 模拟不同写入速率
                    bytes_out_per_sec: 2048.0 * (partition_id + 1) as f64, // 模拟不同读取速率
                    messages_in_per_sec: 10.0 * (partition_id + 1) as f64, // 模拟不同消息速率
                    replica_lag,
                    earliest_offset: 0,
                    latest_offset: 1000 * (partition_id + 1) as u64, // 模拟不同偏移量
                },
            );
        }

        Ok(metrics)
    }

    /// 获取 Topic 指标历史数据
    pub async fn get_topic_metrics_history(
        &self,
        topic_name: &str,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<(u64, TopicMetrics)>> {
        let metrics_history = self.metrics_history.read().await;
        let history = metrics_history
            .get(topic_name)
            .ok_or_else(|| anyhow!("No metrics history found for topic: {}", topic_name))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let start = start_time.unwrap_or_else(|| now.saturating_sub(3600)); // 默认 1 小时前
        let end = end_time.unwrap_or(now);

        let filtered_history: Vec<_> = history
            .iter()
            .filter(|(timestamp, _)| *timestamp >= start && *timestamp <= end)
            .cloned()
            .collect();

        Ok(filtered_history)
    }

    /// 查询 Topic 指标
    pub async fn query_topic_metrics(
        &self,
        filter: TopicMetricsFilter,
    ) -> Result<Vec<TopicMetrics>> {
        let topic_metrics_cache = self.topic_metrics_cache.read().await;
        let mut result = Vec::new();

        for (topic_name, metrics) in topic_metrics_cache.iter() {
            // 应用过滤条件
            if let Some(ref topic_pattern) = filter.topic_pattern {
                if !topic_name.contains(topic_pattern) {
                    continue;
                }
            }

            if let Some(min_partition_count) = filter.min_partition_count {
                if metrics.partition_count < min_partition_count {
                    continue;
                }
            }

            if let Some(max_partition_count) = filter.max_partition_count {
                if metrics.partition_count > max_partition_count {
                    continue;
                }
            }

            if let Some(min_message_rate) = filter.min_message_rate {
                if metrics.messages_in_per_sec < min_message_rate {
                    continue;
                }
            }

            if let Some(max_message_rate) = filter.max_message_rate {
                if metrics.messages_in_per_sec > max_message_rate {
                    continue;
                }
            }

            result.push(metrics.clone());
        }

        // 应用排序
        if let Some(ref sort_by) = filter.sort_by {
            match sort_by.as_str() {
                "name" => result.sort_by(|a, b| a.topic_name.cmp(&b.topic_name)),
                "partition_count" => result.sort_by(|a, b| a.partition_count.cmp(&b.partition_count)),
                "size" => result.sort_by(|a, b| a.total_size_bytes.cmp(&b.total_size_bytes)),
                "message_count" => result.sort_by(|a, b| a.total_message_count.cmp(&b.total_message_count)),
                "message_rate" => result.sort_by(|a, b| {
                    a.messages_in_per_sec
                        .partial_cmp(&b.messages_in_per_sec)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }),
                _ => {}
            }

            if filter.sort_desc.unwrap_or(false) {
                result.reverse();
            }
        }

        // 应用分页
        if let Some(limit) = filter.limit {
            if let Some(offset) = filter.offset {
                let start = offset as usize;
                let end = (offset + limit) as usize;
                if start < result.len() {
                    result = result[start..std::cmp::min(end, result.len())].to_vec();
                } else {
                    result.clear();
                }
            } else {
                result.truncate(limit as usize);
            }
        }

        Ok(result)
    }
}
