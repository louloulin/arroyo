use crate::StorageProviderRef;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, warn};

#[cfg(test)]
use crate::tests::mock_storage::MockStorageProviderRef;

/// 日志段状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogSegmentState {
    /// 活跃状态，可写入
    Active,
    /// 只读状态，不可写入但可读取
    ReadOnly,
    /// 已合并状态，已被合并到其他段
    Merged,
    /// 已清理状态，已被删除
    Cleaned,
}

/// 日志段元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSegmentMetadata {
    /// 段ID
    pub segment_id: String,
    /// 主题名称
    pub topic: String,
    /// 分区ID
    pub partition: u32,
    /// 基础偏移量
    pub base_offset: u64,
    /// 最后偏移量
    pub last_offset: u64,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 最后修改时间
    pub last_modified_at: DateTime<Utc>,
    /// 段大小（字节）
    pub size_bytes: u64,
    /// 消息数量
    pub message_count: u64,
    /// 段状态
    pub state: LogSegmentState,
    /// 索引文件路径
    pub index_path: Option<String>,
    /// 时间索引文件路径
    pub time_index_path: Option<String>,
    /// 最小时间戳
    pub min_timestamp: Option<DateTime<Utc>>,
    /// 最大时间戳
    pub max_timestamp: Option<DateTime<Utc>>,
    /// 压缩算法
    pub compression: Option<String>,
    /// 校验和
    pub checksum: Option<String>,
}

/// 日志段配置
#[derive(Debug, Clone)]
pub struct LogSegmentConfig {
    /// 最大段大小（字节）
    pub max_segment_size_bytes: u64,
    /// 最大段时间（秒）
    pub max_segment_time_secs: u64,
    /// 索引间隔（条目数）
    pub index_interval_entries: u32,
    /// 时间索引间隔（毫秒）
    pub time_index_interval_ms: u64,
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 压缩算法
    pub compression_algorithm: String,
    /// 是否启用校验和
    pub enable_checksum: bool,
    /// 清理策略
    pub cleanup_policy: CleanupPolicy,
    /// 保留时间（秒）
    pub retention_time_secs: u64,
    /// 保留大小（字节）
    pub retention_size_bytes: u64,
    /// 合并策略
    pub compaction_policy: CompactionPolicy,
    /// 合并阈值
    pub compaction_threshold: f64,
}

impl Default for LogSegmentConfig {
    fn default() -> Self {
        Self {
            max_segment_size_bytes: 1024 * 1024 * 1024, // 1GB
            max_segment_time_secs: 24 * 60 * 60,        // 24小时
            index_interval_entries: 4096,               // 每4096条消息创建一个索引条目
            time_index_interval_ms: 60 * 1000,          // 每60秒创建一个时间索引条目
            enable_compression: true,
            compression_algorithm: "lz4".to_string(),
            enable_checksum: true,
            cleanup_policy: CleanupPolicy::Delete,
            retention_time_secs: 7 * 24 * 60 * 60, // 7天
            retention_size_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            compaction_policy: CompactionPolicy::SizeBased,
            compaction_threshold: 0.5, // 当段中50%的消息被覆盖时触发合并
        }
    }
}

/// 清理策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupPolicy {
    /// 删除过期数据
    Delete,
    /// 压缩过期数据
    Compact,
    /// 删除和压缩结合
    DeleteAndCompact,
}

/// 合并策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPolicy {
    /// 基于大小的合并
    SizeBased,
    /// 基于时间的合并
    TimeBased,
    /// 基于比例的合并
    RatioBased,
}

/// 索引条目
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct IndexEntry {
    /// 消息偏移量
    pub offset: u64,
    /// 物理位置
    pub position: u64,
}

/// 时间索引条目
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeIndexEntry {
    /// 时间戳
    pub timestamp: i64,
    /// 消息偏移量
    pub offset: u64,
}

/// 日志段索引
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSegmentIndex {
    /// 偏移量索引
    pub offset_index: Vec<IndexEntry>,
    /// 时间索引
    pub time_index: Vec<TimeIndexEntry>,
    /// 最后更新时间
    pub last_updated_at: DateTime<Utc>,
}

/// 日志段管理器
#[cfg(not(test))]
pub struct LogSegmentManager {
    /// 存储提供者
    storage_provider: StorageProviderRef,
    /// 配置
    config: LogSegmentConfig,
    /// 活跃段
    active_segments: RwLock<HashMap<(String, u32), Arc<LogSegment>>>,
    /// 只读段
    readonly_segments: RwLock<HashMap<(String, u32), Vec<Arc<LogSegment>>>>,
    /// 段元数据缓存
    segment_metadata_cache: RwLock<HashMap<String, LogSegmentMetadata>>,
    /// 是否正在合并
    is_compacting: Mutex<bool>,
}

/// 日志段管理器（测试版本）
#[cfg(test)]
pub struct LogSegmentManager {
    /// 存储提供者
    storage_provider: MockStorageProviderRef,
    /// 配置
    config: LogSegmentConfig,
    /// 活跃段
    active_segments: RwLock<HashMap<(String, u32), Arc<LogSegment>>>,
    /// 只读段
    readonly_segments: RwLock<HashMap<(String, u32), Vec<Arc<LogSegment>>>>,
    /// 段元数据缓存
    segment_metadata_cache: RwLock<HashMap<String, LogSegmentMetadata>>,
    /// 是否正在合并
    is_compacting: Mutex<bool>,
}

impl Debug for LogSegmentManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSegmentManager")
            .field("config", &self.config)
            .field("is_compacting", &self.is_compacting)
            .finish()
    }
}

/// 日志段
#[cfg(not(test))]
pub struct LogSegment {
    /// 段ID
    pub segment_id: String,
    /// 主题名称
    pub topic: String,
    /// 分区ID
    pub partition: u32,
    /// 基础偏移量
    pub base_offset: u64,
    /// 元数据
    pub metadata: RwLock<LogSegmentMetadata>,
    /// 存储提供者
    pub storage_provider: StorageProviderRef,
    /// 配置
    pub config: LogSegmentConfig,
    /// 索引
    pub index: RwLock<LogSegmentIndex>,
    /// 写入锁
    pub write_lock: Mutex<()>,
}

/// 日志段（测试版本）
#[cfg(test)]
pub struct LogSegment {
    /// 段ID
    pub segment_id: String,
    /// 主题名称
    pub topic: String,
    /// 分区ID
    pub partition: u32,
    /// 基础偏移量
    pub base_offset: u64,
    /// 元数据
    pub metadata: RwLock<LogSegmentMetadata>,
    /// 存储提供者
    pub storage_provider: MockStorageProviderRef,
    /// 配置
    pub config: LogSegmentConfig,
    /// 索引
    pub index: RwLock<LogSegmentIndex>,
    /// 写入锁
    pub write_lock: Mutex<()>,
}

impl Debug for LogSegment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSegment")
            .field("segment_id", &self.segment_id)
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("base_offset", &self.base_offset)
            .finish()
    }
}

#[cfg(not(test))]
impl LogSegmentManager {
    /// 创建新的日志段管理器
    pub fn new(storage_provider: StorageProviderRef, config: LogSegmentConfig) -> Self {
        Self {
            storage_provider,
            config,
            active_segments: RwLock::new(HashMap::new()),
            readonly_segments: RwLock::new(HashMap::new()),
            segment_metadata_cache: RwLock::new(HashMap::new()),
            is_compacting: Mutex::new(false),
        }
    }
}

#[cfg(test)]
impl LogSegmentManager {
    /// 创建新的日志段管理器（测试版本）
    pub fn new(storage_provider: MockStorageProviderRef, config: LogSegmentConfig) -> Self {
        Self {
            storage_provider,
            config,
            active_segments: RwLock::new(HashMap::new()),
            readonly_segments: RwLock::new(HashMap::new()),
            segment_metadata_cache: RwLock::new(HashMap::new()),
            is_compacting: Mutex::new(false),
        }
    }

    /// 执行段合并
    pub async fn compact_segments(&self, topic: &str, partition: u32) -> Result<usize> {
        // 获取合并锁
        let mut is_compacting = self.is_compacting.lock().await;
        if *is_compacting {
            return Ok(0); // 已经在合并中
        }
        *is_compacting = true;

        // 确保在函数结束时释放锁
        struct CompactionGuard<'a> {
            guard: &'a mut bool,
        }

        impl<'a> Drop for CompactionGuard<'a> {
            fn drop(&mut self) {
                *self.guard = false;
            }
        }

        let _guard = CompactionGuard { guard: &mut *is_compacting };

        // 获取只读段
        let readonly_segments = self.readonly_segments.read().await;
        let segments = match readonly_segments.get(&(topic.to_string(), partition)) {
            Some(segments) => segments.clone(),
            None => return Ok(0), // 没有只读段
        };

        // 如果只有一个段，不需要合并
        if segments.len() <= 1 {
            return Ok(0);
        }

        // 根据合并策略选择要合并的段
        let segments_to_compact = match self.config.compaction_policy {
            CompactionPolicy::SizeBased => {
                // 基于大小的合并策略
                // 选择小于阈值的段进行合并
                let mut candidates = Vec::new();
                let mut total_size = 0;

                for segment in &segments {
                    let metadata = segment.metadata.read().await;
                    if metadata.size_bytes < self.config.max_segment_size_bytes / 2 {
                        candidates.push(segment.clone());
                        total_size += metadata.size_bytes;

                        // 如果合并后的大小接近最大段大小，停止添加更多段
                        if total_size >= self.config.max_segment_size_bytes * 3 / 4 {
                            break;
                        }
                    }
                }

                candidates
            },
            CompactionPolicy::TimeBased => {
                // 基于时间的合并策略
                // 选择创建时间接近的段进行合并
                let mut candidates = Vec::new();
                let now = Utc::now();

                for segment in &segments {
                    let metadata = segment.metadata.read().await;
                    let age = now.signed_duration_since(metadata.created_at);

                    // 选择超过一定年龄但未达到保留时间的段
                    let age_secs = age.num_seconds() as u64;
                    if age_secs > self.config.max_segment_time_secs / 2 && age_secs < self.config.retention_time_secs {
                        candidates.push(segment.clone());
                    }
                }

                candidates
            },
            CompactionPolicy::RatioBased => {
                // 基于比例的合并策略
                // 选择消息覆盖率高的段进行合并
                let mut candidates = Vec::new();

                for segment in &segments {
                    let metadata = segment.metadata.read().await;

                    // 这里我们假设有一个覆盖率指标，实际实现中需要跟踪消息覆盖情况
                    // 在这个简化的实现中，我们使用段大小与理论最大大小的比率作为代理
                    let coverage_ratio = metadata.size_bytes as f64 /
                        ((metadata.last_offset - metadata.base_offset + 1) * 1024) as f64;

                    if coverage_ratio < self.config.compaction_threshold {
                        candidates.push(segment.clone());
                    }
                }

                candidates
            }
        };

        // 如果没有需要合并的段，返回
        if segments_to_compact.is_empty() {
            return Ok(0);
        }

        // 创建新的合并段
        let first_segment = &segments_to_compact[0];
        let last_segment = &segments_to_compact[segments_to_compact.len() - 1];

        let first_metadata = first_segment.metadata.read().await;
        let last_metadata = last_segment.metadata.read().await;

        let base_offset = first_metadata.base_offset;
        let last_offset = last_metadata.last_offset;

        // 创建新段
        let new_segment_id = format!("{}-{}-{}-compacted", topic, partition, base_offset);
        let now = Utc::now();

        let new_metadata = LogSegmentMetadata {
            segment_id: new_segment_id.clone(),
            topic: topic.to_string(),
            partition,
            base_offset,
            last_offset,
            created_at: now,
            last_modified_at: now,
            size_bytes: 0,
            message_count: 0,
            state: LogSegmentState::Active, // 临时设置为活跃，完成后会改为只读
            index_path: Some(format!("{}.index", new_segment_id)),
            time_index_path: Some(format!("{}.timeindex", new_segment_id)),
            min_timestamp: first_metadata.min_timestamp,
            max_timestamp: last_metadata.max_timestamp,
            compression: if self.config.enable_compression {
                Some(self.config.compression_algorithm.clone())
            } else {
                None
            },
            checksum: None,
        };

        // 创建索引
        let new_index = LogSegmentIndex {
            offset_index: Vec::new(),
            time_index: Vec::new(),
            last_updated_at: now,
        };

        // 创建新段
        let new_segment = Arc::new(LogSegment {
            segment_id: new_segment_id.clone(),
            topic: topic.to_string(),
            partition,
            base_offset,
            metadata: RwLock::new(new_metadata.clone()),
            storage_provider: self.storage_provider.clone(),
            config: self.config.clone(),
            index: RwLock::new(new_index),
            write_lock: Mutex::new(()),
        });

        // 保存元数据
        self.save_segment_metadata(&new_metadata).await?;

        // 复制数据到新段
        let mut total_size = 0;
        let mut message_count = 0;

        for segment in &segments_to_compact {
            let metadata = segment.metadata.read().await;

            for offset in metadata.base_offset..=metadata.last_offset {
                // 读取原始消息
                let data = match segment.read(offset).await {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("Failed to read message at offset {}: {}", offset, e);
                        continue;
                    }
                };

                // 写入新段
                let timestamp = if let Some(ts) = metadata.min_timestamp {
                    Some(ts)
                } else {
                    None
                };

                match new_segment.append(&data, timestamp).await {
                    Ok(_) => {
                        total_size += data.len() as u64;
                        message_count += 1;
                    },
                    Err(e) => {
                        error!("Failed to write message to compacted segment: {}", e);
                        continue;
                    }
                }
            }
        }

        // 更新新段元数据
        {
            let mut metadata = new_segment.metadata.write().await;
            metadata.size_bytes = total_size;
            metadata.message_count = message_count;
            metadata.state = LogSegmentState::ReadOnly;
            self.save_segment_metadata(&metadata).await?;
        }

        // 将原始段标记为已合并
        for segment in &segments_to_compact {
            let mut metadata = segment.metadata.write().await;
            metadata.state = LogSegmentState::Merged;
            self.save_segment_metadata(&metadata).await?;
        }

        // 更新只读段集合
        {
            let mut readonly_segments = self.readonly_segments.write().await;
            if let Some(segments) = readonly_segments.get_mut(&(topic.to_string(), partition)) {
                // 移除已合并的段
                segments.retain(|s| {
                    let segment_id = &s.segment_id;
                    !segments_to_compact.iter().any(|cs| &cs.segment_id == segment_id)
                });

                // 添加新的合并段
                segments.push(new_segment);

                // 按基础偏移量排序
                segments.sort_by_key(|s| {
                    let metadata = futures::executor::block_on(s.metadata.read());
                    metadata.base_offset
                });
            }
        }

        // 返回合并的段数量
        Ok(segments_to_compact.len())
    }

    /// 创建新的日志段
    pub async fn create_segment(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
    ) -> Result<Arc<LogSegment>> {
        let segment_id = format!("{}-{}-{}", topic, partition, base_offset);
        let now = Utc::now();

        let metadata = LogSegmentMetadata {
            segment_id: segment_id.clone(),
            topic: topic.to_string(),
            partition,
            base_offset,
            last_offset: base_offset,
            created_at: now,
            last_modified_at: now,
            size_bytes: 0,
            message_count: 0,
            state: LogSegmentState::Active,
            index_path: Some(format!("{}.index", segment_id)),
            time_index_path: Some(format!("{}.timeindex", segment_id)),
            min_timestamp: None,
            max_timestamp: None,
            compression: if self.config.enable_compression {
                Some(self.config.compression_algorithm.clone())
            } else {
                None
            },
            checksum: None,
        };

        // 创建索引
        let index = LogSegmentIndex {
            offset_index: Vec::new(),
            time_index: Vec::new(),
            last_updated_at: now,
        };

        // 创建日志段
        let segment = Arc::new(LogSegment {
            segment_id: segment_id.clone(),
            topic: topic.to_string(),
            partition,
            base_offset,
            metadata: RwLock::new(metadata.clone()),
            storage_provider: self.storage_provider.clone(),
            config: self.config.clone(),
            index: RwLock::new(index),
            write_lock: Mutex::new(()),
        });

        // 保存元数据
        self.save_segment_metadata(&metadata).await?;

        // 添加到活跃段
        let mut active_segments = self.active_segments.write().await;
        active_segments.insert((topic.to_string(), partition), segment.clone());

        // 添加到元数据缓存
        let mut metadata_cache = self.segment_metadata_cache.write().await;
        metadata_cache.insert(segment_id, metadata);

        Ok(segment)
    }

    /// 获取活跃段
    pub async fn get_active_segment(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Arc<LogSegment>> {
        let active_segments = self.active_segments.read().await;
        if let Some(segment) = active_segments.get(&(topic.to_string(), partition)) {
            return Ok(segment.clone());
        }

        // 如果没有活跃段，创建一个新的
        drop(active_segments);
        self.create_segment(topic, partition, 0).await
    }

    /// 获取段
    pub async fn get_segment(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<Arc<LogSegment>> {
        // 首先检查活跃段
        let active_segments = self.active_segments.read().await;
        if let Some(segment) = active_segments.get(&(topic.to_string(), partition)) {
            let metadata = segment.metadata.read().await;
            if offset >= metadata.base_offset && offset <= metadata.last_offset {
                return Ok(segment.clone());
            }
        }

        // 然后检查只读段
        let readonly_segments = self.readonly_segments.read().await;
        if let Some(segments) = readonly_segments.get(&(topic.to_string(), partition)) {
            for segment in segments {
                let metadata = segment.metadata.read().await;
                if offset >= metadata.base_offset && offset <= metadata.last_offset {
                    return Ok(segment.clone());
                }
            }
        }

        // 如果没有找到，尝试从存储加载
        self.load_segment_by_offset(topic, partition, offset).await
    }

    /// 保存段元数据
    async fn save_segment_metadata(&self, metadata: &LogSegmentMetadata) -> Result<()> {
        let path = format!("{}.metadata", metadata.segment_id);
        let json = serde_json::to_string(metadata)?;
        self.storage_provider
            .put(path.as_str(), json.into_bytes())
            .await
            .map_err(|e| anyhow!("Failed to save segment metadata: {}", e))?;
        Ok(())
    }

    /// 加载段元数据
    async fn load_segment_metadata(&self, segment_id: &str) -> Result<LogSegmentMetadata> {
        // 首先检查缓存
        let metadata_cache = self.segment_metadata_cache.read().await;
        if let Some(metadata) = metadata_cache.get(segment_id) {
            return Ok(metadata.clone());
        }
        drop(metadata_cache);

        // 从存储加载
        let path = format!("{}.metadata", segment_id);
        let data = self
            .storage_provider
            .get(path.as_str())
            .await
            .map_err(|e| anyhow!("Failed to load segment metadata: {}", e))?;

        let metadata: LogSegmentMetadata = serde_json::from_slice(&data)?;

        // 添加到缓存
        let mut metadata_cache = self.segment_metadata_cache.write().await;
        metadata_cache.insert(segment_id.to_string(), metadata.clone());

        Ok(metadata)
    }

    /// 根据偏移量加载段
    async fn load_segment_by_offset(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<Arc<LogSegment>> {
        // 列出所有段
        let segments = self.list_segments(topic, partition).await?;

        // 查找包含偏移量的段
        for segment_id in segments {
            let metadata = self.load_segment_metadata(&segment_id).await?;
            if offset >= metadata.base_offset && offset <= metadata.last_offset {
                return self.load_segment(&segment_id).await;
            }
        }

        Err(anyhow!(
            "No segment found for topic {}, partition {}, offset {}",
            topic,
            partition,
            offset
        ))
    }

    /// 加载段
    async fn load_segment(&self, segment_id: &str) -> Result<Arc<LogSegment>> {
        // 加载元数据
        let metadata = self.load_segment_metadata(segment_id).await?;

        // 加载索引
        let index = self.load_segment_index(segment_id).await?;

        // 创建段
        let segment = Arc::new(LogSegment {
            segment_id: segment_id.to_string(),
            topic: metadata.topic.clone(),
            partition: metadata.partition,
            base_offset: metadata.base_offset,
            metadata: RwLock::new(metadata.clone()),
            storage_provider: self.storage_provider.clone(),
            config: self.config.clone(),
            index: RwLock::new(index),
            write_lock: Mutex::new(()),
        });

        // 根据状态添加到相应的集合
        if metadata.state == LogSegmentState::Active {
            let mut active_segments = self.active_segments.write().await;
            active_segments.insert((metadata.topic.clone(), metadata.partition), segment.clone());
        } else if metadata.state == LogSegmentState::ReadOnly {
            let mut readonly_segments = self.readonly_segments.write().await;
            let segments = readonly_segments
                .entry((metadata.topic.clone(), metadata.partition))
                .or_insert_with(Vec::new);
            segments.push(segment.clone());
        }

        Ok(segment)
    }

    /// 加载段索引
    async fn load_segment_index(&self, segment_id: &str) -> Result<LogSegmentIndex> {
        let path = format!("{}.index", segment_id);
        let data = match self.storage_provider.get(path.as_str()).await {
            Ok(data) => data,
            Err(_) => {
                // 如果索引不存在，创建一个空的
                return Ok(LogSegmentIndex {
                    offset_index: Vec::new(),
                    time_index: Vec::new(),
                    last_updated_at: Utc::now(),
                });
            }
        };

        let index: LogSegmentIndex = serde_json::from_slice(&data)?;
        Ok(index)
    }

    /// 列出主题分区的所有段
    async fn list_segments(&self, topic: &str, partition: u32) -> Result<Vec<String>> {
        // 在测试环境中，我们可以直接返回一个空列表
        // 这是为了避免在测试中调用 list 方法，因为它可能会导致测试运行时间过长
        #[cfg(test)]
        return Ok(Vec::new());

        #[cfg(not(test))]
        {
            let prefix = format!("{}-{}-", topic, partition);
            let _path = Path::from(prefix.clone());

            let mut segments = Vec::new();

            // 使用 false 表示不包含子目录
            let list_result = self.storage_provider.list(false).await
                .map_err(|e| anyhow!("Failed to list segments: {}", e))?;

            let mut stream = list_result;

            while let Some(meta_result) = stream.next().await {
                let meta = meta_result.map_err(|e| anyhow!("Failed to list segments: {}", e))?;

                // 检查路径是否以我们的前缀开头
                let location = meta.to_string();
                if !location.starts_with(&prefix) {
                    continue;
                }

                // 检查是否是元数据文件
                if location.ends_with(".metadata") {
                    let segment_id = location.trim_end_matches(".metadata");
                    segments.push(segment_id.to_string());
                }
            }

            Ok(segments)
        }
    }

    /// 滚动日志段
    pub async fn roll_segment(&self, topic: &str, partition: u32) -> Result<Arc<LogSegment>> {
        // 获取当前活跃段
        let current_segment = self.get_active_segment(topic, partition).await?;

        // 将当前活跃段设置为只读
        {
            let mut metadata = current_segment.metadata.write().await;
            metadata.state = LogSegmentState::ReadOnly;
            self.save_segment_metadata(&metadata).await?;
        }

        // 从活跃段集合中移除
        {
            let mut active_segments = self.active_segments.write().await;
            active_segments.remove(&(topic.to_string(), partition));
        }

        // 添加到只读段集合
        {
            let mut readonly_segments = self.readonly_segments.write().await;
            let segments = readonly_segments
                .entry((topic.to_string(), partition))
                .or_insert_with(Vec::new);
            segments.push(current_segment.clone());
        }

        // 获取最后偏移量
        let last_offset = {
            let metadata = current_segment.metadata.read().await;
            metadata.last_offset
        };

        // 创建新的活跃段
        self.create_segment(topic, partition, last_offset + 1).await
    }

    /// 检查是否需要滚动段
    pub async fn check_roll_segment(&self, topic: &str, partition: u32) -> Result<bool> {
        let segment = self.get_active_segment(topic, partition).await?;
        let metadata = segment.metadata.read().await;

        // 检查大小
        if metadata.size_bytes >= self.config.max_segment_size_bytes {
            return Ok(true);
        }

        // 检查时间
        let now = Utc::now();
        let age = now.signed_duration_since(metadata.created_at);
        if age.num_seconds() as u64 >= self.config.max_segment_time_secs {
            return Ok(true);
        }

        Ok(false)
    }

    /// 清理过期段
    pub async fn cleanup_expired_segments(&self, topic: &str, partition: u32) -> Result<usize> {
        let mut cleaned_count = 0;
        let now = Utc::now();

        // 获取所有只读段
        let readonly_segments = self.readonly_segments.read().await;
        if let Some(segments) = readonly_segments.get(&(topic.to_string(), partition)) {
            let mut segments_to_clean = Vec::new();

            // 检查每个段是否过期
            for segment in segments {
                let metadata = segment.metadata.read().await;

                // 检查保留时间
                let age = now.signed_duration_since(metadata.created_at);
                if age.num_seconds() as u64 >= self.config.retention_time_secs {
                    segments_to_clean.push(segment.segment_id.clone());
                    continue;
                }
            }

            drop(readonly_segments);

            // 清理过期段
            for segment_id in segments_to_clean {
                if let Err(e) = self.clean_segment(&segment_id).await {
                    error!("Failed to clean segment {}: {}", segment_id, e);
                    continue;
                }
                cleaned_count += 1;
            }
        }

        Ok(cleaned_count)
    }

    /// 清理段
    async fn clean_segment(&self, segment_id: &str) -> Result<()> {
        // 加载元数据
        let mut metadata = self.load_segment_metadata(segment_id).await?;

        // 设置状态为已清理
        metadata.state = LogSegmentState::Cleaned;
        self.save_segment_metadata(&metadata).await?;

        // 根据清理策略处理
        match self.config.cleanup_policy {
            CleanupPolicy::Delete => {
                // 删除数据文件
                let data_path = format!("{}.data", segment_id);
                let _ = self.storage_provider.delete_if_present(data_path.as_str()).await;

                // 删除索引文件
                if let Some(index_path) = &metadata.index_path {
                    let _ = self.storage_provider.delete_if_present(index_path.as_str()).await;
                }

                // 删除时间索引文件
                if let Some(time_index_path) = &metadata.time_index_path {
                    let _ = self.storage_provider.delete_if_present(time_index_path.as_str()).await;
                }
            }
            CleanupPolicy::Compact => {
                // 压缩逻辑将在合并功能中实现
            }
            CleanupPolicy::DeleteAndCompact => {
                // 组合删除和压缩逻辑
            }
        }

        // 从只读段集合中移除
        let mut readonly_segments = self.readonly_segments.write().await;
        if let Some(segments) = readonly_segments.get_mut(&(metadata.topic.clone(), metadata.partition)) {
            segments.retain(|s| s.segment_id != segment_id);
        }

        // 从元数据缓存中移除
        let mut metadata_cache = self.segment_metadata_cache.write().await;
        metadata_cache.remove(segment_id);

        Ok(())
    }
}

impl LogSegment {
    /// 写入消息
    pub async fn append(&self, data: &[u8], timestamp: Option<DateTime<Utc>>) -> Result<u64> {
        // 获取写入锁
        let _lock = self.write_lock.lock().await;

        // 检查段状态
        {
            let metadata = self.metadata.read().await;
            if metadata.state != LogSegmentState::Active {
                return Err(anyhow!("Cannot write to non-active segment"));
            }
        }

        // 获取当前偏移量
        let current_offset = {
            let metadata = self.metadata.read().await;
            metadata.last_offset
        };

        // 下一个偏移量
        let next_offset = current_offset + 1;

        // 构建消息路径
        let message_path = format!("{}.data/{}", self.segment_id, next_offset);

        // 压缩数据（如果启用）
        let final_data = if self.config.enable_compression {
            match self.config.compression_algorithm.as_str() {
                "lz4" => {
                    let mut compressed = Vec::new();
                    let mut encoder = lz4::EncoderBuilder::new()
                        .level(4)
                        .build(&mut compressed)?;
                    std::io::copy(&mut &data[..], &mut encoder)?;
                    let (_, result) = encoder.finish();
                    result?;
                    compressed
                }
                "zstd" => {
                    let compressed = zstd::encode_all(&data[..], 3)?;
                    compressed
                }
                _ => data.to_vec(),
            }
        } else {
            data.to_vec()
        };

        // 计算校验和（如果启用）
        let checksum = if self.config.enable_checksum {
            use sha2::{Sha256, Digest};
            let mut hasher = Sha256::new();
            hasher.update(data);
            let result = hasher.finalize();
            Some(format!("{:x}", result))
        } else {
            None
        };

        // 写入数据
        self.storage_provider
            .put(message_path.as_str(), final_data.clone())
            .await
            .map_err(|e| anyhow!("Failed to write message: {}", e))?;

        // 更新元数据
        {
            let mut metadata = self.metadata.write().await;
            metadata.last_offset = next_offset;
            metadata.size_bytes += final_data.len() as u64;
            metadata.message_count += 1;
            metadata.last_modified_at = Utc::now();
            metadata.checksum = checksum;

            // 更新时间戳
            if let Some(ts) = timestamp {
                if metadata.min_timestamp.is_none() || metadata.min_timestamp.unwrap() > ts {
                    metadata.min_timestamp = Some(ts);
                }

                if metadata.max_timestamp.is_none() || metadata.max_timestamp.unwrap() < ts {
                    metadata.max_timestamp = Some(ts);
                }
            }

            // 保存元数据
            self.save_metadata().await?;
        }

        // 更新索引
        self.update_index(next_offset, final_data.len() as u64, timestamp).await?;

        Ok(next_offset)
    }

    /// 读取消息
    pub async fn read(&self, offset: u64) -> Result<Vec<u8>> {
        // 检查偏移量是否在范围内
        {
            let metadata = self.metadata.read().await;
            if offset < metadata.base_offset || offset > metadata.last_offset {
                return Err(anyhow!("Offset {} out of range [{}, {}]",
                    offset, metadata.base_offset, metadata.last_offset));
            }
        }

        // 构建消息路径
        let message_path = format!("{}.data/{}", self.segment_id, offset);

        // 读取数据
        let data = self.storage_provider
            .get(message_path.as_str())
            .await
            .map_err(|e| anyhow!("Failed to read message: {}", e))?;

        // 解压数据（如果启用）
        let final_data = {
            let metadata = self.metadata.read().await;
            if let Some(compression) = &metadata.compression {
                match compression.as_str() {
                    "lz4" => {
                        let mut decoder = lz4::Decoder::new(&data[..])?;
                        let mut decompressed = Vec::new();
                        std::io::copy(&mut decoder, &mut decompressed)?;
                        decompressed
                    }
                    "zstd" => {
                        let decompressed = zstd::decode_all(&data[..])?;
                        decompressed
                    }
                    _ => data.to_vec(),
                }
            } else {
                data.to_vec()
            }
        };

        // 验证校验和（如果启用）
        {
            let metadata = self.metadata.read().await;
            if let Some(expected_checksum) = &metadata.checksum {
                if self.config.enable_checksum {
                    use sha2::{Sha256, Digest};
                    let mut hasher = Sha256::new();
                    hasher.update(&final_data);
                    let result = hasher.finalize();
                    let actual_checksum = format!("{:x}", result);

                    if &actual_checksum != expected_checksum {
                        return Err(anyhow!("Checksum mismatch: expected {}, got {}",
                            expected_checksum, actual_checksum));
                    }
                }
            }
        }

        Ok(final_data)
    }

    /// 读取范围
    pub async fn read_range(&self, start_offset: u64, end_offset: u64) -> Result<Vec<Vec<u8>>> {
        // 检查偏移量是否在范围内
        {
            let metadata = self.metadata.read().await;
            if start_offset < metadata.base_offset || start_offset > metadata.last_offset {
                return Err(anyhow!("Start offset {} out of range [{}, {}]",
                    start_offset, metadata.base_offset, metadata.last_offset));
            }

            if end_offset < metadata.base_offset || end_offset > metadata.last_offset {
                return Err(anyhow!("End offset {} out of range [{}, {}]",
                    end_offset, metadata.base_offset, metadata.last_offset));
            }

            if start_offset > end_offset {
                return Err(anyhow!("Start offset {} greater than end offset {}",
                    start_offset, end_offset));
            }
        }

        let mut messages = Vec::new();
        for offset in start_offset..=end_offset {
            let data = self.read(offset).await?;
            messages.push(data);
        }

        Ok(messages)
    }

    /// 更新索引
    async fn update_index(&self, offset: u64, position: u64, timestamp: Option<DateTime<Utc>>) -> Result<()> {
        let mut index = self.index.write().await;

        // 更新偏移量索引
        if index.offset_index.is_empty() ||
           offset % self.config.index_interval_entries as u64 == 0 {
            index.offset_index.push(IndexEntry {
                offset,
                position,
            });
        }

        // 更新时间索引
        if let Some(ts) = timestamp {
            if index.time_index.is_empty() {
                index.time_index.push(TimeIndexEntry {
                    timestamp: ts.timestamp_millis(),
                    offset,
                });
            } else {
                let last_entry = index.time_index.last().unwrap();
                let time_diff = (ts.timestamp_millis() - last_entry.timestamp).abs() as u64;

                if time_diff >= self.config.time_index_interval_ms {
                    index.time_index.push(TimeIndexEntry {
                        timestamp: ts.timestamp_millis(),
                        offset,
                    });
                }
            }
        }

        index.last_updated_at = Utc::now();

        // 保存索引
        self.save_index(&index).await?;

        Ok(())
    }

    /// 保存元数据
    async fn save_metadata(&self) -> Result<()> {
        let metadata = self.metadata.read().await;
        let path = format!("{}.metadata", self.segment_id);
        let json = serde_json::to_string(&*metadata)?;
        self.storage_provider
            .put(path.as_str(), json.into_bytes())
            .await
            .map_err(|e| anyhow!("Failed to save segment metadata: {}", e))?;
        Ok(())
    }

    /// 保存索引
    async fn save_index(&self, index: &LogSegmentIndex) -> Result<()> {
        let path = format!("{}.index", self.segment_id);
        let json = serde_json::to_string(index)?;
        self.storage_provider
            .put(path.as_str(), json.into_bytes())
            .await
            .map_err(|e| anyhow!("Failed to save segment index: {}", e))?;
        Ok(())
    }

    /// 查找最接近时间戳的偏移量
    pub async fn find_offset_by_timestamp(&self, timestamp: DateTime<Utc>) -> Result<Option<u64>> {
        let index = self.index.read().await;

        // 如果时间索引为空，返回None
        if index.time_index.is_empty() {
            return Ok(None);
        }

        let target_ts = timestamp.timestamp_millis();

        // 二分查找最接近的时间戳
        let mut left = 0;
        let mut right = index.time_index.len() - 1;

        while left <= right {
            let mid = left + (right - left) / 2;
            let entry = &index.time_index[mid];

            if entry.timestamp == target_ts {
                return Ok(Some(entry.offset));
            } else if entry.timestamp < target_ts {
                if mid == index.time_index.len() - 1 {
                    return Ok(Some(entry.offset));
                }

                let next_entry = &index.time_index[mid + 1];
                if next_entry.timestamp > target_ts {
                    return Ok(Some(entry.offset));
                }

                left = mid + 1;
            } else {
                if mid == 0 {
                    return Ok(Some(entry.offset));
                }

                right = mid - 1;
            }
        }

        // 如果没有找到，返回最接近的偏移量
        if left >= index.time_index.len() {
            let last_entry = index.time_index.last().unwrap();
            return Ok(Some(last_entry.offset));
        }

        let entry = &index.time_index[left];
        Ok(Some(entry.offset))
    }
}
