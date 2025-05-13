use crate::log_segment::{
    CleanupPolicy, CompactionPolicy, IndexEntry, LogSegment, LogSegmentConfig, LogSegmentIndex,
    LogSegmentManager, LogSegmentMetadata, LogSegmentState, TimeIndexEntry,
};
use crate::StorageProviderRef;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::io::{self, BufWriter, Cursor};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// 优化的日志段管理器
pub struct OptimizedLogSegmentManager {
    /// 存储提供者
    storage_provider: StorageProviderRef,
    /// 配置
    config: LogSegmentConfig,
    /// 活跃段
    active_segments: RwLock<HashMap<(String, u32), Arc<OptimizedLogSegment>>>,
    /// 只读段
    readonly_segments: RwLock<HashMap<(String, u32), Vec<Arc<OptimizedLogSegment>>>>,
    /// 段元数据缓存
    segment_metadata_cache: RwLock<HashMap<String, LogSegmentMetadata>>,
    /// 是否正在合并
    is_compacting: Mutex<bool>,
    /// 写入缓冲区大小
    write_buffer_size: usize,
    /// 读取缓冲区大小
    read_buffer_size: usize,
    /// 批量写入阈值
    batch_write_threshold: usize,
    /// 预读取启用阈值
    prefetch_threshold: usize,
    /// 索引缓存大小
    index_cache_size: usize,
}

/// 优化的日志段
pub struct OptimizedLogSegment {
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
    /// 写入缓冲区
    pub write_buffer: Mutex<BufWriter<Vec<u8>>>,
    /// 写入缓冲区大小
    pub write_buffer_size: usize,
    /// 上次刷新时间
    pub last_flush_time: RwLock<DateTime<Utc>>,
    /// 读取缓存
    pub read_cache: RwLock<HashMap<u64, Vec<u8>>>,
    /// 读取缓存大小
    pub read_cache_size: usize,
    /// 预读取状态
    pub prefetch_state: RwLock<PrefetchState>,
    /// 索引缓存
    pub index_cache: RwLock<IndexCache>,
}

/// 预读取状态
pub struct PrefetchState {
    /// 是否启用预读取
    pub enabled: bool,
    /// 上次访问的偏移量
    pub last_accessed_offset: u64,
    /// 预读取窗口大小
    pub window_size: usize,
    /// 预读取数据
    pub prefetched_data: HashMap<u64, Vec<u8>>,
}

/// 索引缓存
pub struct IndexCache {
    /// 偏移量索引缓存
    pub offset_index_cache: HashMap<u64, u64>,
    /// 时间戳索引缓存
    pub timestamp_index_cache: HashMap<i64, u64>,
    /// 缓存大小
    pub cache_size: usize,
}

impl OptimizedLogSegmentManager {
    /// 创建新的优化日志段管理器
    pub fn new(
        storage_provider: StorageProviderRef,
        config: LogSegmentConfig,
        write_buffer_size: usize,
        read_buffer_size: usize,
        batch_write_threshold: usize,
        prefetch_threshold: usize,
        index_cache_size: usize,
    ) -> Self {
        Self {
            storage_provider,
            config,
            active_segments: RwLock::new(HashMap::new()),
            readonly_segments: RwLock::new(HashMap::new()),
            segment_metadata_cache: RwLock::new(HashMap::new()),
            is_compacting: Mutex::new(false),
            write_buffer_size,
            read_buffer_size,
            batch_write_threshold,
            prefetch_threshold,
            index_cache_size,
        }
    }

    /// 使用默认配置创建新的优化日志段管理器
    pub fn new_with_defaults(storage_provider: StorageProviderRef, config: LogSegmentConfig) -> Self {
        Self::new(
            storage_provider,
            config,
            1024 * 1024,     // 1MB 写入缓冲区
            4 * 1024 * 1024, // 4MB 读取缓冲区
            64 * 1024,       // 64KB 批量写入阈值
            128 * 1024,      // 128KB 预读取阈值
            10000,           // 10000 条索引缓存
        )
    }

    /// 保存段元数据
    pub async fn save_segment_metadata(&self, metadata: &LogSegmentMetadata) -> Result<()> {
        let path = format!("{}.metadata", metadata.segment_id);
        let json = serde_json::to_vec(metadata)?;

        let object_path = object_store::path::Path::from(path);
        self.storage_provider.put(&object_path, json.into()).await?;

        // 更新缓存
        let mut cache = self.segment_metadata_cache.write().await;
        cache.insert(metadata.segment_id.clone(), metadata.clone());

        Ok(())
    }

    /// 加载段元数据
    pub async fn load_segment_metadata(&self, segment_id: &str) -> Result<LogSegmentMetadata> {
        // 首先检查缓存
        {
            let cache = self.segment_metadata_cache.read().await;
            if let Some(metadata) = cache.get(segment_id) {
                return Ok(metadata.clone());
            }
        }

        // 从存储加载
        let path = format!("{}.metadata", segment_id);
        let object_path = object_store::path::Path::from(path);

        let data = self.storage_provider.get(&object_path).await?;
        let bytes = data.bytes().await?;

        let metadata: LogSegmentMetadata = serde_json::from_slice(&bytes)?;

        // 更新缓存
        let mut cache = self.segment_metadata_cache.write().await;
        cache.insert(segment_id.to_string(), metadata.clone());

        Ok(metadata)
    }

    /// 获取活跃段
    pub async fn get_active_segment(&self, topic: &str, partition: u32) -> Result<Arc<OptimizedLogSegment>> {
        // 检查是否已有活跃段
        {
            let active_segments = self.active_segments.read().await;
            if let Some(segment) = active_segments.get(&(topic.to_string(), partition)) {
                return Ok(segment.clone());
            }
        }

        // 创建新的活跃段
        self.create_segment(topic, partition, 0).await
    }

    /// 获取段
    pub async fn get_segment(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<Arc<OptimizedLogSegment>> {
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

        Err(anyhow!("No segment found for topic {} partition {} offset {}", topic, partition, offset))
    }

    /// 滚动日志段
    pub async fn roll_segment(&self, topic: &str, partition: u32) -> Result<Arc<OptimizedLogSegment>> {
        // 获取当前活跃段
        let current_segment = self.get_active_segment(topic, partition).await?;

        // 将当前活跃段设置为只读
        {
            let mut metadata = current_segment.metadata.write().await;
            metadata.state = LogSegmentState::ReadOnly;
            self.save_segment_metadata(&metadata).await?;
        }

        // 刷新缓冲区
        current_segment.flush().await?;

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

    /// 创建新的日志段
    pub async fn create_segment(
        &self,
        topic: &str,
        partition: u32,
        base_offset: u64,
    ) -> Result<Arc<OptimizedLogSegment>> {
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

        // 创建写入缓冲区
        let write_buffer = BufWriter::with_capacity(self.write_buffer_size, Vec::new());

        // 创建预读取状态
        let prefetch_state = PrefetchState {
            enabled: false,
            last_accessed_offset: base_offset,
            window_size: self.prefetch_threshold,
            prefetched_data: HashMap::new(),
        };

        // 创建索引缓存
        let index_cache = IndexCache {
            offset_index_cache: HashMap::new(),
            timestamp_index_cache: HashMap::new(),
            cache_size: self.index_cache_size,
        };

        // 创建日志段
        let segment = Arc::new(OptimizedLogSegment {
            segment_id: segment_id.clone(),
            topic: topic.to_string(),
            partition,
            base_offset,
            metadata: RwLock::new(metadata.clone()),
            storage_provider: self.storage_provider.clone(),
            config: self.config.clone(),
            index: RwLock::new(index),
            write_lock: Mutex::new(()),
            write_buffer: Mutex::new(write_buffer),
            write_buffer_size: self.write_buffer_size,
            last_flush_time: RwLock::new(now),
            read_cache: RwLock::new(HashMap::new()),
            read_cache_size: self.read_buffer_size,
            prefetch_state: RwLock::new(prefetch_state),
            index_cache: RwLock::new(index_cache),
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
}
