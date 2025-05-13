use crate::log_segment::{
    IndexEntry, LogSegmentConfig, LogSegmentIndex, LogSegmentMetadata, LogSegmentState, TimeIndexEntry,
};
use crate::StorageProviderRef;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::future;
use object_store::path::Path;
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::warn;

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

impl OptimizedLogSegment {
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

        // 计算新偏移量
        let new_offset = current_offset + 1;

        // 确保数据文件存在
        let segment_path = format!("{}.data", self.segment_id);
        let object_path = Path::from(segment_path.clone());
        let exists = self.storage_provider.exists(segment_path.clone()).await.unwrap_or(false);
        if !exists {
            // 创建空文件
            self.storage_provider.put(object_path.clone(), Vec::new()).await?;
        }

        // 获取写入位置
        let position = {
            let mut write_buffer = self.write_buffer.lock().await;
            let position = write_buffer.get_ref().len() as u64;

            // 写入长度前缀
            let len = data.len() as u32;
            write_buffer.write_all(&len.to_le_bytes())?;

            // 写入时间戳
            let ts = timestamp.unwrap_or_else(Utc::now).timestamp_millis();
            write_buffer.write_all(&ts.to_le_bytes())?;

            // 写入数据
            write_buffer.write_all(data)?;

            // 立即刷新到存储，确保数据可见
            self.flush_internal(&mut write_buffer).await?;

            position
        };

        // 更新索引
        {
            let mut index = self.index.write().await;

            // 更新偏移量索引
            if index.offset_index.is_empty() ||
               (new_offset - index.offset_index.last().unwrap().offset) >= self.config.index_interval_entries as u64 {
                index.offset_index.push(IndexEntry {
                    offset: new_offset,
                    position,
                });
            }

            // 更新时间索引
            if let Some(ts) = timestamp {
                let ts_millis = ts.timestamp_millis();

                if index.time_index.is_empty() ||
                   (ts_millis - index.time_index.last().unwrap().timestamp) >= self.config.time_index_interval_ms as i64 {
                    index.time_index.push(TimeIndexEntry {
                        timestamp: ts_millis,
                        offset: new_offset,
                    });
                }
            }

            index.last_updated_at = Utc::now();
        }

        // 更新索引缓存
        {
            let mut index_cache = self.index_cache.write().await;

            // 更新偏移量索引缓存
            index_cache.offset_index_cache.insert(new_offset, position);

            // 更新时间戳索引缓存
            if let Some(ts) = timestamp {
                let ts_millis = ts.timestamp_millis();
                index_cache.timestamp_index_cache.insert(ts_millis, new_offset);

                // 如果缓存过大，移除最旧的条目
                if index_cache.timestamp_index_cache.len() > index_cache.cache_size {
                    if let Some(oldest_ts) = index_cache.timestamp_index_cache.keys().min().cloned() {
                        index_cache.timestamp_index_cache.remove(&oldest_ts);
                    }
                }
            }

            // 如果缓存过大，移除最旧的条目
            if index_cache.offset_index_cache.len() > index_cache.cache_size {
                if let Some(oldest_offset) = index_cache.offset_index_cache.keys().min().cloned() {
                    index_cache.offset_index_cache.remove(&oldest_offset);
                }
            }
        }

        // 更新元数据
        {
            let mut metadata = self.metadata.write().await;
            metadata.last_offset = new_offset;
            metadata.last_modified_at = Utc::now();
            metadata.size_bytes += (4 + 8 + data.len()) as u64; // 长度(4) + 时间戳(8) + 数据
            metadata.message_count += 1;

            // 更新时间戳范围
            if let Some(ts) = timestamp {
                if metadata.min_timestamp.is_none() || metadata.min_timestamp.unwrap() > ts {
                    metadata.min_timestamp = Some(ts);
                }

                if metadata.max_timestamp.is_none() || metadata.max_timestamp.unwrap() < ts {
                    metadata.max_timestamp = Some(ts);
                }
            }
        }

        Ok(new_offset)
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

        // 检查读取缓存
        {
            let read_cache = self.read_cache.read().await;
            if let Some(data) = read_cache.get(&offset) {
                return Ok(data.clone());
            }
        }

        // 检查预读取缓存
        {
            let prefetch_state = self.prefetch_state.read().await;
            if let Some(data) = prefetch_state.prefetched_data.get(&offset) {
                return Ok(data.clone());
            }
        }

        // 确保数据文件存在
        let segment_path = format!("{}.data", self.segment_id);
        let exists = self.storage_provider.exists(segment_path.clone()).await.unwrap_or(false);
        if !exists {
            return Err(anyhow!("Segment data file does not exist: {}", segment_path));
        }

        // 查找位置
        let position = self.find_position(offset).await?;

        // 读取数据
        let data = self.read_at_position(position).await?;

        // 更新读取缓存
        {
            let mut read_cache = self.read_cache.write().await;
            read_cache.insert(offset, data.clone());

            // 如果缓存过大，移除最旧的条目
            if read_cache.len() > self.read_cache_size {
                if let Some(oldest_offset) = read_cache.keys().min().cloned() {
                    read_cache.remove(&oldest_offset);
                }
            }
        }

        // 更新预读取状态
        {
            let mut prefetch_state = self.prefetch_state.write().await;

            // 如果偏移量连续，启用预读取
            if prefetch_state.last_accessed_offset + 1 == offset {
                prefetch_state.enabled = true;

                // 预读取后续数据
                if prefetch_state.enabled {
                    let next_offset = offset + 1;
                    let metadata = self.metadata.read().await;

                    if next_offset <= metadata.last_offset {
                        // 预读取一些数据，但不在后台线程中执行
                        // 这样可以避免生命周期问题
                        let window_size = prefetch_state.window_size;
                        let max_prefetch = std::cmp::min(window_size as u64, metadata.last_offset - next_offset + 1);

                        // 预读取最多5个消息，避免阻塞太久
                        let actual_prefetch = std::cmp::min(max_prefetch, 5);

                        for i in 0..actual_prefetch {
                            let prefetch_offset = next_offset + i;
                            if let Ok(position) = self.find_position(prefetch_offset).await {
                                if let Ok(data) = self.read_at_position(position).await {
                                    prefetch_state.prefetched_data.insert(prefetch_offset, data);

                                    // 如果预读取缓存过大，移除最旧的条目
                                    if prefetch_state.prefetched_data.len() > window_size {
                                        if let Some(oldest_offset) = prefetch_state.prefetched_data.keys().min().cloned() {
                                            prefetch_state.prefetched_data.remove(&oldest_offset);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // 如果偏移量不连续，禁用预读取
                prefetch_state.enabled = false;
            }

            prefetch_state.last_accessed_offset = offset;
        }

        Ok(data)
    }

    /// 查找消息位置
    async fn find_position(&self, offset: u64) -> Result<u64> {
        // 如果偏移量是基础偏移量，返回0
        if offset == self.base_offset {
            return Ok(0);
        }

        // 检查索引缓存
        {
            let index_cache = self.index_cache.read().await;
            if let Some(position) = index_cache.offset_index_cache.get(&offset) {
                return Ok(*position);
            }
        }

        // 查找索引
        let index = self.index.read().await;

        // 如果索引为空，但偏移量是基础偏移量+1，返回0
        if index.offset_index.is_empty() && offset == self.base_offset + 1 {
            return Ok(0);
        }

        // 二分查找最接近的索引条目
        let mut left = 0;
        let mut right = index.offset_index.len();

        while left < right {
            let mid = (left + right) / 2;
            let entry = &index.offset_index[mid];

            if entry.offset == offset {
                return Ok(entry.position);
            } else if entry.offset < offset {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // 找到小于目标偏移量的最大索引条目
        if left > 0 {
            left -= 1;
        }

        if index.offset_index.is_empty() {
            // 如果索引为空，但偏移量是基础偏移量+1，返回0
            if offset == self.base_offset + 1 {
                return Ok(0);
            }
            return Err(anyhow!("Failed to find position for offset {}", offset));
        }

        if left >= index.offset_index.len() {
            return Err(anyhow!("Failed to find position for offset {}", offset));
        }

        let start_entry = &index.offset_index[left];
        let start_offset = start_entry.offset;
        let start_position = start_entry.position;

        // 如果正好找到，直接返回
        if start_offset == offset {
            return Ok(start_position);
        }

        // 否则，从起始位置顺序扫描
        let mut current_position = start_position;
        let mut current_offset = start_offset;

        // 读取整个文件
        let segment_path = format!("{}.data", self.segment_id);
        let object_path = Path::from(segment_path.clone());

        // 检查文件是否存在
        let exists = self.storage_provider.exists(segment_path.clone()).await.unwrap_or(false);
        if !exists {
            return Err(anyhow!("Segment data file does not exist: {}", segment_path));
        }

        // 读取文件
        let data = self.storage_provider.get(object_path).await?;

        // 如果文件为空，但偏移量是基础偏移量+1，返回0
        if data.is_empty() && offset == self.base_offset + 1 {
            return Ok(0);
        }

        while current_offset < offset {
            // 确保位置在范围内
            if current_position as usize + 4 > data.len() {
                return Err(anyhow!("Position out of range: position={}, data_len={}",
                    current_position, data.len()));
            }

            // 读取消息长度
            let len_bytes = &data[current_position as usize..(current_position + 4) as usize];
            let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as u64;

            // 跳过时间戳(8字节)和消息内容
            current_position += 4 + 8 + len;
            current_offset += 1;

            // 更新索引缓存
            {
                let mut index_cache = self.index_cache.write().await;
                index_cache.offset_index_cache.insert(current_offset, current_position);
            }

            if current_offset == offset {
                return Ok(current_position);
            }
        }

        Err(anyhow!("Failed to find position for offset {}", offset))
    }

    /// 从指定位置读取消息
    async fn read_at_position(&self, position: u64) -> Result<Vec<u8>> {
        // 获取写入缓冲区
        let write_buffer = self.write_buffer.lock().await;
        let buffer_data = write_buffer.get_ref();

        // 如果有写入缓冲区数据，直接从缓冲区读取
        if !buffer_data.is_empty() && position == 0 {
            // 读取消息长度
            let len_bytes = &buffer_data[0..4];
            let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as u64;

            // 读取时间戳和消息内容
            let content_start = (4 + 8) as usize;
            let content_end = (4 + 8 + len) as usize;

            // 确保索引在范围内
            if content_end <= buffer_data.len() {
                return Ok(buffer_data[content_start..content_end].to_vec());
            }
        }

        let segment_path = format!("{}.data", self.segment_id);
        let object_path = Path::from(segment_path.clone());

        // 检查文件是否存在
        let exists = self.storage_provider.exists(segment_path.clone()).await.unwrap_or(false);
        if !exists {
            // 如果文件不存在，但有写入缓冲区数据，返回第一条消息
            if !buffer_data.is_empty() && position == 0 {
                // 读取消息长度
                let len_bytes = &buffer_data[0..4];
                let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as u64;

                // 读取时间戳和消息内容
                let content_start = (4 + 8) as usize;
                let content_end = (4 + 8 + len) as usize;

                // 确保索引在范围内
                if content_end <= buffer_data.len() {
                    return Ok(buffer_data[content_start..content_end].to_vec());
                }
            }

            return Err(anyhow!("Segment data file does not exist: {}", segment_path));
        }

        // 读取整个文件
        let data = self.storage_provider.get(object_path).await?;

        // 如果文件为空，但有写入缓冲区数据，返回第一条消息
        if data.is_empty() {
            if !buffer_data.is_empty() && position == 0 {
                // 读取消息长度
                let len_bytes = &buffer_data[0..4];
                let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as u64;

                // 读取时间戳和消息内容
                let content_start = (4 + 8) as usize;
                let content_end = (4 + 8 + len) as usize;

                // 确保索引在范围内
                if content_end <= buffer_data.len() {
                    return Ok(buffer_data[content_start..content_end].to_vec());
                }
            }

            // 如果没有数据，返回空数据
            return Ok(Vec::new());
        }

        // 确保位置在范围内
        if position as usize + 4 > data.len() {
            return Err(anyhow!("Position out of range: position={}, data_len={}",
                position, data.len()));
        }

        // 读取消息长度
        let len_bytes = &data[position as usize..(position + 4) as usize];
        let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as u64;

        // 确保有足够的数据读取时间戳和消息内容
        if (position + 4 + 8) as usize > data.len() {
            return Err(anyhow!("Not enough data to read timestamp: position={}, data_len={}",
                position, data.len()));
        }

        // 读取时间戳和消息内容
        let content_start = (position + 4 + 8) as usize;
        let content_end = (position + 4 + 8 + len) as usize;

        // 确保索引在范围内
        if content_end > data.len() {
            return Err(anyhow!("Invalid message length: position={}, len={}, data_len={}",
                position, len, data.len()));
        }

        // 跳过时间戳(8字节)，只返回消息内容
        Ok(data[content_start..content_end].to_vec())
    }

    /// 刷新缓冲区
    pub async fn flush(&self) -> Result<()> {
        let mut write_buffer = self.write_buffer.lock().await;
        self.flush_internal(&mut write_buffer).await
    }

    /// 内部刷新方法
    async fn flush_internal(&self, write_buffer: &mut BufWriter<Vec<u8>>) -> Result<()> {
        // 如果缓冲区为空，不需要刷新
        if write_buffer.get_ref().is_empty() {
            return Ok(());
        }

        // 刷新缓冲区
        write_buffer.flush()?;

        // 获取数据
        let data = write_buffer.get_ref().clone();

        // 清空缓冲区
        write_buffer.get_mut().clear();

        // 写入存储
        let segment_path = format!("{}.data", self.segment_id);
        let object_path = Path::from(segment_path);

        // 获取元数据
        let _metadata = self.metadata.read().await;

        // 创建空文件，确保文件存在
        let segment_path_str = format!("{}.data", self.segment_id);
        let exists = self.storage_provider.exists(segment_path_str.clone()).await.unwrap_or(false);
        if !exists {
            // 创建空文件
            self.storage_provider.put(object_path.clone(), Vec::new()).await?;
        }

        // 追加写入
        self.storage_provider.put(object_path.clone(), data).await?;

        // 更新最后刷新时间
        let mut last_flush_time = self.last_flush_time.write().await;
        *last_flush_time = Utc::now();

        Ok(())
    }

    /// 创建段的新实例
    pub fn new_instance(&self) -> Arc<Self> {
        let last_flush_time = match self.last_flush_time.try_read() {
            Ok(guard) => *guard,
            Err(_) => Utc::now(),
        };

        let window_size = match self.prefetch_state.try_read() {
            Ok(guard) => guard.window_size,
            Err(_) => 128 * 1024, // 默认值
        };

        let cache_size = match self.index_cache.try_read() {
            Ok(guard) => guard.cache_size,
            Err(_) => 10000, // 默认值
        };

        Arc::new(Self {
            segment_id: self.segment_id.clone(),
            topic: self.topic.clone(),
            partition: self.partition,
            base_offset: self.base_offset,
            metadata: RwLock::new(LogSegmentMetadata {
                segment_id: self.segment_id.clone(),
                topic: self.topic.clone(),
                partition: self.partition,
                base_offset: self.base_offset,
                last_offset: self.base_offset,
                created_at: Utc::now(),
                last_modified_at: Utc::now(),
                size_bytes: 0,
                message_count: 0,
                state: LogSegmentState::Active,
                index_path: Some(format!("{}.index", self.segment_id)),
                time_index_path: Some(format!("{}.timeindex", self.segment_id)),
                min_timestamp: None,
                max_timestamp: None,
                compression: None,
                checksum: None,
            }),
            storage_provider: self.storage_provider.clone(),
            config: self.config.clone(),
            index: RwLock::new(LogSegmentIndex {
                offset_index: Vec::new(),
                time_index: Vec::new(),
                last_updated_at: Utc::now(),
            }),
            write_lock: Mutex::new(()),
            write_buffer: Mutex::new(BufWriter::with_capacity(self.write_buffer_size, Vec::new())),
            write_buffer_size: self.write_buffer_size,
            last_flush_time: RwLock::new(last_flush_time),
            read_cache: RwLock::new(HashMap::new()),
            read_cache_size: self.read_cache_size,
            prefetch_state: RwLock::new(PrefetchState {
                enabled: false,
                last_accessed_offset: self.base_offset,
                window_size,
                prefetched_data: HashMap::new(),
            }),
            index_cache: RwLock::new(IndexCache {
                offset_index_cache: HashMap::new(),
                timestamp_index_cache: HashMap::new(),
                cache_size,
            }),
        })
    }
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

        let object_path = Path::from(path);
        self.storage_provider.put(object_path, json).await?;

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
        let object_path = Path::from(path);

        let data = self.storage_provider.get(object_path).await?;

        let metadata: LogSegmentMetadata = serde_json::from_slice(&data)?;

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

    /// 批量写入消息
    pub async fn batch_append(
        &self,
        topic: &str,
        partition: u32,
        messages: &[Vec<u8>],
        timestamps: Option<&[DateTime<Utc>]>,
    ) -> Result<Vec<u64>> {
        // 获取活跃段
        let segment = self.get_active_segment(topic, partition).await?;

        // 检查是否需要滚动段
        if self.check_roll_segment(topic, partition).await? {
            let new_segment = self.roll_segment(topic, partition).await?;

            // 使用新段进行批量写入
            return self.batch_append_to_segment(&new_segment, messages, timestamps).await;
        }

        // 使用当前段进行批量写入
        self.batch_append_to_segment(&segment, messages, timestamps).await
    }

    /// 批量写入消息到指定段
    async fn batch_append_to_segment(
        &self,
        segment: &OptimizedLogSegment,
        messages: &[Vec<u8>],
        timestamps: Option<&[DateTime<Utc>]>,
    ) -> Result<Vec<u64>> {
        // 获取写入锁
        let _lock = segment.write_lock.lock().await;

        // 检查段状态
        {
            let metadata = segment.metadata.read().await;
            if metadata.state != LogSegmentState::Active {
                return Err(anyhow!("Cannot write to non-active segment"));
            }
        }

        // 确保数据文件存在
        let segment_path = format!("{}.data", segment.segment_id);
        let object_path = Path::from(segment_path.clone());
        let exists = segment.storage_provider.exists(segment_path.clone()).await.unwrap_or(false);
        if !exists {
            // 创建空文件
            segment.storage_provider.put(object_path.clone(), Vec::new()).await?;
        }

        // 获取当前偏移量
        let current_offset = {
            let metadata = segment.metadata.read().await;
            metadata.last_offset
        };

        // 准备批量写入
        let mut offsets = Vec::with_capacity(messages.len());
        let mut total_size = 0;
        let mut positions = Vec::with_capacity(messages.len());
        let mut new_offset_index_entries = Vec::new();
        let mut new_time_index_entries = Vec::new();

        // 获取写入缓冲区
        let mut write_buffer = segment.write_buffer.lock().await;
        let mut buffer_position = write_buffer.get_ref().len() as u64;

        // 批量写入消息
        for (i, data) in messages.iter().enumerate() {
            let offset = current_offset + i as u64 + 1;
            offsets.push(offset);

            // 记录位置
            let position = buffer_position;
            positions.push(position);

            // 获取时间戳
            let timestamp = if let Some(ts) = timestamps {
                if i < ts.len() {
                    Some(ts[i])
                } else {
                    None
                }
            } else {
                None
            };

            // 写入长度前缀
            let len = data.len() as u32;
            write_buffer.write_all(&len.to_le_bytes())?;
            buffer_position += 4;

            // 写入时间戳
            let ts = timestamp.unwrap_or_else(Utc::now).timestamp_millis();
            write_buffer.write_all(&ts.to_le_bytes())?;
            buffer_position += 8;

            // 写入数据
            write_buffer.write_all(data)?;
            buffer_position += data.len() as u64;

            // 更新总大小
            total_size += (4 + 8 + data.len()) as u64;

            // 检查是否需要创建索引条目
            let index = segment.index.read().await;

            // 更新偏移量索引
            if index.offset_index.is_empty() ||
               (offset - index.offset_index.last().unwrap().offset) >= segment.config.index_interval_entries as u64 {
                new_offset_index_entries.push(IndexEntry {
                    offset,
                    position,
                });
            }

            // 更新时间索引
            if let Some(ts) = timestamp {
                let ts_millis = ts.timestamp_millis();

                if index.time_index.is_empty() ||
                   (ts_millis - index.time_index.last().unwrap().timestamp) >= segment.config.time_index_interval_ms as i64 {
                    new_time_index_entries.push(TimeIndexEntry {
                        timestamp: ts_millis,
                        offset,
                    });
                }
            }
        }

        // 如果缓冲区超过阈值，刷新到存储
        if write_buffer.get_ref().len() >= segment.write_buffer_size {
            segment.flush_internal(&mut write_buffer).await?;
        }

        // 更新索引
        if !new_offset_index_entries.is_empty() || !new_time_index_entries.is_empty() {
            let mut index = segment.index.write().await;

            // 添加新的偏移量索引条目
            index.offset_index.extend(new_offset_index_entries);

            // 添加新的时间索引条目
            index.time_index.extend(new_time_index_entries);

            index.last_updated_at = Utc::now();
        }

        // 更新索引缓存
        {
            let mut index_cache = segment.index_cache.write().await;

            // 更新偏移量索引缓存
            for (i, offset) in offsets.iter().enumerate() {
                index_cache.offset_index_cache.insert(*offset, positions[i]);

                // 如果缓存过大，移除最旧的条目
                if index_cache.offset_index_cache.len() > index_cache.cache_size {
                    if let Some(oldest_offset) = index_cache.offset_index_cache.keys().min().cloned() {
                        index_cache.offset_index_cache.remove(&oldest_offset);
                    }
                }
            }

            // 更新时间戳索引缓存
            if let Some(ts) = timestamps {
                for (i, timestamp) in ts.iter().enumerate().take(messages.len()) {
                    let ts_millis = timestamp.timestamp_millis();
                    index_cache.timestamp_index_cache.insert(ts_millis, offsets[i]);

                    // 如果缓存过大，移除最旧的条目
                    if index_cache.timestamp_index_cache.len() > index_cache.cache_size {
                        if let Some(oldest_ts) = index_cache.timestamp_index_cache.keys().min().cloned() {
                            index_cache.timestamp_index_cache.remove(&oldest_ts);
                        }
                    }
                }
            }
        }

        // 更新元数据
        {
            let mut metadata = segment.metadata.write().await;
            metadata.last_offset = current_offset + messages.len() as u64;
            metadata.last_modified_at = Utc::now();
            metadata.size_bytes += total_size;
            metadata.message_count += messages.len() as u64;

            // 更新时间戳范围
            if let Some(ts) = timestamps {
                if !ts.is_empty() {
                    // 找到最小时间戳
                    if let Some(min_ts) = ts.iter().min() {
                        if metadata.min_timestamp.is_none() || metadata.min_timestamp.unwrap() > *min_ts {
                            metadata.min_timestamp = Some(*min_ts);
                        }
                    }

                    // 找到最大时间戳
                    if let Some(max_ts) = ts.iter().max() {
                        if metadata.max_timestamp.is_none() || metadata.max_timestamp.unwrap() < *max_ts {
                            metadata.max_timestamp = Some(*max_ts);
                        }
                    }
                }
            }
        }

        Ok(offsets)
    }

    /// 批量读取消息
    pub async fn batch_read(
        &self,
        topic: &str,
        partition: u32,
        offsets: &[u64],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut results = vec![None; offsets.len()];

        // 为每个偏移量单独读取
        for (i, &offset) in offsets.iter().enumerate() {
            match self.get_segment(topic, partition, offset).await {
                Ok(segment) => {
                    match segment.read(offset).await {
                        Ok(data) => results[i] = Some(data),
                        Err(_) => results[i] = None,
                    }
                },
                Err(_) => {
                    // 如果找不到段，保持为 None
                }
            }
        }

        Ok(results)
    }

    /// 按时间范围读取消息
    pub async fn read_by_time_range(
        &self,
        topic: &str,
        partition: u32,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        max_messages: Option<usize>,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let start_millis = start_time.timestamp_millis();
        let end_millis = end_time.timestamp_millis();

        // 获取所有段
        let mut segments = Vec::new();

        // 获取活跃段
        {
            let active_segments = self.active_segments.read().await;
            if let Some(segment) = active_segments.get(&(topic.to_string(), partition)) {
                // 确保数据文件存在
                let segment_path = format!("{}.data", segment.segment_id);
                let exists = segment.storage_provider.exists(segment_path.clone()).await.unwrap_or(false);
                if exists {
                    segments.push(segment.clone());
                }
            }
        }

        // 获取只读段
        {
            let readonly_segments = self.readonly_segments.read().await;
            if let Some(segment_list) = readonly_segments.get(&(topic.to_string(), partition)) {
                for segment in segment_list {
                    // 确保数据文件存在
                    let segment_path = format!("{}.data", segment.segment_id);
                    let exists = segment.storage_provider.exists(segment_path.clone()).await.unwrap_or(false);
                    if exists {
                        segments.push(segment.clone());
                    }
                }
            }
        }

        // 过滤时间范围内的段
        let mut filtered_segments = Vec::new();
        for segment in &segments {
            let metadata = segment.metadata.read().await;

            // 如果段没有时间戳信息，跳过
            if metadata.min_timestamp.is_none() || metadata.max_timestamp.is_none() {
                continue;
            }

            let min_ts = metadata.min_timestamp.unwrap().timestamp_millis();
            let max_ts = metadata.max_timestamp.unwrap().timestamp_millis();

            // 检查段的时间范围是否与查询范围重叠
            if min_ts <= end_millis && max_ts >= start_millis {
                filtered_segments.push(segment.clone());
            }
        }

        // 按基础偏移量排序段
        filtered_segments.sort_by_key(|s| s.base_offset);

        let mut results = Vec::new();
        let max_count = max_messages.unwrap_or(usize::MAX);

        // 从每个段中读取符合时间范围的消息
        for segment in filtered_segments {
            // 如果已经达到最大消息数，停止读取
            if results.len() >= max_count {
                break;
            }

            // 获取段的索引
            let index = segment.index.read().await;

            // 找到时间范围内的偏移量
            let mut offsets_to_read = Vec::new();

            // 二分查找开始时间
            let mut start_idx = 0;
            let mut end_idx = index.time_index.len();

            while start_idx < end_idx {
                let mid = (start_idx + end_idx) / 2;
                let entry = &index.time_index[mid];

                if entry.timestamp < start_millis {
                    start_idx = mid + 1;
                } else {
                    end_idx = mid;
                }
            }

            // 收集时间范围内的所有偏移量
            for i in start_idx..index.time_index.len() {
                let entry = &index.time_index[i];

                if entry.timestamp > end_millis {
                    break;
                }

                offsets_to_read.push(entry.offset);

                // 如果已经达到最大消息数，停止收集
                if offsets_to_read.len() + results.len() >= max_count {
                    break;
                }
            }

            // 读取消息
            for offset in offsets_to_read {
                if let Ok(data) = segment.read(offset).await {
                    results.push((offset, data));

                    // 如果已经达到最大消息数，停止读取
                    if results.len() >= max_count {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }
}
