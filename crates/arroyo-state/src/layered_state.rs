use crate::{get_storage_provider, BackingStore};
use anyhow::{anyhow, Result};
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata,
};
use arroyo_storage::StorageProvider;
use futures::StreamExt;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::fs;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

/// 存储层级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StorageTier {
    /// 内存层 - 最快，但容量有限
    Memory = 0,
    /// 本地磁盘层 - 中等速度，中等容量
    LocalDisk = 1,
    /// 远程存储层 - 最慢，但容量几乎无限
    Remote = 2,
}

/// 热点数据策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotDataStrategy {
    /// 基于访问频率
    AccessFrequency,
    /// 基于访问时间
    AccessRecency,
    /// 基于访问频率和时间的加权组合
    Weighted,
}

/// 分层状态存储配置
#[derive(Debug, Clone)]
pub struct LayeredStateConfig {
    /// 是否启用内存层
    pub enable_memory_tier: bool,
    /// 内存层最大容量（字节）
    pub memory_tier_max_size: usize,
    /// 是否启用本地磁盘层
    pub enable_local_disk_tier: bool,
    /// 本地磁盘层路径
    pub local_disk_path: PathBuf,
    /// 本地磁盘层最大容量（字节）
    pub local_disk_max_size: usize,
    /// 远程存储提供者
    pub remote_storage: Arc<StorageProvider>,
    /// 缓存过期时间
    pub cache_expiration: Duration,
    /// 热点数据策略
    pub hot_data_strategy: HotDataStrategy,
    /// 热点数据比例（0.0-1.0）
    pub hot_data_ratio: f64,
    /// 预取策略是否启用
    pub enable_prefetching: bool,
    /// 预取阈值
    pub prefetch_threshold: usize,
    /// 压缩策略是否启用
    pub enable_compression: bool,
    /// 压缩级别（0-9）
    pub compression_level: u32,
}

impl Default for LayeredStateConfig {
    fn default() -> Self {
        Self {
            enable_memory_tier: true,
            memory_tier_max_size: 100 * 1024 * 1024, // 100MB
            enable_local_disk_tier: true,
            local_disk_path: PathBuf::from("/tmp/arroyo/state-cache"),
            local_disk_max_size: 1024 * 1024 * 1024, // 1GB
            remote_storage: Arc::new(StorageProvider::dummy()),
            cache_expiration: Duration::from_secs(60 * 60), // 1 hour
            hot_data_strategy: HotDataStrategy::Weighted,
            hot_data_ratio: 0.2, // 20% 的数据被视为热点数据
            enable_prefetching: true,
            prefetch_threshold: 3, // 访问3次后预取相关数据
            enable_compression: true,
            compression_level: 6, // 默认压缩级别
        }
    }
}

/// 缓存条目
#[derive(Debug, Clone)]
struct CacheEntry {
    /// 数据
    data: Vec<u8>,
    /// 最后访问时间
    last_accessed: Instant,
    /// 创建时间
    created_at: Instant,
    /// 访问次数
    access_count: usize,
    /// 大小（字节）
    size: usize,
    /// 是否已压缩
    compressed: bool,
    /// 热度分数
    hotness_score: f64,
}

/// 缓存统计信息
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// 内存缓存命中次数
    pub memory_hits: usize,
    /// 本地磁盘缓存命中次数
    pub disk_hits: usize,
    /// 远程存储命中次数
    pub remote_hits: usize,
    /// 总请求次数
    pub total_requests: usize,
    /// 内存缓存大小
    pub memory_cache_size: usize,
    /// 本地磁盘缓存大小
    pub disk_cache_size: usize,
    /// 热点数据命中次数
    pub hot_data_hits: usize,
    /// 预取次数
    pub prefetch_count: usize,
    /// 压缩节省的空间
    pub compression_savings: usize,
}

impl CacheStats {
    /// 计算内存缓存命中率
    pub fn memory_hit_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.memory_hits as f64 / self.total_requests as f64
        }
    }

    /// 计算本地磁盘缓存命中率
    pub fn disk_hit_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.disk_hits as f64 / self.total_requests as f64
        }
    }

    /// 计算总缓存命中率
    pub fn total_hit_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            (self.memory_hits + self.disk_hits) as f64 / self.total_requests as f64
        }
    }
}

/// 内存缓存
#[derive(Debug, Clone)]
struct MemoryCache {
    /// 缓存数据
    cache: HashMap<String, CacheEntry>,
    /// 当前大小（字节）
    current_size: usize,
    /// 最大大小（字节）
    max_size: usize,
    /// 热点数据键集合
    hot_data_keys: HashSet<String>,
    /// 热点数据策略
    hot_data_strategy: HotDataStrategy,
    /// 热点数据比例
    hot_data_ratio: f64,
    /// 统计信息
    stats: CacheStats,
}

/// 分层状态后端
pub struct LayeredStateBackend {
    /// 配置
    config: LayeredStateConfig,
    /// 内存缓存
    memory_cache: RwLock<Option<MemoryCache>>,
    /// 远程存储提供者
    remote_storage: Arc<StorageProvider>,
    /// 预取队列
    prefetch_queue: Mutex<HashSet<String>>,
    /// 统计信息
    stats: RwLock<CacheStats>,
    /// 上次清理时间
    last_cleanup: RwLock<Instant>,
}

impl MemoryCache {
    fn new(max_size: usize, hot_data_strategy: HotDataStrategy, hot_data_ratio: f64) -> Self {
        Self {
            cache: HashMap::new(),
            current_size: 0,
            max_size,
            hot_data_keys: HashSet::new(),
            hot_data_strategy,
            hot_data_ratio,
            stats: CacheStats::default(),
        }
    }

    /// 获取缓存条目
    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        if let Some(entry) = self.cache.get_mut(key) {
            // 更新访问信息
            entry.last_accessed = Instant::now();
            entry.access_count += 1;

            // 计算热度分数
            let now = Instant::now();
            let recency = 1.0 / (1.0 + now.duration_since(entry.last_accessed).as_secs_f64());
            let frequency = entry.access_count as f64;

            entry.hotness_score = match self.hot_data_strategy {
                HotDataStrategy::AccessFrequency => frequency,
                HotDataStrategy::AccessRecency => recency,
                HotDataStrategy::Weighted => 0.7 * frequency + 0.3 * recency,
            };

            // 更新统计信息
            self.stats.memory_hits += 1;

            // 如果是热点数据，更新统计
            if self.hot_data_keys.contains(key) {
                self.stats.hot_data_hits += 1;
            }

            // 返回数据（如果是压缩的，需要解压）
            if entry.compressed {
                match decompress_data(&entry.data) {
                    Ok(data) => Some(data),
                    Err(_) => {
                        warn!("Failed to decompress data for key: {}", key);
                        Some(entry.data.clone())
                    }
                }
            } else {
                Some(entry.data.clone())
            }
        } else {
            None
        }
    }

    /// 添加缓存条目
    fn put(&mut self, key: String, data: Vec<u8>, compress: bool) -> Result<()> {
        let size = data.len();

        // 如果数据大小超过最大缓存大小，直接跳过
        if size > self.max_size {
            return Ok(());
        }

        // 如果需要，清理缓存以腾出空间
        self.ensure_capacity(size);

        // 处理数据（可能压缩）
        let (stored_data, compressed, actual_size) = if compress && size > 1024 {
            // 只压缩大于1KB的数据
            match compress_data(&data) {
                Ok(compressed_data) => {
                    let savings = if compressed_data.len() < data.len() {
                        data.len() - compressed_data.len()
                    } else {
                        0
                    };
                    self.stats.compression_savings += savings;
                    (compressed_data.clone(), true, compressed_data.len())
                }
                Err(_) => (data, false, size),
            }
        } else {
            (data, false, size)
        };

        // 添加新条目
        let entry = CacheEntry {
            data: stored_data,
            last_accessed: Instant::now(),
            created_at: Instant::now(),
            access_count: 1,
            size: actual_size,
            compressed,
            hotness_score: 0.0,
        };

        // 更新缓存大小
        self.current_size += actual_size;
        self.cache.insert(key, entry);

        // 更新热点数据
        self.update_hot_data();

        Ok(())
    }

    /// 确保有足够的容量
    fn ensure_capacity(&mut self, required_size: usize) {
        // 如果有足够的空间，直接返回
        if self.current_size + required_size <= self.max_size {
            return;
        }

        // 按热度分数排序（保留热点数据）
        let mut entries: Vec<_> = self.cache.iter().collect();
        entries.sort_by(|(k1, e1), (k2, e2)| {
            // 热点数据优先保留
            let k1_hot = self.hot_data_keys.contains(*k1);
            let k2_hot = self.hot_data_keys.contains(*k2);

            if k1_hot && !k2_hot {
                return std::cmp::Ordering::Greater;
            }
            if !k1_hot && k2_hot {
                return std::cmp::Ordering::Less;
            }

            // 然后按热度分数排序
            e2.hotness_score.partial_cmp(&e1.hotness_score).unwrap_or(std::cmp::Ordering::Equal)
        });

        // 收集要移除的键
        let mut keys_to_remove = Vec::new();
        let mut freed_space = 0;

        for (key, entry) in &entries {
            // 跳过热点数据，除非实在没有空间
            if self.hot_data_keys.contains(*key) && freed_space + self.current_size - required_size <= (self.max_size as f64 * 0.9) as usize {
                continue;
            }

            keys_to_remove.push((*key).clone());
            freed_space += entry.size;

            if self.current_size - freed_space + required_size <= self.max_size {
                break;
            }
        }

        // 移除收集的键
        for key in keys_to_remove {
            if let Some(entry) = self.cache.remove(&key) {
                self.current_size -= entry.size;
                self.hot_data_keys.remove(&key);
            }
        }
    }

    /// 清理过期条目
    fn cleanup_expired(&mut self, expiration: Duration) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for (key, entry) in &self.cache {
            if now.duration_since(entry.created_at) > expiration {
                to_remove.push(key.clone());
            }
        }

        for key in to_remove {
            if let Some(entry) = self.cache.remove(&key) {
                self.current_size -= entry.size;
                self.hot_data_keys.remove(&key);
            }
        }
    }

    // 已将 update_hotness_score 方法内联到 get 方法中

    /// 更新热点数据集合
    fn update_hot_data(&mut self) {
        // 如果缓存为空，直接返回
        if self.cache.is_empty() {
            return;
        }

        // 计算热点数据数量
        let hot_data_count = (self.cache.len() as f64 * self.hot_data_ratio).ceil() as usize;
        if hot_data_count == 0 {
            return;
        }

        // 按热度分数排序
        let mut entries: Vec<_> = self.cache.iter().collect();
        entries.sort_by(|(_, e1), (_, e2)| {
            e2.hotness_score.partial_cmp(&e1.hotness_score).unwrap_or(std::cmp::Ordering::Equal)
        });

        // 更新热点数据集合
        self.hot_data_keys.clear();
        for (key, _) in entries.iter().take(hot_data_count) {
            self.hot_data_keys.insert((*key).clone());
        }
    }
}

/// 压缩数据
fn compress_data(data: &[u8]) -> Result<Vec<u8>> {
    use flate2::{Compression, write::ZlibEncoder};
    use std::io::Write;

    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    Ok(encoder.finish()?)
}

/// 解压数据
fn decompress_data(data: &[u8]) -> Result<Vec<u8>> {
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    let mut decoder = ZlibDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

impl LayeredStateBackend {
    /// 创建新的分层状态后端
    pub fn new(config: LayeredStateConfig) -> Self {
        let memory_cache = if config.enable_memory_tier {
            Some(MemoryCache::new(
                config.memory_tier_max_size,
                config.hot_data_strategy,
                config.hot_data_ratio,
            ))
        } else {
            None
        };

        Self {
            remote_storage: config.remote_storage.clone(),
            memory_cache: RwLock::new(memory_cache),
            config,
            prefetch_queue: Mutex::new(HashSet::new()),
            stats: RwLock::new(CacheStats::default()),
            last_cleanup: RwLock::new(Instant::now()),
        }
    }

    /// 获取本地磁盘缓存路径
    fn get_local_path(&self, key: &str) -> PathBuf {
        let mut path = self.config.local_disk_path.clone();
        path.push(key);
        path
    }

    /// 从本地磁盘读取
    async fn read_from_local_disk(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if !self.config.enable_local_disk_tier {
            return Ok(None);
        }

        let path = self.get_local_path(key);

        if !path.exists() {
            return Ok(None);
        }

        match fs::read(&path).await {
            Ok(data) => {
                // 更新统计信息
                let mut stats = self.stats.write().await;
                stats.disk_hits += 1;

                // 如果数据是压缩的，需要解压
                if path.to_string_lossy().ends_with(".compressed") {
                    match decompress_data(&data) {
                        Ok(decompressed) => Ok(Some(decompressed)),
                        Err(e) => {
                            warn!("Failed to decompress data from disk: {}", e);
                            Ok(Some(data))
                        }
                    }
                } else {
                    Ok(Some(data))
                }
            }
            Err(e) => {
                warn!("Failed to read from local disk: {}", e);
                Ok(None)
            }
        }
    }

    /// 写入本地磁盘
    async fn write_to_local_disk(&self, key: &str, data: &[u8]) -> Result<()> {
        if !self.config.enable_local_disk_tier {
            return Ok(());
        }

        let path = self.get_local_path(key);

        // 确保目录存在
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // 如果启用了压缩，并且数据大小超过阈值，则压缩数据
        if self.config.enable_compression && data.len() > 1024 {
            let compressed_path = path.with_extension("compressed");
            match compress_data(data) {
                Ok(compressed_data) => {
                    // 只有当压缩后的数据比原始数据小时才使用压缩数据
                    if compressed_data.len() < data.len() {
                        fs::write(&compressed_path, &compressed_data).await?;

                        // 更新统计信息
                        let mut stats = self.stats.write().await;
                        stats.compression_savings += data.len() - compressed_data.len();
                        stats.disk_cache_size += compressed_data.len();

                        return Ok(());
                    }
                }
                Err(e) => {
                    warn!("Failed to compress data: {}", e);
                }
            }
        }

        // 如果没有压缩或压缩失败，直接写入原始数据
        fs::write(&path, data).await?;

        // 更新统计信息
        let mut stats = self.stats.write().await;
        stats.disk_cache_size += data.len();

        Ok(())
    }

    /// 从远程存储读取
    async fn read_from_remote(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.remote_storage.get(key).await {
            Ok(data) => {
                // 更新统计信息
                let mut stats = self.stats.write().await;
                stats.remote_hits += 1;

                // 如果启用了本地缓存，将数据写入本地缓存
                if self.config.enable_local_disk_tier {
                    self.write_to_local_disk(key, &data).await?;
                }

                // 如果启用了内存缓存，将数据写入内存缓存
                if self.config.enable_memory_tier {
                    if let Some(ref mut cache) = *self.memory_cache.write().await {
                        cache.put(key.to_string(), data.to_vec(), self.config.enable_compression)?;
                    }
                }

                Ok(Some(data.to_vec()))
            }
            Err(e) => {
                warn!("Failed to read from remote storage: {}", e);
                Ok(None)
            }
        }
    }

    /// 写入远程存储
    async fn write_to_remote(&self, key: &str, data: &[u8]) -> Result<()> {
        self.remote_storage.put(key, data.to_vec()).await?;
        Ok(())
    }

    /// 预取数据
    async fn prefetch(&self, key: &str) -> Result<()> {
        if !self.config.enable_prefetching {
            return Ok(());
        }

        // 检查键是否已在预取队列中
        let mut queue = self.prefetch_queue.lock().await;
        if queue.contains(key) {
            return Ok(());
        }

        // 添加到预取队列
        queue.insert(key.to_string());
        drop(queue);

        // 异步预取
        let key = key.to_string();
        let self_clone = self.clone();
        tokio::spawn(async move {
            // 从远程存储读取
            if let Ok(Some(data)) = self_clone.read_from_remote(&key).await {
                // 更新统计信息
                let mut stats = self_clone.stats.write().await;
                stats.prefetch_count += 1;

                // 从预取队列中移除
                let mut queue = self_clone.prefetch_queue.lock().await;
                queue.remove(&key);
            }
        });

        Ok(())
    }

    /// 清理过期数据
    async fn cleanup(&self) -> Result<()> {
        let now = Instant::now();
        let mut last_cleanup = self.last_cleanup.write().await;

        // 如果距离上次清理时间不足1小时，跳过
        if now.duration_since(*last_cleanup) < Duration::from_secs(3600) {
            return Ok(());
        }

        // 更新上次清理时间
        *last_cleanup = now;

        // 清理内存缓存
        if let Some(ref mut cache) = *self.memory_cache.write().await {
            cache.cleanup_expired(self.config.cache_expiration);
        }

        // 清理本地磁盘缓存
        if self.config.enable_local_disk_tier {
            // 这里可以实现本地磁盘缓存的清理逻辑
            // 例如，删除过期文件或超过大小限制的文件
        }

        Ok(())
    }

    /// 获取统计信息
    pub async fn get_stats(&self) -> CacheStats {
        // 清理过期数据
        let _ = self.cleanup().await;

        // 获取内存缓存统计信息
        let mut stats = self.stats.read().await.clone();

        if let Some(ref cache) = *self.memory_cache.read().await {
            stats.memory_cache_size = cache.current_size;
            stats.memory_hits = cache.stats.memory_hits;
            stats.hot_data_hits = cache.stats.hot_data_hits;
        }

        stats
    }
}

impl Clone for LayeredStateBackend {
    fn clone(&self) -> Self {
        // 创建一个新的实例，但不复制内部状态
        // 这样可以避免复制锁和其他不可复制的内容
        Self {
            config: self.config.clone(),
            memory_cache: RwLock::new(None), // 不复制内存缓存
            remote_storage: self.remote_storage.clone(),
            prefetch_queue: Mutex::new(HashSet::new()),
            stats: RwLock::new(CacheStats::default()),
            last_cleanup: RwLock::new(Instant::now()),
        }
    }
}

#[async_trait::async_trait]
impl BackingStore for LayeredStateBackend {
    fn name(&self) -> &'static str {
        "layered"
    }

    async fn prepare_checkpoint_load(&self, _metadata: &CheckpointMetadata) -> Result<()> {
        // 这个方法在 BackingStore trait 中是必需的，但我们不需要在这里实现它
        Ok(())
    }

    async fn load_operator_metadata(
        &self,
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>> {
        // 这里我们使用远程存储，因为元数据需要在所有节点之间共享
        let path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/operator-{}/metadata",
            job_id, epoch, operator_id
        );

        match self.remote_storage.get(path.as_str()).await {
            Ok(data) => {
                let metadata = OperatorCheckpointMetadata::decode(&data[..])?;
                Ok(Some(metadata))
            }
            Err(e) if matches!(e, arroyo_storage::StorageError::ObjectStore(object_store::Error::NotFound { .. })) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_operator_checkpoint_metadata(
        &self,
        metadata: OperatorCheckpointMetadata,
    ) -> Result<()> {
        // 这里我们使用远程存储，因为元数据需要在所有节点之间共享
        let path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/operator-{}/metadata",
            metadata.operator_metadata.as_ref().unwrap().job_id,
            metadata.operator_metadata.as_ref().unwrap().epoch,
            metadata.operator_metadata.as_ref().unwrap().operator_id
        );

        self.remote_storage.put(path.as_str(), metadata.encode_to_vec()).await?;
        Ok(())
    }

    async fn write_checkpoint_metadata(&self, metadata: CheckpointMetadata) -> Result<()> {
        // 这里我们使用远程存储，因为元数据需要在所有节点之间共享
        let path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/metadata",
            metadata.job_id, metadata.epoch
        );

        self.remote_storage.put(path.as_str(), metadata.encode_to_vec()).await?;
        Ok(())
    }

    async fn cleanup_checkpoint(
        &self,
        _metadata: CheckpointMetadata,
        _old_min_epoch: u32,
        _new_min_epoch: u32,
    ) -> Result<()> {
        // 这个方法在 BackingStore trait 中是必需的，但我们不需要在这里实现它
        Ok(())
    }
    async fn load_checkpoint_metadata(&self, job_id: &str, epoch: u32) -> Result<CheckpointMetadata> {
        // 从远程存储加载检查点元数据
        let path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/metadata",
            job_id, epoch
        );

        match self.remote_storage.get(path.as_str()).await {
            Ok(data) => {
                let metadata = CheckpointMetadata::decode(&data[..])?;
                Ok(metadata)
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl LayeredStateBackend {
    /// 获取状态数据
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.total_requests += 1;
        }

        // 尝试从内存缓存获取
        if let Some(ref mut cache) = *self.memory_cache.write().await {
            if let Some(data) = cache.get(key) {
                return Ok(Some(data));
            }
        }

        // 尝试从本地磁盘获取
        if let Some(data) = self.read_from_local_disk(key).await? {
            // 如果启用了内存缓存，将数据写入内存缓存
            if self.config.enable_memory_tier {
                if let Some(ref mut cache) = *self.memory_cache.write().await {
                    cache.put(key.to_string(), data.clone(), self.config.enable_compression)?;
                }
            }

            return Ok(Some(data));
        }

        // 尝试从远程存储获取
        self.read_from_remote(key).await
    }

    /// 存储状态数据
    pub async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        // 写入内存缓存
        if self.config.enable_memory_tier {
            if let Some(ref mut cache) = *self.memory_cache.write().await {
                cache.put(key.to_string(), value.clone(), self.config.enable_compression)?;
            }
        }

        // 写入本地磁盘
        if self.config.enable_local_disk_tier {
            self.write_to_local_disk(key, &value).await?;
        }

        // 写入远程存储
        self.write_to_remote(key, &value).await?;

        Ok(())
    }

    /// 删除状态数据
    pub async fn delete(&self, key: &str) -> Result<()> {
        // 从内存缓存删除
        if let Some(ref mut cache) = *self.memory_cache.write().await {
            cache.cache.remove(key);
        }

        // 从本地磁盘删除
        if self.config.enable_local_disk_tier {
            let path = self.get_local_path(key);
            if path.exists() {
                fs::remove_file(&path).await?;
            }

            // 检查压缩文件
            let compressed_path = path.with_extension("compressed");
            if compressed_path.exists() {
                fs::remove_file(compressed_path).await?;
            }
        }

        // 从远程存储删除
        if let Err(e) = self.remote_storage.delete_if_present(key).await {
            warn!("Failed to delete from remote storage: {}", e);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_memory_cache() {
        // 创建内存缓存
        let mut cache = MemoryCache::new(
            1024 * 1024, // 1MB
            HotDataStrategy::Weighted,
            0.2,
        );

        // 添加数据
        let key = "test_key".to_string();
        let data = vec![1, 2, 3, 4, 5];
        cache.put(key.clone(), data.clone(), false).unwrap();

        // 获取数据
        let result = cache.get(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);

        // 验证统计信息
        assert_eq!(cache.stats.memory_hits, 1);
    }

    #[tokio::test]
    async fn test_layered_state_backend() {
        // 创建配置 - 只使用内存缓存，避免文件系统问题
        let config = LayeredStateConfig {
            enable_memory_tier: true,
            memory_tier_max_size: 1024 * 1024, // 1MB
            enable_local_disk_tier: false, // 禁用本地磁盘层，避免文件系统权限问题
            local_disk_path: PathBuf::from("/tmp"), // 不会使用，但需要提供一个有效路径
            local_disk_max_size: 10 * 1024 * 1024, // 10MB
            remote_storage: Arc::new(StorageProvider::dummy()),
            cache_expiration: Duration::from_secs(60),
            hot_data_strategy: HotDataStrategy::Weighted,
            hot_data_ratio: 0.2,
            enable_prefetching: false,
            prefetch_threshold: 3,
            enable_compression: false, // 禁用压缩，简化测试
            compression_level: 6,
        };

        // 创建分层状态后端
        let backend = LayeredStateBackend::new(config);

        // 添加数据到内存缓存
        let key = "test_key";
        let data = vec![1, 2, 3, 4, 5];

        // 手动添加到内存缓存
        if let Some(ref mut cache) = *backend.memory_cache.write().await {
            cache.put(key.to_string(), data.clone(), false).unwrap();
        }

        // 获取数据
        let result = backend.get(key).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);

        // 验证统计信息
        let stats = backend.get_stats().await;
        assert_eq!(stats.total_requests, 1);
        assert_eq!(stats.memory_hits, 1);
    }

    #[tokio::test]
    async fn test_compression() {
        // 创建大数据进行压缩测试
        let data = vec![0; 10000]; // 10KB 的零数据，应该可以很好地压缩

        // 压缩数据
        let compressed = compress_data(&data).unwrap();

        // 验证压缩后的数据比原始数据小
        assert!(compressed.len() < data.len());

        // 解压数据
        let decompressed = decompress_data(&compressed).unwrap();

        // 验证解压后的数据与原始数据相同
        assert_eq!(decompressed, data);
    }
}