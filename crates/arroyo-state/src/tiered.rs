use crate::{get_storage_provider, BackingStore};
use anyhow::Result;
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata,
};
use arroyo_storage::StorageProvider;
use prost::Message;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::sync::RwLock;
use tracing::warn;

/// 存储层级
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    /// 内存层
    Memory,
    /// 本地磁盘层
    LocalDisk,
    /// 远程存储层
    Remote,
}

/// 分层存储配置
#[derive(Debug, Clone)]
pub struct TieredStorageConfig {
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
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            enable_memory_tier: true,
            memory_tier_max_size: 100 * 1024 * 1024, // 100MB
            enable_local_disk_tier: true,
            local_disk_path: PathBuf::from("/tmp/arroyo/state-cache"),
            local_disk_max_size: 1024 * 1024 * 1024, // 1GB
            remote_storage: Arc::new(StorageProvider::dummy()),
            cache_expiration: Duration::from_secs(60 * 60), // 1 hour
        }
    }
}

/// 缓存条目
#[derive(Debug)]
struct CacheEntry {
    /// 数据
    data: Vec<u8>,
    /// 最后访问时间
    last_accessed: SystemTime,
    /// 创建时间
    created_at: SystemTime,
    /// 大小（字节）
    size: usize,
}

/// 内存缓存
#[derive(Debug)]
struct MemoryCache {
    /// 缓存数据
    cache: HashMap<String, CacheEntry>,
    /// 当前大小（字节）
    current_size: usize,
    /// 最大大小（字节）
    max_size: usize,
}

impl MemoryCache {
    fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::new(),
            current_size: 0,
            max_size,
        }
    }

    /// 获取缓存条目
    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        if let Some(entry) = self.cache.get_mut(key) {
            entry.last_accessed = SystemTime::now();
            Some(entry.data.clone())
        } else {
            None
        }
    }

    /// 添加缓存条目
    fn put(&mut self, key: String, data: Vec<u8>) {
        let size = data.len();

        // 如果数据大小超过最大缓存大小，直接跳过
        if size > self.max_size {
            return;
        }

        // 如果需要，清理缓存以腾出空间
        self.ensure_capacity(size);

        // 添加新条目
        let entry = CacheEntry {
            data,
            last_accessed: SystemTime::now(),
            created_at: SystemTime::now(),
            size,
        };

        // 更新缓存大小
        self.current_size += size;
        self.cache.insert(key, entry);
    }

    /// 确保有足够的容量
    fn ensure_capacity(&mut self, required_size: usize) {
        // 如果有足够的空间，直接返回
        if self.current_size + required_size <= self.max_size {
            return;
        }

        // 按最后访问时间排序
        let mut entries: Vec<_> = self.cache.iter().collect();
        entries.sort_by_key(|(_, entry)| entry.last_accessed);

        // 移除最旧的条目，直到有足够的空间
        for (key, entry) in entries {
            self.current_size -= entry.size;
            self.cache.remove(key);

            if self.current_size + required_size <= self.max_size {
                break;
            }
        }
    }

    /// 清理过期条目
    fn cleanup_expired(&mut self, expiration: Duration) {
        let now = SystemTime::now();
        let mut to_remove = Vec::new();

        for (key, entry) in &self.cache {
            if now.duration_since(entry.created_at).unwrap_or_default() > expiration {
                to_remove.push(key.clone());
            }
        }

        for key in to_remove {
            if let Some(entry) = self.cache.remove(&key) {
                self.current_size -= entry.size;
            }
        }
    }
}

/// 分层状态后端
pub struct TieredStateBackend {
    /// 配置
    config: TieredStorageConfig,
    /// 内存缓存
    memory_cache: RwLock<Option<MemoryCache>>,
    /// 远程存储提供者
    remote_storage: Arc<StorageProvider>,
}

impl TieredStateBackend {
    /// 创建新的分层状态后端
    pub fn new(config: TieredStorageConfig) -> Self {
        let memory_cache = if config.enable_memory_tier {
            Some(MemoryCache::new(config.memory_tier_max_size))
        } else {
            None
        };

        Self {
            remote_storage: config.remote_storage.clone(),
            memory_cache: RwLock::new(memory_cache),
            config,
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
        let path = self.get_local_path(key);

        if !path.exists() {
            return Ok(None);
        }

        match fs::read(&path).await {
            Ok(data) => {
                // 更新访问时间
                let now = SystemTime::now();
                let _ = fs::File::open(&path)
                    .await
                    .and_then(|file| file.set_modified(now));

                Ok(Some(data))
            }
            Err(e) => {
                warn!("Failed to read from local disk: {}", e);
                Ok(None)
            }
        }
    }

    /// 写入本地磁盘
    async fn write_to_local_disk(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.get_local_path(key);

        // 确保目录存在
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        fs::write(&path, data).await?;
        Ok(())
    }

    /// 清理过期的本地磁盘缓存
    async fn cleanup_local_disk(&self, expiration: Duration) -> Result<()> {
        if !self.config.enable_local_disk_tier {
            return Ok(());
        }

        let base_path = &self.config.local_disk_path;
        if !base_path.exists() {
            return Ok(());
        }

        let now = SystemTime::now();
        let mut entries = fs::read_dir(base_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_file() {
                if let Ok(modified) = metadata.modified() {
                    if now.duration_since(modified).unwrap_or_default() > expiration {
                        let _ = fs::remove_file(entry.path()).await;
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl BackingStore for TieredStateBackend {
    fn name() -> &'static str {
        "tiered"
    }

    async fn load_checkpoint_metadata(job_id: &str, epoch: u32) -> Result<CheckpointMetadata> {
        // 这里我们仍然使用远程存储，因为元数据需要在所有节点之间共享
        let storage_client = get_storage_provider().await?;
        let path = format!("{}/checkpoints/checkpoint-{:0>7}/metadata", job_id, epoch);
        let data = storage_client.get(path.as_str()).await?;
        let metadata = CheckpointMetadata::decode(&data[..])?;
        Ok(metadata)
    }

    async fn load_operator_metadata(
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>> {
        // 这里我们仍然使用远程存储，因为元数据需要在所有节点之间共享
        let storage_client = get_storage_provider().await?;
        let path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/operator-{}/metadata",
            job_id, epoch, operator_id
        );

        match storage_client.get(path.as_str()).await {
            Ok(data) => {
                let metadata = OperatorCheckpointMetadata::decode(&data[..])?;
                Ok(Some(metadata))
            }
            Err(arroyo_storage::StorageError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_operator_checkpoint_metadata(
        metadata: OperatorCheckpointMetadata,
    ) -> Result<()> {
        // 这里我们仍然使用远程存储，因为元数据需要在所有节点之间共享
        let storage_client = get_storage_provider().await?;
        let path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/operator-{}/metadata",
            metadata.operator_metadata.as_ref().unwrap().job_id,
            metadata.operator_metadata.as_ref().unwrap().epoch,
            metadata.operator_metadata.as_ref().unwrap().operator_id
        );

        storage_client.put(path.as_str(), metadata.encode_to_vec()).await?;
        Ok(())
    }

    async fn write_checkpoint_metadata(metadata: CheckpointMetadata) -> Result<()> {
        // 这里我们仍然使用远程存储，因为元数据需要在所有节点之间共享
        let storage_client = get_storage_provider().await?;
        let path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/metadata",
            metadata.job_id, metadata.epoch
        );

        storage_client.put(path.as_str(), metadata.encode_to_vec()).await?;
        Ok(())
    }

    async fn prepare_checkpoint_load(_metadata: &CheckpointMetadata) -> anyhow::Result<()> {
        Ok(())
    }

    async fn cleanup_checkpoint(
        metadata: CheckpointMetadata,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<()> {
        // 这里我们仍然使用远程存储，因为清理需要在所有节点之间协调
        let storage_client = get_storage_provider().await?;

        // 删除旧的检查点
        for epoch in old_min_epoch..new_min_epoch {
            let path = format!("{}/checkpoints/checkpoint-{:0>7}", metadata.job_id, epoch);
            let _ = storage_client.delete_prefix(path.as_str()).await;
        }

        Ok(())
    }
}
