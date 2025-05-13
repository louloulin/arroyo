use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::tables::{CompactionConfig, ErasedTable};
use crate::{get_storage_provider, BackingStore};
use anyhow::{bail, Result};
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, TableCheckpointMetadata,
};
use futures::stream::FuturesUnordered;
use futures::StreamExt;

use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::debug;
use tokio::sync::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;
use object_store::path::Path;

pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;
pub const GENERATIONS_TO_COMPACT: u32 = 1; // only compact generation 0 files

// 缓存大小配置
const DEFAULT_METADATA_CACHE_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(100) };
const DEFAULT_DATA_CACHE_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(50) };

/// 优化版的 ParquetBackend，提供更高性能的状态存储
pub struct OptimizedParquetBackend {
    // 元数据缓存，用于减少对存储的访问
    metadata_cache: Mutex<LruCache<String, Vec<u8>>>,
    // 数据缓存，用于减少对存储的访问
    data_cache: Mutex<LruCache<String, Vec<u8>>>,
    // 并行度，用于控制并行读写的数量
    parallelism: usize,
}

impl OptimizedParquetBackend {
    /// 创建一个新的 OptimizedParquetBackend 实例
    pub fn new(parallelism: Option<usize>) -> Self {
        let parallelism = parallelism.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4)
        });

        Self {
            metadata_cache: Mutex::new(LruCache::new(DEFAULT_METADATA_CACHE_SIZE)),
            data_cache: Mutex::new(LruCache::new(DEFAULT_DATA_CACHE_SIZE)),
            parallelism,
        }
    }

    /// 从缓存中获取数据，如果缓存中没有则从存储中获取并缓存
    async fn get_with_cache(&self, path: &str, is_metadata: bool) -> Result<Vec<u8>> {
        // 尝试从缓存中获取
        let cache_key = path.to_string();
        let mut cache = if is_metadata {
            self.metadata_cache.lock().await
        } else {
            self.data_cache.lock().await
        };

        if let Some(data) = cache.get(&cache_key) {
            return Ok(data.clone());
        }

        // 缓存中没有，从存储中获取
        let storage_client = get_storage_provider().await?;
        let data = storage_client.get(path).await?;

        // 将数据放入缓存
        cache.put(cache_key, data.to_vec());

        Ok(data.to_vec())
    }

    /// 将数据写入存储并更新缓存
    async fn put_with_cache(&self, path: &str, data: Vec<u8>, is_metadata: bool) -> Result<()> {
        // 更新缓存
        let cache_key = path.to_string();
        let mut cache = if is_metadata {
            self.metadata_cache.lock().await
        } else {
            self.data_cache.lock().await
        };

        cache.put(cache_key, data.clone());

        // 写入存储
        let storage_client = get_storage_provider().await?;
        storage_client.put(path, data).await?;

        Ok(())
    }

    /// 清除缓存中的数据
    async fn invalidate_cache(&self, path: &str) -> Result<()> {
        let cache_key = path.to_string();
        let mut metadata_cache = self.metadata_cache.lock().await;
        let mut data_cache = self.data_cache.lock().await;

        metadata_cache.pop(&cache_key);
        data_cache.pop(&cache_key);

        Ok(())
    }
}

fn base_path(job_id: &str, epoch: u32) -> String {
    format!("{}/checkpoints/checkpoint-{:0>7}", job_id, epoch)
}

fn metadata_path(path: &str) -> String {
    format!("{}/metadata", path)
}

fn operator_path(job_id: &str, epoch: u32, operator: &str) -> String {
    format!("{}/operator-{}", base_path(job_id, epoch), operator)
}

#[async_trait::async_trait]
impl BackingStore for OptimizedParquetBackend {
    fn name(&self) -> &'static str {
        "optimized_parquet"
    }

    async fn load_checkpoint_metadata(&self, job_id: &str, epoch: u32) -> Result<CheckpointMetadata> {
        let path = metadata_path(&base_path(job_id, epoch));
        let data = self.get_with_cache(&path, true).await?;
        let metadata = CheckpointMetadata::decode(&data[..])?;
        Ok(metadata)
    }

    async fn load_operator_metadata(
        &self,
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>> {
        let storage_client = get_storage_provider().await?;
        let path = metadata_path(&operator_path(job_id, epoch, operator_id));

        // 尝试从缓存获取
        match self.get_with_cache(&path, true).await {
            Ok(data) => Ok(Some(OperatorCheckpointMetadata::decode(&data[..])?)),
            Err(_) => {
                // 如果缓存中没有，尝试从存储中获取
                match storage_client.get_if_present(path.clone()).await? {
                    Some(data) => {
                        // 更新缓存
                        let mut cache = self.metadata_cache.lock().await;
                        cache.put(path, data.to_vec());

                        Ok(Some(OperatorCheckpointMetadata::decode(&data[..])?))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    async fn write_operator_checkpoint_metadata(
        &self,
        metadata: OperatorCheckpointMetadata,
    ) -> Result<()> {
        let operator_metadata = metadata
            .operator_metadata
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing operator metadata"))?;
        let path = metadata_path(&operator_path(
            &operator_metadata.job_id,
            operator_metadata.epoch,
            &operator_metadata.operator_id,
        ));

        // 编码并写入
        let encoded_data = metadata.encode_to_vec();
        self.put_with_cache(&path, encoded_data, true).await?;

        Ok(())
    }

    async fn write_checkpoint_metadata(&self, metadata: CheckpointMetadata) -> Result<()> {
        debug!("writing checkpoint {:?}", metadata);
        let path = metadata_path(&base_path(&metadata.job_id, metadata.epoch));

        // 编码并写入
        let encoded_data = metadata.encode_to_vec();
        self.put_with_cache(&path, encoded_data, true).await?;

        Ok(())
    }

    async fn prepare_checkpoint_load(&self, _metadata: &CheckpointMetadata) -> anyhow::Result<()> {
        // 清空缓存，确保从存储中获取最新数据
        let mut metadata_cache = self.metadata_cache.lock().await;
        let mut data_cache = self.data_cache.lock().await;

        metadata_cache.clear();
        data_cache.clear();

        Ok(())
    }

    async fn cleanup_checkpoint(
        &self,
        metadata: CheckpointMetadata,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<()> {
        // 并行清理每个操作符的数据
        let mut futures = FuturesUnordered::new();

        for operator_id in &metadata.operator_ids {
            let job_id = metadata.job_id.clone();
            let operator_id = operator_id.clone();
            let self_clone = self.clone();

            futures.push(tokio::spawn(async move {
                OptimizedParquetBackend::cleanup_operator(
                    job_id,
                    operator_id,
                    old_min_epoch,
                    new_min_epoch,
                ).await
            }));

            // 控制并行度
            if futures.len() >= self.parallelism {
                if let Some(result) = futures.next().await {
                    result??;
                }
            }
        }

        // 等待所有清理任务完成
        while let Some(result) = futures.next().await {
            result??;
        }

        Ok(())
    }
}

// 为 OptimizedParquetBackend 实现 Clone trait
impl Clone for OptimizedParquetBackend {
    fn clone(&self) -> Self {
        Self {
            metadata_cache: Mutex::new(LruCache::new(DEFAULT_METADATA_CACHE_SIZE)),
            data_cache: Mutex::new(LruCache::new(DEFAULT_DATA_CACHE_SIZE)),
            parallelism: self.parallelism,
        }
    }
}

impl OptimizedParquetBackend {
    /// 清理操作符的旧数据
    pub async fn cleanup_operator(
        job_id: String,
        operator_id: String,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<String> {
        let backend = OptimizedParquetBackend::new(None);
        let operator_metadata = backend.load_operator_metadata(&job_id, &operator_id, new_min_epoch)
            .await?
            .expect("expect new_min_epoch metadata to still be present");

        // 收集需要保留的文件路径
        let mut paths_to_keep: HashSet<String> = HashSet::new();

        for (table_name, metadata) in &operator_metadata.table_checkpoint_metadata {
            let table_config = operator_metadata
                .table_configs
                .get(table_name)
                .unwrap()
                .clone();

            let files = match table_config.table_type() {
                rpc::TableEnum::MissingTableType => vec![],
                rpc::TableEnum::GlobalKeyValue => {
                    let files_set = GlobalKeyedTable::files_to_keep(table_config, metadata.clone()).unwrap();
                    files_set.into_iter().collect::<Vec<_>>()
                }
                rpc::TableEnum::ExpiringKeyedTimeTable => {
                    let files_set = ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone()).unwrap();
                    files_set.into_iter().collect::<Vec<_>>()
                }
            };

            for file in files {
                paths_to_keep.insert(file);
            }
        }

        // 使用 Arc<Mutex<HashSet<String>>> 来共享删除的路径
        let deleted_paths = Arc::new(Mutex::new(HashSet::new()));
        let storage_client = Arc::new(get_storage_provider().await?);

        // 并行删除不再需要的文件
        let mut futures = FuturesUnordered::new();

        for epoch_to_remove in old_min_epoch..new_min_epoch {
            let Some(operator_metadata) =
                backend.load_operator_metadata(&job_id, &operator_id, epoch_to_remove).await?
            else {
                continue;
            };

            for (table_name, metadata) in &operator_metadata.table_checkpoint_metadata {
                let table_config = operator_metadata
                    .table_configs
                    .get(table_name)
                    .unwrap()
                    .clone();

                let files_to_check: Vec<String> = match table_config.table_type() {
                    rpc::TableEnum::MissingTableType => vec![],
                    rpc::TableEnum::GlobalKeyValue => {
                        let files_set = GlobalKeyedTable::files_to_keep(table_config, metadata.clone()).unwrap();
                        files_set.into_iter().collect()
                    }
                    rpc::TableEnum::ExpiringKeyedTimeTable => {
                        let files_set = ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone()).unwrap();
                        files_set.into_iter().collect()
                    }
                };

                for file in files_to_check {
                    // 检查文件是否需要删除
                    let should_delete = {
                        let deleted_paths_guard = deleted_paths.lock().await;
                        !paths_to_keep.contains(&file) && !deleted_paths_guard.contains(&file)
                    };

                    if should_delete {
                        let file_clone = file.clone();
                        let deleted_paths_clone = Arc::clone(&deleted_paths);
                        let backend_clone = backend.clone();
                        let storage_client_clone = Arc::clone(&storage_client);

                        futures.push(tokio::spawn(async move {
                            // 从存储中删除文件
                            storage_client_clone.delete_if_present(file_clone.clone()).await?;

                            // 从缓存中删除
                            backend_clone.invalidate_cache(&file_clone).await?;

                            // 标记为已删除
                            let mut deleted_paths_guard = deleted_paths_clone.lock().await;
                            deleted_paths_guard.insert(file_clone);

                            Ok::<_, anyhow::Error>(())
                        }));

                        // 控制并行度
                        if futures.len() >= backend.parallelism {
                            if let Some(result) = futures.next().await {
                                result??;
                            }
                        }
                    }
                }
            }
        }

        // 等待所有删除任务完成
        while let Some(result) = futures.next().await {
            result??;
        }

        Ok(operator_id)
    }

    /// 压缩操作符的数据（待实现）
    pub async fn compact_operator(
        _job_id: Arc<String>,
        _operator_id: &str,
        _epoch: u32,
    ) -> Result<HashMap<String, TableCheckpointMetadata>> {
        // 这个方法需要更多的修改，我们将在后续实现
        Ok(HashMap::new())
    }
}
