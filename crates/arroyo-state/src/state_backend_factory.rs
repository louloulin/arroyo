use crate::parquet::ParquetBackend;
use crate::optimized_parquet::OptimizedParquetBackend;
use crate::tiered::{TieredStateBackend, TieredStorageConfig};
use crate::layered_state::{HotDataStrategy, LayeredStateBackend, LayeredStateConfig};
use crate::BackingStore;
use anyhow::Result;
use arroyo_rpc::config::{config, StateBackendType};
use arroyo_storage::StorageProvider;
use std::sync::Arc;
use std::path::PathBuf;
use std::time::Duration;

/// 状态后端工厂
///
/// 根据配置创建适当的状态后端
pub struct StateBackendFactory;

impl StateBackendFactory {
    /// 创建状态后端
    pub async fn create_state_backend() -> Result<Box<dyn BackingStore>> {
        let state_config = &config().pipeline.state;

        match state_config.backend_type {
            StateBackendType::Parquet => {
                // 使用默认的Parquet后端
                Ok(Box::new(ParquetBackend))
            }
            StateBackendType::OptimizedParquet => {
                // 使用优化的Parquet后端
                // 获取可用的CPU核心数作为并行度
                let parallelism = std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or(4);

                Ok(Box::new(OptimizedParquetBackend::new(Some(parallelism))))
            }
            StateBackendType::Tiered => {
                // 创建分层状态后端
                let storage_provider = Arc::new(
                    StorageProvider::for_url(&config().checkpoint_url).await?
                );

                let tiered_config = TieredStorageConfig {
                    enable_memory_tier: state_config.enable_memory_cache,
                    memory_tier_max_size: state_config.memory_cache_size,
                    enable_local_disk_tier: state_config.enable_disk_cache,
                    local_disk_path: state_config.disk_cache_path.clone(),
                    local_disk_max_size: state_config.disk_cache_size,
                    remote_storage: storage_provider,
                    cache_expiration: *state_config.cache_expiration,
                    job_id: "default".to_string(),
                };

                Ok(Box::new(TieredStateBackend::new(tiered_config)))
            },
            StateBackendType::Layered => {
                // 创建分层状态后端配置
                let layered_config = LayeredStateConfig {
                    enable_memory_tier: state_config.layered_state_enable_memory,
                    memory_tier_max_size: state_config.layered_state_memory_size.unwrap_or(100 * 1024 * 1024), // 默认100MB
                    enable_local_disk_tier: state_config.layered_state_enable_disk,
                    local_disk_path: PathBuf::from(format!("/tmp/arroyo/state-cache")),
                    local_disk_max_size: state_config.layered_state_disk_size.unwrap_or(1024 * 1024 * 1024), // 默认1GB
                    remote_storage: Arc::new(StorageProvider::dummy()),
                    cache_expiration: Duration::from_secs(state_config.layered_state_cache_ttl.unwrap_or(3600)), // 默认1小时
                    hot_data_strategy: HotDataStrategy::Weighted,
                    hot_data_ratio: state_config.layered_state_hot_ratio.unwrap_or(0.2), // 默认20%
                    enable_prefetching: state_config.layered_state_enable_prefetch.unwrap_or(true),
                    prefetch_threshold: state_config.layered_state_prefetch_threshold.unwrap_or(3),
                    enable_compression: state_config.layered_state_enable_compression.unwrap_or(true),
                    compression_level: state_config.layered_state_compression_level.unwrap_or(6),
                };

                // 创建并返回分层状态后端
                Ok(Box::new(LayeredStateBackend::new(layered_config)))
            }
        }
    }

    /// 创建增量检查点管理器
    pub async fn create_incremental_checkpoint_manager(
        job_id: String,
    ) -> Result<Option<crate::incremental_checkpoint::IncrementalCheckpointManager>> {
        let state_config = &config().pipeline.state;

        if !state_config.enable_incremental_checkpoint {
            return Ok(None);
        }

        let storage_provider = Arc::new(
            StorageProvider::for_url(&config().checkpoint_url).await?
        );

        let manager = crate::incremental_checkpoint::IncrementalCheckpointManager::new(
            storage_provider,
            job_id,
            state_config.max_incremental_checkpoints,
            *state_config.incremental_checkpoint_interval,
        );

        Ok(Some(manager))
    }
}
