use crate::parquet::ParquetBackend;
use crate::tiered::{StorageTier, TieredStateBackend, TieredStorageConfig};
use crate::BackingStore;
use anyhow::Result;
use arroyo_rpc::config::{config, StateBackendType};
use arroyo_storage::StorageProvider;
use std::path::PathBuf;
use std::sync::Arc;
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
                    cache_expiration: state_config.cache_expiration.duration,
                };

                Ok(Box::new(TieredStateBackend::new(tiered_config)))
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
            state_config.incremental_checkpoint_interval.0,
        );

        Ok(Some(manager))
    }
}
