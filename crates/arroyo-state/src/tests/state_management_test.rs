#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use anyhow::Result;
    use arroyo_rpc::grpc::rpc::{CheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata};
    use arroyo_storage::StorageProvider;
    use arroyo_types::to_micros;
    use tempfile::tempdir;

    use crate::incremental_checkpoint::{
        IncrementalChangeTracker, IncrementalCheckpointManager, IncrementalCheckpointMetadata,
    };
    use crate::tiered::{StorageTier, TieredStateBackend, TieredStorageConfig};
    use crate::BackingStore;

    #[tokio::test]
    async fn test_tiered_state_backend() -> Result<()> {
        // 创建临时目录
        let temp_dir = tempdir()?;
        let temp_path = temp_dir.path().to_path_buf();

        // 创建本地存储提供者
        let storage_url = format!("file://{}", temp_path.to_string_lossy());
        let storage_provider = Arc::new(StorageProvider::for_url(&storage_url).await?);

        // 创建分层状态后端配置
        let config = TieredStorageConfig {
            enable_memory_tier: true,
            memory_tier_max_size: 10 * 1024 * 1024, // 10MB
            enable_local_disk_tier: true,
            local_disk_path: temp_path.clone(),
            local_disk_max_size: 100 * 1024 * 1024, // 100MB
            remote_storage: storage_provider.clone(),
            cache_expiration: Duration::from_secs(60), // 1 minute
        };

        // 创建分层状态后端
        let backend = TieredStateBackend::new(config);

        // 创建测试数据
        let job_id = "test-job";
        let epoch = 1;
        let operator_id = "test-operator";

        // 创建检查点元数据
        let checkpoint_metadata = CheckpointMetadata {
            job_id: job_id.to_string(),
            epoch,
            min_epoch: 0,
            start_time: to_micros(SystemTime::now()),
            finish_time: to_micros(SystemTime::now()),
            operator_ids: vec![operator_id.to_string()],
        };

        // 创建操作符元数据
        let operator_metadata = OperatorMetadata {
            job_id: job_id.to_string(),
            operator_id: operator_id.to_string(),
            epoch,
            min_watermark: None,
            max_watermark: None,
            parallelism: 1,
        };

        // 创建操作符检查点元数据
        let operator_checkpoint_metadata = OperatorCheckpointMetadata {
            operator_metadata: Some(operator_metadata),
            start_time: to_micros(SystemTime::now()),
            finish_time: to_micros(SystemTime::now()),
            table_checkpoint_metadata: HashMap::new(),
            table_configs: HashMap::new(),
        };

        // 写入检查点元数据
        backend.write_checkpoint_metadata(checkpoint_metadata.clone()).await?;

        // 写入操作符检查点元数据
        backend.write_operator_checkpoint_metadata(operator_checkpoint_metadata.clone()).await?;

        // 读取检查点元数据
        let loaded_checkpoint_metadata = backend.load_checkpoint_metadata(job_id, epoch).await?;

        // 验证检查点元数据
        assert_eq!(loaded_checkpoint_metadata.job_id, checkpoint_metadata.job_id);
        assert_eq!(loaded_checkpoint_metadata.epoch, checkpoint_metadata.epoch);

        // 读取操作符检查点元数据
        let loaded_operator_checkpoint_metadata = backend.load_operator_metadata(job_id, operator_id, epoch).await?;

        // 验证操作符检查点元数据
        assert!(loaded_operator_checkpoint_metadata.is_some());
        let loaded_operator_checkpoint_metadata = loaded_operator_checkpoint_metadata.unwrap();
        assert_eq!(
            loaded_operator_checkpoint_metadata.operator_metadata.as_ref().unwrap().job_id,
            operator_checkpoint_metadata.operator_metadata.as_ref().unwrap().job_id
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_incremental_checkpoint() -> Result<()> {
        // 创建临时目录
        let temp_dir = tempdir()?;
        let temp_path = temp_dir.path().to_path_buf();

        // 创建本地存储提供者
        let storage_url = format!("file://{}", temp_path.to_string_lossy());
        let storage_provider = Arc::new(StorageProvider::for_url(&storage_url).await?);

        // 创建增量检查点管理器
        let job_id = "test-job";
        let max_incremental_checkpoints = 3;
        let incremental_interval = Duration::from_secs(1);

        let mut manager = IncrementalCheckpointManager::new(
            storage_provider.clone(),
            job_id.to_string(),
            max_incremental_checkpoints,
            incremental_interval,
        );

        // 创建基础检查点
        let base_epoch = 10;
        let current_epoch = 11;
        let operator_id = "test-operator";

        // 创建变更跟踪器
        let mut tracker = IncrementalChangeTracker::new();

        // 添加一些变更
        tracker.add_change("key1".to_string(), vec![1, 2, 3]);
        tracker.add_change("key2".to_string(), vec![4, 5, 6]);

        // 创建增量检查点
        manager.create_full_checkpoint().await?;

        // 检查是否应该创建增量检查点
        assert!(manager.should_create_incremental());

        // 创建增量检查点
        manager.create_incremental_checkpoint(
            base_epoch,
            current_epoch,
            operator_id,
            tracker.get_changes().clone(),
        ).await?;

        // 应用增量检查点
        let metadata = manager.apply_incremental_checkpoint(base_epoch, current_epoch).await?;

        // 验证元数据
        assert_eq!(metadata.job_id, job_id);

        Ok(())
    }
}
