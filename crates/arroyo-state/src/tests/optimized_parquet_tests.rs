use crate::optimized_parquet::OptimizedParquetBackend;
use crate::BackingStore;
use anyhow::Result;
use arroyo_rpc::grpc::rpc::{CheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata};
use std::time::SystemTime;

/// 测试 OptimizedParquetBackend 的基本功能
#[tokio::test]
async fn test_optimized_parquet_backend_basic() -> Result<()> {
    // 创建 OptimizedParquetBackend 实例
    let backend = OptimizedParquetBackend::new(Some(2));

    // 验证名称
    assert_eq!(backend.name(), "optimized_parquet");

    // 创建测试数据
    let job_id = "test_job";
    let operator_id = "test_operator";
    let epoch = 1;

    // 创建检查点元数据
    let checkpoint_metadata = CheckpointMetadata {
        job_id: job_id.to_string(),
        epoch,
        min_epoch: 0,
        start_time: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        finish_time: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        operator_ids: vec![operator_id.to_string()],
    };

    // 创建操作符元数据
    let operator_metadata = OperatorCheckpointMetadata {
        start_time: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        finish_time: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        table_checkpoint_metadata: Default::default(),
        table_configs: Default::default(),
        operator_metadata: Some(OperatorMetadata {
            job_id: job_id.to_string(),
            operator_id: operator_id.to_string(),
            epoch,
            min_watermark: Some(0),
            max_watermark: Some(0),
            parallelism: 1,
        }),
    };

    // 写入检查点元数据
    backend.write_checkpoint_metadata(checkpoint_metadata.clone()).await?;

    // 写入操作符元数据
    backend.write_operator_checkpoint_metadata(operator_metadata.clone()).await?;

    // 读取检查点元数据
    let loaded_checkpoint_metadata = backend.load_checkpoint_metadata(job_id, epoch).await?;

    // 验证检查点元数据
    assert_eq!(loaded_checkpoint_metadata.job_id, job_id);
    assert_eq!(loaded_checkpoint_metadata.epoch, epoch);
    assert_eq!(loaded_checkpoint_metadata.operator_ids.len(), 1);
    assert_eq!(loaded_checkpoint_metadata.operator_ids[0], operator_id);

    // 读取操作符元数据
    let loaded_operator_metadata = backend.load_operator_metadata(job_id, operator_id, epoch).await?;

    // 验证操作符元数据
    assert!(loaded_operator_metadata.is_some());
    let loaded_operator_metadata = loaded_operator_metadata.unwrap();
    assert_eq!(
        loaded_operator_metadata.operator_metadata.as_ref().unwrap().job_id,
        job_id
    );
    assert_eq!(
        loaded_operator_metadata.operator_metadata.as_ref().unwrap().operator_id,
        operator_id
    );
    assert_eq!(
        loaded_operator_metadata.operator_metadata.as_ref().unwrap().epoch,
        epoch
    );

    Ok(())
}

/// 测试 OptimizedParquetBackend 的缓存功能
#[tokio::test]
async fn test_optimized_parquet_backend_cache() -> Result<()> {
    // 创建 OptimizedParquetBackend 实例
    let backend = OptimizedParquetBackend::new(Some(2));

    // 创建测试数据
    let job_id = "test_job_cache";
    let operator_id = "test_operator_cache";
    let epoch = 2;

    // 创建检查点元数据
    let checkpoint_metadata = CheckpointMetadata {
        job_id: job_id.to_string(),
        epoch,
        min_epoch: 0,
        start_time: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        finish_time: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        operator_ids: vec![operator_id.to_string()],
    };

    // 写入检查点元数据
    backend.write_checkpoint_metadata(checkpoint_metadata.clone()).await?;

    // 读取检查点元数据（应该从缓存中获取）
    let loaded_checkpoint_metadata = backend.load_checkpoint_metadata(job_id, epoch).await?;

    // 验证检查点元数据
    assert_eq!(loaded_checkpoint_metadata.job_id, job_id);
    assert_eq!(loaded_checkpoint_metadata.epoch, epoch);

    // 清除缓存
    backend.prepare_checkpoint_load(&checkpoint_metadata).await?;

    // 读取检查点元数据（应该从存储中获取）
    let loaded_checkpoint_metadata = backend.load_checkpoint_metadata(job_id, epoch).await?;

    // 验证检查点元数据
    assert_eq!(loaded_checkpoint_metadata.job_id, job_id);
    assert_eq!(loaded_checkpoint_metadata.epoch, epoch);

    Ok(())
}
