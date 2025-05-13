use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, ExpiringKeyedTimeTableConfig, GlobalKeyedTableConfig,
    OperatorCheckpointMetadata, TableCheckpointMetadata, TableConfig, TableEnum,
};
use arroyo_types::single_item_hash_map;
use async_trait::async_trait;
use bincode::config::Configuration;
use bincode::{Decode, Encode};

use arroyo_rpc::config::config;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_storage::StorageProvider;
use prost::Message;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub mod checkpoint_state;
pub mod committing_state;
mod metrics;
pub mod parquet;
pub mod optimized_parquet;
pub mod tiered;
pub mod incremental_checkpoint;
pub mod state_backend_factory;
pub mod layered_state;
pub(crate) mod schemas;
pub mod tables;
pub mod two_phase_commit;

#[cfg(test)]
pub mod tests;

pub const BINCODE_CONFIG: Configuration = bincode::config::standard();
pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;

#[derive(Debug)]
pub enum StateMessage {
    Checkpoint(CheckpointMessage),
    Compaction(HashMap<String, TableCheckpointMetadata>),
    TableData { table: String, data: TableData },
}
#[derive(Debug)]
pub struct CheckpointMessage {
    epoch: u32,
    time: SystemTime,
    watermark: Option<SystemTime>,
    then_stop: bool,
}

#[derive(Debug)]
pub enum TableData {
    RecordBatch(RecordBatch),
    CommitData { data: Vec<u8> },
    KeyedData { key: Vec<u8>, value: Vec<u8> },
}

// 状态后端类型别名，用于向后兼容
pub type StateBackend = dyn BackingStore + Send + Sync;

pub fn global_table_config(
    name: impl Into<String>,
    description: impl Into<String>,
) -> HashMap<String, TableConfig> {
    let name = name.into();
    single_item_hash_map(
        name.clone(),
        TableConfig {
            table_type: TableEnum::GlobalKeyValue.into(),
            config: GlobalKeyedTableConfig {
                table_name: name,
                description: description.into(),
                uses_two_phase_commit: false,
            }
            .encode_to_vec(),
        },
    )
}

pub fn timestamp_table_config(
    name: impl Into<String>,
    description: impl Into<String>,
    retention: Duration,
    generational: bool,
    schema: ArroyoSchema,
) -> TableConfig {
    TableConfig {
        table_type: TableEnum::ExpiringKeyedTimeTable.into(),
        config: ExpiringKeyedTimeTableConfig {
            table_name: name.into(),
            description: description.into(),
            retention_micros: retention.as_micros() as u64,
            generational,
            schema: Some(schema.into()),
        }
        .encode_to_vec(),
    }
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteTimeKeyOperation {
    pub timestamp: SystemTime,
    pub key: Vec<u8>,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteKeyOperation {
    pub key: Vec<u8>,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteValueOperation {
    pub key: Vec<u8>,
    pub timestamp: SystemTime,
    pub value: Vec<u8>,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteTimeRangeOperation {
    pub key: Vec<u8>,
    pub start: SystemTime,
    pub end: SystemTime,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub enum DataOperation {
    Insert,
    DeleteTimeKey(DeleteTimeKeyOperation), // delete single key of a TimeKeyMap
    DeleteKey(DeleteKeyOperation),         // delete all data for a key in a KeyTimeMultiMap
    DeleteValue(DeleteValueOperation),     // delete single value of a KeyTimeMultiMap
    DeleteTimeRange(DeleteTimeRangeOperation), // delete all values for key in range (only for KeyTimeMultiMap)
}
#[async_trait]
pub trait BackingStore: Send + Sync {
    /// 返回状态后端的名称
    fn name(&self) -> &'static str;

    /// 准备加载检查点，例如删除未来的数据
    async fn prepare_checkpoint_load(&self, metadata: &CheckpointMetadata) -> Result<()>;

    /// 加载给定作业ID和epoch的检查点元数据
    async fn load_checkpoint_metadata(&self, job_id: &str, epoch: u32) -> Result<CheckpointMetadata>;

    /// 加载给定作业ID、操作符ID和epoch的操作符检查点元数据
    async fn load_operator_metadata(
        &self,
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>>;

    /// 将操作符检查点元数据写入后端存储
    async fn write_operator_checkpoint_metadata(
        &self,
        metadata: OperatorCheckpointMetadata,
    ) -> Result<()>;

    /// 将检查点元数据写入后端存储
    async fn write_checkpoint_metadata(&self, metadata: CheckpointMetadata) -> Result<()>;

    /// 通过删除不再需要的数据来清理检查点
    async fn cleanup_checkpoint(
        &self,
        metadata: CheckpointMetadata,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<()>;
}

pub fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

static STORAGE_PROVIDER: tokio::sync::OnceCell<Arc<StorageProvider>> =
    tokio::sync::OnceCell::const_new();

pub(crate) async fn get_storage_provider() -> Result<&'static Arc<StorageProvider>> {
    // TODO: this should be encoded in the config so that the controller doesn't need
    // to be synchronized with the workers

    STORAGE_PROVIDER
        .get_or_try_init(|| async {
            let storage_url = &config().checkpoint_url;

            StorageProvider::for_url(storage_url)
                .await
                .context(format!(
                    "failed to construct checkpoint backend for URL {}",
                    storage_url
                ))
                .map(Arc::new)
        })
        .await
}

/// 获取状态后端实例
pub async fn get_state_backend(job_id: &str) -> Result<Arc<dyn BackingStore>> {
    // 检查是否启用了分层状态存储
    let config = config();
    let state_config = &config.pipeline.state;

    if state_config.enable_layered_state {
        // 创建分层状态后端配置
        let storage_provider = get_storage_provider().await?;

        let layered_config = layered_state::LayeredStateConfig {
            enable_memory_tier: state_config.layered_state_enable_memory,
            memory_tier_max_size: state_config.layered_state_memory_size.unwrap_or(100 * 1024 * 1024), // 默认100MB
            enable_local_disk_tier: state_config.layered_state_enable_disk,
            local_disk_path: std::path::PathBuf::from(format!("/tmp/arroyo/{}/state-cache", job_id)),
            local_disk_max_size: state_config.layered_state_disk_size.unwrap_or(1024 * 1024 * 1024), // 默认1GB
            remote_storage: storage_provider.clone(),
            cache_expiration: Duration::from_secs(state_config.layered_state_cache_ttl.unwrap_or(3600)), // 默认1小时
            hot_data_strategy: layered_state::HotDataStrategy::Weighted,
            hot_data_ratio: state_config.layered_state_hot_ratio.unwrap_or(0.2), // 默认20%
            enable_prefetching: state_config.layered_state_enable_prefetch.unwrap_or(true),
            prefetch_threshold: state_config.layered_state_prefetch_threshold.unwrap_or(3),
            enable_compression: state_config.layered_state_enable_compression.unwrap_or(true),
            compression_level: state_config.layered_state_compression_level.unwrap_or(6),
        };

        // 创建并返回分层状态后端
        return Ok(Arc::new(layered_state::LayeredStateBackend::new(layered_config)));
    }

    // 使用状态后端工厂创建状态后端
    let state_backend = state_backend_factory::StateBackendFactory::create_state_backend().await?;

    // 如果配置了使用 TieredStateBackend，则创建并返回
    if state_config.enable_tiered_state {
        let storage_provider = get_storage_provider().await?;
        return Ok(Arc::new(tiered::TieredStateBackend::new_with_provider(
            storage_provider.clone(),
            job_id.to_string(),
        )));
    }

    // 默认使用 ParquetBackend
    Ok(Arc::new(parquet::ParquetBackend))
}
