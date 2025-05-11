use crate::BackingStore;
use anyhow::Result;
use arroyo_rpc::grpc::rpc::CheckpointMetadata;
use arroyo_storage::StorageProvider;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// 增量检查点元数据
#[derive(Debug, Clone)]
pub struct IncrementalCheckpointMetadata {
    /// 基础检查点元数据
    pub base_metadata: CheckpointMetadata,
    /// 增量数据文件列表
    pub incremental_files: HashMap<String, Vec<String>>,
    /// 增量检查点创建时间
    pub creation_time: SystemTime,
}

/// 可序列化的增量检查点元数据
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SerializableIncrementalCheckpointMetadata {
    /// 基础检查点元数据（序列化为字节）
    pub base_metadata_bytes: Vec<u8>,
    /// 增量数据文件列表
    pub incremental_files: HashMap<String, Vec<String>>,
    /// 增量检查点创建时间（微秒时间戳）
    pub creation_time_micros: u64,
}

impl From<IncrementalCheckpointMetadata> for SerializableIncrementalCheckpointMetadata {
    fn from(metadata: IncrementalCheckpointMetadata) -> Self {
        use prost::Message;

        Self {
            base_metadata_bytes: metadata.base_metadata.encode_to_vec(),
            incremental_files: metadata.incremental_files,
            creation_time_micros: metadata.creation_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
        }
    }
}

impl TryFrom<SerializableIncrementalCheckpointMetadata> for IncrementalCheckpointMetadata {
    type Error = anyhow::Error;

    fn try_from(serializable: SerializableIncrementalCheckpointMetadata) -> Result<Self> {
        use prost::Message;

        Ok(Self {
            base_metadata: CheckpointMetadata::decode(&serializable.base_metadata_bytes[..])?,
            incremental_files: serializable.incremental_files,
            creation_time: SystemTime::UNIX_EPOCH + Duration::from_micros(serializable.creation_time_micros),
        })
    }
}

/// 增量检查点管理器
pub struct IncrementalCheckpointManager {
    /// 存储提供者
    storage_provider: Arc<StorageProvider>,
    /// 作业ID
    job_id: String,
    /// 最大增量检查点数量
    max_incremental_checkpoints: usize,
    /// 增量检查点间隔
    incremental_interval: Duration,
    /// 上次完整检查点时间
    last_full_checkpoint_time: Option<SystemTime>,
    /// 上次增量检查点时间
    last_incremental_checkpoint_time: Option<SystemTime>,
    /// 当前增量检查点数量
    current_incremental_count: usize,
}

impl IncrementalCheckpointManager {
    /// 创建新的增量检查点管理器
    pub fn new(
        storage_provider: Arc<StorageProvider>,
        job_id: String,
        max_incremental_checkpoints: usize,
        incremental_interval: Duration,
    ) -> Self {
        Self {
            storage_provider,
            job_id,
            max_incremental_checkpoints,
            incremental_interval,
            last_full_checkpoint_time: None,
            last_incremental_checkpoint_time: None,
            current_incremental_count: 0,
        }
    }

    /// 检查是否应该创建增量检查点
    pub fn should_create_incremental(&self) -> bool {
        // 如果没有完整检查点，不能创建增量检查点
        if self.last_full_checkpoint_time.is_none() {
            return false;
        }

        // 如果增量检查点数量已达到最大值，不能创建增量检查点
        if self.current_incremental_count >= self.max_incremental_checkpoints {
            return false;
        }

        // 检查是否达到增量检查点间隔
        if let Some(last_time) = self.last_incremental_checkpoint_time {
            let now = SystemTime::now();
            if now.duration_since(last_time).unwrap_or_default() < self.incremental_interval {
                return false;
            }
        }

        true
    }

    /// 检查是否应该创建完整检查点
    pub fn should_create_full(&self) -> bool {
        // 如果没有完整检查点，应该创建一个
        if self.last_full_checkpoint_time.is_none() {
            return true;
        }

        // 如果增量检查点数量达到最大值，应该创建完整检查点
        if self.current_incremental_count >= self.max_incremental_checkpoints {
            return true;
        }

        false
    }

    /// 创建增量检查点
    pub async fn create_incremental_checkpoint(
        &mut self,
        base_epoch: u32,
        current_epoch: u32,
        operator_id: &str,
        changes: HashMap<String, Vec<u8>>,
    ) -> Result<()> {
        // 获取基础检查点元数据
        let base_metadata = crate::StateBackend::load_checkpoint_metadata(&self.job_id, base_epoch).await?;

        // 创建增量文件路径
        let incremental_path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/incremental-{:0>7}/operator-{}",
            self.job_id, base_epoch, current_epoch, operator_id
        );

        // 保存增量数据
        let mut incremental_files = HashMap::new();
        for (key, data) in changes {
            let file_path = format!("{}/{}", incremental_path, key);
            self.storage_provider.put(file_path.as_str(), data).await?;

            incremental_files.entry(operator_id.to_string())
                .or_insert_with(Vec::new)
                .push(key);
        }

        // 创建增量检查点元数据
        let incremental_metadata = IncrementalCheckpointMetadata {
            base_metadata,
            incremental_files,
            creation_time: SystemTime::now(),
        };

        // 保存增量检查点元数据
        let metadata_path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/incremental-{:0>7}/metadata",
            self.job_id, base_epoch, current_epoch
        );

        // 转换为可序列化的格式
        let serializable_metadata = SerializableIncrementalCheckpointMetadata::from(incremental_metadata);

        // 序列化为JSON
        let metadata_json = serde_json::to_string(&serializable_metadata)?;
        self.storage_provider.put(metadata_path.as_str(), metadata_json.into_bytes()).await?;

        // 更新状态
        self.last_incremental_checkpoint_time = Some(SystemTime::now());
        self.current_incremental_count += 1;

        Ok(())
    }

    /// 创建完整检查点
    pub async fn create_full_checkpoint(&mut self) -> Result<()> {
        // 这里我们不实际创建完整检查点，因为这由现有的检查点机制处理
        // 我们只需要更新状态
        self.last_full_checkpoint_time = Some(SystemTime::now());
        self.last_incremental_checkpoint_time = None;
        self.current_incremental_count = 0;

        Ok(())
    }

    /// 应用增量检查点
    pub async fn apply_incremental_checkpoint(
        &self,
        base_epoch: u32,
        incremental_epoch: u32,
    ) -> Result<CheckpointMetadata> {
        // 加载增量检查点元数据
        let metadata_path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/incremental-{:0>7}/metadata",
            self.job_id, base_epoch, incremental_epoch
        );

        let metadata_bytes = self.storage_provider.get(metadata_path.as_str()).await?;
        let serializable_metadata: SerializableIncrementalCheckpointMetadata = serde_json::from_slice(&metadata_bytes)?;
        let incremental_metadata = IncrementalCheckpointMetadata::try_from(serializable_metadata)?;

        // 返回基础检查点元数据
        // 实际应用增量变更的逻辑应该在恢复过程中处理
        Ok(incremental_metadata.base_metadata)
    }
}

/// 增量检查点跟踪器
/// 用于跟踪哪些数据已更改，需要包含在增量检查点中
pub struct IncrementalChangeTracker {
    /// 变更的键
    changed_keys: HashSet<String>,
    /// 变更的数据
    changes: HashMap<String, Vec<u8>>,
}

impl IncrementalChangeTracker {
    /// 创建新的增量变更跟踪器
    pub fn new() -> Self {
        Self {
            changed_keys: HashSet::new(),
            changes: HashMap::new(),
        }
    }

    /// 跟踪键的变更
    pub fn track_change(&mut self, key: String) {
        self.changed_keys.insert(key);
    }

    /// 添加变更数据
    pub fn add_change(&mut self, key: String, data: Vec<u8>) {
        self.changes.insert(key, data);
    }

    /// 获取所有变更
    pub fn get_changes(&self) -> &HashMap<String, Vec<u8>> {
        &self.changes
    }

    /// 清除所有变更
    pub fn clear(&mut self) {
        self.changed_keys.clear();
        self.changes.clear();
    }
}

/// 异步检查点创建器
pub struct AsyncCheckpointCreator {
    /// 存储提供者
    storage_provider: Arc<StorageProvider>,
    /// 作业ID
    job_id: String,
    /// 操作符ID
    operator_id: String,
    /// 当前检查点epoch
    current_epoch: u32,
}

impl AsyncCheckpointCreator {
    /// 创建新的异步检查点创建器
    pub fn new(
        storage_provider: Arc<StorageProvider>,
        job_id: String,
        operator_id: String,
        current_epoch: u32,
    ) -> Self {
        Self {
            storage_provider,
            job_id,
            operator_id,
            current_epoch,
        }
    }

    /// 异步创建检查点
    pub async fn create_checkpoint_async(
        &self,
        data: HashMap<String, Vec<u8>>,
    ) -> Result<()> {
        // 创建检查点路径
        let checkpoint_path = format!(
            "{}/checkpoints/checkpoint-{:0>7}/operator-{}",
            self.job_id, self.current_epoch, self.operator_id
        );

        // 异步保存所有数据
        let mut futures = FuturesUnordered::new();

        for (key, value) in data {
            let file_path = format!("{}/{}", checkpoint_path, key);
            let storage = self.storage_provider.clone();

            let future = async move {
                storage.put(file_path.as_str(), value).await
            };

            futures.push(future);
        }

        // 等待所有异步操作完成
        while let Some(result) = futures.next().await {
            result?;
        }

        Ok(())
    }
}
