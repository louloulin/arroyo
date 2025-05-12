use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, Result};
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata, TableCheckpointMetadata,
};
use arroyo_state;
use arroyo_storage;
use tracing::{debug, info, warn};

/// 动态扩展操作类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingOperation {
    /// 扩展操作（增加并行度）
    ScaleOut {
        /// 操作符ID
        operator_id: String,
        /// 原始并行度
        original_parallelism: u32,
        /// 新并行度
        new_parallelism: u32,
    },
    /// 收缩操作（减少并行度）
    ScaleIn {
        /// 操作符ID
        operator_id: String,
        /// 原始并行度
        original_parallelism: u32,
        /// 新并行度
        new_parallelism: u32,
    },
}

/// 动态扩展计划
#[derive(Debug, Clone)]
pub struct ScalingPlan {
    /// 作业ID
    pub job_id: String,
    /// 当前epoch
    pub epoch: u32,
    /// 扩展操作列表
    pub operations: Vec<ScalingOperation>,
    /// 计划创建时间
    pub created_at: SystemTime,
}

impl ScalingPlan {
    /// 创建新的扩展计划
    pub fn new(
        job_id: String,
        epoch: u32,
        old_parallelism: &HashMap<u32, usize>,
        new_parallelism: &HashMap<u32, usize>,
    ) -> Self {
        let mut operations = Vec::new();

        // 比较新旧并行度，生成扩展操作
        for (node_id, new_p) in new_parallelism {
            if let Some(old_p) = old_parallelism.get(node_id) {
                if new_p > old_p {
                    operations.push(ScalingOperation::ScaleOut {
                        operator_id: node_id.to_string(),
                        original_parallelism: *old_p as u32,
                        new_parallelism: *new_p as u32,
                    });
                } else if new_p < old_p {
                    operations.push(ScalingOperation::ScaleIn {
                        operator_id: node_id.to_string(),
                        original_parallelism: *old_p as u32,
                        new_parallelism: *new_p as u32,
                    });
                }
            }
        }

        Self {
            job_id,
            epoch,
            operations,
            created_at: SystemTime::now(),
        }
    }

    /// 检查是否有扩展操作
    pub fn has_operations(&self) -> bool {
        !self.operations.is_empty()
    }

    /// 获取所有需要扩展的操作符ID
    pub fn get_affected_operators(&self) -> HashSet<String> {
        self.operations
            .iter()
            .map(|op| match op {
                ScalingOperation::ScaleOut { operator_id, .. } => operator_id.clone(),
                ScalingOperation::ScaleIn { operator_id, .. } => operator_id.clone(),
            })
            .collect()
    }
}

/// 状态重分配计划
#[derive(Debug, Clone)]
pub struct StateRedistributionPlan {
    /// 作业ID
    pub job_id: String,
    /// 检查点epoch
    pub checkpoint_epoch: u32,
    /// 操作符ID
    pub operator_id: String,
    /// 原始并行度
    pub original_parallelism: u32,
    /// 新并行度
    pub new_parallelism: u32,
    /// 状态映射：键 -> 原始子任务索引 -> 新子任务索引列表
    pub state_mapping: HashMap<String, HashMap<u32, Vec<u32>>>,
}

impl StateRedistributionPlan {
    /// 创建新的状态重分配计划
    pub fn new(
        job_id: String,
        checkpoint_epoch: u32,
        operator_id: String,
        original_parallelism: u32,
        new_parallelism: u32,
    ) -> Self {
        Self {
            job_id,
            checkpoint_epoch,
            operator_id,
            original_parallelism,
            new_parallelism,
            state_mapping: HashMap::new(),
        }
    }

    /// 计算状态重分配映射
    pub fn compute_state_mapping(&mut self, table_names: &[String]) -> Result<()> {
        for table_name in table_names {
            let mut mapping = HashMap::new();

            // 为每个原始子任务计算新的目标子任务
            for subtask_idx in 0..self.original_parallelism {
                let target_subtasks = if self.new_parallelism > self.original_parallelism {
                    // 扩展情况：状态可能需要拆分到多个新子任务
                    self.compute_scale_out_mapping(subtask_idx)
                } else {
                    // 收缩情况：多个原始子任务的状态合并到一个新子任务
                    self.compute_scale_in_mapping(subtask_idx)
                };

                mapping.insert(subtask_idx, target_subtasks);
            }

            self.state_mapping.insert(table_name.clone(), mapping);
        }

        Ok(())
    }

    /// 计算扩展时的状态映射
    fn compute_scale_out_mapping(&self, subtask_idx: u32) -> Vec<u32> {
        let scale_factor = self.new_parallelism as f64 / self.original_parallelism as f64;
        let start_idx = (subtask_idx as f64 * scale_factor).floor() as u32;
        let end_idx = ((subtask_idx + 1) as f64 * scale_factor).ceil() as u32;

        (start_idx..end_idx).collect()
    }

    /// 计算收缩时的状态映射
    fn compute_scale_in_mapping(&self, subtask_idx: u32) -> Vec<u32> {
        let target_idx = subtask_idx * self.new_parallelism / self.original_parallelism;
        vec![target_idx]
    }

    /// 执行状态重分配
    pub async fn execute(&self) -> Result<()> {
        info!(
            "Executing state redistribution for job {} operator {} from parallelism {} to {}",
            self.job_id, self.operator_id, self.original_parallelism, self.new_parallelism
        );

        // 获取存储提供者
        let storage_provider = arroyo_storage::get_storage_provider().await?;

        // 获取状态后端
        let state_backend = arroyo_state::get_state_backend(&self.job_id).await?;

        // 加载检查点元数据
        let checkpoint_metadata = state_backend.load_checkpoint_metadata(&self.job_id, self.checkpoint_epoch).await?;

        // 获取操作符元数据
        let operator_metadata = checkpoint_metadata.operators.get(&self.operator_id)
            .ok_or_else(|| anyhow!("Operator metadata not found for operator {}", self.operator_id))?;

        // 对每个表执行状态重分配
        for (table_name, mapping) in &self.state_mapping {
            debug!(
                "Redistributing state for table {} according to mapping {:?}",
                table_name, mapping
            );

            // 获取表元数据
            let table_metadata = operator_metadata.table_checkpoint_metadata.get(table_name)
                .ok_or_else(|| anyhow!("Table metadata not found for table {}", table_name))?;

            // 对每个原始子任务执行状态重分配
            for (source_subtask, target_subtasks) in mapping {
                // 读取原始子任务的状态
                let state_path = format!(
                    "{}/{}/{}/{}/{}",
                    self.job_id, self.checkpoint_epoch, self.operator_id, table_name, source_subtask
                );

                // 检查状态是否存在
                if !storage_provider.exists(&state_path).await? {
                    debug!("No state found for subtask {} in table {}", source_subtask, table_name);
                    continue;
                }

                // 读取状态数据
                let state_data = storage_provider.get(&state_path).await?;

                // 对每个目标子任务写入状态
                for target_subtask in target_subtasks {
                    // 创建目标状态路径
                    let target_path = format!(
                        "{}/{}/{}/{}/{}",
                        self.job_id, self.checkpoint_epoch, self.operator_id, table_name, target_subtask
                    );

                    // 写入状态数据
                    // 注意：这里简化处理，直接复制状态数据
                    // 实际实现中，可能需要根据状态类型进行更复杂的重分配
                    storage_provider.put(&target_path, state_data.clone()).await?;

                    info!(
                        "Redistributed state from subtask {} to subtask {} for table {}",
                        source_subtask, target_subtask, table_name
                    );
                }
            }
        }

        // 更新检查点元数据，标记状态已重分配
        // 这里需要更新元数据，以便作业恢复时能够正确加载重分配后的状态
        // 实际实现中，可能需要更新更多元数据字段

        info!(
            "State redistribution completed for job {} operator {}",
            self.job_id, self.operator_id
        );

        Ok(())
    }
}
