use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arroyo_types::TaskInfo;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::SystemTime;

/// 两阶段提交策略
#[derive(Debug, Clone, Copy)]
pub enum CommitStrategy {
    /// 每个子任务作为提交者，将预提交消息写入状态以便恢复
    PerSubtask,
    /// 每个算子使用子任务0作为提交者，通过控制系统/检查点元数据传递所有预提交数据
    PerOperator,
}

/// 两阶段提交接口
///
/// 该接口定义了流处理系统中两阶段提交的接口，负责以容错方式将记录提交到持久存储。
/// 两阶段提交协议用于确保在发生故障时所有记录要么被提交，要么被回滚。
///
/// 接口定义了初始化提交者、插入记录、提交记录和执行检查点的方法。
/// 实现此接口的类型必须是 `Send` 和 `'static`。
///
/// 接口有两个关联类型：`DataRecovery`，表示在发生故障时可以恢复的数据类型；
/// 和 `PreCommit`，表示在最终提交前预提交的数据类型。
#[async_trait]
pub trait TwoPhaseCommitSink: Send + 'static {
    /// 恢复数据类型
    type DataRecovery: Send + Sync + Clone + Debug + 'static;

    /// 预提交数据类型
    type PreCommit: Send + Sync + Clone + Debug + 'static;

    /// 获取接收器名称
    fn name(&self) -> String;

    /// 初始化接收器
    ///
    /// 在启动时调用，用于初始化接收器并恢复任何之前的状态
    async fn init(
        &mut self,
        task_info: &TaskInfo,
        data_recovery: Vec<Self::DataRecovery>,
    ) -> Result<()>;

    /// 插入批次数据
    ///
    /// 处理一批输入记录
    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// 提交阶段
    ///
    /// 在两阶段提交的提交阶段调用，用于最终提交预提交的数据
    async fn commit(
        &mut self,
        task_info: &TaskInfo,
        pre_commit: Vec<Self::PreCommit>,
    ) -> Result<()>;

    /// 检查点阶段
    ///
    /// 在检查点创建时调用，用于准备提交数据并返回恢复数据和预提交数据
    async fn checkpoint(
        &mut self,
        task_info: &TaskInfo,
        watermark: Option<SystemTime>,
        stopping: bool,
    ) -> Result<(Self::DataRecovery, HashMap<String, Self::PreCommit>)>;

    /// 获取提交策略
    fn commit_strategy(&self) -> CommitStrategy {
        CommitStrategy::PerSubtask
    }

    /// 中止提交
    ///
    /// 在提交失败时调用，用于清理任何部分提交的状态
    async fn abort(&mut self, task_info: &TaskInfo) -> Result<()> {
        // 默认实现为空，子类可以根据需要重写
        Ok(())
    }
}

/// 两阶段提交接收器操作符
///
/// 包装实现了 `TwoPhaseCommitSink` 接口的接收器，提供与 Arroyo 操作符系统的集成
pub struct TwoPhaseCommitSinkOperator<S: TwoPhaseCommitSink> {
    /// 内部接收器实现
    sink: S,
    /// 预提交数据
    pre_commits: Vec<S::PreCommit>,
    /// 事务ID
    transaction_id: Option<String>,
}

impl<S: TwoPhaseCommitSink> TwoPhaseCommitSinkOperator<S> {
    /// 创建新的两阶段提交接收器操作符
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            pre_commits: Vec::new(),
            transaction_id: None,
        }
    }

    /// 获取内部接收器
    pub fn inner(&self) -> &S {
        &self.sink
    }

    /// 获取内部接收器的可变引用
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.sink
    }

    /// 设置事务ID
    pub fn set_transaction_id(&mut self, transaction_id: String) {
        self.transaction_id = Some(transaction_id);
    }

    /// 获取事务ID
    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }
}

/// 为两阶段提交接收器操作符实现 ArrowOperator 接口
///
/// 这个实现将在 arroyo-connectors 包中完成，因为它需要访问 ArrowOperator 接口
/// 这里只是一个占位符，表示需要实现的内容
#[cfg(test)]
mod tests {
    // 测试代码将在这里实现
}
