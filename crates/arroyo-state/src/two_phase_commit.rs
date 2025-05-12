use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use anyhow::{anyhow, Result};
use arroyo_rpc::grpc::rpc::{
    OperatorCommitData, TableCommitData,
};
use tracing::{debug, info, warn};

/// 事务状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    /// 准备阶段
    Preparing,
    /// 提交阶段
    Committing,
    /// 中止阶段
    Aborting,
    /// 已完成
    Completed,
    /// 已中止
    Aborted,
}

/// 两阶段提交协调器
/// 负责协调分布式事务的两阶段提交过程
#[derive(Debug)]
pub struct TwoPhaseCommitCoordinator {
    /// 事务ID
    transaction_id: String,
    /// 检查点ID
    checkpoint_id: String,
    /// 检查点epoch
    epoch: u32,
    /// 事务状态
    state: TransactionState,
    /// 开始时间
    start_time: SystemTime,
    /// 参与者列表 (operator_id, subtask_index)
    participants: HashSet<(String, u32)>,
    /// 已准备好的参与者
    prepared_participants: HashSet<(String, u32)>,
    /// 已提交的参与者
    committed_participants: HashSet<(String, u32)>,
    /// 提交数据 operator_id -> table_name -> subtask_index -> data
    commit_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>,
}

impl TwoPhaseCommitCoordinator {
    /// 创建新的两阶段提交协调器
    pub fn new(
        transaction_id: String,
        checkpoint_id: String,
        epoch: u32,
        participants: HashSet<(String, u32)>,
        commit_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>,
    ) -> Self {
        Self {
            transaction_id,
            checkpoint_id,
            epoch,
            state: TransactionState::Preparing,
            start_time: SystemTime::now(),
            participants,
            prepared_participants: HashSet::new(),
            committed_participants: HashSet::new(),
            commit_data,
        }
    }

    /// 获取事务ID
    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    /// 获取检查点ID
    pub fn checkpoint_id(&self) -> &str {
        &self.checkpoint_id
    }

    /// 获取事务状态
    pub fn state(&self) -> &TransactionState {
        &self.state
    }

    /// 获取开始时间
    pub fn start_time(&self) -> SystemTime {
        self.start_time
    }

    /// 参与者准备完成
    pub fn participant_prepared(&mut self, operator_id: String, subtask_index: u32) -> Result<()> {
        let participant = (operator_id.clone(), subtask_index);

        if !self.participants.contains(&participant) {
            return Err(anyhow!(
                "Participant ({}, {}) not in transaction {}",
                operator_id,
                subtask_index,
                self.transaction_id
            ));
        }

        if self.state != TransactionState::Preparing {
            return Err(anyhow!(
                "Cannot prepare participant in state {:?}",
                self.state
            ));
        }

        self.prepared_participants.insert(participant);

        debug!(
            "Participant prepared: ({}, {}), transaction: {}, prepared: {}/{}",
            operator_id,
            subtask_index,
            self.transaction_id,
            self.prepared_participants.len(),
            self.participants.len()
        );

        Ok(())
    }

    /// 检查是否所有参与者都已准备好
    pub fn all_participants_prepared(&self) -> bool {
        self.prepared_participants.len() == self.participants.len()
    }

    /// 开始提交阶段
    pub fn begin_commit(&mut self) -> Result<()> {
        if self.state != TransactionState::Preparing {
            return Err(anyhow!(
                "Cannot begin commit in state {:?}",
                self.state
            ));
        }

        if !self.all_participants_prepared() {
            return Err(anyhow!(
                "Cannot begin commit when not all participants are prepared"
            ));
        }

        self.state = TransactionState::Committing;
        info!(
            "Beginning commit phase for transaction {}, participants: {}",
            self.transaction_id,
            self.participants.len()
        );

        Ok(())
    }

    /// 参与者提交完成
    pub fn participant_committed(&mut self, operator_id: String, subtask_index: u32) -> Result<()> {
        let participant = (operator_id.clone(), subtask_index);

        if !self.participants.contains(&participant) {
            return Err(anyhow!(
                "Participant ({}, {}) not in transaction {}",
                operator_id,
                subtask_index,
                self.transaction_id
            ));
        }

        if self.state != TransactionState::Committing {
            return Err(anyhow!(
                "Cannot commit participant in state {:?}",
                self.state
            ));
        }

        self.committed_participants.insert(participant);

        debug!(
            "Participant committed: ({}, {}), transaction: {}, committed: {}/{}",
            operator_id,
            subtask_index,
            self.transaction_id,
            self.committed_participants.len(),
            self.participants.len()
        );

        Ok(())
    }

    /// 处理提交完成请求
    pub fn handle_commit_completed(&mut self, operator_id: String, subtask_index: u32) -> Result<()> {
        self.participant_committed(operator_id, subtask_index)
    }

    /// 检查是否所有参与者都已提交
    pub fn all_participants_committed(&self) -> bool {
        self.committed_participants.len() == self.participants.len()
    }

    /// 完成事务
    pub fn complete(&mut self) -> Result<()> {
        if self.state != TransactionState::Committing {
            return Err(anyhow!(
                "Cannot complete transaction in state {:?}",
                self.state
            ));
        }

        if !self.all_participants_committed() {
            return Err(anyhow!(
                "Cannot complete transaction when not all participants are committed"
            ));
        }

        self.state = TransactionState::Completed;
        info!(
            "Transaction {} completed successfully",
            self.transaction_id
        );

        Ok(())
    }

    /// 中止事务
    pub fn abort(&mut self) {
        match self.state {
            TransactionState::Completed | TransactionState::Aborted => {
                warn!(
                    "Cannot abort transaction {} in state {:?}",
                    self.transaction_id, self.state
                );
                return;
            }
            _ => {
                self.state = TransactionState::Aborting;
                info!("Aborting transaction {}", self.transaction_id);
            }
        }
    }

    /// 获取提交数据
    pub fn commit_data(&self) -> HashMap<String, OperatorCommitData> {
        let operators_to_commit: HashSet<_> = self
            .participants
            .iter()
            .map(|(operator_id, _subtask_id)| operator_id.clone())
            .collect();

        operators_to_commit
            .into_iter()
            .map(|node_id| {
                let committing_data = self
                    .commit_data
                    .get(&node_id)
                    .map(|table_map| {
                        table_map
                            .iter()
                            .map(|(table_name, subtask_to_commit_data)| {
                                (
                                    table_name.clone(),
                                    TableCommitData {
                                        commit_data_by_subtask: subtask_to_commit_data.clone(),
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (node_id, OperatorCommitData { committing_data })
            })
            .collect()
    }
}
