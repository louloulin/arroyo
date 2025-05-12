#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use anyhow::Result;

    use crate::two_phase_commit::{TransactionState, TwoPhaseCommitCoordinator};

    #[tokio::test]
    async fn test_two_phase_commit_coordinator() -> Result<()> {
        // 创建参与者列表
        let mut participants = HashSet::new();
        participants.insert(("operator1".to_string(), 0));
        participants.insert(("operator1".to_string(), 1));
        participants.insert(("operator2".to_string(), 0));

        // 创建提交数据
        let mut commit_data = HashMap::new();
        let mut operator1_data = HashMap::new();
        let mut table1_data = HashMap::new();
        table1_data.insert(0, vec![1, 2, 3]);
        table1_data.insert(1, vec![4, 5, 6]);
        operator1_data.insert("table1".to_string(), table1_data);
        commit_data.insert("operator1".to_string(), operator1_data);

        // 创建两阶段提交协调器
        let mut coordinator = TwoPhaseCommitCoordinator::new(
            "tx-1".to_string(),
            "checkpoint-1".to_string(),
            1,
            participants.clone(),
            commit_data,
        );

        // 验证初始状态
        assert_eq!(coordinator.transaction_id(), "tx-1");
        assert_eq!(coordinator.checkpoint_id(), "checkpoint-1");
        assert_eq!(*coordinator.state(), TransactionState::Preparing);
        assert!(!coordinator.all_participants_prepared());

        // 准备阶段
        coordinator.participant_prepared("operator1".to_string(), 0)?;
        assert!(!coordinator.all_participants_prepared());

        coordinator.participant_prepared("operator1".to_string(), 1)?;
        assert!(!coordinator.all_participants_prepared());

        coordinator.participant_prepared("operator2".to_string(), 0)?;
        assert!(coordinator.all_participants_prepared());

        // 开始提交阶段
        coordinator.begin_commit()?;
        assert_eq!(*coordinator.state(), TransactionState::Committing);
        assert!(!coordinator.all_participants_committed());

        // 提交阶段
        coordinator.participant_committed("operator1".to_string(), 0)?;
        assert!(!coordinator.all_participants_committed());

        coordinator.participant_committed("operator1".to_string(), 1)?;
        assert!(!coordinator.all_participants_committed());

        coordinator.participant_committed("operator2".to_string(), 0)?;
        assert!(coordinator.all_participants_committed());

        // 完成事务
        coordinator.complete()?;
        assert_eq!(*coordinator.state(), TransactionState::Completed);

        Ok(())
    }

    #[tokio::test]
    async fn test_two_phase_commit_abort() -> Result<()> {
        // 创建参与者列表
        let mut participants = HashSet::new();
        participants.insert(("operator1".to_string(), 0));
        participants.insert(("operator2".to_string(), 0));

        // 创建两阶段提交协调器
        let mut coordinator = TwoPhaseCommitCoordinator::new(
            "tx-2".to_string(),
            "checkpoint-2".to_string(),
            2,
            participants,
            HashMap::new(),
        );

        // 准备阶段
        coordinator.participant_prepared("operator1".to_string(), 0)?;
        assert!(!coordinator.all_participants_prepared());

        // 中止事务
        coordinator.abort();
        assert_eq!(*coordinator.state(), TransactionState::Aborting);

        // 尝试在中止状态下准备参与者，应该失败
        let result = coordinator.participant_prepared("operator2".to_string(), 0);
        assert!(result.is_err());

        // 尝试在中止状态下开始提交，应该失败
        let result = coordinator.begin_commit();
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_two_phase_commit_error_handling() -> Result<()> {
        // 创建参与者列表
        let mut participants = HashSet::new();
        participants.insert(("operator1".to_string(), 0));

        // 创建两阶段提交协调器
        let mut coordinator = TwoPhaseCommitCoordinator::new(
            "tx-3".to_string(),
            "checkpoint-3".to_string(),
            3,
            participants,
            HashMap::new(),
        );

        // 尝试准备不存在的参与者，应该失败
        let result = coordinator.participant_prepared("operator2".to_string(), 0);
        assert!(result.is_err());

        // 尝试在未准备好所有参与者的情况下开始提交，应该失败
        let result = coordinator.begin_commit();
        assert!(result.is_err());

        // 准备参与者
        coordinator.participant_prepared("operator1".to_string(), 0)?;

        // 开始提交
        coordinator.begin_commit()?;

        // 尝试提交不存在的参与者，应该失败
        let result = coordinator.participant_committed("operator2".to_string(), 0);
        assert!(result.is_err());

        // 尝试在未提交所有参与者的情况下完成事务，应该失败
        let result = coordinator.complete();
        assert!(result.is_err());

        Ok(())
    }
}
