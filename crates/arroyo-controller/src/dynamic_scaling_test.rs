#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use crate::dynamic_scaling::{ScalingOperation, ScalingPlan, StateRedistributionPlan};

    #[test]
    fn test_scaling_plan_creation() {
        // 测试扩展计划创建
        let job_id = "test-job".to_string();
        let epoch = 10;
        
        // 原始并行度
        let mut old_parallelism = HashMap::new();
        old_parallelism.insert(1, 2);
        old_parallelism.insert(2, 4);
        old_parallelism.insert(3, 1);
        
        // 新并行度
        let mut new_parallelism = HashMap::new();
        new_parallelism.insert(1, 4); // 扩展
        new_parallelism.insert(2, 2); // 收缩
        new_parallelism.insert(3, 1); // 不变
        
        // 创建扩展计划
        let plan = ScalingPlan::new(job_id.clone(), epoch, &old_parallelism, &new_parallelism);
        
        // 验证计划内容
        assert_eq!(plan.job_id, job_id);
        assert_eq!(plan.epoch, epoch);
        assert_eq!(plan.operations.len(), 2); // 应该有两个操作（一个扩展，一个收缩）
        
        // 验证操作类型
        let mut scale_out_count = 0;
        let mut scale_in_count = 0;
        
        for op in &plan.operations {
            match op {
                ScalingOperation::ScaleOut { operator_id, original_parallelism, new_parallelism } => {
                    assert_eq!(*operator_id, "1");
                    assert_eq!(*original_parallelism, 2);
                    assert_eq!(*new_parallelism, 4);
                    scale_out_count += 1;
                }
                ScalingOperation::ScaleIn { operator_id, original_parallelism, new_parallelism } => {
                    assert_eq!(*operator_id, "2");
                    assert_eq!(*original_parallelism, 4);
                    assert_eq!(*new_parallelism, 2);
                    scale_in_count += 1;
                }
            }
        }
        
        assert_eq!(scale_out_count, 1);
        assert_eq!(scale_in_count, 1);
        
        // 验证 has_operations 方法
        assert!(plan.has_operations());
        
        // 验证 get_affected_operators 方法
        let affected_operators = plan.get_affected_operators();
        assert_eq!(affected_operators.len(), 2);
        assert!(affected_operators.contains("1"));
        assert!(affected_operators.contains("2"));
    }
    
    #[test]
    fn test_empty_scaling_plan() {
        // 测试没有变化时的扩展计划
        let job_id = "test-job".to_string();
        let epoch = 10;
        
        // 原始并行度
        let mut old_parallelism = HashMap::new();
        old_parallelism.insert(1, 2);
        old_parallelism.insert(2, 4);
        
        // 相同的并行度
        let mut new_parallelism = HashMap::new();
        new_parallelism.insert(1, 2);
        new_parallelism.insert(2, 4);
        
        // 创建扩展计划
        let plan = ScalingPlan::new(job_id.clone(), epoch, &old_parallelism, &new_parallelism);
        
        // 验证计划内容
        assert_eq!(plan.job_id, job_id);
        assert_eq!(plan.epoch, epoch);
        assert_eq!(plan.operations.len(), 0); // 应该没有操作
        
        // 验证 has_operations 方法
        assert!(!plan.has_operations());
        
        // 验证 get_affected_operators 方法
        let affected_operators = plan.get_affected_operators();
        assert_eq!(affected_operators.len(), 0);
    }
    
    #[test]
    fn test_state_redistribution_plan() {
        // 测试状态重分配计划
        let job_id = "test-job".to_string();
        let checkpoint_epoch = 10;
        let operator_id = "test-operator".to_string();
        let original_parallelism = 2;
        let new_parallelism = 4;
        
        // 创建状态重分配计划
        let mut plan = StateRedistributionPlan::new(
            job_id.clone(),
            checkpoint_epoch,
            operator_id.clone(),
            original_parallelism,
            new_parallelism,
        );
        
        // 验证计划内容
        assert_eq!(plan.job_id, job_id);
        assert_eq!(plan.checkpoint_epoch, checkpoint_epoch);
        assert_eq!(plan.operator_id, operator_id);
        assert_eq!(plan.original_parallelism, original_parallelism);
        assert_eq!(plan.new_parallelism, new_parallelism);
        assert!(plan.state_mapping.is_empty());
        
        // 计算状态映射
        let table_names = vec!["table1".to_string(), "table2".to_string()];
        plan.compute_state_mapping(&table_names).unwrap();
        
        // 验证状态映射
        assert_eq!(plan.state_mapping.len(), 2);
        assert!(plan.state_mapping.contains_key("table1"));
        assert!(plan.state_mapping.contains_key("table2"));
        
        // 验证扩展映射
        let table1_mapping = plan.state_mapping.get("table1").unwrap();
        assert_eq!(table1_mapping.len(), 2); // 原始并行度为2
        
        // 验证第一个子任务的映射
        let subtask0_targets = table1_mapping.get(&0).unwrap();
        assert_eq!(subtask0_targets.len(), 2); // 应该映射到2个新子任务
        assert_eq!(subtask0_targets[0], 0);
        assert_eq!(subtask0_targets[1], 1);
        
        // 验证第二个子任务的映射
        let subtask1_targets = table1_mapping.get(&1).unwrap();
        assert_eq!(subtask1_targets.len(), 2); // 应该映射到2个新子任务
        assert_eq!(subtask1_targets[0], 2);
        assert_eq!(subtask1_targets[1], 3);
    }
    
    #[test]
    fn test_scale_in_mapping() {
        // 测试收缩映射
        let job_id = "test-job".to_string();
        let checkpoint_epoch = 10;
        let operator_id = "test-operator".to_string();
        let original_parallelism = 4;
        let new_parallelism = 2;
        
        // 创建状态重分配计划
        let mut plan = StateRedistributionPlan::new(
            job_id.clone(),
            checkpoint_epoch,
            operator_id.clone(),
            original_parallelism,
            new_parallelism,
        );
        
        // 计算状态映射
        let table_names = vec!["table1".to_string()];
        plan.compute_state_mapping(&table_names).unwrap();
        
        // 验证状态映射
        let table1_mapping = plan.state_mapping.get("table1").unwrap();
        assert_eq!(table1_mapping.len(), 4); // 原始并行度为4
        
        // 验证子任务映射
        assert_eq!(*table1_mapping.get(&0).unwrap(), vec![0]);
        assert_eq!(*table1_mapping.get(&1).unwrap(), vec![0]);
        assert_eq!(*table1_mapping.get(&2).unwrap(), vec![1]);
        assert_eq!(*table1_mapping.get(&3).unwrap(), vec![1]);
    }
}
