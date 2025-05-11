use super::partition::PartitionManager;
use arroyo_rpc::api_types::partitions::{
    PartitionAssignmentConfig, PartitionAssignmentStrategy, PartitionRebalanceStrategy,
    PartitionRole, RebalanceTriggerConfig,
};
use arroyo_rpc::api_types::topics::TopicConfig;
use std::env;

#[cfg(test)]
mod tests {
    use super::*;

    // 获取测试服务器地址
    fn get_test_server() -> String {
        env::var("KAFKA_TEST_SERVER").unwrap_or_else(|_| "localhost:9092".to_string())
    }

    // 检查是否应该跳过需要 Kafka 服务器的测试
    fn should_skip_kafka_tests() -> bool {
        env::var("SKIP_KAFKA_TESTS").unwrap_or_else(|_| "true".to_string()) == "true"
    }

    // 生成唯一的 Topic 名称
    fn generate_topic_name(prefix: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        format!("{}-{}", prefix, timestamp)
    }

    #[tokio::test]
    async fn test_partition_manager_creation() {
        if should_skip_kafka_tests() {
            println!("Skipping test_partition_manager_creation because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 PartitionManager
        let server = get_test_server();
        let partition_manager = PartitionManager::new(&server, Some(10))
            .expect("Failed to create PartitionManager");

        // 获取分区分配配置
        let config = partition_manager.get_assignment_config().await;
        assert_eq!(config.assignment_strategy, PartitionAssignmentStrategy::RoundRobin);
        assert_eq!(config.rebalance_strategy, PartitionRebalanceStrategy::LoadBased);
        assert!(config.rebalance_trigger.enabled);
    }

    #[tokio::test]
    async fn test_partition_assignment_round_robin() {
        if should_skip_kafka_tests() {
            println!("Skipping test_partition_assignment_round_robin because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 PartitionManager
        let server = get_test_server();
        let partition_manager = PartitionManager::new(&server, Some(10))
            .expect("Failed to create PartitionManager");

        // 更新分区分配配置
        let config = PartitionAssignmentConfig {
            assignment_strategy: PartitionAssignmentStrategy::RoundRobin,
            rebalance_strategy: PartitionRebalanceStrategy::LoadBased,
            rebalance_trigger: RebalanceTriggerConfig {
                enabled: true,
                load_imbalance_threshold: 10.0,
                min_rebalance_interval_sec: 300,
                max_rebalance_interval_sec: 86400,
                rebalance_on_node_join: true,
                rebalance_on_node_leave: true,
                max_partitions_to_move: 10,
            },
        };
        partition_manager.update_assignment_config(config).await.unwrap();

        // 创建 Topic 配置
        let topic_config = TopicConfig {
            name: generate_topic_name("test-round-robin"),
            partitions: 6,
            replication_factor: 1,
            retention_ms: Some(86400000), // 1 day
            retention_bytes: None,
            cleanup_policy: "delete".to_string(),
            max_message_bytes: None,
            description: Some("Test topic for round robin partition assignment".to_string()),
        };

        // 模拟节点
        let nodes = vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
        ];

        // 分配分区
        let assignments = partition_manager
            .assign_partitions(&topic_config, Some(nodes.clone()))
            .await
            .expect("Failed to assign partitions");

        // 验证分配结果
        assert_eq!(assignments.len(), 6); // 6 个分区，每个分区 1 个副本

        // 验证轮询分配
        let mut node_counts = vec![0, 0, 0];
        for assignment in &assignments {
            let node_idx = nodes.iter().position(|n| n == &assignment.node_id).unwrap();
            node_counts[node_idx] += 1;
        }

        // 验证分区均匀分配
        assert_eq!(node_counts[0], 2); // 节点 1 分配了 2 个分区
        assert_eq!(node_counts[1], 2); // 节点 2 分配了 2 个分区
        assert_eq!(node_counts[2], 2); // 节点 3 分配了 2 个分区
    }

    #[tokio::test]
    async fn test_partition_assignment_random() {
        if should_skip_kafka_tests() {
            println!("Skipping test_partition_assignment_random because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 PartitionManager
        let server = get_test_server();
        let partition_manager = PartitionManager::new(&server, Some(10))
            .expect("Failed to create PartitionManager");

        // 更新分区分配配置
        let config = PartitionAssignmentConfig {
            assignment_strategy: PartitionAssignmentStrategy::Random,
            rebalance_strategy: PartitionRebalanceStrategy::LoadBased,
            rebalance_trigger: RebalanceTriggerConfig {
                enabled: true,
                load_imbalance_threshold: 10.0,
                min_rebalance_interval_sec: 300,
                max_rebalance_interval_sec: 86400,
                rebalance_on_node_join: true,
                rebalance_on_node_leave: true,
                max_partitions_to_move: 10,
            },
        };
        partition_manager.update_assignment_config(config).await.unwrap();

        // 创建 Topic 配置
        let topic_config = TopicConfig {
            name: generate_topic_name("test-random"),
            partitions: 10,
            replication_factor: 1,
            retention_ms: Some(86400000), // 1 day
            retention_bytes: None,
            cleanup_policy: "delete".to_string(),
            max_message_bytes: None,
            description: Some("Test topic for random partition assignment".to_string()),
        };

        // 模拟节点
        let nodes = vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
        ];

        // 分配分区
        let assignments = partition_manager
            .assign_partitions(&topic_config, Some(nodes.clone()))
            .await
            .expect("Failed to assign partitions");

        // 验证分配结果
        assert_eq!(assignments.len(), 10); // 10 个分区，每个分区 1 个副本

        // 验证所有分区都有分配
        let mut assigned_partitions = vec![false; 10];
        for assignment in &assignments {
            assigned_partitions[assignment.partition_id as usize] = true;
        }
        assert!(assigned_partitions.iter().all(|&assigned| assigned));
    }

    #[tokio::test]
    async fn test_partition_assignment_with_replication() {
        if should_skip_kafka_tests() {
            println!("Skipping test_partition_assignment_with_replication because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 PartitionManager
        let server = get_test_server();
        let partition_manager = PartitionManager::new(&server, Some(10))
            .expect("Failed to create PartitionManager");

        // 创建 Topic 配置（带副本）
        let topic_config = TopicConfig {
            name: generate_topic_name("test-replication"),
            partitions: 3,
            replication_factor: 2, // 每个分区 2 个副本
            retention_ms: Some(86400000), // 1 day
            retention_bytes: None,
            cleanup_policy: "delete".to_string(),
            max_message_bytes: None,
            description: Some("Test topic for partition assignment with replication".to_string()),
        };

        // 模拟节点
        let nodes = vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
        ];

        // 分配分区
        let assignments = partition_manager
            .assign_partitions(&topic_config, Some(nodes.clone()))
            .await
            .expect("Failed to assign partitions");

        // 验证分配结果
        assert_eq!(assignments.len(), 6); // 3 个分区，每个分区 2 个副本

        // 验证每个分区都有一个领导者和一个跟随者
        let mut leader_counts = vec![0; 3];
        let mut follower_counts = vec![0; 3];

        for assignment in &assignments {
            match assignment.role {
                PartitionRole::Leader => leader_counts[assignment.partition_id as usize] += 1,
                PartitionRole::Follower => follower_counts[assignment.partition_id as usize] += 1,
            }
        }

        // 验证每个分区都有一个领导者和一个跟随者
        for i in 0..3 {
            assert_eq!(leader_counts[i], 1, "Partition {} should have 1 leader", i);
            assert_eq!(follower_counts[i], 1, "Partition {} should have 1 follower", i);
        }

        // 验证同一个分区的领导者和跟随者不在同一个节点上
        for partition_id in 0..3 {
            let leader_node = assignments
                .iter()
                .find(|a| a.partition_id == partition_id && a.role == PartitionRole::Leader)
                .map(|a| a.node_id.clone())
                .unwrap();

            let follower_node = assignments
                .iter()
                .find(|a| a.partition_id == partition_id && a.role == PartitionRole::Follower)
                .map(|a| a.node_id.clone())
                .unwrap();

            assert_ne!(leader_node, follower_node, "Leader and follower should not be on the same node for partition {}", partition_id);
        }
    }

    #[tokio::test]
    async fn test_cluster_balance_status() {
        if should_skip_kafka_tests() {
            println!("Skipping test_cluster_balance_status because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 PartitionManager
        let server = get_test_server();
        let partition_manager = PartitionManager::new(&server, Some(10))
            .expect("Failed to create PartitionManager");

        // 获取集群平衡状态
        let balance_status = partition_manager
            .get_cluster_balance_status(true)
            .await
            .expect("Failed to get cluster balance status");

        // 验证平衡状态
        println!("Cluster balance status: {:?}", balance_status.is_balanced);
        println!("Imbalance percent: {:.2}%", balance_status.imbalance_percent);
        println!("Node loads: {:?}", balance_status.node_loads);
        println!("Suggested migrations: {:?}", balance_status.suggested_migrations);
    }
}
