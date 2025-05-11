use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use std::collections::HashMap;

/// 分区分配策略
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum PartitionAssignmentStrategy {
    /// 轮询分配策略
    RoundRobin,
    /// 随机分配策略
    Random,
    /// 基于负载的分配策略
    LoadBased,
    /// 基于机架感知的分配策略
    RackAware,
}

/// 分区再平衡策略
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum PartitionRebalanceStrategy {
    /// 不再平衡
    None,
    /// 基于负载的再平衡策略
    LoadBased,
    /// 基于机架感知的再平衡策略
    RackAware,
}

/// 分区再平衡触发条件
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceTriggerConfig {
    /// 是否启用自动再平衡
    pub enabled: bool,
    /// 负载不平衡阈值（百分比）
    pub load_imbalance_threshold: f64,
    /// 最小再平衡间隔（秒）
    pub min_rebalance_interval_sec: u64,
    /// 最大再平衡间隔（秒）
    pub max_rebalance_interval_sec: u64,
    /// 节点加入后是否触发再平衡
    pub rebalance_on_node_join: bool,
    /// 节点离开后是否触发再平衡
    pub rebalance_on_node_leave: bool,
    /// 最大迁移分区数量
    pub max_partitions_to_move: u32,
}

/// 分区分配配置
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionAssignmentConfig {
    /// 分区分配策略
    pub assignment_strategy: PartitionAssignmentStrategy,
    /// 分区再平衡策略
    pub rebalance_strategy: PartitionRebalanceStrategy,
    /// 再平衡触发条件
    pub rebalance_trigger: RebalanceTriggerConfig,
}

/// 分区分配信息
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionAssignment {
    /// 分区 ID
    pub partition_id: i32,
    /// 节点 ID
    pub node_id: String,
    /// 副本角色（Leader 或 Follower）
    pub role: PartitionRole,
    /// 分配时间
    pub assigned_at: u64,
}

/// 分区角色
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum PartitionRole {
    /// 领导者副本
    Leader,
    /// 跟随者副本
    Follower,
}

/// 分区状态
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum PartitionStatus {
    /// 正常
    Normal,
    /// 再平衡中
    Rebalancing,
    /// 迁移中
    Migrating,
    /// 恢复中
    Recovering,
    /// 离线
    Offline,
}

/// 分区详细信息
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionDetails {
    /// 分区 ID
    pub id: i32,
    /// Topic 名称
    pub topic_name: String,
    /// 分区状态
    pub status: PartitionStatus,
    /// 分区大小（字节）
    pub size_bytes: u64,
    /// 分区消息数量
    pub message_count: u64,
    /// 最早偏移量
    pub earliest_offset: i64,
    /// 最新偏移量
    pub latest_offset: i64,
    /// 分区副本分配
    pub replicas: Vec<PartitionAssignment>,
    /// 分区指标
    pub metrics: PartitionMetrics,
}

/// 分区指标
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionMetrics {
    /// 生产速率（字节/秒）
    pub bytes_in_per_sec: f64,
    /// 消费速率（字节/秒）
    pub bytes_out_per_sec: f64,
    /// 消息生产速率（消息/秒）
    pub messages_in_per_sec: f64,
    /// 副本滞后（消息数）
    pub replica_lag: HashMap<String, i64>,
}

/// 节点负载信息
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeLoad {
    /// 节点 ID
    pub node_id: String,
    /// 分区数量
    pub partition_count: u32,
    /// 领导者分区数量
    pub leader_partition_count: u32,
    /// 总分区大小（字节）
    pub total_size_bytes: u64,
    /// 总消息数量
    pub total_message_count: u64,
    /// 总生产速率（字节/秒）
    pub total_bytes_in_per_sec: f64,
    /// 总消费速率（字节/秒）
    pub total_bytes_out_per_sec: f64,
    /// CPU 使用率（百分比）
    pub cpu_usage_percent: f64,
    /// 内存使用率（百分比）
    pub memory_usage_percent: f64,
    /// 磁盘使用率（百分比）
    pub disk_usage_percent: f64,
    /// 网络使用率（百分比）
    pub network_usage_percent: f64,
}

/// 分区迁移计划
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionMigrationPlan {
    /// 分区 ID
    pub partition_id: i32,
    /// Topic 名称
    pub topic_name: String,
    /// 源节点 ID
    pub source_node_id: String,
    /// 目标节点 ID
    pub target_node_id: String,
    /// 迁移优先级
    pub priority: u32,
    /// 估计迁移大小（字节）
    pub estimated_size_bytes: u64,
    /// 估计迁移时间（秒）
    pub estimated_duration_sec: u64,
}

/// 集群平衡状态
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterBalanceStatus {
    /// 是否平衡
    pub is_balanced: bool,
    /// 不平衡程度（百分比）
    pub imbalance_percent: f64,
    /// 节点负载信息
    pub node_loads: Vec<NodeLoad>,
    /// 建议的迁移计划
    pub suggested_migrations: Vec<PartitionMigrationPlan>,
    /// 上次再平衡时间
    pub last_rebalance_time: Option<u64>,
    /// 下次再平衡时间
    pub next_rebalance_time: Option<u64>,
}

/// 获取分区详情请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetPartitionDetailsRequest {
    /// Topic 名称
    pub topic_name: String,
    /// 分区 ID
    pub partition_id: i32,
}

/// 获取集群平衡状态请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterBalanceStatusRequest {
    /// 是否包含迁移计划
    pub include_migration_plan: bool,
}

/// 触发分区再平衡请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TriggerRebalanceRequest {
    /// 是否执行迁移（否则只生成计划）
    pub execute: bool,
    /// 最大迁移分区数量
    pub max_partitions_to_move: Option<u32>,
    /// 特定 Topic 列表（为空则所有 Topic）
    pub topics: Option<Vec<String>>,
}

/// 迁移分区请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigratePartitionRequest {
    /// Topic 名称
    pub topic_name: String,
    /// 分区 ID
    pub partition_id: i32,
    /// 目标节点 ID
    pub target_node_id: String,
}

/// 迁移状态
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum MigrationStatus {
    /// 计划中
    Planned,
    /// 进行中
    InProgress,
    /// 已完成
    Completed,
    /// 失败
    Failed,
}

/// 迁移详情
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigrationDetails {
    /// 迁移 ID
    pub migration_id: String,
    /// Topic 名称
    pub topic_name: String,
    /// 分区 ID
    pub partition_id: i32,
    /// 源节点 ID
    pub source_node_id: String,
    /// 目标节点 ID
    pub target_node_id: String,
    /// 迁移状态
    pub status: MigrationStatus,
    /// 进度（百分比）
    pub progress_percent: f64,
    /// 已迁移字节数
    pub bytes_transferred: u64,
    /// 总字节数
    pub total_bytes: u64,
    /// 开始时间
    pub start_time: u64,
    /// 完成时间
    pub end_time: Option<u64>,
    /// 错误信息
    pub error_message: Option<String>,
}
