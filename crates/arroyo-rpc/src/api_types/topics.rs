use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Topic 配置选项
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfig {
    /// Topic 名称
    pub name: String,

    /// 分区数量
    #[serde(default = "default_partitions")]
    pub partitions: i32,

    /// 副本因子
    #[serde(default = "default_replication_factor")]
    pub replication_factor: i16,

    /// 保留策略（毫秒）
    #[serde(default)]
    pub retention_ms: Option<i64>,

    /// 保留策略（字节）
    #[serde(default)]
    pub retention_bytes: Option<i64>,

    /// 清理策略（delete 或 compact）
    #[serde(default = "default_cleanup_policy")]
    pub cleanup_policy: String,

    /// 最大消息大小（字节）
    #[serde(default)]
    pub max_message_bytes: Option<i32>,

    /// 描述信息
    #[serde(default)]
    pub description: Option<String>,
}

fn default_partitions() -> i32 {
    1
}

fn default_replication_factor() -> i16 {
    1
}

fn default_cleanup_policy() -> String {
    "delete".to_string()
}

/// Topic 创建请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateTopicRequest {
    /// Topic 配置
    pub config: TopicConfig,
}

/// Topic 更新请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTopicRequest {
    /// Topic 配置
    pub config: TopicConfig,
}

/// Topic 删除请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTopicRequest {
    /// Topic 名称
    pub name: String,
}

/// Topic 信息
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicInfo {
    /// Topic 名称
    pub name: String,

    /// 分区数量
    pub partitions: i32,

    /// 副本因子
    pub replication_factor: i16,

    /// 保留策略（毫秒）
    pub retention_ms: Option<i64>,

    /// 保留策略（字节）
    pub retention_bytes: Option<i64>,

    /// 清理策略
    pub cleanup_policy: String,

    /// 最大消息大小（字节）
    pub max_message_bytes: Option<i32>,

    /// 描述信息
    pub description: Option<String>,

    /// 创建时间
    pub created_at: u64,

    /// 更新时间
    pub updated_at: u64,
}

/// Topic 分区信息
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicPartitionInfo {
    /// 分区 ID
    pub id: i32,

    /// 领导副本 ID
    pub leader: i32,

    /// 副本列表
    pub replicas: Vec<i32>,

    /// 同步副本列表
    pub isr: Vec<i32>,
}

/// Topic 详细信息
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicDetails {
    /// 基本信息
    #[serde(flatten)]
    pub info: TopicInfo,

    /// 分区信息
    pub partitions: Vec<TopicPartitionInfo>,

    /// 消息数量
    pub message_count: u64,

    /// 存储大小（字节）
    pub size_bytes: u64,
}

/// Topic 列表响应
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicListResponse {
    /// Topic 列表
    pub topics: Vec<TopicInfo>,
}

/// Topic 详情响应
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicDetailsResponse {
    /// Topic 详情
    pub topic: TopicDetails,
}

/// Topic 分区状态
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TopicPartitionStatus {
    /// 健康
    Healthy,
    /// 降级
    Degraded,
    /// 不健康
    Unhealthy,
}

/// Topic 健康状态
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TopicHealthStatus {
    /// 健康
    Healthy,
    /// 降级
    Degraded,
    /// 不健康
    Unhealthy,
}

/// Topic 分区健康信息
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicPartitionHealth {
    /// 分区 ID
    pub partition_id: i32,
    /// 状态
    pub status: TopicPartitionStatus,
    /// 领导者 ID
    pub leader_id: i32,
    /// 副本数量
    pub replica_count: i32,
    /// 同步副本数量
    pub isr_count: i32,
    /// 问题列表
    pub issues: Vec<String>,
}

/// Topic 健康信息
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicHealth {
    /// Topic 名称
    pub topic_name: String,
    /// 状态
    pub status: TopicHealthStatus,
    /// 分区数量
    pub partition_count: i32,
    /// 分区健康信息
    pub partitions: Vec<TopicPartitionHealth>,
    /// 问题列表
    pub issues: Vec<String>,
    /// 检查时间
    pub checked_at: u64,
}

/// Topic 健康检查请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicHealthCheckRequest {
    /// Topic 名称列表
    pub topics: Option<Vec<String>>,
    /// 是否强制刷新
    #[serde(default)]
    pub force_refresh: bool,
}

/// Topic 健康检查响应
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicHealthCheckResponse {
    /// Topic 健康信息列表
    pub topics: Vec<TopicHealth>,
}
