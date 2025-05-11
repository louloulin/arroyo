use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// 配额类型
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum QuotaType {
    /// 生产者配额
    Producer,
    /// 消费者配额
    Consumer,
    /// 请求配额
    Request,
}

/// 配额单位
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum QuotaUnit {
    /// 每秒字节数
    BytesPerSecond,
    /// 每秒请求数
    RequestsPerSecond,
    /// 每秒消息数
    MessagesPerSecond,
}

/// 配额范围
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum QuotaScope {
    /// 集群范围
    Cluster,
    /// Topic 范围
    Topic,
    /// 用户范围
    User,
    /// 客户端 ID 范围
    ClientId,
    /// 用户和客户端 ID 范围
    UserClientId,
}

/// 配额配置
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuotaConfig {
    /// 配额类型
    pub quota_type: QuotaType,
    /// 配额单位
    pub unit: QuotaUnit,
    /// 配额范围
    pub scope: QuotaScope,
    /// 配额值
    pub value: f64,
    /// 配额名称（用于标识特定的配额）
    pub name: Option<String>,
    /// 配额描述
    pub description: Option<String>,
}

/// Topic 限制配置
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicLimitConfig {
    /// Topic 名称
    pub topic_name: String,
    /// 最大分区数
    pub max_partitions: Option<i32>,
    /// 最大副本因子
    pub max_replication_factor: Option<i16>,
    /// 最大保留时间（毫秒）
    pub max_retention_ms: Option<i64>,
    /// 最大保留大小（字节）
    pub max_retention_bytes: Option<i64>,
    /// 最大消息大小（字节）
    pub max_message_bytes: Option<i32>,
    /// 最大生产速率（字节/秒）
    pub max_producer_bytes_per_sec: Option<i64>,
    /// 最大消费速率（字节/秒）
    pub max_consumer_bytes_per_sec: Option<i64>,
    /// 最大请求速率（请求/秒）
    pub max_request_rate: Option<i32>,
    /// 描述信息
    pub description: Option<String>,
}

/// 配额使用情况
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuotaUsage {
    /// 配额类型
    pub quota_type: QuotaType,
    /// 配额单位
    pub unit: QuotaUnit,
    /// 配额范围
    pub scope: QuotaScope,
    /// 配额值
    pub quota_value: f64,
    /// 当前使用值
    pub current_value: f64,
    /// 使用百分比
    pub usage_percentage: f64,
    /// 是否超过配额
    pub is_exceeded: bool,
    /// 上次更新时间
    pub last_updated: u64,
}

/// 创建配额请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateQuotaRequest {
    /// 配额配置
    pub config: QuotaConfig,
}

/// 更新配额请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateQuotaRequest {
    /// 配额配置
    pub config: QuotaConfig,
}

/// 删除配额请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeleteQuotaRequest {
    /// 配额类型
    pub quota_type: QuotaType,
    /// 配额范围
    pub scope: QuotaScope,
    /// 配额名称
    pub name: Option<String>,
}

/// 配额列表响应
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuotaListResponse {
    /// 配额列表
    pub quotas: Vec<QuotaConfig>,
}

/// 配额使用情况响应
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QuotaUsageResponse {
    /// 配额使用情况列表
    pub usages: Vec<QuotaUsage>,
}

/// 创建 Topic 限制请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateTopicLimitRequest {
    /// Topic 限制配置
    pub config: TopicLimitConfig,
}

/// 更新 Topic 限制请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTopicLimitRequest {
    /// Topic 限制配置
    pub config: TopicLimitConfig,
}

/// 删除 Topic 限制请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTopicLimitRequest {
    /// Topic 名称
    pub topic_name: String,
}

/// Topic 限制列表响应
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicLimitListResponse {
    /// Topic 限制列表
    pub limits: Vec<TopicLimitConfig>,
}
