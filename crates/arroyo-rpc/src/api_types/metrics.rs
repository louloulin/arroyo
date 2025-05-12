use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum_macros::{EnumCount, EnumString};
use utoipa::ToSchema;

#[derive(
    Serialize, Deserialize, Copy, Clone, Debug, ToSchema, Hash, PartialEq, Eq, EnumCount, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum MetricName {
    BytesRecv,
    BytesSent,
    MessagesRecv,
    MessagesSent,
    Backpressure,
    TxQueueSize,
    TxQueueRem,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Metric {
    pub time: u64,
    pub value: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubtaskMetrics {
    pub index: u32,
    pub metrics: Vec<Metric>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricGroup {
    pub name: MetricName,
    pub subtasks: Vec<SubtaskMetrics>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperatorMetricGroup {
    pub node_id: u32,
    pub metric_groups: Vec<MetricGroup>,
}

/// 指标类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetricType {
    /// 计数器
    Counter,
    /// 仪表
    Gauge,
    /// 直方图
    Histogram,
    /// 摘要
    Summary,
}

/// 指标值
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetricValue {
    /// 整数值
    Integer(i64),
    /// 浮点值
    Float(f64),
    /// 布尔值
    Boolean(bool),
    /// 字符串值
    String(String),
    /// 直方图值
    Histogram(Vec<(f64, u64)>),
    /// 摘要值
    Summary {
        count: u64,
        sum: f64,
        quantiles: Vec<(f64, f64)>,
    },
}

/// Topic 指标
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetrics {
    /// Topic 名称
    pub topic_name: String,
    /// 分区数量
    pub partition_count: u32,
    /// 总大小（字节）
    pub total_size_bytes: u64,
    /// 总消息数
    pub total_message_count: u64,
    /// 每秒入站字节数
    pub bytes_in_per_sec: f64,
    /// 每秒出站字节数
    pub bytes_out_per_sec: f64,
    /// 每秒入站消息数
    pub messages_in_per_sec: f64,
    /// 创建时间（Unix 时间戳，秒）
    pub created_at: u64,
    /// 更新时间（Unix 时间戳，秒）
    pub updated_at: u64,
    /// 保留大小（字节）
    pub retention_bytes: u64,
    /// 保留时间（毫秒）
    pub retention_ms: u64,
    /// 其他指标
    pub metrics: HashMap<String, MetricValue>,
}

/// 分区指标
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionMetrics {
    /// 分区 ID
    pub partition_id: i32,
    /// Topic 名称
    pub topic_name: String,
    /// 大小（字节）
    pub size_bytes: u64,
    /// 消息数
    pub message_count: u64,
    /// 每秒入站字节数
    pub bytes_in_per_sec: f64,
    /// 每秒出站字节数
    pub bytes_out_per_sec: f64,
    /// 每秒入站消息数
    pub messages_in_per_sec: f64,
    /// 副本延迟（节点 ID -> 延迟消息数）
    pub replica_lag: HashMap<String, u64>,
    /// 最早偏移量
    pub earliest_offset: u64,
    /// 最新偏移量
    pub latest_offset: u64,
}

/// Topic 指标过滤器
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetricsFilter {
    /// Topic 名称模式（支持部分匹配）
    pub topic_pattern: Option<String>,
    /// 最小分区数
    pub min_partition_count: Option<u32>,
    /// 最大分区数
    pub max_partition_count: Option<u32>,
    /// 最小消息速率（每秒消息数）
    pub min_message_rate: Option<f64>,
    /// 最大消息速率（每秒消息数）
    pub max_message_rate: Option<f64>,
    /// 排序字段
    pub sort_by: Option<String>,
    /// 是否降序排序
    pub sort_desc: Option<bool>,
    /// 限制返回数量
    pub limit: Option<u32>,
    /// 偏移量（用于分页）
    pub offset: Option<u32>,
}

/// 指标查询请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricsQueryRequest {
    /// Topic 名称
    pub topic_name: Option<String>,
    /// 分区 ID
    pub partition_id: Option<i32>,
    /// 开始时间（Unix 时间戳，秒）
    pub start_time: Option<u64>,
    /// 结束时间（Unix 时间戳，秒）
    pub end_time: Option<u64>,
    /// 指标过滤器
    pub filter: Option<TopicMetricsFilter>,
    /// 是否包含历史数据
    pub include_history: Option<bool>,
    /// 历史数据采样间隔（秒）
    pub history_interval: Option<u64>,
}

/// 指标查询响应
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricsQueryResponse {
    /// Topic 指标
    pub topic_metrics: Option<TopicMetrics>,
    /// 分区指标
    pub partition_metrics: Option<HashMap<i32, PartitionMetrics>>,
    /// 历史数据
    pub history: Option<Vec<(u64, TopicMetrics)>>,
    /// 所有 Topic 指标
    pub all_topics: Option<Vec<TopicMetrics>>,
}
