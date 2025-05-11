use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// Topic 指标类型
#[derive(Serialize, Deserialize, Copy, Clone, Debug, ToSchema, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TopicMetricType {
    /// 生产速率（字节/秒）
    BytesInPerSec,
    /// 消费速率（字节/秒）
    BytesOutPerSec,
    /// 消息生产速率（消息/秒）
    MessagesInPerSec,
    /// 消息消费速率（消息/秒）
    MessagesOutPerSec,
    /// 总消息数
    TotalMessages,
    /// 总存储大小（字节）
    TotalSizeBytes,
    /// 副本滞后（消息数）
    ReplicaLag,
    /// 分区数量
    PartitionCount,
    /// 请求速率（请求/秒）
    RequestsPerSec,
    /// 请求延迟（毫秒）
    RequestLatencyMs,
    /// 失败请求率（请求/秒）
    FailedRequestsPerSec,
}

/// 时间序列指标点
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricPoint {
    /// 时间戳（微秒）
    pub timestamp: u64,
    /// 指标值
    pub value: f64,
}

/// 分区指标
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionMetricData {
    /// 分区 ID
    pub partition_id: i32,
    /// 指标数据点
    pub data_points: Vec<MetricPoint>,
}

/// Topic 指标组
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetricGroup {
    /// 指标类型
    pub metric_type: TopicMetricType,
    /// 分区级别指标
    pub partitions: Vec<PartitionMetricData>,
    /// Topic 级别聚合指标
    pub topic_aggregate: Vec<MetricPoint>,
}

/// Topic 指标响应
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetricsResponse {
    /// Topic 名称
    pub topic_name: String,
    /// 指标组
    pub metric_groups: Vec<TopicMetricGroup>,
    /// 最后更新时间
    pub last_updated: u64,
}

/// 获取 Topic 指标请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetTopicMetricsRequest {
    /// Topic 名称
    pub topic_name: String,
    /// 开始时间（微秒）
    pub start_time: Option<u64>,
    /// 结束时间（微秒）
    pub end_time: Option<u64>,
    /// 指标类型列表
    pub metric_types: Option<Vec<TopicMetricType>>,
    /// 是否包含分区级别指标
    pub include_partitions: Option<bool>,
    /// 是否强制刷新
    pub force_refresh: Option<bool>,
}

/// 集群指标摘要
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterMetricsSummary {
    /// 总 Topic 数量
    pub total_topics: u32,
    /// 总分区数量
    pub total_partitions: u32,
    /// 总消息数量
    pub total_messages: u64,
    /// 总存储大小（字节）
    pub total_size_bytes: u64,
    /// 总生产速率（字节/秒）
    pub total_bytes_in_per_sec: f64,
    /// 总消费速率（字节/秒）
    pub total_bytes_out_per_sec: f64,
    /// 总消息生产速率（消息/秒）
    pub total_messages_in_per_sec: f64,
    /// 总消息消费速率（消息/秒）
    pub total_messages_out_per_sec: f64,
    /// 平均请求延迟（毫秒）
    pub avg_request_latency_ms: f64,
    /// 最后更新时间
    pub last_updated: u64,
}

/// 获取集群指标摘要请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterMetricsSummaryRequest {
    /// 是否强制刷新
    pub force_refresh: Option<bool>,
}

/// Topic 指标告警级别
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum AlertLevel {
    /// 信息
    Info,
    /// 警告
    Warning,
    /// 错误
    Error,
    /// 严重
    Critical,
}

/// Topic 指标告警
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetricAlert {
    /// 告警 ID
    pub alert_id: String,
    /// Topic 名称
    pub topic_name: String,
    /// 分区 ID（如果适用）
    pub partition_id: Option<i32>,
    /// 指标类型
    pub metric_type: TopicMetricType,
    /// 告警级别
    pub level: AlertLevel,
    /// 告警消息
    pub message: String,
    /// 当前值
    pub current_value: f64,
    /// 阈值
    pub threshold_value: f64,
    /// 触发时间
    pub triggered_at: u64,
    /// 是否已解决
    pub resolved: bool,
    /// 解决时间
    pub resolved_at: Option<u64>,
}

/// 获取 Topic 指标告警请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetTopicMetricAlertsRequest {
    /// Topic 名称
    pub topic_name: Option<String>,
    /// 告警级别
    pub level: Option<AlertLevel>,
    /// 是否只包含未解决的告警
    pub unresolved_only: Option<bool>,
    /// 开始时间（微秒）
    pub start_time: Option<u64>,
    /// 结束时间（微秒）
    pub end_time: Option<u64>,
}

/// Topic 指标告警响应
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetricAlertsResponse {
    /// 告警列表
    pub alerts: Vec<TopicMetricAlert>,
}

/// 告警规则操作符
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum AlertRuleOperator {
    /// 大于
    GreaterThan,
    /// 小于
    LessThan,
    /// 大于等于
    GreaterThanOrEqual,
    /// 小于等于
    LessThanOrEqual,
    /// 等于
    Equal,
    /// 不等于
    NotEqual,
}

/// Topic 指标告警规则
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetricAlertRule {
    /// 规则 ID
    pub rule_id: String,
    /// 规则名称
    pub name: String,
    /// Topic 名称模式
    pub topic_pattern: String,
    /// 指标类型
    pub metric_type: TopicMetricType,
    /// 操作符
    pub operator: AlertRuleOperator,
    /// 阈值
    pub threshold: f64,
    /// 告警级别
    pub level: AlertLevel,
    /// 持续时间（秒）
    pub duration_sec: Option<u64>,
    /// 描述
    pub description: Option<String>,
    /// 是否启用
    pub enabled: bool,
}

/// 创建告警规则请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateAlertRuleRequest {
    /// 规则配置
    pub rule: TopicMetricAlertRule,
}

/// 更新告警规则请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAlertRuleRequest {
    /// 规则配置
    pub rule: TopicMetricAlertRule,
}

/// 获取告警规则请求
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAlertRulesRequest {
    /// Topic 名称
    pub topic_name: Option<String>,
    /// 指标类型
    pub metric_type: Option<TopicMetricType>,
    /// 告警级别
    pub level: Option<AlertLevel>,
    /// 是否只包含启用的规则
    pub enabled_only: Option<bool>,
}

/// 告警规则响应
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertRulesResponse {
    /// 规则列表
    pub rules: Vec<TopicMetricAlertRule>,
}
