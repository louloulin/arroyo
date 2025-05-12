use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// 告警级别
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AlertSeverity {
    /// 信息
    Info,
    /// 警告
    Warning,
    /// 错误
    Error,
    /// 严重
    Critical,
}

/// 告警状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AlertState {
    /// 待处理
    Pending,
    /// 已触发
    Firing,
    /// 已解决
    Resolved,
    /// 已确认
    Acknowledged,
    /// 已忽略
    Silenced,
}

/// 告警规则类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AlertRuleType {
    /// 阈值
    Threshold,
    /// 异常检测
    Anomaly,
    /// 趋势
    Trend,
    /// 自定义
    Custom,
}

/// 告警规则条件操作符
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AlertConditionOperator {
    /// 大于
    GreaterThan,
    /// 大于等于
    GreaterThanOrEqual,
    /// 小于
    LessThan,
    /// 小于等于
    LessThanOrEqual,
    /// 等于
    Equal,
    /// 不等于
    NotEqual,
    /// 包含
    Contains,
    /// 不包含
    NotContains,
}

/// 告警规则条件
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertCondition {
    /// 指标名称
    pub metric_name: String,
    /// 操作符
    pub operator: AlertConditionOperator,
    /// 阈值
    pub threshold: f64,
    /// 持续时间（秒）
    pub duration_sec: Option<u64>,
}

/// 告警通知方式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AlertNotificationType {
    /// 邮件
    Email,
    /// Webhook
    Webhook,
    /// Slack
    Slack,
    /// 短信
    Sms,
    /// 系统内通知
    InApp,
}

/// 告警通知配置
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertNotificationConfig {
    /// 通知类型
    pub notification_type: AlertNotificationType,
    /// 接收者
    pub recipients: Vec<String>,
    /// 配置参数
    pub config: HashMap<String, String>,
    /// 是否启用
    pub enabled: bool,
}

/// 告警规则
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertRule {
    /// 规则 ID
    pub id: String,
    /// 规则名称
    pub name: String,
    /// 规则描述
    pub description: Option<String>,
    /// 规则类型
    pub rule_type: AlertRuleType,
    /// 告警级别
    pub severity: AlertSeverity,
    /// Topic 名称（可选，如果为空则适用于所有 Topic）
    pub topic_name: Option<String>,
    /// 条件列表（多个条件之间是 AND 关系）
    pub conditions: Vec<AlertCondition>,
    /// 通知配置
    pub notifications: Vec<AlertNotificationConfig>,
    /// 是否启用
    pub enabled: bool,
    /// 创建时间（Unix 时间戳，秒）
    pub created_at: u64,
    /// 更新时间（Unix 时间戳，秒）
    pub updated_at: u64,
    /// 标签
    pub labels: HashMap<String, String>,
    /// 注解
    pub annotations: HashMap<String, String>,
}

/// 告警事件
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertEvent {
    /// 事件 ID
    pub id: String,
    /// 规则 ID
    pub rule_id: String,
    /// 规则名称
    pub rule_name: String,
    /// Topic 名称
    pub topic_name: Option<String>,
    /// 告警级别
    pub severity: AlertSeverity,
    /// 告警状态
    pub state: AlertState,
    /// 告警消息
    pub message: String,
    /// 触发值
    pub value: f64,
    /// 阈值
    pub threshold: f64,
    /// 开始时间（Unix 时间戳，秒）
    pub started_at: u64,
    /// 结束时间（Unix 时间戳，秒，仅当状态为 Resolved 时有效）
    pub ended_at: Option<u64>,
    /// 标签
    pub labels: HashMap<String, String>,
    /// 注解
    pub annotations: HashMap<String, String>,
}

/// 创建告警规则请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateAlertRuleRequest {
    /// 规则名称
    pub name: String,
    /// 规则描述
    pub description: Option<String>,
    /// 规则类型
    pub rule_type: AlertRuleType,
    /// 告警级别
    pub severity: AlertSeverity,
    /// Topic 名称（可选，如果为空则适用于所有 Topic）
    pub topic_name: Option<String>,
    /// 条件列表（多个条件之间是 AND 关系）
    pub conditions: Vec<AlertCondition>,
    /// 通知配置
    pub notifications: Vec<AlertNotificationConfig>,
    /// 是否启用
    pub enabled: bool,
    /// 标签
    pub labels: Option<HashMap<String, String>>,
    /// 注解
    pub annotations: Option<HashMap<String, String>>,
}

/// 更新告警规则请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAlertRuleRequest {
    /// 规则名称
    pub name: Option<String>,
    /// 规则描述
    pub description: Option<String>,
    /// 规则类型
    pub rule_type: Option<AlertRuleType>,
    /// 告警级别
    pub severity: Option<AlertSeverity>,
    /// Topic 名称（可选，如果为空则适用于所有 Topic）
    pub topic_name: Option<String>,
    /// 条件列表（多个条件之间是 AND 关系）
    pub conditions: Option<Vec<AlertCondition>>,
    /// 通知配置
    pub notifications: Option<Vec<AlertNotificationConfig>>,
    /// 是否启用
    pub enabled: Option<bool>,
    /// 标签
    pub labels: Option<HashMap<String, String>>,
    /// 注解
    pub annotations: Option<HashMap<String, String>>,
}

/// 查询告警规则请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryAlertRulesRequest {
    /// Topic 名称
    pub topic_name: Option<String>,
    /// 规则类型
    pub rule_type: Option<AlertRuleType>,
    /// 告警级别
    pub severity: Option<AlertSeverity>,
    /// 是否启用
    pub enabled: Option<bool>,
    /// 标签过滤器
    pub labels: Option<HashMap<String, String>>,
    /// 限制返回数量
    pub limit: Option<u32>,
    /// 偏移量（用于分页）
    pub offset: Option<u32>,
}

/// 查询告警事件请求
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryAlertEventsRequest {
    /// 规则 ID
    pub rule_id: Option<String>,
    /// Topic 名称
    pub topic_name: Option<String>,
    /// 告警级别
    pub severity: Option<AlertSeverity>,
    /// 告警状态
    pub state: Option<AlertState>,
    /// 开始时间（Unix 时间戳，秒）
    pub start_time: Option<u64>,
    /// 结束时间（Unix 时间戳，秒）
    pub end_time: Option<u64>,
    /// 标签过滤器
    pub labels: Option<HashMap<String, String>>,
    /// 限制返回数量
    pub limit: Option<u32>,
    /// 偏移量（用于分页）
    pub offset: Option<u32>,
}

/// 告警规则响应
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertRulesResponse {
    /// 告警规则列表
    pub rules: Vec<AlertRule>,
    /// 总数
    pub total: u32,
}

/// 告警事件响应
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertEventsResponse {
    /// 告警事件列表
    pub events: Vec<AlertEvent>,
    /// 总数
    pub total: u32,
}
