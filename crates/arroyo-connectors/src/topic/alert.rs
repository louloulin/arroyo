use anyhow::{anyhow, Result};
use arroyo_rpc::api_types::alerts::{
    AlertCondition, AlertConditionOperator, AlertEvent, AlertNotificationConfig, AlertRule,
    AlertRulesResponse, AlertState, CreateAlertRuleRequest, QueryAlertEventsRequest,
    QueryAlertRulesRequest, UpdateAlertRuleRequest,
};
use arroyo_rpc::api_types::metrics::TopicMetrics;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::{self, Duration};
use uuid::Uuid;

/// 告警通知发送器
pub trait AlertNotifier: Send + Sync {
    /// 发送告警通知
    fn send_notification(&self, event: &AlertEvent, config: &AlertNotificationConfig) -> Result<()>;
}

/// 邮件通知发送器
pub struct EmailNotifier {
    /// SMTP 服务器
    smtp_server: String,
    /// SMTP 端口
    smtp_port: u16,
    /// SMTP 用户名
    smtp_username: String,
    /// SMTP 密码
    smtp_password: String,
    /// 发件人
    from_address: String,
}

impl EmailNotifier {
    /// 创建邮件通知发送器
    pub fn new(
        smtp_server: &str,
        smtp_port: u16,
        smtp_username: &str,
        smtp_password: &str,
        from_address: &str,
    ) -> Self {
        Self {
            smtp_server: smtp_server.to_string(),
            smtp_port,
            smtp_username: smtp_username.to_string(),
            smtp_password: smtp_password.to_string(),
            from_address: from_address.to_string(),
        }
    }
}

impl AlertNotifier for EmailNotifier {
    fn send_notification(&self, event: &AlertEvent, config: &AlertNotificationConfig) -> Result<()> {
        // 在实际实现中，这里应该使用 SMTP 客户端发送邮件
        // 这里简单模拟一下
        println!(
            "Sending email notification for alert: {} to recipients: {:?}",
            event.id, config.recipients
        );
        println!("Alert message: {}", event.message);
        println!(
            "From: {}, SMTP: {}:{}",
            self.from_address, self.smtp_server, self.smtp_port
        );
        Ok(())
    }
}

/// Webhook 通知发送器
pub struct WebhookNotifier {
    /// HTTP 客户端
    client: reqwest::Client,
}

impl WebhookNotifier {
    /// 创建 Webhook 通知发送器
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl AlertNotifier for WebhookNotifier {
    fn send_notification(&self, event: &AlertEvent, config: &AlertNotificationConfig) -> Result<()> {
        // 在实际实现中，这里应该使用 HTTP 客户端发送 Webhook 请求
        // 这里简单模拟一下
        println!(
            "Sending webhook notification for alert: {} to URLs: {:?}",
            event.id, config.recipients
        );
        println!("Alert message: {}", event.message);
        Ok(())
    }
}

/// Topic 告警管理器
pub struct TopicAlertManager {
    /// 告警规则
    rules: RwLock<HashMap<String, AlertRule>>,
    /// 告警事件
    events: RwLock<HashMap<String, AlertEvent>>,
    /// 通知发送器
    notifiers: HashMap<String, Arc<dyn AlertNotifier>>,
    /// 告警检查间隔（秒）
    check_interval: u64,
    /// 告警保留时间（秒）
    retention_period: u64,
}

impl TopicAlertManager {
    /// 创建告警管理器
    pub fn new(
        check_interval: Option<u64>,
        retention_period: Option<u64>,
        notifiers: HashMap<String, Arc<dyn AlertNotifier>>,
    ) -> Self {
        Self {
            rules: RwLock::new(HashMap::new()),
            events: RwLock::new(HashMap::new()),
            notifiers,
            check_interval: check_interval.unwrap_or(60),
            retention_period: retention_period.unwrap_or(86400 * 7), // 默认保留 7 天
        }
    }

    /// 启动告警检查任务
    pub async fn start_alert_checker(&self) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(self.check_interval));
        loop {
            interval.tick().await;
            if let Err(e) = self.check_alerts().await {
                eprintln!("Error checking alerts: {}", e);
            }
            if let Err(e) = self.clean_old_events().await {
                eprintln!("Error cleaning old events: {}", e);
            }
        }
    }

    /// 检查告警
    async fn check_alerts(&self) -> Result<()> {
        // 在实际实现中，这里应该从监控系统获取最新的指标数据
        // 然后根据告警规则进行评估
        // 这里简单模拟一下
        let rules = self.rules.read().await;
        for rule in rules.values() {
            if !rule.enabled {
                continue;
            }

            // 获取 Topic 指标
            let metrics = self.get_topic_metrics(rule.topic_name.as_deref()).await?;

            // 评估告警规则
            for topic_metrics in metrics {
                let should_alert = self.evaluate_rule(rule, &topic_metrics);
                if should_alert {
                    // 创建或更新告警事件
                    self.create_or_update_alert_event(rule, &topic_metrics).await?;
                } else {
                    // 解决告警事件
                    self.resolve_alert_event(rule, &topic_metrics).await?;
                }
            }
        }
        Ok(())
    }

    /// 获取 Topic 指标
    async fn get_topic_metrics(&self, topic_name: Option<&str>) -> Result<Vec<TopicMetrics>> {
        // 在实际实现中，这里应该从监控系统获取真实的指标数据
        // 这里简单模拟一下
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut metrics = Vec::new();
        if let Some(topic) = topic_name {
            // 模拟单个 Topic 的指标
            metrics.push(TopicMetrics {
                topic_name: topic.to_string(),
                partition_count: 3,
                total_size_bytes: 1024 * 1024 * 10, // 10MB
                total_message_count: 1000,
                bytes_in_per_sec: 1024.0,
                bytes_out_per_sec: 2048.0,
                messages_in_per_sec: 10.0,
                created_at: now - 3600,
                updated_at: now,
                retention_bytes: 1073741824, // 1GB
                retention_ms: 604800000,     // 7 天
                metrics: HashMap::new(),
            });
        } else {
            // 模拟所有 Topic 的指标
            for i in 1..4 {
                metrics.push(TopicMetrics {
                    topic_name: format!("test-topic-{}", i),
                    partition_count: 3,
                    total_size_bytes: 1024 * 1024 * i as u64, // i MB
                    total_message_count: 1000 * i as u64,
                    bytes_in_per_sec: 1024.0 * i as f64,
                    bytes_out_per_sec: 2048.0 * i as f64,
                    messages_in_per_sec: 10.0 * i as f64,
                    created_at: now - 3600,
                    updated_at: now,
                    retention_bytes: 1073741824, // 1GB
                    retention_ms: 604800000,     // 7 天
                    metrics: HashMap::new(),
                });
            }
        }

        Ok(metrics)
    }

    /// 评估告警规则
    fn evaluate_rule(&self, rule: &AlertRule, metrics: &TopicMetrics) -> bool {
        // 如果规则指定了 Topic，但与当前指标的 Topic 不匹配，则跳过
        if let Some(topic) = &rule.topic_name {
            if topic != &metrics.topic_name {
                return false;
            }
        }

        // 评估所有条件（多个条件之间是 AND 关系）
        for condition in &rule.conditions {
            let value = self.get_metric_value(condition, metrics);
            let threshold = condition.threshold;

            let condition_met = match condition.operator {
                AlertConditionOperator::GreaterThan => value > threshold,
                AlertConditionOperator::GreaterThanOrEqual => value >= threshold,
                AlertConditionOperator::LessThan => value < threshold,
                AlertConditionOperator::LessThanOrEqual => value <= threshold,
                AlertConditionOperator::Equal => (value - threshold).abs() < f64::EPSILON,
                AlertConditionOperator::NotEqual => (value - threshold).abs() >= f64::EPSILON,
                AlertConditionOperator::Contains => {
                    // 这里简单处理，实际实现可能更复杂
                    false
                }
                AlertConditionOperator::NotContains => {
                    // 这里简单处理，实际实现可能更复杂
                    false
                }
            };

            if !condition_met {
                return false;
            }
        }

        true
    }

    /// 获取指标值
    fn get_metric_value(&self, condition: &AlertCondition, metrics: &TopicMetrics) -> f64 {
        match condition.metric_name.as_str() {
            "partition_count" => metrics.partition_count as f64,
            "total_size_bytes" => metrics.total_size_bytes as f64,
            "total_message_count" => metrics.total_message_count as f64,
            "bytes_in_per_sec" => metrics.bytes_in_per_sec,
            "bytes_out_per_sec" => metrics.bytes_out_per_sec,
            "messages_in_per_sec" => metrics.messages_in_per_sec,
            _ => {
                // 尝试从自定义指标中获取
                if let Some(value) = metrics.metrics.get(&condition.metric_name) {
                    match value {
                        arroyo_rpc::api_types::metrics::MetricValue::Integer(i) => *i as f64,
                        arroyo_rpc::api_types::metrics::MetricValue::Float(f) => *f,
                        _ => 0.0,
                    }
                } else {
                    0.0
                }
            }
        }
    }

    /// 创建或更新告警事件
    async fn create_or_update_alert_event(
        &self,
        rule: &AlertRule,
        metrics: &TopicMetrics,
    ) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 检查是否已经存在相同规则和 Topic 的告警事件
        let mut events = self.events.write().await;
        let event_id = format!("{}:{}", rule.id, metrics.topic_name);

        if let Some(event) = events.get_mut(&event_id) {
            // 如果事件已经存在且状态为 Resolved，则创建新事件
            if event.state == AlertState::Resolved {
                let new_event = self.create_alert_event(rule, metrics, now);
                events.insert(event_id.clone(), new_event.clone());
                // 发送通知
                self.send_notifications(rule, &new_event).await?;
            } else if event.state == AlertState::Pending {
                // 如果事件状态为 Pending，则更新为 Firing
                event.state = AlertState::Firing;
                event.value = self.get_metric_value(&rule.conditions[0], metrics);
                // 发送通知
                self.send_notifications(rule, event).await?;
            }
            // 如果事件状态为 Firing，则不做任何操作
        } else {
            // 创建新事件
            let new_event = self.create_alert_event(rule, metrics, now);
            events.insert(event_id, new_event.clone());
            // 发送通知
            self.send_notifications(rule, &new_event).await?;
        }

        Ok(())
    }

    /// 创建告警事件
    fn create_alert_event(&self, rule: &AlertRule, metrics: &TopicMetrics, now: u64) -> AlertEvent {
        let condition = &rule.conditions[0]; // 使用第一个条件作为主要条件
        let value = self.get_metric_value(condition, metrics);

        AlertEvent {
            id: Uuid::new_v4().to_string(),
            rule_id: rule.id.clone(),
            rule_name: rule.name.clone(),
            topic_name: Some(metrics.topic_name.clone()),
            severity: rule.severity,
            state: AlertState::Firing,
            message: format!(
                "Alert '{}' triggered for topic '{}': {} {} {}",
                rule.name,
                metrics.topic_name,
                condition.metric_name,
                self.operator_to_string(condition.operator),
                condition.threshold
            ),
            value,
            threshold: condition.threshold,
            started_at: now,
            ended_at: None,
            labels: rule.labels.clone(),
            annotations: rule.annotations.clone(),
        }
    }

    /// 操作符转字符串
    fn operator_to_string(&self, operator: AlertConditionOperator) -> &'static str {
        match operator {
            AlertConditionOperator::GreaterThan => ">",
            AlertConditionOperator::GreaterThanOrEqual => ">=",
            AlertConditionOperator::LessThan => "<",
            AlertConditionOperator::LessThanOrEqual => "<=",
            AlertConditionOperator::Equal => "==",
            AlertConditionOperator::NotEqual => "!=",
            AlertConditionOperator::Contains => "contains",
            AlertConditionOperator::NotContains => "not contains",
        }
    }

    /// 解决告警事件
    async fn resolve_alert_event(&self, rule: &AlertRule, metrics: &TopicMetrics) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 检查是否存在相同规则和 Topic 的告警事件
        let mut events = self.events.write().await;
        let event_id = format!("{}:{}", rule.id, metrics.topic_name);

        if let Some(event) = events.get_mut(&event_id) {
            // 只有当事件状态为 Firing 时才解决
            if event.state == AlertState::Firing {
                event.state = AlertState::Resolved;
                event.ended_at = Some(now);
                // 发送恢复通知
                self.send_recovery_notifications(rule, event).await?;
            }
        }

        Ok(())
    }

    /// 发送告警通知
    async fn send_notifications(&self, rule: &AlertRule, event: &AlertEvent) -> Result<()> {
        for notification_config in &rule.notifications {
            if !notification_config.enabled {
                continue;
            }

            let notifier_type = match notification_config.notification_type {
                arroyo_rpc::api_types::alerts::AlertNotificationType::Email => "email",
                arroyo_rpc::api_types::alerts::AlertNotificationType::Webhook => "webhook",
                arroyo_rpc::api_types::alerts::AlertNotificationType::Slack => "slack",
                arroyo_rpc::api_types::alerts::AlertNotificationType::Sms => "sms",
                arroyo_rpc::api_types::alerts::AlertNotificationType::InApp => "in_app",
            };

            if let Some(notifier) = self.notifiers.get(notifier_type) {
                if let Err(e) = notifier.send_notification(event, notification_config) {
                    eprintln!(
                        "Error sending notification for alert {}: {}",
                        event.id, e
                    );
                }
            }
        }

        Ok(())
    }

    /// 发送恢复通知
    async fn send_recovery_notifications(&self, rule: &AlertRule, event: &AlertEvent) -> Result<()> {
        // 在实际实现中，这里可能需要发送不同的恢复通知
        // 这里简单复用告警通知逻辑
        self.send_notifications(rule, event).await
    }

    /// 清理过期的告警事件
    async fn clean_old_events(&self) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let retention_threshold = now.saturating_sub(self.retention_period);

        let mut events = self.events.write().await;
        events.retain(|_, event| {
            // 保留未解决的事件或者解决时间在保留期内的事件
            event.state != AlertState::Resolved
                || event.ended_at.unwrap_or(now) >= retention_threshold
        });

        Ok(())
    }

    /// 创建告警规则
    pub async fn create_alert_rule(&self, request: CreateAlertRuleRequest) -> Result<AlertRule> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let rule = AlertRule {
            id: Uuid::new_v4().to_string(),
            name: request.name,
            description: request.description,
            rule_type: request.rule_type,
            severity: request.severity,
            topic_name: request.topic_name,
            conditions: request.conditions,
            notifications: request.notifications,
            enabled: request.enabled,
            created_at: now,
            updated_at: now,
            labels: request.labels.unwrap_or_default(),
            annotations: request.annotations.unwrap_or_default(),
        };

        let mut rules = self.rules.write().await;
        rules.insert(rule.id.clone(), rule.clone());

        Ok(rule)
    }

    /// 更新告警规则
    pub async fn update_alert_rule(
        &self,
        rule_id: &str,
        request: UpdateAlertRuleRequest,
    ) -> Result<AlertRule> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut rules = self.rules.write().await;
        let rule = rules
            .get_mut(rule_id)
            .ok_or_else(|| anyhow!("Alert rule not found: {}", rule_id))?;

        if let Some(name) = request.name {
            rule.name = name;
        }
        if let Some(description) = request.description {
            rule.description = Some(description);
        }
        if let Some(rule_type) = request.rule_type {
            rule.rule_type = rule_type;
        }
        if let Some(severity) = request.severity {
            rule.severity = severity;
        }
        if let Some(topic_name) = request.topic_name {
            rule.topic_name = Some(topic_name);
        }
        if let Some(conditions) = request.conditions {
            rule.conditions = conditions;
        }
        if let Some(notifications) = request.notifications {
            rule.notifications = notifications;
        }
        if let Some(enabled) = request.enabled {
            rule.enabled = enabled;
        }
        if let Some(labels) = request.labels {
            rule.labels = labels;
        }
        if let Some(annotations) = request.annotations {
            rule.annotations = annotations;
        }

        rule.updated_at = now;

        Ok(rule.clone())
    }

    /// 删除告警规则
    pub async fn delete_alert_rule(&self, rule_id: &str) -> Result<()> {
        let mut rules = self.rules.write().await;
        if rules.remove(rule_id).is_none() {
            return Err(anyhow!("Alert rule not found: {}", rule_id));
        }

        // 删除相关的告警事件
        let mut events = self.events.write().await;
        events.retain(|id, _| !id.starts_with(&format!("{}:", rule_id)));

        Ok(())
    }

    /// 获取告警规则
    pub async fn get_alert_rule(&self, rule_id: &str) -> Result<AlertRule> {
        let rules = self.rules.read().await;
        rules
            .get(rule_id)
            .cloned()
            .ok_or_else(|| anyhow!("Alert rule not found: {}", rule_id))
    }

    /// 查询告警规则
    pub async fn query_alert_rules(
        &self,
        request: QueryAlertRulesRequest,
    ) -> Result<AlertRulesResponse> {
        let rules = self.rules.read().await;
        let mut result = Vec::new();

        for rule in rules.values() {
            // 应用过滤条件
            if let Some(ref topic_name) = request.topic_name {
                if let Some(ref rule_topic) = rule.topic_name {
                    if rule_topic != topic_name {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            if let Some(rule_type) = request.rule_type {
                if rule.rule_type != rule_type {
                    continue;
                }
            }

            if let Some(severity) = request.severity {
                if rule.severity != severity {
                    continue;
                }
            }

            if let Some(enabled) = request.enabled {
                if rule.enabled != enabled {
                    continue;
                }
            }

            if let Some(ref labels) = request.labels {
                let mut match_labels = true;
                for (key, value) in labels {
                    if !rule.labels.contains_key(key) || rule.labels.get(key) != Some(value) {
                        match_labels = false;
                        break;
                    }
                }
                if !match_labels {
                    continue;
                }
            }

            result.push(rule.clone());
        }

        // 应用分页
        let total = result.len() as u32;
        if let Some(limit) = request.limit {
            if let Some(offset) = request.offset {
                let start = offset as usize;
                let end = (offset + limit) as usize;
                if start < result.len() {
                    result = result[start..std::cmp::min(end, result.len())].to_vec();
                } else {
                    result.clear();
                }
            } else {
                result.truncate(limit as usize);
            }
        }

        Ok(AlertRulesResponse { rules: result, total })
    }

    /// 查询告警事件
    pub async fn query_alert_events(
        &self,
        request: QueryAlertEventsRequest,
    ) -> Result<Vec<AlertEvent>> {
        let events = self.events.read().await;
        let mut result = Vec::new();

        for event in events.values() {
            // 应用过滤条件
            if let Some(ref rule_id) = request.rule_id {
                if event.rule_id != *rule_id {
                    continue;
                }
            }

            if let Some(ref topic_name) = request.topic_name {
                if let Some(ref event_topic) = event.topic_name {
                    if event_topic != topic_name {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            if let Some(severity) = request.severity {
                if event.severity != severity {
                    continue;
                }
            }

            if let Some(state) = request.state {
                if event.state != state {
                    continue;
                }
            }

            if let Some(start_time) = request.start_time {
                if event.started_at < start_time {
                    continue;
                }
            }

            if let Some(end_time) = request.end_time {
                if event.started_at > end_time {
                    continue;
                }
            }

            if let Some(ref labels) = request.labels {
                let mut match_labels = true;
                for (key, value) in labels {
                    if !event.labels.contains_key(key) || event.labels.get(key) != Some(value) {
                        match_labels = false;
                        break;
                    }
                }
                if !match_labels {
                    continue;
                }
            }

            result.push(event.clone());
        }

        // 应用分页
        if let Some(limit) = request.limit {
            if let Some(offset) = request.offset {
                let start = offset as usize;
                let end = (offset + limit) as usize;
                if start < result.len() {
                    result = result[start..std::cmp::min(end, result.len())].to_vec();
                } else {
                    result.clear();
                }
            } else {
                result.truncate(limit as usize);
            }
        }

        Ok(result)
    }

    /// 确认告警事件
    pub async fn acknowledge_alert_event(&self, event_id: &str) -> Result<AlertEvent> {
        let mut events = self.events.write().await;
        let event = events
            .get_mut(event_id)
            .ok_or_else(|| anyhow!("Alert event not found: {}", event_id))?;

        if event.state == AlertState::Firing {
            event.state = AlertState::Acknowledged;
        }

        Ok(event.clone())
    }

    /// 忽略告警事件
    pub async fn silence_alert_event(&self, event_id: &str) -> Result<AlertEvent> {
        let mut events = self.events.write().await;
        let event = events
            .get_mut(event_id)
            .ok_or_else(|| anyhow!("Alert event not found: {}", event_id))?;

        if event.state == AlertState::Firing || event.state == AlertState::Acknowledged {
            event.state = AlertState::Silenced;
        }

        Ok(event.clone())
    }
}
