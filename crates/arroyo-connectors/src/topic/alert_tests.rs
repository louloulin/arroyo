#[cfg(test)]
mod tests {
    use crate::topic::alert::{AlertNotifier, TopicAlertManager};
    use anyhow::Result;
    use arroyo_rpc::api_types::alerts::{
        AlertCondition, AlertConditionOperator, AlertEvent, AlertNotificationConfig,
        AlertNotificationType, AlertRuleType, AlertSeverity, CreateAlertRuleRequest,
        QueryAlertRulesRequest, UpdateAlertRuleRequest,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    // 测试通知发送器
    struct TestNotifier {
        notifications: std::sync::Mutex<Vec<String>>,
    }

    impl TestNotifier {
        fn new() -> Self {
            Self {
                notifications: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn get_notifications(&self) -> Vec<String> {
            let notifications = self.notifications.lock().unwrap();
            notifications.clone()
        }
    }

    impl AlertNotifier for TestNotifier {
        fn send_notification(
            &self,
            event: &AlertEvent,
            config: &AlertNotificationConfig,
        ) -> Result<()> {
            let mut notifications = self.notifications.lock().unwrap();
            notifications.push(format!(
                "Alert: {} - {} - {:?}",
                event.id, event.message, config.recipients
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_create_alert_rule() {
        // 创建告警管理器
        let notifier = Arc::new(TestNotifier::new());
        let mut notifiers = HashMap::new();
        notifiers.insert("test".to_string(), notifier.clone() as Arc<dyn AlertNotifier>);
        let manager = TopicAlertManager::new(Some(1), Some(3600), notifiers);

        // 创建告警规则
        let request = CreateAlertRuleRequest {
            name: "Test Alert".to_string(),
            description: Some("Test alert description".to_string()),
            rule_type: AlertRuleType::Threshold,
            severity: AlertSeverity::Warning,
            topic_name: Some("test-topic".to_string()),
            conditions: vec![AlertCondition {
                metric_name: "messages_in_per_sec".to_string(),
                operator: AlertConditionOperator::GreaterThan,
                threshold: 100.0,
                duration_sec: Some(60),
            }],
            notifications: vec![AlertNotificationConfig {
                notification_type: AlertNotificationType::Email,
                recipients: vec!["test@example.com".to_string()],
                config: HashMap::new(),
                enabled: true,
            }],
            enabled: true,
            labels: Some(HashMap::new()),
            annotations: Some(HashMap::new()),
        };

        let rule = manager.create_alert_rule(request).await.unwrap();

        // 验证规则
        assert_eq!(rule.name, "Test Alert");
        assert_eq!(rule.description, Some("Test alert description".to_string()));
        assert_eq!(rule.rule_type, AlertRuleType::Threshold);
        assert_eq!(rule.severity, AlertSeverity::Warning);
        assert_eq!(rule.topic_name, Some("test-topic".to_string()));
        assert_eq!(rule.conditions.len(), 1);
        assert_eq!(rule.conditions[0].metric_name, "messages_in_per_sec");
        assert_eq!(
            rule.conditions[0].operator,
            AlertConditionOperator::GreaterThan
        );
        assert_eq!(rule.conditions[0].threshold, 100.0);
        assert_eq!(rule.notifications.len(), 1);
        assert_eq!(
            rule.notifications[0].notification_type,
            AlertNotificationType::Email
        );
        assert_eq!(rule.notifications[0].recipients.len(), 1);
        assert_eq!(rule.notifications[0].recipients[0], "test@example.com");
        assert!(rule.enabled);
    }

    #[tokio::test]
    async fn test_update_alert_rule() {
        // 创建告警管理器
        let notifier = Arc::new(TestNotifier::new());
        let mut notifiers = HashMap::new();
        notifiers.insert("test".to_string(), notifier.clone() as Arc<dyn AlertNotifier>);
        let manager = TopicAlertManager::new(Some(1), Some(3600), notifiers);

        // 创建告警规则
        let request = CreateAlertRuleRequest {
            name: "Test Alert".to_string(),
            description: Some("Test alert description".to_string()),
            rule_type: AlertRuleType::Threshold,
            severity: AlertSeverity::Warning,
            topic_name: Some("test-topic".to_string()),
            conditions: vec![AlertCondition {
                metric_name: "messages_in_per_sec".to_string(),
                operator: AlertConditionOperator::GreaterThan,
                threshold: 100.0,
                duration_sec: Some(60),
            }],
            notifications: vec![AlertNotificationConfig {
                notification_type: AlertNotificationType::Email,
                recipients: vec!["test@example.com".to_string()],
                config: HashMap::new(),
                enabled: true,
            }],
            enabled: true,
            labels: Some(HashMap::new()),
            annotations: Some(HashMap::new()),
        };

        let rule = manager.create_alert_rule(request).await.unwrap();

        // 更新告警规则
        let update_request = UpdateAlertRuleRequest {
            name: Some("Updated Alert".to_string()),
            description: Some("Updated description".to_string()),
            rule_type: Some(AlertRuleType::Anomaly),
            severity: Some(AlertSeverity::Critical),
            topic_name: Some("updated-topic".to_string()),
            conditions: Some(vec![AlertCondition {
                metric_name: "bytes_in_per_sec".to_string(),
                operator: AlertConditionOperator::LessThan,
                threshold: 50.0,
                duration_sec: Some(30),
            }]),
            notifications: Some(vec![AlertNotificationConfig {
                notification_type: AlertNotificationType::Webhook,
                recipients: vec!["https://example.com/webhook".to_string()],
                config: HashMap::new(),
                enabled: true,
            }]),
            enabled: Some(false),
            labels: Some(HashMap::new()),
            annotations: Some(HashMap::new()),
        };

        let updated_rule = manager.update_alert_rule(&rule.id, update_request).await.unwrap();

        // 验证更新后的规则
        assert_eq!(updated_rule.name, "Updated Alert");
        assert_eq!(updated_rule.description, Some("Updated description".to_string()));
        assert_eq!(updated_rule.rule_type, AlertRuleType::Anomaly);
        assert_eq!(updated_rule.severity, AlertSeverity::Critical);
        assert_eq!(updated_rule.topic_name, Some("updated-topic".to_string()));
        assert_eq!(updated_rule.conditions.len(), 1);
        assert_eq!(updated_rule.conditions[0].metric_name, "bytes_in_per_sec");
        assert_eq!(
            updated_rule.conditions[0].operator,
            AlertConditionOperator::LessThan
        );
        assert_eq!(updated_rule.conditions[0].threshold, 50.0);
        assert_eq!(updated_rule.notifications.len(), 1);
        assert_eq!(
            updated_rule.notifications[0].notification_type,
            AlertNotificationType::Webhook
        );
        assert_eq!(updated_rule.notifications[0].recipients.len(), 1);
        assert_eq!(
            updated_rule.notifications[0].recipients[0],
            "https://example.com/webhook"
        );
        assert!(!updated_rule.enabled);
    }

    #[tokio::test]
    async fn test_delete_alert_rule() {
        // 创建告警管理器
        let notifier = Arc::new(TestNotifier::new());
        let mut notifiers = HashMap::new();
        notifiers.insert("test".to_string(), notifier.clone() as Arc<dyn AlertNotifier>);
        let manager = TopicAlertManager::new(Some(1), Some(3600), notifiers);

        // 创建告警规则
        let request = CreateAlertRuleRequest {
            name: "Test Alert".to_string(),
            description: Some("Test alert description".to_string()),
            rule_type: AlertRuleType::Threshold,
            severity: AlertSeverity::Warning,
            topic_name: Some("test-topic".to_string()),
            conditions: vec![AlertCondition {
                metric_name: "messages_in_per_sec".to_string(),
                operator: AlertConditionOperator::GreaterThan,
                threshold: 100.0,
                duration_sec: Some(60),
            }],
            notifications: vec![AlertNotificationConfig {
                notification_type: AlertNotificationType::Email,
                recipients: vec!["test@example.com".to_string()],
                config: HashMap::new(),
                enabled: true,
            }],
            enabled: true,
            labels: Some(HashMap::new()),
            annotations: Some(HashMap::new()),
        };

        let rule = manager.create_alert_rule(request).await.unwrap();

        // 删除告警规则
        manager.delete_alert_rule(&rule.id).await.unwrap();

        // 验证规则已被删除
        let result = manager.get_alert_rule(&rule.id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_query_alert_rules() {
        // 创建告警管理器
        let notifier = Arc::new(TestNotifier::new());
        let mut notifiers = HashMap::new();
        notifiers.insert("test".to_string(), notifier.clone() as Arc<dyn AlertNotifier>);
        let manager = TopicAlertManager::new(Some(1), Some(3600), notifiers);

        // 创建多个告警规则
        for i in 1..=3 {
            let request = CreateAlertRuleRequest {
                name: format!("Test Alert {}", i),
                description: Some(format!("Test alert description {}", i)),
                rule_type: AlertRuleType::Threshold,
                severity: AlertSeverity::Warning,
                topic_name: Some(format!("test-topic-{}", i)),
                conditions: vec![AlertCondition {
                    metric_name: "messages_in_per_sec".to_string(),
                    operator: AlertConditionOperator::GreaterThan,
                    threshold: 100.0 * i as f64,
                    duration_sec: Some(60),
                }],
                notifications: vec![AlertNotificationConfig {
                    notification_type: AlertNotificationType::Email,
                    recipients: vec![format!("test{}@example.com", i)],
                    config: HashMap::new(),
                    enabled: true,
                }],
                enabled: true,
                labels: Some({
                    let mut labels = HashMap::new();
                    labels.insert("env".to_string(), "test".to_string());
                    labels.insert("index".to_string(), i.to_string());
                    labels
                }),
                annotations: Some(HashMap::new()),
            };

            manager.create_alert_rule(request).await.unwrap();
        }

        // 查询所有规则
        let query_request = QueryAlertRulesRequest {
            topic_name: None,
            rule_type: None,
            severity: None,
            enabled: None,
            labels: None,
            limit: None,
            offset: None,
        };

        let response = manager.query_alert_rules(query_request).await.unwrap();
        assert_eq!(response.total, 3);
        assert_eq!(response.rules.len(), 3);

        // 按 Topic 名称查询
        let query_request = QueryAlertRulesRequest {
            topic_name: Some("test-topic-2".to_string()),
            rule_type: None,
            severity: None,
            enabled: None,
            labels: None,
            limit: None,
            offset: None,
        };

        let response = manager.query_alert_rules(query_request).await.unwrap();
        assert_eq!(response.total, 1);
        assert_eq!(response.rules.len(), 1);
        assert_eq!(response.rules[0].name, "Test Alert 2");

        // 按标签查询
        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "test".to_string());
        labels.insert("index".to_string(), "3".to_string());

        let query_request = QueryAlertRulesRequest {
            topic_name: None,
            rule_type: None,
            severity: None,
            enabled: None,
            labels: Some(labels),
            limit: None,
            offset: None,
        };

        let response = manager.query_alert_rules(query_request).await.unwrap();
        assert_eq!(response.total, 1);
        assert_eq!(response.rules.len(), 1);
        assert_eq!(response.rules[0].name, "Test Alert 3");
    }
}
