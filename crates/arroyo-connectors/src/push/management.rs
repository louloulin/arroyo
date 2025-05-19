use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use tracing::{debug, error, info};

use crate::push::topic::{TopicManager, Topic, CreateTopicRequest, TopicError};
use crate::push::metrics::TopicMetricsManager;
use crate::push::messages::MessageStore;
use crate::push::validator::MessageValidator;
use crate::push::PushConnectorConfig;

/// Management plane for Push connector
/// Responsible for topic management, health checks, and other management functions
#[derive(Clone)]
pub struct PushManagementPlane {
    /// Topic manager for managing topics
    topic_manager: Arc<TopicManager>,

    /// Metrics manager for collecting and reporting metrics
    metrics_manager: Arc<TopicMetricsManager>,

    /// Message store for storing and retrieving messages
    message_store: Arc<MessageStore>,

    /// Message validator for validating messages
    message_validator: Arc<MessageValidator>,

    /// Configuration for the Push connector
    config: PushConnectorConfig,
}

impl PushManagementPlane {
    /// Create a new management plane with default configuration
    pub fn new() -> Self {
        Self::with_config(PushConnectorConfig::default())
    }

    /// Create a new management plane with the specified configuration
    pub fn with_config(config: PushConnectorConfig) -> Self {
        Self {
            topic_manager: Arc::new(TopicManager::new()),
            metrics_manager: Arc::new(TopicMetricsManager::new()),
            message_store: Arc::new(MessageStore::new(1000)), // Store up to 1000 messages per topic
            message_validator: Arc::new(MessageValidator::new(
                config.max_message_size,
                100, // max_field_count
                256, // max_field_name_length
                10 * 1024, // max_field_value_length (10 KB)
            )),
            config,
        }
    }

    /// Get the topic manager
    pub fn topic_manager(&self) -> Arc<TopicManager> {
        self.topic_manager.clone()
    }

    /// Get the metrics manager
    pub fn metrics_manager(&self) -> Arc<TopicMetricsManager> {
        self.metrics_manager.clone()
    }

    /// Get the message store
    pub fn message_store(&self) -> Arc<MessageStore> {
        self.message_store.clone()
    }

    /// Get the message validator
    pub fn message_validator(&self) -> Arc<MessageValidator> {
        self.message_validator.clone()
    }

    /// Get the configuration
    pub fn config(&self) -> &PushConnectorConfig {
        &self.config
    }

    /// Create a new topic
    pub fn create_topic(&self, request: CreateTopicRequest) -> Result<Topic, TopicError> {
        self.topic_manager.create_topic(request)
    }

    /// Get a topic by name
    pub fn get_topic(&self, name: &str) -> Result<Topic, TopicError> {
        self.topic_manager.get_topic(name)
    }

    /// Get all topics
    pub fn get_topics(&self) -> Result<Vec<Topic>, TopicError> {
        self.topic_manager.get_topics()
    }

    /// Delete a topic
    pub fn delete_topic(&self, name: &str) -> Result<(), TopicError> {
        self.topic_manager.delete_topic(name)
    }

    /// Record a message for a topic
    pub fn record_message(&self, topic: &str, size: usize) -> Result<(), TopicError> {
        self.topic_manager.record_message(topic, size)
    }

    /// Get health status
    pub fn health_check(&self) -> HealthStatus {
        HealthStatus {
            status: "ok".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            topics_count: self.topic_manager.topics_count(),
            uptime: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

/// Health status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    /// Status of the service
    pub status: String,

    /// Version of the service
    pub version: String,

    /// Number of topics
    pub topics_count: usize,

    /// Uptime in seconds
    pub uptime: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_management_plane_create_topic() {
        let management_plane = PushManagementPlane::new();

        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: false,
        };

        let result = management_plane.create_topic(request);
        assert!(result.is_ok());

        let topic = result.unwrap();
        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.retention_period, 3600);
        assert_eq!(topic.compression, false);
    }

    #[test]
    fn test_management_plane_get_topic() {
        let management_plane = PushManagementPlane::new();

        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: false,
        };

        let _ = management_plane.create_topic(request);

        let result = management_plane.get_topic("test-topic");
        assert!(result.is_ok());

        let topic = result.unwrap();
        assert_eq!(topic.name, "test-topic");
    }

    #[test]
    fn test_management_plane_health_check() {
        let management_plane = PushManagementPlane::new();

        let health = management_plane.health_check();
        assert_eq!(health.status, "ok");
        assert!(!health.version.is_empty());
        assert_eq!(health.topics_count, 0);
    }
}
