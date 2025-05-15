use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info};

/// Topic information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Topic {
    /// Topic name
    pub name: String,
    /// Number of messages in the topic
    pub messages: u64,
    /// Creation timestamp (seconds since epoch)
    pub created_at: u64,
    /// Last activity timestamp (seconds since epoch)
    pub last_activity: Option<u64>,
    /// Retention period in seconds
    pub retention_period: u64,
    /// Whether compression is enabled
    pub compression: bool,
}

/// Topic creation request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicRequest {
    /// Topic name
    pub name: String,
    /// Retention period in seconds (default: 7 days)
    #[serde(default = "default_retention_period")]
    pub retention_period: u64,
    /// Whether compression is enabled (default: false)
    #[serde(default)]
    pub compression: bool,
}

fn default_retention_period() -> u64 {
    // 7 days in seconds
    7 * 24 * 60 * 60
}

/// Topic error
#[derive(Debug, Error)]
pub enum TopicError {
    #[error("Topic already exists: {0}")]
    TopicAlreadyExists(String),
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    #[error("Invalid topic name: {0}")]
    InvalidTopicName(String),
    #[error("Internal error: {0}")]
    InternalError(String),
}

/// Topic manager
#[derive(Debug, Clone)]
pub struct TopicManager {
    /// Topics by name
    topics: Arc<RwLock<HashMap<String, Topic>>>,
}

impl TopicManager {
    /// Create a new topic manager
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get all topics
    pub fn get_topics(&self) -> Result<Vec<Topic>, TopicError> {
        let topics = self.topics.read().map_err(|e| {
            TopicError::InternalError(format!("Failed to acquire read lock: {}", e))
        })?;

        Ok(topics.values().cloned().collect())
    }

    /// Get topic by name
    pub fn get_topic(&self, name: &str) -> Result<Topic, TopicError> {
        let topics = self.topics.read().map_err(|e| {
            TopicError::InternalError(format!("Failed to acquire read lock: {}", e))
        })?;

        topics
            .get(name)
            .cloned()
            .ok_or_else(|| TopicError::TopicNotFound(name.to_string()))
    }

    /// Create a new topic
    pub fn create_topic(&self, request: CreateTopicRequest) -> Result<Topic, TopicError> {
        // Validate topic name
        if request.name.is_empty() || !is_valid_topic_name(&request.name) {
            return Err(TopicError::InvalidTopicName(request.name));
        }

        let mut topics = self.topics.write().map_err(|e| {
            TopicError::InternalError(format!("Failed to acquire write lock: {}", e))
        })?;

        // Check if topic already exists
        if topics.contains_key(&request.name) {
            return Err(TopicError::TopicAlreadyExists(request.name));
        }

        // Create topic
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let topic = Topic {
            name: request.name.clone(),
            messages: 0,
            created_at: now,
            last_activity: None,
            retention_period: request.retention_period,
            compression: request.compression,
        };

        // Add topic to map
        topics.insert(request.name.clone(), topic.clone());

        info!("Created topic: {}", request.name);
        Ok(topic)
    }

    /// Delete topic
    pub fn delete_topic(&self, name: &str) -> Result<(), TopicError> {
        let mut topics = self.topics.write().map_err(|e| {
            TopicError::InternalError(format!("Failed to acquire write lock: {}", e))
        })?;

        // Check if topic exists
        if !topics.contains_key(name) {
            return Err(TopicError::TopicNotFound(name.to_string()));
        }

        // Remove topic
        topics.remove(name);

        info!("Deleted topic: {}", name);
        Ok(())
    }

    /// Record message for topic
    pub fn record_message(&self, topic_name: &str, message_size: usize) -> Result<(), TopicError> {
        let mut topics = self.topics.write().map_err(|e| {
            TopicError::InternalError(format!("Failed to acquire write lock: {}", e))
        })?;

        // Check if topic exists
        let topic = topics.get_mut(topic_name).ok_or_else(|| {
            TopicError::TopicNotFound(topic_name.to_string())
        })?;

        // Update topic
        topic.messages += 1;
        topic.last_activity = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        );

        Ok(())
    }
}

/// Check if topic name is valid
fn is_valid_topic_name(name: &str) -> bool {
    // Topic name must be alphanumeric, underscore, or hyphen
    name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-')
}

impl Default for TopicManager {
    fn default() -> Self {
        Self::new()
    }
}
