use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};


/// Topic metrics
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopicMetrics {
    /// Topic name
    pub name: String,
    /// Total number of messages
    pub total_messages: u64,
    /// Messages per second (calculated over the last minute)
    pub messages_per_second: f64,
    /// Average message size in bytes (calculated over the last minute)
    pub avg_message_size: f64,
    /// Total bytes
    pub total_bytes: u64,
    /// Bytes per second (calculated over the last minute)
    pub bytes_per_second: f64,
    /// Last update timestamp (seconds since epoch)
    pub last_update: u64,
    /// Error count
    pub error_count: u64,
    /// Processing time (milliseconds)
    pub processing_time_ms: u64,
    /// Average processing time (milliseconds)
    pub avg_processing_time_ms: f64,
}

/// Message record for metrics calculation
#[derive(Debug, Clone)]
struct MessageRecord {
    /// Timestamp
    timestamp: Instant,
    /// Message size in bytes
    size: usize,
    /// Processing time in milliseconds
    processing_time_ms: u64,
    /// Whether the message was processed successfully
    success: bool,
}

/// Topic metrics manager
#[derive(Debug)]
pub struct TopicMetricsManager {
    /// Metrics by topic name
    metrics: Arc<RwLock<HashMap<String, TopicMetrics>>>,
    /// Recent messages by topic name (for calculating rates)
    recent_messages: Arc<RwLock<HashMap<String, Vec<MessageRecord>>>>,
    /// Window duration for rate calculations
    window_duration: Duration,
}

impl TopicMetricsManager {
    /// Create a new topic metrics manager
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            recent_messages: Arc::new(RwLock::new(HashMap::new())),
            window_duration: Duration::from_secs(60), // 1 minute window
        }
    }

    /// Record a message for a topic
    pub fn record_message(&self, topic_name: &str, message_size: usize) -> Result<(), String> {
        self.record_message_with_details(topic_name, message_size, 0, true)
    }

    /// Record a message with processing time and success status
    pub fn record_message_with_details(
        &self,
        topic_name: &str,
        message_size: usize,
        processing_time_ms: u64,
        success: bool,
    ) -> Result<(), String> {
        let now = Instant::now();
        let now_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| format!("Failed to get system time: {}", e))?
            .as_secs();

        // Record message
        let mut recent_messages = self.recent_messages.write().map_err(|e| {
            format!("Failed to acquire write lock for recent messages: {}", e)
        })?;

        let topic_messages = recent_messages.entry(topic_name.to_string()).or_insert_with(Vec::new);
        topic_messages.push(MessageRecord {
            timestamp: now,
            size: message_size,
            processing_time_ms,
            success,
        });

        // Remove old messages
        let cutoff = now - self.window_duration;
        topic_messages.retain(|record| record.timestamp >= cutoff);

        // Update metrics
        let mut metrics = self.metrics.write().map_err(|e| {
            format!("Failed to acquire write lock for metrics: {}", e)
        })?;

        let topic_metrics = metrics.entry(topic_name.to_string()).or_insert_with(|| TopicMetrics {
            name: topic_name.to_string(),
            total_messages: 0,
            messages_per_second: 0.0,
            avg_message_size: 0.0,
            total_bytes: 0,
            bytes_per_second: 0.0,
            last_update: now_epoch,
            error_count: 0,
            processing_time_ms: 0,
            avg_processing_time_ms: 0.0,
        });

        // Update total counts
        topic_metrics.total_messages += 1;
        topic_metrics.total_bytes += message_size as u64;
        topic_metrics.last_update = now_epoch;

        // Update processing time
        topic_metrics.processing_time_ms += processing_time_ms;

        // Update error count if message processing failed
        if !success {
            topic_metrics.error_count += 1;
        }

        // Calculate rates
        if !topic_messages.is_empty() {
            let window_seconds = self.window_duration.as_secs_f64();
            let message_count = topic_messages.len() as f64;
            let total_size: usize = topic_messages.iter().map(|record| record.size).sum();
            let total_processing_time: u64 = topic_messages.iter().map(|record| record.processing_time_ms).sum();
            let success_count = topic_messages.iter().filter(|record| record.success).count() as f64;

            topic_metrics.messages_per_second = message_count / window_seconds;
            topic_metrics.avg_message_size = if message_count > 0.0 {
                total_size as f64 / message_count
            } else {
                0.0
            };
            topic_metrics.bytes_per_second = total_size as f64 / window_seconds;
            topic_metrics.avg_processing_time_ms = if message_count > 0.0 {
                total_processing_time as f64 / message_count
            } else {
                0.0
            };
        }

        Ok(())
    }

    /// Record an error for a topic
    pub fn record_error(&self, topic_name: &str) -> Result<(), String> {
        let now_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| format!("Failed to get system time: {}", e))?
            .as_secs();

        // Update metrics
        let mut metrics = self.metrics.write().map_err(|e| {
            format!("Failed to acquire write lock for metrics: {}", e)
        })?;

        let topic_metrics = metrics.entry(topic_name.to_string()).or_insert_with(|| TopicMetrics {
            name: topic_name.to_string(),
            total_messages: 0,
            messages_per_second: 0.0,
            avg_message_size: 0.0,
            total_bytes: 0,
            bytes_per_second: 0.0,
            last_update: now_epoch,
            error_count: 0,
            processing_time_ms: 0,
            avg_processing_time_ms: 0.0,
        });

        // Update error count
        topic_metrics.error_count += 1;
        topic_metrics.last_update = now_epoch;

        Ok(())
    }

    /// Get metrics for a topic
    pub fn get_topic_metrics(&self, topic_name: &str) -> Result<Option<TopicMetrics>, String> {
        let metrics = self.metrics.read().map_err(|e| {
            format!("Failed to acquire read lock for metrics: {}", e)
        })?;

        Ok(metrics.get(topic_name).cloned())
    }

    /// Get metrics for all topics
    pub fn get_all_metrics(&self) -> Result<HashMap<String, TopicMetrics>, String> {
        let metrics = self.metrics.read().map_err(|e| {
            format!("Failed to acquire read lock for metrics: {}", e)
        })?;

        Ok(metrics.clone())
    }
}

impl Default for TopicMetricsManager {
    fn default() -> Self {
        Self::new()
    }
}
