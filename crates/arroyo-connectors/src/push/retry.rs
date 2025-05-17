use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::push::messages::{MessageStatus, MessageStore};
use crate::push::source::PushMessage;

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retries
    pub max_retries: usize,
    /// Initial retry delay in milliseconds
    pub initial_delay_ms: u64,
    /// Maximum retry delay in milliseconds
    pub max_delay_ms: u64,
    /// Retry delay multiplier
    pub delay_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 10000,
            delay_multiplier: 2.0,
        }
    }
}

/// Retry manager
#[derive(Debug)]
pub struct RetryManager {
    /// Message store
    message_store: Arc<MessageStore>,
    /// Retry configuration
    config: RetryConfig,
    /// Retry attempts by message ID
    retry_attempts: Arc<RwLock<HashMap<(String, u64), usize>>>,
    /// Last retry time by message ID
    last_retry_time: Arc<RwLock<HashMap<(String, u64), Instant>>>,
}

impl RetryManager {
    /// Create a new retry manager
    pub fn new(message_store: Arc<MessageStore>, config: RetryConfig) -> Self {
        Self {
            message_store,
            config,
            retry_attempts: Arc::new(RwLock::new(HashMap::new())),
            last_retry_time: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the retry manager
    pub async fn start(self: Arc<Self>, tx: mpsc::Sender<PushMessage>) {
        info!("Starting retry manager");
        
        let retry_manager = self.clone();
        
        tokio::spawn(async move {
            loop {
                // Sleep for a short time to avoid busy waiting
                sleep(Duration::from_millis(100)).await;
                
                // Check for failed messages
                if let Err(e) = retry_manager.check_and_retry_failed_messages(&tx).await {
                    error!("Error checking and retrying failed messages: {}", e);
                }
            }
        });
    }
    
    /// Check for failed messages and retry them
    async fn check_and_retry_failed_messages(&self, tx: &mpsc::Sender<PushMessage>) -> Result<(), String> {
        // Get all topics
        let topics = self.message_store.get_all_topics()?;
        
        for topic in topics {
            // Query failed messages for this topic
            let query = crate::push::messages::MessageQuery {
                topic: topic.clone(),
                limit: 100,
                offset: 0,
                start_time: None,
                end_time: None,
            };
            
            let messages = self.message_store.query_messages_by_status(&query, MessageStatus::Failed)?;
            
            for message in messages {
                // Check if we should retry this message
                if self.should_retry(&topic, message.id) {
                    // Increment retry attempts
                    let attempts = self.increment_retry_attempts(&topic, message.id);
                    
                    // Update last retry time
                    self.update_last_retry_time(&topic, message.id);
                    
                    // Mark message as processing
                    if let Err(e) = self.message_store.mark_message_processing(&topic, message.id) {
                        error!("Failed to mark message as processing: {}", e);
                        continue;
                    }
                    
                    // Create push message
                    let push_message = PushMessage {
                        id: message.id,
                        topic: topic.clone(),
                        data: message.content.clone(),
                        timestamp: std::time::UNIX_EPOCH + std::time::Duration::from_secs(message.timestamp),
                    };
                    
                    // Send message
                    debug!("Retrying message {} for topic {} (attempt {})", message.id, topic, attempts);
                    if let Err(e) = tx.send(push_message).await {
                        error!("Failed to send message for retry: {}", e);
                        
                        // Mark message as failed
                        if let Err(e) = self.message_store.mark_message_failed(&topic, message.id, format!("Failed to send message for retry: {}", e)) {
                            error!("Failed to mark message as failed: {}", e);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Check if we should retry a message
    fn should_retry(&self, topic: &str, message_id: u64) -> bool {
        let key = (topic.to_string(), message_id);
        
        // Check retry attempts
        let attempts = {
            let attempts_map = self.retry_attempts.read().unwrap();
            attempts_map.get(&key).cloned().unwrap_or(0)
        };
        
        if attempts >= self.config.max_retries {
            return false;
        }
        
        // Check last retry time
        let last_retry = {
            let last_retry_map = self.last_retry_time.read().unwrap();
            last_retry_map.get(&key).cloned()
        };
        
        if let Some(last_retry) = last_retry {
            let delay = self.get_retry_delay(attempts);
            let elapsed = last_retry.elapsed();
            
            if elapsed < delay {
                return false;
            }
        }
        
        true
    }
    
    /// Get retry delay based on retry attempts
    fn get_retry_delay(&self, attempts: usize) -> Duration {
        let delay_ms = (self.config.initial_delay_ms as f64 * self.config.delay_multiplier.powi(attempts as i32)) as u64;
        let delay_ms = delay_ms.min(self.config.max_delay_ms);
        
        Duration::from_millis(delay_ms)
    }
    
    /// Increment retry attempts
    fn increment_retry_attempts(&self, topic: &str, message_id: u64) -> usize {
        let key = (topic.to_string(), message_id);
        
        let mut attempts_map = self.retry_attempts.write().unwrap();
        let attempts = attempts_map.entry(key.clone()).or_insert(0);
        *attempts += 1;
        
        *attempts
    }
    
    /// Update last retry time
    fn update_last_retry_time(&self, topic: &str, message_id: u64) {
        let key = (topic.to_string(), message_id);
        
        let mut last_retry_map = self.last_retry_time.write().unwrap();
        last_retry_map.insert(key, Instant::now());
    }
}
