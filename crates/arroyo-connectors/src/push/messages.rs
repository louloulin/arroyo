use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// Message data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    /// Message ID
    pub id: u64,
    /// Topic name
    pub topic: String,
    /// Message content
    pub content: Vec<u8>,
    /// Message timestamp
    pub timestamp: u64,
    /// Message size in bytes
    pub size: usize,
}

/// Message query parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageQuery {
    /// Topic name
    pub topic: String,
    /// Maximum number of messages to return
    pub limit: usize,
    /// Offset for pagination
    pub offset: usize,
    /// Start timestamp (inclusive)
    pub start_time: Option<u64>,
    /// End timestamp (inclusive)
    pub end_time: Option<u64>,
}

/// Message store
#[derive(Debug)]
pub struct MessageStore {
    /// Messages by topic
    messages: Arc<RwLock<HashMap<String, VecDeque<MessageData>>>>,
    /// Maximum number of messages to store per topic
    max_messages_per_topic: usize,
    /// Next message ID
    next_id: Arc<RwLock<u64>>,
}

impl MessageStore {
    /// Create a new message store
    pub fn new(max_messages_per_topic: usize) -> Self {
        Self {
            messages: Arc::new(RwLock::new(HashMap::new())),
            max_messages_per_topic,
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Store a message
    pub fn store_message(&self, topic: &str, content: Vec<u8>) -> Result<MessageData, String> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| format!("Failed to get system time: {}", e))?
            .as_secs();

        // Get next message ID
        let id = {
            let mut next_id = self.next_id.write().map_err(|e| {
                format!("Failed to acquire write lock for next_id: {}", e)
            })?;
            let id = *next_id;
            *next_id += 1;
            id
        };

        // Create message data
        let message = MessageData {
            id,
            topic: topic.to_string(),
            content: content.clone(),
            timestamp: now,
            size: content.len(),
        };

        // Store message
        let mut messages = self.messages.write().map_err(|e| {
            format!("Failed to acquire write lock for messages: {}", e)
        })?;

        let topic_messages = messages.entry(topic.to_string()).or_insert_with(VecDeque::new);
        
        // Add message to the end
        topic_messages.push_back(message.clone());
        
        // Remove oldest messages if we exceed the limit
        while topic_messages.len() > self.max_messages_per_topic {
            topic_messages.pop_front();
        }

        Ok(message)
    }

    /// Query messages
    pub fn query_messages(&self, query: &MessageQuery) -> Result<Vec<MessageData>, String> {
        let messages = self.messages.read().map_err(|e| {
            format!("Failed to acquire read lock for messages: {}", e)
        })?;

        let topic_messages = match messages.get(&query.topic) {
            Some(msgs) => msgs,
            None => return Ok(Vec::new()),
        };

        // Filter messages by time range
        let filtered_messages: Vec<MessageData> = topic_messages
            .iter()
            .filter(|msg| {
                if let Some(start_time) = query.start_time {
                    if msg.timestamp < start_time {
                        return false;
                    }
                }
                if let Some(end_time) = query.end_time {
                    if msg.timestamp > end_time {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        // Apply pagination
        let start = query.offset.min(filtered_messages.len());
        let end = (query.offset + query.limit).min(filtered_messages.len());

        Ok(filtered_messages[start..end].to_vec())
    }

    /// Get message by ID
    pub fn get_message(&self, topic: &str, id: u64) -> Result<Option<MessageData>, String> {
        let messages = self.messages.read().map_err(|e| {
            format!("Failed to acquire read lock for messages: {}", e)
        })?;

        let topic_messages = match messages.get(topic) {
            Some(msgs) => msgs,
            None => return Ok(None),
        };

        // Find message by ID
        let message = topic_messages.iter().find(|msg| msg.id == id).cloned();

        Ok(message)
    }

    /// Get message count for a topic
    pub fn get_message_count(&self, topic: &str) -> Result<usize, String> {
        let messages = self.messages.read().map_err(|e| {
            format!("Failed to acquire read lock for messages: {}", e)
        })?;

        let count = messages.get(topic).map(|msgs| msgs.len()).unwrap_or(0);

        Ok(count)
    }

    /// Clear messages for a topic
    pub fn clear_messages(&self, topic: &str) -> Result<(), String> {
        let mut messages = self.messages.write().map_err(|e| {
            format!("Failed to acquire write lock for messages: {}", e)
        })?;

        if let Some(topic_messages) = messages.get_mut(topic) {
            topic_messages.clear();
        }

        Ok(())
    }
}

impl Default for MessageStore {
    fn default() -> Self {
        Self::new(1000) // Default to storing 1000 messages per topic
    }
}
