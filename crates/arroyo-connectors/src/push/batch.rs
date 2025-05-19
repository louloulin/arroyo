use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::push::source::PushMessage;

/// Batch processor for push messages
pub struct BatchProcessor {
    /// Maximum batch size
    max_batch_size: usize,
    /// Maximum wait time
    max_wait_time: Duration,
    /// Current batch
    batch: HashMap<String, Vec<PushMessage>>,
    /// Last flush time
    last_flush: Instant,
}

impl BatchProcessor {
    /// Create a new batch processor
    pub fn new(max_batch_size: usize, max_wait_time: Duration) -> Self {
        Self {
            max_batch_size,
            max_wait_time,
            batch: HashMap::new(),
            last_flush: Instant::now(),
        }
    }

    /// Add a message to the batch
    pub fn add(&mut self, message: PushMessage) -> Option<HashMap<String, Vec<PushMessage>>> {
        // Get topic
        let topic = message.topic.clone();

        // Add message to batch
        let topic_batch = self.batch.entry(topic.clone()).or_insert_with(Vec::new);
        topic_batch.push(message);

        // Check if batch is full
        let total_messages = self.batch.values().map(|v| v.len()).sum::<usize>();
        if total_messages >= self.max_batch_size {
            debug!("Batch is full ({} messages), flushing", total_messages);
            return self.flush();
        }

        None
    }

    /// Flush the batch
    pub fn flush(&mut self) -> Option<HashMap<String, Vec<PushMessage>>> {
        // Check if batch is empty
        if self.batch.is_empty() {
            return None;
        }

        // Take batch
        let batch = std::mem::take(&mut self.batch);

        // Update last flush time
        self.last_flush = Instant::now();

        // Return batch
        Some(batch)
    }

    /// Check if batch should be flushed
    pub fn should_flush(&self) -> bool {
        // Check if batch is empty
        if self.batch.is_empty() {
            return false;
        }

        // Check if max wait time has elapsed
        if self.last_flush.elapsed() >= self.max_wait_time {
            return true;
        }

        false
    }

    /// Get batch size
    pub fn size(&self) -> usize {
        self.batch.values().map(|v| v.len()).sum()
    }

    /// Get batch size by topic
    pub fn size_by_topic(&self) -> HashMap<String, usize> {
        self.batch.iter().map(|(k, v)| (k.clone(), v.len())).collect()
    }

    /// Get time since last flush
    pub fn time_since_last_flush(&self) -> Duration {
        self.last_flush.elapsed()
    }

    /// Check if batch should be flushed and return it if so
    pub fn check(&mut self) -> Option<HashMap<String, Vec<PushMessage>>> {
        if self.should_flush() {
            self.flush()
        } else {
            None
        }
    }
}

/// Batch processor runner
pub struct BatchProcessorRunner {
    /// Batch processor
    processor: BatchProcessor,
    /// Batch handler
    handler: Box<dyn Fn(HashMap<String, Vec<PushMessage>>) -> Result<(), anyhow::Error> + Send + Sync>,
}

impl BatchProcessorRunner {
    /// Create a new batch processor runner
    pub fn new(
        max_batch_size: usize,
        max_wait_time: Duration,
        handler: Box<dyn Fn(HashMap<String, Vec<PushMessage>>) -> Result<(), anyhow::Error> + Send + Sync>,
    ) -> Self {
        Self {
            processor: BatchProcessor::new(max_batch_size, max_wait_time),
            handler,
        }
    }

    /// Run the batch processor
    pub async fn run(&mut self, mut rx: tokio::sync::mpsc::Receiver<PushMessage>) -> Result<(), anyhow::Error> {
        loop {
            tokio::select! {
                // Process incoming message
                Some(message) = rx.recv() => {
                    // Add message to batch
                    if let Some(batch) = self.processor.add(message) {
                        // Process batch
                        if let Err(e) = (self.handler)(batch) {
                            error!("Error processing batch: {}", e);
                        }
                    }
                }

                // Check if batch should be flushed
                _ = sleep(Duration::from_millis(10)) => {
                    if self.processor.should_flush() {
                        debug!("Batch timeout ({} ms), flushing {} messages",
                               self.processor.time_since_last_flush().as_millis(),
                               self.processor.size());

                        if let Some(batch) = self.processor.flush() {
                            // Process batch
                            if let Err(e) = (self.handler)(batch) {
                                error!("Error processing batch: {}", e);
                            }
                        }
                    }
                }

                // Channel closed
                else => {
                    info!("Channel closed, flushing remaining messages");

                    // Flush remaining messages
                    if let Some(batch) = self.processor.flush() {
                        // Process batch
                        if let Err(e) = (self.handler)(batch) {
                            error!("Error processing batch: {}", e);
                        }
                    }

                    break;
                }
            }
        }

        Ok(())
    }
}
