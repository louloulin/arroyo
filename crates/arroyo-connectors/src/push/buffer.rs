use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::push::source::PushMessage;

/// Buffer error
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    /// Buffer is full
    #[error("Buffer is full")]
    Full,

    /// Buffer is closed
    #[error("Buffer is closed")]
    Closed,

    /// Timeout
    #[error("Timeout")]
    Timeout,

    /// Other error
    #[error("Buffer error: {0}")]
    Other(String),
}

/// Buffer statistics
#[derive(Debug, Clone)]
pub struct BufferStats {
    /// Current size
    pub current_size: usize,
    /// Maximum size
    pub max_size: usize,
    /// Total messages pushed
    pub total_pushed: u64,
    /// Total messages popped
    pub total_popped: u64,
    /// Total bytes pushed
    pub total_bytes_pushed: u64,
    /// Total bytes popped
    pub total_bytes_popped: u64,
    /// Number of times the buffer was full
    pub full_count: u64,
    /// Average message size
    pub avg_message_size: f64,
    /// Current utilization (0.0 - 1.0)
    pub utilization: f64,
}

/// Memory buffer for push messages
pub struct MemoryBuffer {
    /// Buffer data
    buffer: Mutex<VecDeque<PushMessage>>,
    /// Maximum buffer size in bytes
    max_size_bytes: usize,
    /// Current buffer size in bytes
    current_size_bytes: RwLock<usize>,
    /// Notify when data is available
    data_available: Notify,
    /// Notify when space is available
    space_available: Notify,
    /// Whether the buffer is closed
    closed: RwLock<bool>,
    /// Buffer statistics
    stats: RwLock<BufferStats>,
}

impl MemoryBuffer {
    /// Create a new memory buffer
    pub fn new(max_size_bytes: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            max_size_bytes,
            current_size_bytes: RwLock::new(0),
            data_available: Notify::new(),
            space_available: Notify::new(),
            closed: RwLock::new(false),
            stats: RwLock::new(BufferStats {
                current_size: 0,
                max_size: max_size_bytes,
                total_pushed: 0,
                total_popped: 0,
                total_bytes_pushed: 0,
                total_bytes_popped: 0,
                full_count: 0,
                avg_message_size: 0.0,
                utilization: 0.0,
            }),
        }
    }

    /// Push a message to the buffer
    pub async fn push(&self, message: PushMessage) -> Result<(), BufferError> {
        // Check if buffer is closed
        if *self.closed.read().await {
            return Err(BufferError::Closed);
        }

        // Get message size
        let message_size = message.data.len();

        // Check if message fits in buffer
        {
            let current_size = *self.current_size_bytes.read().await;
            if current_size + message_size > self.max_size_bytes {
                // Update stats
                let mut stats = self.stats.write().await;
                stats.full_count += 1;

                // Return error
                return Err(BufferError::Full);
            }
        }

        // Push message to buffer
        {
            let mut buffer = self.buffer.lock().await;
            buffer.push_back(message);

            // Update current size
            let mut current_size = self.current_size_bytes.write().await;
            *current_size += message_size;

            // Update stats
            let mut stats = self.stats.write().await;
            stats.current_size = buffer.len();
            stats.total_pushed += 1;
            stats.total_bytes_pushed += message_size as u64;
            stats.avg_message_size = stats.total_bytes_pushed as f64 / stats.total_pushed as f64;
            stats.utilization = *current_size as f64 / self.max_size_bytes as f64;

            debug!("Pushed message to buffer, current size: {}/{} bytes, {} messages",
                   *current_size, self.max_size_bytes, buffer.len());
        }

        // Notify that data is available
        self.data_available.notify_one();

        Ok(())
    }

    /// Push a message to the buffer with timeout
    pub async fn push_timeout(&self, message: PushMessage, timeout: Duration) -> Result<(), BufferError> {
        let start = Instant::now();

        loop {
            match self.push(message.clone()).await {
                Ok(()) => return Ok(()),
                Err(BufferError::Full) => {
                    // Check timeout
                    if start.elapsed() >= timeout {
                        return Err(BufferError::Timeout);
                    }

                    // Wait for space to be available
                    let space_available = self.space_available.notified();
                    tokio::select! {
                        _ = space_available => {
                            // Space is available, try again
                            continue;
                        }
                        _ = sleep(Duration::from_millis(10)) => {
                            // Timeout, try again
                            continue;
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Pop a message from the buffer
    pub async fn pop(&self) -> Result<PushMessage, BufferError> {
        // Check if buffer is closed
        if *self.closed.read().await && self.is_empty().await {
            return Err(BufferError::Closed);
        }

        // Pop message from buffer
        let message = {
            let mut buffer = self.buffer.lock().await;

            // Wait for data if buffer is empty
            if buffer.is_empty() {
                // Release lock and wait for data
                drop(buffer);

                // Wait for data to be available
                let data_available = self.data_available.notified();
                tokio::select! {
                    _ = data_available => {
                        // Data is available, try again
                        let mut buffer = self.buffer.lock().await;
                        buffer.pop_front()
                    }
                    _ = sleep(Duration::from_millis(10)) => {
                        // Timeout, try again
                        None
                    }
                }
            } else {
                buffer.pop_front()
            }
        };

        // Process message if available
        if let Some(message) = message {
            // Get message size
            let message_size = message.data.len();

            // Update current size
            {
                let mut current_size = self.current_size_bytes.write().await;
                *current_size = current_size.saturating_sub(message_size);

                // Update stats
                let mut stats = self.stats.write().await;
                stats.current_size -= 1;
                stats.total_popped += 1;
                stats.total_bytes_popped += message_size as u64;
                stats.utilization = *current_size as f64 / self.max_size_bytes as f64;

                debug!("Popped message from buffer, current size: {}/{} bytes, {} messages",
                       *current_size, self.max_size_bytes, stats.current_size);
            }

            // Notify that space is available
            self.space_available.notify_one();

            Ok(message)
        } else {
            // Try again with indirection to avoid infinite recursion
            Box::pin(self.pop()).await
        }
    }

    /// Pop a message from the buffer with timeout
    pub async fn pop_timeout(&self, timeout: Duration) -> Result<PushMessage, BufferError> {
        let start = Instant::now();

        loop {
            match self.try_pop().await {
                Ok(message) => return Ok(message),
                Err(BufferError::Full) => {
                    // Check timeout
                    if start.elapsed() >= timeout {
                        return Err(BufferError::Timeout);
                    }

                    // Wait for data to be available
                    let data_available = self.data_available.notified();
                    tokio::select! {
                        _ = data_available => {
                            // Data is available, try again
                            continue;
                        }
                        _ = sleep(Duration::from_millis(10)) => {
                            // Timeout, try again
                            continue;
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Try to pop a message from the buffer
    pub async fn try_pop(&self) -> Result<PushMessage, BufferError> {
        // Check if buffer is closed
        if *self.closed.read().await && self.is_empty().await {
            return Err(BufferError::Closed);
        }

        // Pop message from buffer
        let message = {
            let mut buffer = self.buffer.lock().await;

            // Return error if buffer is empty
            if buffer.is_empty() {
                return Err(BufferError::Full);
            }

            buffer.pop_front()
        };

        // Process message if available
        if let Some(message) = message {
            // Get message size
            let message_size = message.data.len();

            // Update current size
            {
                let mut current_size = self.current_size_bytes.write().await;
                *current_size = current_size.saturating_sub(message_size);

                // Update stats
                let mut stats = self.stats.write().await;
                stats.current_size -= 1;
                stats.total_popped += 1;
                stats.total_bytes_popped += message_size as u64;
                stats.utilization = *current_size as f64 / self.max_size_bytes as f64;

                debug!("Popped message from buffer, current size: {}/{} bytes, {} messages",
                       *current_size, self.max_size_bytes, stats.current_size);
            }

            // Notify that space is available
            self.space_available.notify_one();

            Ok(message)
        } else {
            Err(BufferError::Other("Buffer is empty but lock reported non-empty".to_string()))
        }
    }

    /// Check if buffer is empty
    pub async fn is_empty(&self) -> bool {
        let buffer = self.buffer.lock().await;
        buffer.is_empty()
    }

    /// Get buffer size
    pub async fn len(&self) -> usize {
        let buffer = self.buffer.lock().await;
        buffer.len()
    }

    /// Get buffer stats
    pub async fn stats(&self) -> BufferStats {
        self.stats.read().await.clone()
    }

    /// Close the buffer
    pub async fn close(&self) {
        let mut closed = self.closed.write().await;
        *closed = true;

        // Notify all waiters
        self.data_available.notify_waiters();
        self.space_available.notify_waiters();

        info!("Buffer closed");
    }
}
