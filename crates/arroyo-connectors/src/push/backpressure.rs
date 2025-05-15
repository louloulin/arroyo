use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Backoff strategy
#[derive(Debug, Clone)]
pub enum BackoffStrategy {
    /// Constant backoff
    Constant(Duration),
    /// Exponential backoff
    Exponential {
        /// Initial backoff
        initial: Duration,
        /// Maximum backoff
        max: Duration,
        /// Multiplier
        multiplier: f64,
        /// Current attempt
        current_attempt: usize,
    },
    /// Linear backoff
    Linear {
        /// Initial backoff
        initial: Duration,
        /// Maximum backoff
        max: Duration,
        /// Increment
        increment: Duration,
        /// Current attempt
        current_attempt: usize,
    },
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        Self::Exponential {
            initial: Duration::from_millis(10),
            max: Duration::from_secs(10),
            multiplier: 2.0,
            current_attempt: 0,
        }
    }
}

impl BackoffStrategy {
    /// Get next backoff duration
    pub fn next_backoff(&mut self) -> Duration {
        match self {
            Self::Constant(duration) => *duration,
            Self::Exponential {
                initial,
                max,
                multiplier,
                current_attempt,
            } => {
                let backoff = initial.mul_f64(multiplier.powi(*current_attempt as i32));
                *current_attempt += 1;
                std::cmp::min(backoff, *max)
            }
            Self::Linear {
                initial,
                max,
                increment,
                current_attempt,
            } => {
                let backoff = *initial + increment.mul_f64(*current_attempt as f64);
                *current_attempt += 1;
                std::cmp::min(backoff, *max)
            }
        }
    }
    
    /// Reset backoff
    pub fn reset(&mut self) {
        match self {
            Self::Constant(_) => {}
            Self::Exponential {
                current_attempt, ..
            } => {
                *current_attempt = 0;
            }
            Self::Linear {
                current_attempt, ..
            } => {
                *current_attempt = 0;
            }
        }
    }
}

/// Backpressure controller
pub struct BackpressureController {
    /// Maximum buffer size
    max_buffer_size: usize,
    /// Current buffer size
    current_buffer_size: AtomicUsize,
    /// Backoff strategy
    backoff_strategy: Mutex<BackoffStrategy>,
    /// Notify when space is available
    space_available: Notify,
}

impl BackpressureController {
    /// Create a new backpressure controller
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            max_buffer_size,
            current_buffer_size: AtomicUsize::new(0),
            backoff_strategy: Mutex::new(BackoffStrategy::default()),
            space_available: Notify::new(),
        }
    }
    
    /// Create a new backpressure controller with custom backoff strategy
    pub fn with_backoff_strategy(max_buffer_size: usize, backoff_strategy: BackoffStrategy) -> Self {
        Self {
            max_buffer_size,
            current_buffer_size: AtomicUsize::new(0),
            backoff_strategy: Mutex::new(backoff_strategy),
            space_available: Notify::new(),
        }
    }
    
    /// Acquire space in the buffer
    pub async fn acquire(&self, size: usize) -> Result<(), anyhow::Error> {
        loop {
            // Check if there's enough space
            let current = self.current_buffer_size.load(Ordering::Relaxed);
            if current + size <= self.max_buffer_size {
                // Try to acquire space
                match self.current_buffer_size.compare_exchange(
                    current,
                    current + size,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // Reset backoff strategy
                        let mut backoff_strategy = self.backoff_strategy.lock().await;
                        backoff_strategy.reset();
                        
                        debug!("Acquired {} bytes, current buffer size: {}/{}", 
                               size, current + size, self.max_buffer_size);
                        
                        return Ok(());
                    }
                    Err(_) => {
                        // Another thread modified the value, try again
                        continue;
                    }
                }
            }
            
            // Not enough space, apply backpressure
            let backoff = {
                let mut backoff_strategy = self.backoff_strategy.lock().await;
                backoff_strategy.next_backoff()
            };
            
            debug!("Backpressure applied, waiting for {} ms", backoff.as_millis());
            
            // Wait for space to be available or timeout
            let space_available = self.space_available.notified();
            tokio::select! {
                _ = space_available => {
                    // Space is available, try again
                    continue;
                }
                _ = sleep(backoff) => {
                    // Timeout, try again
                    continue;
                }
            }
        }
    }
    
    /// Release space in the buffer
    pub fn release(&self, size: usize) {
        let current = self.current_buffer_size.fetch_sub(size, Ordering::Relaxed);
        let new_size = current.saturating_sub(size);
        
        debug!("Released {} bytes, current buffer size: {}/{}", 
               size, new_size, self.max_buffer_size);
        
        // Notify that space is available
        self.space_available.notify_all();
    }
    
    /// Get current buffer size
    pub fn current_buffer_size(&self) -> usize {
        self.current_buffer_size.load(Ordering::Relaxed)
    }
    
    /// Get maximum buffer size
    pub fn max_buffer_size(&self) -> usize {
        self.max_buffer_size
    }
    
    /// Get utilization (0.0 - 1.0)
    pub fn utilization(&self) -> f64 {
        let current = self.current_buffer_size.load(Ordering::Relaxed);
        current as f64 / self.max_buffer_size as f64
    }
}
