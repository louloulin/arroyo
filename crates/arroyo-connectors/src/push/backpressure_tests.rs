#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tokio::sync::Barrier;
    use tokio::time::sleep;

    use crate::push::backpressure::{BackoffStrategy, BackpressureController};

    #[tokio::test]
    async fn test_backoff_strategy_constant() {
        let mut strategy = BackoffStrategy::Constant(Duration::from_millis(100));
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(100));
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(100));
    }
    
    #[tokio::test]
    async fn test_backoff_strategy_exponential() {
        let mut strategy = BackoffStrategy::Exponential {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(1000),
            multiplier: 2.0,
            current_attempt: 0,
        };
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(10));
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(20));
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(40));
        
        // Reset
        strategy.reset();
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(10));
    }
    
    #[tokio::test]
    async fn test_backoff_strategy_linear() {
        let mut strategy = BackoffStrategy::Linear {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(1000),
            increment: Duration::from_millis(10),
            current_attempt: 0,
        };
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(10));
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(20));
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(30));
        
        // Reset
        strategy.reset();
        
        let backoff = strategy.next_backoff();
        assert_eq!(backoff, Duration::from_millis(10));
    }
    
    #[tokio::test]
    async fn test_backpressure_controller_acquire_release() {
        let controller = BackpressureController::new(100);
        
        // Acquire space
        let result = controller.acquire(50).await;
        assert!(result.is_ok());
        
        // Check current buffer size
        assert_eq!(controller.current_buffer_size(), 50);
        
        // Release space
        controller.release(30);
        
        // Check current buffer size
        assert_eq!(controller.current_buffer_size(), 20);
        
        // Release remaining space
        controller.release(20);
        
        // Check current buffer size
        assert_eq!(controller.current_buffer_size(), 0);
    }
    
    #[tokio::test]
    async fn test_backpressure_controller_full() {
        let controller = BackpressureController::new(100);
        
        // Acquire space
        let result = controller.acquire(80).await;
        assert!(result.is_ok());
        
        // Try to acquire more space than available
        let start = Instant::now();
        let result = controller.acquire(30).await;
        assert!(result.is_ok());
        
        // Check that it took some time due to backpressure
        assert!(start.elapsed() > Duration::from_millis(5));
        
        // Check current buffer size
        assert_eq!(controller.current_buffer_size(), 110);
        
        // Release space
        controller.release(110);
        
        // Check current buffer size
        assert_eq!(controller.current_buffer_size(), 0);
    }
    
    #[tokio::test]
    async fn test_backpressure_controller_concurrent() {
        let controller = Arc::new(BackpressureController::new(100));
        let barrier = Arc::new(Barrier::new(3));
        
        // Spawn two tasks that try to acquire space
        let controller_clone = controller.clone();
        let barrier_clone = barrier.clone();
        let task1 = tokio::spawn(async move {
            barrier_clone.wait().await;
            let result = controller_clone.acquire(60).await;
            assert!(result.is_ok());
            sleep(Duration::from_millis(100)).await;
            controller_clone.release(60);
        });
        
        let controller_clone = controller.clone();
        let barrier_clone = barrier.clone();
        let task2 = tokio::spawn(async move {
            barrier_clone.wait().await;
            let result = controller_clone.acquire(60).await;
            assert!(result.is_ok());
            sleep(Duration::from_millis(100)).await;
            controller_clone.release(60);
        });
        
        // Wait for barrier
        barrier.wait().await;
        
        // Wait for tasks to complete
        let _ = tokio::join!(task1, task2);
        
        // Check current buffer size
        assert_eq!(controller.current_buffer_size(), 0);
    }
    
    #[tokio::test]
    async fn test_backpressure_controller_utilization() {
        let controller = BackpressureController::new(100);
        
        // Check initial utilization
        assert_eq!(controller.utilization(), 0.0);
        
        // Acquire space
        let result = controller.acquire(30).await;
        assert!(result.is_ok());
        
        // Check utilization
        assert_eq!(controller.utilization(), 0.3);
        
        // Acquire more space
        let result = controller.acquire(40).await;
        assert!(result.is_ok());
        
        // Check utilization
        assert_eq!(controller.utilization(), 0.7);
        
        // Release space
        controller.release(70);
        
        // Check utilization
        assert_eq!(controller.utilization(), 0.0);
    }
}
