#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    

    use crate::push::buffer::MemoryBuffer;
    use crate::push::source::PushMessage;

    #[tokio::test]
    async fn test_memory_buffer_push_pop() {
        // Create buffer
        let buffer = MemoryBuffer::new(1024);

        // Create message
        let message = PushMessage {
            id: 1,
            topic: "test-topic".to_string(),
            data: vec![1, 2, 3, 4],
            timestamp: SystemTime::now(),
        };

        // Push message
        let result = buffer.push(message.clone()).await;
        assert!(result.is_ok());

        // Pop message
        let result = buffer.pop().await;
        assert!(result.is_ok());
        let popped = result.unwrap();
        assert_eq!(popped.topic, message.topic);
        assert_eq!(popped.data, message.data);
    }

    #[tokio::test]
    async fn test_memory_buffer_full() {
        // Create buffer with small size
        let buffer = MemoryBuffer::new(10);

        // Create message larger than buffer
        let message = PushMessage {
            id: 2,
            topic: "test-topic".to_string(),
            data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            timestamp: SystemTime::now(),
        };

        // Push message
        let result = buffer.push(message).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_buffer_push_timeout() {
        // Create buffer with small size
        let buffer = MemoryBuffer::new(10);

        // Create message larger than buffer
        let message = PushMessage {
            id: 3,
            topic: "test-topic".to_string(),
            data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            timestamp: SystemTime::now(),
        };

        // Push message with timeout
        let result = buffer.push_timeout(message, Duration::from_millis(100)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_buffer_stats() {
        // Create buffer
        let buffer = MemoryBuffer::new(1024);

        // Get initial stats
        let stats = buffer.stats().await;
        assert_eq!(stats.current_size, 0);
        assert_eq!(stats.max_size, 1024);
        assert_eq!(stats.total_pushed, 0);
        assert_eq!(stats.total_popped, 0);
        assert_eq!(stats.total_bytes_pushed, 0);
        assert_eq!(stats.total_bytes_popped, 0);
        assert_eq!(stats.full_count, 0);
        assert_eq!(stats.avg_message_size, 0.0);
        assert_eq!(stats.utilization, 0.0);

        // Create message
        let message = PushMessage {
            id: 4,
            topic: "test-topic".to_string(),
            data: vec![1, 2, 3, 4],
            timestamp: SystemTime::now(),
        };

        // Push message
        let result = buffer.push(message.clone()).await;
        assert!(result.is_ok());

        // Get stats after push
        let stats = buffer.stats().await;
        assert_eq!(stats.current_size, 1);
        assert_eq!(stats.max_size, 1024);
        assert_eq!(stats.total_pushed, 1);
        assert_eq!(stats.total_popped, 0);
        assert_eq!(stats.total_bytes_pushed, 4);
        assert_eq!(stats.total_bytes_popped, 0);
        assert_eq!(stats.full_count, 0);
        assert_eq!(stats.avg_message_size, 4.0);
        assert!(stats.utilization > 0.0);

        // Pop message
        let result = buffer.pop().await;
        assert!(result.is_ok());

        // Get stats after pop
        let stats = buffer.stats().await;
        assert_eq!(stats.current_size, 0);
        assert_eq!(stats.max_size, 1024);
        assert_eq!(stats.total_pushed, 1);
        assert_eq!(stats.total_popped, 1);
        assert_eq!(stats.total_bytes_pushed, 4);
        assert_eq!(stats.total_bytes_popped, 4);
        assert_eq!(stats.full_count, 0);
        assert_eq!(stats.avg_message_size, 4.0);
        assert_eq!(stats.utilization, 0.0);
    }

    #[tokio::test]
    async fn test_memory_buffer_close() {
        // Create buffer
        let buffer = MemoryBuffer::new(1024);

        // Close buffer
        buffer.close().await;

        // Try to push message
        let message = PushMessage {
            id: 5,
            topic: "test-topic".to_string(),
            data: vec![1, 2, 3, 4],
            timestamp: SystemTime::now(),
        };
        let result = buffer.push(message).await;
        assert!(result.is_err());

        // Try to pop message
        let result = buffer.pop().await;
        assert!(result.is_err());
    }
}
