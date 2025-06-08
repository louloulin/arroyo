#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};
    use std::thread;

    use crate::push::metrics::TopicMetricsManager;

    #[test]
    fn test_record_message() {
        let manager = TopicMetricsManager::new();

        // Record a message
        manager.record_message("test-topic", 100).unwrap();

        // Get metrics
        let metrics = manager.get_topic_metrics("test-topic").unwrap().unwrap();

        // Verify metrics
        assert_eq!(metrics.name, "test-topic");
        assert_eq!(metrics.total_messages, 1);
        assert_eq!(metrics.total_bytes, 100);
        assert!(metrics.messages_per_second > 0.0);
        assert!(metrics.bytes_per_second > 0.0);
        assert_eq!(metrics.avg_message_size, 100.0);

        // Current time should be close to last_update
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(now - metrics.last_update < 2);
    }

    #[test]
    fn test_record_multiple_messages() {
        let manager = TopicMetricsManager::new();

        // Record multiple messages
        manager.record_message("test-topic", 100).unwrap();
        manager.record_message("test-topic", 200).unwrap();
        manager.record_message("test-topic", 300).unwrap();

        // Get metrics
        let metrics = manager.get_topic_metrics("test-topic").unwrap().unwrap();

        // Verify metrics
        assert_eq!(metrics.total_messages, 3);
        assert_eq!(metrics.total_bytes, 600);
        assert!(metrics.messages_per_second > 0.0);
        assert!(metrics.bytes_per_second > 0.0);
        assert_eq!(metrics.avg_message_size, 200.0); // (100 + 200 + 300) / 3
    }

    #[test]
    fn test_record_messages_for_multiple_topics() {
        let manager = TopicMetricsManager::new();

        // Record messages for multiple topics
        manager.record_message("topic1", 100).unwrap();
        manager.record_message("topic2", 200).unwrap();
        manager.record_message("topic1", 300).unwrap();

        // Get metrics for topic1
        let metrics1 = manager.get_topic_metrics("topic1").unwrap().unwrap();

        // Verify metrics for topic1
        assert_eq!(metrics1.name, "topic1");
        assert_eq!(metrics1.total_messages, 2);
        assert_eq!(metrics1.total_bytes, 400);
        assert_eq!(metrics1.avg_message_size, 200.0); // (100 + 300) / 2

        // Get metrics for topic2
        let metrics2 = manager.get_topic_metrics("topic2").unwrap().unwrap();

        // Verify metrics for topic2
        assert_eq!(metrics2.name, "topic2");
        assert_eq!(metrics2.total_messages, 1);
        assert_eq!(metrics2.total_bytes, 200);
        assert_eq!(metrics2.avg_message_size, 200.0);
    }

    #[test]
    fn test_get_all_metrics() {
        let manager = TopicMetricsManager::new();

        // Record messages for multiple topics
        manager.record_message("topic1", 100).unwrap();
        manager.record_message("topic2", 200).unwrap();

        // Get all metrics
        let metrics = manager.get_all_metrics().unwrap();

        // Verify metrics
        assert_eq!(metrics.len(), 2);

        // Get metrics for each topic
        let metrics1 = metrics.get("topic1").unwrap();
        let metrics2 = metrics.get("topic2").unwrap();

        // Verify metrics for topic1
        assert_eq!(metrics1.total_messages, 1);
        assert_eq!(metrics1.total_bytes, 100);

        // Verify metrics for topic2
        assert_eq!(metrics2.total_messages, 1);
        assert_eq!(metrics2.total_bytes, 200);
    }

    #[test]
    fn test_get_nonexistent_topic_metrics() {
        let manager = TopicMetricsManager::new();

        // Get metrics for a nonexistent topic
        let result = manager.get_topic_metrics("nonexistent").unwrap();

        // Should return None
        assert!(result.is_none());
    }

    #[test]
    fn test_rate_calculation() {
        let manager = TopicMetricsManager::new();

        // Record messages at a specific rate
        for _ in 0..10 {
            manager.record_message("test-topic", 100).unwrap();
            thread::sleep(Duration::from_millis(100)); // 10 messages per second
        }

        // Get metrics
        let metrics = manager.get_topic_metrics("test-topic").unwrap().unwrap();

        // Verify metrics
        assert_eq!(metrics.total_messages, 10);
        assert_eq!(metrics.total_bytes, 1000);

        // Rate should be positive
        assert!(metrics.messages_per_second > 0.0);

        // Bytes per second should be positive
        assert!(metrics.bytes_per_second > 0.0);
    }
}
