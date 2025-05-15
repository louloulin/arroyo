#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use tokio::time::sleep;

    use crate::push::batch::BatchProcessor;
    use crate::push::source::PushMessage;

    #[test]
    fn test_batch_processor_add() {
        // Create batch processor
        let mut processor = BatchProcessor::new(3, Duration::from_millis(100));

        // Create messages
        let message1 = PushMessage {
            topic: "test-topic".to_string(),
            data: vec![1, 2, 3],
            timestamp: SystemTime::now(),
        };
        let message2 = PushMessage {
            topic: "test-topic".to_string(),
            data: vec![4, 5, 6],
            timestamp: SystemTime::now(),
        };
        let message3 = PushMessage {
            topic: "test-topic".to_string(),
            data: vec![7, 8, 9],
            timestamp: SystemTime::now(),
        };

        // Add messages
        let result = processor.add(message1);
        assert!(result.is_none());

        let result = processor.add(message2);
        assert!(result.is_none());

        // Adding the third message should trigger a flush
        let result = processor.add(message3);
        assert!(result.is_some());

        // Check batch
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert!(batch.contains_key("test-topic"));
        assert_eq!(batch["test-topic"].len(), 3);
    }

    #[test]
    fn test_batch_processor_flush() {
        // Create batch processor
        let mut processor = BatchProcessor::new(10, Duration::from_millis(100));

        // Create messages
        let message1 = PushMessage {
            topic: "test-topic".to_string(),
            data: vec![1, 2, 3],
            timestamp: SystemTime::now(),
        };
        let message2 = PushMessage {
            topic: "test-topic".to_string(),
            data: vec![4, 5, 6],
            timestamp: SystemTime::now(),
        };

        // Add messages
        processor.add(message1);
        processor.add(message2);

        // Flush
        let result = processor.flush();
        assert!(result.is_some());

        // Check batch
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert!(batch.contains_key("test-topic"));
        assert_eq!(batch["test-topic"].len(), 2);

        // Flush again
        let result = processor.flush();
        assert!(result.is_none());
    }

    #[test]
    fn test_batch_processor_should_flush() {
        // Create batch processor with short wait time
        let mut processor = BatchProcessor::new(10, Duration::from_millis(10));

        // Create message
        let message = PushMessage {
            topic: "test-topic".to_string(),
            data: vec![1, 2, 3],
            timestamp: SystemTime::now(),
        };

        // Add message
        processor.add(message);

        // Should not flush yet
        assert!(!processor.should_flush());

        // Wait for max wait time
        std::thread::sleep(Duration::from_millis(20));

        // Should flush now
        assert!(processor.should_flush());
    }

    #[test]
    fn test_batch_processor_multiple_topics() {
        // Create batch processor
        let mut processor = BatchProcessor::new(10, Duration::from_millis(100));

        // Create messages for different topics
        let message1 = PushMessage {
            topic: "topic1".to_string(),
            data: vec![1, 2, 3],
            timestamp: SystemTime::now(),
        };
        let message2 = PushMessage {
            topic: "topic2".to_string(),
            data: vec![4, 5, 6],
            timestamp: SystemTime::now(),
        };
        let message3 = PushMessage {
            topic: "topic1".to_string(),
            data: vec![7, 8, 9],
            timestamp: SystemTime::now(),
        };

        // Add messages
        processor.add(message1);
        processor.add(message2);
        processor.add(message3);

        // Flush
        let result = processor.flush();
        assert!(result.is_some());

        // Check batch
        let batch = result.unwrap();
        assert_eq!(batch.len(), 2);
        assert!(batch.contains_key("topic1"));
        assert!(batch.contains_key("topic2"));
        assert_eq!(batch["topic1"].len(), 2);
        assert_eq!(batch["topic2"].len(), 1);
    }

    #[test]
    fn test_batch_processor_size() {
        // Create batch processor
        let mut processor = BatchProcessor::new(10, Duration::from_millis(100));

        // Check initial size
        assert_eq!(processor.size(), 0);

        // Create messages
        let message1 = PushMessage {
            topic: "topic1".to_string(),
            data: vec![1, 2, 3],
            timestamp: SystemTime::now(),
        };
        let message2 = PushMessage {
            topic: "topic2".to_string(),
            data: vec![4, 5, 6],
            timestamp: SystemTime::now(),
        };

        // Add messages
        processor.add(message1);
        processor.add(message2);

        // Check size
        assert_eq!(processor.size(), 2);

        // Check size by topic
        let size_by_topic = processor.size_by_topic();
        assert_eq!(size_by_topic.len(), 2);
        assert_eq!(size_by_topic["topic1"], 1);
        assert_eq!(size_by_topic["topic2"], 1);

        // Flush
        processor.flush();

        // Check size after flush
        assert_eq!(processor.size(), 0);
    }
}
