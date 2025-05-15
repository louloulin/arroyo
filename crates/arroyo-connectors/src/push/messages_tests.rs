#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::push::messages::{MessageStore, MessageQuery};

    #[test]
    fn test_store_message() {
        let store = MessageStore::new(1000);

        // Store a message
        let message = store.store_message("test-topic", b"Hello, world!".to_vec()).unwrap();

        // Verify message
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.content, b"Hello, world!");
        assert_eq!(message.size, 13);
        assert_eq!(message.id, 1);

        // Current time should be close to message timestamp
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(now - message.timestamp < 2);
    }

    #[test]
    fn test_query_messages() {
        let store = MessageStore::new(1000);

        // Store multiple messages
        store.store_message("test-topic", b"Message 1".to_vec()).unwrap();
        store.store_message("test-topic", b"Message 2".to_vec()).unwrap();
        store.store_message("test-topic", b"Message 3".to_vec()).unwrap();

        // Query messages
        let query = MessageQuery {
            topic: "test-topic".to_string(),
            limit: 10,
            offset: 0,
            start_time: None,
            end_time: None,
        };

        let messages = store.query_messages(&query).unwrap();

        // Verify messages
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].id, 1);
        assert_eq!(messages[0].content, b"Message 1");
        assert_eq!(messages[1].id, 2);
        assert_eq!(messages[1].content, b"Message 2");
        assert_eq!(messages[2].id, 3);
        assert_eq!(messages[2].content, b"Message 3");
    }

    #[test]
    fn test_query_messages_with_pagination() {
        let store = MessageStore::new(1000);

        // Store multiple messages
        for i in 1..=10 {
            store.store_message("test-topic", format!("Message {}", i).as_bytes().to_vec()).unwrap();
        }

        // Query first page
        let query1 = MessageQuery {
            topic: "test-topic".to_string(),
            limit: 5,
            offset: 0,
            start_time: None,
            end_time: None,
        };

        let messages1 = store.query_messages(&query1).unwrap();

        // Verify first page
        assert_eq!(messages1.len(), 5);
        assert_eq!(messages1[0].id, 1);
        assert_eq!(messages1[4].id, 5);

        // Query second page
        let query2 = MessageQuery {
            topic: "test-topic".to_string(),
            limit: 5,
            offset: 5,
            start_time: None,
            end_time: None,
        };

        let messages2 = store.query_messages(&query2).unwrap();

        // Verify second page
        assert_eq!(messages2.len(), 5);
        assert_eq!(messages2[0].id, 6);
        assert_eq!(messages2[4].id, 10);
    }

    #[test]
    fn test_query_messages_with_time_range() {
        let store = MessageStore::new(1000);

        // Store messages with different timestamps
        let message1 = store.store_message("test-topic", b"Message 1".to_vec()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10)); // Ensure different timestamps
        let message2 = store.store_message("test-topic", b"Message 2".to_vec()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10)); // Ensure different timestamps
        let message3 = store.store_message("test-topic", b"Message 3".to_vec()).unwrap();

        // Query messages with time range
        let query = MessageQuery {
            topic: "test-topic".to_string(),
            limit: 10,
            offset: 0,
            start_time: Some(message1.timestamp),
            end_time: Some(message2.timestamp),
        };

        let messages = store.query_messages(&query).unwrap();

        // Verify that we have at least message1 and message2
        assert!(messages.len() >= 2);
        assert!(messages.iter().any(|m| m.id == 1));
        assert!(messages.iter().any(|m| m.id == 2));

        // Verify that message3 is not included if timestamps are different
        if message3.timestamp > message2.timestamp {
            assert!(!messages.iter().any(|m| m.id == 3));
        }
    }

    #[test]
    fn test_get_message() {
        let store = MessageStore::new(1000);

        // Store a message
        store.store_message("test-topic", b"Hello, world!".to_vec()).unwrap();

        // Get message
        let message = store.get_message("test-topic", 1).unwrap().unwrap();

        // Verify message
        assert_eq!(message.id, 1);
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.content, b"Hello, world!");
    }

    #[test]
    fn test_get_nonexistent_message() {
        let store = MessageStore::new(1000);

        // Get nonexistent message
        let result = store.get_message("test-topic", 1).unwrap();

        // Should return None
        assert!(result.is_none());
    }

    #[test]
    fn test_message_count() {
        let store = MessageStore::new(1000);

        // Store messages
        store.store_message("topic1", b"Message 1".to_vec()).unwrap();
        store.store_message("topic1", b"Message 2".to_vec()).unwrap();
        store.store_message("topic2", b"Message 3".to_vec()).unwrap();

        // Get message count
        let count1 = store.get_message_count("topic1").unwrap();
        let count2 = store.get_message_count("topic2").unwrap();
        let count3 = store.get_message_count("topic3").unwrap();

        // Verify counts
        assert_eq!(count1, 2);
        assert_eq!(count2, 1);
        assert_eq!(count3, 0);
    }

    #[test]
    fn test_clear_messages() {
        let store = MessageStore::new(1000);

        // Store messages
        store.store_message("test-topic", b"Message 1".to_vec()).unwrap();
        store.store_message("test-topic", b"Message 2".to_vec()).unwrap();

        // Clear messages
        store.clear_messages("test-topic").unwrap();

        // Get message count
        let count = store.get_message_count("test-topic").unwrap();

        // Verify count
        assert_eq!(count, 0);
    }

    #[test]
    fn test_max_messages_per_topic() {
        let store = MessageStore::new(3);

        // Store more messages than the limit
        store.store_message("test-topic", b"Message 1".to_vec()).unwrap();
        store.store_message("test-topic", b"Message 2".to_vec()).unwrap();
        store.store_message("test-topic", b"Message 3".to_vec()).unwrap();
        store.store_message("test-topic", b"Message 4".to_vec()).unwrap();
        store.store_message("test-topic", b"Message 5".to_vec()).unwrap();

        // Query messages
        let query = MessageQuery {
            topic: "test-topic".to_string(),
            limit: 10,
            offset: 0,
            start_time: None,
            end_time: None,
        };

        let messages = store.query_messages(&query).unwrap();

        // Verify messages (oldest messages should be removed)
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].id, 3);
        assert_eq!(messages[0].content, b"Message 3");
        assert_eq!(messages[1].id, 4);
        assert_eq!(messages[1].content, b"Message 4");
        assert_eq!(messages[2].id, 5);
        assert_eq!(messages[2].content, b"Message 5");
    }
}
