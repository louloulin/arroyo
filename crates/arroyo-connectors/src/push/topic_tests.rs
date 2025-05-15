#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::push::topic::{CreateTopicRequest, TopicError, TopicManager};

    #[test]
    fn test_create_topic() {
        let manager = TopicManager::new();

        // Create a topic
        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: true,
        };

        let topic = manager.create_topic(request).unwrap();

        // Verify topic properties
        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.messages, 0);
        assert_eq!(topic.retention_period, 3600);
        assert_eq!(topic.compression, true);
        assert!(topic.last_activity.is_none());

        // Current time should be close to created_at
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(now - topic.created_at < 2);
    }

    #[test]
    fn test_create_duplicate_topic() {
        let manager = TopicManager::new();

        // Create a topic
        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: true,
        };

        manager.create_topic(request.clone()).unwrap();

        // Try to create the same topic again
        let result = manager.create_topic(request);

        // Should fail with TopicAlreadyExists
        match result {
            Err(TopicError::TopicAlreadyExists(name)) => {
                assert_eq!(name, "test-topic");
            }
            _ => panic!("Expected TopicAlreadyExists error"),
        }
    }

    #[test]
    fn test_get_topic() {
        let manager = TopicManager::new();

        // Create a topic
        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: true,
        };

        manager.create_topic(request).unwrap();

        // Get the topic
        let topic = manager.get_topic("test-topic").unwrap();

        // Verify topic properties
        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.messages, 0);
        assert_eq!(topic.retention_period, 3600);
        assert_eq!(topic.compression, true);
    }

    #[test]
    fn test_get_nonexistent_topic() {
        let manager = TopicManager::new();

        // Try to get a nonexistent topic
        let result = manager.get_topic("nonexistent");

        // Should fail with TopicNotFound
        match result {
            Err(TopicError::TopicNotFound(name)) => {
                assert_eq!(name, "nonexistent");
            }
            _ => panic!("Expected TopicNotFound error"),
        }
    }

    #[test]
    fn test_get_topics() {
        let manager = TopicManager::new();

        // Create some topics
        let topics = vec![
            CreateTopicRequest {
                name: "topic1".to_string(),
                retention_period: 3600,
                compression: true,
            },
            CreateTopicRequest {
                name: "topic2".to_string(),
                retention_period: 7200,
                compression: false,
            },
        ];

        for request in topics {
            manager.create_topic(request).unwrap();
        }

        // Get all topics
        let topics = manager.get_topics().unwrap();

        // Should have 2 topics
        assert_eq!(topics.len(), 2);

        // Verify we have the expected topics
        let topic_names: Vec<String> = topics.iter().map(|t| t.name.clone()).collect();
        assert!(topic_names.contains(&"topic1".to_string()));
        assert!(topic_names.contains(&"topic2".to_string()));

        // Verify topic properties
        let topic1 = topics.iter().find(|t| t.name == "topic1").unwrap();
        assert_eq!(topic1.retention_period, 3600);
        assert_eq!(topic1.compression, true);

        let topic2 = topics.iter().find(|t| t.name == "topic2").unwrap();
        assert_eq!(topic2.retention_period, 7200);
        assert_eq!(topic2.compression, false);
    }

    #[test]
    fn test_delete_topic() {
        let manager = TopicManager::new();

        // Create a topic
        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: true,
        };

        manager.create_topic(request).unwrap();

        // Delete the topic
        manager.delete_topic("test-topic").unwrap();

        // Try to get the deleted topic
        let result = manager.get_topic("test-topic");

        // Should fail with TopicNotFound
        match result {
            Err(TopicError::TopicNotFound(name)) => {
                assert_eq!(name, "test-topic");
            }
            _ => panic!("Expected TopicNotFound error"),
        }
    }

    #[test]
    fn test_delete_nonexistent_topic() {
        let manager = TopicManager::new();

        // Try to delete a nonexistent topic
        let result = manager.delete_topic("nonexistent");

        // Should fail with TopicNotFound
        match result {
            Err(TopicError::TopicNotFound(name)) => {
                assert_eq!(name, "nonexistent");
            }
            _ => panic!("Expected TopicNotFound error"),
        }
    }

    #[test]
    fn test_record_message() {
        let manager = TopicManager::new();

        // Create a topic
        let request = CreateTopicRequest {
            name: "test-topic".to_string(),
            retention_period: 3600,
            compression: true,
        };

        manager.create_topic(request).unwrap();

        // Record a message
        manager.record_message("test-topic", 100).unwrap();

        // Get the topic
        let topic = manager.get_topic("test-topic").unwrap();

        // Verify message count and last activity
        assert_eq!(topic.messages, 1);
        assert!(topic.last_activity.is_some());

        // Last activity should be close to current time
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(now - topic.last_activity.unwrap() < 2);

        // Record another message
        manager.record_message("test-topic", 200).unwrap();

        // Get the topic again
        let topic = manager.get_topic("test-topic").unwrap();

        // Verify message count
        assert_eq!(topic.messages, 2);
    }

    #[test]
    fn test_record_message_nonexistent_topic() {
        let manager = TopicManager::new();

        // Try to record a message for a nonexistent topic
        let result = manager.record_message("nonexistent", 100);

        // Should fail with TopicNotFound
        match result {
            Err(TopicError::TopicNotFound(name)) => {
                assert_eq!(name, "nonexistent");
            }
            _ => panic!("Expected TopicNotFound error"),
        }
    }

    #[test]
    fn test_invalid_topic_name() {
        let manager = TopicManager::new();

        // Try to create a topic with an invalid name
        let request = CreateTopicRequest {
            name: "invalid topic name".to_string(),
            retention_period: 3600,
            compression: true,
        };

        let result = manager.create_topic(request);

        // Should fail with InvalidTopicName
        match result {
            Err(TopicError::InvalidTopicName(name)) => {
                assert_eq!(name, "invalid topic name");
            }
            _ => panic!("Expected InvalidTopicName error"),
        }
    }
}
