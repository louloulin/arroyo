#[cfg(test)]
mod tests {
    use crate::controllers::topics::TopicController;
    use arroyo_rpc::api_types::topics::{
        CreateTopicRequest, TopicConfig, TopicDetails, TopicInfo, TopicPartitionInfo,
        UpdateTopicRequest,
    };
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // Mock implementation of TopicAdmin for testing
    struct MockTopicAdmin {
        topics: Mutex<Vec<TopicInfo>>,
    }

    impl MockTopicAdmin {
        fn new() -> Self {
            Self {
                topics: Mutex::new(Vec::new()),
            }
        }

        async fn create_topic(
            &self,
            config: &TopicConfig,
            _user_id: Option<&str>,
        ) -> anyhow::Result<TopicInfo> {
            let mut topics = self.topics.lock().await;
            
            // Check if topic already exists
            if topics.iter().any(|t| t.name == config.name) {
                return Err(anyhow::anyhow!("Topic already exists"));
            }

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let topic_info = TopicInfo {
                name: config.name.clone(),
                partitions: config.partitions,
                replication_factor: config.replication_factor,
                retention_ms: config.retention_ms,
                retention_bytes: config.retention_bytes,
                cleanup_policy: config.cleanup_policy.clone(),
                max_message_bytes: config.max_message_bytes,
                description: config.description.clone(),
                created_at: now,
                updated_at: now,
            };

            topics.push(topic_info.clone());
            Ok(topic_info)
        }

        async fn update_topic(
            &self,
            config: &TopicConfig,
            _user_id: Option<&str>,
        ) -> anyhow::Result<TopicInfo> {
            let mut topics = self.topics.lock().await;
            
            // Find the topic
            let index = topics
                .iter()
                .position(|t| t.name == config.name)
                .ok_or_else(|| anyhow::anyhow!("Topic not found"))?;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let topic_info = TopicInfo {
                name: config.name.clone(),
                partitions: topics[index].partitions,
                replication_factor: topics[index].replication_factor,
                retention_ms: config.retention_ms,
                retention_bytes: config.retention_bytes,
                cleanup_policy: config.cleanup_policy.clone(),
                max_message_bytes: config.max_message_bytes,
                description: config.description.clone(),
                created_at: topics[index].created_at,
                updated_at: now,
            };

            topics[index] = topic_info.clone();
            Ok(topic_info)
        }

        async fn delete_topic(&self, name: &str, _user_id: Option<&str>) -> anyhow::Result<()> {
            let mut topics = self.topics.lock().await;
            
            // Find the topic
            let index = topics
                .iter()
                .position(|t| t.name == name)
                .ok_or_else(|| anyhow::anyhow!("Topic not found"))?;

            topics.remove(index);
            Ok(())
        }

        async fn list_topics(&self, _user_id: Option<&str>) -> anyhow::Result<Vec<TopicInfo>> {
            let topics = self.topics.lock().await;
            Ok(topics.clone())
        }

        async fn get_topic_details(
            &self,
            name: &str,
            _user_id: Option<&str>,
        ) -> anyhow::Result<TopicDetails> {
            let topics = self.topics.lock().await;
            
            // Find the topic
            let topic = topics
                .iter()
                .find(|t| t.name == name)
                .ok_or_else(|| anyhow::anyhow!("Topic not found"))?;

            // Create mock partitions
            let partitions = vec![
                TopicPartitionInfo {
                    id: 0,
                    leader: 1,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                },
                TopicPartitionInfo {
                    id: 1,
                    leader: 2,
                    replicas: vec![2, 3],
                    isr: vec![2, 3],
                },
            ];

            Ok(TopicDetails {
                info: topic.clone(),
                partitions,
                message_count: 100,
                size_bytes: 1024 * 1024, // 1 MB
            })
        }
    }

    // Mock implementation of TopicHealthChecker for testing
    struct MockTopicHealthChecker {}

    impl MockTopicHealthChecker {
        fn new() -> Self {
            Self {}
        }
    }

    #[tokio::test]
    async fn test_create_topic() {
        // Create mock admin
        let admin = Arc::new(MockTopicAdmin::new());
        let health_checker = Arc::new(tokio::sync::Mutex::new(MockTopicHealthChecker::new()));

        // Create controller with mock admin
        let controller = TopicController {
            admin,
            health_checker,
        };

        // Create topic request
        let request = CreateTopicRequest {
            config: TopicConfig {
                name: "test-topic".to_string(),
                partitions: 2,
                replication_factor: 1,
                retention_ms: Some(86400000), // 1 day
                retention_bytes: None,
                cleanup_policy: "delete".to_string(),
                max_message_bytes: None,
                description: Some("Test topic".to_string()),
            },
        };

        // Create topic
        let response = controller.create_topic(request, None).await.unwrap();
        
        // Check response
        let (status, json) = response.into_parts();
        assert_eq!(status, axum::http::StatusCode::CREATED);
        
        let topic_info: TopicInfo = serde_json::from_slice(&json.0).unwrap();
        assert_eq!(topic_info.name, "test-topic");
        assert_eq!(topic_info.partitions, 2);
        assert_eq!(topic_info.replication_factor, 1);
        assert_eq!(topic_info.retention_ms, Some(86400000));
        assert_eq!(topic_info.cleanup_policy, "delete");
        assert_eq!(topic_info.description, Some("Test topic".to_string()));
    }

    // Add more tests for other controller methods
}
