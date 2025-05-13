#[cfg(test)]
mod tests {
    use crate::controllers::topics::TopicController;
    use arroyo_rpc::api_types::topics::{
        CreateTopicRequest, TopicConfig, TopicHealthCheckRequest, TopicHealthStatus,
        UpdateTopicRequest,
    };
    use axum::response::IntoResponse;
    use axum::body::{self, Body};
    use std::env;
    use std::time::Duration;
    use tokio::time::sleep;

    // 获取测试服务器地址
    fn get_test_server() -> String {
        env::var("KAFKA_TEST_SERVER").unwrap_or_else(|_| "localhost:9092".to_string())
    }

    // 检查是否应该跳过需要 Kafka 服务器的测试
    fn should_skip_kafka_tests() -> bool {
        // 默认跳过 Kafka 测试，除非明确设置 SKIP_KAFKA_TESTS=false
        env::var("SKIP_KAFKA_TESTS").map(|v| v != "false").unwrap_or(true)
    }

    // 生成唯一的 Topic 名称
    fn generate_topic_name(prefix: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        format!("{}-{}", prefix, timestamp)
    }

    #[tokio::test]
    async fn test_topic_controller_crud() {
        if should_skip_kafka_tests() {
            println!("Skipping test_topic_controller_crud because SKIP_KAFKA_TESTS is true");
            return;
        }

        // 创建 TopicController
        let server = get_test_server();
        let controller = TopicController::new(&server).expect("Failed to create TopicController");

        // 生成唯一的 Topic 名称
        let topic_name = generate_topic_name("test-crud");

        // 创建 Topic
        let create_request = CreateTopicRequest {
            config: TopicConfig {
                name: topic_name.clone(),
                partitions: 3,
                replication_factor: 1,
                retention_ms: Some(86400000), // 1 day
                retention_bytes: Some(1073741824), // 1 GB
                cleanup_policy: "delete".to_string(),
                max_message_bytes: Some(1048576), // 1 MB
                description: Some("Test topic for CRUD operations".to_string()),
            },
        };

        // 创建 Topic
        let _create_response = controller
            .create_topic(create_request, None)
            .await
            .expect("Failed to create topic");

        // 等待 Topic 创建完成
        sleep(Duration::from_secs(1)).await;

        // 获取 Topic 列表
        let list_response = controller
            .list_topics(None)
            .await
            .expect("Failed to list topics");

        // 验证 Topic 是否在列表中
        // 从 IntoResponse 中提取 Json 数据
        let (_, _) = match list_response.into_response().into_parts() {
            (status, _body) => {
                // 简化测试，直接断言 Topic 已创建
                (status, ())
            }
        };

        // 再次获取 Topic 列表，验证 Topic 是否存在
        let list_response = controller
            .list_topics(None)
            .await
            .expect("Failed to list topics");

        // 从响应中提取 Json 数据
        let response_body = list_response.into_response().into_body();
        let response_bytes = body::to_bytes(response_body, 1024 * 1024).await.unwrap();
        let list_json: axum::Json<arroyo_rpc::api_types::topics::TopicListResponse> =
            serde_json::from_slice::<arroyo_rpc::api_types::topics::TopicListResponse>(&response_bytes).unwrap().into();

        // 验证 Topic 是否在列表中
        let topics = list_json.0.topics;
        assert!(
            topics.iter().any(|t| t.name == topic_name),
            "Created topic not found in list"
        );

        // 获取 Topic 详情
        let details_response = controller
            .get_topic_details(topic_name.clone(), None)
            .await
            .expect("Failed to get topic details");

        // 验证 Topic 详情
        // 从 IntoResponse 中提取 Json 数据
        let response_body = details_response.into_response().into_body();
        let response_bytes = body::to_bytes(response_body, 1024 * 1024).await.unwrap();
        let details_json: axum::Json<arroyo_rpc::api_types::topics::TopicDetailsResponse> =
            serde_json::from_slice::<arroyo_rpc::api_types::topics::TopicDetailsResponse>(&response_bytes).unwrap().into();

        // 验证 Topic 详情
        let topic_details = details_json.0.topic;
        assert_eq!(topic_details.info.name, topic_name);
        assert_eq!(topic_details.info.partitions, 3);
        assert_eq!(topic_details.info.replication_factor, 1);
        assert_eq!(topic_details.info.cleanup_policy, "delete");
        assert_eq!(
            topic_details.info.description,
            Some("Test topic for CRUD operations".to_string())
        );

        // 更新 Topic
        let update_request = UpdateTopicRequest {
            config: TopicConfig {
                name: topic_name.clone(),
                partitions: 3, // 分区数不能更改
                replication_factor: 1, // 副本因子不能更改
                retention_ms: Some(172800000), // 2 days
                retention_bytes: Some(2147483648), // 2 GB
                cleanup_policy: "delete".to_string(),
                max_message_bytes: Some(2097152), // 2 MB
                description: Some("Updated test topic".to_string()),
            },
        };

        // 更新 Topic
        let _update_response = controller
            .update_topic(topic_name.clone(), update_request, None)
            .await
            .expect("Failed to update topic");

        // 等待 Topic 更新完成
        sleep(Duration::from_secs(1)).await;

        // 获取更新后的 Topic 详情
        let updated_details_response = controller
            .get_topic_details(topic_name.clone(), None)
            .await
            .expect("Failed to get updated topic details");

        // 验证更新后的 Topic 详情
        // 从 IntoResponse 中提取 Json 数据
        let response_body = updated_details_response.into_response().into_body();
        let response_bytes = body::to_bytes(response_body, 1024 * 1024).await.unwrap();
        let updated_details_json: axum::Json<arroyo_rpc::api_types::topics::TopicDetailsResponse> =
            serde_json::from_slice::<arroyo_rpc::api_types::topics::TopicDetailsResponse>(&response_bytes).unwrap().into();

        // 验证更新后的 Topic 详情
        let updated_topic_details = updated_details_json.0.topic;
        assert_eq!(updated_topic_details.info.name, topic_name);
        assert_eq!(updated_topic_details.info.retention_ms, Some(172800000));
        assert_eq!(updated_topic_details.info.retention_bytes, Some(2147483648));
        assert_eq!(updated_topic_details.info.max_message_bytes, Some(2097152));
        assert_eq!(
            updated_topic_details.info.description,
            Some("Updated test topic".to_string())
        );

        // 检查 Topic 健康状态
        let health_request = TopicHealthCheckRequest {
            topics: Some(vec![topic_name.clone()]),
            force_refresh: false,
        };

        // 检查 Topic 健康状态
        let health_response = controller
            .check_topic_health(health_request)
            .await
            .expect("Failed to check topic health");

        // 验证健康状态
        // 从 IntoResponse 中提取 Json 数据
        let response_body = health_response.into_response().into_body();
        let response_bytes = body::to_bytes(response_body, 1024 * 1024).await.unwrap();
        let health_json: axum::Json<arroyo_rpc::api_types::topics::TopicHealthCheckResponse> =
            serde_json::from_slice::<arroyo_rpc::api_types::topics::TopicHealthCheckResponse>(&response_bytes).unwrap().into();

        // 验证健康状态
        assert!(health_json.0.topics.len() > 0);
        let topic_health = &health_json.0.topics[0];
        assert_eq!(topic_health.topic_name, topic_name);
        assert_eq!(topic_health.status, TopicHealthStatus::Healthy);

        // 删除 Topic
        let _delete_response = controller
            .delete_topic(topic_name.clone(), None)
            .await
            .expect("Failed to delete topic");

        // 等待 Topic 删除完成
        sleep(Duration::from_secs(1)).await;

        // 验证 Topic 已被删除
        let list_after_delete_response = controller
            .list_topics(None)
            .await
            .expect("Failed to list topics after delete");

        // 验证 Topic 已被删除
        // 从 IntoResponse 中提取 Json 数据
        let response_body = list_after_delete_response.into_response().into_body();
        let response_bytes = body::to_bytes(response_body, 1024 * 1024).await.unwrap();
        let list_after_delete_json: axum::Json<arroyo_rpc::api_types::topics::TopicListResponse> =
            serde_json::from_slice::<arroyo_rpc::api_types::topics::TopicListResponse>(&response_bytes).unwrap().into();

        // 验证 Topic 已被删除
        let topics_after_delete = list_after_delete_json.0.topics;
        assert!(
            !topics_after_delete.iter().any(|t| t.name == topic_name),
            "Deleted topic still found in list"
        );
    }
}
