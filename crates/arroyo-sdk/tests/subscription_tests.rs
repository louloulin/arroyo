use arroyo_sdk::{ArroyoClient, Consumer, ConsumerBuilder, Result, SubscriptionType};
use mockito;

use std::time::Duration;

// 创建模拟客户端
async fn create_mock_client(server: &mockito::Server) -> ArroyoClient {
    ArroyoClient::new(&server.url()).unwrap()
}

#[tokio::test]
async fn test_single_topic_subscription() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 获取消息
    let _m = server
        .mock("GET", "/api/topics/test-topic/messages")
        .match_query(mockito::Matcher::AllOf(vec![
            mockito::Matcher::UrlEncoded("group_id".into(), "test-group".into()),
            mockito::Matcher::Any,
            mockito::Matcher::Any,
        ]))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[
            {
                "key": [116, 101, 115, 116, 45, 107, 101, 121],
                "value": [116, 101, 115, 116, 45, 118, 97, 108, 117, 101],
                "timestamp": 1625097600000,
                "partition": 0,
                "offset": 42
            }
        ]"#)
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let consumer = Consumer::new(
        client,
        "test-topic",
        ConsumerBuilder::new("test-group")
            .subscribe("test-topic")
            .build(),
    );

    // 拉取消息
    let messages = consumer.poll(Duration::from_secs(1)).await?;

    // 验证结果
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].key, Some(b"test-key".to_vec()));
    assert_eq!(messages[0].value, b"test-value".to_vec());
    assert_eq!(messages[0].partition, Some(0));
    assert_eq!(messages[0].offset, Some(42));

    Ok(())
}

#[tokio::test]
async fn test_multiple_topics_subscription() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 获取所有 Topic
    let _m1 = server
        .mock("GET", "/api/topics")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"["topic1", "topic2", "topic3", "other-topic"]"#)
        .create_async()
        .await;

    // 设置模拟响应 - 获取 topic1 的消息
    let _m2 = server
        .mock("GET", "/api/topics/topic1/messages")
        .match_query(mockito::Matcher::AllOf(vec![
            mockito::Matcher::UrlEncoded("group_id".into(), "test-group".into()),
            mockito::Matcher::Any,
            mockito::Matcher::Any,
        ]))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[
            {
                "key": [107, 101, 121, 49],
                "value": [118, 97, 108, 117, 101, 49],
                "timestamp": 1625097600000,
                "partition": 0,
                "offset": 1
            }
        ]"#)
        .expect(1)
        .create_async()
        .await;

    // 设置模拟响应 - 获取 topic2 的消息
    let _m3 = server
        .mock("GET", "/api/topics/topic2/messages")
        .match_query(mockito::Matcher::AllOf(vec![
            mockito::Matcher::UrlEncoded("group_id".into(), "test-group".into()),
            mockito::Matcher::Any,
            mockito::Matcher::Any,
        ]))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[
            {
                "key": [107, 101, 121, 50],
                "value": [118, 97, 108, 117, 101, 50],
                "timestamp": 1625097600000,
                "partition": 0,
                "offset": 2
            }
        ]"#)
        .expect(1)
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let consumer = Consumer::new_with_subscription(
        client,
        SubscriptionType::multiple(vec!["topic1", "topic2"]),
        ConsumerBuilder::new("test-group")
            .build(),
    );

    // 拉取消息
    let messages = consumer.poll(Duration::from_secs(1)).await?;

    // 验证结果
    assert_eq!(messages.len(), 2);

    // 验证是否包含 topic1 的消息
    let topic1_message = messages.iter().find(|m| m.key == Some(b"key1".to_vec()));
    assert!(topic1_message.is_some());
    assert_eq!(topic1_message.unwrap().value, b"value1".to_vec());

    // 验证是否包含 topic2 的消息
    let topic2_message = messages.iter().find(|m| m.key == Some(b"key2".to_vec()));
    assert!(topic2_message.is_some());
    assert_eq!(topic2_message.unwrap().value, b"value2".to_vec());

    Ok(())
}

#[tokio::test]
async fn test_pattern_subscription() -> Result<()> {
    // 创建服务器
    let mut server = mockito::Server::new_async().await;

    // 设置模拟响应 - 获取所有 Topic
    let _m1 = server
        .mock("GET", "/api/topics")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"["test-topic1", "test-topic2", "other-topic"]"#)
        .create_async()
        .await;

    // 设置模拟响应 - 获取 test-topic1 的消息
    let _m2 = server
        .mock("GET", "/api/topics/test-topic1/messages")
        .match_query(mockito::Matcher::AllOf(vec![
            mockito::Matcher::UrlEncoded("group_id".into(), "test-group".into()),
            mockito::Matcher::Any,
            mockito::Matcher::Any,
        ]))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[
            {
                "key": [107, 101, 121, 49],
                "value": [118, 97, 108, 117, 101, 49],
                "timestamp": 1625097600000,
                "partition": 0,
                "offset": 1
            }
        ]"#)
        .expect(1)
        .create_async()
        .await;

    // 设置模拟响应 - 获取 test-topic2 的消息
    let _m3 = server
        .mock("GET", "/api/topics/test-topic2/messages")
        .match_query(mockito::Matcher::AllOf(vec![
            mockito::Matcher::UrlEncoded("group_id".into(), "test-group".into()),
            mockito::Matcher::Any,
            mockito::Matcher::Any,
        ]))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[
            {
                "key": [107, 101, 121, 50],
                "value": [118, 97, 108, 117, 101, 50],
                "timestamp": 1625097600000,
                "partition": 0,
                "offset": 2
            }
        ]"#)
        .expect(1)
        .create_async()
        .await;

    let client = create_mock_client(&server).await;
    let consumer = Consumer::new_with_subscription(
        client,
        SubscriptionType::pattern("test-.*"),
        ConsumerBuilder::new("test-group")
            .build(),
    );

    // 拉取消息
    let messages = consumer.poll(Duration::from_secs(1)).await?;

    // 验证结果
    assert_eq!(messages.len(), 2);

    // 验证是否包含 test-topic1 的消息
    let topic1_message = messages.iter().find(|m| m.key == Some(b"key1".to_vec()));
    assert!(topic1_message.is_some());
    assert_eq!(topic1_message.unwrap().value, b"value1".to_vec());

    // 验证是否包含 test-topic2 的消息
    let topic2_message = messages.iter().find(|m| m.key == Some(b"key2".to_vec()));
    assert!(topic2_message.is_some());
    assert_eq!(topic2_message.unwrap().value, b"value2".to_vec());

    Ok(())
}
