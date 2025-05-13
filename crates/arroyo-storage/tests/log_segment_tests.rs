use arroyo_storage::log_segment::{
    CompactionPolicy, LogSegmentConfig, LogSegmentManager, LogSegmentState,
};
use arroyo_storage::tests::mock_storage::MockStorageProvider;
use chrono::Utc;
use std::sync::Arc;

#[tokio::test]
async fn test_create_segment() {
    let storage_provider = Arc::new(MockStorageProvider::new());
    let config = LogSegmentConfig::default();
    let manager = LogSegmentManager::new(storage_provider, config);

    let topic = "test-topic";
    let partition = 0;
    let base_offset = 0;

    let segment = manager.create_segment(topic, partition, base_offset).await.unwrap();

    // 验证段元数据
    let metadata = segment.metadata.read().await;
    assert_eq!(metadata.topic, topic);
    assert_eq!(metadata.partition, partition);
    assert_eq!(metadata.base_offset, base_offset);
    assert_eq!(metadata.last_offset, base_offset);
    assert_eq!(metadata.state, LogSegmentState::Active);
    assert_eq!(metadata.size_bytes, 0);
    assert_eq!(metadata.message_count, 0);
}

#[tokio::test]
async fn test_append_and_read() {
    let storage_provider = Arc::new(MockStorageProvider::new());
    let config = LogSegmentConfig::default();
    let manager = LogSegmentManager::new(storage_provider, config);

    let topic = "test-topic";
    let partition = 0;
    let base_offset = 0;

    let segment = manager.create_segment(topic, partition, base_offset).await.unwrap();

    // 写入消息
    let message1 = b"Hello, world!";
    let offset1 = segment.append(message1, Some(Utc::now())).await.unwrap();
    assert_eq!(offset1, 1);

    let message2 = b"This is a test message";
    let offset2 = segment.append(message2, Some(Utc::now())).await.unwrap();
    assert_eq!(offset2, 2);

    // 读取消息
    let read_message1 = segment.read(offset1).await.unwrap();
    assert_eq!(&read_message1, message1);

    let read_message2 = segment.read(offset2).await.unwrap();
    assert_eq!(&read_message2, message2);

    // 验证段元数据
    let metadata = segment.metadata.read().await;
    assert_eq!(metadata.last_offset, 2);
    assert_eq!(metadata.message_count, 2);
    assert!(metadata.size_bytes > 0);
}

#[tokio::test]
async fn test_read_range() {
    let storage_provider = Arc::new(MockStorageProvider::new());
    let config = LogSegmentConfig::default();
    let manager = LogSegmentManager::new(storage_provider, config);

    let topic = "test-topic";
    let partition = 0;
    let base_offset = 0;

    let segment = manager.create_segment(topic, partition, base_offset).await.unwrap();

    // 写入多条消息
    let messages = vec![
        b"Message 1".to_vec(),
        b"Message 2".to_vec(),
        b"Message 3".to_vec(),
        b"Message 4".to_vec(),
        b"Message 5".to_vec(),
    ];

    for message in &messages {
        segment.append(message, Some(Utc::now())).await.unwrap();
    }

    // 读取范围
    let read_messages = segment.read_range(1, 3).await.unwrap();
    assert_eq!(read_messages.len(), 3);
    assert_eq!(&read_messages[0], &messages[0]);
    assert_eq!(&read_messages[1], &messages[1]);
    assert_eq!(&read_messages[2], &messages[2]);
}

#[tokio::test]
async fn test_roll_segment() {
    let storage_provider = Arc::new(MockStorageProvider::new());
    let config = LogSegmentConfig::default();
    let manager = LogSegmentManager::new(storage_provider, config);

    let topic = "test-topic";
    let partition = 0;

    // 创建并写入第一个段
    let segment1 = manager.get_active_segment(topic, partition).await.unwrap();
    let message1 = b"Message in segment 1";
    segment1.append(message1, Some(Utc::now())).await.unwrap();

    // 滚动到新段
    let segment2 = manager.roll_segment(topic, partition).await.unwrap();

    // 验证第一个段状态
    {
        let metadata = segment1.metadata.read().await;
        assert_eq!(metadata.state, LogSegmentState::ReadOnly);
    }

    // 验证第二个段状态
    {
        let metadata = segment2.metadata.read().await;
        assert_eq!(metadata.state, LogSegmentState::Active);
        assert_eq!(metadata.base_offset, 2);
    }

    // 写入第二个段
    let message2 = b"Message in segment 2";
    let offset2 = segment2.append(message2, Some(Utc::now())).await.unwrap();
    assert_eq!(offset2, 2);

    // 验证可以从两个段读取
    let read_message1 = segment1.read(1).await.unwrap();
    assert_eq!(&read_message1, message1);

    let read_message2 = segment2.read(offset2).await.unwrap();
    assert_eq!(&read_message2, message2);
}

#[tokio::test]
async fn test_get_segment() {
    let storage_provider = Arc::new(MockStorageProvider::new());
    let config = LogSegmentConfig::default();
    let manager = LogSegmentManager::new(storage_provider, config);

    let topic = "test-topic";
    let partition = 0;

    // 创建并写入第一个段
    let segment1 = manager.get_active_segment(topic, partition).await.unwrap();
    segment1.append(b"Message 1", Some(Utc::now())).await.unwrap();

    // 滚动到新段
    let segment2 = manager.roll_segment(topic, partition).await.unwrap();
    segment2.append(b"Message 2", Some(Utc::now())).await.unwrap();

    // 通过偏移量获取段
    let segment_for_offset1 = manager.get_segment(topic, partition, 1).await.unwrap();
    let segment_for_offset2 = manager.get_segment(topic, partition, 2).await.unwrap();

    assert_eq!(segment_for_offset1.segment_id, segment1.segment_id);
    assert_eq!(segment_for_offset2.segment_id, segment2.segment_id);
}

#[tokio::test]
async fn test_cleanup_expired_segments() {
    let temp_dir = TempDir::new().unwrap();
    let url = format!("file://{}", temp_dir.path().to_str().unwrap());
    let storage_provider = Arc::new(StorageProvider::for_url(&url).await.unwrap());

    // 使用较短的保留时间
    let mut config = LogSegmentConfig::default();
    config.retention_time_secs = 0; // 立即过期

    let manager = LogSegmentManager::new(storage_provider, config);

    let topic = "test-topic";
    let partition = 0;

    // 创建并写入第一个段
    let segment1 = manager.get_active_segment(topic, partition).await.unwrap();
    segment1.append(b"Message 1", Some(Utc::now())).await.unwrap();

    // 滚动到新段
    let segment2 = manager.roll_segment(topic, partition).await.unwrap();
    segment2.append(b"Message 2", Some(Utc::now())).await.unwrap();

    // 清理过期段
    let cleaned_count = manager.cleanup_expired_segments(topic, partition).await.unwrap();
    assert_eq!(cleaned_count, 1); // 应该清理了一个段

    // 验证第一个段已被清理
    let result = segment1.read(1).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_compact_segments() {
    let temp_dir = TempDir::new().unwrap();
    let url = format!("file://{}", temp_dir.path().to_str().unwrap());
    let storage_provider = Arc::new(StorageProvider::for_url(&url).await.unwrap());

    // 配置合并策略
    let mut config = LogSegmentConfig::default();
    config.compaction_policy = CompactionPolicy::SizeBased;
    config.max_segment_size_bytes = 1024; // 小一点，便于测试

    let manager = LogSegmentManager::new(storage_provider, config);

    let topic = "test-topic";
    let partition = 0;

    // 创建多个小段
    let segment1 = manager.get_active_segment(topic, partition).await.unwrap();
    segment1.append(b"Message 1", Some(Utc::now())).await.unwrap();

    let segment2 = manager.roll_segment(topic, partition).await.unwrap();
    segment2.append(b"Message 2", Some(Utc::now())).await.unwrap();

    let segment3 = manager.roll_segment(topic, partition).await.unwrap();
    segment3.append(b"Message 3", Some(Utc::now())).await.unwrap();

    // 执行合并
    let compacted_count = manager.compact_segments(topic, partition).await.unwrap();
    assert!(compacted_count > 0); // 应该合并了一些段

    // 验证可以读取所有消息
    let segment_for_offset1 = manager.get_segment(topic, partition, 1).await.unwrap();
    let message1 = segment_for_offset1.read(1).await.unwrap();
    assert_eq!(&message1, b"Message 1");

    let segment_for_offset2 = manager.get_segment(topic, partition, 2).await.unwrap();
    let message2 = segment_for_offset2.read(2).await.unwrap();
    assert_eq!(&message2, b"Message 2");

    let segment_for_offset3 = manager.get_segment(topic, partition, 3).await.unwrap();
    let message3 = segment_for_offset3.read(3).await.unwrap();
    assert_eq!(&message3, b"Message 3");
}
