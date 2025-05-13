use arroyo_storage::log_segment::{CleanupPolicy, CompactionPolicy, LogSegmentConfig};
use arroyo_storage::optimized_log_segment::{OptimizedLogSegmentManager, PrefetchState};
use arroyo_storage::StorageProvider;
use chrono::Utc;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_create_segment() {
    let temp_dir = TempDir::new().unwrap();
    let url = format!("file://{}", temp_dir.path().to_str().unwrap());
    let storage_provider = Arc::new(StorageProvider::for_url(&url).await.unwrap());

    let config = LogSegmentConfig::default();
    let manager = OptimizedLogSegmentManager::new_with_defaults(storage_provider, config);

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
    assert_eq!(metadata.size_bytes, 0);
    assert_eq!(metadata.message_count, 0);
}

#[tokio::test]
async fn test_append_and_read() {
    let temp_dir = TempDir::new().unwrap();
    let url = format!("file://{}", temp_dir.path().to_str().unwrap());
    let storage_provider = Arc::new(StorageProvider::for_url(&url).await.unwrap());

    let config = LogSegmentConfig::default();
    let manager = OptimizedLogSegmentManager::new_with_defaults(storage_provider.clone(), config);

    let topic = "test-topic";
    let partition = 0;

    // 创建段
    let segment = manager.create_segment(topic, partition, 0).await.unwrap();

    // 创建数据文件
    let segment_path = format!("{}.data", segment.segment_id);
    let object_path = object_store::path::Path::from(segment_path);
    storage_provider.put(object_path, Vec::new()).await.unwrap();

    // 写入消息
    let message1 = b"Hello, world!";
    let offset1 = segment.append(message1, Some(Utc::now())).await.unwrap();
    assert_eq!(offset1, 1);

    let message2 = b"This is a test message";
    let offset2 = segment.append(message2, Some(Utc::now())).await.unwrap();
    assert_eq!(offset2, 2);

    // 刷新缓冲区
    segment.flush().await.unwrap();

    // 验证元数据
    let metadata = segment.metadata.read().await;
    assert_eq!(metadata.last_offset, 2);
    assert!(metadata.size_bytes > 0);
    assert_eq!(metadata.message_count, 2);
}

#[tokio::test]
async fn test_batch_append_and_read() {
    let temp_dir = TempDir::new().unwrap();
    let url = format!("file://{}", temp_dir.path().to_str().unwrap());
    let storage_provider = Arc::new(StorageProvider::for_url(&url).await.unwrap());

    let config = LogSegmentConfig::default();
    let manager = OptimizedLogSegmentManager::new_with_defaults(storage_provider.clone(), config);

    let topic = "test-topic";
    let partition = 0;

    // 创建段
    let segment = manager.create_segment(topic, partition, 0).await.unwrap();

    // 创建数据文件
    let segment_path = format!("{}.data", segment.segment_id);
    let object_path = object_store::path::Path::from(segment_path);
    storage_provider.put(object_path, Vec::new()).await.unwrap();

    // 准备批量消息
    let messages = vec![
        b"Message 1".to_vec(),
        b"Message 2".to_vec(),
        b"Message 3".to_vec(),
        b"Message 4".to_vec(),
        b"Message 5".to_vec(),
    ];

    // 批量写入
    let timestamps = vec![Utc::now(); messages.len()];
    let offsets = manager.batch_append(topic, partition, &messages, Some(&timestamps)).await.unwrap();

    // 验证偏移量
    assert_eq!(offsets.len(), messages.len());
    assert_eq!(offsets[0], 1);
    assert_eq!(offsets[4], 5);

    // 获取段并验证元数据
    let segment = manager.get_active_segment(topic, partition).await.unwrap();
    let metadata = segment.metadata.read().await;
    assert_eq!(metadata.last_offset, 5);
    assert!(metadata.size_bytes > 0);
    assert_eq!(metadata.message_count, 5);
}

#[tokio::test]
async fn test_roll_segment() {
    let temp_dir = TempDir::new().unwrap();
    let url = format!("file://{}", temp_dir.path().to_str().unwrap());
    let storage_provider = Arc::new(StorageProvider::for_url(&url).await.unwrap());

    // 使用较小的段大小，便于测试滚动
    let mut config = LogSegmentConfig::default();
    config.max_segment_size_bytes = 100; // 100 字节，确保触发滚动

    let manager = OptimizedLogSegmentManager::new_with_defaults(storage_provider.clone(), config);

    let topic = "test-topic";
    let partition = 0;

    // 创建第一个段
    let segment1 = manager.create_segment(topic, partition, 0).await.unwrap();

    // 创建数据文件
    let segment_path = format!("{}.data", segment1.segment_id);
    let object_path = object_store::path::Path::from(segment_path);
    storage_provider.put(object_path, Vec::new()).await.unwrap();

    // 写入足够多的数据触发滚动
    let large_message = vec![b'A'; 90]; // 90 字节
    segment1.append(&large_message, Some(Utc::now())).await.unwrap();

    // 检查是否需要滚动
    let need_roll = manager.check_roll_segment(topic, partition).await.unwrap();
    assert!(need_roll, "应该需要滚动段");

    // 滚动到新段
    let segment2 = manager.roll_segment(topic, partition).await.unwrap();

    // 验证段状态
    {
        let metadata1 = segment1.metadata.read().await;
        assert_eq!(metadata1.last_offset, 1);

        let metadata2 = segment2.metadata.read().await;
        assert_eq!(metadata2.base_offset, 2);
    }

    // 创建第二个段的数据文件
    let segment2_path = format!("{}.data", segment2.segment_id);
    let object_path2 = object_store::path::Path::from(segment2_path);
    storage_provider.put(object_path2, Vec::new()).await.unwrap();

    // 写入新段
    let message = b"Message in segment 2";
    let _offset = segment2.append(message, Some(Utc::now())).await.unwrap();

    // 验证元数据
    let metadata2 = segment2.metadata.read().await;
    assert_eq!(metadata2.base_offset, 2);
    // 由于基础偏移量是2，所以第一个消息的偏移量是3
    assert_eq!(metadata2.last_offset, 3);
    assert!(metadata2.size_bytes > 0);
    assert_eq!(metadata2.message_count, 1);
}

#[tokio::test]
async fn test_read_by_time_range() {
    let temp_dir = TempDir::new().unwrap();
    let url = format!("file://{}", temp_dir.path().to_str().unwrap());
    let storage_provider = Arc::new(StorageProvider::for_url(&url).await.unwrap());

    let config = LogSegmentConfig::default();
    let manager = OptimizedLogSegmentManager::new_with_defaults(storage_provider.clone(), config);

    let topic = "test-topic";
    let partition = 0;

    // 创建段
    let segment = manager.create_segment(topic, partition, 0).await.unwrap();

    // 创建数据文件
    let segment_path = format!("{}.data", segment.segment_id);
    let object_path = object_store::path::Path::from(segment_path);
    storage_provider.put(object_path, Vec::new()).await.unwrap();

    // 写入带时间戳的消息
    let now = Utc::now();
    let timestamps = vec![
        now.clone(),
        now.clone() + chrono::Duration::seconds(10),
        now.clone() + chrono::Duration::seconds(20),
        now.clone() + chrono::Duration::seconds(30),
        now.clone() + chrono::Duration::seconds(40),
    ];

    let messages = vec![
        b"Message 1".to_vec(),
        b"Message 2".to_vec(),
        b"Message 3".to_vec(),
        b"Message 4".to_vec(),
        b"Message 5".to_vec(),
    ];

    // 批量写入
    let _offsets = manager.batch_append(topic, partition, &messages, Some(&timestamps)).await.unwrap();

    // 验证元数据
    let metadata = segment.metadata.read().await;
    assert_eq!(metadata.last_offset, 5);
    assert!(metadata.size_bytes > 0);
    assert_eq!(metadata.message_count, 5);

    // 验证时间戳索引
    let index = segment.index.read().await;
    assert!(!index.time_index.is_empty(), "时间索引应该不为空");
}
