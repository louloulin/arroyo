#[cfg(test)]
mod tests {
    use crate::topic::metrics::TopicMetricsCollector;
    use arroyo_rpc::api_types::metrics::TopicMetricsFilter;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn test_collect_topic_metrics() {
        // 创建指标收集器
        let collector = TopicMetricsCollector::new("localhost:9092", Some(1), Some(3600)).unwrap();

        // 收集 Topic 指标
        let metrics = collector.collect_topic_metrics("test-topic").await.unwrap();

        // 验证指标
        assert_eq!(metrics.topic_name, "test-topic");
        assert_eq!(metrics.partition_count, 3);
        assert!(metrics.total_size_bytes > 0);
        assert!(metrics.total_message_count > 0);
        assert!(metrics.bytes_in_per_sec > 0.0);
        assert!(metrics.bytes_out_per_sec > 0.0);
        assert!(metrics.messages_in_per_sec > 0.0);
    }

    #[tokio::test]
    async fn test_collect_partition_metrics() {
        // 创建指标收集器
        let collector = TopicMetricsCollector::new("localhost:9092", Some(1), Some(3600)).unwrap();

        // 收集分区指标
        let metrics = collector.collect_partition_metrics("test-topic").await.unwrap();

        // 验证指标
        assert_eq!(metrics.len(), 3);
        for (partition_id, partition_metrics) in metrics {
            assert_eq!(partition_metrics.topic_name, "test-topic");
            assert_eq!(partition_metrics.partition_id, partition_id);
            assert!(partition_metrics.size_bytes > 0);
            assert!(partition_metrics.message_count > 0);
            assert!(partition_metrics.bytes_in_per_sec > 0.0);
            assert!(partition_metrics.bytes_out_per_sec > 0.0);
            assert!(partition_metrics.messages_in_per_sec > 0.0);
        }
    }

    #[tokio::test]
    async fn test_get_topic_metrics_history() {
        // 创建指标收集器
        let collector = TopicMetricsCollector::new("localhost:9092", Some(1), Some(3600)).unwrap();

        // 收集 Topic 指标，以便创建历史数据
        let _ = collector.collect_topic_metrics("test-topic").await.unwrap();

        // 等待一秒，以便创建新的历史数据点
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // 再次收集 Topic 指标
        let _ = collector.collect_topic_metrics("test-topic").await.unwrap();

        // 获取历史数据
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let history = collector
            .get_topic_metrics_history("test-topic", Some(now - 3600), Some(now))
            .await
            .unwrap();

        // 验证历史数据
        assert!(history.len() >= 2);
        for (timestamp, metrics) in &history {
            assert!(*timestamp <= now);
            assert_eq!(metrics.topic_name, "test-topic");
        }
    }

    #[tokio::test]
    async fn test_query_topic_metrics() {
        // 创建指标收集器
        let collector = TopicMetricsCollector::new("localhost:9092", Some(1), Some(3600)).unwrap();

        // 收集多个 Topic 的指标
        let _ = collector.collect_topic_metrics("test-topic-1").await.unwrap();
        let _ = collector.collect_topic_metrics("test-topic-2").await.unwrap();
        let _ = collector.collect_topic_metrics("other-topic").await.unwrap();

        // 查询 Topic 指标
        let filter = TopicMetricsFilter {
            topic_pattern: Some("test".to_string()),
            min_partition_count: Some(1),
            max_partition_count: None,
            min_message_rate: None,
            max_message_rate: None,
            sort_by: Some("name".to_string()),
            sort_desc: Some(false),
            limit: Some(10),
            offset: Some(0),
        };

        let results = collector.query_topic_metrics(filter).await.unwrap();

        // 验证查询结果
        assert_eq!(results.len(), 2);
        assert!(results[0].topic_name.contains("test"));
        assert!(results[1].topic_name.contains("test"));
    }
}
