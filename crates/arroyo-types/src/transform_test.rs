#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::{ArrowMessage, Record, RecordMetadata, SignalMessage, Watermark};
    use crate::transform::{DefaultTransformer, ProcessingMode, TransformContext, Transformer, TransformFactory};

    #[test]
    fn test_transform_context() {
        // 测试默认上下文
        let default_ctx = TransformContext::default();
        assert_eq!(default_ctx.mode, ProcessingMode::Hybrid);
        assert!(default_ctx.schema.is_none());
        assert!(default_ctx.config.is_empty());

        // 测试自定义上下文
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
        ]));

        let mut ctx = TransformContext::new(ProcessingMode::Streaming)
            .with_schema(schema.clone())
            .with_config("batch_size", "100");

        assert_eq!(ctx.mode, ProcessingMode::Streaming);
        assert_eq!(ctx.schema.as_ref().unwrap().as_ref(), schema.as_ref());
        assert_eq!(ctx.config.get("batch_size"), Some(&"100".to_string()));

        // 测试切换模式
        ctx.switch_mode(ProcessingMode::Messaging);
        assert_eq!(ctx.mode, ProcessingMode::Messaging);
    }

    #[test]
    fn test_default_transformer() {
        let transformer = DefaultTransformer;
        let ctx = TransformContext::default();

        // 创建测试记录
        let timestamp = SystemTime::now();
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100)
            .with_watermark(timestamp)
            .with_event_time(timestamp - Duration::from_secs(10));

        let mut headers = HashMap::new();
        headers.insert("header1".to_string(), vec![1, 2, 3]);

        let record = Record::new("test-value".to_string(), timestamp, metadata.clone())
            .with_key(vec![4, 5, 6])
            .with_headers(headers);

        // 测试 Record 到 ArrowMessage 的转换
        let arrow_message = transformer.transform_to_arrow(&record, &ctx).unwrap();

        // 测试 ArrowMessage 到 Record 的转换
        let transformed_record = transformer.transform_to_record(&arrow_message, &ctx).unwrap();
        assert!(transformed_record.is_some());

        let transformed_record = transformed_record.unwrap();
        assert_eq!(transformed_record.value, record.value);
        assert_eq!(transformed_record.key, record.key);
        assert_eq!(transformed_record.metadata.topic, record.metadata.topic);
        assert_eq!(transformed_record.metadata.partition, record.metadata.partition);
        assert_eq!(transformed_record.metadata.offset, record.metadata.offset);
    }

    #[test]
    fn test_processing_modes() {
        let transformer = DefaultTransformer;

        // 创建测试记录
        let timestamp = SystemTime::now();
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100);
        let _record = Record::new("test-value".to_string(), timestamp, metadata);

        // 创建信号消息
        let watermark = ArrowMessage::Signal(SignalMessage::Watermark(Watermark::EventTime(timestamp)));

        // 测试流处理模式
        let streaming_ctx = TransformContext::new(ProcessingMode::Streaming);
        let result = transformer.transform_to_record(&watermark, &streaming_ctx).unwrap();
        assert!(result.is_none()); // 信号消息不会转换为记录

        // 测试消息队列模式
        let messaging_ctx = TransformContext::new(ProcessingMode::Messaging);
        let result = transformer.transform_to_record(&watermark, &messaging_ctx).unwrap();
        assert!(result.is_none()); // 在消息队列模式下忽略信号消息

        // 测试混合模式
        let hybrid_ctx = TransformContext::new(ProcessingMode::Hybrid);
        let result = transformer.transform_to_record(&watermark, &hybrid_ctx).unwrap();
        assert!(result.is_none()); // 信号消息不会转换为记录
    }

    #[test]
    fn test_batch_transform() {
        let transformer = DefaultTransformer;
        let ctx = TransformContext::default();

        // 创建测试记录
        let timestamp = SystemTime::now();
        let metadata1 = RecordMetadata::new("test-topic".to_string(), 1, 100);
        let metadata2 = RecordMetadata::new("test-topic".to_string(), 1, 101);

        let record1 = Record::new("value1".to_string(), timestamp, metadata1);
        let record2 = Record::new("value2".to_string(), timestamp, metadata2);

        let records = vec![record1, record2];

        // 测试批量转换 Record 到 ArrowMessage
        let messages = transformer.transform_records_to_batch(&records, &ctx).unwrap();
        assert_eq!(messages.len(), 2);

        // 测试批量转换 ArrowMessage 到 Record
        let transformed_records = transformer.transform_batch_to_records(&messages, &ctx).unwrap();
        assert_eq!(transformed_records.len(), 2);
        assert_eq!(transformed_records[0].value, "value1");
        assert_eq!(transformed_records[1].value, "value2");
    }

    #[test]
    fn test_transform_factory() {
        let factory = TransformFactory::new();

        // 测试创建默认转换器
        let default_transformer = factory.create_default();
        let ctx = TransformContext::default();

        // 创建测试记录
        let timestamp = SystemTime::now();
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100);
        let record = Record::new("test-value".to_string(), timestamp, metadata);

        // 测试转换
        let arrow_message = default_transformer.transform_to_arrow(&record, &ctx).unwrap();
        let transformed_record = default_transformer.transform_to_record(&arrow_message, &ctx).unwrap();

        assert!(transformed_record.is_some());
        let transformed_record = transformed_record.unwrap();
        assert_eq!(transformed_record.value, record.value);
    }
}
