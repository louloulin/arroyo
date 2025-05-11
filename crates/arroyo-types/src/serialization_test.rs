#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use crate::{
        create_deserializer, create_raw_bytes_deserializer, create_serializer, Record, RecordMetadata,
        SerializationFormat,
    };

    #[test]
    fn test_json_serialization() {
        let timestamp = SystemTime::now();
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100)
            .with_watermark(timestamp)
            .with_event_time(timestamp);

        let mut headers = HashMap::new();
        headers.insert("header1".to_string(), vec![1, 2, 3]);

        let record = Record::new("test-value".to_string(), timestamp, metadata.clone())
            .with_key(vec![4, 5, 6])
            .with_headers(headers);

        // 创建 JSON 序列化器
        let serializer = create_serializer::<String>(SerializationFormat::Json).unwrap();
        assert_eq!(serializer.format(), SerializationFormat::Json);

        // 序列化记录
        let serialized = serializer.serialize(&record).unwrap();

        // 创建 JSON 反序列化器
        let deserializer = create_deserializer::<String>(SerializationFormat::Json).unwrap();
        assert_eq!(deserializer.format(), SerializationFormat::Json);

        // 反序列化记录
        let deserialized = deserializer.deserialize(&serialized, metadata).unwrap();

        // 验证反序列化结果
        assert_eq!(deserialized.value, record.value);
        assert_eq!(deserialized.key, record.key);
        assert_eq!(deserialized.headers.len(), record.headers.len());
        assert_eq!(
            deserialized.headers.get("header1"),
            record.headers.get("header1")
        );
        assert_eq!(deserialized.metadata.topic, record.metadata.topic);
        assert_eq!(deserialized.metadata.partition, record.metadata.partition);
        assert_eq!(deserialized.metadata.offset, record.metadata.offset);
    }

    #[test]
    fn test_raw_bytes_serialization() {
        let timestamp = SystemTime::now();
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100);

        let value = "test-value".to_string();
        let record = Record::new(value.clone(), timestamp, metadata.clone());

        // 创建原始字节序列化器
        let serializer = create_serializer::<String>(SerializationFormat::RawBytes).unwrap();
        assert_eq!(serializer.format(), SerializationFormat::RawBytes);

        // 序列化记录
        let serialized = serializer.serialize(&record).unwrap();

        // 创建原始字节反序列化器
        let deserializer = create_raw_bytes_deserializer::<Vec<u8>>(SerializationFormat::RawBytes).unwrap();
        assert_eq!(deserializer.format(), SerializationFormat::RawBytes);

        // 反序列化记录
        let deserialized = deserializer.deserialize(&serialized, metadata).unwrap();

        // 验证反序列化结果
        assert_eq!(String::from_utf8(deserialized.value).unwrap(), record.value);
        assert!(deserialized.key.is_none());
        assert!(deserialized.headers.is_empty());
        assert_eq!(deserialized.metadata.topic, record.metadata.topic);
        assert_eq!(deserialized.metadata.partition, record.metadata.partition);
        assert_eq!(deserialized.metadata.offset, record.metadata.offset);
    }

    #[test]
    fn test_unsupported_formats() {
        // 测试不支持的序列化格式
        assert!(create_serializer::<String>(SerializationFormat::Avro).is_err());
        assert!(create_serializer::<String>(SerializationFormat::Protobuf).is_err());

        // 测试不支持的反序列化格式
        assert!(create_deserializer::<String>(SerializationFormat::Avro).is_err());
        assert!(create_deserializer::<String>(SerializationFormat::Protobuf).is_err());
        assert!(create_deserializer::<String>(SerializationFormat::RawBytes).is_err());

        // 测试 RawBytes 反序列化器的限制
        assert!(create_raw_bytes_deserializer::<Vec<u8>>(SerializationFormat::Json).is_err());
        assert!(create_raw_bytes_deserializer::<Vec<u8>>(SerializationFormat::Avro).is_err());
        assert!(create_raw_bytes_deserializer::<Vec<u8>>(SerializationFormat::Protobuf).is_err());
    }
}
