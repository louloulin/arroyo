#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    use crate::{
        arrow_message_to_record, record_to_arrow_message, ArrowMessage, Record, RecordMetadata,
    };

    #[test]
    fn test_record_metadata() {
        let topic = "test-topic".to_string();
        let partition = 1;
        let offset = 100;

        let metadata = RecordMetadata::new(topic.clone(), partition, offset);
        assert_eq!(metadata.topic, topic);
        assert_eq!(metadata.partition, partition);
        assert_eq!(metadata.offset, offset);
        assert_eq!(metadata.watermark, None);
        assert_eq!(metadata.event_time, None);

        let now = SystemTime::now();
        let metadata_with_watermark = metadata.clone().with_watermark(now);
        assert_eq!(metadata_with_watermark.watermark, Some(now));

        let event_time = now - Duration::from_secs(10);
        let metadata_with_event_time = metadata.with_event_time(event_time);
        assert_eq!(metadata_with_event_time.event_time, Some(event_time));
    }

    #[test]
    fn test_record() {
        let value = "test-value".to_string();
        let timestamp = SystemTime::now();
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100);

        let record = Record::new(value.clone(), timestamp, metadata.clone());
        assert_eq!(record.key(), None);
        assert_eq!(record.value(), &value);
        assert_eq!(record.timestamp(), timestamp);
        assert_eq!(record.metadata(), &metadata);
        assert!(record.headers.is_empty());

        let key = vec![1, 2, 3];
        let record_with_key = record.clone().with_key(key.clone());
        assert_eq!(record_with_key.key(), Some(key.as_slice()));

        let header_key = "header1".to_string();
        let header_value = vec![4, 5, 6];
        let record_with_header = record.clone().with_header(header_key.clone(), header_value.clone());
        assert_eq!(record_with_header.header(&header_key), Some(header_value.as_slice()));

        let mut headers = HashMap::new();
        headers.insert("header2".to_string(), vec![7, 8, 9]);
        let record_with_headers = record.with_headers(headers.clone());
        assert_eq!(record_with_headers.headers.len(), 1);
        assert_eq!(
            record_with_headers.header("header2"),
            Some(vec![7, 8, 9].as_slice())
        );
    }

    #[test]
    fn test_record_map() {
        let value = "42".to_string();
        let timestamp = SystemTime::now();
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100);

        let record = Record::new(value, timestamp, metadata);
        let mapped_record = record.map(|v| v.parse::<i32>().unwrap());

        assert_eq!(mapped_record.value(), &42);
    }

    #[test]
    fn test_record_to_arrow_message() {
        let timestamp = SystemTime::now();
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100)
            .with_watermark(timestamp)
            .with_event_time(timestamp - Duration::from_secs(10));

        let mut headers = HashMap::new();
        headers.insert("header1".to_string(), vec![1, 2, 3]);

        let record = Record::new("test-value".to_string(), timestamp, metadata)
            .with_key(vec![4, 5, 6])
            .with_headers(headers);

        let arrow_message = record_to_arrow_message(&record);
        match arrow_message {
            ArrowMessage::Data(batch) => {
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.num_columns(), 9);
            }
            _ => panic!("Expected ArrowMessage::Data"),
        }
    }

    #[test]
    fn test_arrow_message_to_record() {
        let timestamp = SystemTime::now();
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100)
            .with_watermark(timestamp)
            .with_event_time(timestamp - Duration::from_secs(10));

        let mut headers = HashMap::new();
        headers.insert("header1".to_string(), vec![1, 2, 3]);

        let original_record = Record::new("test-value".to_string(), timestamp, metadata)
            .with_key(vec![4, 5, 6])
            .with_headers(headers);

        let arrow_message = record_to_arrow_message(&original_record);
        let converted_record = arrow_message_to_record(&arrow_message).unwrap();

        assert_eq!(converted_record.value, original_record.value);
        assert_eq!(converted_record.key, original_record.key);
        assert_eq!(converted_record.metadata.topic, original_record.metadata.topic);
        assert_eq!(converted_record.metadata.partition, original_record.metadata.partition);
        assert_eq!(converted_record.metadata.offset, original_record.metadata.offset);
    }
}
