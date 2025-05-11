use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::{
    Array, BinaryArray, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow_array::builder::{BinaryBuilder, StringBuilder, TimestampNanosecondBuilder};

use crate::{ArrowMessage, Record, RecordMetadata, to_nanos, from_nanos};

// 常量定义
const KEY_FIELD: &str = "_key";
const VALUE_FIELD: &str = "value";
const HEADERS_FIELD: &str = "_headers";
const TIMESTAMP_FIELD: &str = "_timestamp";
const TOPIC_FIELD: &str = "_topic";
const PARTITION_FIELD: &str = "_partition";
const OFFSET_FIELD: &str = "_offset";
const WATERMARK_FIELD: &str = "_watermark";
const EVENT_TIME_FIELD: &str = "_event_time";

/// 从 Record<String> 创建 ArrowMessage
pub fn record_to_arrow_message(record: &Record<String>) -> ArrowMessage {
    // 创建 Schema
    let schema = Arc::new(Schema::new(vec![
        Field::new(KEY_FIELD, DataType::Binary, true),
        Field::new(VALUE_FIELD, DataType::Utf8, false),
        Field::new(HEADERS_FIELD, DataType::Utf8, true),
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(TOPIC_FIELD, DataType::Utf8, false),
        Field::new(PARTITION_FIELD, DataType::UInt32, false),
        Field::new(OFFSET_FIELD, DataType::UInt64, false),
        Field::new(
            WATERMARK_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            EVENT_TIME_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));

    // 创建列数据
    let mut key_builder = BinaryBuilder::new();
    if let Some(key) = &record.key {
        key_builder.append_value(key);
    } else {
        key_builder.append_null();
    }
    let key_array = key_builder.finish();

    let mut value_builder = StringBuilder::new();
    value_builder.append_value(&record.value);
    let value_array = value_builder.finish();

    let mut headers_builder = StringBuilder::new();
    if !record.headers.is_empty() {
        let headers_json = serde_json::to_string(&record.headers).unwrap_or_default();
        headers_builder.append_value(&headers_json);
    } else {
        headers_builder.append_null();
    }
    let headers_array = headers_builder.finish();

    let mut timestamp_builder = TimestampNanosecondBuilder::new();
    timestamp_builder.append_value(to_nanos(record.timestamp) as i64);
    let timestamp_array = timestamp_builder.finish();

    let mut topic_builder = StringBuilder::new();
    topic_builder.append_value(&record.metadata.topic);
    let topic_array = topic_builder.finish();

    let partition_array = arrow_array::UInt32Array::from(vec![record.metadata.partition]);
    let offset_array = arrow_array::UInt64Array::from(vec![record.metadata.offset]);

    let mut watermark_builder = TimestampNanosecondBuilder::new();
    if let Some(watermark) = record.metadata.watermark {
        watermark_builder.append_value(to_nanos(watermark) as i64);
    } else {
        watermark_builder.append_null();
    }
    let watermark_array = watermark_builder.finish();

    let mut event_time_builder = TimestampNanosecondBuilder::new();
    if let Some(event_time) = record.metadata.event_time {
        event_time_builder.append_value(to_nanos(event_time) as i64);
    } else {
        event_time_builder.append_null();
    }
    let event_time_array = event_time_builder.finish();

    // 创建 RecordBatch
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(key_array),
        Arc::new(value_array),
        Arc::new(headers_array),
        Arc::new(timestamp_array),
        Arc::new(topic_array),
        Arc::new(partition_array),
        Arc::new(offset_array),
        Arc::new(watermark_array),
        Arc::new(event_time_array),
    ];

    let record_batch = RecordBatch::try_new(schema, columns).unwrap();
    ArrowMessage::Data(record_batch)
}

/// 从 ArrowMessage 创建 Record<String>
pub fn arrow_message_to_record(message: &ArrowMessage) -> Option<Record<String>> {
    match message {
        ArrowMessage::Data(batch) => {
            if batch.num_rows() == 0 {
                return None;
            }

            // 获取第一行数据
            let key = if let Some(key_array) = batch
                .column_by_name(KEY_FIELD)
                .and_then(|col| col.as_any().downcast_ref::<BinaryArray>())
            {
                if key_array.is_null(0) {
                    None
                } else {
                    Some(key_array.value(0).to_vec())
                }
            } else {
                None
            };

            let value = batch
                .column_by_name(VALUE_FIELD)
                .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                .map(|arr| arr.value(0).to_string())
                .unwrap_or_default();

            let headers = batch
                .column_by_name(HEADERS_FIELD)
                .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                .and_then(|arr| {
                    if arr.is_null(0) {
                        None
                    } else {
                        serde_json::from_str::<HashMap<String, Vec<u8>>>(arr.value(0)).ok()
                    }
                })
                .unwrap_or_default();

            let timestamp = batch
                .column_by_name(TIMESTAMP_FIELD)
                .and_then(|col| col.as_any().downcast_ref::<TimestampNanosecondArray>())
                .map(|arr| from_nanos(arr.value(0) as u128))
                .unwrap_or_else(SystemTime::now);

            let topic = batch
                .column_by_name(TOPIC_FIELD)
                .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                .map(|arr| arr.value(0).to_string())
                .unwrap_or_default();

            let partition = batch
                .column_by_name(PARTITION_FIELD)
                .and_then(|col| col.as_any().downcast_ref::<arrow_array::UInt32Array>())
                .map(|arr| arr.value(0))
                .unwrap_or_default();

            let offset = batch
                .column_by_name(OFFSET_FIELD)
                .and_then(|col| col.as_any().downcast_ref::<arrow_array::UInt64Array>())
                .map(|arr| arr.value(0))
                .unwrap_or_default();

            let watermark = batch
                .column_by_name(WATERMARK_FIELD)
                .and_then(|col| col.as_any().downcast_ref::<TimestampNanosecondArray>())
                .and_then(|arr| {
                    if arr.is_null(0) {
                        None
                    } else {
                        Some(from_nanos(arr.value(0) as u128))
                    }
                });

            let event_time = batch
                .column_by_name(EVENT_TIME_FIELD)
                .and_then(|col| col.as_any().downcast_ref::<TimestampNanosecondArray>())
                .and_then(|arr| {
                    if arr.is_null(0) {
                        None
                    } else {
                        Some(from_nanos(arr.value(0) as u128))
                    }
                });

            let metadata = RecordMetadata {
                topic,
                partition,
                offset,
                watermark,
                event_time,
            };

            Some(Record {
                key,
                value,
                headers,
                timestamp,
                metadata,
            })
        }
        ArrowMessage::Signal(_) => None,
    }
}
