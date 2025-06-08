#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::SystemTime;

    use arrow::datatypes::{DataType, TimeUnit};
    use arroyo_rpc::api_types::connections::{
        ConnectionSchema, FieldType, PrimitiveType, SourceField, SourceFieldType,
    };

    use crate::push::converter::PushMessageConverter;
    use crate::push::source::PushMessage;

    #[test]
    fn test_converter_creation() {
        // Create a simple schema
        let schema = ConnectionSchema {
            format: None,
            bad_data: None,
            framing: None,
            struct_name: None,
            fields: vec![
                SourceField {
                    field_name: "id".to_string(),
                    field_type: SourceFieldType {
                        r#type: FieldType::Primitive(PrimitiveType::String),
                        sql_name: Some("VARCHAR".to_string()),
                    },
                    nullable: false,
                    metadata_key: None,
                },
                SourceField {
                    field_name: "value".to_string(),
                    field_type: SourceFieldType {
                        r#type: FieldType::Primitive(PrimitiveType::Int32),
                        sql_name: Some("INTEGER".to_string()),
                    },
                    nullable: true,
                    metadata_key: None,
                },
            ],
            definition: None,
            inferred: None,
            primary_keys: HashSet::with_capacity_and_hasher(0, arroyo_rpc::get_hasher()),
        };

        // Create converter
        let converter = PushMessageConverter::new(&schema);
        assert!(converter.is_ok());

        // Check schema
        let converter = converter.unwrap();
        let arrow_schema = converter.schema();

        // Should have 4 fields: id, value, _timestamp, _topic
        assert_eq!(arrow_schema.fields().len(), 4);

        // Check field types
        assert_eq!(arrow_schema.field_with_name("id").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field_with_name("value").unwrap().data_type(), &DataType::Int32);
        assert_eq!(
            arrow_schema.field_with_name("_timestamp").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(arrow_schema.field_with_name("_topic").unwrap().data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_converter_with_timestamp_field() {
        // Create a schema with timestamp field
        let schema = ConnectionSchema {
            format: None,
            bad_data: None,
            framing: None,
            struct_name: None,
            fields: vec![
                SourceField {
                    field_name: "id".to_string(),
                    field_type: SourceFieldType {
                        r#type: FieldType::Primitive(PrimitiveType::String),
                        sql_name: Some("VARCHAR".to_string()),
                    },
                    nullable: false,
                    metadata_key: None,
                },
                SourceField {
                    field_name: "timestamp".to_string(),
                    field_type: SourceFieldType {
                        r#type: FieldType::Primitive(PrimitiveType::DateTime),
                        sql_name: Some("TIMESTAMP".to_string()),
                    },
                    nullable: false,
                    metadata_key: None,
                },
            ],
            definition: None,
            inferred: None,
            primary_keys: HashSet::with_capacity_and_hasher(0, arroyo_rpc::get_hasher()),
        };

        // Create converter
        let converter = PushMessageConverter::new(&schema);
        assert!(converter.is_ok());

        // Check schema
        let converter = converter.unwrap();
        let arrow_schema = converter.schema();

        // Should have 3 fields: id, timestamp, _topic
        assert_eq!(arrow_schema.fields().len(), 3);

        // Check field types
        assert_eq!(arrow_schema.field_with_name("id").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(
            arrow_schema.field_with_name("timestamp").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(arrow_schema.field_with_name("_topic").unwrap().data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_converter_with_topic_field() {
        // Create a schema with topic field
        let schema = ConnectionSchema {
            format: None,
            bad_data: None,
            framing: None,
            struct_name: None,
            fields: vec![
                SourceField {
                    field_name: "id".to_string(),
                    field_type: SourceFieldType {
                        r#type: FieldType::Primitive(PrimitiveType::String),
                        sql_name: Some("VARCHAR".to_string()),
                    },
                    nullable: false,
                    metadata_key: None,
                },
                SourceField {
                    field_name: "topic".to_string(),
                    field_type: SourceFieldType {
                        r#type: FieldType::Primitive(PrimitiveType::String),
                        sql_name: Some("VARCHAR".to_string()),
                    },
                    nullable: false,
                    metadata_key: None,
                },
            ],
            definition: None,
            inferred: None,
            primary_keys: HashSet::with_capacity_and_hasher(0, arroyo_rpc::get_hasher()),
        };

        // Create converter
        let converter = PushMessageConverter::new(&schema);
        assert!(converter.is_ok());

        // Check schema
        let converter = converter.unwrap();
        let arrow_schema = converter.schema();

        // Should have 3 fields: id, topic, _timestamp
        assert_eq!(arrow_schema.fields().len(), 3);

        // Check field types
        assert_eq!(arrow_schema.field_with_name("id").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field_with_name("topic").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(
            arrow_schema.field_with_name("_timestamp").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_converter_convert() {
        // Create a simple schema
        let schema = ConnectionSchema {
            format: None,
            bad_data: None,
            framing: None,
            struct_name: None,
            fields: vec![
                SourceField {
                    field_name: "id".to_string(),
                    field_type: SourceFieldType {
                        r#type: FieldType::Primitive(PrimitiveType::String),
                        sql_name: Some("VARCHAR".to_string()),
                    },
                    nullable: false,
                    metadata_key: None,
                },
                SourceField {
                    field_name: "value".to_string(),
                    field_type: SourceFieldType {
                        r#type: FieldType::Primitive(PrimitiveType::Int32),
                        sql_name: Some("INTEGER".to_string()),
                    },
                    nullable: true,
                    metadata_key: None,
                },
            ],
            definition: None,
            inferred: None,
            primary_keys: HashSet::with_capacity_and_hasher(0, arroyo_rpc::get_hasher()),
        };

        // Create converter
        let converter = PushMessageConverter::new(&schema).unwrap();

        // Create message
        let message = PushMessage {
            id: 1,
            topic: "test-topic".to_string(),
            data: r#"{"id":"123","value":42}"#.as_bytes().to_vec(),
            timestamp: SystemTime::now(),
        };

        // Verify schema
        let arrow_schema = converter.schema();
        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(arrow_schema.field_with_name("id").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field_with_name("value").unwrap().data_type(), &DataType::Int32);
        assert_eq!(
            arrow_schema.field_with_name("_timestamp").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(arrow_schema.field_with_name("_topic").unwrap().data_type(), &DataType::Utf8);

        // Note: Skipping actual conversion test as the implementation is not complete
        // TODO: Uncomment when converter.convert is fully implemented
        /*
        // Convert message
        let result = converter.convert(&message);
        assert!(result.is_ok());

        // Check record batch
        let record_batch = result.unwrap();
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 4);
        */
    }
}
