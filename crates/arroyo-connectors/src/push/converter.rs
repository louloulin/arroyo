use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatchOptions;
use arroyo_rpc::api_types::connections::{ConnectionSchema, SourceField};
use arroyo_types::UserError;
use tracing::{debug, error, info, warn};

use crate::push::source::PushMessage;

/// Converter for push messages to Arrow format
pub struct PushMessageConverter {
    /// Schema for the Arrow data
    schema: Arc<Schema>,
    /// Field names
    field_names: Vec<String>,
    /// Timestamp field index
    timestamp_field_index: Option<usize>,
    /// Topic field index
    topic_field_index: Option<usize>,
}

impl PushMessageConverter {
    /// Create a new converter
    pub fn new(connection_schema: &ConnectionSchema) -> Result<Self, UserError> {
        // Create Arrow schema from connection schema
        let mut fields = Vec::new();
        let mut field_names = Vec::new();
        let mut timestamp_field_index = None;
        let mut topic_field_index = None;

        // Add fields from connection schema
        for (i, field) in connection_schema.fields.iter().enumerate() {
            let arrow_field = Self::source_field_to_arrow_field(field)?;
            
            // Check if this is a timestamp field
            if arrow_field.data_type().is_timestamp() {
                timestamp_field_index = Some(i);
            }
            
            // Check if this is a topic field
            if field.field_name == "topic" {
                topic_field_index = Some(i);
            }
            
            field_names.push(field.field_name.clone());
            fields.push(arrow_field);
        }
        
        // If no timestamp field is found, add one
        if timestamp_field_index.is_none() {
            fields.push(Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ));
            field_names.push("_timestamp".to_string());
            timestamp_field_index = Some(fields.len() - 1);
        }
        
        // If no topic field is found, add one
        if topic_field_index.is_none() {
            fields.push(Field::new("_topic", DataType::Utf8, false));
            field_names.push("_topic".to_string());
            topic_field_index = Some(fields.len() - 1);
        }
        
        let schema = Arc::new(Schema::new(fields));
        
        Ok(Self {
            schema,
            field_names,
            timestamp_field_index,
            topic_field_index,
        })
    }
    
    /// Convert source field to Arrow field
    fn source_field_to_arrow_field(field: &SourceField) -> Result<Field, UserError> {
        // Get field data type
        let data_type = match &field.field_type.r#type {
            arroyo_rpc::api_types::connections::FieldType::Primitive(primitive_type) => {
                match primitive_type {
                    arroyo_rpc::api_types::connections::PrimitiveType::String => DataType::Utf8,
                    arroyo_rpc::api_types::connections::PrimitiveType::Int32 => DataType::Int32,
                    arroyo_rpc::api_types::connections::PrimitiveType::Int64 => DataType::Int64,
                    arroyo_rpc::api_types::connections::PrimitiveType::Float32 => DataType::Float32,
                    arroyo_rpc::api_types::connections::PrimitiveType::Float64 => DataType::Float64,
                    arroyo_rpc::api_types::connections::PrimitiveType::Boolean => DataType::Boolean,
                    arroyo_rpc::api_types::connections::PrimitiveType::Bytes => DataType::Binary,
                    arroyo_rpc::api_types::connections::PrimitiveType::Date => DataType::Date32,
                    arroyo_rpc::api_types::connections::PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
                    arroyo_rpc::api_types::connections::PrimitiveType::Timestamp => {
                        DataType::Timestamp(TimeUnit::Microsecond, None)
                    }
                    arroyo_rpc::api_types::connections::PrimitiveType::UInt32 => DataType::UInt32,
                    arroyo_rpc::api_types::connections::PrimitiveType::UInt64 => DataType::UInt64,
                    _ => {
                        return Err(UserError {
                            name: "Unsupported data type".to_string(),
                            details: format!("Unsupported primitive type: {:?}", primitive_type),
                            temporary: false,
                        });
                    }
                }
            }
            arroyo_rpc::api_types::connections::FieldType::List(item_type) => {
                match &**item_type {
                    arroyo_rpc::api_types::connections::FieldType::Primitive(primitive_type) => {
                        match primitive_type {
                            arroyo_rpc::api_types::connections::PrimitiveType::String => {
                                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                            }
                            arroyo_rpc::api_types::connections::PrimitiveType::Int32 => {
                                DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
                            }
                            arroyo_rpc::api_types::connections::PrimitiveType::Int64 => {
                                DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
                            }
                            arroyo_rpc::api_types::connections::PrimitiveType::Float32 => {
                                DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
                            }
                            arroyo_rpc::api_types::connections::PrimitiveType::Float64 => {
                                DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                            }
                            arroyo_rpc::api_types::connections::PrimitiveType::Boolean => {
                                DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)))
                            }
                            arroyo_rpc::api_types::connections::PrimitiveType::Bytes => {
                                DataType::List(Arc::new(Field::new("item", DataType::Binary, true)))
                            }
                            _ => {
                                return Err(UserError {
                                    name: "Unsupported data type".to_string(),
                                    details: format!("Unsupported list item type: {:?}", primitive_type),
                                    temporary: false,
                                });
                            }
                        }
                    }
                    _ => {
                        return Err(UserError {
                            name: "Unsupported data type".to_string(),
                            details: "Nested list types are not supported".to_string(),
                            temporary: false,
                        });
                    }
                }
            }
            _ => {
                return Err(UserError {
                    name: "Unsupported data type".to_string(),
                    details: format!("Unsupported field type: {:?}", field.field_type.r#type),
                    temporary: false,
                });
            }
        };
        
        Ok(Field::new(&field.field_name, data_type, field.nullable))
    }
    
    /// Convert push message to Arrow record batch
    pub fn convert(&self, message: &PushMessage) -> Result<RecordBatch, UserError> {
        // Parse message data based on format
        let parsed_data = self.parse_message_data(message)?;
        
        // Create arrays for each field
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.field_names.len());
        
        // Fill arrays with data
        for (i, field_name) in self.field_names.iter().enumerate() {
            if Some(i) == self.timestamp_field_index {
                // Add timestamp field
                let timestamp_array = TimestampMicrosecondArray::from(vec![
                    message.timestamp
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as i64
                ]);
                arrays.push(Arc::new(timestamp_array));
            } else if Some(i) == self.topic_field_index {
                // Add topic field
                let topic_array = StringArray::from(vec![message.topic.as_str()]);
                arrays.push(Arc::new(topic_array));
            } else {
                // Add data field
                if let Some(value) = parsed_data.get(field_name) {
                    arrays.push(value.clone());
                } else {
                    // Field not found in data, add null value
                    let field = self.schema.field(i);
                    let null_array = Self::create_null_array(field.data_type(), 1)?;
                    arrays.push(null_array);
                }
            }
        }
        
        // Create record batch
        let options = RecordBatchOptions::new().with_row_count(Some(1));
        let record_batch = RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)
            .map_err(|e| UserError {
                name: "Record batch creation error".to_string(),
                details: format!("Failed to create record batch: {}", e),
                temporary: false,
            })?;
        
        Ok(record_batch)
    }
    
    /// Parse message data based on format
    fn parse_message_data(&self, message: &PushMessage) -> Result<HashMap<String, ArrayRef>, UserError> {
        // TODO: Implement parsing based on format (JSON, Avro, Protobuf, etc.)
        // For now, just return an empty map
        Ok(HashMap::new())
    }
    
    /// Create a null array of the specified type
    fn create_null_array(data_type: &DataType, length: usize) -> Result<ArrayRef, UserError> {
        // TODO: Implement null array creation for all supported types
        // For now, just return a null string array
        Ok(Arc::new(StringArray::from(vec![None as Option<&str>; length])))
    }
    
    /// Get the schema
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
