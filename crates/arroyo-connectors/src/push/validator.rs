use serde_json::Value;
use tracing::warn;

/// Message validation error
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Message size exceeds maximum allowed size of {0} bytes")]
    MessageTooLarge(usize),
    
    #[error("Invalid topic name: {0}")]
    InvalidTopicName(String),
    
    #[error("Invalid JSON: {0}")]
    InvalidJson(String),
    
    #[error("JSON object has too many fields: {0} (max: {1})")]
    TooManyFields(usize, usize),
    
    #[error("Field name too long: {0} (max: {1})")]
    FieldNameTooLong(usize, usize),
    
    #[error("Field value too long for '{0}': {1} (max: {2})")]
    FieldValueTooLong(String, usize, usize),
}

/// Message validator
pub struct MessageValidator {
    max_message_size: usize,
    max_field_count: usize,
    max_field_name_length: usize,
    max_field_value_length: usize,
}

impl MessageValidator {
    /// Create a new message validator
    pub fn new(
        max_message_size: usize,
        max_field_count: usize,
        max_field_name_length: usize,
        max_field_value_length: usize,
    ) -> Self {
        Self {
            max_message_size,
            max_field_count,
            max_field_name_length,
            max_field_value_length,
        }
    }

    /// Create a new message validator with default settings
    pub fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024, // 1 MB
            max_field_count: 100,
            max_field_name_length: 256,
            max_field_value_length: 10 * 1024, // 10 KB
        }
    }

    /// Validate message size
    pub fn validate_size(&self, message: &[u8]) -> Result<(), ValidationError> {
        if message.len() > self.max_message_size {
            return Err(ValidationError::MessageTooLarge(self.max_message_size));
        }
        Ok(())
    }

    /// Validate topic name
    pub fn validate_topic_name(&self, topic: &str) -> Result<(), ValidationError> {
        let valid_name_regex = regex::Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap();
        if !valid_name_regex.is_match(topic) {
            return Err(ValidationError::InvalidTopicName(topic.to_string()));
        }
        Ok(())
    }

    /// Validate JSON message
    pub fn validate_json(&self, message: &[u8]) -> Result<Value, ValidationError> {
        // Parse JSON
        let value: Value = serde_json::from_slice(message)
            .map_err(|e| ValidationError::InvalidJson(e.to_string()))?;

        // Validate fields
        if let Value::Object(obj) = &value {
            // Check field count
            if obj.len() > self.max_field_count {
                return Err(ValidationError::TooManyFields(obj.len(), self.max_field_count));
            }

            // Check field names and values
            for (key, val) in obj {
                // Check field name length
                if key.len() > self.max_field_name_length {
                    return Err(ValidationError::FieldNameTooLong(key.len(), self.max_field_name_length));
                }

                // Check field value length for strings
                if let Value::String(s) = val {
                    if s.len() > self.max_field_value_length {
                        return Err(ValidationError::FieldValueTooLong(
                            key.clone(),
                            s.len(),
                            self.max_field_value_length,
                        ));
                    }
                }
            }
        }

        Ok(value)
    }

    /// Validate message
    pub fn validate(&self, topic: &str, message: &[u8]) -> Result<(), ValidationError> {
        // Validate topic name
        self.validate_topic_name(topic)?;
        
        // Validate message size
        self.validate_size(message)?;
        
        // Try to validate JSON, but don't fail if it's not JSON
        if let Err(e) = self.validate_json(message) {
            // Only warn about JSON validation errors, don't fail
            warn!("JSON validation failed: {}", e);
        }
        
        Ok(())
    }
}
