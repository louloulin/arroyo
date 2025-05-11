#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use crate::schema_compat::{check_schema_compatibility, SchemaCompatibility};

    #[test]
    fn test_identical_schemas() {
        let schema1 = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
        ]);

        let schema2 = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
        ]);

        // Identical schemas should be compatible in all modes
        assert!(check_schema_compatibility(&schema1, &schema2, SchemaCompatibility::None).is_ok());
        assert!(check_schema_compatibility(&schema1, &schema2, SchemaCompatibility::Backward).is_ok());
        assert!(check_schema_compatibility(&schema1, &schema2, SchemaCompatibility::Forward).is_ok());
        assert!(check_schema_compatibility(&schema1, &schema2, SchemaCompatibility::Full).is_ok());
    }

    #[test]
    fn test_backward_compatibility() {
        // Original schema
        let old_schema = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
        ]);

        // New schema adds a nullable field (backward compatible)
        let new_schema_compatible = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
            Field::new("field3", DataType::Float64, true),
        ]);

        // New schema removes a field (not backward compatible)
        let new_schema_incompatible1 = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
        ]);

        // New schema changes a field type (not backward compatible)
        let new_schema_incompatible2 = Schema::new(vec![
            Field::new("field1", DataType::Utf8, false), // Int32 -> Utf8 (incompatible change)
            Field::new("field2", DataType::Utf8, true),
        ]);

        // New schema makes a nullable field non-nullable (not backward compatible)
        let new_schema_incompatible3 = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, false),
        ]);

        // Test backward compatibility
        assert!(check_schema_compatibility(&old_schema, &new_schema_compatible, SchemaCompatibility::Backward).is_ok());
        assert!(check_schema_compatibility(&old_schema, &new_schema_incompatible1, SchemaCompatibility::Backward).is_err());
        assert!(check_schema_compatibility(&old_schema, &new_schema_incompatible2, SchemaCompatibility::Backward).is_err());
        assert!(check_schema_compatibility(&old_schema, &new_schema_incompatible3, SchemaCompatibility::Backward).is_err());
    }

    #[test]
    fn test_forward_compatibility() {
        // Original schema
        let old_schema = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
        ]);

        // New schema removes a field (forward compatible)
        let new_schema_compatible1 = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
        ]);

        // New schema adds a nullable field (forward compatible)
        let new_schema_compatible2 = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
            Field::new("field3", DataType::Float64, true),
        ]);

        // New schema adds a non-nullable field (not forward compatible)
        let new_schema_incompatible = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
            Field::new("field3", DataType::Float64, false),
        ]);

        // Test forward compatibility
        assert!(check_schema_compatibility(&old_schema, &new_schema_compatible1, SchemaCompatibility::Forward).is_ok());
        assert!(check_schema_compatibility(&old_schema, &new_schema_compatible2, SchemaCompatibility::Forward).is_ok());
        assert!(check_schema_compatibility(&old_schema, &new_schema_incompatible, SchemaCompatibility::Forward).is_err());
    }

    #[test]
    fn test_full_compatibility() {
        // Original schema
        let old_schema = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
        ]);

        // New schema adds a nullable field (full compatible)
        let new_schema_compatible = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
            Field::new("field3", DataType::Float64, true),
        ]);

        // New schema removes a field (not full compatible)
        let new_schema_incompatible1 = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
        ]);

        // New schema adds a non-nullable field (not full compatible)
        let new_schema_incompatible2 = Schema::new(vec![
            Field::new("field1", DataType::Int32, false),
            Field::new("field2", DataType::Utf8, true),
            Field::new("field3", DataType::Float64, false),
        ]);

        // Test full compatibility
        assert!(check_schema_compatibility(&old_schema, &new_schema_compatible, SchemaCompatibility::Full).is_ok());
        assert!(check_schema_compatibility(&old_schema, &new_schema_incompatible1, SchemaCompatibility::Full).is_err());
        assert!(check_schema_compatibility(&old_schema, &new_schema_incompatible2, SchemaCompatibility::Full).is_err());
    }

    #[test]
    fn test_numeric_type_promotion() {
        // Test numeric type promotions
        let old_schema = Schema::new(vec![
            Field::new("int8_field", DataType::Int8, false),
            Field::new("int16_field", DataType::Int16, false),
            Field::new("int32_field", DataType::Int32, false),
            Field::new("float32_field", DataType::Float32, false),
        ]);

        let new_schema = Schema::new(vec![
            Field::new("int8_field", DataType::Int16, false),  // Int8 -> Int16 (promotion)
            Field::new("int16_field", DataType::Int32, false), // Int16 -> Int32 (promotion)
            Field::new("int32_field", DataType::Int64, false), // Int32 -> Int64 (promotion)
            Field::new("float32_field", DataType::Float64, false), // Float32 -> Float64 (promotion)
        ]);

        assert!(check_schema_compatibility(&old_schema, &new_schema, SchemaCompatibility::Backward).is_ok());
    }

    #[test]
    fn test_struct_compatibility() {
        // Test struct field compatibility
        let old_schema = Schema::new(vec![
            Field::new(
                "struct_field",
                DataType::Struct(vec![
                    Arc::new(Field::new("nested1", DataType::Int32, false)),
                    Arc::new(Field::new("nested2", DataType::Utf8, true)),
                ].into()),
                false,
            ),
        ]);

        // Compatible: adds a nullable field to struct
        let new_schema_compatible = Schema::new(vec![
            Field::new(
                "struct_field",
                DataType::Struct(vec![
                    Arc::new(Field::new("nested1", DataType::Int32, false)),
                    Arc::new(Field::new("nested2", DataType::Utf8, true)),
                    Arc::new(Field::new("nested3", DataType::Float64, true)),
                ].into()),
                false,
            ),
        ]);

        // Incompatible: removes a field from struct
        let new_schema_incompatible = Schema::new(vec![
            Field::new(
                "struct_field",
                DataType::Struct(vec![
                    Arc::new(Field::new("nested1", DataType::Int32, false)),
                ].into()),
                false,
            ),
        ]);

        assert!(check_schema_compatibility(&old_schema, &new_schema_compatible, SchemaCompatibility::Backward).is_ok());
        assert!(check_schema_compatibility(&old_schema, &new_schema_incompatible, SchemaCompatibility::Backward).is_err());
    }

    #[test]
    fn test_timestamp_compatibility() {
        // Test timestamp compatibility
        let old_schema = Schema::new(vec![
            Field::new("ts_field", DataType::Timestamp(TimeUnit::Second, None), false),
        ]);

        // Compatible: Second -> Millisecond (higher precision)
        let new_schema_compatible1 = Schema::new(vec![
            Field::new("ts_field", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        ]);

        // Compatible: Second -> Microsecond (higher precision)
        let new_schema_compatible2 = Schema::new(vec![
            Field::new("ts_field", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]);

        // Incompatible: timezone change
        let new_schema_incompatible = Schema::new(vec![
            Field::new("ts_field", DataType::Timestamp(TimeUnit::Second, Some("UTC".into())), false),
        ]);

        assert!(check_schema_compatibility(&old_schema, &new_schema_compatible1, SchemaCompatibility::Backward).is_ok());
        assert!(check_schema_compatibility(&old_schema, &new_schema_compatible2, SchemaCompatibility::Backward).is_ok());
        assert!(check_schema_compatibility(&old_schema, &new_schema_incompatible, SchemaCompatibility::Backward).is_err());
    }
}
