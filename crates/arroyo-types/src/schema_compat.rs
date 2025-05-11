use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::RecordBatch;

/// Schema compatibility level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaCompatibility {
    /// No compatibility checks
    None,
    /// New schema must be readable with old schema (backward compatible)
    Backward,
    /// Old schema must be readable with new schema (forward compatible)
    Forward,
    /// Both backward and forward compatible
    Full,
}

/// Check if two schemas are compatible according to the specified compatibility level
pub fn check_schema_compatibility(
    old_schema: &Schema,
    new_schema: &Schema,
    compatibility: SchemaCompatibility,
) -> Result<()> {
    match compatibility {
        SchemaCompatibility::None => Ok(()),
        SchemaCompatibility::Backward => check_backward_compatibility(old_schema, new_schema),
        SchemaCompatibility::Forward => check_forward_compatibility(old_schema, new_schema),
        SchemaCompatibility::Full => {
            check_backward_compatibility(old_schema, new_schema)?;
            check_forward_compatibility(old_schema, new_schema)
        }
    }
}

/// Check if new schema is backward compatible with old schema
fn check_backward_compatibility(old_schema: &Schema, new_schema: &Schema) -> Result<()> {
    // Create a map of field names to fields for the old schema
    let old_fields: HashMap<_, _> = old_schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f))
        .collect();

    // For backward compatibility, all fields in the old schema must exist in the new schema
    // and be compatible
    for (name, old_field) in old_fields {
        match new_schema.field_with_name(&name) {
            Ok(new_field) => {
                check_field_compatibility(old_field, new_field)?;
            }
            Err(_) => {
                bail!(
                    "Field '{}' exists in old schema but not in new schema",
                    name
                );
            }
        }
    }

    Ok(())
}

/// Check if old schema is forward compatible with new schema
fn check_forward_compatibility(old_schema: &Schema, new_schema: &Schema) -> Result<()> {
    // Create a map of field names to fields for the new schema
    let new_fields: HashMap<_, _> = new_schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f))
        .collect();

    // For forward compatibility, all fields in the new schema must exist in the old schema
    // or be nullable in the new schema
    for (name, new_field) in new_fields {
        match old_schema.field_with_name(&name) {
            Ok(old_field) => {
                check_field_compatibility(old_field, new_field)?;
            }
            Err(_) => {
                if !new_field.is_nullable() {
                    bail!(
                        "Field '{}' exists in new schema but not in old schema and is not nullable",
                        name
                    );
                }
            }
        }
    }

    Ok(())
}

/// Check if two fields are compatible
fn check_field_compatibility(old_field: &Field, new_field: &Field) -> Result<()> {
    // For backward compatibility:
    // 1. If the old field is nullable, the new field must also be nullable
    if old_field.is_nullable() && !new_field.is_nullable() {
        bail!(
            "Field '{}' was nullable in old schema but not in new schema",
            old_field.name()
        );
    }

    // 2. Field names must match
    if old_field.name() != new_field.name() {
        bail!(
            "Field name mismatch: '{}' in old schema, '{}' in new schema",
            old_field.name(),
            new_field.name()
        );
    }

    // 3. Check data type compatibility
    check_datatype_compatibility(old_field.data_type(), new_field.data_type())
}

/// Check if two data types are compatible
fn check_datatype_compatibility(old_type: &DataType, new_type: &DataType) -> Result<()> {
    match (old_type, new_type) {
        // Same types are always compatible
        (a, b) if a == b => Ok(()),

        // Numeric type promotions (old type -> new type)
        (DataType::Int8, DataType::Int16)
        | (DataType::Int8, DataType::Int32)
        | (DataType::Int8, DataType::Int64)
        | (DataType::Int16, DataType::Int32)
        | (DataType::Int16, DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::UInt8, DataType::UInt16)
        | (DataType::UInt8, DataType::UInt32)
        | (DataType::UInt8, DataType::UInt64)
        | (DataType::UInt16, DataType::UInt32)
        | (DataType::UInt16, DataType::UInt64)
        | (DataType::UInt32, DataType::UInt64)
        | (DataType::Float32, DataType::Float64) => Ok(()),

        // Struct type compatibility
        (DataType::Struct(old_fields), DataType::Struct(new_fields)) => {
            // For backward compatibility, all fields in the old struct must exist in the new struct
            let new_field_map: HashMap<_, _> = new_fields
                .iter()
                .map(|f| (f.name().clone(), f.as_ref()))
                .collect();

            for old_field in old_fields.iter() {
                match new_field_map.get(old_field.name()) {
                    Some(new_field) => {
                        check_field_compatibility(old_field, new_field)?;
                    }
                    None => {
                        bail!(
                            "Field '{}' exists in old struct but not in new struct",
                            old_field.name()
                        );
                    }
                }
            }
            Ok(())
        }

        // List type compatibility
        (DataType::List(old_field), DataType::List(new_field)) => {
            check_field_compatibility(old_field.as_ref(), new_field.as_ref())
        }

        // Map type compatibility
        (
            DataType::Map(old_field, old_sorted),
            DataType::Map(new_field, new_sorted),
        ) => {
            if old_sorted != new_sorted {
                bail!("Map sorting changed from {:?} to {:?}", old_sorted, new_sorted);
            }
            check_field_compatibility(old_field.as_ref(), new_field.as_ref())
        }

        // Timestamp compatibility
        (
            DataType::Timestamp(old_unit, old_tz),
            DataType::Timestamp(new_unit, new_tz),
        ) => {
            if old_tz != new_tz {
                bail!(
                    "Timestamp timezone changed from {:?} to {:?}",
                    old_tz,
                    new_tz
                );
            }

            // Allow certain time unit conversions
            match (old_unit, new_unit) {
                (a, b) if a == b => Ok(()),
                (TimeUnit::Second, TimeUnit::Millisecond)
                | (TimeUnit::Second, TimeUnit::Microsecond)
                | (TimeUnit::Second, TimeUnit::Nanosecond)
                | (TimeUnit::Millisecond, TimeUnit::Microsecond)
                | (TimeUnit::Millisecond, TimeUnit::Nanosecond)
                | (TimeUnit::Microsecond, TimeUnit::Nanosecond) => Ok(()),
                _ => bail!(
                    "Incompatible timestamp unit change from {:?} to {:?}",
                    old_unit,
                    new_unit
                ),
            }
        }

        // Any other type change is not compatible for backward compatibility
        _ => bail!(
            "Incompatible data types: {:?} in old schema, {:?} in new schema",
            old_type,
            new_type
        ),
    }
}

/// Convert a RecordBatch to a Schema that is compatible with the target schema
pub fn convert_batch_to_compatible_schema(
    batch: &RecordBatch,
    target_schema: &Schema,
) -> Result<RecordBatch> {
    let source_schema = batch.schema();

    // Create a map of field names to column indices for the source batch
    let source_fields: HashMap<_, _> = source_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().clone(), i))
        .collect();

    // Create new columns based on the target schema
    let mut columns = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        if let Some(&source_idx) = source_fields.get(field.name()) {
            let source_field = source_schema.field(source_idx);
            let source_column = batch.column(source_idx);

            if source_field.data_type() == field.data_type() {
                // Types match, use column as is
                columns.push(source_column.clone());
            } else {
                // Types don't match, need to convert
                bail!(
                    "Type conversion not implemented for field '{}': {:?} to {:?}",
                    field.name(),
                    source_field.data_type(),
                    field.data_type()
                );
            }
        } else if field.is_nullable() {
            // Field doesn't exist in source but is nullable, add null column
            bail!(
                "Creating null columns not implemented for field '{}'",
                field.name()
            );
        } else {
            // Field doesn't exist and is not nullable
            bail!(
                "Field '{}' required by target schema doesn't exist in source batch",
                field.name()
            );
        }
    }

    // Create new RecordBatch with target schema
    RecordBatch::try_new(Arc::new(target_schema.clone()), columns)
        .map_err(|e| anyhow!("Failed to create compatible record batch: {}", e))
}
