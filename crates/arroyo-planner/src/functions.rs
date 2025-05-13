use crate::ArroyoSchemaProvider;
use arrow::row::{RowConverter, SortField};
use arrow_array::builder::{FixedSizeBinaryBuilder, ListBuilder, StringBuilder};
use arrow_array::cast::{as_string_array, AsArray};
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, StringArray, UnionArray};
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::common::{Result, TableReference};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::{
    create_udf, ColumnarValue, LogicalPlan, Projection, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::{col, Expr};
use serde_json_path::JsonPath;
use std::any::Any;
use std::fmt::{Debug, Write};
use std::sync::{Arc, OnceLock};

const SERIALIZE_JSON_UNION: &str = "serialize_json_union";

/// Borrowed from DataFusion
///
/// Creates a singleton `ScalarUDF` of the `$UDF` function named `$GNAME` and a
/// function named `$NAME` which returns that function named $NAME.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
#[macro_export]
macro_rules! make_udf_function {
    ($UDF:ty, $GNAME:ident, $NAME:ident) => {
        /// Singleton instance of the function
        static $GNAME: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> =
            std::sync::OnceLock::new();

        /// Return a [`ScalarUDF`] for [`$UDF`]
        ///
        /// [`ScalarUDF`]: datafusion_expr::ScalarUDF
        pub fn $NAME() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
            $GNAME
                .get_or_init(|| {
                    std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                        <$UDF>::default(),
                    ))
                })
                .clone()
        }
    };
}

make_udf_function!(MultiHashFunction, MULTI_HASH, multi_hash);

pub fn register_all(registry: &mut dyn FunctionRegistry) {
    // 注册 JSON 相关函数
    registry
        .register_udf(Arc::new(create_udf(
            "get_first_json_object",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(get_first_json_object),
        )))
        .unwrap();

    registry
        .register_udf(Arc::new(create_udf(
            "extract_json",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            Volatility::Immutable,
            Arc::new(extract_json),
        )))
        .unwrap();

    registry
        .register_udf(Arc::new(create_udf(
            "extract_json_string",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(extract_json_string),
        )))
        .unwrap();

    registry
        .register_udf(Arc::new(create_udf(
            SERIALIZE_JSON_UNION,
            vec![DataType::Union(union_fields(), UnionMode::Sparse)],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(serialize_json_union),
        )))
        .unwrap();

    // 注册哈希函数
    registry.register_udf(multi_hash()).unwrap();

    // 注册高级窗口聚合函数

    // 方差函数
    registry
        .register_udf(Arc::new(create_udf(
            "var_pop",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(var_pop),
        )))
        .unwrap();

    registry
        .register_udf(Arc::new(create_udf(
            "var_samp",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(var_samp),
        )))
        .unwrap();

    // 标准差函数
    registry
        .register_udf(Arc::new(create_udf(
            "stddev_pop",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(stddev_pop),
        )))
        .unwrap();

    registry
        .register_udf(Arc::new(create_udf(
            "stddev_samp",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(stddev_samp),
        )))
        .unwrap();

    // 百分位数函数
    registry
        .register_udf(Arc::new(create_udf(
            "percentile_cont",
            vec![DataType::Float64, DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(percentile_cont),
        )))
        .unwrap();

    // 中位数函数
    registry
        .register_udf(Arc::new(create_udf(
            "median",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(median),
        )))
        .unwrap();

    // 协方差函数
    registry
        .register_udf(Arc::new(create_udf(
            "covar_pop",
            vec![DataType::Float64, DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(covar_pop),
        )))
        .unwrap();

    registry
        .register_udf(Arc::new(create_udf(
            "covar_samp",
            vec![DataType::Float64, DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(covar_samp),
        )))
        .unwrap();

    // 相关系数函数
    registry
        .register_udf(Arc::new(create_udf(
            "corr",
            vec![DataType::Float64, DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(correlation),
        )))
        .unwrap();
}

fn parse_path(name: &str, path: &ScalarValue) -> Result<Arc<JsonPath>> {
    let path = match path {
        ScalarValue::Utf8(Some(s)) => JsonPath::parse(s)
            .map_err(|e| DataFusionError::Execution(format!("Invalid json path '{s}': {:?}", e)))?,
        ScalarValue::Utf8(None) => {
            return Err(DataFusionError::Execution(format!(
                "The path argument to {name} cannot be null"
            )));
        }
        _ => {
            return Err(DataFusionError::Execution(format!(
                "The path argument to {name} must be of type TEXT"
            )));
        }
    };

    Ok(Arc::new(path))
}

// Hash function that can take any number of arguments and produces a fast (non-cryptographic)
// 128-bit hash from their string representations
#[derive(Debug)]
pub struct MultiHashFunction {
    signature: Signature,
}

impl Default for MultiHashFunction {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MultiHashFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "multi_hash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::FixedSizeBinary(size_of::<u128>() as i32))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();

        let all_scalar = args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));

        let length = args
            .iter()
            .map(|t| match t {
                ColumnarValue::Scalar(_) => 1,
                ColumnarValue::Array(a) => a.len(),
            })
            .max()
            .ok_or_else(|| {
                DataFusionError::Plan("multi_hash must have at least one argument".to_string())
            })?;

        let row_builder = RowConverter::new(
            args.iter()
                .map(|t| SortField::new(t.data_type().clone()))
                .collect(),
        )?;

        let arrays = args
            .iter()
            .map(|c| c.clone().into_array(length))
            .collect::<Result<Vec<_>>>()?;
        let rows = row_builder.convert_columns(&arrays)?;

        if all_scalar {
            hasher.update(rows.row(0).as_ref());
            let result = hasher.digest128().to_be_bytes().to_vec();
            hasher.reset();
            Ok(ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(
                size_of::<u128>() as i32,
                Some(result),
            )))
        } else {
            let mut builder =
                FixedSizeBinaryBuilder::with_capacity(length, size_of::<u128>() as i32);

            for row in rows.iter() {
                hasher.update(row.as_ref());
                builder.append_value(hasher.digest128().to_be_bytes())?;
                hasher.reset();
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }
}

fn json_function<T, ArrayT, F, ToS>(
    name: &str,
    f: F,
    to_scalar: ToS,
    args: &[ColumnarValue],
) -> Result<ColumnarValue>
where
    ArrayT: Array + FromIterator<Option<T>> + 'static,
    F: Fn(serde_json::Value, &JsonPath) -> Option<T>,
    ToS: Fn(Option<T>) -> ScalarValue,
{
    assert_eq!(args.len(), 2);
    Ok(match (&args[0], &args[1]) {
        (ColumnarValue::Array(values), ColumnarValue::Scalar(path)) => {
            let path = parse_path(name, path)?;
            let vs = as_string_array(values);
            ColumnarValue::Array(Arc::new(
                vs.iter()
                    .map(|s| s.and_then(|s| f(serde_json::from_str(s).ok()?, &path)))
                    .collect::<ArrayT>(),
            ) as ArrayRef)
        }
        (ColumnarValue::Scalar(value), ColumnarValue::Scalar(path)) => {
            let path = parse_path(name, path)?;
            let ScalarValue::Utf8(ref value) = value else {
                return Err(DataFusionError::Execution(format!(
                    "The value argument to {name} must be of type TEXT"
                )));
            };

            let result = value
                .as_ref()
                .and_then(|v| f(serde_json::from_str(v).ok()?, &path));
            ColumnarValue::Scalar(to_scalar(result))
        }
        _ => {
            return Err(DataFusionError::Execution(
                "The path argument to {name} must be a literal".to_string(),
            ))
        }
    })
}

pub fn extract_json(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 2);

    let inner = |s, path: &JsonPath| {
        Some(
            path.query(&serde_json::from_str(s).ok()?)
                .iter()
                .map(|v| Some(v.to_string()))
                .collect::<Vec<Option<String>>>(),
        )
    };

    Ok(match (&args[0], &args[1]) {
        (ColumnarValue::Array(values), ColumnarValue::Scalar(path)) => {
            let path = parse_path("extract_json", path)?;
            let values = as_string_array(values);

            let mut builder = ListBuilder::with_capacity(StringBuilder::new(), values.len());

            let queried = values.iter().map(|s| s.and_then(|s| inner(s, &path)));

            for v in queried {
                builder.append_option(v);
            }

            ColumnarValue::Array(Arc::new(builder.finish()))
        }
        (ColumnarValue::Scalar(value), ColumnarValue::Scalar(path)) => {
            let path = parse_path("extract_json", path)?;
            let ScalarValue::Utf8(ref v) = value else {
                return Err(DataFusionError::Execution(
                    "The value argument to extract_json must be of type TEXT".to_string(),
                ));
            };

            let mut builder = ListBuilder::with_capacity(StringBuilder::new(), 1);
            let result = v.as_ref().and_then(|s| inner(s, &path));
            builder.append_option(result);

            ColumnarValue::Scalar(ScalarValue::List(Arc::new(builder.finish())))
        }
        _ => {
            return Err(DataFusionError::Execution(
                "The path argument to extract_json must be a literal".to_string(),
            ))
        }
    })
}
pub fn get_first_json_object(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    json_function::<String, StringArray, _, _>(
        "get_first_json_object",
        |s, path| path.query(&s).first().map(|v| v.to_string()),
        |s| s.as_deref().into(),
        args,
    )
}

pub fn extract_json_string(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    json_function::<String, StringArray, _, _>(
        "extract_json_string",
        |s, path| {
            path.query(&s)
                .first()
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        },
        |s| s.as_deref().into(),
        args,
    )
}

// This code is vendored from
// https://github.com/datafusion-contrib/datafusion-functions-json/blob/main/src/common_union.rs
// as the `is_json_union` function is not public. It should be kept in sync with that code so
// that we are able to detect JSON unions and rewrite them to serialized JSON for sinks.
pub(crate) fn is_json_union(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(fields, UnionMode::Sparse) => fields == &union_fields(),
        _ => false,
    }
}

pub(crate) const TYPE_ID_NULL: i8 = 0;
const TYPE_ID_BOOL: i8 = 1;
const TYPE_ID_INT: i8 = 2;
const TYPE_ID_FLOAT: i8 = 3;
const TYPE_ID_STR: i8 = 4;
const TYPE_ID_ARRAY: i8 = 5;
const TYPE_ID_OBJECT: i8 = 6;

fn union_fields() -> UnionFields {
    static FIELDS: OnceLock<UnionFields> = OnceLock::new();
    FIELDS
        .get_or_init(|| {
            UnionFields::from_iter([
                (
                    TYPE_ID_NULL,
                    Arc::new(Field::new("null", DataType::Null, true)),
                ),
                (
                    TYPE_ID_BOOL,
                    Arc::new(Field::new("bool", DataType::Boolean, false)),
                ),
                (
                    TYPE_ID_INT,
                    Arc::new(Field::new("int", DataType::Int64, false)),
                ),
                (
                    TYPE_ID_FLOAT,
                    Arc::new(Field::new("float", DataType::Float64, false)),
                ),
                (
                    TYPE_ID_STR,
                    Arc::new(Field::new("str", DataType::Utf8, false)),
                ),
                (
                    TYPE_ID_ARRAY,
                    Arc::new(Field::new("array", DataType::Utf8, false)),
                ),
                (
                    TYPE_ID_OBJECT,
                    Arc::new(Field::new("object", DataType::Utf8, false)),
                ),
            ])
        })
        .clone()
}
// End vendored code

pub fn serialize_json_union(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 1);
    let array = match args.first().unwrap() {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(1)?,
    };

    let mut b = StringBuilder::with_capacity(array.len(), array.get_array_memory_size());

    write_union(&mut b, &array)?;

    Ok(ColumnarValue::Array(Arc::new(b.finish())))
}

fn write_union(b: &mut StringBuilder, array: &ArrayRef) -> Result<(), std::fmt::Error> {
    assert!(
        is_json_union(array.data_type()),
        "array item is not a valid JSON union"
    );
    let json_union = array.as_any().downcast_ref::<UnionArray>().unwrap();

    for i in 0..json_union.len() {
        if json_union.is_null(i) {
            b.append_null();
        } else {
            write_value(b, json_union.type_id(i), &json_union.value(i))?;
            b.append_value("");
        }
    }

    Ok(())
}

fn write_value(b: &mut StringBuilder, id: i8, a: &ArrayRef) -> Result<(), std::fmt::Error> {
    match id {
        TYPE_ID_NULL => write!(b, "null")?,
        TYPE_ID_BOOL => write!(b, "{}", a.as_boolean().value(0))?,
        TYPE_ID_INT => write!(b, "{}", a.as_primitive::<Int64Type>().value(0))?,
        TYPE_ID_FLOAT => write!(b, "{}", a.as_primitive::<Float64Type>().value(0))?,
        TYPE_ID_STR => {
            // assumes that this is already a valid (escaped) json string as the only way to
            // construct these values are by parsing (valid) JSON
            b.write_char('"')?;
            b.write_str(a.as_string::<i32>().value(0))?;
            b.write_char('"')?;
        }
        TYPE_ID_ARRAY => {
            // write_array(b, a.as_list::<i32>())?;
            b.write_str(a.as_string::<i32>().value(0))?;
        }
        TYPE_ID_OBJECT => {
            b.write_str(a.as_string::<i32>().value(0))?;
        }
        _ => unreachable!("invalid union type in JSON union: {}", id),
    }

    Ok(())
}

pub(crate) fn serialize_outgoing_json(
    registry: &ArroyoSchemaProvider,
    node: Arc<LogicalPlan>,
) -> LogicalPlan {
    let exprs = node
        .schema()
        .fields()
        .iter()
        .map(|f| {
            if is_json_union(f.data_type()) {
                Expr::Alias(Alias::new(
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        registry.udf(SERIALIZE_JSON_UNION).unwrap(),
                        vec![col(f.name())],
                    )),
                    Option::<TableReference>::None,
                    f.name(),
                ))
            } else {
                col(f.name())
            }
        })
        .collect();

    LogicalPlan::Projection(Projection::try_new(exprs, node).unwrap())
}

#[cfg(test)]
mod test {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::StringArray;
    use datafusion::common::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_extract_json() {
        let input = Arc::new(StringArray::from(vec![
            r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#,
            r#"{"a": 3, "b": 4}"#,
            r#"{"a": 5, "b": 6}"#,
        ]));

        let path = "$.c.d";

        let result = super::extract_json(&[
            super::ColumnarValue::Array(input),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let mut expected = ListBuilder::new(StringBuilder::new());
        expected.append_value(vec![Some("\"hello\"".to_string())]);
        expected.append_value(Vec::<Option<String>>::new());
        expected.append_value(Vec::<Option<String>>::new());
        if let super::ColumnarValue::Array(result) = result {
            assert_eq!(*result, expected.finish());
        } else {
            panic!("Expected array, got scalar");
        }

        let result = super::extract_json(&[
            super::ColumnarValue::Scalar(r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#.into()),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let mut expected = ListBuilder::with_capacity(StringBuilder::new(), 1);
        expected.append_value(vec![Some("\"hello\"".to_string())]);

        if let super::ColumnarValue::Scalar(ScalarValue::List(result)) = result {
            assert_eq!(*result, expected.finish());
        } else {
            panic!("Expected scalar list");
        }
    }

    #[test]
    fn test_get_first_json_object() {
        let input = Arc::new(StringArray::from(vec![
            r#"{"a": 1, "b": 2}"#,
            r#"{"a": 3}"#,
            r#"{"a": 5, "b": 6}"#,
        ]));

        let path = "$.b";

        let result = super::get_first_json_object(&[
            super::ColumnarValue::Array(input),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let expected = StringArray::from(vec![Some("2"), None, Some("6")]);

        if let super::ColumnarValue::Array(result) = result {
            assert_eq!(*result, expected);
        } else {
            panic!("Expected array, got scalar");
        }

        let result = super::get_first_json_object(&[
            super::ColumnarValue::Scalar(r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#.into()),
            super::ColumnarValue::Scalar("$.c.d".into()),
        ])
        .unwrap();

        let expected = ScalarValue::Utf8(Some("\"hello\"".to_string()));

        if let super::ColumnarValue::Scalar(result) = result {
            assert_eq!(result, expected);
        } else {
            panic!("Expected scalar");
        }
    }

    #[test]
    fn test_extract_json_string() {
        let input = Arc::new(StringArray::from(vec![
            r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#,
            r#"{"a": 3, "b": 4}"#,
            r#"{"a": 5, "b": 6}"#,
        ]));

        let path = "$.c.d";

        let result = super::extract_json_string(&[
            super::ColumnarValue::Array(input),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let expected = StringArray::from(vec![Some("hello"), None, None]);

        if let super::ColumnarValue::Array(result) = result {
            assert_eq!(*result, expected);
        } else {
            panic!("Expected array, got scalar");
        }

        let result = super::extract_json_string(&[
            super::ColumnarValue::Scalar(r#"{"a": 1, "b": 2, "c": { "d": "hello" }}"#.into()),
            super::ColumnarValue::Scalar(path.into()),
        ])
        .unwrap();

        let expected = ScalarValue::Utf8(Some("hello".to_string()));

        if let super::ColumnarValue::Scalar(result) = result {
            assert_eq!(result, expected);
        } else {
            panic!("Expected scalar");
        }
    }
}

// 高级窗口聚合函数实现

/// 总体方差函数
pub fn var_pop(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 1);

    match &args[0] {
        ColumnarValue::Array(array) => {
            let float_array = array.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("var_pop expects Float64 input".to_string()))?;

            let mut sum = 0.0;
            let mut sum_squared = 0.0;
            let mut count = 0;

            for i in 0..float_array.len() {
                if !float_array.is_null(i) {
                    let value = float_array.value(i);
                    sum += value;
                    sum_squared += value * value;
                    count += 1;
                }
            }

            let variance = if count > 0 {
                let mean = sum / count as f64;
                sum_squared / count as f64 - mean * mean
            } else {
                0.0
            };

            Ok(ColumnarValue::Array(Arc::new(arrow_array::Float64Array::from(vec![variance]))))
        }
        ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) => {
            // 单个值的方差为 0
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))))
        }
        _ => Err(DataFusionError::Execution("var_pop expects Float64 input".to_string())),
    }
}

/// 样本方差函数
pub fn var_samp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 1);

    match &args[0] {
        ColumnarValue::Array(array) => {
            let float_array = array.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("var_samp expects Float64 input".to_string()))?;

            let mut sum = 0.0;
            let mut sum_squared = 0.0;
            let mut count = 0;

            for i in 0..float_array.len() {
                if !float_array.is_null(i) {
                    let value = float_array.value(i);
                    sum += value;
                    sum_squared += value * value;
                    count += 1;
                }
            }

            let variance = if count > 1 {
                let mean = sum / count as f64;
                (sum_squared - count as f64 * mean * mean) / (count - 1) as f64
            } else {
                0.0
            };

            Ok(ColumnarValue::Array(Arc::new(arrow_array::Float64Array::from(vec![variance]))))
        }
        ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) => {
            // 单个值的样本方差为 null
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
        }
        _ => Err(DataFusionError::Execution("var_samp expects Float64 input".to_string())),
    }
}

/// 总体标准差函数
pub fn stddev_pop(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let variance_result = var_pop(args)?;

    match variance_result {
        ColumnarValue::Array(array) => {
            let float_array = array.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("stddev_pop expects Float64 input".to_string()))?;

            let mut result = arrow_array::Float64Array::builder(float_array.len());

            for i in 0..float_array.len() {
                if float_array.is_null(i) {
                    result.append_null()?;
                } else {
                    let variance = float_array.value(i);
                    result.append_value(variance.sqrt())?;
                }
            }

            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }
        ColumnarValue::Scalar(ScalarValue::Float64(Some(variance))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(variance.sqrt()))))
        }
        ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
        }
        _ => Err(DataFusionError::Execution("stddev_pop expects Float64 input".to_string())),
    }
}

/// 样本标准差函数
pub fn stddev_samp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let variance_result = var_samp(args)?;

    match variance_result {
        ColumnarValue::Array(array) => {
            let float_array = array.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("stddev_samp expects Float64 input".to_string()))?;

            let mut result = arrow_array::Float64Array::builder(float_array.len());

            for i in 0..float_array.len() {
                if float_array.is_null(i) {
                    result.append_null()?;
                } else {
                    let variance = float_array.value(i);
                    result.append_value(variance.sqrt())?;
                }
            }

            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }
        ColumnarValue::Scalar(ScalarValue::Float64(Some(variance))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(variance.sqrt()))))
        }
        ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
        }
        _ => Err(DataFusionError::Execution("stddev_samp expects Float64 input".to_string())),
    }
}

/// 百分位数函数
pub fn percentile_cont(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 2);

    let (values, percentile) = match (&args[0], &args[1]) {
        (ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Float64(Some(p)))) => {
            let float_array = array.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("percentile_cont expects Float64 input".to_string()))?;

            // 收集非空值
            let mut values: Vec<f64> = Vec::with_capacity(float_array.len());
            for i in 0..float_array.len() {
                if !float_array.is_null(i) {
                    values.push(float_array.value(i));
                }
            }

            (values, *p)
        }
        (ColumnarValue::Scalar(ScalarValue::Float64(Some(value))), ColumnarValue::Scalar(ScalarValue::Float64(Some(p)))) => {
            (vec![*value], *p)
        }
        _ => return Err(DataFusionError::Execution("percentile_cont expects Float64 input and Float64 percentile".to_string())),
    };

    // 验证百分位数在 [0, 1] 范围内
    if percentile < 0.0 || percentile > 1.0 {
        return Err(DataFusionError::Execution(format!("percentile must be between 0 and 1, got {}", percentile)));
    }

    // 计算百分位数
    let result = if values.is_empty() {
        None
    } else {
        // 排序值
        let mut sorted_values = values.clone();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = sorted_values.len();
        let rank = percentile * (n as f64 - 1.0);
        let rank_floor = rank.floor() as usize;
        let rank_ceil = rank.ceil() as usize;

        if rank_floor == rank_ceil {
            // 整数索引
            Some(sorted_values[rank_floor])
        } else {
            // 插值
            let floor_val = sorted_values[rank_floor];
            let ceil_val = sorted_values[rank_ceil];
            let fraction = rank - rank_floor as f64;

            Some(floor_val + fraction * (ceil_val - floor_val))
        }
    };

    Ok(ColumnarValue::Scalar(ScalarValue::Float64(result)))
}

/// 中位数函数
pub fn median(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // 中位数是 50th 百分位数
    let mut median_args = args.to_vec();
    median_args.push(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5))));

    percentile_cont(&median_args)
}

/// 总体协方差函数
pub fn covar_pop(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 2);

    match (&args[0], &args[1]) {
        (ColumnarValue::Array(array_x), ColumnarValue::Array(array_y)) => {
            let float_array_x = array_x.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("covar_pop expects Float64 input".to_string()))?;

            let float_array_y = array_y.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("covar_pop expects Float64 input".to_string()))?;

            if float_array_x.len() != float_array_y.len() {
                return Err(DataFusionError::Execution("covar_pop expects arrays of same length".to_string()));
            }

            let mut sum_x = 0.0;
            let mut sum_y = 0.0;
            let mut sum_xy = 0.0;
            let mut count = 0;

            for i in 0..float_array_x.len() {
                if !float_array_x.is_null(i) && !float_array_y.is_null(i) {
                    let x = float_array_x.value(i);
                    let y = float_array_y.value(i);
                    sum_x += x;
                    sum_y += y;
                    sum_xy += x * y;
                    count += 1;
                }
            }

            let covariance = if count > 0 {
                let mean_x = sum_x / count as f64;
                let mean_y = sum_y / count as f64;
                sum_xy / count as f64 - mean_x * mean_y
            } else {
                0.0
            };

            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(covariance))))
        }
        _ => Err(DataFusionError::Execution("covar_pop expects Float64 arrays".to_string())),
    }
}

/// 样本协方差函数
pub fn covar_samp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 2);

    match (&args[0], &args[1]) {
        (ColumnarValue::Array(array_x), ColumnarValue::Array(array_y)) => {
            let float_array_x = array_x.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("covar_samp expects Float64 input".to_string()))?;

            let float_array_y = array_y.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("covar_samp expects Float64 input".to_string()))?;

            if float_array_x.len() != float_array_y.len() {
                return Err(DataFusionError::Execution("covar_samp expects arrays of same length".to_string()));
            }

            let mut sum_x = 0.0;
            let mut sum_y = 0.0;
            let mut sum_xy = 0.0;
            let mut count = 0;

            for i in 0..float_array_x.len() {
                if !float_array_x.is_null(i) && !float_array_y.is_null(i) {
                    let x = float_array_x.value(i);
                    let y = float_array_y.value(i);
                    sum_x += x;
                    sum_y += y;
                    sum_xy += x * y;
                    count += 1;
                }
            }

            let covariance = if count > 1 {
                let mean_x = sum_x / count as f64;
                let mean_y = sum_y / count as f64;
                (sum_xy - count as f64 * mean_x * mean_y) / (count - 1) as f64
            } else {
                0.0
            };

            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(covariance))))
        }
        _ => Err(DataFusionError::Execution("covar_samp expects Float64 arrays".to_string())),
    }
}

/// 相关系数函数
pub fn correlation(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    assert_eq!(args.len(), 2);

    match (&args[0], &args[1]) {
        (ColumnarValue::Array(array_x), ColumnarValue::Array(array_y)) => {
            let float_array_x = array_x.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("corr expects Float64 input".to_string()))?;

            let float_array_y = array_y.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("corr expects Float64 input".to_string()))?;

            if float_array_x.len() != float_array_y.len() {
                return Err(DataFusionError::Execution("corr expects arrays of same length".to_string()));
            }

            let mut sum_x = 0.0;
            let mut sum_y = 0.0;
            let mut sum_xy = 0.0;
            let mut sum_x2 = 0.0;
            let mut sum_y2 = 0.0;
            let mut count = 0;

            for i in 0..float_array_x.len() {
                if !float_array_x.is_null(i) && !float_array_y.is_null(i) {
                    let x = float_array_x.value(i);
                    let y = float_array_y.value(i);
                    sum_x += x;
                    sum_y += y;
                    sum_xy += x * y;
                    sum_x2 += x * x;
                    sum_y2 += y * y;
                    count += 1;
                }
            }

            let correlation = if count > 1 {
                let mean_x = sum_x / count as f64;
                let mean_y = sum_y / count as f64;

                let cov = sum_xy / count as f64 - mean_x * mean_y;
                let var_x = sum_x2 / count as f64 - mean_x * mean_x;
                let var_y = sum_y2 / count as f64 - mean_y * mean_y;

                if var_x.abs() < f64::EPSILON || var_y.abs() < f64::EPSILON {
                    0.0 // 避免除以零
                } else {
                    cov / (var_x.sqrt() * var_y.sqrt())
                }
            } else {
                0.0
            };

            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(correlation))))
        }
        _ => Err(DataFusionError::Execution("corr expects Float64 arrays".to_string())),
    }
}