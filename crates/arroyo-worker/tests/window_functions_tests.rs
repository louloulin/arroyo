use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

use arroyo_worker::arrow::window_functions::{
    AvgWindowFunction, CountWindowFunction, MaxWindowFunction, MinWindowFunction, SumWindowFunction,
    WindowFunction,
};

/// 测试求和窗口函数
#[tokio::test]
async fn test_sum_window_function() {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "D"])),
            Arc::new(Int64Array::from(vec![5, 15, 25])),
        ],
    )
    .unwrap();

    // 创建求和窗口函数
    let sum_function = SumWindowFunction::new(schema.clone(), 1);

    // 应用窗口函数
    let result = sum_function.apply(&[batch1, batch2]).unwrap();

    // 验证结果
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column(0).data_type(), &DataType::Int64);

    let sum_array = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(sum_array.value(0), 105); // 10 + 20 + 30 + 5 + 15 + 25 = 105
}

/// 测试平均值窗口函数
#[tokio::test]
async fn test_avg_window_function() {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "D"])),
            Arc::new(Int64Array::from(vec![5, 15, 25])),
        ],
    )
    .unwrap();

    // 创建平均值窗口函数
    let avg_function = AvgWindowFunction::new(schema.clone(), 1);

    // 应用窗口函数
    let result = avg_function.apply(&[batch1, batch2]).unwrap();

    // 验证结果
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column(0).data_type(), &DataType::Float64);

    let avg_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(avg_array.value(0), 17.5); // (10 + 20 + 30 + 5 + 15 + 25) / 6 = 17.5
}

/// 测试计数窗口函数
#[tokio::test]
async fn test_count_window_function() {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "D"])),
            Arc::new(Int64Array::from(vec![5, 15, 25])),
        ],
    )
    .unwrap();

    // 创建计数窗口函数
    let count_function = CountWindowFunction::new(schema.clone(), 1);

    // 应用窗口函数
    let result = count_function.apply(&[batch1, batch2]).unwrap();

    // 验证结果
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column(0).data_type(), &DataType::UInt64);

    let count_array = result
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!(count_array.value(0), 6); // 总共 6 个值
}

/// 测试最大值窗口函数
#[tokio::test]
async fn test_max_window_function() {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "D"])),
            Arc::new(Int64Array::from(vec![5, 15, 25])),
        ],
    )
    .unwrap();

    // 创建最大值窗口函数
    let max_function = MaxWindowFunction::new(schema.clone(), 1);

    // 应用窗口函数
    let result = max_function.apply(&[batch1, batch2]).unwrap();

    // 验证结果
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column(0).data_type(), &DataType::Int64);

    let max_array = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(max_array.value(0), 30); // 最大值是 30
}

/// 测试最小值窗口函数
#[tokio::test]
async fn test_min_window_function() {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "D"])),
            Arc::new(Int64Array::from(vec![5, 15, 25])),
        ],
    )
    .unwrap();

    // 创建最小值窗口函数
    let min_function = MinWindowFunction::new(schema.clone(), 1);

    // 应用窗口函数
    let result = min_function.apply(&[batch1, batch2]).unwrap();

    // 验证结果
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column(0).data_type(), &DataType::Int64);

    let min_array = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(min_array.value(0), 5); // 最小值是 5
}
