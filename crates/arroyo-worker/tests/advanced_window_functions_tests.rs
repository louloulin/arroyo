use anyhow::Result;
use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use arroyo_worker::arrow::advanced_window_functions::{
    CorrelationWindowFunction, CovarianceWindowFunction, MedianWindowFunction, PercentileWindowFunction,
    StdDevWindowFunction, VarianceWindowFunction,
};
use arroyo_worker::arrow::window_functions::WindowFunction;

#[test]
fn test_variance_function() -> Result<()> {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let id_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3, 4, 5]));
    let value_array: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]));

    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, value_array])?;

    // 创建总体方差窗口函数
    let var_pop_function = VarianceWindowFunction::new(schema.clone(), 1, false);

    // 应用窗口函数
    let result = var_pop_function.apply(&[batch.clone()])?;

    // 验证结果
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column(0).data_type(), &DataType::Float64);

    let var_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的总体方差: ((10-30)^2 + (20-30)^2 + (30-30)^2 + (40-30)^2 + (50-30)^2) / 5 = 200
    let expected_var_pop = 200.0;
    assert!((var_array.value(0) - expected_var_pop).abs() < 0.001);

    // 创建样本方差窗口函数
    let var_samp_function = VarianceWindowFunction::new(schema.clone(), 1, true);

    // 应用窗口函数
    let result = var_samp_function.apply(&[batch.clone()])?;

    // 验证结果
    let var_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的样本方差: ((10-30)^2 + (20-30)^2 + (30-30)^2 + (40-30)^2 + (50-30)^2) / 4 = 250
    let expected_var_samp = 250.0;
    assert!((var_array.value(0) - expected_var_samp).abs() < 0.001);

    Ok(())
}

#[test]
fn test_stddev_function() -> Result<()> {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let id_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3, 4, 5]));
    let value_array: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]));

    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, value_array])?;

    // 创建总体标准差窗口函数
    let stddev_pop_function = StdDevWindowFunction::new(schema.clone(), 1, false);

    // 应用窗口函数
    let result = stddev_pop_function.apply(&[batch.clone()])?;

    // 验证结果
    let stddev_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的总体标准差: sqrt(200) ≈ 14.142
    let expected_stddev_pop = 200.0_f64.sqrt();
    assert!((stddev_array.value(0) - expected_stddev_pop).abs() < 0.001);

    // 创建样本标准差窗口函数
    let stddev_samp_function = StdDevWindowFunction::new(schema.clone(), 1, true);

    // 应用窗口函数
    let result = stddev_samp_function.apply(&[batch.clone()])?;

    // 验证结果
    let stddev_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的样本标准差: sqrt(250) ≈ 15.811
    let expected_stddev_samp = 250.0_f64.sqrt();
    assert!((stddev_array.value(0) - expected_stddev_samp).abs() < 0.001);

    Ok(())
}

#[test]
fn test_percentile_function() -> Result<()> {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let id_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3, 4, 5]));
    let value_array: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]));

    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, value_array])?;

    // 创建 25th 百分位数窗口函数
    let p25_function = PercentileWindowFunction::new(schema.clone(), 1, 0.25);

    // 应用窗口函数
    let result = p25_function.apply(&[batch.clone()])?;

    // 验证结果
    let p25_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的 25th 百分位数: 值为 20.0
    let expected_p25 = 20.0;
    assert!((p25_array.value(0) - expected_p25).abs() < 0.001);

    // 创建 75th 百分位数窗口函数
    let p75_function = PercentileWindowFunction::new(schema.clone(), 1, 0.75);

    // 应用窗口函数
    let result = p75_function.apply(&[batch.clone()])?;

    // 验证结果
    let p75_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的 75th 百分位数: 值为 40.0
    let expected_p75 = 40.0;
    assert!((p75_array.value(0) - expected_p75).abs() < 0.001);

    Ok(())
}

#[test]
fn test_median_function() -> Result<()> {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let id_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3, 4, 5]));
    let value_array: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]));

    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, value_array])?;

    // 创建中位数窗口函数
    let median_function = MedianWindowFunction::new(schema.clone(), 1);

    // 应用窗口函数
    let result = median_function.apply(&[batch.clone()])?;

    // 验证结果
    let median_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的中位数: 值为 30.0
    let expected_median = 30.0;
    assert!((median_array.value(0) - expected_median).abs() < 0.001);

    Ok(())
}

#[test]
fn test_covariance_function() -> Result<()> {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
    ]));

    let x_array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    let y_array: ArrayRef = Arc::new(Float64Array::from(vec![5.0, 7.0, 9.0, 11.0, 13.0]));

    let batch = RecordBatch::try_new(schema.clone(), vec![x_array, y_array])?;

    // 创建总体协方差窗口函数
    let covar_pop_function = CovarianceWindowFunction::new(schema.clone(), 0, 1, false);

    // 应用窗口函数
    let result = covar_pop_function.apply(&[batch.clone()])?;

    // 验证结果
    let covar_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的总体协方差: 2.0
    let expected_covar_pop = 2.0;
    assert!((covar_array.value(0) - expected_covar_pop).abs() < 0.001);

    // 创建样本协方差窗口函数
    let covar_samp_function = CovarianceWindowFunction::new(schema.clone(), 0, 1, true);

    // 应用窗口函数
    let result = covar_samp_function.apply(&[batch.clone()])?;

    // 验证结果
    let covar_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的样本协方差: 2.5
    let expected_covar_samp = 2.5;
    assert!((covar_array.value(0) - expected_covar_samp).abs() < 0.001);

    Ok(())
}

#[test]
fn test_correlation_function() -> Result<()> {
    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
    ]));

    let x_array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    let y_array: ArrayRef = Arc::new(Float64Array::from(vec![5.0, 7.0, 9.0, 11.0, 13.0]));

    let batch = RecordBatch::try_new(schema.clone(), vec![x_array, y_array])?;

    // 创建相关系数窗口函数
    let corr_function = CorrelationWindowFunction::new(schema.clone(), 0, 1);

    // 应用窗口函数
    let result = corr_function.apply(&[batch.clone()])?;

    // 验证结果
    let corr_array = result.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    
    // 计算预期的相关系数: 1.0（完全正相关）
    let expected_corr = 1.0;
    assert!((corr_array.value(0) - expected_corr).abs() < 0.001);

    Ok(())
}
