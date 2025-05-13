use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::window_functions::WindowFunction;

/// 方差窗口函数
pub struct VarianceWindowFunction {
    /// 输入模式
    input_schema: SchemaRef,
    /// 要聚合的列索引
    column_index: usize,
    /// 函数名称
    name: String,
    /// 是否是样本方差（除以 n-1）
    is_sample: bool,
}

impl VarianceWindowFunction {
    /// 创建新的方差窗口函数
    pub fn new(input_schema: SchemaRef, column_index: usize, is_sample: bool) -> Self {
        let name = if is_sample {
            format!("var_samp({})", input_schema.field(column_index).name())
        } else {
            format!("var_pop({})", input_schema.field(column_index).name())
        };
        Self {
            input_schema,
            column_index,
            name,
            is_sample,
        }
    }
}

impl WindowFunction for VarianceWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        if input.is_empty() {
            // 如果没有输入，返回空批次
            let schema = self.output_schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // 将所有批次的值合并到一个数组中
        let mut values = Vec::new();
        for batch in input {
            let array = batch
                .column(self.column_index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("无法转换为 Float64Array"))?;
            for i in 0..array.len() {
                if !array.is_null(i) {
                    values.push(array.value(i));
                }
            }
        }

        // 计算方差
        let variance = if values.is_empty() {
            0.0
        } else if values.len() == 1 {
            0.0
        } else {
            let n = values.len() as f64;
            let mean = values.iter().sum::<f64>() / n;
            let sum_squared_diff = values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>();

            if self.is_sample {
                // 样本方差，除以 n-1
                sum_squared_diff / (n - 1.0)
            } else {
                // 总体方差，除以 n
                sum_squared_diff / n
            }
        };

        // 创建结果数组
        let result = Arc::new(Float64Array::from(vec![variance])) as ArrayRef;

        // 创建输出批次
        let schema = self.output_schema();
        Ok(RecordBatch::try_new(schema, vec![result])?)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            &self.name,
            DataType::Float64,
            true,
        )]))
    }
}

/// 标准差窗口函数
pub struct StdDevWindowFunction {
    /// 内部方差函数
    variance_function: VarianceWindowFunction,
    /// 函数名称
    name: String,
}

impl StdDevWindowFunction {
    /// 创建新的标准差窗口函数
    pub fn new(input_schema: SchemaRef, column_index: usize, is_sample: bool) -> Self {
        let variance_function = VarianceWindowFunction::new(input_schema, column_index, is_sample);
        let name = if is_sample {
            format!("stddev_samp({})", variance_function.input_schema.field(column_index).name())
        } else {
            format!("stddev_pop({})", variance_function.input_schema.field(column_index).name())
        };
        Self {
            variance_function,
            name,
        }
    }
}

impl WindowFunction for StdDevWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        // 先计算方差
        let variance_result = self.variance_function.apply(input)?;

        // 取方差的平方根
        let variance_array = variance_result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow!("无法转换为 Float64Array"))?;

        let stddev = if variance_array.is_null(0) {
            0.0
        } else {
            variance_array.value(0).sqrt()
        };

        // 创建结果数组
        let result = Arc::new(Float64Array::from(vec![stddev])) as ArrayRef;

        // 创建输出批次
        let schema = self.output_schema();
        Ok(RecordBatch::try_new(schema, vec![result])?)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            &self.name,
            DataType::Float64,
            true,
        )]))
    }
}

/// 百分位数窗口函数
pub struct PercentileWindowFunction {
    /// 输入模式
    input_schema: SchemaRef,
    /// 要聚合的列索引
    column_index: usize,
    /// 百分位数（0.0 - 1.0）
    percentile: f64,
    /// 函数名称
    name: String,
}

impl PercentileWindowFunction {
    /// 创建新的百分位数窗口函数
    pub fn new(input_schema: SchemaRef, column_index: usize, percentile: f64) -> Self {
        let name = format!(
            "percentile_{}({})",
            (percentile * 100.0) as i32,
            input_schema.field(column_index).name()
        );
        Self {
            input_schema,
            column_index,
            percentile,
            name,
        }
    }

    /// 计算百分位数
    fn compute_percentile(&self, values: &mut [f64], percentile: f64) -> f64 {
        if values.is_empty() {
            return 0.0;
        }

        // 排序值
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = values.len();
        let rank = percentile * (n as f64 - 1.0);
        let rank_floor = rank.floor() as usize;
        let rank_ceil = rank.ceil() as usize;

        if rank_floor == rank_ceil {
            // 整数索引
            values[rank_floor]
        } else {
            // 插值
            let floor_val = values[rank_floor];
            let ceil_val = values[rank_ceil];
            let fraction = rank - rank_floor as f64;

            floor_val + fraction * (ceil_val - floor_val)
        }
    }
}

impl WindowFunction for PercentileWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        if input.is_empty() {
            // 如果没有输入，返回空批次
            let schema = self.output_schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // 将所有批次的值合并到一个数组中
        let mut values = Vec::new();
        for batch in input {
            let array = batch
                .column(self.column_index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("无法转换为 Float64Array"))?;
            for i in 0..array.len() {
                if !array.is_null(i) {
                    values.push(array.value(i));
                }
            }
        }

        // 计算百分位数
        let percentile_value = self.compute_percentile(&mut values, self.percentile);

        // 创建结果数组
        let result = Arc::new(Float64Array::from(vec![percentile_value])) as ArrayRef;

        // 创建输出批次
        let schema = self.output_schema();
        Ok(RecordBatch::try_new(schema, vec![result])?)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            &self.name,
            DataType::Float64,
            true,
        )]))
    }
}

/// 中位数窗口函数（50th 百分位数）
pub struct MedianWindowFunction {
    /// 内部百分位数函数
    percentile_function: PercentileWindowFunction,
}

impl MedianWindowFunction {
    /// 创建新的中位数窗口函数
    pub fn new(input_schema: SchemaRef, column_index: usize) -> Self {
        Self {
            percentile_function: PercentileWindowFunction::new(input_schema, column_index, 0.5),
        }
    }
}

impl WindowFunction for MedianWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        self.percentile_function.apply(input)
    }

    fn name(&self) -> &str {
        "median"
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "median",
            DataType::Float64,
            true,
        )]))
    }
}

/// 协方差窗口函数
pub struct CovarianceWindowFunction {
    /// 输入模式
    input_schema: SchemaRef,
    /// 第一个列索引
    column_index_x: usize,
    /// 第二个列索引
    column_index_y: usize,
    /// 是否是样本协方差（除以 n-1）
    is_sample: bool,
    /// 函数名称
    name: String,
}

impl CovarianceWindowFunction {
    /// 创建新的协方差窗口函数
    pub fn new(input_schema: SchemaRef, column_index_x: usize, column_index_y: usize, is_sample: bool) -> Self {
        let name = if is_sample {
            format!(
                "covar_samp({}, {})",
                input_schema.field(column_index_x).name(),
                input_schema.field(column_index_y).name()
            )
        } else {
            format!(
                "covar_pop({}, {})",
                input_schema.field(column_index_x).name(),
                input_schema.field(column_index_y).name()
            )
        };
        Self {
            input_schema,
            column_index_x,
            column_index_y,
            is_sample,
            name,
        }
    }
}

impl WindowFunction for CovarianceWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        if input.is_empty() {
            // 如果没有输入，返回空批次
            let schema = self.output_schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // 将所有批次的值合并到两个数组中
        let mut values_x = Vec::new();
        let mut values_y = Vec::new();

        for batch in input {
            let array_x = batch
                .column(self.column_index_x)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("无法转换为 Float64Array"))?;

            let array_y = batch
                .column(self.column_index_y)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("无法转换为 Float64Array"))?;

            for i in 0..batch.num_rows() {
                if !array_x.is_null(i) && !array_y.is_null(i) {
                    values_x.push(array_x.value(i));
                    values_y.push(array_y.value(i));
                }
            }
        }

        // 计算协方差
        let covariance = if values_x.is_empty() || values_x.len() < 2 {
            0.0
        } else {
            let n = values_x.len() as f64;
            let mean_x = values_x.iter().sum::<f64>() / n;
            let mean_y = values_y.iter().sum::<f64>() / n;

            let sum_product_diff = values_x.iter().zip(values_y.iter())
                .map(|(&x, &y)| (x - mean_x) * (y - mean_y))
                .sum::<f64>();

            if self.is_sample {
                // 样本协方差，除以 n-1
                sum_product_diff / (n - 1.0)
            } else {
                // 总体协方差，除以 n
                sum_product_diff / n
            }
        };

        // 创建结果数组
        let result = Arc::new(Float64Array::from(vec![covariance])) as ArrayRef;

        // 创建输出批次
        let schema = self.output_schema();
        Ok(RecordBatch::try_new(schema, vec![result])?)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            &self.name,
            DataType::Float64,
            true,
        )]))
    }
}

/// 相关系数窗口函数
pub struct CorrelationWindowFunction {
    /// 输入模式
    input_schema: SchemaRef,
    /// 第一个列索引
    column_index_x: usize,
    /// 第二个列索引
    column_index_y: usize,
    /// 函数名称
    name: String,
}

impl CorrelationWindowFunction {
    /// 创建新的相关系数窗口函数
    pub fn new(input_schema: SchemaRef, column_index_x: usize, column_index_y: usize) -> Self {
        let name = format!(
            "corr({}, {})",
            input_schema.field(column_index_x).name(),
            input_schema.field(column_index_y).name()
        );
        Self {
            input_schema,
            column_index_x,
            column_index_y,
            name,
        }
    }
}

impl WindowFunction for CorrelationWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        if input.is_empty() {
            // 如果没有输入，返回空批次
            let schema = self.output_schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // 将所有批次的值合并到两个数组中
        let mut values_x = Vec::new();
        let mut values_y = Vec::new();

        for batch in input {
            let array_x = batch
                .column(self.column_index_x)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("无法转换为 Float64Array"))?;

            let array_y = batch
                .column(self.column_index_y)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("无法转换为 Float64Array"))?;

            for i in 0..batch.num_rows() {
                if !array_x.is_null(i) && !array_y.is_null(i) {
                    values_x.push(array_x.value(i));
                    values_y.push(array_y.value(i));
                }
            }
        }

        // 计算相关系数
        let correlation = if values_x.is_empty() || values_x.len() < 2 {
            0.0
        } else {
            let n = values_x.len() as f64;
            let mean_x = values_x.iter().sum::<f64>() / n;
            let mean_y = values_y.iter().sum::<f64>() / n;

            let sum_product_diff = values_x.iter().zip(values_y.iter())
                .map(|(&x, &y)| (x - mean_x) * (y - mean_y))
                .sum::<f64>();

            let sum_squared_diff_x = values_x.iter()
                .map(|&x| (x - mean_x).powi(2))
                .sum::<f64>();

            let sum_squared_diff_y = values_y.iter()
                .map(|&y| (y - mean_y).powi(2))
                .sum::<f64>();

            let denominator = (sum_squared_diff_x * sum_squared_diff_y).sqrt();

            if denominator.abs() < f64::EPSILON {
                0.0 // 避免除以零
            } else {
                sum_product_diff / denominator
            }
        };

        // 创建结果数组
        let result = Arc::new(Float64Array::from(vec![correlation])) as ArrayRef;

        // 创建输出批次
        let schema = self.output_schema();
        Ok(RecordBatch::try_new(schema, vec![result])?)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            &self.name,
            DataType::Float64,
            true,
        )]))
    }
}