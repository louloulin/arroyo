use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, UInt64Array};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

/// 窗口函数特性，定义窗口聚合函数的接口
pub trait WindowFunction: Send + Sync {
    /// 应用窗口函数到输入批次，返回聚合结果
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch>;

    /// 获取函数名称
    fn name(&self) -> &str;

    /// 获取输出模式
    fn output_schema(&self) -> SchemaRef;
}

/// 求和窗口函数
pub struct SumWindowFunction {
    /// 输入模式
    input_schema: SchemaRef,
    /// 要聚合的列索引
    column_index: usize,
    /// 函数名称
    name: String,
}

impl SumWindowFunction {
    /// 创建新的求和窗口函数
    pub fn new(input_schema: SchemaRef, column_index: usize) -> Self {
        let name = format!("sum({})", input_schema.field(column_index).name());
        Self {
            input_schema,
            column_index,
            name,
        }
    }
}

impl WindowFunction for SumWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        if input.is_empty() {
            // 如果没有输入，返回空批次
            let schema = self.output_schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // 合并所有批次
        let mut all_rows = 0;
        for batch in input {
            all_rows += batch.num_rows();
        }

        // 根据数据类型执行求和操作
        let result = match input[0].column(self.column_index).data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 Int64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算总和
                let sum: i64 = values.iter().sum();

                // 创建结果数组
                Arc::new(Int64Array::from(vec![sum])) as ArrayRef
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<UInt64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 UInt64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算总和
                let sum: u64 = values.iter().sum();

                // 创建结果数组
                Arc::new(UInt64Array::from(vec![sum])) as ArrayRef
            }
            DataType::Float32 | DataType::Float64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<Float64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 Float64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算总和
                let sum: f64 = values.iter().sum();

                // 创建结果数组
                Arc::new(Float64Array::from(vec![sum])) as ArrayRef
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "不支持的数据类型: {}",
                    input[0].column(self.column_index).data_type()
                ));
            }
        };

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
            match self.input_schema.field(self.column_index).data_type() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    DataType::Int64
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    DataType::UInt64
                }
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                dt => dt.clone(),
            },
            true,
        )]))
    }
}

/// 平均值窗口函数
pub struct AvgWindowFunction {
    /// 输入模式
    input_schema: SchemaRef,
    /// 要聚合的列索引
    column_index: usize,
    /// 函数名称
    name: String,
}

impl AvgWindowFunction {
    /// 创建新的平均值窗口函数
    pub fn new(input_schema: SchemaRef, column_index: usize) -> Self {
        let name = format!("avg({})", input_schema.field(column_index).name());
        Self {
            input_schema,
            column_index,
            name,
        }
    }
}

impl WindowFunction for AvgWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        if input.is_empty() {
            // 如果没有输入，返回空批次
            let schema = self.output_schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // 合并所有批次
        let mut all_rows = 0;
        for batch in input {
            all_rows += batch.num_rows();
        }

        // 根据数据类型执行平均值操作
        let result = match input[0].column(self.column_index).data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 Int64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i) as f64);
                        }
                    }
                }

                // 计算平均值
                let avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                };

                // 创建结果数组
                Arc::new(Float64Array::from(vec![avg])) as ArrayRef
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<UInt64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 UInt64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i) as f64);
                        }
                    }
                }

                // 计算平均值
                let avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                };

                // 创建结果数组
                Arc::new(Float64Array::from(vec![avg])) as ArrayRef
            }
            DataType::Float32 | DataType::Float64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<Float64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 Float64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算平均值
                let avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                };

                // 创建结果数组
                Arc::new(Float64Array::from(vec![avg])) as ArrayRef
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "不支持的数据类型: {}",
                    input[0].column(self.column_index).data_type()
                ));
            }
        };

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

/// 计数窗口函数
pub struct CountWindowFunction {
    /// 输入模式
    input_schema: SchemaRef,
    /// 要聚合的列索引
    column_index: usize,
    /// 函数名称
    name: String,
}

impl CountWindowFunction {
    /// 创建新的计数窗口函数
    pub fn new(input_schema: SchemaRef, column_index: usize) -> Self {
        let name = format!("count({})", input_schema.field(column_index).name());
        Self {
            input_schema,
            column_index,
            name,
        }
    }
}

impl WindowFunction for CountWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        if input.is_empty() {
            // 如果没有输入，返回空批次
            let schema = self.output_schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // 计算所有非空值的数量
        let mut count: u64 = 0;
        for batch in input {
            let array = batch.column(self.column_index);
            for i in 0..array.len() {
                if !array.is_null(i) {
                    count += 1;
                }
            }
        }

        // 创建结果数组
        let result = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;

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
            DataType::UInt64,
            false,
        )]))
    }
}

/// 最大值窗口函数
pub struct MaxWindowFunction {
    /// 输入模式
    input_schema: SchemaRef,
    /// 要聚合的列索引
    column_index: usize,
    /// 函数名称
    name: String,
}

impl MaxWindowFunction {
    /// 创建新的最大值窗口函数
    pub fn new(input_schema: SchemaRef, column_index: usize) -> Self {
        let name = format!("max({})", input_schema.field(column_index).name());
        Self {
            input_schema,
            column_index,
            name,
        }
    }
}

impl WindowFunction for MaxWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        if input.is_empty() {
            // 如果没有输入，返回空批次
            let schema = self.output_schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // 合并所有批次
        let mut all_rows = 0;
        for batch in input {
            all_rows += batch.num_rows();
        }

        // 根据数据类型执行最大值操作
        let result = match input[0].column(self.column_index).data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 Int64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算最大值
                let max = values.iter().max().copied().unwrap_or(0);

                // 创建结果数组
                Arc::new(Int64Array::from(vec![max])) as ArrayRef
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<UInt64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 UInt64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算最大值
                let max = values.iter().max().copied().unwrap_or(0);

                // 创建结果数组
                Arc::new(UInt64Array::from(vec![max])) as ArrayRef
            }
            DataType::Float32 | DataType::Float64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<Float64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 Float64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算最大值
                let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                // 创建结果数组
                Arc::new(Float64Array::from(vec![max])) as ArrayRef
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "不支持的数据类型: {}",
                    input[0].column(self.column_index).data_type()
                ));
            }
        };

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
            self.input_schema.field(self.column_index).data_type().clone(),
            true,
        )]))
    }
}

/// 最小值窗口函数
pub struct MinWindowFunction {
    /// 输入模式
    input_schema: SchemaRef,
    /// 要聚合的列索引
    column_index: usize,
    /// 函数名称
    name: String,
}

impl MinWindowFunction {
    /// 创建新的最小值窗口函数
    pub fn new(input_schema: SchemaRef, column_index: usize) -> Self {
        let name = format!("min({})", input_schema.field(column_index).name());
        Self {
            input_schema,
            column_index,
            name,
        }
    }
}

impl WindowFunction for MinWindowFunction {
    fn apply(&self, input: &[RecordBatch]) -> Result<RecordBatch> {
        if input.is_empty() {
            // 如果没有输入，返回空批次
            let schema = self.output_schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // 合并所有批次
        let mut all_rows = 0;
        for batch in input {
            all_rows += batch.num_rows();
        }

        // 根据数据类型执行最小值操作
        let result = match input[0].column(self.column_index).data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<Int64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 Int64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算最小值
                let min = values.iter().min().copied().unwrap_or(0);

                // 创建结果数组
                Arc::new(Int64Array::from(vec![min])) as ArrayRef
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<UInt64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 UInt64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算最小值
                let min = values.iter().min().copied().unwrap_or(0);

                // 创建结果数组
                Arc::new(UInt64Array::from(vec![min])) as ArrayRef
            }
            DataType::Float32 | DataType::Float64 => {
                // 将所有批次的值合并到一个数组中
                let mut values = Vec::with_capacity(all_rows);
                for batch in input {
                    let array = batch.column(self.column_index).as_any().downcast_ref::<Float64Array>()
                        .ok_or_else(|| anyhow::anyhow!("无法转换为 Float64Array"))?;
                    for i in 0..array.len() {
                        if !array.is_null(i) {
                            values.push(array.value(i));
                        }
                    }
                }

                // 计算最小值
                let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));

                // 创建结果数组
                Arc::new(Float64Array::from(vec![min])) as ArrayRef
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "不支持的数据类型: {}",
                    input[0].column(self.column_index).data_type()
                ));
            }
        };

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
            self.input_schema.field(self.column_index).data_type().clone(),
            true,
        )]))
    }
}
