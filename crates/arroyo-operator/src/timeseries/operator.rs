use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{Array, Float64Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use tracing::{debug, info, warn};

use crate::context::{Collector, OperatorContext};
use crate::operator::{ArrowOperator, ConstructedOperator, OperatorConstructor};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_types::{to_nanos, Watermark};

use super::{TimeSeriesAnalysisConfig, TimeSeriesAnalysisMethod, TimeSeriesAnalysisResult};
use super::analysis::TimeSeriesAnalysisEngine;

/// 时间序列分析操作符
pub struct TimeSeriesAnalysisOperator {
    /// 分析引擎
    engine: TimeSeriesAnalysisEngine,
    /// 输入模式
    input_schema: Arc<ArroyoSchema>,
    /// 输出模式
    output_schema: Arc<ArroyoSchema>,
    /// 时间字段名称
    time_field: String,
    /// 值字段名称
    value_field: String,
    /// 分析方法
    method: i32,
}

impl TimeSeriesAnalysisOperator {
    /// 创建新的时间序列分析操作符
    pub fn new(
        config: TimeSeriesAnalysisConfig,
        input_schema: Arc<ArroyoSchema>,
    ) -> Result<Self> {
        // 创建分析引擎
        let engine = TimeSeriesAnalysisEngine::new(config.clone(), &input_schema)?;

        // 创建输出模式 - 根据分析方法添加不同的输出字段
        let mut output_fields = input_schema.schema.fields().to_vec();

        // 添加分析结果字段
        match TimeSeriesAnalysisMethod::try_from(config.method) {
            Ok(TimeSeriesAnalysisMethod::MovingAverage) | Ok(TimeSeriesAnalysisMethod::ExponentialMovingAverage) => {
                output_fields.push(Field::new(
                    format!("{}_ma", config.value_field),
                    DataType::Float64,
                    true,
                ));
            }
            Ok(TimeSeriesAnalysisMethod::TrendAnalysis) => {
                output_fields.push(Field::new(
                    format!("{}_trend", config.value_field),
                    DataType::Float64,
                    true,
                ));
                output_fields.push(Field::new(
                    "trend_intercept",
                    DataType::Float64,
                    true,
                ));
                output_fields.push(Field::new(
                    "trend_slope",
                    DataType::Float64,
                    true,
                ));
            }
            Ok(TimeSeriesAnalysisMethod::SeasonalityDetection) => {
                output_fields.push(Field::new(
                    "is_seasonal",
                    DataType::Boolean,
                    true,
                ));
                output_fields.push(Field::new(
                    "seasonality_period",
                    DataType::UInt32,
                    true,
                ));
            }
            Ok(TimeSeriesAnalysisMethod::AnomalyDetection) => {
                output_fields.push(Field::new(
                    "is_anomaly",
                    DataType::Boolean,
                    true,
                ));
                output_fields.push(Field::new(
                    "anomaly_score",
                    DataType::Float64,
                    true,
                ));
            }
            Ok(TimeSeriesAnalysisMethod::Forecasting) => {
                output_fields.push(Field::new(
                    format!("{}_forecast", config.value_field),
                    DataType::Float64,
                    true,
                ));
                output_fields.push(Field::new(
                    "forecast_time",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ));
            }
            Ok(TimeSeriesAnalysisMethod::CorrelationAnalysis) => {
                for field in &config.correlation_fields {
                    output_fields.push(Field::new(
                        format!("correlation_{}", field),
                        DataType::Float64,
                        true,
                    ));
                }
            }
            Err(_) => {
                return Err(anyhow!("Invalid time series analysis method: {}", config.method));
            }
        }

        let output_schema_arrow = Arc::new(Schema::new(output_fields));
        let output_schema = Arc::new(ArroyoSchema {
            schema: output_schema_arrow,
            timestamp_index: input_schema.timestamp_index,
            key_indices: input_schema.key_indices.clone(),
        });

        Ok(Self {
            engine,
            input_schema,
            output_schema,
            time_field: config.time_field,
            value_field: config.value_field,
            method: config.method,
        })
    }

    /// 将分析结果转换为记录批次
    fn result_to_batch(&self, batch: &RecordBatch, result: TimeSeriesAnalysisResult) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        // 创建新的列数组，包括原始列和分析结果列
        let mut columns = batch.columns().to_vec();

        // 根据分析方法添加不同的结果列
        match TimeSeriesAnalysisMethod::try_from(self.method) {
            Ok(TimeSeriesAnalysisMethod::MovingAverage) | Ok(TimeSeriesAnalysisMethod::ExponentialMovingAverage) => {
                // 添加移动平均列
                let mut ma_values = vec![0.0; batch.num_rows()];

                // 找到对应的处理后值
                for (i, row) in batch.rows().enumerate() {
                    if i < result.processed_series.len() {
                        ma_values[i] = result.processed_series[i].1;
                    }
                }

                columns.push(Arc::new(Float64Array::from(ma_values)));
            }
            Ok(TimeSeriesAnalysisMethod::TrendAnalysis) => {
                // 添加趋势列
                let mut trend_values = vec![0.0; batch.num_rows()];

                // 找到对应的趋势值
                for (i, row) in batch.rows().enumerate() {
                    if i < result.processed_series.len() {
                        trend_values[i] = result.processed_series[i].1;
                    }
                }

                columns.push(Arc::new(Float64Array::from(trend_values)));

                // 添加趋势系数列
                if let Some(coefficients) = result.trend_coefficients {
                    if coefficients.len() >= 2 {
                        let intercept = vec![coefficients[0]; batch.num_rows()];
                        let slope = vec![coefficients[1]; batch.num_rows()];

                        columns.push(Arc::new(Float64Array::from(intercept)));
                        columns.push(Arc::new(Float64Array::from(slope)));
                    }
                }
            }
            Ok(TimeSeriesAnalysisMethod::SeasonalityDetection) => {
                // 添加季节性检测列
                let is_seasonal = result.seasonality_coefficients.is_some();
                let is_seasonal_array = vec![is_seasonal; batch.num_rows()];

                columns.push(Arc::new(arrow::array::BooleanArray::from(is_seasonal_array)));

                // 添加季节性周期列
                let period = if is_seasonal {
                    result.seasonality_coefficients.as_ref().map(|c| c.len()).unwrap_or(0) as u32
                } else {
                    0
                };
                let period_array = vec![period; batch.num_rows()];

                columns.push(Arc::new(arrow::array::UInt32Array::from(period_array)));
            }
            Ok(TimeSeriesAnalysisMethod::AnomalyDetection) => {
                // 添加异常检测列
                let mut is_anomaly = vec![false; batch.num_rows()];
                let mut anomaly_score = vec![0.0; batch.num_rows()];

                // 找到异常点
                if let Some(anomalies) = &result.anomalies {
                    for (i, row) in batch.rows().enumerate() {
                        let timestamp_array = batch
                            .column(self.input_schema.timestamp_index)
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();

                        let event_time = arroyo_types::from_nanos(timestamp_array.value(i));

                        // 检查是否为异常点
                        for (anomaly_time, anomaly_value) in anomalies {
                            if event_time == *anomaly_time {
                                is_anomaly[i] = true;

                                // 计算异常分数（与均值的偏差）
                                let mean = result.original_series.iter().map(|(_, v)| *v).sum::<f64>()
                                    / result.original_series.len() as f64;
                                anomaly_score[i] = (*anomaly_value - mean).abs();
                                break;
                            }
                        }
                    }
                }

                columns.push(Arc::new(arrow::array::BooleanArray::from(is_anomaly)));
                columns.push(Arc::new(Float64Array::from(anomaly_score)));
            }
            Ok(TimeSeriesAnalysisMethod::Forecasting) => {
                // 添加预测列
                let mut forecast_values = vec![0.0; batch.num_rows()];
                let mut forecast_times = vec![0; batch.num_rows()];

                // 使用最后一个预测值
                if let Some(forecasts) = &result.forecasts {
                    if !forecasts.is_empty() {
                        let last_forecast = forecasts.last().unwrap();
                        for i in 0..batch.num_rows() {
                            forecast_values[i] = last_forecast.1;
                            forecast_times[i] = to_nanos(last_forecast.0);
                        }
                    }
                }

                columns.push(Arc::new(Float64Array::from(forecast_values)));
                columns.push(Arc::new(TimestampNanosecondArray::from(forecast_times)));
            }
            Ok(TimeSeriesAnalysisMethod::CorrelationAnalysis) => {
                // 添加相关性分析列
                if let Some(correlations) = &result.correlations {
                    for field in &self.engine.config.correlation_fields {
                        let correlation = correlations.get(field).cloned().unwrap_or(0.0);
                        let correlation_array = vec![correlation; batch.num_rows()];

                        columns.push(Arc::new(Float64Array::from(correlation_array)));
                    }
                }
            }
            Err(_) => {
                return Err(anyhow!("Invalid time series analysis method: {}", self.method));
            }
        }

        // 创建新的记录批次
        let batch = RecordBatch::try_new(self.output_schema.schema.clone(), columns)
            .map_err(|e| anyhow!("Failed to create output batch: {}", e))?;

        Ok(Some(batch))
    }
}

#[async_trait]
impl ArrowOperator for TimeSeriesAnalysisOperator {
    fn name(&self) -> String {
        "TimeSeriesAnalysis".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) {
        info!("Starting TimeSeriesAnalysis operator");
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        match self.engine.process_batch(&batch) {
            Ok(Some(result)) => {
                debug!("Time series analysis completed");

                match self.result_to_batch(&batch, result) {
                    Ok(Some(output_batch)) => {
                        collector.collect(output_batch).await;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!("Failed to convert analysis result to batch: {}", e);
                    }
                }
            }
            Ok(None) => {
                // 数据不足，直接传递原始批次
                collector.collect(batch).await;
            }
            Err(e) => {
                warn!("Error processing batch in TimeSeriesAnalysis operator: {}", e);
                // 出错时，直接传递原始批次
                collector.collect(batch).await;
            }
        }
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Option<Watermark> {
        match self.engine.process_watermark(&watermark) {
            Ok(Some(result)) => {
                debug!("Time series analysis completed on watermark");

                // 创建一个空批次，用于输出分析结果
                let empty_batch = RecordBatch::new_empty(self.input_schema.schema.clone());

                match self.result_to_batch(&empty_batch, result) {
                    Ok(Some(output_batch)) => {
                        collector.collect(output_batch).await;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!("Failed to convert analysis result to batch: {}", e);
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!("Error processing watermark in TimeSeriesAnalysis operator: {}", e);
            }
        }

        Some(watermark)
    }
}

/// 时间序列分析操作符构造器
pub struct TimeSeriesAnalysisOperatorConstructor;

impl OperatorConstructor for TimeSeriesAnalysisOperatorConstructor {
    type ConfigT = TimeSeriesAnalysisConfig;

    fn with_config(
        &self,
        config: Self::ConfigT,
        _registry: Arc<crate::operator::Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let input_schema = Arc::new(ArroyoSchema::default());

        let operator = TimeSeriesAnalysisOperator::new(config, input_schema)?;

        Ok(ConstructedOperator::from_operator(Box::new(operator)))
    }
}
