use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Result};
use arrow::array::{Float64Array, RecordBatch};
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::PrimitiveArray;
use linregress::{FormulaRegressionBuilder, RegressionDataBuilder};

use arroyo_rpc::df::ArroyoSchema;
use arroyo_types::{from_nanos, Watermark};

use super::{
    AnomalyDetectionMethod, ForecastingMethod, MovingAverageType, TimeSeriesAnalysisConfig,
    TimeSeriesAnalysisMethod, TimeSeriesAnalysisResult, TrendAnalysisType,
};

/// 时间序列分析引擎
pub struct TimeSeriesAnalysisEngine {
    /// 配置
    config: TimeSeriesAnalysisConfig,
    /// 时间字段索引
    time_field_index: usize,
    /// 值字段索引
    value_field_index: usize,
    /// 相关性字段索引
    correlation_field_indices: Vec<usize>,
    /// 时间序列数据
    time_series_data: VecDeque<(SystemTime, f64)>,
    /// 相关性数据
    correlation_data: HashMap<String, VecDeque<f64>>,
    /// 窗口大小
    window_size: Duration,
    /// 滑动步长
    slide: Duration,
    /// 最后处理的水印时间
    last_watermark: Option<SystemTime>,
}

impl TimeSeriesAnalysisEngine {
    /// 创建新的时间序列分析引擎
    pub fn new(
        config: TimeSeriesAnalysisConfig,
        input_schema: &ArroyoSchema,
    ) -> Result<Self> {
        // 查找时间字段索引
        let time_field_index = input_schema
            .schema
            .fields()
            .iter()
            .position(|f| f.name() == config.time_field)
            .ok_or_else(|| anyhow!("Time field not found: {}", config.time_field))?;

        // 查找值字段索引
        let value_field_index = input_schema
            .schema
            .fields()
            .iter()
            .position(|f| f.name() == config.value_field)
            .ok_or_else(|| anyhow!("Value field not found: {}", config.value_field))?;

        // 查找相关性字段索引
        let correlation_field_indices = config
            .correlation_fields
            .iter()
            .map(|field| {
                input_schema
                    .schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == field)
                    .ok_or_else(|| anyhow!("Correlation field not found: {}", field))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            config: config.clone(),
            time_field_index,
            value_field_index,
            correlation_field_indices,
            time_series_data: VecDeque::new(),
            correlation_data: config
                .correlation_fields
                .iter()
                .map(|field| (field.clone(), VecDeque::new()))
                .collect(),
            window_size: Duration::from_millis(config.window_size_ms),
            slide: Duration::from_millis(config.slide_ms),
            last_watermark: None,
        })
    }

    /// 处理新的事件批次
    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<Option<TimeSeriesAnalysisResult>> {
        let timestamp_array = batch
            .column(self.time_field_index)
            .as_primitive::<TimestampNanosecondType>();

        let value_array = batch
            .column(self.value_field_index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow!("Value field is not a Float64Array"))?;

        // 收集相关性字段数据
        let mut correlation_arrays = HashMap::new();
        for (i, field) in self.config.correlation_fields.iter().enumerate() {
            let array = batch
                .column(self.correlation_field_indices[i])
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("Correlation field {} is not a Float64Array", field))?;
            correlation_arrays.insert(field.clone(), array);
        }

        // 处理每一行数据
        for row_idx in 0..batch.num_rows() {
            let event_time = from_nanos(timestamp_array.value(row_idx));
            let value = value_array.value(row_idx);

            // 添加到时间序列数据
            self.time_series_data.push_back((event_time, value));

            // 添加到相关性数据
            for (field, array) in &correlation_arrays {
                self.correlation_data
                    .get_mut(field)
                    .unwrap()
                    .push_back(array.value(row_idx));
            }
        }

        // 清理过期数据
        self.clean_old_data();

        // 如果数据足够，执行分析
        if self.time_series_data.len() >= self.config.ma_window_size as usize {
            self.analyze()
        } else {
            Ok(None)
        }
    }

    /// 处理水印
    pub fn process_watermark(&mut self, watermark: &Watermark) -> Result<Option<TimeSeriesAnalysisResult>> {
        let watermark_time = watermark.timestamp;
        self.last_watermark = Some(watermark_time);

        // 清理过期数据
        self.clean_old_data();

        // 如果数据足够，执行分析
        if self.time_series_data.len() >= self.config.ma_window_size as usize {
            self.analyze()
        } else {
            Ok(None)
        }
    }

    /// 清理过期数据
    fn clean_old_data(&mut self) {
        if let Some(watermark_time) = self.last_watermark {
            let cutoff_time = watermark_time - self.window_size;

            // 清理时间序列数据
            while let Some((time, _)) = self.time_series_data.front() {
                if *time < cutoff_time {
                    self.time_series_data.pop_front();
                } else {
                    break;
                }
            }

            // 清理相关性数据
            for data in self.correlation_data.values_mut() {
                while data.len() > self.time_series_data.len() {
                    data.pop_front();
                }
            }
        }
    }

    /// 执行时间序列分析
    fn analyze(&self) -> Result<Option<TimeSeriesAnalysisResult>> {
        let method = TimeSeriesAnalysisMethod::try_from(self.config.method)
            .map_err(|_| anyhow!("Invalid time series analysis method: {}", self.config.method))?;

        match method {
            TimeSeriesAnalysisMethod::MovingAverage => self.compute_moving_average(),
            TimeSeriesAnalysisMethod::ExponentialMovingAverage => self.compute_exponential_moving_average(),
            TimeSeriesAnalysisMethod::TrendAnalysis => self.analyze_trend(),
            TimeSeriesAnalysisMethod::SeasonalityDetection => self.detect_seasonality(),
            TimeSeriesAnalysisMethod::AnomalyDetection => self.detect_anomalies(),
            TimeSeriesAnalysisMethod::Forecasting => self.forecast(),
            TimeSeriesAnalysisMethod::CorrelationAnalysis => self.analyze_correlation(),
        }
    }

    /// 计算移动平均
    fn compute_moving_average(&self) -> Result<Option<TimeSeriesAnalysisResult>> {
        let ma_type = MovingAverageType::try_from(self.config.ma_type)
            .map_err(|_| anyhow!("Invalid moving average type: {}", self.config.ma_type))?;

        let window_size = self.config.ma_window_size as usize;
        if self.time_series_data.len() < window_size {
            return Ok(None);
        }

        let mut processed_series = Vec::with_capacity(self.time_series_data.len());

        match ma_type {
            MovingAverageType::Simple => {
                // 简单移动平均
                for i in window_size - 1..self.time_series_data.len() {
                    let sum: f64 = (i + 1 - window_size..=i)
                        .map(|j| self.time_series_data[j].1)
                        .sum();
                    let avg = sum / window_size as f64;
                    processed_series.push((self.time_series_data[i].0, avg));
                }
            }
            MovingAverageType::Weighted => {
                // 加权移动平均
                for i in window_size - 1..self.time_series_data.len() {
                    let mut sum = 0.0;
                    let mut weight_sum = 0.0;

                    for (j, w) in (i + 1 - window_size..=i).enumerate() {
                        let weight = (j + 1) as f64;
                        sum += self.time_series_data[w].1 * weight;
                        weight_sum += weight;
                    }

                    let avg = sum / weight_sum;
                    processed_series.push((self.time_series_data[i].0, avg));
                }
            }
            MovingAverageType::Exponential => {
                // 指数移动平均
                let alpha = self.config.ema_alpha;
                let mut ema = self.time_series_data[0].1;

                for i in 0..self.time_series_data.len() {
                    ema = alpha * self.time_series_data[i].1 + (1.0 - alpha) * ema;
                    processed_series.push((self.time_series_data[i].0, ema));
                }
            }
        }

        Ok(Some(TimeSeriesAnalysisResult {
            original_series: self.time_series_data.iter().cloned().collect(),
            processed_series,
            trend_coefficients: None,
            seasonality_coefficients: None,
            anomalies: None,
            forecasts: None,
            correlations: None,
        }))
    }

    // 其他分析方法将在后续实现
    fn compute_exponential_moving_average(&self) -> Result<Option<TimeSeriesAnalysisResult>> {
        // 简化实现，实际上与 MovingAverageType::Exponential 相同
        let alpha = self.config.ema_alpha;
        let mut ema = self.time_series_data[0].1;
        let mut processed_series = Vec::with_capacity(self.time_series_data.len());

        for i in 0..self.time_series_data.len() {
            ema = alpha * self.time_series_data[i].1 + (1.0 - alpha) * ema;
            processed_series.push((self.time_series_data[i].0, ema));
        }

        Ok(Some(TimeSeriesAnalysisResult {
            original_series: self.time_series_data.iter().cloned().collect(),
            processed_series,
            trend_coefficients: None,
            seasonality_coefficients: None,
            anomalies: None,
            forecasts: None,
            correlations: None,
        }))
    }

    fn analyze_trend(&self) -> Result<Option<TimeSeriesAnalysisResult>> {
        // 简化实现，仅支持线性趋势
        if self.time_series_data.len() < 2 {
            return Ok(None);
        }

        // 准备数据
        let x: Vec<f64> = (0..self.time_series_data.len()).map(|i| i as f64).collect();
        let y: Vec<f64> = self.time_series_data.iter().map(|(_, v)| *v).collect();

        // 线性回归
        let data = RegressionDataBuilder::new()
            .build_from(x.as_slice(), y.as_slice())
            .map_err(|e| anyhow!("Failed to build regression data: {}", e))?;

        let formula = FormulaRegressionBuilder::new()
            .formula("y ~ x")
            .data(&data)
            .fit()
            .map_err(|e| anyhow!("Failed to fit regression model: {}", e))?;

        let params = formula.parameters();
        let intercept = params[0];
        let slope = params[1];

        // 计算趋势线
        let mut processed_series = Vec::with_capacity(self.time_series_data.len());
        for i in 0..self.time_series_data.len() {
            let trend_value = intercept + slope * (i as f64);
            processed_series.push((self.time_series_data[i].0, trend_value));
        }

        Ok(Some(TimeSeriesAnalysisResult {
            original_series: self.time_series_data.iter().cloned().collect(),
            processed_series,
            trend_coefficients: Some(vec![intercept, slope]),
            seasonality_coefficients: None,
            anomalies: None,
            forecasts: None,
            correlations: None,
        }))
    }

    fn detect_seasonality(&self) -> Result<Option<TimeSeriesAnalysisResult>> {
        // 简化实现，仅检测是否存在周期性
        if self.time_series_data.len() < self.config.seasonality_period as usize * 2 {
            return Ok(None);
        }

        // 提取值
        let values: Vec<f64> = self.time_series_data.iter().map(|(_, v)| *v).collect();

        // 计算自相关
        let period = self.config.seasonality_period as usize;
        let mut autocorr = Vec::with_capacity(period);

        for lag in 1..=period {
            let mut sum_product = 0.0;
            let mut sum_sq = 0.0;

            for i in 0..values.len() - lag {
                sum_product += values[i] * values[i + lag];
                sum_sq += values[i].powi(2);
            }

            let autocorr_value = sum_product / sum_sq;
            autocorr.push(autocorr_value);
        }

        // 找出最大自相关值对应的滞后期
        let max_autocorr_idx = autocorr
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(i, _)| i + 1)
            .unwrap_or(0);

        // 如果最大自相关值对应的滞后期接近指定的周期，则认为存在季节性
        let seasonality_coefficients = if (max_autocorr_idx as i32 - period as i32).abs() <= period as i32 / 4 {
            Some(autocorr)
        } else {
            None
        };

        Ok(Some(TimeSeriesAnalysisResult {
            original_series: self.time_series_data.iter().cloned().collect(),
            processed_series: self.time_series_data.iter().cloned().collect(),
            trend_coefficients: None,
            seasonality_coefficients: seasonality_coefficients.map(|v| v.iter().cloned().collect()),
            anomalies: None,
            forecasts: None,
            correlations: None,
        }))
    }

    fn detect_anomalies(&self) -> Result<Option<TimeSeriesAnalysisResult>> {
        // 简化实现，使用标准差方法检测异常
        if self.time_series_data.len() < 10 {
            return Ok(None);
        }

        // 提取值
        let values: Vec<f64> = self.time_series_data.iter().map(|(_, v)| *v).collect();

        // 计算均值和标准差
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let std_dev = variance.sqrt();

        // 检测异常
        let threshold = self.config.anomaly_threshold * std_dev;
        let mut anomalies = Vec::new();

        for (i, (time, value)) in self.time_series_data.iter().enumerate() {
            if (value - mean).abs() > threshold {
                anomalies.push((*time, *value));
            }
        }

        Ok(Some(TimeSeriesAnalysisResult {
            original_series: self.time_series_data.iter().cloned().collect(),
            processed_series: self.time_series_data.iter().cloned().collect(),
            trend_coefficients: None,
            seasonality_coefficients: None,
            anomalies: Some(anomalies),
            forecasts: None,
            correlations: None,
        }))
    }

    fn forecast(&self) -> Result<Option<TimeSeriesAnalysisResult>> {
        // 简化实现，使用简单移动平均进行预测
        if self.time_series_data.len() < self.config.ma_window_size as usize {
            return Ok(None);
        }

        let window_size = self.config.ma_window_size as usize;
        let forecast_steps = self.config.forecast_steps as usize;

        // 提取值
        let values: Vec<f64> = self.time_series_data.iter().map(|(_, v)| *v).collect();

        // 计算最后一个窗口的平均值
        let last_window_avg = values[values.len() - window_size..].iter().sum::<f64>() / window_size as f64;

        // 预测未来值
        let mut forecasts = Vec::with_capacity(forecast_steps);
        let last_time = self.time_series_data.back().unwrap().0;
        let time_step = if self.time_series_data.len() > 1 {
            let time_diff = self.time_series_data[self.time_series_data.len() - 1].0
                .duration_since(self.time_series_data[self.time_series_data.len() - 2].0)
                .unwrap_or(Duration::from_secs(1));
            time_diff
        } else {
            Duration::from_secs(1)
        };

        for i in 1..=forecast_steps {
            let forecast_time = last_time + time_step * i as u32;
            forecasts.push((forecast_time, last_window_avg));
        }

        Ok(Some(TimeSeriesAnalysisResult {
            original_series: self.time_series_data.iter().cloned().collect(),
            processed_series: self.time_series_data.iter().cloned().collect(),
            trend_coefficients: None,
            seasonality_coefficients: None,
            anomalies: None,
            forecasts: Some(forecasts),
            correlations: None,
        }))
    }

    fn analyze_correlation(&self) -> Result<Option<TimeSeriesAnalysisResult>> {
        // 简化实现，计算皮尔逊相关系数
        if self.time_series_data.len() < 10 {
            return Ok(None);
        }

        // 提取主时间序列值
        let main_values: Vec<f64> = self.time_series_data.iter().map(|(_, v)| *v).collect();

        // 计算相关系数
        let mut correlations = HashMap::new();

        for (field, data) in &self.correlation_data {
            if data.len() != self.time_series_data.len() {
                continue;
            }

            let other_values: Vec<f64> = data.iter().cloned().collect();

            // 计算皮尔逊相关系数
            let main_mean = main_values.iter().sum::<f64>() / main_values.len() as f64;
            let other_mean = other_values.iter().sum::<f64>() / other_values.len() as f64;

            let mut numerator = 0.0;
            let mut main_denom = 0.0;
            let mut other_denom = 0.0;

            for i in 0..main_values.len() {
                let main_diff = main_values[i] - main_mean;
                let other_diff = other_values[i] - other_mean;

                numerator += main_diff * other_diff;
                main_denom += main_diff.powi(2);
                other_denom += other_diff.powi(2);
            }

            let correlation = numerator / (main_denom.sqrt() * other_denom.sqrt());
            correlations.insert(field.clone(), correlation);
        }

        Ok(Some(TimeSeriesAnalysisResult {
            original_series: self.time_series_data.iter().cloned().collect(),
            processed_series: self.time_series_data.iter().cloned().collect(),
            trend_coefficients: None,
            seasonality_coefficients: None,
            anomalies: None,
            forecasts: None,
            correlations: Some(correlations),
        }))
    }
}
