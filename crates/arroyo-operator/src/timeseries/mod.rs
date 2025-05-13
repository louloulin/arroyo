use std::collections::HashMap;
use std::time::SystemTime;

use prost::Message;
use serde::{Deserialize, Serialize};

/// 时间序列分析方法
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeSeriesAnalysisMethod {
    /// 移动平均
    MovingAverage,
    /// 指数移动平均
    ExponentialMovingAverage,
    /// 趋势分析
    TrendAnalysis,
    /// 季节性检测
    SeasonalityDetection,
    /// 异常检测
    AnomalyDetection,
    /// 预测
    Forecasting,
    /// 相关性分析
    CorrelationAnalysis,
}

/// 移动平均类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MovingAverageType {
    /// 简单移动平均
    Simple,
    /// 加权移动平均
    Weighted,
    /// 指数移动平均
    Exponential,
}

/// 趋势分析类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrendAnalysisType {
    /// 线性趋势
    Linear,
    /// 多项式趋势
    Polynomial,
    /// 指数趋势
    Exponential,
}

/// 异常检测方法
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnomalyDetectionMethod {
    /// 标准差方法
    StandardDeviation,
    /// 四分位数方法
    InterquartileRange,
    /// 密度方法
    DensityBased,
}

/// 预测方法
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForecastingMethod {
    /// 自回归
    AutoRegressive,
    /// 移动平均
    MovingAverage,
    /// 自回归移动平均
    ARMA,
    /// 自回归积分移动平均
    ARIMA,
    /// 指数平滑
    ExponentialSmoothing,
}

// 实现 TryFrom<i32> 特性，用于从整数转换为枚举
impl TryFrom<i32> for TimeSeriesAnalysisMethod {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::MovingAverage),
            1 => Ok(Self::ExponentialMovingAverage),
            2 => Ok(Self::TrendAnalysis),
            3 => Ok(Self::SeasonalityDetection),
            4 => Ok(Self::AnomalyDetection),
            5 => Ok(Self::Forecasting),
            6 => Ok(Self::CorrelationAnalysis),
            _ => Err(()),
        }
    }
}

// 实现 TryFrom<i32> 特性，用于从整数转换为枚举
impl TryFrom<i32> for MovingAverageType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Simple),
            1 => Ok(Self::Weighted),
            2 => Ok(Self::Exponential),
            _ => Err(()),
        }
    }
}

// 实现 TryFrom<i32> 特性，用于从整数转换为枚举
impl TryFrom<i32> for TrendAnalysisType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Linear),
            1 => Ok(Self::Polynomial),
            2 => Ok(Self::Exponential),
            _ => Err(()),
        }
    }
}

// 实现 TryFrom<i32> 特性，用于从整数转换为枚举
impl TryFrom<i32> for AnomalyDetectionMethod {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::StandardDeviation),
            1 => Ok(Self::InterquartileRange),
            2 => Ok(Self::DensityBased),
            _ => Err(()),
        }
    }
}

// 实现 TryFrom<i32> 特性，用于从整数转换为枚举
impl TryFrom<i32> for ForecastingMethod {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::AutoRegressive),
            1 => Ok(Self::MovingAverage),
            2 => Ok(Self::ARMA),
            3 => Ok(Self::ARIMA),
            4 => Ok(Self::ExponentialSmoothing),
            _ => Err(()),
        }
    }
}

/// 时间序列分析操作符配置
#[derive(Clone, Message)]
pub struct TimeSeriesAnalysisConfig {
    /// 分析方法
    #[prost(enumeration = "i32", tag = "1")]
    pub method: i32,

    /// 时间字段名称
    #[prost(string, tag = "2")]
    pub time_field: String,

    /// 值字段名称
    #[prost(string, tag = "3")]
    pub value_field: String,

    /// 窗口大小（毫秒）
    #[prost(uint64, tag = "4")]
    pub window_size_ms: u64,

    /// 滑动步长（毫秒）
    #[prost(uint64, tag = "5")]
    pub slide_ms: u64,

    /// 移动平均类型（仅用于移动平均方法）
    #[prost(enumeration = "i32", tag = "6")]
    pub ma_type: i32,

    /// 移动平均窗口大小
    #[prost(uint32, tag = "7")]
    pub ma_window_size: u32,

    /// 指数移动平均平滑因子（0-1）
    #[prost(float, tag = "8")]
    pub ema_alpha: f32,

    /// 趋势分析类型
    #[prost(enumeration = "i32", tag = "9")]
    pub trend_type: i32,

    /// 多项式趋势阶数
    #[prost(uint32, tag = "10")]
    pub polynomial_degree: u32,

    /// 异常检测方法
    #[prost(enumeration = "i32", tag = "11")]
    pub anomaly_method: i32,

    /// 异常检测阈值（标准差倍数或四分位距倍数）
    #[prost(float, tag = "12")]
    pub anomaly_threshold: f32,

    /// 预测方法
    #[prost(enumeration = "i32", tag = "13")]
    pub forecast_method: i32,

    /// 预测步数
    #[prost(uint32, tag = "14")]
    pub forecast_steps: u32,

    /// 季节性周期（以时间单位计）
    #[prost(uint32, tag = "15")]
    pub seasonality_period: u32,

    /// 相关性分析的其他字段
    #[prost(string, repeated, tag = "16")]
    pub correlation_fields: Vec<String>,
}

/// 时间序列分析结果
#[derive(Debug, Clone)]
pub struct TimeSeriesAnalysisResult {
    /// 原始时间序列
    pub original_series: Vec<(SystemTime, f64)>,
    /// 处理后的时间序列
    pub processed_series: Vec<(SystemTime, f64)>,
    /// 趋势系数（用于趋势分析）
    pub trend_coefficients: Option<Vec<f64>>,
    /// 季节性系数（用于季节性检测）
    pub seasonality_coefficients: Option<Vec<f64>>,
    /// 异常点（用于异常检测）
    pub anomalies: Option<Vec<(SystemTime, f64)>>,
    /// 预测值（用于预测）
    pub forecasts: Option<Vec<(SystemTime, f64)>>,
    /// 相关系数（用于相关性分析）
    pub correlations: Option<HashMap<String, f64>>,
}

// 在后续文件中实现时间序列分析操作符
pub mod operator;
pub mod analysis;

#[cfg(test)]
mod tests;
