use crate::error::{Error, Result};
use prometheus::{
    register_counter_vec, register_histogram_vec, register_int_counter_vec, register_int_gauge_vec,
    Counter, CounterVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Opts,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// 客户端指标类型
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClientMetricType {
    /// 请求计数
    RequestCount,
    /// 请求延迟
    RequestLatency,
    /// 请求错误计数
    ErrorCount,
    /// 重试计数
    RetryCount,
    /// 故障转移计数
    FailoverCount,
    /// 缓存命中计数
    CacheHitCount,
    /// 缓存未命中计数
    CacheMissCount,
    /// 预取计数
    PrefetchCount,
    /// 连接池大小
    PoolSize,
    /// 连接池使用量
    PoolUsage,
    /// 会话计数
    SessionCount,
    /// 发送字节数
    BytesSent,
    /// 接收字节数
    BytesReceived,
    /// 发送消息数
    MessagesSent,
    /// 接收消息数
    MessagesReceived,
    /// 批处理大小
    BatchSize,
    /// 批处理延迟
    BatchLatency,
    /// 自定义指标
    Custom(String),
}

/// 客户端指标配置
#[derive(Debug, Clone, PartialEq)]
pub struct ClientMetricsConfig {
    /// 是否启用指标收集
    pub enabled: bool,
    /// 指标前缀
    pub prefix: String,
    /// 指标标签
    pub labels: HashMap<String, String>,
    /// 收集间隔（秒）
    pub collection_interval_secs: u64,
    /// 是否启用直方图
    pub enable_histograms: bool,
    /// 直方图桶
    pub histogram_buckets: Vec<f64>,
    /// 是否启用自动导出
    pub enable_export: bool,
    /// 导出地址
    pub export_address: Option<String>,
    /// 导出间隔（秒）
    pub export_interval_secs: u64,
    /// 是否启用调试日志
    pub enable_debug_logs: bool,
}

impl Default for ClientMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefix: "arroyo_client".to_string(),
            labels: HashMap::new(),
            collection_interval_secs: 60,
            enable_histograms: true,
            histogram_buckets: vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
            enable_export: false,
            export_address: None,
            export_interval_secs: 60,
            enable_debug_logs: false,
        }
    }
}

/// 客户端指标管理器
#[derive(Clone)]
pub struct ClientMetricsManager {
    /// 指标配置
    config: ClientMetricsConfig,
    /// 整数计数器
    int_counters: Arc<RwLock<HashMap<String, IntCounterVec>>>,
    /// 浮点计数器
    counters: Arc<RwLock<HashMap<String, CounterVec>>>,
    /// 整数仪表
    int_gauges: Arc<RwLock<HashMap<String, IntGaugeVec>>>,
    /// 直方图
    histograms: Arc<RwLock<HashMap<String, HistogramVec>>>,
    /// 最后收集时间
    last_collection: Arc<Mutex<Instant>>,
    /// 是否正在收集
    is_collecting: Arc<Mutex<bool>>,
    /// 自定义指标
    custom_metrics: Arc<RwLock<HashMap<String, Box<dyn MetricCollector + Send + Sync>>>>,
}

/// 指标收集器特质
pub trait MetricCollector {
    /// 收集指标
    fn collect(&self) -> HashMap<String, f64>;
    /// 获取指标名称
    fn name(&self) -> &str;
    /// 获取指标帮助信息
    fn help(&self) -> &str;
}

impl ClientMetricsManager {
    /// 创建新的客户端指标管理器
    pub fn new(config: ClientMetricsConfig) -> Self {
        let manager = Self {
            config,
            int_counters: Arc::new(RwLock::new(HashMap::new())),
            counters: Arc::new(RwLock::new(HashMap::new())),
            int_gauges: Arc::new(RwLock::new(HashMap::new())),
            histograms: Arc::new(RwLock::new(HashMap::new())),
            last_collection: Arc::new(Mutex::new(Instant::now())),
            is_collecting: Arc::new(Mutex::new(false)),
            custom_metrics: Arc::new(RwLock::new(HashMap::new())),
        };

        // 如果启用了指标收集，启动收集任务
        if manager.config.enabled {
            let manager_clone = manager.clone();
            tokio::spawn(async move {
                manager_clone.start_collection_task().await;
            });

            // 如果启用了导出，启动导出任务
            if manager.config.enable_export {
                let manager_clone = manager.clone();
                tokio::spawn(async move {
                    manager_clone.start_export_task().await;
                });
            }
        }

        manager
    }

    /// 获取或创建整数计数器
    pub async fn get_or_create_int_counter(
        &self,
        name: &str,
        help: &str,
        label_names: &[&str],
    ) -> Result<IntCounterVec> {
        let mut int_counters = self.int_counters.write().await;

        if let Some(counter) = int_counters.get(name) {
            return Ok(counter.clone());
        }

        // 创建新的计数器
        let counter_name = format!("{}_{}", self.config.prefix, name);
        let opts = Opts::new(&counter_name, help).const_labels(self.config.labels.clone());
        let counter = register_int_counter_vec!(opts, label_names)
            .map_err(|e| Error::MetricsError(format!("Failed to register counter: {}", e)))?;

        int_counters.insert(name.to_string(), counter.clone());
        Ok(counter)
    }

    /// 获取或创建浮点计数器
    pub async fn get_or_create_counter(
        &self,
        name: &str,
        help: &str,
        label_names: &[&str],
    ) -> Result<CounterVec> {
        let mut counters = self.counters.write().await;

        if let Some(counter) = counters.get(name) {
            return Ok(counter.clone());
        }

        // 创建新的计数器
        let counter_name = format!("{}_{}", self.config.prefix, name);
        let opts = Opts::new(&counter_name, help).const_labels(self.config.labels.clone());
        let counter = register_counter_vec!(opts, label_names)
            .map_err(|e| Error::MetricsError(format!("Failed to register counter: {}", e)))?;

        counters.insert(name.to_string(), counter.clone());
        Ok(counter)
    }

    /// 获取或创建整数仪表
    pub async fn get_or_create_int_gauge(
        &self,
        name: &str,
        help: &str,
        label_names: &[&str],
    ) -> Result<IntGaugeVec> {
        let mut int_gauges = self.int_gauges.write().await;

        if let Some(gauge) = int_gauges.get(name) {
            return Ok(gauge.clone());
        }

        // 创建新的仪表
        let gauge_name = format!("{}_{}", self.config.prefix, name);
        let opts = Opts::new(&gauge_name, help).const_labels(self.config.labels.clone());
        let gauge = register_int_gauge_vec!(opts, label_names)
            .map_err(|e| Error::MetricsError(format!("Failed to register gauge: {}", e)))?;

        int_gauges.insert(name.to_string(), gauge.clone());
        Ok(gauge)
    }

    /// 获取或创建直方图
    pub async fn get_or_create_histogram(
        &self,
        name: &str,
        help: &str,
        label_names: &[&str],
    ) -> Result<HistogramVec> {
        if !self.config.enable_histograms {
            return Err(Error::MetricsError("Histograms are disabled".to_string()));
        }

        let mut histograms = self.histograms.write().await;

        if let Some(histogram) = histograms.get(name) {
            return Ok(histogram.clone());
        }

        // 创建新的直方图
        let histogram_name = format!("{}_{}", self.config.prefix, name);
        let opts = HistogramOpts::new(&histogram_name, help)
            .const_labels(self.config.labels.clone())
            .buckets(self.config.histogram_buckets.clone());
        let histogram = register_histogram_vec!(opts, label_names)
            .map_err(|e| Error::MetricsError(format!("Failed to register histogram: {}", e)))?;

        histograms.insert(name.to_string(), histogram.clone());
        Ok(histogram)
    }

    /// 增加整数计数器
    pub async fn increment_int_counter(
        &self,
        name: &str,
        help: &str,
        label_values: &[&str],
        value: u64,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let label_names = label_values.iter().map(|_| "").collect::<Vec<_>>();
        let counter = self
            .get_or_create_int_counter(name, help, &label_names)
            .await?;
        counter.with_label_values(label_values).inc_by(value);

        if self.config.enable_debug_logs {
            debug!(
                "Incremented counter {} with labels {:?} by {}",
                name, label_values, value
            );
        }

        Ok(())
    }

    /// 增加浮点计数器
    pub async fn increment_counter(
        &self,
        name: &str,
        help: &str,
        label_values: &[&str],
        value: f64,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let label_names = label_values.iter().map(|_| "").collect::<Vec<_>>();
        let counter = self.get_or_create_counter(name, help, &label_names).await?;
        counter.with_label_values(label_values).inc_by(value);

        if self.config.enable_debug_logs {
            debug!(
                "Incremented counter {} with labels {:?} by {}",
                name, label_values, value
            );
        }

        Ok(())
    }

    /// 设置整数仪表
    pub async fn set_int_gauge(
        &self,
        name: &str,
        help: &str,
        label_values: &[&str],
        value: i64,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let label_names = label_values.iter().map(|_| "").collect::<Vec<_>>();
        let gauge = self.get_or_create_int_gauge(name, help, &label_names).await?;
        gauge.with_label_values(label_values).set(value);

        if self.config.enable_debug_logs {
            debug!(
                "Set gauge {} with labels {:?} to {}",
                name, label_values, value
            );
        }

        Ok(())
    }

    /// 观察直方图
    pub async fn observe_histogram(
        &self,
        name: &str,
        help: &str,
        label_values: &[&str],
        value: f64,
    ) -> Result<()> {
        if !self.config.enabled || !self.config.enable_histograms {
            return Ok(());
        }

        let label_names = label_values.iter().map(|_| "").collect::<Vec<_>>();
        let histogram = self
            .get_or_create_histogram(name, help, &label_names)
            .await?;
        histogram.with_label_values(label_values).observe(value);

        if self.config.enable_debug_logs {
            debug!(
                "Observed histogram {} with labels {:?}: {}",
                name, label_values, value
            );
        }

        Ok(())
    }

    /// 添加自定义指标收集器
    pub async fn add_custom_collector(
        &self,
        collector: Box<dyn MetricCollector + Send + Sync>,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let mut custom_metrics = self.custom_metrics.write().await;
        custom_metrics.insert(collector.name().to_string(), collector);

        Ok(())
    }

    /// 启动收集任务
    async fn start_collection_task(&self) {
        let interval = Duration::from_secs(self.config.collection_interval_secs);

        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = self.collect_metrics().await {
                error!("Failed to collect metrics: {}", e);
            }
        }
    }

    /// 启动导出任务
    async fn start_export_task(&self) {
        if !self.config.enable_export {
            return;
        }

        let interval = Duration::from_secs(self.config.export_interval_secs);

        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = self.export_metrics().await {
                error!("Failed to export metrics: {}", e);
            }
        }
    }

    /// 收集指标
    async fn collect_metrics(&self) -> Result<()> {
        let mut is_collecting = self.is_collecting.lock().await;
        if *is_collecting {
            return Ok(());
        }
        *is_collecting = true;

        // 收集自定义指标
        let custom_metrics = self.custom_metrics.read().await;
        for (name, collector) in custom_metrics.iter() {
            let metrics = collector.collect();
            for (key, value) in metrics {
                let _ = self
                    .increment_counter(
                        &format!("custom_{}", name),
                        collector.help(),
                        &[&key],
                        value,
                    )
                    .await;
            }
        }

        *is_collecting = false;
        Ok(())
    }

    /// 导出指标
    async fn export_metrics(&self) -> Result<()> {
        if !self.config.enable_export {
            return Ok(());
        }

        // 在实际实现中，这里应该将指标导出到指定的地址
        // 例如 Prometheus Pushgateway 或其他监控系统
        if let Some(export_address) = &self.config.export_address {
            info!("Exporting metrics to {}", export_address);
            // TODO: 实现实际的导出逻辑
        }

        Ok(())
    }
}

impl std::fmt::Debug for ClientMetricsManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientMetricsManager")
            .field("config", &self.config)
            .field("last_collection", &self.last_collection)
            .field("is_collecting", &self.is_collecting)
            .finish_non_exhaustive()
    }
}
