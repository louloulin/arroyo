use crate::metrics::{MetricCollector};
use crate::connection::{ConnectionPool};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::debug;

/// 连接池指标收集器
pub struct ConnectionPoolMetricsCollector {
    /// 连接池
    pool: ConnectionPool,
    /// 名称
    name: String,
    /// 帮助信息
    help: String,
    /// 上次收集时间
    last_collection: Instant,
}

impl ConnectionPoolMetricsCollector {
    /// 创建新的连接池指标收集器
    pub fn new(pool: ConnectionPool) -> Self {
        Self {
            pool,
            name: "connection_pool".to_string(),
            help: "Connection pool metrics".to_string(),
            last_collection: Instant::now(),
        }
    }
}

impl MetricCollector for ConnectionPoolMetricsCollector {
    fn collect(&self) -> HashMap<String, f64> {
        let mut metrics = HashMap::new();

        // 获取连接池统计信息
        let stats = futures::executor::block_on(self.pool.get_pool_stats());
        metrics.insert("total_connections".to_string(), stats.total_connections as f64);
        metrics.insert("in_use_connections".to_string(), stats.in_use_connections as f64);
        metrics.insert("idle_connections".to_string(), stats.idle_connections as f64);
        metrics.insert("validating_connections".to_string(), stats.validating_connections as f64);
        metrics.insert("closed_connections".to_string(), stats.closed_connections as f64);
        metrics.insert("total_sessions".to_string(), stats.total_sessions as f64);
        metrics.insert("max_connections".to_string(), stats.max_connections as f64);
        metrics.insert("min_connections".to_string(), stats.min_connections as f64);

        metrics
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn help(&self) -> &str {
        &self.help
    }
}

/// 请求指标收集器
#[derive(Debug, Clone)]
pub struct RequestMetricsCollector {
    /// 请求计数
    request_count: Arc<RwLock<HashMap<String, u64>>>,
    /// 请求延迟
    request_latency: Arc<RwLock<HashMap<String, Vec<Duration>>>>,
    /// 错误计数
    error_count: Arc<RwLock<HashMap<String, u64>>>,
    /// 名称
    name: String,
    /// 帮助信息
    help: String,
}

impl RequestMetricsCollector {
    /// 创建新的请求指标收集器
    pub fn new() -> Self {
        Self {
            request_count: Arc::new(RwLock::new(HashMap::new())),
            request_latency: Arc::new(RwLock::new(HashMap::new())),
            error_count: Arc::new(RwLock::new(HashMap::new())),
            name: "request".to_string(),
            help: "Request metrics".to_string(),
        }
    }

    /// 记录请求
    pub async fn record_request(&self, method: &str, path: &str, duration: Duration, success: bool) {
        let key = format!("{}_{}", method, path);

        // 更新请求计数
        let mut request_count = self.request_count.write().await;
        *request_count.entry(key.clone()).or_insert(0) += 1;

        // 更新请求延迟
        let mut request_latency = self.request_latency.write().await;
        request_latency.entry(key.clone()).or_insert_with(Vec::new).push(duration);

        // 如果请求失败，更新错误计数
        if !success {
            let mut error_count = self.error_count.write().await;
            *error_count.entry(key).or_insert(0) += 1;
        }
    }
}

impl MetricCollector for RequestMetricsCollector {
    fn collect(&self) -> HashMap<String, f64> {
        let mut metrics = HashMap::new();

        // 获取请求计数
        let request_count = futures::executor::block_on(async {
            self.request_count.read().await
        });
        for (key, count) in request_count.iter() {
            metrics.insert(format!("count_{}", key), *count as f64);
        }

        // 获取请求延迟
        let request_latency = futures::executor::block_on(async {
            self.request_latency.read().await
        });
        for (key, latencies) in request_latency.iter() {
            if !latencies.is_empty() {
                // 计算平均延迟
                let avg_latency = latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / latencies.len() as f64;
                metrics.insert(format!("latency_{}", key), avg_latency);

                // 计算最大延迟
                let max_latency = latencies.iter().map(|d| d.as_secs_f64()).fold(0.0, f64::max);
                metrics.insert(format!("max_latency_{}", key), max_latency);

                // 计算最小延迟
                let min_latency = latencies.iter().map(|d| d.as_secs_f64()).fold(f64::INFINITY, |a, b| a.min(b));
                metrics.insert(format!("min_latency_{}", key), min_latency);

                // 计算 p95 延迟
                let mut sorted_latencies = latencies.iter().map(|d| d.as_secs_f64()).collect::<Vec<_>>();
                sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let p95_index = (sorted_latencies.len() as f64 * 0.95) as usize;
                if p95_index < sorted_latencies.len() {
                    metrics.insert(format!("p95_latency_{}", key), sorted_latencies[p95_index]);
                }
            }
        }

        // 获取错误计数
        let error_count = futures::executor::block_on(async {
            self.error_count.read().await
        });
        for (key, count) in error_count.iter() {
            metrics.insert(format!("errors_{}", key), *count as f64);
        }

        metrics
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn help(&self) -> &str {
        &self.help
    }
}

/// 缓存指标收集器
#[derive(Debug, Clone)]
pub struct CacheMetricsCollector {
    /// 缓存命中计数
    cache_hit_count: Arc<RwLock<HashMap<String, u64>>>,
    /// 缓存未命中计数
    cache_miss_count: Arc<RwLock<HashMap<String, u64>>>,
    /// 缓存大小
    cache_size: Arc<RwLock<HashMap<String, u64>>>,
    /// 名称
    name: String,
    /// 帮助信息
    help: String,
}

impl CacheMetricsCollector {
    /// 创建新的缓存指标收集器
    pub fn new() -> Self {
        Self {
            cache_hit_count: Arc::new(RwLock::new(HashMap::new())),
            cache_miss_count: Arc::new(RwLock::new(HashMap::new())),
            cache_size: Arc::new(RwLock::new(HashMap::new())),
            name: "cache".to_string(),
            help: "Cache metrics".to_string(),
        }
    }

    /// 记录缓存命中
    pub async fn record_cache_hit(&self, path: &str) {
        let mut cache_hit_count = self.cache_hit_count.write().await;
        *cache_hit_count.entry(path.to_string()).or_insert(0) += 1;
    }

    /// 记录缓存未命中
    pub async fn record_cache_miss(&self, path: &str) {
        let mut cache_miss_count = self.cache_miss_count.write().await;
        *cache_miss_count.entry(path.to_string()).or_insert(0) += 1;
    }

    /// 更新缓存大小
    pub async fn update_cache_size(&self, path: &str, size: u64) {
        let mut cache_size = self.cache_size.write().await;
        cache_size.insert(path.to_string(), size);
    }
}

impl MetricCollector for CacheMetricsCollector {
    fn collect(&self) -> HashMap<String, f64> {
        let mut metrics = HashMap::new();

        // 获取缓存命中计数
        let cache_hit_count = futures::executor::block_on(async {
            self.cache_hit_count.read().await
        });
        for (key, count) in cache_hit_count.iter() {
            metrics.insert(format!("hits_{}", key), *count as f64);
        }

        // 获取缓存未命中计数
        let cache_miss_count = futures::executor::block_on(async {
            self.cache_miss_count.read().await
        });
        for (key, count) in cache_miss_count.iter() {
            metrics.insert(format!("misses_{}", key), *count as f64);
        }

        // 获取缓存大小
        let cache_size = futures::executor::block_on(async {
            self.cache_size.read().await
        });
        for (key, size) in cache_size.iter() {
            metrics.insert(format!("size_{}", key), *size as f64);
        }

        // 计算缓存命中率
        for key in cache_hit_count.keys().chain(cache_miss_count.keys()) {
            let hits = cache_hit_count.get(key).copied().unwrap_or(0) as f64;
            let misses = cache_miss_count.get(key).copied().unwrap_or(0) as f64;
            let total = hits + misses;

            if total > 0.0 {
                metrics.insert(format!("hit_rate_{}", key), hits / total);
            }
        }

        metrics
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn help(&self) -> &str {
        &self.help
    }
}
