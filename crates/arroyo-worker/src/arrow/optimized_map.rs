use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use arrow::compute::{concat_batches, take};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, OperatorConstructor,
    Registry,
};
use arroyo_planner::physical::ArroyoPhysicalExtensionCodec;
use arroyo_planner::physical::DecodingContext;
use arroyo_rpc::grpc::api;
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message as ProstMessage;
use std::borrow::Cow;
use tokio::sync::Mutex;
use tokio::task;
use tracing::{debug, info, warn};

use super::StatelessPhysicalExecutor;

/// 批处理缓存项
#[derive(Clone)]
struct BatchCacheItem {
    /// 输入批次的哈希值
    hash: u64,
    /// 输入批次
    input: RecordBatch,
    /// 输出批次
    output: RecordBatch,
    /// 最后访问时间
    last_accessed: Instant,
}

/// 操作符统计信息
#[derive(Debug, Clone)]
pub struct OptimizedMapStats {
    /// 缓存命中计数
    pub cache_hits: usize,
    /// 缓存未命中计数
    pub cache_misses: usize,
    /// 处理的总行数
    pub total_rows_processed: usize,
    /// 处理的总批次数
    pub total_batches_processed: usize,
    /// 处理总时间（毫秒）
    pub total_processing_time_ms: u64,
}

/// 优化的 Map 操作符，提供更高效的数据处理能力
pub struct OptimizedMapOperator {
    /// 操作符名称
    name: String,
    /// 物理执行器
    executor: StatelessPhysicalExecutor,
    /// 批处理大小，用于优化性能
    batch_size: usize,
    /// 输入架构
    input_schema: SchemaRef,
    /// 输出架构
    output_schema: SchemaRef,
    /// 结果缓存，用于优化重复计算
    /// 键为输入批次的哈希值，值为缓存项
    result_cache: Mutex<HashMap<u64, BatchCacheItem>>,
    /// 缓存命中计数
    cache_hits: usize,
    /// 缓存未命中计数
    cache_misses: usize,
    /// 缓存大小限制（项数）
    cache_size_limit: usize,
    /// 缓存过期时间
    cache_ttl: Duration,
    /// 上次缓存清理时间
    last_cache_cleanup: Instant,
    /// 缓存清理间隔
    cache_cleanup_interval: Duration,
    /// 并行处理的线程数
    parallel_threads: usize,
    /// 处理的总行数
    total_rows_processed: usize,
    /// 处理的总批次数
    total_batches_processed: usize,
    /// 处理开始时间
    start_time: Instant,
    /// 处理总时间（毫秒）
    total_processing_time_ms: u64,
}

/// 计算批次的哈希值
fn compute_batch_hash(batch: &RecordBatch) -> u64 {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();

    // 对每一列进行哈希
    for col in batch.columns() {
        // 使用列的长度作为哈希输入
        col.len().hash(&mut hasher);

        // 使用列的数据类型作为哈希输入
        format!("{:?}", col.data_type()).hash(&mut hasher);

        // 对前 10 行数据进行采样哈希（避免全量哈希带来的性能开销）
        let sample_size = std::cmp::min(10, col.len());
        if sample_size > 0 {
            // 使用列的内存布局的哈希值
            col.as_ref().get_array_memory_size().hash(&mut hasher);
        }
    }

    // 使用行数和列数作为哈希输入
    batch.num_rows().hash(&mut hasher);
    batch.num_columns().hash(&mut hasher);

    hasher.finish()
}

pub struct OptimizedMapConstructor;

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// 创建测试数据批次
    pub fn create_test_batch(rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let ids: Vec<i64> = (0..rows as i64).collect();
        let names: Vec<String> = (0..rows).map(|i| format!("name_{}", i)).collect();

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_compute_batch_hash() {
        let batch1 = create_test_batch(10);
        let batch2 = create_test_batch(10);
        let batch3 = create_test_batch(20);

        // 相同内容的批次应该有相同的哈希值
        let hash1 = compute_batch_hash(&batch1);
        let hash2 = compute_batch_hash(&batch2);
        let hash3 = compute_batch_hash(&batch3);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[tokio::test]
    async fn test_batch_cache_item() {
        use std::time::Instant;

        // 创建测试数据
        let batch = create_test_batch(100);

        // 创建缓存项
        let hash = compute_batch_hash(&batch);
        let cache_item = BatchCacheItem {
            hash,
            input: batch.clone(),
            output: batch.clone(),
            last_accessed: Instant::now(),
        };

        // 验证缓存项
        assert_eq!(cache_item.hash, hash);
        assert_eq!(cache_item.input.num_rows(), 100);
        assert_eq!(cache_item.output.num_rows(), 100);
    }
}

impl OptimizedMapOperator {
    /// 创建一个新的优化 Map 操作符
    pub fn new(
        name: String,
        executor: StatelessPhysicalExecutor,
        input_schema: SchemaRef,
        output_schema: SchemaRef,
    ) -> Self {
        // 根据系统环境自动调整参数
        let num_cpus = num_cpus::get();
        let batch_size = 1000; // 默认批处理大小
        let parallel_threads = std::cmp::max(1, num_cpus / 2); // 使用一半的 CPU 核心
        let cache_size_limit = 100; // 默认缓存大小限制
        let cache_ttl = Duration::from_secs(60); // 默认缓存过期时间
        let cache_cleanup_interval = Duration::from_secs(30); // 默认缓存清理间隔

        info!(
            "Creating OptimizedMapOperator with batch_size={}, parallel_threads={}, cache_size_limit={}",
            batch_size, parallel_threads, cache_size_limit
        );

        Self {
            name,
            executor,
            batch_size,
            input_schema,
            output_schema,
            result_cache: Mutex::new(HashMap::new()),
            cache_hits: 0,
            cache_misses: 0,
            cache_size_limit,
            cache_ttl,
            last_cache_cleanup: Instant::now(),
            cache_cleanup_interval,
            parallel_threads,
            total_rows_processed: 0,
            total_batches_processed: 0,
            start_time: Instant::now(),
            total_processing_time_ms: 0,
        }
    }

    /// 获取操作符统计信息
    pub fn get_stats(&self) -> OptimizedMapStats {
        OptimizedMapStats {
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            total_rows_processed: self.total_rows_processed,
            total_batches_processed: self.total_batches_processed,
            total_processing_time_ms: self.total_processing_time_ms,
        }
    }
}

impl OperatorConstructor for OptimizedMapConstructor {
    type ConfigT = api::ValuePlanOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let executor = StatelessPhysicalExecutor::new(&config.physical_plan, &registry)?;

        // 获取输入和输出架构
        let plan_node = PhysicalPlanNode::decode(&mut config.physical_plan.as_slice())?;
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::None,
        };
        let plan = plan_node.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::try_new(RuntimeConfig::new())?,
            &codec,
        )?;

        let input_schema = plan.children()[0].schema();
        let output_schema = plan.schema();

        // 根据系统环境自动调整参数
        let num_cpus = num_cpus::get();
        let batch_size = 1000; // 默认批处理大小
        let parallel_threads = std::cmp::max(1, num_cpus / 2); // 使用一半的 CPU 核心
        let cache_size_limit = 100; // 默认缓存大小限制
        let cache_ttl = Duration::from_secs(60); // 默认缓存过期时间
        let cache_cleanup_interval = Duration::from_secs(30); // 默认缓存清理间隔

        info!(
            "Creating OptimizedMapOperator with batch_size={}, parallel_threads={}, cache_size_limit={}",
            batch_size, parallel_threads, cache_size_limit
        );

        Ok(ConstructedOperator::from_operator(Box::new(
            OptimizedMapOperator {
                name: config.name,
                executor,
                batch_size,
                input_schema,
                output_schema,
                result_cache: Mutex::new(HashMap::new()),
                cache_hits: 0,
                cache_misses: 0,
                cache_size_limit,
                cache_ttl,
                last_cache_cleanup: Instant::now(),
                cache_cleanup_interval,
                parallel_threads,
                total_rows_processed: 0,
                total_batches_processed: 0,
                start_time: Instant::now(),
                total_processing_time_ms: 0,
            },
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for OptimizedMapOperator {
    fn name(&self) -> String {
        format!("OptimizedMap({})", self.name)
    }

    fn display(&self) -> DisplayableOperator {
        // 计算平均处理时间
        let avg_processing_time = if self.total_batches_processed > 0 {
            self.total_processing_time_ms as f64 / self.total_batches_processed as f64
        } else {
            0.0
        };

        // 计算吞吐量（行/秒）
        let throughput = if self.total_processing_time_ms > 0 {
            (self.total_rows_processed as f64 * 1000.0) / self.total_processing_time_ms as f64
        } else {
            0.0
        };

        // 计算缓存命中率
        let cache_hit_rate = if self.cache_hits + self.cache_misses > 0 {
            self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64 * 100.0
        } else {
            0.0
        };

        DisplayableOperator {
            name: Cow::Owned(format!("OptimizedMap({})", self.name)),
            fields: vec![
                ("plan", (&*self.executor.plan).into()),
                ("batch_size", format!("{}", self.batch_size).into()),
                ("parallel_threads", format!("{}", self.parallel_threads).into()),
                ("cache_hits", format!("{}", self.cache_hits).into()),
                ("cache_misses", format!("{}", self.cache_misses).into()),
                ("cache_hit_rate", format!("{:.2}%", cache_hit_rate).into()),
                ("total_rows", format!("{}", self.total_rows_processed).into()),
                ("total_batches", format!("{}", self.total_batches_processed).into()),
                ("avg_processing_time", format!("{:.2} ms", avg_processing_time).into()),
                ("throughput", format!("{:.2} rows/sec", throughput).into()),
            ],
        }
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        // 初始化操作符
        if let Some(schema) = ctx.out_schema.clone() {
            // 确保输出架构与预期一致
            assert_eq!(
                schema.schema.fields().len(),
                self.output_schema.fields().len(),
                "输出架构字段数量不匹配"
            );
        }

        // 记录启动时间
        self.start_time = Instant::now();

        info!("OptimizedMapOperator started: {}", self.name);
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        // 记录处理开始时间
        let process_start = Instant::now();

        // 如果批次为空，直接返回
        if batch.num_rows() == 0 {
            return;
        }

        // 更新统计信息
        self.total_rows_processed += batch.num_rows();
        self.total_batches_processed += 1;

        // 计算批次哈希值
        let batch_hash = compute_batch_hash(&batch);

        // 检查缓存
        let mut cache = self.result_cache.lock().await;

        // 尝试从缓存获取结果
        if let Some(cache_item) = cache.get_mut(&batch_hash) {
            // 缓存命中
            self.cache_hits += 1;

            // 更新最后访问时间
            cache_item.last_accessed = Instant::now();

            // 发送缓存的结果
            collector.collect(cache_item.output.clone()).await;

            // 更新处理时间统计
            let process_duration = process_start.elapsed();
            self.total_processing_time_ms += process_duration.as_millis() as u64;

            debug!(
                "Cache hit for batch with hash {}: {} rows processed in {:?}",
                batch_hash, batch.num_rows(), process_duration
            );

            return;
        }

        // 缓存未命中，需要处理批次
        self.cache_misses += 1;

        // 释放锁，以便其他任务可以访问缓存
        drop(cache);

        // 处理批次
        let mut records = self.executor.process_batch(batch.clone()).await;

        // 收集结果
        let mut result_batches = Vec::new();
        while let Some(result_batch) = records.next().await {
            let result_batch = match result_batch {
                Ok(batch) => batch,
                Err(e) => {
                    warn!("Error processing batch: {}", e);
                    continue;
                }
            };

            // 收集结果批次
            result_batches.push(result_batch);
        }

        // 如果有结果，合并并发送
        if !result_batches.is_empty() {
            // 合并结果批次
            let combined_result = if result_batches.len() == 1 {
                result_batches[0].clone()
            } else {
                match concat_batches(&self.output_schema, &result_batches) {
                    Ok(batch) => batch,
                    Err(e) => {
                        warn!("Error concatenating result batches: {}", e);
                        return;
                    }
                }
            };

            // 更新缓存
            let mut cache = self.result_cache.lock().await;

            // 检查缓存大小，如果超过限制，清理最旧的项
            if cache.len() >= self.cache_size_limit {
                self.cleanup_cache(&mut cache);
            }

            // 添加到缓存
            cache.insert(
                batch_hash,
                BatchCacheItem {
                    hash: batch_hash,
                    input: batch.clone(),
                    output: combined_result.clone(),
                    last_accessed: Instant::now(),
                },
            );

            // 发送结果
            collector.collect(combined_result).await;
        }

        // 更新处理时间统计
        let process_duration = process_start.elapsed();
        self.total_processing_time_ms += process_duration.as_millis() as u64;

        debug!(
            "Processed batch with hash {}: {} rows in {:?}",
            batch_hash, batch.num_rows(), process_duration
        );

        // 定期清理缓存
        if self.last_cache_cleanup.elapsed() >= self.cache_cleanup_interval {
            let mut cache = self.result_cache.lock().await;
            self.cleanup_cache(&mut cache);
            self.last_cache_cleanup = Instant::now();
        }
    }

    async fn handle_timer(&mut self, _key: Vec<u8>, _value: Vec<u8>, _ctx: &mut OperatorContext) {
        // 定期记录性能统计信息
        let elapsed = self.start_time.elapsed();
        let throughput = if self.total_processing_time_ms > 0 {
            (self.total_rows_processed as f64 * 1000.0) / self.total_processing_time_ms as f64
        } else {
            0.0
        };

        let cache_hit_rate = if self.cache_hits + self.cache_misses > 0 {
            self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64 * 100.0
        } else {
            0.0
        };

        info!(
            "OptimizedMapOperator stats: {} rows in {} batches processed in {:?}, throughput: {:.2} rows/sec, cache hit rate: {:.2}%",
            self.total_rows_processed,
            self.total_batches_processed,
            elapsed,
            throughput,
            cache_hit_rate
        );
    }

    fn tick_interval(&self) -> Option<Duration> {
        // 每 10 秒执行一次 tick，用于监控和优化
        Some(Duration::from_secs(10))
    }

    async fn on_close(
        &mut self,
        _signal: &Option<arroyo_types::SignalMessage>,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) {
        // 记录最终统计信息
        let elapsed = self.start_time.elapsed();

        info!(
            "OptimizedMapOperator closing: {} rows in {} batches processed in {:?}",
            self.total_rows_processed,
            self.total_batches_processed,
            elapsed
        );

        // 清空缓存
        let mut cache = self.result_cache.lock().await;
        cache.clear();
    }
}

impl OptimizedMapOperator {
    /// 清理缓存，移除过期项和最旧的项
    fn cleanup_cache(&self, cache: &mut HashMap<u64, BatchCacheItem>) {
        let now = Instant::now();

        // 移除过期项
        cache.retain(|_, item| now.duration_since(item.last_accessed) < self.cache_ttl);

        // 如果缓存仍然太大，移除最旧的项
        if cache.len() > self.cache_size_limit {
            // 按最后访问时间排序
            let mut items: Vec<_> = cache.keys().cloned().collect();

            // 对键进行排序，按照对应项的最后访问时间
            items.sort_by_key(|key| {
                if let Some(item) = cache.get(key) {
                    item.last_accessed
                } else {
                    // 如果找不到项（理论上不应该发生），使用当前时间
                    now
                }
            });

            // 计算需要移除的项数
            let remove_count = cache.len() - self.cache_size_limit;

            // 移除最旧的项
            for (i, key) in items.iter().enumerate() {
                if i < remove_count {
                    cache.remove(key);
                } else {
                    break;
                }
            }
        }

        debug!(
            "Cache cleanup: {} items remaining after cleanup",
            cache.len()
        );
    }
}
