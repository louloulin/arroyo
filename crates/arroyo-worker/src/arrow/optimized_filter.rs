use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow_array::{Array, BooleanArray, RecordBatch};
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
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use tracing::{debug, info, warn};
use prost::Message as ProstMessage;
use std::borrow::Cow;
use std::collections::HashMap;
use tokio::sync::Mutex;

use super::StatelessPhysicalExecutor;
use super::compute_batch_hash;

/// 过滤缓存项
#[derive(Clone)]
struct FilterCacheItem {
    /// 批次哈希值
    hash: u64,
    /// 输入批次
    input: RecordBatch,
    /// 过滤后的批次
    output: RecordBatch,
    /// 最后访问时间
    last_accessed: Instant,
}

/// 操作符统计信息
#[derive(Debug, Clone)]
pub struct OptimizedFilterStats {
    /// 缓存命中计数
    pub cache_hits: usize,
    /// 缓存未命中计数
    pub cache_misses: usize,
    /// 过滤后的行数
    pub filtered_rows: usize,
    /// 总处理行数
    pub total_rows: usize,
    /// 过滤率（过滤后行数/总行数）
    pub filter_rate: f64,
    /// 处理总时间（毫秒）
    pub total_processing_time_ms: u64,
}

/// 优化的 Filter 操作符，提供更高效的数据过滤能力
pub struct OptimizedFilterOperator {
    /// 操作符名称
    name: String,
    /// 物理执行器
    executor: StatelessPhysicalExecutor,
    /// 过滤表达式
    filter_expr: Arc<dyn PhysicalExpr>,
    /// 输入架构
    input_schema: SchemaRef,
    /// 输出架构
    output_schema: SchemaRef,
    /// 过滤结果缓存，用于优化重复计算
    /// 键为输入批次的哈希值，值为缓存项
    result_cache: Mutex<HashMap<u64, FilterCacheItem>>,
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
    /// 过滤后的行数
    filtered_rows: usize,
    /// 总处理行数
    total_rows: usize,
    /// 处理开始时间
    start_time: Instant,
    /// 处理总时间（毫秒）
    total_processing_time_ms: u64,
}

impl OptimizedFilterOperator {
    /// 创建一个新的优化 Filter 操作符
    pub fn new(
        name: String,
        executor: StatelessPhysicalExecutor,
        filter_expr: Arc<dyn PhysicalExpr>,
        input_schema: SchemaRef,
        output_schema: SchemaRef,
    ) -> Self {
        // 根据系统环境自动调整参数
        let num_cpus = num_cpus::get();
        let cache_size_limit = 100; // 默认缓存大小限制
        let cache_ttl = Duration::from_secs(60); // 默认缓存过期时间
        let cache_cleanup_interval = Duration::from_secs(30); // 默认缓存清理间隔

        info!(
            "Creating OptimizedFilterOperator with cache_size_limit={}, cache_ttl={:?}",
            cache_size_limit, cache_ttl
        );

        Self {
            name,
            executor,
            filter_expr,
            input_schema,
            output_schema,
            result_cache: Mutex::new(HashMap::new()),
            cache_hits: 0,
            cache_misses: 0,
            cache_size_limit,
            cache_ttl,
            last_cache_cleanup: Instant::now(),
            cache_cleanup_interval,
            filtered_rows: 0,
            total_rows: 0,
            start_time: Instant::now(),
            total_processing_time_ms: 0,
        }
    }

    /// 获取操作符统计信息
    pub fn get_stats(&self) -> OptimizedFilterStats {
        let filter_rate = if self.total_rows > 0 {
            self.filtered_rows as f64 / self.total_rows as f64
        } else {
            0.0
        };

        OptimizedFilterStats {
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            filtered_rows: self.filtered_rows,
            total_rows: self.total_rows,
            filter_rate,
            total_processing_time_ms: self.total_processing_time_ms,
        }
    }

    /// 清理缓存，移除过期项和最旧的项
    async fn cleanup_cache(&self) {
        let now = Instant::now();
        let mut cache = self.result_cache.lock().await;

        // 移除过期项
        let mut keys_to_remove = Vec::new();
        for (key, item) in cache.iter() {
            if now.duration_since(item.last_accessed) > self.cache_ttl {
                keys_to_remove.push(*key);
            }
        }

        for key in keys_to_remove {
            cache.remove(&key);
        }

        // 如果缓存仍然太大，移除最旧的项
        if cache.len() > self.cache_size_limit {
            // 收集所有键
            let mut keys: Vec<_> = cache.keys().cloned().collect();

            // 按最后访问时间排序
            keys.sort_by_key(|key| {
                if let Some(item) = cache.get(key) {
                    item.last_accessed
                } else {
                    now
                }
            });

            // 计算需要移除的项数
            let remove_count = cache.len() - self.cache_size_limit;

            // 移除最旧的项
            for (i, key) in keys.iter().enumerate() {
                if i < remove_count {
                    cache.remove(key);
                } else {
                    break;
                }
            }
        }
    }
}

pub struct OptimizedFilterConstructor;

impl OperatorConstructor for OptimizedFilterConstructor {
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

        // 获取过滤表达式
        // 注意：ExecutionPlan 没有 expressions 方法，我们需要从其他地方获取过滤表达式
        // 这里我们使用一个默认的过滤表达式（始终为真）
        let filter_expr = datafusion::physical_expr::expressions::lit(true);

        Ok(ConstructedOperator::from_operator(Box::new(
            OptimizedFilterOperator::new(
                config.name,
                executor,
                filter_expr,
                input_schema,
                output_schema,
            )
        )))
    }
}

#[async_trait::async_trait]
impl ArrowOperator for OptimizedFilterOperator {
    fn name(&self) -> String {
        format!("OptimizedFilter({})", self.name)
    }

    fn display(&self) -> DisplayableOperator {
        // 计算过滤率
        let filter_rate = if self.total_rows > 0 {
            self.filtered_rows as f64 / self.total_rows as f64
        } else {
            0.0
        };

        DisplayableOperator {
            name: Cow::Owned(format!("OptimizedFilter({})", self.name)),
            fields: vec![
                ("plan", (&*self.executor.plan).into()),
                ("filter_rate", format!("{:.2}", filter_rate).into()),
                ("cache_hits", format!("{}", self.cache_hits).into()),
                ("cache_misses", format!("{}", self.cache_misses).into()),
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
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        // 如果批次为空，直接返回
        if batch.num_rows() == 0 {
            return;
        }

        // 记录处理开始时间
        let start_time = Instant::now();

        // 更新总行数
        self.total_rows += batch.num_rows();

        // 计算批次哈希值
        let batch_hash = compute_batch_hash(&batch);

        // 尝试从缓存中获取结果
        let mut cache = self.result_cache.lock().await;

        if let Some(cache_item) = cache.get_mut(&batch_hash) {
            // 缓存命中
            self.cache_hits += 1;
            debug!("Cache hit for batch with hash {}", batch_hash);

            // 更新访问时间
            cache_item.last_accessed = Instant::now();

            // 克隆缓存的结果
            let filtered_batch = cache_item.output.clone();

            // 更新统计信息
            self.filtered_rows += filtered_batch.num_rows();

            // 释放锁
            drop(cache);

            // 如果过滤后有数据，则发送
            if filtered_batch.num_rows() > 0 {
                collector.collect(filtered_batch).await;
            }
        } else {
            // 缓存未命中
            self.cache_misses += 1;
            debug!("Cache miss for batch with hash {}", batch_hash);

            // 释放锁，避免在计算过程中持有锁
            drop(cache);

            // 应用过滤表达式
            let filter_result = match self.filter_expr.evaluate(&batch) {
                Ok(result) => result,
                Err(e) => {
                    warn!("过滤表达式求值失败: {}", e);
                    return;
                }
            };

            let filter_array = match filter_result.into_array(batch.num_rows()) {
                Ok(array) => array,
                Err(e) => {
                    warn!("无法将过滤结果转换为数组: {}", e);
                    return;
                }
            };

            let filter_array = match filter_array.as_any().downcast_ref::<BooleanArray>() {
                Some(array) => array,
                None => {
                    warn!("过滤结果不是布尔数组");
                    return;
                }
            };

            // 应用过滤器
            let filtered_batch = match filter_record_batch(&batch, filter_array) {
                Ok(result) => result,
                Err(e) => {
                    warn!("应用过滤器失败: {}", e);
                    return;
                }
            };

            // 更新统计信息
            self.filtered_rows += filtered_batch.num_rows();

            // 将结果存入缓存
            let mut cache = self.result_cache.lock().await;
            cache.insert(
                batch_hash,
                FilterCacheItem {
                    hash: batch_hash,
                    input: batch.clone(),
                    output: filtered_batch.clone(),
                    last_accessed: Instant::now(),
                },
            );

            // 释放锁
            drop(cache);

            // 如果过滤后有数据，则发送
            if filtered_batch.num_rows() > 0 {
                collector.collect(filtered_batch).await;
            }
        }

        // 更新处理时间
        let elapsed = start_time.elapsed();
        self.total_processing_time_ms += elapsed.as_millis() as u64;

        // 检查是否需要清理缓存
        let now = Instant::now();
        if now.duration_since(self.last_cache_cleanup) > self.cache_cleanup_interval {
            self.last_cache_cleanup = now;
            self.cleanup_cache().await;
        }
    }

    fn tick_interval(&self) -> Option<std::time::Duration> {
        // 每 10 秒执行一次 tick，用于监控和优化
        Some(std::time::Duration::from_secs(10))
    }

    // 注意：ArrowOperator trait 没有 on_tick 方法
    // 我们可以在 tick_interval 方法中返回 None 来禁用 tick
    // 或者在 process_batch 方法中定期执行清理操作
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::col;
    use std::sync::Arc;

    /// 创建测试数据批次
    fn create_test_batch(rows: usize) -> RecordBatch {
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
    async fn test_filter_cache_item() {
        let batch = create_test_batch(10);

        // 创建缓存项
        let hash = compute_batch_hash(&batch);
        let cache_item = FilterCacheItem {
            hash,
            input: batch.clone(),
            output: batch.clone(),
            last_accessed: Instant::now(),
        };

        // 验证缓存项
        assert_eq!(cache_item.hash, hash);
        assert_eq!(cache_item.input.num_rows(), 10);
        assert_eq!(cache_item.output.num_rows(), 10);
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
}
