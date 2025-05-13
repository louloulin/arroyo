use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Result};
use arrow::compute::{sort_to_indices, take};
use arrow_array::{Array, PrimitiveArray, RecordBatch};
use arrow_array::types::TimestampNanosecondType;
use arrow_schema::SchemaRef;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{ArrowOperator, ConstructedOperator, OperatorConstructor, Registry};
use arroyo_planner::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_planner::schemas::add_timestamp_field_arrow;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::{api, rpc::TableConfig};
use arroyo_state::timestamp_table_config;
use arroyo_types::{from_nanos, to_nanos, CheckpointBarrier, Watermark};
use datafusion::arrow::compute::partition;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use futures::{stream::FuturesUnordered, StreamExt};
use prost::Message;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use super::sync::streams::KeyedCloneableStreamFuture;
use super::utils::SendableRecordBatchStream;

type NextBatchFuture<K> = KeyedCloneableStreamFuture<K, SendableRecordBatchStream>;

/// 优化的滚动窗口实现，提供更高效的窗口聚合功能
pub struct OptimizedTumblingWindowFunc<K: Copy> {
    /// 窗口宽度
    width: Duration,
    /// 用于计算数据所属窗口的函数
    binning_function: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    /// 部分聚合计划
    partial_aggregation_plan: Arc<dyn ExecutionPlan>,
    /// 部分聚合结果的模式
    partial_schema: ArroyoSchema,
    /// 最终聚合计划
    finish_execution_plan: Arc<dyn ExecutionPlan>,
    /// 带时间戳的聚合结果模式
    aggregate_with_timestamp_schema: SchemaRef,
    /// 最终投影计划（可选）
    final_projection: Option<Arc<dyn ExecutionPlan>>,
    /// 接收器，用于接收数据
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    /// 最终批次传递器
    final_batches_passer: Arc<RwLock<Vec<RecordBatch>>>,
    /// 异步计算的 Future 集合
    futures: Arc<Mutex<FuturesUnordered<NextBatchFuture<K>>>>,
    /// 按窗口开始时间索引的计算持有者
    execs: BTreeMap<K, BinComputingHolder<K>>,
    /// 窗口结果缓存，用于避免重复计算
    window_cache: Arc<Mutex<HashMap<K, Vec<RecordBatch>>>>,
    /// 缓存命中计数器，用于监控缓存效率
    cache_hits: usize,
    /// 缓存未命中计数器，用于监控缓存效率
    cache_misses: usize,
}

/// 窗口计算持有者，存储窗口计算的中间状态
struct BinComputingHolder<K: Copy> {
    /// 活动的执行计划
    active_exec: Option<NextBatchFuture<K>>,
    /// 发送器，用于发送数据到执行计划
    sender: Option<UnboundedSender<RecordBatch>>,
    /// 已完成的批次
    finished_batches: Vec<RecordBatch>,
}

impl<K: Copy> Default for BinComputingHolder<K> {
    fn default() -> Self {
        Self {
            active_exec: None,
            sender: None,
            finished_batches: Vec::new(),
        }
    }
}

impl<K: Copy + std::hash::Hash + Eq + Send + Sync + 'static + std::cmp::Ord> OptimizedTumblingWindowFunc<K> {
    /// 创建一个新的优化滚动窗口函数
    pub fn new(
        width: Duration,
        binning_function: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        partial_aggregation_plan: Arc<dyn ExecutionPlan>,
        partial_schema: ArroyoSchema,
        finish_execution_plan: Arc<dyn ExecutionPlan>,
        aggregate_with_timestamp_schema: SchemaRef,
        final_projection: Option<Arc<dyn ExecutionPlan>>,
        receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
        final_batches_passer: Arc<RwLock<Vec<RecordBatch>>>,
    ) -> Self {
        Self {
            width,
            binning_function,
            partial_aggregation_plan,
            partial_schema,
            finish_execution_plan,
            aggregate_with_timestamp_schema,
            final_projection,
            receiver,
            final_batches_passer,
            futures: Arc::new(Mutex::new(FuturesUnordered::new())),
            execs: BTreeMap::new(),
            window_cache: Arc::new(Mutex::new(HashMap::new())),
            cache_hits: 0,
            cache_misses: 0,
        }
    }

    /// 计算时间戳所属的窗口开始时间
    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        let nanos = to_nanos(timestamp);
        let width_nanos = self.width.as_nanos() as u64;
        let bin_start_nanos = (nanos / width_nanos as u128) * (width_nanos as u128);
        from_nanos(bin_start_nanos)
    }

    /// 添加时间戳字段到批次
    fn add_bin_start_as_timestamp(
        batch: &RecordBatch,
        bin_start: SystemTime,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let bin_start_nanos = to_nanos(bin_start);
        let timestamp_array = PrimitiveArray::<TimestampNanosecondType>::from_value(
            bin_start_nanos.try_into().unwrap(),
            batch.num_rows(),
        );

        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(timestamp_array));

        RecordBatch::try_new(schema, columns).map_err(|e| anyhow!(e))
    }

    /// 从缓存中获取窗口结果，如果缓存中没有则计算
    async fn get_window_result(&mut self, bin: K) -> Result<Vec<RecordBatch>> {
        let mut cache = self.window_cache.lock().await;
        if let Some(result) = cache.get(&bin) {
            self.cache_hits += 1;
            return Ok(result.clone());
        }

        self.cache_misses += 1;

        // 缓存中没有结果，需要计算
        let holder = self.execs.get_mut(&bin).ok_or_else(|| anyhow!("No holder for bin"))?;
        let result = holder.finished_batches.clone();

        // 将结果放入缓存
        cache.insert(bin, result.clone());

        Ok(result)
    }

    /// 批量处理数据，减少计算次数
    async fn batch_process(&mut self, batches: Vec<RecordBatch>) -> Result<()> {
        // 按窗口分组批次
        let mut batches_by_bin: HashMap<K, Vec<RecordBatch>> = HashMap::new();

        for batch in batches {
            let bin = self
                .binning_function
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();

            let indices = sort_to_indices(bin.as_ref(), None, None).unwrap();
            let columns = batch
                .columns()
                .iter()
                .map(|c| take(c, &indices, None).unwrap())
                .collect();

            let sorted = RecordBatch::try_new(batch.schema(), columns).unwrap();
            let sorted_bins = take(&*bin, &indices, None).unwrap();

            let partition = partition(vec![sorted_bins.clone()].as_slice()).unwrap();
            let typed_bin = sorted_bins
                .as_any()
                .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
                .unwrap();

            for range in partition.ranges() {
                if range.end <= range.start {
                    continue;
                }

                let bin_value = typed_bin.value(range.start);
                let bin_start = from_nanos(bin_value as u128);
                let bin_key = unsafe { std::mem::transmute_copy(&bin_start) };

                let bin_batch = sorted.slice(range.start, range.end - range.start);
                batches_by_bin.entry(bin_key).or_default().push(bin_batch);
            }
        }

        // 处理每个窗口的批次
        for (bin, bin_batches) in batches_by_bin {
            let bin_exec = self.execs.entry(bin).or_default();

            // 如果没有活动的执行计划，创建一个
            if bin_exec.active_exec.is_none() {
                let (unbounded_sender, unbounded_receiver) = unbounded_channel();
                bin_exec.sender = Some(unbounded_sender);
                {
                    let mut internal_receiver = self.receiver.write().unwrap();
                    *internal_receiver = Some(unbounded_receiver);
                }

                self.partial_aggregation_plan.reset().unwrap();
                let new_exec = self
                    .partial_aggregation_plan
                    .execute(0, SessionContext::new().task_ctx())
                    .unwrap();

                let next_batch_future = NextBatchFuture::new(bin, new_exec);
                self.futures.lock().await.push(next_batch_future.clone());
                bin_exec.active_exec = Some(next_batch_future);
            }

            // 发送批次到执行计划
            if let Some(sender) = &bin_exec.sender {
                for batch in bin_batches {
                    sender.send(batch).unwrap();
                }
            }
        }

        Ok(())
    }

    /// 获取缓存统计信息
    pub fn get_cache_stats(&self) -> (usize, usize) {
        (self.cache_hits, self.cache_misses)
    }

    /// 清除缓存
    pub async fn clear_cache(&mut self) {
        let mut cache = self.window_cache.lock().await;
        cache.clear();
        self.cache_hits = 0;
        self.cache_misses = 0;
    }
}

#[async_trait::async_trait]
impl<K: Copy + std::hash::Hash + Eq + Send + Sync + 'static + std::cmp::Ord> ArrowOperator for OptimizedTumblingWindowFunc<K> {
    fn name(&self) -> String {
        "OptimizedTumblingWindow".to_string()
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        let watermark = ctx.last_present_watermark();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", watermark)
            .await
            .expect("should be able to load table");

        // 从表中加载所有批次
        for (timestamp, batch) in table.all_batches_for_watermark(watermark) {
            let bin = self.bin_start(*timestamp);
            let bin_key = unsafe { std::mem::transmute_copy(&bin) };
            let holder = self.execs.entry(bin_key).or_default();
            batch
                .iter()
                .for_each(|batch| holder.finished_batches.push(batch.clone()));
        }

        // 记录启动时的缓存统计
        debug!("OptimizedTumblingWindow started with {} existing bins", self.execs.len());
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // 批量处理数据
        self.batch_process(vec![batch]).await.unwrap();

        // 处理完成的计算
        let mut futures = self.futures.lock().await;
        let mut completed = Vec::new();

        while let Some((bin, result)) = futures.next().now_or_never() {
            if let Some((batch_result, next_exec)) = result {
                let batch = batch_result.expect("should be able to compute batch");
                let bin_start = unsafe { std::mem::transmute_copy(&bin) };

                // 将结果保存到状态
                let table = ctx
                    .table_manager
                    .get_expiring_time_key_table("t", ctx.last_present_watermark())
                    .await
                    .expect("should be able to load table");

                let state_batch = Self::add_bin_start_as_timestamp(
                    &batch,
                    bin_start,
                    self.partial_schema.schema.clone(),
                )
                .expect("should be able to add timestamp");

                table.insert(bin_start, state_batch);

                // 保存完成的批次
                let exec = self.execs.get_mut(&bin).unwrap();
                exec.finished_batches.push(batch);

                // 继续处理下一个批次
                exec.active_exec = Some(next_exec.clone());
                completed.push(next_exec);
            }
        }

        // 将完成的计算添加回 futures
        for exec in completed {
            futures.push(exec);
        }
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Option<Watermark> {
        // 处理水印
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", ctx.last_present_watermark())
            .await
            .expect("should be able to load table");

        // 处理所有活动的执行计划
        let mut futures = self.futures.lock().await;
        let mut completed_bins = Vec::new();

        for (bin, exec) in &mut self.execs {
            if let Some(active_exec) = exec.active_exec.take() {
                let mut current_exec = active_exec;

                while let Some((batch_result, next_exec)) = current_exec.now_or_never() {
                    current_exec = next_exec;

                    if let Some(batch) = batch_result {
                        let batch = batch.expect("should be able to compute batch");
                        let bin_start = unsafe { std::mem::transmute_copy(bin) };

                        let state_batch = Self::add_bin_start_as_timestamp(
                            &batch,
                            bin_start,
                            self.partial_schema.schema.clone(),
                        )
                        .expect("should be able to add timestamp");

                        table.insert(bin_start, state_batch);
                        exec.finished_batches.push(batch);
                    } else {
                        break;
                    }
                }

                // 如果发送器已关闭，标记为已完成
                if exec.sender.as_ref().map_or(true, |s| s.is_closed()) {
                    completed_bins.push(*bin);
                } else {
                    exec.active_exec = Some(current_exec);
                }
            }
        }

        // 移除已完成的窗口
        for bin in completed_bins {
            if let Some(exec) = self.execs.remove(&bin) {
                // 发送最终结果
                if !exec.finished_batches.is_empty() {
                    let bin_start = unsafe { std::mem::transmute_copy(&bin) };

                    // 计算最终结果
                    let mut final_batches = self.final_batches_passer.write().unwrap();
                    final_batches.clear();
                    final_batches.extend(exec.finished_batches);

                    // 执行最终聚合
                    let final_result = self.finish_execution_plan
                        .execute(0, SessionContext::new().task_ctx())
                        .unwrap()
                        .collect()
                        .await
                        .unwrap();

                    // 应用最终投影（如果有）
                    let final_result = if let Some(final_projection) = &self.final_projection {
                        final_projection
                            .execute(0, SessionContext::new().task_ctx())
                            .unwrap()
                            .collect()
                            .await
                            .unwrap()
                    } else {
                        final_result
                    };

                    // 发送结果
                    for batch in final_result {
                        let batch_with_window = Self::add_bin_start_as_timestamp(
                            &batch,
                            bin_start,
                            self.aggregate_with_timestamp_schema.clone(),
                        )
                        .expect("should be able to add timestamp");

                        collector.collect(batch_with_window).await;
                    }
                }
            }
        }

        // 刷新表
        if let Watermark::Watermark(timestamp) = watermark {
            table.flush(Some(timestamp)).await.unwrap();
        }

        // 记录缓存统计
        let (hits, misses) = self.get_cache_stats();
        if hits + misses > 0 {
            debug!(
                "Window cache stats: {} hits, {} misses, hit rate: {:.2}%",
                hits,
                misses,
                (hits as f64 / (hits + misses) as f64) * 100.0
            );
        }

        Some(watermark)
    }

    async fn handle_checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // 处理检查点
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", ctx.last_present_watermark())
            .await
            .expect("should be able to load table");

        table.flush(ctx.last_present_watermark()).await.unwrap();

        // 清除过期的缓存项
        self.clear_cache().await;

        debug!("Checkpoint {} completed, cache cleared", barrier.epoch);
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = HashMap::new();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![]));
        let arroyo_schema = ArroyoSchema::new_unkeyed(schema, 0);
        tables.insert("t".to_string(), timestamp_table_config("t", "Timestamp table for window", self.width * 3, false, arroyo_schema));
        tables
    }
}

/// 优化滚动窗口构造器
pub struct OptimizedTumblingWindowConstructor;

impl OperatorConstructor for OptimizedTumblingWindowConstructor {
    type ConfigT = api::TumblingWindowAggregateOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        // 解析窗口宽度
        let width = Duration::from_micros(config.width_micros);

        // 解析时间字段表达式
        let binning_function = PhysicalExprNode::decode(&mut config.binning_function.as_slice())?
            .try_into_physical_expr(registry.as_ref())?;

        // 创建接收器和批次传递器
        let receiver = Arc::new(RwLock::new(None));
        let final_batches_passer = Arc::new(RwLock::new(Vec::new()));

        // 创建编解码器
        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver.clone()),
        };

        // 解析部分聚合计划
        let partial_aggregation_plan =
            PhysicalPlanNode::decode(&mut config.partial_aggregation_plan.as_slice())?;

        // 将部分聚合计划转换为执行计划
        let partial_aggregation_plan = partial_aggregation_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::try_new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        // 解析部分聚合模式
        let partial_schema = config
            .partial_schema
            .ok_or_else(|| anyhow!("requires partial schema"))?
            .try_into()?;

        // 解析最终聚合计划
        let finish_plan = PhysicalPlanNode::decode(&mut config.final_aggregation_plan.as_slice())?;

        // 创建最终编解码器
        let final_codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::LockedBatchVec(final_batches_passer.clone()),
        };

        // 将最终聚合计划转换为执行计划
        let finish_execution_plan = finish_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::try_new(RuntimeConfig::new()).unwrap(),
            &final_codec,
        )?;

        // 解析最终投影计划（如果有）
        let finish_projection = config
            .final_projection
            .map(|proto| PhysicalPlanNode::decode(&mut proto.as_slice()))
            .transpose()?;

        // 将最终投影计划转换为执行计划（如果有）
        let final_projection_plan = finish_projection
            .map(|finish_projection| {
                finish_projection.try_into_physical_plan(
                    registry.as_ref(),
                    &RuntimeEnv::try_new(RuntimeConfig::new()).unwrap(),
                    &final_codec,
                )
            })
            .transpose()?;

        // 创建带时间戳的聚合结果模式
        let aggregate_with_timestamp_schema =
            add_timestamp_field_arrow((*finish_execution_plan.schema()).clone());

        // 创建优化滚动窗口函数
        let window_func = OptimizedTumblingWindowFunc::<SystemTime>::new(
            width,
            binning_function,
            partial_aggregation_plan,
            partial_schema,
            finish_execution_plan,
            aggregate_with_timestamp_schema,
            final_projection_plan,
            receiver,
            final_batches_passer,
        );

        // 返回构造的操作符
        Ok(ConstructedOperator::from_operator(Box::new(window_func)))
    }
}
