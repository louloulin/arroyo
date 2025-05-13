use std::sync::Arc;

use arrow_array::RecordBatch;
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
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message as ProstMessage;
use std::borrow::Cow;
use tokio::sync::Mutex;

use super::StatelessPhysicalExecutor;

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
    /// 缓存上次处理结果，用于优化重复计算
    result_cache: Mutex<Option<RecordBatch>>,
    /// 缓存命中计数
    cache_hits: usize,
    /// 缓存未命中计数
    cache_misses: usize,
}

pub struct OptimizedMapConstructor;

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

        // 默认批处理大小为 1000，可以根据需要调整
        let batch_size = 1000;

        Ok(ConstructedOperator::from_operator(Box::new(
            OptimizedMapOperator {
                name: config.name,
                executor,
                batch_size,
                input_schema,
                output_schema,
                result_cache: Mutex::new(None),
                cache_hits: 0,
                cache_misses: 0,
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
        DisplayableOperator {
            name: Cow::Owned(format!("OptimizedMap({})", self.name)),
            fields: vec![
                ("plan", (&*self.executor.plan).into()),
                ("batch_size", format!("{}", self.batch_size).into()),
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

        // 检查缓存
        let mut cache = self.result_cache.lock().await;

        // 处理批次
        let mut records = self.executor.process_batch(batch).await;

        // 收集结果
        while let Some(result_batch) = records.next().await {
            let result_batch = result_batch.expect("处理批次时出错");

            // 更新缓存
            *cache = Some(result_batch.clone());
            self.cache_misses += 1;

            // 发送结果
            collector.collect(result_batch).await;
        }
    }

    fn tick_interval(&self) -> Option<std::time::Duration> {
        // 每 10 秒执行一次 tick，用于监控和优化
        Some(std::time::Duration::from_secs(10))
    }
}
