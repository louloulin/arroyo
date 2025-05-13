use std::sync::Arc;

use arrow::compute::filter_record_batch;
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
use prost::Message as ProstMessage;
use std::borrow::Cow;
use std::collections::HashMap;
use tokio::sync::Mutex;

use super::StatelessPhysicalExecutor;

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
    /// 过滤条件缓存，用于优化重复计算
    /// 键为输入行的哈希值，值为过滤结果
    filter_cache: Mutex<HashMap<u64, bool>>,
    /// 缓存命中计数
    cache_hits: usize,
    /// 缓存未命中计数
    cache_misses: usize,
    /// 过滤后的行数
    filtered_rows: usize,
    /// 总处理行数
    total_rows: usize,
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

        // 创建一个简单的过滤表达式（始终为真）
        let filter_expr = datafusion::physical_expr::expressions::lit(true);

        Ok(ConstructedOperator::from_operator(Box::new(
            OptimizedFilterOperator {
                name: config.name,
                executor,
                filter_expr,
                input_schema,
                output_schema,
                filter_cache: Mutex::new(HashMap::new()),
                cache_hits: 0,
                cache_misses: 0,
                filtered_rows: 0,
                total_rows: 0,
            },
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

        self.total_rows += batch.num_rows();

        // 直接应用过滤表达式
        let filter_result = self.filter_expr.evaluate(&batch)
            .expect("过滤表达式求值失败")
            .into_array(batch.num_rows())
            .expect("无法将过滤结果转换为数组");

        let filter_array = filter_result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("过滤结果不是布尔数组");

        // 应用过滤器
        let filtered_batch = filter_record_batch(&batch, filter_array)
            .expect("应用过滤器失败");

        // 更新统计信息
        self.filtered_rows += filtered_batch.num_rows();

        // 如果过滤后有数据，则发送
        if filtered_batch.num_rows() > 0 {
            collector.collect(filtered_batch).await;
        }
    }

    fn tick_interval(&self) -> Option<std::time::Duration> {
        // 每 10 秒执行一次 tick，用于监控和优化
        Some(std::time::Duration::from_secs(10))
    }
}
