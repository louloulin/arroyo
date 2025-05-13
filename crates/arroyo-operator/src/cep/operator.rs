use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use tracing::{debug, info, warn};

use crate::context::{Collector, OperatorContext};
use crate::operator::{ArrowOperator, ConstructedOperator, OperatorConstructor};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_types::Watermark;

use super::{CepOperatorConfig, PatternMatchingEngine};

/// 复杂事件处理操作符
pub struct CepOperator {
    /// 模式匹配引擎
    engine: PatternMatchingEngine,
    /// 输入模式
    input_schema: Arc<ArroyoSchema>,
    /// 输出模式
    output_schema: Arc<ArroyoSchema>,
    /// 时间字段名称
    time_field: String,
    /// 时间字段索引
    time_field_index: usize,
    /// 是否输出部分匹配
    output_partial_matches: bool,
}

impl CepOperator {
    /// 创建新的 CEP 操作符
    pub fn new(
        config: CepOperatorConfig,
        input_schema: Arc<ArroyoSchema>,
    ) -> Result<Self> {
        // 查找时间字段索引
        let time_field_index = input_schema
            .schema
            .fields()
            .iter()
            .position(|f| *f.name() == config.time_field)
            .ok_or_else(|| anyhow!("Time field not found: {}", config.time_field))?;

        // 创建模式匹配引擎
        let engine = PatternMatchingEngine::from_json(
            &config.pattern_json,
            time_field_index,
            Duration::from_millis(config.match_timeout_ms),
            config.allow_overlapping,
        )?;

        // 创建输出模式 - 与输入模式相同，但添加了匹配相关字段
        let mut output_fields: Vec<Arc<arrow::datatypes::Field>> = input_schema.schema.fields().to_vec();
        output_fields.push(Arc::new(arrow::datatypes::Field::new(
            "pattern_name",
            arrow::datatypes::DataType::Utf8,
            false,
        )));
        output_fields.push(Arc::new(arrow::datatypes::Field::new(
            "match_start_time",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        )));
        output_fields.push(Arc::new(arrow::datatypes::Field::new(
            "match_end_time",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        )));

        let output_schema_arrow = Arc::new(arrow::datatypes::Schema::new(output_fields));
        let key_indices = input_schema.storage_keys().cloned().unwrap_or_default();
        let output_schema = Arc::new(ArroyoSchema::new_keyed(
            output_schema_arrow,
            input_schema.timestamp_index,
            key_indices,
        ));

        Ok(Self {
            engine,
            input_schema,
            output_schema,
            time_field: config.time_field,
            time_field_index,
            output_partial_matches: config.output_partial_matches,
        })
    }

    /// 将匹配结果转换为记录批次
    fn matches_to_batch(&self, matches: Vec<super::PatternMatch>) -> Result<Option<RecordBatch>> {
        if matches.is_empty() {
            return Ok(None);
        }

        // 简化实现：只输出第一个匹配的第一个事件，添加匹配元数据
        // 实际实现应该合并所有匹配事件并添加匹配元数据

        let first_match = &matches[0];
        let first_event = &first_match.events[0];

        // 创建新的列数组，包括原始事件列和匹配元数据列
        let mut columns = first_event.columns().to_vec();

        // 添加模式名称列
        let mut pattern_name_builder = arrow::array::StringBuilder::new();
        pattern_name_builder.append_value(&first_match.pattern_name);
        columns.push(Arc::new(pattern_name_builder.finish()));

        // 添加匹配开始时间列
        let mut start_time_builder = arrow::array::TimestampNanosecondBuilder::new();
        let start_nanos = arroyo_types::to_nanos(first_match.start_time);
        let start_nanos_i64 = if start_nanos <= i64::MAX as u128 {
            start_nanos as i64
        } else {
            warn!("Start timestamp too large for i64: {}", start_nanos);
            i64::MAX
        };
        start_time_builder.append_value(start_nanos_i64);
        columns.push(Arc::new(start_time_builder.finish()));

        // 添加匹配结束时间列
        let mut end_time_builder = arrow::array::TimestampNanosecondBuilder::new();
        let end_nanos = arroyo_types::to_nanos(first_match.end_time);
        let end_nanos_i64 = if end_nanos <= i64::MAX as u128 {
            end_nanos as i64
        } else {
            warn!("End timestamp too large for i64: {}", end_nanos);
            i64::MAX
        };
        end_time_builder.append_value(end_nanos_i64);
        columns.push(Arc::new(end_time_builder.finish()));

        // 创建新的记录批次
        let batch = RecordBatch::try_new(self.output_schema.schema.clone(), columns)
            .map_err(|e| anyhow!("Failed to create output batch: {}", e))?;

        Ok(Some(batch))
    }
}

#[async_trait]
impl ArrowOperator for CepOperator {
    fn name(&self) -> String {
        "ComplexEventProcessing".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) {
        info!("Starting CEP operator");
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        match self.engine.process_batch(&batch) {
            Ok(matches) => {
                if !matches.is_empty() {
                    debug!("Found {} pattern matches", matches.len());

                    match self.matches_to_batch(matches) {
                        Ok(Some(output_batch)) => {
                            collector.collect(output_batch).await;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!("Failed to convert matches to batch: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Error processing batch in CEP operator: {}", e);
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
            Ok(matches) => {
                if !matches.is_empty() {
                    debug!("Found {} pattern matches on watermark", matches.len());

                    match self.matches_to_batch(matches) {
                        Ok(Some(output_batch)) => {
                            collector.collect(output_batch).await;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!("Failed to convert matches to batch: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Error processing watermark in CEP operator: {}", e);
            }
        }

        Some(watermark)
    }
}

/// CEP 操作符构造器
pub struct CepOperatorConstructor;

impl OperatorConstructor for CepOperatorConstructor {
    type ConfigT = CepOperatorConfig;

    fn with_config(
        &self,
        config: Self::ConfigT,
        _registry: Arc<crate::operator::Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        // 创建一个空的 Schema
        let empty_schema = Arc::new(arrow::datatypes::Schema::new(Vec::<Arc<arrow::datatypes::Field>>::new()));
        let input_schema = Arc::new(ArroyoSchema::new_unkeyed(empty_schema, 0));

        let operator = CepOperator::new(config, input_schema)?;

        Ok(ConstructedOperator::from_operator(Box::new(operator)))
    }
}
