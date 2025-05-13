use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::SchemaRef;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::PrimitiveArray;
use serde_json::Value;
use tracing::{debug, info, warn};

use arroyo_rpc::df::ArroyoSchema;
use arroyo_types::{from_nanos, to_nanos, ArrowMessage, Watermark};

use super::{ConditionType, MatchingState, Pattern, PatternCondition, PatternMatch, PatternMatchingEngine, PatternType};

/// 状态推进结果
#[derive(Debug)]
pub enum StateAdvanceResult {
    /// 状态已推进
    Advanced(MatchingState),
    /// 匹配已完成
    Completed(PatternMatch),
    /// 不匹配
    NoMatch,
}

impl PatternMatchingEngine {
    /// 创建新的模式匹配引擎
    pub fn new(
        pattern: Pattern,
        time_field_index: usize,
        match_timeout: Duration,
        allow_overlapping: bool,
    ) -> Self {
        Self {
            pattern,
            active_states: Vec::new(),
            completed_matches: Vec::new(),
            time_field_index,
            match_timeout,
            allow_overlapping,
            last_watermark: None,
        }
    }

    /// 从 JSON 字符串解析模式
    pub fn from_json(json_str: &str, time_field_index: usize, match_timeout: Duration, allow_overlapping: bool) -> Result<Self> {
        let pattern: Pattern = serde_json::from_str(json_str)
            .map_err(|e| anyhow!("Failed to parse pattern JSON: {}", e))?;

        Ok(Self::new(pattern, time_field_index, match_timeout, allow_overlapping))
    }

    /// 处理新的事件批次
    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<Vec<PatternMatch>> {
        let timestamp_array = batch
            .column(self.time_field_index)
            .as_primitive::<TimestampNanosecondType>();

        // 处理每一行数据
        for row_idx in 0..batch.num_rows() {
            // 将 i64 转换为 u128
            let timestamp_value = timestamp_array.value(row_idx);
            let timestamp_u128 = if timestamp_value >= 0 {
                timestamp_value as u128
            } else {
                // 处理负时间戳（通常不应该出现）
                return Err(anyhow!("Negative timestamp encountered: {}", timestamp_value));
            };

            let event_time = from_nanos(timestamp_u128);

            // 创建单行记录批次用于匹配
            let row_batch = self.extract_row(batch, row_idx)?;

            // 处理这一行数据
            self.process_event(row_batch, event_time)?;
        }

        // 返回完成的匹配并清空
        let matches = self.completed_matches.clone();
        self.completed_matches.clear();

        Ok(matches)
    }

    /// 处理水印
    pub fn process_watermark(&mut self, watermark: &Watermark) -> Result<Vec<PatternMatch>> {
        let watermark_time = match watermark {
            Watermark::EventTime(time) => *time,
            Watermark::Idle => {
                // 对于空闲水印，我们可以使用当前系统时间
                SystemTime::now()
            }
        };

        self.last_watermark = Some(watermark_time);

        // 清理超时的匹配状态
        self.clean_timed_out_states(watermark_time);

        // 返回完成的匹配并清空
        let matches = self.completed_matches.clone();
        self.completed_matches.clear();

        Ok(matches)
    }

    /// 清理超时的匹配状态
    fn clean_timed_out_states(&mut self, current_time: SystemTime) {
        self.active_states.retain(|state| {
            match current_time.duration_since(state.last_event_time) {
                Ok(duration) => duration < self.match_timeout,
                Err(_) => true, // 如果时间计算出错，保留状态
            }
        });
    }

    /// 从批次中提取单行数据
    fn extract_row(&self, batch: &RecordBatch, row_idx: usize) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| col.slice(row_idx, 1))
            .collect();

        RecordBatch::try_new(batch.schema(), arrays)
            .map_err(|e| anyhow!("Failed to create row batch: {}", e))
    }

    /// 处理单个事件
    fn process_event(&mut self, event: RecordBatch, event_time: SystemTime) -> Result<()> {
        // 为新事件创建初始状态
        if self.matches_initial_condition(&event)? {
            let new_state = MatchingState {
                current_pattern: self.pattern.clone(),
                matched_events: vec![event.clone()],
                start_time: event_time,
                last_event_time: event_time,
                partial_matches: Vec::new(),
            };

            self.active_states.push(new_state);
        }

        // 处理现有的活跃状态
        let mut new_states = Vec::new();
        let mut completed = Vec::new();

        // 创建一个临时向量来存储需要处理的状态
        let states_to_process = self.active_states.clone();
        self.active_states.clear();

        for state in states_to_process {
            match self.advance_state(&state, &event, event_time)? {
                StateAdvanceResult::Advanced(new_state) => {
                    new_states.push(new_state);
                },
                StateAdvanceResult::Completed(match_result) => {
                    completed.push(match_result);
                },
                StateAdvanceResult::NoMatch => {
                    // 保留原状态
                    self.active_states.push(state);
                },
            }
        }

        // 添加新状态和完成的匹配
        self.active_states.append(&mut new_states);
        self.completed_matches.append(&mut completed);

        Ok(())
    }

    /// 检查事件是否匹配初始条件
    fn matches_initial_condition(&self, event: &RecordBatch) -> Result<bool> {
        self.evaluate_condition(&self.pattern.condition, event)
    }

    /// 评估条件
    fn evaluate_condition(&self, condition: &PatternCondition, event: &RecordBatch) -> Result<bool> {
        match condition.condition_type {
            ConditionType::Simple => {
                // 简单条件格式: "field_name operator value"
                // 例如: "amount > 100"
                let parts: Vec<&str> = condition.expression.split_whitespace().collect();
                if parts.len() != 3 {
                    return Err(anyhow!("Invalid simple condition format: {}", condition.expression));
                }

                let field_name = parts[0];
                let operator = parts[1];
                let value_str = parts[2];

                // 获取字段值
                let field_idx = event.schema().index_of(field_name)
                    .map_err(|_| anyhow!("Field not found: {}", field_name))?;

                let column = event.column(field_idx);
                if column.len() == 0 {
                    return Ok(false);
                }

                // 根据操作符比较值
                match operator {
                    "=" | "==" => self.compare_equal(column, 0, value_str),
                    "!=" => Ok(!self.compare_equal(column, 0, value_str)?),
                    ">" => self.compare_greater(column, 0, value_str),
                    ">=" => self.compare_greater_equal(column, 0, value_str),
                    "<" => self.compare_less(column, 0, value_str),
                    "<=" => self.compare_less_equal(column, 0, value_str),
                    _ => Err(anyhow!("Unsupported operator: {}", operator)),
                }
            },
            ConditionType::Compound => {
                // 复合条件需要在子模式中实现
                Err(anyhow!("Compound conditions not implemented yet"))
            },
            ConditionType::Regex => {
                // 正则表达式条件
                Err(anyhow!("Regex conditions not implemented yet"))
            },
            ConditionType::SqlExpression => {
                // SQL 表达式条件
                Err(anyhow!("SQL expression conditions not implemented yet"))
            },
            ConditionType::CustomFunction => {
                // 自定义函数条件
                Err(anyhow!("Custom function conditions not implemented yet"))
            },
        }
    }

    // 比较函数实现
    fn compare_equal(&self, column: &ArrayRef, row: usize, value_str: &str) -> Result<bool> {
        // 简化实现，实际需要根据数据类型进行比较
        // 由于 ArrayRef 没有实现 ToString，我们需要使用其他方式比较
        // 这里只是一个占位实现，实际应该根据列的类型进行适当的比较
        Ok(format!("{:?}", column).contains(value_str))
    }

    fn compare_greater(&self, column: &ArrayRef, row: usize, value_str: &str) -> Result<bool> {
        // 简化实现
        Err(anyhow!("Greater comparison not implemented yet"))
    }

    fn compare_greater_equal(&self, column: &ArrayRef, row: usize, value_str: &str) -> Result<bool> {
        // 简化实现
        Err(anyhow!("Greater equal comparison not implemented yet"))
    }

    fn compare_less(&self, column: &ArrayRef, row: usize, value_str: &str) -> Result<bool> {
        // 简化实现
        Err(anyhow!("Less comparison not implemented yet"))
    }

    fn compare_less_equal(&self, column: &ArrayRef, row: usize, value_str: &str) -> Result<bool> {
        // 简化实现
        Err(anyhow!("Less equal comparison not implemented yet"))
    }

    // 使用外部定义的 StateAdvanceResult 枚举

    /// 推进匹配状态
    fn advance_state(&self, state: &MatchingState, event: &RecordBatch, event_time: SystemTime) -> Result<StateAdvanceResult> {
        // 根据模式类型处理
        match state.current_pattern.pattern_type {
            PatternType::Singleton => {
                // 单一事件模式已经在初始匹配时处理
                Ok(StateAdvanceResult::Completed(PatternMatch {
                    pattern_name: state.current_pattern.name.clone(),
                    events: state.matched_events.clone(),
                    start_time: state.start_time,
                    end_time: event_time,
                }))
            },
            PatternType::Sequence => {
                // 序列模式需要按顺序匹配子模式
                Err(anyhow!("Sequence pattern not implemented yet"))
            },
            PatternType::Followed => {
                // 跟随模式需要匹配所有子模式，但顺序不重要
                Err(anyhow!("Followed pattern not implemented yet"))
            },
            PatternType::Negation => {
                // 否定模式需要确保某些事件不出现
                Err(anyhow!("Negation pattern not implemented yet"))
            },
            PatternType::Repetition => {
                // 重复模式需要匹配事件多次
                Err(anyhow!("Repetition pattern not implemented yet"))
            },
            PatternType::TimeConstrained => {
                // 时间约束模式需要在特定时间窗口内匹配
                Err(anyhow!("Time constrained pattern not implemented yet"))
            },
        }
    }
}
