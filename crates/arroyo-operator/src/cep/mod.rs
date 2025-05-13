use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Display};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use prost::Message;
use serde::{Deserialize, Serialize};

use arroyo_rpc::df::ArroyoSchema;
use arroyo_types::{ArrowMessage, Watermark};

/// 模式类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PatternType {
    /// 单一事件模式
    Singleton,
    /// 序列模式（有序事件序列）
    Sequence,
    /// 跟随模式（无序事件集合）
    Followed,
    /// 否定模式（不应出现的事件）
    Negation,
    /// 重复模式（事件重复出现）
    Repetition,
    /// 时间约束模式
    TimeConstrained,
}

/// 模式条件类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionType {
    /// 简单条件（字段比较）
    Simple,
    /// 复合条件（AND/OR/NOT）
    Compound,
    /// 正则表达式条件
    Regex,
    /// SQL 表达式条件
    SqlExpression,
    /// 自定义函数条件
    CustomFunction,
}

/// 模式条件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternCondition {
    /// 条件类型
    pub condition_type: ConditionType,
    /// 条件表达式
    pub expression: String,
    /// 条件参数
    pub parameters: HashMap<String, String>,
}

/// 模式定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pattern {
    /// 模式名称
    pub name: String,
    /// 模式类型
    pub pattern_type: PatternType,
    /// 模式条件
    pub condition: PatternCondition,
    /// 子模式（用于复合模式）
    pub sub_patterns: Vec<Pattern>,
    /// 时间约束（可选）
    pub time_constraint: Option<Duration>,
    /// 重复次数约束（用于重复模式）
    pub repetition: Option<(usize, Option<usize>)>, // (min, max)
    /// 是否贪婪匹配
    pub greedy: bool,
}

/// 模式匹配结果
#[derive(Debug, Clone)]
pub struct PatternMatch {
    /// 匹配的模式名称
    pub pattern_name: String,
    /// 匹配的事件
    pub events: Vec<RecordBatch>,
    /// 匹配开始时间
    pub start_time: SystemTime,
    /// 匹配结束时间
    pub end_time: SystemTime,
}

/// 模式匹配状态
#[derive(Debug, Clone)]
pub struct MatchingState {
    /// 当前正在匹配的模式
    pub current_pattern: Pattern,
    /// 已匹配的事件
    pub matched_events: Vec<RecordBatch>,
    /// 匹配开始时间
    pub start_time: SystemTime,
    /// 上一次匹配事件时间
    pub last_event_time: SystemTime,
    /// 部分匹配状态
    pub partial_matches: Vec<MatchingState>,
}

/// CEP 操作符配置
#[derive(Debug, Clone, Message)]
pub struct CepOperatorConfig {
    /// 模式定义 JSON 字符串
    #[prost(string, tag = "1")]
    pub pattern_json: String,

    /// 时间字段名称
    #[prost(string, tag = "2")]
    pub time_field: String,

    /// 是否输出部分匹配
    #[prost(bool, tag = "3")]
    pub output_partial_matches: bool,

    /// 匹配超时时间（毫秒）
    #[prost(uint64, tag = "4")]
    pub match_timeout_ms: u64,

    /// 是否允许重叠匹配
    #[prost(bool, tag = "5")]
    pub allow_overlapping: bool,
}

/// 模式匹配引擎
pub struct PatternMatchingEngine {
    /// 模式定义
    pattern: Pattern,
    /// 当前活跃的匹配状态
    active_states: Vec<MatchingState>,
    /// 完成的匹配
    completed_matches: Vec<PatternMatch>,
    /// 时间字段索引
    time_field_index: usize,
    /// 匹配超时时间
    match_timeout: Duration,
    /// 是否允许重叠匹配
    allow_overlapping: bool,
    /// 最后处理的水印时间
    last_watermark: Option<SystemTime>,
}

// 在后续文件中实现 PatternMatchingEngine 和 CEP 操作符
pub mod operator;
pub mod engine;

#[cfg(test)]
mod tests;
