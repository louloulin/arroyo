use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::array::builder::TimestampNanosecondBuilder;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_state::tables::table_manager::TableManager;
use arroyo_types::{to_nanos, TaskInfo, Watermark};
use tokio::sync::mpsc::Sender;

use crate::cep::{CepOperatorConfig, ConditionType, Pattern, PatternCondition, PatternType, PatternMatchingEngine};
use crate::cep::operator::CepOperator;
use crate::context::{Collector, ErrorReporter, OperatorContext, WatermarkHolder};
use crate::operator::ArrowOperator;

// 模拟收集器，用于测试
struct MockCollector {
    batches: Vec<RecordBatch>,
}

impl MockCollector {
    fn new() -> Self {
        Self { batches: Vec::new() }
    }
}

#[async_trait::async_trait]
impl Collector for MockCollector {
    async fn collect(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    async fn broadcast_watermark(&mut self, _watermark: Watermark) {}
}

// 创建测试数据批次
fn create_test_batch(
    transaction_ids: Vec<&str>,
    user_ids: Vec<&str>,
    amounts: Vec<f64>,
    timestamps: Vec<SystemTime>,
) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("transaction_id", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new(
            "transaction_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]);

    let transaction_id_array = StringArray::from(transaction_ids);
    let user_id_array = StringArray::from(user_ids);
    let amount_array = arrow::array::Float64Array::from(amounts);

    let mut timestamp_builder = TimestampNanosecondBuilder::new();
    for ts in timestamps {
        timestamp_builder.append_value(to_nanos(ts));
    }
    let timestamp_array = timestamp_builder.finish();

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(transaction_id_array),
            Arc::new(user_id_array),
            Arc::new(amount_array),
            Arc::new(timestamp_array),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_pattern_matching_engine() {
    // 创建一个简单的模式：金额 > 100
    let pattern = Pattern {
        name: "large_transaction".to_string(),
        pattern_type: PatternType::Singleton,
        condition: PatternCondition {
            condition_type: ConditionType::Simple,
            expression: "amount > 100".to_string(),
            parameters: Default::default(),
        },
        sub_patterns: vec![],
        time_constraint: None,
        repetition: None,
        greedy: true,
    };

    // 创建模式匹配引擎
    let mut engine = PatternMatchingEngine::new(
        pattern,
        3, // 时间字段索引
        Duration::from_secs(60),
        true,
    );

    // 创建测试数据
    let now = SystemTime::now();
    let batch = create_test_batch(
        vec!["tx1", "tx2", "tx3"],
        vec!["user1", "user2", "user1"],
        vec![50.0, 150.0, 200.0],
        vec![
            now,
            now + Duration::from_secs(10),
            now + Duration::from_secs(20),
        ],
    );

    // 处理批次
    let matches = engine.process_batch(&batch).unwrap();

    // 验证结果
    assert_eq!(matches.len(), 2); // 应该匹配两个事务：tx2 和 tx3
    assert_eq!(matches[0].pattern_name, "large_transaction");
    assert_eq!(matches[1].pattern_name, "large_transaction");
}

#[tokio::test]
async fn test_cep_operator() {
    // 创建模式 JSON
    let pattern_json = r#"{
        "name": "large_transaction",
        "pattern_type": "Singleton",
        "condition": {
            "condition_type": "Simple",
            "expression": "amount > 100",
            "parameters": {}
        },
        "sub_patterns": [],
        "greedy": true
    }"#;

    // 创建操作符配置
    let config = CepOperatorConfig {
        pattern_json: pattern_json.to_string(),
        time_field: "transaction_time".to_string(),
        output_partial_matches: false,
        match_timeout_ms: 60000,
        allow_overlapping: true,
    };

    // 创建输入模式
    let schema = Schema::new(vec![
        Field::new("transaction_id", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new(
            "transaction_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]);
    let input_schema = Arc::new(ArroyoSchema::new_keyed(Arc::new(schema), 3, vec![1])); // user_id 作为键

    // 创建 CEP 操作符
    let mut operator = CepOperator::new(config, input_schema.clone()).unwrap();

    // 创建测试数据
    let now = SystemTime::now();
    let batch = create_test_batch(
        vec!["tx1", "tx2", "tx3"],
        vec!["user1", "user2", "user1"],
        vec![50.0, 150.0, 200.0],
        vec![
            now,
            now + Duration::from_secs(10),
            now + Duration::from_secs(20),
        ],
    );

    // 创建收集器
    let mut collector = MockCollector::new();

    // 创建一个模拟的 OperatorContext
    let task_info = Arc::new(TaskInfo {
        job_id: "test-job".to_string(),
        node_id: 1,
        operator_name: "test-operator".to_string(),
        operator_id: "test-operator-id".to_string(),
        task_index: 0,
        parallelism: 1,
        key_range: (0, u64::MAX),
    });

    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let mut ctx = OperatorContext {
        task_info,
        control_tx: tx,
        watermarks: WatermarkHolder::new(vec![None]),
        in_schemas: vec![input_schema.clone()],
        out_schema: Some(input_schema.clone()),
        table_manager: TableManager::new_for_test(),
        error_reporter: ErrorReporter {
            tx: tx.clone(),
            task_info: Arc::new(TaskInfo {
                job_id: "test-job".to_string(),
                node_id: 1,
                operator_name: "test-operator".to_string(),
                operator_id: "test-operator-id".to_string(),
                task_index: 0,
                parallelism: 1,
                key_range: (0, u64::MAX),
            }),
        },
    };

    // 处理批次
    operator
        .process_batch(batch, &mut ctx, &mut collector)
        .await;

    // 验证结果
    assert_eq!(collector.batches.len(), 2); // 应该输出两个匹配结果
}
