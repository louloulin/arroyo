use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arroyo_operator::context::{Collector, ErrorReporter, OperatorContext, WatermarkHolder};
use arroyo_operator::operator::ArrowOperator;
use arroyo_state::tables::table_manager::TableManager;
use arroyo_types::{TaskInfo, Watermark};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;

use arroyo_worker::arrow::window_assigner::{TumblingWindow};
use arroyo_worker::arrow::window_assigners::TumblingWindowAssigner;
use arroyo_worker::arrow::window_operator::GenericWindowOperator;
use arroyo_worker::arrow::window_trigger::EventTimeTrigger;

// 测试收集器
struct TestCollector {
    batches: Vec<RecordBatch>,
    watermarks: Vec<Watermark>,
}

impl TestCollector {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            watermarks: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl Collector for TestCollector {
    async fn collect(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    async fn broadcast_watermark(&mut self, watermark: Watermark) {
        self.watermarks.push(watermark);
    }
}

// 创建测试错误报告器
fn create_test_error_reporter() -> ErrorReporter {
    let (tx, _) = tokio::sync::mpsc::channel(1);
    ErrorReporter {
        tx,
        task_info: Arc::new(TaskInfo {
            job_id: "test".to_string(),
            node_id: 0,
            operator_name: "test".to_string(),
            operator_id: "test".to_string(),
            task_index: 0,
            parallelism: 1,
            key_range: std::ops::RangeInclusive::new(0, 0),
        }),
    }
}

// 创建测试上下文
async fn create_test_context() -> OperatorContext {
    let task_info = Arc::new(TaskInfo {
        job_id: "test_job".to_string(),
        node_id: 1,
        operator_name: "test_operator".to_string(),
        operator_id: "test_operator_1".to_string(),
        task_index: 0,
        parallelism: 1,
        key_range: std::ops::RangeInclusive::new(0, u64::MAX),
    });

    let (control_tx, _) = tokio::sync::mpsc::channel(16);

    // 创建一个空的 TableManager
    let (table_manager, _) = TableManager::load(
        task_info.clone(),
        std::collections::HashMap::new(),
        control_tx.clone(),
        None,
    )
    .await
    .expect("Failed to create TableManager");

    // 创建测试架构
    let schema = Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("timestamp", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
    ]);

    let in_schema = Arc::new(arroyo_rpc::df::ArroyoSchema::new_keyed(
        Arc::new(schema.clone()),
        2,
        vec![0],
    ));

    OperatorContext {
        task_info,
        control_tx,
        watermarks: WatermarkHolder::new(vec![None]),
        in_schemas: vec![in_schema.clone()],
        out_schema: Some(in_schema),
        table_manager,
        error_reporter: create_test_error_reporter(),
    }
}

// 创建测试数据批次
fn create_test_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("timestamp", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
    ]);

    let keys = StringArray::from(vec!["key1", "key2", "key3", "key1", "key2"]);
    let values = Int64Array::from(vec![1, 2, 3, 4, 5]);

    // 创建时间戳数组
    let now = SystemTime::now();
    let timestamps = vec![
        now - Duration::from_secs(10),
        now - Duration::from_secs(8),
        now - Duration::from_secs(6),
        now - Duration::from_secs(4),
        now - Duration::from_secs(2),
    ];

    let timestamp_nanos: Vec<i64> = timestamps
        .iter()
        .map(|t| {
            t.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64
        })
        .collect();

    let timestamp_array = TimestampNanosecondArray::from(timestamp_nanos);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(keys),
            Arc::new(values),
            Arc::new(timestamp_array),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_tumbling_window_operator() {
    // 创建测试上下文和收集器
    let mut ctx = create_test_context().await;
    let mut collector = TestCollector::new();

    // 创建测试数据
    let batch = create_test_batch();

    // 创建窗口分配器
    let window_assigner = TumblingWindowAssigner::new(
        Duration::from_secs(5),
        Duration::from_secs(0),
        Duration::from_secs(10),
    );

    // 创建触发器
    let trigger = EventTimeTrigger::<TumblingWindow>::new();

    // 创建空的聚合计划
    let schema = batch.schema();
    let aggregation_plan = Arc::new(EmptyExec::new(schema.clone()));

    // 创建窗口操作符
    let mut window_operator = GenericWindowOperator::<TumblingWindow, _, _>::new(
        Arc::new(arroyo_rpc::df::ArroyoSchema::new_unkeyed(schema.clone(), 2)),
        Arc::new(arroyo_rpc::df::ArroyoSchema::new_unkeyed(schema.clone(), 2)),
        window_assigner,
        trigger,
        aggregation_plan,
        2, // timestamp 字段的索引
    );

    // 初始化操作符
    window_operator.on_start(&mut ctx).await;

    // 处理数据批次
    window_operator.process_batch(batch.clone(), &mut ctx, &mut collector).await;

    // 发送水印
    let watermark = Watermark::EventTime(SystemTime::now());
    let _ = window_operator.handle_watermark(watermark, &mut ctx, &mut collector).await;

    // 验证结果
    assert!(!collector.batches.is_empty(), "应该有输出批次");
    assert!(!collector.watermarks.is_empty(), "应该有水印");

    // 打印结果
    println!("输出批次: {:?}", collector.batches);
    println!("输出水印: {:?}", collector.watermarks);
}
