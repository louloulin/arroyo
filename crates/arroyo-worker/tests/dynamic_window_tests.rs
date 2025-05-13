use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::physical_plan::empty::EmptyExec;

use arroyo_state::tables::table_manager::TableManager;
use arroyo_types::{TaskInfo, Watermark};

use async_trait::async_trait;
use arroyo_operator::operator::ArrowOperator;

use arroyo_worker::arrow::window_assigners::DynamicWindowAssigner;
use arroyo_worker::arrow::window_operator::{GenericWindowOperator, WindowOperator};
use arroyo_worker::arrow::window_trigger::DynamicTrigger;

/// 测试收集器，用于收集窗口操作符的输出
#[derive(Debug)]
struct TestCollector {
    /// 收集的批次
    pub batches: Vec<RecordBatch>,
    /// 收集的水印
    pub watermarks: Vec<Watermark>,
}

#[async_trait::async_trait]
impl arroyo_operator::context::Collector for TestCollector {
    async fn collect(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    async fn broadcast_watermark(&mut self, watermark: Watermark) {
        self.watermarks.push(watermark);
    }
}

/// 创建测试批次
fn create_test_batch(
    user: &str,
    values: Vec<i64>,
    timestamps: Vec<SystemTime>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));

    let user_array = StringArray::from(vec![user; values.len()]);
    let value_array = Int64Array::from(values);
    let timestamp_array = TimestampNanosecondArray::from(
        timestamps
            .iter()
            .map(|t| {
                Some(
                    t.duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64,
                )
            })
            .collect::<Vec<_>>(),
    );

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(user_array),
            Arc::new(value_array),
            Arc::new(timestamp_array),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_dynamic_window_with_progress_trigger() {
    // 创建测试收集器
    let mut collector = TestCollector {
        batches: Vec::new(),
        watermarks: Vec::new(),
    };

    // 创建操作符上下文
    let task_info = Arc::new(TaskInfo {
        job_id: "test-job".to_string(),
        node_id: 1,
        operator_name: "test-operator".to_string(),
        operator_id: "test-operator-1".to_string(),
        task_index: 0,
        parallelism: 1,
        key_range: 0..=u64::MAX,
    });

    // 创建一个简化的 OperatorContext
    let control_tx = tokio::sync::mpsc::channel(1).0;

    // 创建一个空的 TableManager
    let (table_manager, _) = TableManager::load(
        task_info.clone(),
        std::collections::HashMap::new(),
        control_tx.clone(),
        None,
    )
    .await
    .expect("Failed to create TableManager");

    let mut ctx = arroyo_operator::context::OperatorContext {
        task_info: task_info.clone(),
        control_tx,
        watermarks: arroyo_operator::context::WatermarkHolder::new(vec![None]),
        in_schemas: vec![],
        out_schema: None,
        table_manager,
        error_reporter: arroyo_operator::context::ErrorReporter {
            tx: tokio::sync::mpsc::channel(1).0,
            task_info,
        },
    };

    // 创建当前时间
    let now = SystemTime::now();

    // 创建动态窗口分配器
    // 这个分配器会根据元素的时间戳创建窗口，窗口大小为 5 秒
    let window_assigner = DynamicWindowAssigner::new(
        |timestamp| {
            // 窗口开始时间为时间戳向下取整到最近的 5 秒
            let start = timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let start = start - (start % 5);
            let start_time = SystemTime::UNIX_EPOCH + Duration::from_secs(start);

            // 窗口结束时间为开始时间 + 5 秒
            let end_time = start_time + Duration::from_secs(5);

            (start_time, end_time)
        },
        Duration::from_secs(1),
    );

    // 创建动态触发器
    // 这个触发器会在窗口进度达到 50% 时触发
    let trigger = DynamicTrigger::progress_based(0.5);

    // 创建输入和输出模式
    let schema = Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));

    let input_schema = Arc::new(arroyo_rpc::df::ArroyoSchema::new_unkeyed(schema.clone(), 2));

    let output_schema = Arc::new(arroyo_rpc::df::ArroyoSchema::new_unkeyed(schema.clone(), 2));

    // 创建聚合计划
    let aggregation_plan = Arc::new(EmptyExec::new(schema.clone()));

    // 创建窗口操作符
    let mut window_operator = GenericWindowOperator::new(
        input_schema,
        output_schema,
        window_assigner,
        trigger,
        aggregation_plan,
        2, // 时间戳列索引
    );

    // 创建测试批次
    let batch = create_test_batch(
        "user1",
        vec![1, 2, 3, 4, 5],
        vec![
            now,
            now + Duration::from_secs(1),
            now + Duration::from_secs(2),
            now + Duration::from_secs(3),
            now + Duration::from_secs(4),
        ],
    );

    // 处理批次
    window_operator.process_batch(batch.clone(), &mut ctx, &mut collector).await;

    // 发送水印，应该触发窗口计算
    let watermark = Watermark::EventTime(now + Duration::from_secs(3));
    window_operator.handle_watermark(watermark, &mut ctx, &mut collector).await.unwrap();

    // 手动触发窗口计算
    for window in window_operator.get_windows() {
        window_operator.trigger_window(&window, &mut ctx, &mut collector).await.unwrap();
    }

    // 验证结果
    assert!(!collector.batches.is_empty(), "应该有输出批次");
    assert!(!collector.watermarks.is_empty(), "应该有水印");
}

#[tokio::test]
async fn test_dynamic_window_with_data_driven_trigger() {
    // 创建测试收集器
    let mut collector = TestCollector {
        batches: Vec::new(),
        watermarks: Vec::new(),
    };

    // 创建操作符上下文
    let task_info = Arc::new(TaskInfo {
        job_id: "test-job".to_string(),
        node_id: 1,
        operator_name: "test-operator".to_string(),
        operator_id: "test-operator-1".to_string(),
        task_index: 0,
        parallelism: 1,
        key_range: 0..=u64::MAX,
    });

    // 创建一个简化的 OperatorContext
    let control_tx = tokio::sync::mpsc::channel(1).0;

    // 创建一个空的 TableManager
    let (table_manager, _) = TableManager::load(
        task_info.clone(),
        std::collections::HashMap::new(),
        control_tx.clone(),
        None,
    )
    .await
    .expect("Failed to create TableManager");

    let mut ctx = arroyo_operator::context::OperatorContext {
        task_info: task_info.clone(),
        control_tx,
        watermarks: arroyo_operator::context::WatermarkHolder::new(vec![None]),
        in_schemas: vec![],
        out_schema: None,
        table_manager,
        error_reporter: arroyo_operator::context::ErrorReporter {
            tx: tokio::sync::mpsc::channel(1).0,
            task_info,
        },
    };

    // 创建当前时间
    let now = SystemTime::now();

    // 创建动态窗口分配器
    // 这个分配器会创建一个 10 秒的窗口
    let window_assigner = DynamicWindowAssigner::new(
        |timestamp| {
            (timestamp, timestamp + Duration::from_secs(10))
        },
        Duration::from_secs(1),
    );

    // 创建数据驱动的动态触发器
    let trigger = DynamicTrigger::data_driven();

    // 创建输入和输出模式
    let schema = Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));

    let input_schema = Arc::new(arroyo_rpc::df::ArroyoSchema::new_unkeyed(schema.clone(), 2));

    let output_schema = Arc::new(arroyo_rpc::df::ArroyoSchema::new_unkeyed(schema.clone(), 2));

    // 创建聚合计划
    let aggregation_plan = Arc::new(EmptyExec::new(schema.clone()));

    // 创建窗口操作符
    let mut window_operator = GenericWindowOperator::new(
        input_schema,
        output_schema,
        window_assigner,
        trigger,
        aggregation_plan,
        2, // 时间戳列索引
    );

    // 创建测试批次
    let batch = create_test_batch(
        "user1",
        vec![1, 2, 3, 4, 5],
        vec![
            now,
            now + Duration::from_secs(2),
            now + Duration::from_secs(4),
            now + Duration::from_secs(6),
            now + Duration::from_secs(8),
        ],
    );

    // 处理批次
    window_operator.process_batch(batch.clone(), &mut ctx, &mut collector).await;

    // 发送水印，应该触发窗口计算
    let watermark = Watermark::EventTime(now + Duration::from_secs(5));
    window_operator.handle_watermark(watermark, &mut ctx, &mut collector).await.unwrap();

    // 手动触发窗口计算
    for window in window_operator.get_windows() {
        window_operator.trigger_window(&window, &mut ctx, &mut collector).await.unwrap();
    }

    // 验证结果
    assert!(!collector.batches.is_empty(), "应该有输出批次");
    assert!(!collector.watermarks.is_empty(), "应该有水印");
}
