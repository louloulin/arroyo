use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::physical_plan::empty::EmptyExec;

use arroyo_state::tables::table_manager::TableManager;
use arroyo_types::{TaskInfo, Watermark};

use arroyo_operator::operator::ArrowOperator;

use arroyo_worker::arrow::window_assigner::Window;
use arroyo_worker::arrow::window_assigners::MultiDimensionalWindowAssigner;
use arroyo_worker::arrow::window_operator::{GenericWindowOperator, WindowOperator};
use arroyo_worker::arrow::window_trigger::EventTimeTrigger;

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

/// 测试多维窗口功能
#[tokio::test]
async fn test_multi_dimensional_window() {
    // 创建测试收集器
    let mut collector = TestCollector {
        batches: Vec::new(),
        watermarks: Vec::new(),
    };

    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));

    // 创建当前时间
    let now = SystemTime::now();
    let timestamp_nanos = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    // 创建测试批次
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user1", "user2", "user1", "user3"])),
            Arc::new(StringArray::from(vec!["east", "west", "east", "west"])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(TimestampNanosecondArray::from(vec![
                timestamp_nanos,
                timestamp_nanos,
                timestamp_nanos,
                timestamp_nanos,
            ])),
        ],
    )
    .unwrap();

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

    // 创建多维窗口分配器
    // 这个分配器会创建一个 10 秒的窗口，并按用户和区域进行分组
    let window_assigner = MultiDimensionalWindowAssigner::new(
        |timestamp| {
            // 窗口时间维度：10 秒的窗口
            (timestamp, timestamp + Duration::from_secs(10))
        },
        |batch| {
            // 窗口其他维度：用户和区域
            let user_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected string column");

            let region_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected string column");

            // 提取第一行的用户和区域作为维度
            // 在实际应用中，可能需要更复杂的逻辑
            if batch.num_rows() > 0 {
                vec![
                    ("user".to_string(), user_array.value(0).to_string()),
                    ("region".to_string(), region_array.value(0).to_string()),
                ]
            } else {
                Vec::new()
            }
        },
        Duration::from_secs(1),
    );

    // 创建事件时间触发器
    let trigger = EventTimeTrigger::new();

    // 创建输入和输出模式
    let input_schema = Arc::new(arroyo_rpc::df::ArroyoSchema::new_unkeyed(schema.clone(), 3));
    let output_schema = Arc::new(arroyo_rpc::df::ArroyoSchema::new_unkeyed(schema.clone(), 3));

    // 创建聚合计划
    let aggregation_plan = Arc::new(EmptyExec::new(schema.clone()));

    // 创建窗口操作符
    let mut window_operator = GenericWindowOperator::new(
        input_schema,
        output_schema,
        window_assigner,
        trigger,
        aggregation_plan,
        3, // 时间戳列索引
    );

    // 处理批次
    window_operator.process_batch(batch.clone(), &mut ctx, &mut collector).await;

    // 发送水印，应该触发窗口计算
    let watermark = Watermark::EventTime(now + Duration::from_secs(10));
    window_operator.handle_watermark(watermark, &mut ctx, &mut collector).await.unwrap();

    // 手动触发窗口计算
    for window in window_operator.get_windows() {
        window_operator.trigger_window(&window, &mut ctx, &mut collector).await.unwrap();
    }

    // 验证结果
    assert!(!collector.batches.is_empty(), "应该有输出批次");
    assert!(!collector.watermarks.is_empty(), "应该有水印");

    // 打印结果
    println!("收集到的批次数量: {}", collector.batches.len());
    println!("收集到的水印数量: {}", collector.watermarks.len());

    // 验证窗口维度
    let windows = window_operator.get_windows();
    assert!(!windows.is_empty(), "应该有活动窗口");

    for window in windows {
        println!("窗口: {:?}", window);
        println!("  开始时间: {:?}", window.start());
        println!("  结束时间: {:?}", window.end());
        println!("  维度键: {:?}", window.dimension_keys);
        println!("  维度值: {:?}", window.dimension_values);
    }
}
