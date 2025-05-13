use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{Float64Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_types::{to_nanos, Watermark};

use crate::context::{Collector, OperatorContext, WatermarkHolder};
use crate::operator::ArrowOperator;
use crate::timeseries::{TimeSeriesAnalysisConfig, TimeSeriesAnalysisMethod};
use crate::timeseries::operator::TimeSeriesAnalysisOperator;

// 模拟收集器，用于测试
struct MockCollector {
    batches: Vec<RecordBatch>,
    watermarks: Vec<Watermark>,
}

impl MockCollector {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            watermarks: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl Collector for MockCollector {
    async fn collect(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    async fn broadcast_watermark(&mut self, watermark: Watermark) {
        self.watermarks.push(watermark);
    }
}

// 创建测试数据批次
fn create_test_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("temperature", DataType::Float64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
    ]);

    let now = SystemTime::now();
    let timestamps = vec![
        now,
        now + Duration::from_secs(60),
        now + Duration::from_secs(120),
        now + Duration::from_secs(180),
        now + Duration::from_secs(240),
        now + Duration::from_secs(300),
        now + Duration::from_secs(360),
        now + Duration::from_secs(420),
        now + Duration::from_secs(480),
        now + Duration::from_secs(540),
    ];

    let sensor_ids = arrow::array::StringArray::from(vec![
        "sensor1", "sensor1", "sensor1", "sensor1", "sensor1",
        "sensor1", "sensor1", "sensor1", "sensor1", "sensor1",
    ]);

    let temperatures = Float64Array::from(vec![
        20.5, 21.0, 21.5, 22.0, 22.5, 23.0, 23.5, 24.0, 24.5, 25.0,
    ]);

    let timestamp_nanos: Vec<i64> = timestamps
        .iter()
        .map(|t| to_nanos(*t))
        .collect();

    let timestamp_array = TimestampNanosecondArray::from(timestamp_nanos);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(sensor_ids),
            Arc::new(temperatures),
            Arc::new(timestamp_array),
        ],
    )
    .unwrap()
}

// 创建测试上下文
fn create_test_context() -> OperatorContext {
    use arroyo_rpc::ControlResp;
    use arroyo_state::tables::table_manager::TableManager;
    use arroyo_types::TaskInfo;
    use tokio::sync::mpsc::channel;

    let task_info = Arc::new(TaskInfo {
        job_id: "test-job".to_string(),
        node_id: 1,
        operator_name: "test-operator".to_string(),
        operator_id: "test-operator-id".to_string(),
        task_index: 0,
        parallelism: 1,
        key_range: (0, u64::MAX),
    });

    let (tx, _rx) = channel(16);
    OperatorContext {
        task_info: task_info.clone(),
        control_tx: tx.clone(),
        watermarks: WatermarkHolder::new(vec![None]),
        in_schemas: vec![],
        out_schema: None,
        table_manager: TableManager::new_for_test(),
        error_reporter: crate::context::ErrorReporter {
            tx,
            task_info,
        },
    }
}

#[tokio::test]
async fn test_moving_average() {
    // 创建测试数据
    let batch = create_test_batch();
    let schema = batch.schema();

    // 创建输入模式
    let input_schema = Arc::new(ArroyoSchema::new_unkeyed(schema.clone(), 2));

    // 创建配置
    let config = TimeSeriesAnalysisConfig {
        method: TimeSeriesAnalysisMethod::MovingAverage as i32,
        time_field: "timestamp".to_string(),
        value_field: "temperature".to_string(),
        window_size_ms: 600000, // 10分钟
        slide_ms: 60000,        // 1分钟
        ma_type: 0,             // 简单移动平均
        ma_window_size: 3,      // 窗口大小为3
        ema_alpha: 0.2,         // 指数移动平均平滑因子
        trend_type: 0,          // 线性趋势
        polynomial_degree: 2,   // 多项式趋势阶数
        anomaly_method: 0,      // 标准差方法
        anomaly_threshold: 2.0, // 标准差倍数
        forecast_method: 0,     // 自回归
        forecast_steps: 3,      // 预测步数
        seasonality_period: 24, // 季节性周期（小时）
        correlation_fields: vec![],
    };

    // 创建时间序列分析操作符
    let mut operator = TimeSeriesAnalysisOperator::new(config, input_schema).unwrap();

    // 创建收集器
    let mut collector = MockCollector::new();

    // 创建上下文
    let mut ctx = create_test_context();

    // 处理批次
    operator.process_batch(batch, &mut ctx, &mut collector).await;

    // 验证结果
    assert!(!collector.batches.is_empty());
    let result_batch = &collector.batches[0];

    // 验证输出模式
    assert!(result_batch.schema().field_with_name("temperature_ma").is_ok());

    // 验证移动平均值
    let ma_column = result_batch
        .column_by_name("temperature_ma")
        .expect("移动平均列不存在");

    let ma_array = ma_column
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("移动平均列不是 Float64Array");

    // 前两个值应该是 NaN（因为窗口大小为3）
    assert!(ma_array.value(0).is_nan() || ma_array.value(0) == 0.0);
    assert!(ma_array.value(1).is_nan() || ma_array.value(1) == 0.0);

    // 第三个值应该是前三个温度的平均值
    let expected_ma = (20.5 + 21.0 + 21.5) / 3.0;
    assert!((ma_array.value(2) - expected_ma).abs() < 0.001);
}

#[tokio::test]
async fn test_trend_analysis() {
    // 创建测试数据
    let batch = create_test_batch();
    let schema = batch.schema();

    // 创建输入模式
    let input_schema = Arc::new(ArroyoSchema::new_unkeyed(schema.clone(), 2));

    // 创建配置
    let config = TimeSeriesAnalysisConfig {
        method: TimeSeriesAnalysisMethod::TrendAnalysis as i32,
        time_field: "timestamp".to_string(),
        value_field: "temperature".to_string(),
        window_size_ms: 600000, // 10分钟
        slide_ms: 60000,        // 1分钟
        ma_type: 0,             // 简单移动平均
        ma_window_size: 3,      // 窗口大小为3
        ema_alpha: 0.2,         // 指数移动平均平滑因子
        trend_type: 0,          // 线性趋势
        polynomial_degree: 2,   // 多项式趋势阶数
        anomaly_method: 0,      // 标准差方法
        anomaly_threshold: 2.0, // 标准差倍数
        forecast_method: 0,     // 自回归
        forecast_steps: 3,      // 预测步数
        seasonality_period: 24, // 季节性周期（小时）
        correlation_fields: vec![],
    };

    // 创建时间序列分析操作符
    let mut operator = TimeSeriesAnalysisOperator::new(config, input_schema).unwrap();

    // 创建收集器
    let mut collector = MockCollector::new();

    // 创建上下文
    let mut ctx = create_test_context();

    // 处理批次
    operator.process_batch(batch, &mut ctx, &mut collector).await;

    // 验证结果
    assert!(!collector.batches.is_empty());
    let result_batch = &collector.batches[0];

    // 验证输出模式
    assert!(result_batch.schema().field_with_name("temperature_trend").is_ok());
    assert!(result_batch.schema().field_with_name("trend_intercept").is_ok());
    assert!(result_batch.schema().field_with_name("trend_slope").is_ok());

    // 验证趋势系数
    let intercept_column = result_batch
        .column_by_name("trend_intercept")
        .expect("趋势截距列不存在");

    let slope_column = result_batch
        .column_by_name("trend_slope")
        .expect("趋势斜率列不存在");

    let intercept_array = intercept_column
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("趋势截距列不是 Float64Array");

    let slope_array = slope_column
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("趋势斜率列不是 Float64Array");

    // 斜率应该是正的（因为温度在上升）
    assert!(slope_array.value(0) > 0.0);
}
