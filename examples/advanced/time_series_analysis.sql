-- 时间序列分析示例
-- 本示例展示如何使用 Arroyo 的时间序列分析功能

-- 创建传感器数据流
CREATE TABLE sensor_data (
    sensor_id STRING,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    event_time TIMESTAMP,
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    connector = 'kafka',
    bootstrap.servers = 'localhost:9092',
    topic = 'sensor_data',
    format = 'json'
);

-- 示例 1: 移动平均分析
-- 计算温度的移动平均值，使用 3 个数据点的窗口
CREATE VIEW temperature_ma AS
SELECT 
    sensor_id,
    event_time,
    temperature,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'MOVING_AVERAGE',
        '{"window_size": 3, "type": "SIMPLE"}'
    ) AS temperature_ma
FROM sensor_data;

-- 示例 2: 趋势分析
-- 检测温度的上升/下降趋势
CREATE VIEW temperature_trend AS
SELECT 
    sensor_id,
    event_time,
    temperature,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'TREND_ANALYSIS',
        '{"type": "LINEAR"}'
    ) AS trend_result
FROM sensor_data
GROUP BY sensor_id, TUMBLE(event_time, INTERVAL '1' HOUR);

-- 示例 3: 异常检测
-- 检测温度异常值
CREATE VIEW temperature_anomalies AS
SELECT 
    sensor_id,
    event_time,
    temperature,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'ANOMALY_DETECTION',
        '{"method": "STANDARD_DEVIATION", "threshold": 2.0}'
    ) AS anomaly_result
FROM sensor_data;

-- 示例 4: 季节性检测
-- 检测温度的周期性模式
CREATE VIEW temperature_seasonality AS
SELECT 
    sensor_id,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'SEASONALITY_DETECTION',
        '{"period": 24}'
    ) AS seasonality_result
FROM sensor_data
GROUP BY sensor_id, TUMBLE(event_time, INTERVAL '1' DAY);

-- 示例 5: 预测
-- 基于历史数据预测未来温度
CREATE VIEW temperature_forecast AS
SELECT 
    sensor_id,
    event_time,
    temperature,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'FORECASTING',
        '{"method": "ARIMA", "steps": 5}'
    ) AS forecast_result
FROM sensor_data
GROUP BY sensor_id, TUMBLE(event_time, INTERVAL '1' HOUR);

-- 示例 6: 相关性分析
-- 分析温度、湿度和压力之间的相关性
CREATE VIEW sensor_correlations AS
SELECT 
    sensor_id,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'CORRELATION_ANALYSIS',
        '{"fields": ["humidity", "pressure"]}'
    ) AS correlation_result
FROM sensor_data
GROUP BY sensor_id, TUMBLE(event_time, INTERVAL '1' DAY);

-- 示例 7: 指数移动平均
-- 计算温度的指数移动平均，赋予最近的数据更高的权重
CREATE VIEW temperature_ema AS
SELECT 
    sensor_id,
    event_time,
    temperature,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'EXPONENTIAL_MOVING_AVERAGE',
        '{"alpha": 0.2}'
    ) AS temperature_ema
FROM sensor_data;

-- 示例 8: 组合分析
-- 结合多种时间序列分析方法
CREATE VIEW comprehensive_analysis AS
SELECT 
    sensor_id,
    event_time,
    temperature,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'MOVING_AVERAGE',
        '{"window_size": 5, "type": "SIMPLE"}'
    ) AS temperature_ma,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'ANOMALY_DETECTION',
        '{"method": "STANDARD_DEVIATION", "threshold": 2.0}'
    ) AS is_anomaly,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'TREND_ANALYSIS',
        '{"type": "LINEAR"}'
    ) AS trend_slope
FROM sensor_data
GROUP BY sensor_id, TUMBLE(event_time, INTERVAL '1' HOUR);

-- 示例 9: 实时监控仪表板
-- 创建用于实时监控的视图
CREATE VIEW monitoring_dashboard AS
SELECT 
    sensor_id,
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS window_end,
    AVG(temperature) AS avg_temperature,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'MOVING_AVERAGE',
        '{"window_size": 12, "type": "SIMPLE"}'
    ) AS temperature_ma,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'TREND_ANALYSIS',
        '{"type": "LINEAR"}'
    ) AS trend,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'ANOMALY_DETECTION',
        '{"method": "STANDARD_DEVIATION", "threshold": 3.0}'
    ) AS anomaly_score
FROM sensor_data
GROUP BY sensor_id, TUMBLE(event_time, INTERVAL '5' MINUTE);

-- 示例 10: 预测性维护
-- 基于传感器数据预测设备故障
CREATE VIEW predictive_maintenance AS
SELECT 
    sensor_id,
    TUMBLE_START(event_time, INTERVAL '1' HOUR) AS window_start,
    TIME_SERIES_ANALYSIS(
        temperature,
        event_time,
        'ANOMALY_DETECTION',
        '{"method": "STANDARD_DEVIATION", "threshold": 2.5}'
    ) AS temperature_anomaly,
    TIME_SERIES_ANALYSIS(
        humidity,
        event_time,
        'ANOMALY_DETECTION',
        '{"method": "STANDARD_DEVIATION", "threshold": 2.5}'
    ) AS humidity_anomaly,
    TIME_SERIES_ANALYSIS(
        pressure,
        event_time,
        'ANOMALY_DETECTION',
        '{"method": "STANDARD_DEVIATION", "threshold": 2.5}'
    ) AS pressure_anomaly,
    CASE 
        WHEN temperature_anomaly > 0 AND humidity_anomaly > 0 THEN 'HIGH'
        WHEN temperature_anomaly > 0 OR humidity_anomaly > 0 OR pressure_anomaly > 0 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS failure_risk
FROM sensor_data
GROUP BY sensor_id, TUMBLE(event_time, INTERVAL '1' HOUR);
