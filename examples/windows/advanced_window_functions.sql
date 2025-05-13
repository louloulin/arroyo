-- 高级窗口聚合函数示例
-- 本示例展示如何使用 Arroyo 的高级窗口聚合函数

-- 创建传感器数据流
CREATE TABLE sensor_data (
    sensor_id STRING,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    timestamp TIMESTAMP,
    WATERMARK FOR timestamp AS timestamp - INTERVAL '30' SECOND
) WITH (
    connector = 'kafka',
    bootstrap.servers = 'localhost:9092',
    topic = 'sensor_data',
    format = 'json'
);

-- 使用滚动窗口和方差函数分析温度波动
SELECT 
    sensor_id,
    TUMBLE_START(timestamp, INTERVAL '1' HOUR) AS window_start,
    TUMBLE_END(timestamp, INTERVAL '1' HOUR) AS window_end,
    COUNT(*) AS reading_count,
    AVG(temperature) AS avg_temperature,
    -- 总体方差
    VAR_POP(temperature) AS temperature_variance_pop,
    -- 样本方差
    VAR_SAMP(temperature) AS temperature_variance_samp,
    -- 总体标准差
    STDDEV_POP(temperature) AS temperature_stddev_pop,
    -- 样本标准差
    STDDEV_SAMP(temperature) AS temperature_stddev_samp
FROM sensor_data
GROUP BY 
    sensor_id,
    TUMBLE(timestamp, INTERVAL '1' HOUR);

-- 使用滑动窗口和百分位数函数分析温度分布
SELECT 
    sensor_id,
    HOP_START(timestamp, INTERVAL '10' MINUTE, INTERVAL '1' HOUR) AS window_start,
    HOP_END(timestamp, INTERVAL '10' MINUTE, INTERVAL '1' HOUR) AS window_end,
    COUNT(*) AS reading_count,
    -- 中位数（50th 百分位数）
    PERCENTILE_CONT(temperature, 0.5) AS temperature_median,
    -- 25th 百分位数
    PERCENTILE_CONT(temperature, 0.25) AS temperature_p25,
    -- 75th 百分位数
    PERCENTILE_CONT(temperature, 0.75) AS temperature_p75,
    -- 四分位距（IQR）
    PERCENTILE_CONT(temperature, 0.75) - PERCENTILE_CONT(temperature, 0.25) AS temperature_iqr,
    -- 90th 百分位数
    PERCENTILE_CONT(temperature, 0.9) AS temperature_p90,
    -- 95th 百分位数
    PERCENTILE_CONT(temperature, 0.95) AS temperature_p95,
    -- 99th 百分位数
    PERCENTILE_CONT(temperature, 0.99) AS temperature_p99
FROM sensor_data
GROUP BY 
    sensor_id,
    HOP(timestamp, INTERVAL '10' MINUTE, INTERVAL '1' HOUR);

-- 使用会话窗口和相关性函数分析温度与湿度的关系
SELECT 
    sensor_id,
    SESSION_START(timestamp, INTERVAL '15' MINUTE) AS session_start,
    SESSION_END(timestamp, INTERVAL '15' MINUTE) AS session_end,
    COUNT(*) AS reading_count,
    -- 温度和湿度的协方差（总体）
    COVAR_POP(temperature, humidity) AS temp_humidity_covar_pop,
    -- 温度和湿度的协方差（样本）
    COVAR_SAMP(temperature, humidity) AS temp_humidity_covar_samp,
    -- 温度和湿度的相关系数
    CORR(temperature, humidity) AS temp_humidity_correlation,
    -- 温度和压力的相关系数
    CORR(temperature, pressure) AS temp_pressure_correlation,
    -- 湿度和压力的相关系数
    CORR(humidity, pressure) AS humidity_pressure_correlation
FROM sensor_data
GROUP BY 
    sensor_id,
    SESSION(timestamp, INTERVAL '15' MINUTE);

-- 使用全局窗口和高级聚合函数进行异常检测
SELECT 
    sensor_id,
    -- 使用 Z-Score 进行异常检测
    -- Z-Score = (x - mean) / stddev
    (temperature - AVG(temperature) OVER w) / STDDEV_SAMP(temperature) OVER w AS temperature_zscore,
    -- 使用 IQR 进行异常检测
    -- 下界 = Q1 - 1.5 * IQR
    -- 上界 = Q3 + 1.5 * IQR
    PERCENTILE_CONT(temperature, 0.25) OVER w - 
        1.5 * (PERCENTILE_CONT(temperature, 0.75) OVER w - PERCENTILE_CONT(temperature, 0.25) OVER w) 
        AS temperature_lower_bound,
    PERCENTILE_CONT(temperature, 0.75) OVER w + 
        1.5 * (PERCENTILE_CONT(temperature, 0.75) OVER w - PERCENTILE_CONT(temperature, 0.25) OVER w) 
        AS temperature_upper_bound,
    -- 判断是否为异常值
    CASE 
        WHEN temperature < PERCENTILE_CONT(temperature, 0.25) OVER w - 
            1.5 * (PERCENTILE_CONT(temperature, 0.75) OVER w - PERCENTILE_CONT(temperature, 0.25) OVER w) 
            THEN 'Low Outlier'
        WHEN temperature > PERCENTILE_CONT(temperature, 0.75) OVER w + 
            1.5 * (PERCENTILE_CONT(temperature, 0.75) OVER w - PERCENTILE_CONT(temperature, 0.25) OVER w) 
            THEN 'High Outlier'
        ELSE 'Normal'
    END AS temperature_outlier_status,
    temperature,
    humidity,
    pressure,
    timestamp
FROM sensor_data
WINDOW w AS (PARTITION BY sensor_id ORDER BY timestamp ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);
