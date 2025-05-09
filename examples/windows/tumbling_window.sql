-- tumbling_window.sql
-- 这个示例展示了如何使用滚动窗口（固定大小、不重叠的窗口）进行聚合操作

-- 创建一个带有事件时间和水印的临时表
CREATE TEMPORARY TABLE sensor_data (
    sensor_id INT,
    event_time TIMESTAMP(3),
    temperature DOUBLE,
    humidity DOUBLE,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 插入一些测试数据
INSERT INTO sensor_data
SELECT 
    CAST(RAND() * 10 AS INT) AS sensor_id,  -- 10个传感器（ID 0-9）
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,  -- 过去1小时内的随机时间
    15 + RAND() * 25 AS temperature,  -- 15-40度之间的随机温度
    30 + RAND() * 60 AS humidity  -- 30-90%之间的随机湿度
FROM impulse()
LIMIT 1000;

-- 使用滚动窗口计算每个传感器在每个10分钟窗口内的平均温度和湿度
SELECT 
    sensor_id,
    TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '10' MINUTE) AS window_end,
    COUNT(*) AS reading_count,
    AVG(temperature) AS avg_temperature,
    MIN(temperature) AS min_temperature,
    MAX(temperature) AS max_temperature,
    AVG(humidity) AS avg_humidity
FROM TABLE(
    TUMBLE(TABLE sensor_data, DESCRIPTOR(event_time), INTERVAL '10' MINUTE)
)
GROUP BY sensor_id, TUMBLE_START(event_time, INTERVAL '10' MINUTE), TUMBLE_END(event_time, INTERVAL '10' MINUTE)
ORDER BY window_start, sensor_id;

-- 说明：
-- 1. 创建一个临时表 sensor_data，包含传感器数据（ID、时间、温度、湿度）
-- 2. 为 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入 1000 条随机测试数据，模拟 10 个传感器在过去 1 小时内的读数
-- 4. 使用 TUMBLE 函数创建 10 分钟的滚动窗口，基于事件时间
-- 5. 对每个传感器的每个窗口计算读数数量、平均/最小/最大温度和平均湿度
-- 6. 滚动窗口特点：固定大小（10分钟），窗口之间不重叠
-- 7. 结果按窗口开始时间和传感器 ID 排序
