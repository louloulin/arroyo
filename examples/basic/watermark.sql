-- watermark.sql
-- 这个示例展示了如何定义和使用水印处理事件时间

-- 创建一个带有事件时间和水印的临时表
CREATE TEMPORARY TABLE events (
    id INT,
    event_time TIMESTAMP(3),
    value DOUBLE,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 插入一些测试数据
INSERT INTO events
SELECT 
    counter AS id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 60 AS INT) SECOND AS event_time,
    RAND() * 100 AS value
FROM impulse()
LIMIT 100;

-- 使用事件时间窗口进行聚合
SELECT 
    TUMBLE_START(event_time, INTERVAL '10' SECOND) AS window_start,
    TUMBLE_END(event_time, INTERVAL '10' SECOND) AS window_end,
    COUNT(*) AS event_count,
    SUM(value) AS total_value,
    AVG(value) AS avg_value
FROM TABLE(
    TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
)
GROUP BY TUMBLE_START(event_time, INTERVAL '10' SECOND), TUMBLE_END(event_time, INTERVAL '10' SECOND);

-- 说明：
-- 1. 创建一个临时表 events，包含 id、event_time 和 value 字段
-- 2. 为 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入一些测试数据，事件时间随机分布在过去 60 秒内
-- 4. 使用 TUMBLE 函数创建 10 秒的滚动窗口，基于事件时间
-- 5. 对每个窗口计算事件数量、值总和和平均值
-- 6. 水印确保即使有延迟数据，窗口结果也能正确计算
