-- sliding_window.sql
-- 这个示例展示了如何使用滑动窗口（固定大小、可重叠的窗口）进行聚合操作

-- 创建一个带有事件时间和水印的临时表
CREATE TEMPORARY TABLE stock_data (
    symbol VARCHAR,
    event_time TIMESTAMP(3),
    price DOUBLE,
    volume INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 插入一些测试数据
INSERT INTO stock_data
SELECT 
    ARRAY['AAPL', 'MSFT', 'GOOG', 'AMZN', 'META'][FLOOR(RAND() * 5) + 1] AS symbol,  -- 5个股票符号
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,  -- 过去1小时内的随机时间
    100 + RAND() * 200 AS price,  -- 100-300之间的随机价格
    CAST(RAND() * 1000 AS INT) AS volume  -- 0-999之间的随机交易量
FROM impulse()
LIMIT 1000;

-- 使用滑动窗口计算每个股票在每个5分钟窗口内的平均价格和总交易量
-- 窗口大小为5分钟，每1分钟滑动一次
SELECT 
    symbol,
    HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_start,
    HOP_END(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_end,
    COUNT(*) AS trade_count,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    SUM(volume) AS total_volume
FROM TABLE(
    HOP(TABLE stock_data, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
)
GROUP BY symbol, HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE), HOP_END(event_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
ORDER BY window_start, symbol;

-- 说明：
-- 1. 创建一个临时表 stock_data，包含股票交易数据（符号、时间、价格、交易量）
-- 2. 为 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入 1000 条随机测试数据，模拟 5 个股票在过去 1 小时内的交易
-- 4. 使用 HOP 函数创建滑动窗口，窗口大小为 5 分钟，每 1 分钟滑动一次
-- 5. 对每个股票的每个窗口计算交易次数、平均/最小/最大价格和总交易量
-- 6. 滑动窗口特点：固定大小（5分钟），窗口之间有重叠（每1分钟滑动一次）
-- 7. 结果按窗口开始时间和股票符号排序
-- 8. 与滚动窗口相比，滑动窗口可以提供更平滑的指标变化，但会产生更多的计算负担
