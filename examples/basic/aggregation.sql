-- aggregation.sql
-- 这个示例展示了如何使用聚合函数对数据流进行聚合操作

-- 使用 impulse() 源生成测试数据，并按照 counter % 5 分组进行聚合
SELECT 
    counter % 5 AS group_key,  -- 分组键
    
    -- 聚合函数
    COUNT(*) AS count,
    SUM(counter) AS sum_counter,
    AVG(counter) AS avg_counter,
    MIN(counter) AS min_counter,
    MAX(counter) AS max_counter,
    
    -- 高级聚合函数
    STDDEV(counter) AS stddev_counter,
    VARIANCE(counter) AS variance_counter,
    
    -- 字符串聚合
    STRING_AGG(CAST(counter AS VARCHAR), ',') AS counters_string
FROM (
    SELECT counter FROM impulse() LIMIT 100
)
GROUP BY counter % 5;

-- 说明：
-- 1. 这个示例使用 counter % 5 作为分组键，将记录分为 5 组
-- 2. 对每组记录应用各种聚合函数
-- 3. 包括基本聚合函数（COUNT, SUM, AVG, MIN, MAX）和高级聚合函数（STDDEV, VARIANCE）
-- 4. STRING_AGG 函数将每组的 counter 值连接成一个字符串
-- 5. 结果将包含 5 条记录，每条记录对应一个分组
