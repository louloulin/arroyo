-- simple_filter.sql
-- 这个示例展示了如何使用 WHERE 子句过滤数据流中的记录

-- 使用 impulse() 源生成测试数据
-- impulse() 源会生成包含 counter 和 timestamp 字段的记录
SELECT 
    counter,
    CAST(counter AS DOUBLE) / 10 AS value,
    timestamp
FROM impulse()
WHERE counter % 2 = 0  -- 只保留偶数记录
LIMIT 100;  -- 限制结果数量

-- 说明：
-- 1. impulse() 是 Arroyo 内置的测试数据源，生成递增的计数器和时间戳
-- 2. WHERE 子句用于过滤数据，这里只保留 counter 为偶数的记录
-- 3. LIMIT 子句限制结果数量为 100 条
-- 4. 这个查询将生成 50 条记录，counter 值为 0, 2, 4, ..., 98
