-- global_window.sql
-- 这个示例展示了如何使用全局窗口（处理整个数据流）进行聚合操作

-- 创建一个带有事件时间和水印的临时表
CREATE TEMPORARY TABLE sales_data (
    product_id INT,
    category VARCHAR,
    event_time TIMESTAMP(3),
    amount DOUBLE,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 插入一些测试数据
INSERT INTO sales_data
SELECT 
    CAST(RAND() * 100 AS INT) AS product_id,  -- 100个产品（ID 0-99）
    ARRAY['electronics', 'clothing', 'food', 'books', 'home'][FLOOR(RAND() * 5) + 1] AS category,  -- 5个类别
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 86400 AS INT) SECOND AS event_time,  -- 过去24小时内的随机时间
    10 + RAND() * 990 AS amount  -- 10-1000之间的随机销售金额
FROM impulse()
LIMIT 5000;

-- 使用全局窗口计算每个类别的总销售额和平均销售额
-- 注意：全局窗口处理整个数据流，不按时间分割
SELECT 
    category,
    COUNT(*) AS sale_count,
    SUM(amount) AS total_sales,
    AVG(amount) AS avg_sale,
    MIN(amount) AS min_sale,
    MAX(amount) AS max_sale,
    COUNT(DISTINCT product_id) AS unique_products
FROM sales_data
GROUP BY category
ORDER BY total_sales DESC;

-- 使用全局窗口计算每小时的销售趋势
SELECT 
    EXTRACT(HOUR FROM event_time) AS hour_of_day,
    COUNT(*) AS sale_count,
    SUM(amount) AS total_sales,
    AVG(amount) AS avg_sale
FROM sales_data
GROUP BY EXTRACT(HOUR FROM event_time)
ORDER BY hour_of_day;

-- 使用全局窗口计算每个产品的销售排名
SELECT 
    product_id,
    category,
    COUNT(*) AS sale_count,
    SUM(amount) AS total_sales,
    RANK() OVER (ORDER BY SUM(amount) DESC) AS sales_rank,
    DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS dense_sales_rank,
    RANK() OVER (PARTITION BY category ORDER BY SUM(amount) DESC) AS category_sales_rank
FROM sales_data
GROUP BY product_id, category
ORDER BY total_sales DESC
LIMIT 20;

-- 说明：
-- 1. 创建一个临时表 sales_data，包含销售数据（产品ID、类别、时间、金额）
-- 2. 为 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入 5000 条随机测试数据，模拟 100 个产品在过去 24 小时内的销售
-- 4. 第一个查询：使用全局窗口按类别分组，计算每个类别的销售统计信息
-- 5. 第二个查询：使用全局窗口按小时分组，计算每小时的销售趋势
-- 6. 第三个查询：使用全局窗口和窗口函数计算产品销售排名
-- 7. 全局窗口特点：处理整个数据流，不按时间分割，适合计算全局统计信息和排名
-- 8. 注意：在实际的流处理中，全局窗口会随着新数据的到来不断更新结果
