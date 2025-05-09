-- stream_stream_join.sql
-- 这个示例展示了如何连接两个数据流，处理两个流中的相关事件

-- 创建点击数据临时表
CREATE TEMPORARY TABLE clicks (
    user_id INT,
    event_time TIMESTAMP(3),
    page_id VARCHAR,
    item_id INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 创建购买数据临时表
CREATE TEMPORARY TABLE purchases (
    user_id INT,
    event_time TIMESTAMP(3),
    item_id INT,
    quantity INT,
    price DOUBLE,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 插入一些点击测试数据
INSERT INTO clicks
SELECT 
    CAST(RAND() * 100 AS INT) AS user_id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,
    CONCAT('page_', CAST(RAND() * 20 AS INT)) AS page_id,
    CAST(RAND() * 50 AS INT) AS item_id
FROM impulse()
LIMIT 1000;

-- 插入一些购买测试数据
-- 注意：我们确保一部分购买与点击相关联
INSERT INTO purchases
SELECT 
    user_id,  -- 使用相同的用户ID
    event_time + INTERVAL CAST(RAND() * 600 AS INT) SECOND AS event_time,  -- 点击后0-10分钟内的购买
    item_id,  -- 使用相同的商品ID
    CAST(RAND() * 5 + 1 AS INT) AS quantity,  -- 1-5件的随机数量
    10 + RAND() * 90 AS price  -- 10-100元的随机价格
FROM (
    SELECT * FROM clicks WHERE RAND() < 0.3  -- 30%的点击会导致购买
)
LIMIT 300;

-- 再插入一些随机购买数据（无对应点击）
INSERT INTO purchases
SELECT 
    CAST(RAND() * 100 AS INT) AS user_id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,
    CAST(RAND() * 50 AS INT) AS item_id,
    CAST(RAND() * 5 + 1 AS INT) AS quantity,
    10 + RAND() * 90 AS price
FROM impulse()
LIMIT 200;

-- 1. 基本流-流连接：连接点击和购买事件
-- 找出点击后10分钟内发生的购买
SELECT 
    c.user_id,
    c.page_id,
    c.item_id,
    c.event_time AS click_time,
    p.event_time AS purchase_time,
    p.quantity,
    p.price,
    p.quantity * p.price AS total_amount,
    p.event_time - c.event_time AS time_to_purchase
FROM clicks c
JOIN purchases p
ON c.user_id = p.user_id AND c.item_id = p.item_id
WHERE p.event_time BETWEEN c.event_time AND c.event_time + INTERVAL '10' MINUTE
ORDER BY c.event_time
LIMIT 20;

-- 2. 计算每个页面的转化率
SELECT 
    c.page_id,
    COUNT(DISTINCT c.user_id) AS unique_visitors,
    COUNT(DISTINCT p.user_id) AS unique_buyers,
    COUNT(DISTINCT p.user_id) * 100.0 / COUNT(DISTINCT c.user_id) AS conversion_rate,
    AVG(p.quantity * p.price) AS avg_order_value,
    SUM(p.quantity * p.price) AS total_revenue
FROM clicks c
LEFT JOIN purchases p
ON c.user_id = p.user_id AND c.item_id = p.item_id
WHERE p.event_time IS NULL OR (p.event_time BETWEEN c.event_time AND c.event_time + INTERVAL '10' MINUTE)
GROUP BY c.page_id
ORDER BY conversion_rate DESC;

-- 3. 分析用户的点击到购买路径
WITH user_journey AS (
    SELECT 
        c.user_id,
        c.page_id,
        c.item_id,
        c.event_time AS click_time,
        p.event_time AS purchase_time,
        p.quantity,
        p.price,
        ROW_NUMBER() OVER (PARTITION BY c.user_id, c.item_id, p.event_time ORDER BY c.event_time DESC) AS click_rank
    FROM clicks c
    JOIN purchases p
    ON c.user_id = p.user_id AND c.item_id = p.item_id
    WHERE p.event_time BETWEEN c.event_time AND c.event_time + INTERVAL '10' MINUTE
)
SELECT 
    user_id,
    STRING_AGG(page_id, ' -> ' ORDER BY click_time) AS click_path,
    COUNT(*) AS clicks_before_purchase,
    purchase_time - MIN(click_time) AS journey_duration,
    quantity * price AS purchase_amount
FROM user_journey
WHERE click_rank = 1
GROUP BY user_id, purchase_time, quantity, price
ORDER BY journey_duration DESC
LIMIT 10;

-- 说明：
-- 1. 创建两个临时表：clicks（点击数据）和 purchases（购买数据）
-- 2. 为两个表的 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入测试数据，确保一部分购买与点击相关联（30%的点击会导致购买）
-- 4. 第一个查询：基本流-流连接，找出点击后10分钟内发生的购买
-- 5. 第二个查询：计算每个页面的转化率，分析页面性能
-- 6. 第三个查询：分析用户的点击到购买路径，了解用户行为
-- 7. 流-流连接特点：连接两个动态变化的数据流，处理相关事件
-- 8. 这种连接方式非常适合分析用户行为、事件关联和转化漏斗
