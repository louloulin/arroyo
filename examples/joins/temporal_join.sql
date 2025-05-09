-- temporal_join.sql
-- 这个示例展示了如何使用时态连接（Temporal Join）处理随时间变化的维度表

-- 创建订单流数据临时表
CREATE TEMPORARY TABLE orders (
    order_id INT,
    product_id INT,
    user_id INT,
    event_time TIMESTAMP(3),
    quantity INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 创建产品维度表（随时间变化）
CREATE TABLE product_catalog (
    product_id INT,
    product_name VARCHAR,
    category VARCHAR,
    price DOUBLE,
    valid_from TIMESTAMP(3),
    valid_to TIMESTAMP(3)
);

-- 创建用户维度表（随时间变化）
CREATE TABLE user_profiles (
    user_id INT,
    user_level VARCHAR,
    discount_rate DOUBLE,
    valid_from TIMESTAMP(3),
    valid_to TIMESTAMP(3)
);

-- 插入一些订单测试数据
INSERT INTO orders
SELECT 
    counter AS order_id,
    CAST(RAND() * 20 AS INT) AS product_id,
    CAST(RAND() * 50 AS INT) AS user_id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 86400 AS INT) SECOND AS event_time,
    CAST(RAND() * 5 + 1 AS INT) AS quantity
FROM impulse()
LIMIT 1000;

-- 插入产品数据（包括历史版本）
-- 产品1的价格历史
INSERT INTO product_catalog VALUES
    (1, 'Laptop Pro', 'Electronics', 1299.99, TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-03-31 23:59:59'),
    (1, 'Laptop Pro', 'Electronics', 1199.99, TIMESTAMP '2023-04-01 00:00:00', TIMESTAMP '2023-06-30 23:59:59'),
    (1, 'Laptop Pro 2023', 'Electronics', 1399.99, TIMESTAMP '2023-07-01 00:00:00', NULL);

-- 产品2的价格历史
INSERT INTO product_catalog VALUES
    (2, 'Smartphone X', 'Electronics', 899.99, TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-05-14 23:59:59'),
    (2, 'Smartphone X', 'Electronics', 799.99, TIMESTAMP '2023-05-15 00:00:00', NULL);

-- 其他产品（当前版本）
INSERT INTO product_catalog
SELECT 
    counter + 2 AS product_id,
    CONCAT('Product ', counter + 2) AS product_name,
    ARRAY['Electronics', 'Clothing', 'Home', 'Books', 'Sports'][FLOOR(RAND() * 5) + 1] AS category,
    50 + RAND() * 950 AS price,
    TIMESTAMP '2023-01-01 00:00:00' AS valid_from,
    NULL AS valid_to
FROM impulse()
LIMIT 18;

-- 插入用户数据（包括历史版本）
-- 一些用户的等级变化
INSERT INTO user_profiles VALUES
    (1, 'Bronze', 0.00, TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-04-30 23:59:59'),
    (1, 'Silver', 0.05, TIMESTAMP '2023-05-01 00:00:00', NULL),
    (2, 'Bronze', 0.00, TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-03-31 23:59:59'),
    (2, 'Silver', 0.05, TIMESTAMP '2023-04-01 00:00:00', TIMESTAMP '2023-07-31 23:59:59'),
    (2, 'Gold', 0.10, TIMESTAMP '2023-08-01 00:00:00', NULL),
    (3, 'Silver', 0.05, TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-06-30 23:59:59'),
    (3, 'Gold', 0.10, TIMESTAMP '2023-07-01 00:00:00', NULL);

-- 其他用户（当前版本）
INSERT INTO user_profiles
SELECT 
    counter + 3 AS user_id,
    CASE 
        WHEN RAND() < 0.7 THEN 'Bronze'
        WHEN RAND() < 0.9 THEN 'Silver'
        ELSE 'Gold'
    END AS user_level,
    CASE 
        WHEN user_level = 'Bronze' THEN 0.00
        WHEN user_level = 'Silver' THEN 0.05
        ELSE 0.10
    END AS discount_rate,
    TIMESTAMP '2023-01-01 00:00:00' AS valid_from,
    NULL AS valid_to
FROM impulse()
LIMIT 47;

-- 1. 基本时态连接：连接订单与当时有效的产品和用户信息
SELECT 
    o.order_id,
    o.event_time,
    o.product_id,
    p.product_name,
    p.category,
    p.price,
    o.quantity,
    o.user_id,
    u.user_level,
    u.discount_rate,
    p.price * o.quantity AS gross_amount,
    p.price * o.quantity * (1 - u.discount_rate) AS net_amount
FROM orders o
JOIN product_catalog FOR SYSTEM_TIME AS OF o.event_time AS p
    ON o.product_id = p.product_id
JOIN user_profiles FOR SYSTEM_TIME AS OF o.event_time AS u
    ON o.user_id = u.user_id
ORDER BY o.event_time DESC
LIMIT 20;

-- 2. 分析产品价格变化对销售的影响
WITH product_sales AS (
    SELECT 
        o.product_id,
        p.product_name,
        p.price,
        p.valid_from,
        p.valid_to,
        COUNT(*) AS order_count,
        SUM(o.quantity) AS total_quantity,
        SUM(p.price * o.quantity) AS total_sales
    FROM orders o
    JOIN product_catalog FOR SYSTEM_TIME AS OF o.event_time AS p
        ON o.product_id = p.product_id
    GROUP BY o.product_id, p.product_name, p.price, p.valid_from, p.valid_to
)
SELECT 
    product_id,
    product_name,
    price,
    valid_from,
    valid_to,
    order_count,
    total_quantity,
    total_sales,
    order_count / DATEDIFF(
        COALESCE(valid_to, CURRENT_TIMESTAMP), 
        valid_from
    ) * 86400 AS orders_per_day,
    total_quantity / DATEDIFF(
        COALESCE(valid_to, CURRENT_TIMESTAMP), 
        valid_from
    ) * 86400 AS quantity_per_day
FROM product_sales
WHERE product_id IN (1, 2)  -- 只分析有价格变化的产品
ORDER BY product_id, valid_from;

-- 3. 分析用户等级变化对购买行为的影响
WITH user_purchases AS (
    SELECT 
        o.user_id,
        u.user_level,
        u.discount_rate,
        u.valid_from,
        u.valid_to,
        COUNT(*) AS order_count,
        SUM(p.price * o.quantity) AS gross_sales,
        SUM(p.price * o.quantity * (1 - u.discount_rate)) AS net_sales
    FROM orders o
    JOIN product_catalog FOR SYSTEM_TIME AS OF o.event_time AS p
        ON o.product_id = p.product_id
    JOIN user_profiles FOR SYSTEM_TIME AS OF o.event_time AS u
        ON o.user_id = u.user_id
    GROUP BY o.user_id, u.user_level, u.discount_rate, u.valid_from, u.valid_to
)
SELECT 
    user_id,
    user_level,
    discount_rate,
    valid_from,
    valid_to,
    order_count,
    gross_sales,
    net_sales,
    order_count / DATEDIFF(
        COALESCE(valid_to, CURRENT_TIMESTAMP), 
        valid_from
    ) * 30 AS orders_per_month,
    gross_sales / DATEDIFF(
        COALESCE(valid_to, CURRENT_TIMESTAMP), 
        valid_from
    ) * 30 AS gross_sales_per_month
FROM user_purchases
WHERE user_id IN (1, 2, 3)  -- 只分析有等级变化的用户
ORDER BY user_id, valid_from;

-- 说明：
-- 1. 创建三个表：orders（订单流数据）、product_catalog（产品维度表）和 user_profiles（用户维度表）
-- 2. 为 orders 表的 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 维度表（product_catalog 和 user_profiles）包含有效期字段（valid_from 和 valid_to），记录数据的有效时间范围
-- 4. 插入测试数据，包括订单数据和维度表的历史版本
-- 5. 第一个查询：基本时态连接，连接订单与当时有效的产品和用户信息
-- 6. 第二个查询：分析产品价格变化对销售的影响，比较不同价格下的销售情况
-- 7. 第三个查询：分析用户等级变化对购买行为的影响，比较用户在不同等级下的购买行为
-- 8. 时态连接特点：根据事件时间连接维度表的历史版本，处理随时间变化的维度数据
-- 9. 这种连接方式非常适合处理缓慢变化维度（Slowly Changing Dimensions）和历史分析
