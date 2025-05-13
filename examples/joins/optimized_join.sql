-- 优化的流式 JOIN 示例
-- 本示例展示了 Arroyo 中优化的流式 JOIN 操作

-- 创建订单流
CREATE TABLE orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    price DOUBLE,
    order_time TIMESTAMP,
    WATERMARK FOR order_time AS order_time - INTERVAL '30' SECOND
) WITH (
    connector = 'kafka',
    bootstrap.servers = 'localhost:9092',
    topic = 'orders',
    format = 'json'
);

-- 创建客户维度表
CREATE TABLE customers (
    customer_id STRING PRIMARY KEY,
    customer_name STRING,
    customer_email STRING,
    customer_address STRING,
    customer_segment STRING,
    last_updated TIMESTAMP
) WITH (
    connector = 'jdbc',
    url = 'jdbc:postgresql://localhost:5432/arroyo',
    table = 'customers',
    user = 'arroyo',
    password = 'arroyo'
);

-- 创建产品维度表
CREATE TABLE products (
    product_id STRING PRIMARY KEY,
    product_name STRING,
    product_category STRING,
    product_price DOUBLE,
    product_inventory INT,
    last_updated TIMESTAMP
) WITH (
    connector = 'jdbc',
    url = 'jdbc:postgresql://localhost:5432/arroyo',
    table = 'products',
    user = 'arroyo',
    password = 'arroyo'
);

-- 创建支付流
CREATE TABLE payments (
    payment_id STRING,
    order_id STRING,
    payment_method STRING,
    payment_amount DOUBLE,
    payment_status STRING,
    payment_time TIMESTAMP,
    WATERMARK FOR payment_time AS payment_time - INTERVAL '30' SECOND
) WITH (
    connector = 'kafka',
    bootstrap.servers = 'localhost:9092',
    topic = 'payments',
    format = 'json'
);

-- 示例 1: 优化的查找 JOIN（流-表 JOIN）
-- 这个查询将订单流与客户和产品维度表连接，丰富订单数据
-- 优化器会自动将这个 JOIN 转换为查找 JOIN，提高性能
SELECT
    o.order_id,
    o.order_time,
    c.customer_name,
    c.customer_segment,
    p.product_name,
    p.product_category,
    o.quantity,
    o.price,
    o.quantity * o.price AS total_amount
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.order_time AS c
    ON o.customer_id = c.customer_id
JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p
    ON o.product_id = p.product_id;

-- 示例 2: 优化的窗口 JOIN（流-流 JOIN）
-- 这个查询将订单流与支付流在时间窗口内连接，关联订单和支付事件
-- 优化器会自动将这个 JOIN 转换为窗口 JOIN，提高性能
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.price,
    o.order_time,
    p.payment_id,
    p.payment_method,
    p.payment_amount,
    p.payment_status,
    p.payment_time,
    p.payment_time - o.order_time AS payment_delay
FROM orders o
JOIN payments p
    ON o.order_id = p.order_id
WHERE p.payment_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR;

-- 示例 3: 优化的分区 JOIN（大表-大表 JOIN）
-- 这个查询将两个大表连接，优化器会自动选择分区 JOIN 策略
SELECT
    o1.order_id AS order_id_1,
    o2.order_id AS order_id_2,
    o1.customer_id,
    o1.product_id,
    o2.product_id AS related_product_id,
    o1.order_time,
    o2.order_time AS related_order_time
FROM orders o1
JOIN orders o2
    ON o1.customer_id = o2.customer_id
    AND o1.order_id <> o2.order_id
WHERE o2.order_time BETWEEN o1.order_time AND o1.order_time + INTERVAL '7' DAY;

-- 示例 4: 优化的多路 JOIN
-- 这个查询将多个流和表连接，优化器会自动选择最优的 JOIN 策略组合
SELECT
    o.order_id,
    o.order_time,
    c.customer_name,
    p.product_name,
    o.quantity,
    o.price,
    pay.payment_method,
    pay.payment_status,
    pay.payment_time - o.order_time AS payment_delay
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.order_time AS c
    ON o.customer_id = c.customer_id
JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p
    ON o.product_id = p.product_id
JOIN payments pay
    ON o.order_id = pay.order_id
WHERE pay.payment_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR
AND c.customer_segment = 'VIP'
AND p.product_category = 'Electronics';

-- 示例 5: 优化的聚合 JOIN
-- 这个查询在 JOIN 前进行聚合，减少参与 JOIN 的数据量
SELECT
    c.customer_segment,
    p.product_category,
    COUNT(*) AS order_count,
    SUM(o.quantity) AS total_quantity,
    SUM(o.quantity * o.price) AS total_amount,
    AVG(pay.payment_time - o.order_time) AS avg_payment_delay
FROM orders o
JOIN customers FOR SYSTEM_TIME AS OF o.order_time AS c
    ON o.customer_id = c.customer_id
JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p
    ON o.product_id = p.product_id
JOIN payments pay
    ON o.order_id = pay.order_id
WHERE pay.payment_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR
GROUP BY
    c.customer_segment,
    p.product_category;
