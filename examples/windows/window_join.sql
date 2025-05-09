-- window_join.sql
-- 这个示例展示了如何在窗口内连接多个数据流

-- 创建订单数据临时表
CREATE TEMPORARY TABLE orders (
    order_id INT,
    user_id INT,
    event_time TIMESTAMP(3),
    total_amount DOUBLE,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 创建支付数据临时表
CREATE TEMPORARY TABLE payments (
    payment_id INT,
    order_id INT,
    event_time TIMESTAMP(3),
    payment_method VARCHAR,
    amount DOUBLE,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 插入一些订单测试数据
INSERT INTO orders
SELECT 
    counter AS order_id,
    CAST(RAND() * 100 AS INT) AS user_id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,
    50 + RAND() * 450 AS total_amount
FROM impulse()
LIMIT 500;

-- 插入一些支付测试数据
-- 注意：我们确保每个订单都有对应的支付，但支付时间会稍晚于订单时间
INSERT INTO payments
SELECT 
    counter + 1000 AS payment_id,
    counter AS order_id,  -- 与订单ID对应
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND + INTERVAL CAST(RAND() * 300 AS INT) SECOND AS event_time,  -- 订单后0-5分钟内的支付
    ARRAY['credit_card', 'paypal', 'bank_transfer', 'crypto', 'gift_card'][FLOOR(RAND() * 5) + 1] AS payment_method,
    50 + RAND() * 450 AS amount  -- 金额与订单金额大致相同
FROM impulse()
LIMIT 500;

-- 1. 使用窗口连接分析订单和支付的关系
-- 在10分钟的滚动窗口内连接订单和支付数据
SELECT 
    o.order_id,
    o.user_id,
    o.total_amount AS order_amount,
    p.payment_id,
    p.payment_method,
    p.amount AS payment_amount,
    p.event_time - o.event_time AS payment_delay,
    TUMBLE_START(o.event_time, INTERVAL '10' MINUTE) AS window_start,
    TUMBLE_END(o.event_time, INTERVAL '10' MINUTE) AS window_end
FROM TABLE(
    TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '10' MINUTE)
) AS o
JOIN TABLE(
    TUMBLE(TABLE payments, DESCRIPTOR(event_time), INTERVAL '10' MINUTE)
) AS p
ON o.order_id = p.order_id AND o.window_start = p.window_start AND o.window_end = p.window_end
ORDER BY o.event_time
LIMIT 20;

-- 2. 使用窗口聚合分析每个时间窗口内的订单和支付统计
SELECT 
    TUMBLE_START(event_time, INTERVAL '15' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '15' MINUTE) AS window_end,
    'orders' AS stream_type,
    COUNT(*) AS record_count,
    SUM(total_amount) AS total_amount,
    AVG(total_amount) AS avg_amount
FROM TABLE(
    TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '15' MINUTE)
)
GROUP BY TUMBLE_START(event_time, INTERVAL '15' MINUTE), TUMBLE_END(event_time, INTERVAL '15' MINUTE)

UNION ALL

SELECT 
    TUMBLE_START(event_time, INTERVAL '15' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '15' MINUTE) AS window_end,
    'payments' AS stream_type,
    COUNT(*) AS record_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM TABLE(
    TUMBLE(TABLE payments, DESCRIPTOR(event_time), INTERVAL '15' MINUTE)
)
GROUP BY TUMBLE_START(event_time, INTERVAL '15' MINUTE), TUMBLE_END(event_time, INTERVAL '15' MINUTE)
ORDER BY window_start, stream_type;

-- 3. 使用会话窗口分析用户的订单行为
SELECT 
    user_id,
    SESSION_START(event_time, INTERVAL '30' MINUTE) AS session_start,
    SESSION_END(event_time, INTERVAL '30' MINUTE) AS session_end,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_spent,
    AVG(total_amount) AS avg_order_value
FROM TABLE(
    SESSION(TABLE orders, DESCRIPTOR(event_time), INTERVAL '30' MINUTE)
)
GROUP BY user_id, SESSION_START(event_time, INTERVAL '30' MINUTE), SESSION_END(event_time, INTERVAL '30' MINUTE)
HAVING COUNT(*) > 1  -- 只显示在一个会话中有多个订单的用户
ORDER BY order_count DESC, total_spent DESC
LIMIT 10;

-- 说明：
-- 1. 创建两个临时表：orders（订单数据）和 payments（支付数据）
-- 2. 为两个表的 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入测试数据，确保每个订单都有对应的支付，但支付时间会稍晚于订单时间
-- 4. 第一个查询：使用滚动窗口连接订单和支付数据，分析订单和支付的关系
-- 5. 第二个查询：使用窗口聚合分析每个时间窗口内的订单和支付统计
-- 6. 第三个查询：使用会话窗口分析用户的订单行为，找出在短时间内下多个订单的用户
-- 7. 窗口连接特点：在时间窗口内连接多个数据流，处理时间上相关但不完全匹配的事件
-- 8. 这种连接方式非常适合分析业务流程中的相关事件，如订单-支付、点击-购买等
