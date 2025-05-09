-- data_generation.sql
-- 这个示例展示了如何使用 impulse() 和其他函数生成测试数据

-- 1. 基本 impulse() 源
-- impulse() 源会生成包含 counter 和 timestamp 字段的记录
SELECT 
    counter,
    timestamp
FROM impulse()
LIMIT 10;

-- 2. 生成随机数据
SELECT 
    counter,
    timestamp,
    RAND() AS random_value,                   -- 0.0 到 1.0 之间的随机数
    RAND() * 100 AS random_0_to_100,          -- 0.0 到 100.0 之间的随机数
    FLOOR(RAND() * 100) AS random_int_0_to_99, -- 0 到 99 之间的随机整数
    CASE 
        WHEN RAND() > 0.5 THEN TRUE 
        ELSE FALSE 
    END AS random_boolean,
    ARRAY[RAND(), RAND(), RAND()] AS random_array -- 随机数组
FROM impulse()
LIMIT 10;

-- 3. 生成随机字符串
SELECT 
    counter,
    timestamp,
    CONCAT('user_', CAST(FLOOR(RAND() * 1000) AS VARCHAR)) AS random_user_id,
    CASE 
        WHEN RAND() < 0.33 THEN 'small'
        WHEN RAND() < 0.66 THEN 'medium'
        ELSE 'large'
    END AS random_size,
    ARRAY['red', 'green', 'blue', 'yellow', 'black'][FLOOR(RAND() * 5) + 1] AS random_color
FROM impulse()
LIMIT 10;

-- 4. 生成随机日期时间
SELECT 
    counter,
    timestamp,
    timestamp - INTERVAL CAST(RAND() * 86400 AS INT) SECOND AS random_past_timestamp, -- 过去 24 小时内的随机时间
    CURRENT_DATE - INTERVAL CAST(RAND() * 365 AS INT) DAY AS random_past_date, -- 过去一年内的随机日期
    EXTRACT(HOUR FROM timestamp) AS current_hour,
    EXTRACT(MINUTE FROM timestamp) AS current_minute
FROM impulse()
LIMIT 10;

-- 5. 生成结构化数据（模拟电子商务订单）
SELECT 
    counter AS order_id,
    timestamp AS order_time,
    CONCAT('user_', CAST(FLOOR(RAND() * 1000) AS VARCHAR)) AS user_id,
    CONCAT('product_', CAST(FLOOR(RAND() * 100) AS VARCHAR)) AS product_id,
    FLOOR(RAND() * 10) + 1 AS quantity,
    ROUND(RAND() * 1000, 2) AS price,
    CASE 
        WHEN RAND() < 0.7 THEN 'completed'
        WHEN RAND() < 0.9 THEN 'pending'
        ELSE 'cancelled'
    END AS status,
    ARRAY['credit_card', 'paypal', 'bank_transfer'][FLOOR(RAND() * 3) + 1] AS payment_method
FROM impulse()
LIMIT 20;

-- 说明：
-- 1. impulse() 是 Arroyo 内置的测试数据源，生成递增的计数器和时间戳
-- 2. 使用 RAND() 函数生成随机值
-- 3. 使用 CASE 表达式和数组索引生成随机分类数据
-- 4. 使用日期时间函数生成随机的时间和日期
-- 5. 组合以上技术生成模拟的结构化数据
