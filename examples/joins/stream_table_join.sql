-- stream_table_join.sql
-- 这个示例展示了如何将流数据与静态表数据连接，丰富流数据的上下文

-- 创建交易流数据临时表
CREATE TEMPORARY TABLE transactions (
    transaction_id INT,
    user_id INT,
    event_time TIMESTAMP(3),
    amount DOUBLE,
    currency VARCHAR,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 创建用户维度表
CREATE TABLE users (
    user_id INT,
    username VARCHAR,
    country VARCHAR,
    age INT,
    registration_date DATE,
    user_type VARCHAR
);

-- 创建货币汇率维度表
CREATE TABLE exchange_rates (
    currency VARCHAR,
    rate_to_usd DOUBLE,
    last_updated TIMESTAMP(3)
);

-- 插入一些交易测试数据
INSERT INTO transactions
SELECT 
    counter AS transaction_id,
    CAST(RAND() * 100 AS INT) AS user_id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,
    10 + RAND() * 990 AS amount,
    ARRAY['USD', 'EUR', 'GBP', 'JPY', 'CNY'][FLOOR(RAND() * 5) + 1] AS currency
FROM impulse()
LIMIT 1000;

-- 插入一些用户数据
INSERT INTO users
SELECT 
    counter AS user_id,
    CONCAT('user_', counter) AS username,
    ARRAY['US', 'UK', 'DE', 'FR', 'CN', 'JP', 'CA', 'AU', 'BR', 'IN'][FLOOR(RAND() * 10) + 1] AS country,
    18 + CAST(RAND() * 62 AS INT) AS age,
    CURRENT_DATE - INTERVAL CAST(RAND() * 1000 AS INT) DAY AS registration_date,
    CASE WHEN RAND() < 0.2 THEN 'premium' ELSE 'standard' END AS user_type
FROM impulse()
LIMIT 100;

-- 插入货币汇率数据
INSERT INTO exchange_rates VALUES
    ('USD', 1.0, CURRENT_TIMESTAMP),
    ('EUR', 1.1, CURRENT_TIMESTAMP),
    ('GBP', 1.3, CURRENT_TIMESTAMP),
    ('JPY', 0.0091, CURRENT_TIMESTAMP),
    ('CNY', 0.15, CURRENT_TIMESTAMP);

-- 1. 基本流-表连接：丰富交易数据
SELECT 
    t.transaction_id,
    t.event_time,
    t.amount,
    t.currency,
    u.username,
    u.country,
    u.age,
    u.user_type,
    e.rate_to_usd,
    t.amount * e.rate_to_usd AS amount_usd
FROM transactions t
JOIN users u ON t.user_id = u.user_id
JOIN exchange_rates e ON t.currency = e.currency
ORDER BY t.event_time DESC
LIMIT 20;

-- 2. 按国家和用户类型分析交易
SELECT 
    u.country,
    u.user_type,
    COUNT(*) AS transaction_count,
    SUM(t.amount * e.rate_to_usd) AS total_amount_usd,
    AVG(t.amount * e.rate_to_usd) AS avg_amount_usd,
    MIN(t.amount * e.rate_to_usd) AS min_amount_usd,
    MAX(t.amount * e.rate_to_usd) AS max_amount_usd
FROM transactions t
JOIN users u ON t.user_id = u.user_id
JOIN exchange_rates e ON t.currency = e.currency
GROUP BY u.country, u.user_type
ORDER BY total_amount_usd DESC;

-- 3. 分析用户年龄段的交易行为
SELECT 
    CASE 
        WHEN u.age < 25 THEN '18-24'
        WHEN u.age < 35 THEN '25-34'
        WHEN u.age < 45 THEN '35-44'
        WHEN u.age < 55 THEN '45-54'
        WHEN u.age < 65 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT t.user_id) AS unique_users,
    SUM(t.amount * e.rate_to_usd) AS total_amount_usd,
    AVG(t.amount * e.rate_to_usd) AS avg_amount_usd
FROM transactions t
JOIN users u ON t.user_id = u.user_id
JOIN exchange_rates e ON t.currency = e.currency
GROUP BY 
    CASE 
        WHEN u.age < 25 THEN '18-24'
        WHEN u.age < 35 THEN '25-34'
        WHEN u.age < 45 THEN '35-44'
        WHEN u.age < 55 THEN '45-54'
        WHEN u.age < 65 THEN '55-64'
        ELSE '65+'
    END
ORDER BY age_group;

-- 4. 分析用户注册时长与交易行为的关系
SELECT 
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, u.registration_date) < 30 THEN '< 30 days'
        WHEN DATEDIFF(CURRENT_DATE, u.registration_date) < 90 THEN '30-89 days'
        WHEN DATEDIFF(CURRENT_DATE, u.registration_date) < 180 THEN '90-179 days'
        WHEN DATEDIFF(CURRENT_DATE, u.registration_date) < 365 THEN '180-364 days'
        ELSE '365+ days'
    END AS account_age,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT t.user_id) AS unique_users,
    SUM(t.amount * e.rate_to_usd) AS total_amount_usd,
    AVG(t.amount * e.rate_to_usd) AS avg_amount_usd
FROM transactions t
JOIN users u ON t.user_id = u.user_id
JOIN exchange_rates e ON t.currency = e.currency
GROUP BY 
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, u.registration_date) < 30 THEN '< 30 days'
        WHEN DATEDIFF(CURRENT_DATE, u.registration_date) < 90 THEN '30-89 days'
        WHEN DATEDIFF(CURRENT_DATE, u.registration_date) < 180 THEN '90-179 days'
        WHEN DATEDIFF(CURRENT_DATE, u.registration_date) < 365 THEN '180-364 days'
        ELSE '365+ days'
    END
ORDER BY account_age;

-- 说明：
-- 1. 创建三个表：transactions（交易流数据）、users（用户维度表）和 exchange_rates（货币汇率维度表）
-- 2. 为 transactions 表的 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入测试数据，模拟交易流和维度表数据
-- 4. 第一个查询：基本流-表连接，丰富交易数据，添加用户信息和汇率转换
-- 5. 第二个查询：按国家和用户类型分析交易，了解不同地区和用户类型的交易行为
-- 6. 第三个查询：分析用户年龄段的交易行为，了解不同年龄段的消费模式
-- 7. 第四个查询：分析用户注册时长与交易行为的关系，了解用户忠诚度对消费的影响
-- 8. 流-表连接特点：将动态的流数据与静态的表数据连接，丰富流数据的上下文
-- 9. 这种连接方式非常适合数据丰富、维度分析和报表生成
