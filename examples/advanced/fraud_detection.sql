-- fraud_detection.sql
-- 这个示例展示了如何使用流处理进行实时欺诈检测

-- 步骤 1: 创建交易数据临时表
CREATE TEMPORARY TABLE transactions (
    transaction_id VARCHAR,
    user_id INT,
    merchant_id INT,
    card_id VARCHAR,
    amount DOUBLE,
    transaction_time TIMESTAMP(3),
    location VARCHAR,
    device_id VARCHAR,
    ip_address VARCHAR,
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
);

-- 步骤 2: 插入一些测试数据
INSERT INTO transactions
SELECT 
    CONCAT('txn-', counter) AS transaction_id,
    CAST(RAND() * 100 AS INT) AS user_id,
    CAST(RAND() * 1000 AS INT) AS merchant_id,
    CONCAT('card-', CAST(RAND() * 100 AS INT)) AS card_id,
    CASE 
        -- 生成一些异常大额交易
        WHEN RAND() < 0.05 THEN 1000 + RAND() * 9000
        -- 正常交易金额
        ELSE 10 + RAND() * 990
    END AS amount,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS transaction_time,
    ARRAY['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'London', 'Tokyo', 'Paris', 'Berlin', 'Sydney'][FLOOR(RAND() * 10) + 1] AS location,
    CONCAT('device-', CAST(RAND() * 50 AS INT)) AS device_id,
    CONCAT(
        CAST(RAND() * 255 AS INT), '.', 
        CAST(RAND() * 255 AS INT), '.', 
        CAST(RAND() * 255 AS INT), '.', 
        CAST(RAND() * 255 AS INT)
    ) AS ip_address
FROM impulse()
LIMIT 1000;

-- 步骤 3: 创建用户配置文件临时表
CREATE TEMPORARY TABLE user_profiles (
    user_id INT,
    usual_locations VARCHAR ARRAY,
    usual_devices VARCHAR ARRAY,
    avg_transaction_amount DOUBLE,
    max_transaction_amount DOUBLE
);

-- 步骤 4: 插入一些用户配置文件数据
INSERT INTO user_profiles
SELECT 
    user_id,
    ARRAY_AGG(DISTINCT location) AS usual_locations,
    ARRAY_AGG(DISTINCT device_id) AS usual_devices,
    AVG(amount) AS avg_transaction_amount,
    MAX(amount) * 1.5 AS max_transaction_amount
FROM transactions
GROUP BY user_id;

-- 步骤 5: 检测可疑交易
-- 这个查询使用多种规则检测可能的欺诈交易
WITH suspicious_transactions AS (
    SELECT 
        t.transaction_id,
        t.user_id,
        t.merchant_id,
        t.card_id,
        t.amount,
        t.transaction_time,
        t.location,
        t.device_id,
        t.ip_address,
        p.usual_locations,
        p.usual_devices,
        p.avg_transaction_amount,
        p.max_transaction_amount,
        -- 欺诈指标 1: 交易金额异常
        CASE WHEN t.amount > p.max_transaction_amount THEN 1 ELSE 0 END AS amount_flag,
        -- 欺诈指标 2: 位置异常
        CASE WHEN NOT array_contains(p.usual_locations, t.location) THEN 1 ELSE 0 END AS location_flag,
        -- 欺诈指标 3: 设备异常
        CASE WHEN NOT array_contains(p.usual_devices, t.device_id) THEN 1 ELSE 0 END AS device_flag,
        -- 欺诈指标 4: 交易频率异常（短时间内多笔交易）
        COUNT(*) OVER (
            PARTITION BY t.card_id 
            ORDER BY t.transaction_time 
            RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
        ) AS tx_frequency_10min
    FROM transactions t
    JOIN user_profiles p ON t.user_id = p.user_id
)
SELECT 
    transaction_id,
    user_id,
    card_id,
    amount,
    transaction_time,
    location,
    device_id,
    ip_address,
    -- 计算总体欺诈分数
    amount_flag + location_flag + device_flag + 
    CASE WHEN tx_frequency_10min > 3 THEN 1 ELSE 0 END AS fraud_score,
    -- 欺诈原因
    CASE 
        WHEN amount_flag = 1 THEN '异常金额: ' || CAST(amount AS VARCHAR) || ' (通常最大: ' || CAST(max_transaction_amount AS VARCHAR) || ')'
        ELSE ''
    END ||
    CASE 
        WHEN location_flag = 1 THEN '异常位置: ' || location
        ELSE ''
    END ||
    CASE 
        WHEN device_flag = 1 THEN '异常设备: ' || device_id
        ELSE ''
    END ||
    CASE 
        WHEN tx_frequency_10min > 3 THEN '10分钟内交易频繁: ' || CAST(tx_frequency_10min AS VARCHAR) || '笔'
        ELSE ''
    END AS fraud_reasons,
    -- 欺诈风险级别
    CASE 
        WHEN amount_flag + location_flag + device_flag + CASE WHEN tx_frequency_10min > 3 THEN 1 ELSE 0 END >= 3 THEN '高风险'
        WHEN amount_flag + location_flag + device_flag + CASE WHEN tx_frequency_10min > 3 THEN 1 ELSE 0 END = 2 THEN '中风险'
        WHEN amount_flag + location_flag + device_flag + CASE WHEN tx_frequency_10min > 3 THEN 1 ELSE 0 END = 1 THEN '低风险'
        ELSE '正常'
    END AS risk_level
FROM suspicious_transactions
WHERE amount_flag + location_flag + device_flag + CASE WHEN tx_frequency_10min > 3 THEN 1 ELSE 0 END > 0
ORDER BY fraud_score DESC, transaction_time DESC;

-- 步骤 6: 检测短时间内跨地域交易
-- 这个查询检测同一张卡在短时间内在不同地域的交易，这通常是欺诈的强烈信号
WITH card_transactions AS (
    SELECT 
        card_id,
        transaction_id,
        transaction_time,
        location,
        amount,
        LAG(transaction_time) OVER (PARTITION BY card_id ORDER BY transaction_time) AS prev_time,
        LAG(location) OVER (PARTITION BY card_id ORDER BY transaction_time) AS prev_location
    FROM transactions
)
SELECT 
    card_id,
    transaction_id AS current_transaction_id,
    transaction_time AS current_time,
    location AS current_location,
    amount AS current_amount,
    prev_time,
    prev_location,
    TIMESTAMPDIFF(MINUTE, prev_time, transaction_time) AS minutes_between_transactions,
    'Card used in ' || prev_location || ' and then in ' || location || ' after only ' || 
    CAST(TIMESTAMPDIFF(MINUTE, prev_time, transaction_time) AS VARCHAR) || ' minutes' AS alert_message
FROM card_transactions
WHERE 
    prev_location IS NOT NULL AND
    prev_location != location AND
    TIMESTAMPDIFF(MINUTE, prev_time, transaction_time) < 60  -- 不可能在1小时内在不同城市使用同一张卡
ORDER BY minutes_between_transactions;

-- 说明：
-- 1. 创建交易数据临时表和用户配置文件临时表
-- 2. 插入测试数据，包括一些异常交易
-- 3. 第一个查询使用多种规则检测可疑交易：
--    - 交易金额异常：超过用户历史最大交易金额的1.5倍
--    - 位置异常：交易发生在用户不常用的位置
--    - 设备异常：交易使用用户不常用的设备
--    - 交易频率异常：10分钟内有超过3笔交易
-- 4. 计算总体欺诈分数和风险级别
-- 5. 第二个查询检测短时间内跨地域交易，这通常是欺诈的强烈信号
-- 6. 这个示例展示了如何使用流处理进行实时欺诈检测，可以扩展为更复杂的欺诈检测系统
