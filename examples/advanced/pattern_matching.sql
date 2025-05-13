-- 复杂事件处理 (CEP) 示例
-- 本示例展示如何使用 Arroyo 的复杂事件处理功能检测事件序列模式

-- 创建交易数据流
CREATE TABLE transactions (
    transaction_id STRING,
    user_id STRING,
    card_id STRING,
    amount DOUBLE,
    transaction_time TIMESTAMP,
    merchant_id STRING,
    merchant_category STRING,
    location STRING,
    device_id STRING,
    ip_address STRING,
    status STRING
) WITH (
    connector = 'kafka',
    bootstrap.servers = 'localhost:9092',
    topic = 'transactions',
    format = 'json'
);

-- 设置水印，基于交易时间
CREATE VIEW transactions_with_watermark AS
SELECT 
    *,
    PROCTIME() as processing_time
FROM transactions
/*+ WATERMARK(transaction_time, 30 SECONDS) */;

-- 示例 1: 检测快速连续交易模式
-- 检测同一用户在短时间内（5分钟内）进行的3次或更多交易
SELECT 
    pattern_name,
    user_id,
    COUNT(*) as transaction_count,
    MIN(transaction_time) as first_transaction_time,
    MAX(transaction_time) as last_transaction_time,
    SUM(amount) as total_amount
FROM MATCH_RECOGNIZE(
    TABLE transactions_with_watermark,
    PARTITION BY user_id,
    ORDER BY transaction_time,
    MEASURES
        'rapid_transactions' as pattern_name
    PATTERN (A B+ C)
    DEFINE
        A AS true,
        B AS B.transaction_time BETWEEN A.transaction_time AND A.transaction_time + INTERVAL '5' MINUTE,
        C AS C.transaction_time BETWEEN A.transaction_time AND A.transaction_time + INTERVAL '5' MINUTE
)
GROUP BY pattern_name, user_id;

-- 示例 2: 检测欺诈模式 - 不同地点的快速连续交易
-- 检测同一张卡在不同地点短时间内（1小时内）的交易
SELECT 
    pattern_name,
    card_id,
    COLLECT_LIST(location) as locations,
    COLLECT_LIST(transaction_time) as transaction_times,
    COLLECT_LIST(amount) as amounts,
    TIMESTAMPDIFF(MINUTE, MIN(transaction_time), MAX(transaction_time)) as minutes_between
FROM MATCH_RECOGNIZE(
    TABLE transactions_with_watermark,
    PARTITION BY card_id,
    ORDER BY transaction_time,
    MEASURES
        'location_hopping' as pattern_name
    PATTERN (A B)
    DEFINE
        A AS true,
        B AS B.location != A.location AND 
            B.transaction_time BETWEEN A.transaction_time AND A.transaction_time + INTERVAL '60' MINUTE
)
GROUP BY pattern_name, card_id;

-- 示例 3: 检测大额交易后的多笔小额交易模式
-- 这可能表示测试卡片有效性后进行的欺诈活动
SELECT 
    pattern_name,
    card_id,
    COLLECT_LIST(amount) as amounts,
    COLLECT_LIST(transaction_time) as transaction_times,
    COUNT(*) as transaction_count
FROM MATCH_RECOGNIZE(
    TABLE transactions_with_watermark,
    PARTITION BY card_id,
    ORDER BY transaction_time,
    MEASURES
        'test_then_fraud' as pattern_name
    PATTERN (A B+)
    DEFINE
        A AS A.amount > 500,
        B AS B.amount < 100 AND 
            B.transaction_time BETWEEN A.transaction_time AND A.transaction_time + INTERVAL '1' DAY
)
GROUP BY pattern_name, card_id
HAVING COUNT(*) >= 3;

-- 示例 4: 检测交易被拒绝后立即重试的模式
-- 这可能表示有人在尝试猜测卡片信息
SELECT 
    pattern_name,
    card_id,
    COLLECT_LIST(status) as statuses,
    COLLECT_LIST(amount) as amounts,
    COLLECT_LIST(transaction_time) as transaction_times
FROM MATCH_RECOGNIZE(
    TABLE transactions_with_watermark,
    PARTITION BY card_id,
    ORDER BY transaction_time,
    MEASURES
        'declined_retry' as pattern_name
    PATTERN (A B)
    DEFINE
        A AS A.status = 'declined',
        B AS B.status = 'completed' AND 
            B.transaction_time BETWEEN A.transaction_time AND A.transaction_time + INTERVAL '5' MINUTE AND
            ABS(B.amount - A.amount) < 1
)
GROUP BY pattern_name, card_id;

-- 示例 5: 检测用户行为异常模式
-- 检测用户在新设备、新位置进行大额交易的情况
SELECT 
    pattern_name,
    user_id,
    device_id,
    location,
    amount,
    transaction_time
FROM MATCH_RECOGNIZE(
    TABLE transactions_with_watermark,
    PARTITION BY user_id,
    ORDER BY transaction_time,
    MEASURES
        'unusual_behavior' as pattern_name,
        LAST(device_id) as device_id,
        LAST(location) as location,
        LAST(amount) as amount,
        LAST(transaction_time) as transaction_time
    PATTERN (A+ B)
    DEFINE
        A AS true,
        B AS NOT EXISTS(
                SELECT * FROM PREV(A, 10) 
                WHERE A.device_id = B.device_id OR A.location = B.location
            ) AND
            B.amount > 1000
);
