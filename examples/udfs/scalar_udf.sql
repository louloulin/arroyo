-- scalar_udf.sql
-- 这个示例展示了如何创建和使用标量 UDF（用户定义函数）

-- 步骤 1: 创建 Rust 标量 UDF
CREATE FUNCTION haversine_distance(lat1 DOUBLE, lon1 DOUBLE, lat2 DOUBLE, lon2 DOUBLE) RETURNS DOUBLE
LANGUAGE RUST
AS $$
#[arroyo_udf]
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    // 地球半径（公里）
    const R: f64 = 6371.0;
    
    // 将经纬度从度转换为弧度
    let lat1_rad = lat1.to_radians();
    let lon1_rad = lon1.to_radians();
    let lat2_rad = lat2.to_radians();
    let lon2_rad = lon2.to_radians();
    
    // Haversine 公式
    let dlat = lat2_rad - lat1_rad;
    let dlon = lon2_rad - lon1_rad;
    let a = (dlat / 2.0).sin().powi(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    
    // 距离（公里）
    R * c
}
$$;

-- 步骤 2: 创建 Rust 标量 UDF - 信用卡掩码函数
CREATE FUNCTION mask_credit_card(card_number VARCHAR) RETURNS VARCHAR
LANGUAGE RUST
AS $$
#[arroyo_udf]
fn mask_credit_card(card_number: String) -> String {
    if card_number.len() <= 4 {
        return card_number;
    }
    
    let visible_digits = &card_number[card_number.len() - 4..];
    let mask_length = card_number.len() - 4;
    let mask = "*".repeat(mask_length);
    
    format!("{}{}", mask, visible_digits)
}
$$;

-- 步骤 3: 创建 Python 标量 UDF - 情感分析函数
CREATE FUNCTION sentiment_score(text VARCHAR) RETURNS DOUBLE
LANGUAGE PYTHON
AS $$
import re

def sentiment_score(text):
    """
    一个简单的情感分析函数，基于关键词匹配
    返回范围从 -1（非常负面）到 1（非常正面）的分数
    """
    if not text:
        return 0.0
        
    # 正面词汇
    positive_words = [
        'good', 'great', 'excellent', 'amazing', 'wonderful', 'fantastic',
        'happy', 'love', 'best', 'awesome', 'perfect', 'pleased', 'thank',
        'thanks', 'satisfied', 'enjoy', 'enjoyed', 'like', 'recommend'
    ]
    
    # 负面词汇
    negative_words = [
        'bad', 'terrible', 'awful', 'horrible', 'worst', 'poor', 'disappointed',
        'disappointing', 'hate', 'dislike', 'problem', 'issue', 'complaint',
        'unhappy', 'sad', 'angry', 'frustrating', 'frustrated', 'waste'
    ]
    
    # 将文本转换为小写并分词
    text = text.lower()
    words = re.findall(r'\b\w+\b', text)
    
    # 计算正面和负面词汇的出现次数
    positive_count = sum(1 for word in words if word in positive_words)
    negative_count = sum(1 for word in words if word in negative_words)
    
    # 计算总词数
    total_words = len(words)
    if total_words == 0:
        return 0.0
    
    # 计算情感分数
    score = (positive_count - negative_count) / (positive_count + negative_count + 1)
    
    return score
$$;

-- 步骤 4: 使用 UDF 处理数据
-- 创建一个临时表用于测试
CREATE TEMPORARY TABLE test_data (
    id INT,
    customer_name VARCHAR,
    credit_card VARCHAR,
    comment VARCHAR,
    origin_lat DOUBLE,
    origin_lon DOUBLE,
    destination_lat DOUBLE,
    destination_lon DOUBLE
);

-- 插入一些测试数据
INSERT INTO test_data
SELECT 
    counter AS id,
    CONCAT('Customer ', counter) AS customer_name,
    CONCAT('4532', CAST(RAND() * 1000 AS INT), '5678', CAST(RAND() * 1000 AS INT)) AS credit_card,
    ARRAY[
        'I really enjoyed the service, it was fantastic!',
        'The product was good but delivery was slow.',
        'Terrible experience, will not recommend to anyone.',
        'Amazing quality and fast shipping, very satisfied!',
        'Had some issues with the product, not happy with my purchase.'
    ][FLOOR(RAND() * 5) + 1] AS comment,
    40.7128 + (RAND() * 10 - 5) AS origin_lat,  -- 纽约附近
    -74.0060 + (RAND() * 10 - 5) AS origin_lon,
    34.0522 + (RAND() * 10 - 5) AS destination_lat,  -- 洛杉矶附近
    -118.2437 + (RAND() * 10 - 5) AS destination_lon
FROM impulse()
LIMIT 10;

-- 使用 UDF 查询数据
SELECT 
    id,
    customer_name,
    mask_credit_card(credit_card) AS masked_credit_card,
    comment,
    sentiment_score(comment) AS sentiment,
    CASE 
        WHEN sentiment_score(comment) > 0.3 THEN 'Positive'
        WHEN sentiment_score(comment) < -0.3 THEN 'Negative'
        ELSE 'Neutral'
    END AS sentiment_category,
    haversine_distance(origin_lat, origin_lon, destination_lat, destination_lon) AS distance_km,
    ROUND(haversine_distance(origin_lat, origin_lon, destination_lat, destination_lon) / 100) AS estimated_travel_hours
FROM test_data
ORDER BY sentiment DESC;

-- 说明：
-- 1. 创建了三个用户定义函数 (UDF)：
--    - haversine_distance：计算两个地理坐标之间的距离（Rust）
--    - mask_credit_card：掩盖信用卡号码，只显示最后四位（Rust）
--    - sentiment_score：简单的情感分析，基于关键词匹配（Python）
-- 2. 创建临时表并插入测试数据
-- 3. 使用 UDF 处理数据，展示了如何在 SQL 查询中调用 UDF
-- 4. 这个示例展示了如何创建和使用标量 UDF，可以扩展为更复杂的数据处理逻辑
