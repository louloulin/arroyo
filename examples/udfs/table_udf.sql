-- table_udf.sql
-- 这个示例展示了如何创建和使用表值 UDF（用户定义函数）

-- 步骤 1: 创建 Rust 表值 UDF - 字符串分词函数
CREATE FUNCTION tokenize(text VARCHAR) RETURNS TABLE(token VARCHAR, position INT)
LANGUAGE RUST
AS $$
#[arroyo_udf]
fn tokenize(text: String) -> Vec<(String, i32)> {
    if text.is_empty() {
        return Vec::new();
    }
    
    // 简单的分词：按空格分割
    let tokens: Vec<(String, i32)> = text
        .split_whitespace()
        .enumerate()
        .map(|(i, token)| {
            // 移除标点符号
            let clean_token = token
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == '\'')
                .collect::<String>()
                .to_lowercase();
            (clean_token, i as i32)
        })
        .collect();
    
    tokens
}
$$;

-- 步骤 2: 创建 Rust 表值 UDF - 日期范围生成函数
CREATE FUNCTION date_range(start_date DATE, end_date DATE) RETURNS TABLE(date DATE, day_of_week VARCHAR, is_weekend BOOLEAN)
LANGUAGE RUST
AS $$
use chrono::{NaiveDate, Duration};

#[arroyo_udf]
fn date_range(start_date: NaiveDate, end_date: NaiveDate) -> Vec<(NaiveDate, String, bool)> {
    if start_date > end_date {
        return Vec::new();
    }
    
    let days = (end_date - start_date).num_days() as usize + 1;
    let mut result = Vec::with_capacity(days);
    
    let day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
    
    for i in 0..days {
        let current_date = start_date + Duration::days(i as i64);
        let weekday = current_date.weekday().num_days_from_monday() as usize;
        let day_name = day_names[weekday].to_string();
        let is_weekend = weekday >= 5; // 5 = Saturday, 6 = Sunday
        
        result.push((current_date, day_name, is_weekend));
    }
    
    result
}
$$;

-- 步骤 3: 创建 Python 表值 UDF - JSON 解析函数
CREATE FUNCTION parse_json_array(json_array VARCHAR) RETURNS TABLE(item_id INT, item_name VARCHAR, item_value DOUBLE)
LANGUAGE PYTHON
AS $$
import json

def parse_json_array(json_array):
    """
    解析 JSON 数组，返回每个项目的 ID、名称和值
    """
    if not json_array:
        return []
        
    try:
        items = json.loads(json_array)
        result = []
        
        for item in items:
            item_id = item.get('id', 0)
            item_name = item.get('name', '')
            item_value = float(item.get('value', 0.0))
            result.append((item_id, item_name, item_value))
            
        return result
    except Exception as e:
        # 返回错误信息作为单个记录
        return [(0, f"Error: {str(e)}", 0.0)]
$$;

-- 步骤 4: 使用表值 UDF 处理数据
-- 创建一个临时表用于测试
CREATE TEMPORARY TABLE test_data (
    id INT,
    text_content VARCHAR,
    start_date DATE,
    end_date DATE,
    json_data VARCHAR
);

-- 插入一些测试数据
INSERT INTO test_data
SELECT 
    counter AS id,
    ARRAY[
        'The quick brown fox jumps over the lazy dog.',
        'To be or not to be, that is the question.',
        'All that glitters is not gold.',
        'The early bird catches the worm.',
        'Actions speak louder than words.'
    ][FLOOR(RAND() * 5) + 1] AS text_content,
    CURRENT_DATE AS start_date,
    CURRENT_DATE + INTERVAL CAST(RAND() * 10 AS INT) DAY AS end_date,
    ARRAY[
        '[{"id": 1, "name": "Item 1", "value": 10.5}, {"id": 2, "name": "Item 2", "value": 20.75}]',
        '[{"id": 3, "name": "Product A", "value": 99.99}, {"id": 4, "name": "Product B", "value": 49.95}, {"id": 5, "name": "Product C", "value": 149.99}]',
        '[{"id": 6, "name": "Service X", "value": 199.99}]',
        '[{"id": 7, "name": "Book 1", "value": 12.99}, {"id": 8, "name": "Book 2", "value": 15.99}]'
    ][FLOOR(RAND() * 4) + 1] AS json_data
FROM impulse()
LIMIT 5;

-- 使用 tokenize 表值 UDF
SELECT 
    t.id,
    t.text_content,
    token.token,
    token.position
FROM test_data t
CROSS JOIN TABLE(tokenize(t.text_content)) AS token
ORDER BY t.id, token.position;

-- 使用 date_range 表值 UDF
SELECT 
    t.id,
    t.start_date,
    t.end_date,
    d.date,
    d.day_of_week,
    d.is_weekend
FROM test_data t
CROSS JOIN TABLE(date_range(t.start_date, t.end_date)) AS d
ORDER BY t.id, d.date;

-- 使用 parse_json_array 表值 UDF
SELECT 
    t.id,
    t.json_data,
    j.item_id,
    j.item_name,
    j.item_value
FROM test_data t
CROSS JOIN TABLE(parse_json_array(t.json_data)) AS j
ORDER BY t.id, j.item_id;

-- 组合使用多个表值 UDF
-- 找出每个日期范围内的周末，并计算每个周末的项目总值
SELECT 
    t.id,
    d.date,
    d.day_of_week,
    SUM(j.item_value) AS total_value
FROM test_data t
CROSS JOIN TABLE(date_range(t.start_date, t.end_date)) AS d
CROSS JOIN TABLE(parse_json_array(t.json_data)) AS j
WHERE d.is_weekend = TRUE
GROUP BY t.id, d.date, d.day_of_week
ORDER BY t.id, d.date;

-- 说明：
-- 1. 创建了三个表值用户定义函数 (UDF)：
--    - tokenize：将文本分词，返回每个词及其位置（Rust）
--    - date_range：生成日期范围，返回每个日期及其星期信息（Rust）
--    - parse_json_array：解析 JSON 数组，返回每个项目的信息（Python）
-- 2. 创建临时表并插入测试数据
-- 3. 使用表值 UDF 处理数据，展示了如何在 SQL 查询中调用表值 UDF
-- 4. 展示了如何组合使用多个表值 UDF
-- 5. 表值 UDF 的特点是返回多行数据，可以用于数据展开、解析和生成
