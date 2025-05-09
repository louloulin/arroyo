-- transformation.sql
-- 这个示例展示了如何使用各种函数和表达式转换数据

-- 使用 impulse() 源生成测试数据
SELECT 
    counter,
    timestamp,
    
    -- 数学函数
    ABS(-counter) AS abs_value,
    CEIL(counter / 3.0) AS ceil_value,
    FLOOR(counter / 3.0) AS floor_value,
    ROUND(counter / 3.0, 2) AS round_value,
    
    -- 字符串函数
    UPPER(CONCAT('id-', CAST(counter AS VARCHAR))) AS upper_id,
    LOWER(CONCAT('ID-', CAST(counter AS VARCHAR))) AS lower_id,
    SUBSTRING(CONCAT('id-', CAST(counter AS VARCHAR)), 1, 5) AS sub_id,
    LENGTH(CAST(counter AS VARCHAR)) AS id_length,
    
    -- 日期时间函数
    EXTRACT(YEAR FROM timestamp) AS year,
    EXTRACT(MONTH FROM timestamp) AS month,
    EXTRACT(DAY FROM timestamp) AS day,
    EXTRACT(HOUR FROM timestamp) AS hour,
    EXTRACT(MINUTE FROM timestamp) AS minute,
    EXTRACT(SECOND FROM timestamp) AS second,
    
    -- 条件函数
    COALESCE(NULL, counter, 0) AS coalesce_value,
    NULLIF(counter % 5, 0) AS nullif_value,
    GREATEST(counter, counter % 7, counter % 11) AS greatest_value,
    LEAST(counter, counter % 7, counter % 11) AS least_value,
    
    -- 类型转换
    CAST(counter AS VARCHAR) AS counter_string,
    CAST(counter AS DOUBLE) AS counter_double,
    CAST(timestamp AS VARCHAR) AS timestamp_string,
    
    -- JSON 函数（如果支持）
    -- JSON_VALUE('{"id": 123, "name": "test"}', '$.id') AS json_id,
    -- JSON_QUERY('{"id": 123, "name": "test"}', '$.name') AS json_name,
    
    -- 正则表达式函数（如果支持）
    -- REGEXP_EXTRACT(CONCAT('id-', CAST(counter AS VARCHAR)), 'id-(\d+)', 1) AS regex_id
    
    -- 自定义表达式
    CASE 
        WHEN counter % 3 = 0 AND counter % 5 = 0 THEN 'FizzBuzz'
        WHEN counter % 3 = 0 THEN 'Fizz'
        WHEN counter % 5 = 0 THEN 'Buzz'
        ELSE CAST(counter AS VARCHAR)
    END AS fizz_buzz
FROM impulse()
LIMIT 20;

-- 说明：
-- 1. 这个示例展示了各种数据转换函数和表达式
-- 2. 包括数学函数、字符串函数、日期时间函数、条件函数和类型转换
-- 3. 注释掉的部分是一些可能不被所有 SQL 方言支持的函数
-- 4. LIMIT 子句限制结果数量为 20 条
