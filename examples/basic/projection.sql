-- projection.sql
-- 这个示例展示了如何选择和转换数据流中的列

-- 使用 impulse() 源生成测试数据
SELECT 
    -- 基本列选择
    counter,
    timestamp,
    
    -- 列重命名
    counter AS id,
    
    -- 数学运算
    counter * 2 AS doubled_counter,
    POWER(counter, 2) AS squared_counter,
    SQRT(counter) AS sqrt_counter,
    
    -- 字符串操作
    CONCAT('ID-', CAST(counter AS VARCHAR)) AS id_string,
    
    -- 条件表达式
    CASE 
        WHEN counter % 3 = 0 THEN 'Fizz'
        WHEN counter % 5 = 0 THEN 'Buzz'
        WHEN counter % 15 = 0 THEN 'FizzBuzz'
        ELSE CAST(counter AS VARCHAR)
    END AS fizz_buzz,
    
    -- 时间操作
    CURRENT_TIMESTAMP AS current_time,
    timestamp + INTERVAL '1' HOUR AS timestamp_plus_1hour
FROM impulse()
LIMIT 50;

-- 说明：
-- 1. 这个示例展示了各种列投影和转换操作
-- 2. 包括基本列选择、列重命名、数学运算、字符串操作、条件表达式和时间操作
-- 3. LIMIT 子句限制结果数量为 50 条
