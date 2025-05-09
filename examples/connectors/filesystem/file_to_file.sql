-- file_to_file.sql
-- 这个示例展示了如何从文件读取数据，处理后再写入文件

-- 步骤 1: 创建文件源表
CREATE TABLE customer_source (
  customer_id INT,
  name VARCHAR,
  email VARCHAR,
  age INT,
  city VARCHAR,
  registration_date DATE
)
WITH (
  'connector' = 'filesystem',
  'path' = '/tmp/arroyo/input/customers',
  'format' = 'csv',
  'csv.field-delimiter' = ',',
  'csv.quote-character' = '"',
  'csv.null-value' = 'NULL',
  'csv.ignore-parse-errors' = 'true',
  'csv.allow-comments' = 'true'
);

-- 步骤 2: 创建文件目标表
CREATE TABLE customer_segments (
  segment VARCHAR,
  customer_count INT,
  avg_age DOUBLE,
  cities VARCHAR,
  min_registration_date DATE,
  max_registration_date DATE
)
WITH (
  'connector' = 'filesystem',
  'path' = '/tmp/arroyo/output/segments',
  'format' = 'parquet',
  'write_mode' = 'overwrite'
);

-- 步骤 3: 处理数据并写入目标表
-- 这个查询对客户数据进行分段分析
INSERT INTO customer_segments
SELECT
  CASE
    WHEN age < 25 THEN 'Young'
    WHEN age BETWEEN 25 AND 40 THEN 'Adult'
    WHEN age BETWEEN 41 AND 60 THEN 'Middle-aged'
    ELSE 'Senior'
  END AS segment,
  COUNT(*) AS customer_count,
  AVG(age) AS avg_age,
  STRING_AGG(DISTINCT city, ', ') AS cities,
  MIN(registration_date) AS min_registration_date,
  MAX(registration_date) AS max_registration_date
FROM customer_source
GROUP BY
  CASE
    WHEN age < 25 THEN 'Young'
    WHEN age BETWEEN 25 AND 40 THEN 'Adult'
    WHEN age BETWEEN 41 AND 60 THEN 'Middle-aged'
    ELSE 'Senior'
  END;

-- 说明：
-- 1. 创建文件源表，从 CSV 文件读取客户数据
-- 2. 创建文件目标表，将结果写入 Parquet 文件
-- 3. 处理客户数据，按年龄段进行分组分析
-- 4. 计算每个年龄段的客户数量、平均年龄、城市列表和注册日期范围
-- 5. 将分析结果写入目标表

-- 注意：
-- 1. 运行此示例前，请确保输入目录存在并包含 CSV 格式的客户数据
-- 2. 确保输出目录存在并有写入权限
-- 3. 这个示例展示了不同文件格式之间的转换（CSV 到 Parquet）
-- 4. CSV 配置参数说明：
--    - 'csv.field-delimiter': 字段分隔符
--    - 'csv.quote-character': 引号字符
--    - 'csv.null-value': 空值表示
--    - 'csv.ignore-parse-errors': 是否忽略解析错误
--    - 'csv.allow-comments': 是否允许注释

-- 示例输入数据 (/tmp/arroyo/input/customers/data.csv)：
-- customer_id,name,email,age,city,registration_date
-- 1,John Doe,john@example.com,32,New York,2022-01-15
-- 2,Jane Smith,jane@example.com,28,Boston,2022-02-20
-- 3,Bob Johnson,bob@example.com,45,Chicago,2021-11-10
-- 4,Alice Brown,alice@example.com,22,San Francisco,2022-03-05
-- 5,Charlie Davis,charlie@example.com,55,Seattle,2021-09-30
