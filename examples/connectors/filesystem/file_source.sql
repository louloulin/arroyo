-- file_source.sql
-- 这个示例展示了如何从文件系统读取数据

-- 步骤 1: 创建文件源表
CREATE TABLE file_source (
  id INT,
  name VARCHAR,
  age INT,
  city VARCHAR
)
WITH (
  'connector' = 'filesystem',
  'path' = '/tmp/arroyo/input',
  'format' = 'json',
  'pattern' = '*.json'
);

-- 步骤 2: 查询文件数据
-- 这个查询从文件读取数据，过滤年龄大于 30 的人，并按城市分组计算平均年龄
SELECT
  city,
  COUNT(*) AS person_count,
  AVG(age) AS avg_age,
  MIN(age) AS min_age,
  MAX(age) AS max_age
FROM file_source
WHERE age > 25
GROUP BY city
ORDER BY avg_age DESC;

-- 说明：
-- 1. 创建文件源表，定义表结构和连接参数
--    - 'path': 文件路径
--    - 'format': 文件格式（这里是 JSON）
--    - 'pattern': 文件匹配模式
-- 2. 使用 SQL 查询从文件读取数据并进行处理
-- 3. 这个查询过滤年龄大于 25 的人，按城市分组计算统计信息

-- 注意：
-- 1. 运行此示例前，请确保输入目录存在并包含数据文件
-- 2. 可以根据实际环境修改文件路径
-- 3. 支持的文件格式包括 JSON、CSV、Parquet、Avro、ORC 和纯文本
