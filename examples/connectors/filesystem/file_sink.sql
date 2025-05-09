-- file_sink.sql
-- 这个示例展示了如何将数据写入文件系统

-- 步骤 1: 创建文件目标表
CREATE TABLE file_sink (
  id INT,
  name VARCHAR,
  email VARCHAR,
  registration_date DATE,
  last_login TIMESTAMP(3),
  active BOOLEAN
)
WITH (
  'connector' = 'filesystem',
  'path' = '/tmp/arroyo/output',
  'format' = 'json',
  'write_mode' = 'append'
);

-- 步骤 2: 生成测试数据并写入文件
INSERT INTO file_sink
SELECT
  counter AS id,
  CONCAT('User ', counter) AS name,
  CONCAT('user', counter, '@example.com') AS email,
  CURRENT_DATE - INTERVAL CAST(RAND() * 365 AS INT) DAY AS registration_date,
  CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 86400 AS INT) SECOND AS last_login,
  CASE WHEN RAND() > 0.2 THEN TRUE ELSE FALSE END AS active
FROM impulse()
LIMIT 100;

-- 说明：
-- 1. 创建文件目标表，定义表结构和连接参数
--    - 'path': 输出文件路径
--    - 'format': 文件格式（这里是 JSON）
--    - 'write_mode': 写入模式（append 表示追加写入）
-- 2. 使用 INSERT INTO 语句将数据写入文件
-- 3. 使用 impulse() 源生成测试数据，模拟用户信息
-- 4. LIMIT 子句限制生成的记录数为 100 条

-- 注意：
-- 1. 运行此示例前，请确保输出目录存在并有写入权限
-- 2. 可以根据实际环境修改文件路径
-- 3. 支持的写入模式包括：
--    - 'append': 追加到现有文件
--    - 'overwrite': 覆盖现有文件
-- 4. 支持的文件格式包括 JSON、CSV、Parquet、Avro、ORC 和纯文本
-- 5. 可以使用以下命令查看输出文件内容：
--    cat /tmp/arroyo/output/*
