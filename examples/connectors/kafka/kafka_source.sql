-- kafka_source.sql
-- 这个示例展示了如何从 Kafka 主题读取数据

-- 步骤 1: 创建 Kafka 连接配置
CREATE CONNECTION kafka_connection
TYPE 'kafka'
WITH (
  'bootstrap_servers' = 'localhost:9092'
);

-- 步骤 2: 创建 Kafka 源表
CREATE TABLE kafka_source (
  id INT,
  name VARCHAR,
  price DOUBLE,
  event_time TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
WITH (
  'connector' = 'kafka',
  'connection' = 'kafka_connection',
  'topic' = 'input-topic',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

-- 步骤 3: 查询 Kafka 数据
-- 这个查询从 Kafka 主题读取数据，过滤价格大于 10 的产品，并计算价格加税后的值
SELECT
  id,
  name,
  price,
  price * 1.1 AS price_with_tax,
  event_time
FROM kafka_source
WHERE price > 10;

-- 说明：
-- 1. 创建 Kafka 连接配置，指定 Kafka 服务器地址
-- 2. 创建 Kafka 源表，定义表结构和连接参数
--    - 'topic': Kafka 主题名称
--    - 'format': 数据格式（这里是 JSON）
--    - 'scan.startup.mode': 读取模式（earliest-offset 表示从最早的偏移量开始读取）
-- 3. 使用 SQL 查询从 Kafka 主题读取数据并进行处理
-- 4. event_time 字段使用 METADATA FROM 'timestamp' 从 Kafka 消息的时间戳获取
-- 5. 为 event_time 定义水印，允许最多 5 秒的延迟

-- 注意：
-- 1. 运行此示例前，请确保 Kafka 服务器已启动
-- 2. 确保 'input-topic' 主题已创建并包含数据
-- 3. 可以根据实际环境修改连接参数
