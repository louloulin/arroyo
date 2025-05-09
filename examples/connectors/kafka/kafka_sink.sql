-- kafka_sink.sql
-- 这个示例展示了如何将数据写入 Kafka 主题

-- 步骤 1: 创建 Kafka 连接配置
CREATE CONNECTION kafka_connection
TYPE 'kafka'
WITH (
  'bootstrap_servers' = 'localhost:9092'
);

-- 步骤 2: 创建 Kafka 目标表
CREATE TABLE kafka_sink (
  id INT,
  name VARCHAR,
  original_price DOUBLE,
  discounted_price DOUBLE,
  discount_percentage DOUBLE,
  event_time TIMESTAMP(3)
)
WITH (
  'connector' = 'kafka',
  'connection' = 'kafka_connection',
  'topic' = 'output-topic',
  'format' = 'json',
  'key.fields' = 'id',
  'sink.delivery-guarantee' = 'at-least-once'
);

-- 步骤 3: 生成测试数据并写入 Kafka
INSERT INTO kafka_sink
SELECT
  counter AS id,
  CONCAT('Product ', counter) AS name,
  ROUND(50 + RAND() * 950, 2) AS original_price,
  ROUND((50 + RAND() * 950) * (1 - RAND() * 0.3), 2) AS discounted_price,
  ROUND(RAND() * 30, 1) AS discount_percentage,
  CURRENT_TIMESTAMP AS event_time
FROM impulse()
LIMIT 100;

-- 说明：
-- 1. 创建 Kafka 连接配置，指定 Kafka 服务器地址
-- 2. 创建 Kafka 目标表，定义表结构和连接参数
--    - 'topic': Kafka 主题名称
--    - 'format': 数据格式（这里是 JSON）
--    - 'key.fields': 指定作为 Kafka 消息键的字段
--    - 'sink.delivery-guarantee': 传递保证（at-least-once 表示至少一次传递）
-- 3. 使用 INSERT INTO 语句将数据写入 Kafka 主题
-- 4. 使用 impulse() 源生成测试数据，模拟产品信息
-- 5. LIMIT 子句限制生成的记录数为 100 条

-- 注意：
-- 1. 运行此示例前，请确保 Kafka 服务器已启动
-- 2. 确保 'output-topic' 主题已创建
-- 3. 可以根据实际环境修改连接参数
-- 4. 可以使用 Kafka 控制台消费者查看写入的数据：
--    docker exec -it kafka kafka-console-consumer --topic output-topic --bootstrap-server localhost:9092 --from-beginning
