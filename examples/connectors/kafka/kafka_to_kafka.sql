-- kafka_to_kafka.sql
-- 这个示例展示了如何从 Kafka 读取数据，处理后再写回 Kafka

-- 步骤 1: 创建 Kafka 连接配置
CREATE CONNECTION kafka_connection
TYPE 'kafka'
WITH (
  'bootstrap_servers' = 'localhost:9092'
);

-- 步骤 2: 创建 Kafka 源表
CREATE TABLE orders_source (
  order_id INT,
  customer_id INT,
  product_id INT,
  quantity INT,
  unit_price DOUBLE,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
)
WITH (
  'connector' = 'kafka',
  'connection' = 'kafka_connection',
  'topic' = 'orders',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

-- 步骤 3: 创建 Kafka 目标表
CREATE TABLE order_summary_sink (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  total_orders INT,
  total_items INT,
  total_revenue DOUBLE,
  avg_order_value DOUBLE
)
WITH (
  'connector' = 'kafka',
  'connection' = 'kafka_connection',
  'topic' = 'order-summary',
  'format' = 'json'
);

-- 步骤 4: 处理数据并写入目标表
-- 这个查询计算每10分钟窗口的订单汇总信息
INSERT INTO order_summary_sink
SELECT
  TUMBLE_START(order_time, INTERVAL '10' MINUTE) AS window_start,
  TUMBLE_END(order_time, INTERVAL '10' MINUTE) AS window_end,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(quantity) AS total_items,
  SUM(quantity * unit_price) AS total_revenue,
  SUM(quantity * unit_price) / COUNT(DISTINCT order_id) AS avg_order_value
FROM TABLE(
  TUMBLE(TABLE orders_source, DESCRIPTOR(order_time), INTERVAL '10' MINUTE)
)
GROUP BY TUMBLE_START(order_time, INTERVAL '10' MINUTE), TUMBLE_END(order_time, INTERVAL '10' MINUTE);

-- 说明：
-- 1. 创建 Kafka 连接配置，指定 Kafka 服务器地址
-- 2. 创建 Kafka 源表，从 'orders' 主题读取订单数据
-- 3. 创建 Kafka 目标表，将结果写入 'order-summary' 主题
-- 4. 使用滚动窗口（10分钟）对订单数据进行聚合计算
-- 5. 计算每个窗口的订单总数、商品总数、总收入和平均订单价值
-- 6. 将计算结果插入目标表，写入 Kafka

-- 注意：
-- 1. 运行此示例前，请确保 Kafka 服务器已启动
-- 2. 确保 'orders' 和 'order-summary' 主题已创建
-- 3. 可以使用以下命令向 'orders' 主题发送测试数据：
--    docker exec -it kafka kafka-console-producer --topic orders --bootstrap-server localhost:9092
--    然后输入 JSON 格式的订单数据，例如：
--    {"order_id": 1, "customer_id": 101, "product_id": 201, "quantity": 2, "unit_price": 29.99, "order_time": "2023-09-15T10:15:30Z"}
-- 4. 可以使用以下命令查看 'order-summary' 主题的结果：
--    docker exec -it kafka kafka-console-consumer --topic order-summary --bootstrap-server localhost:9092 --from-beginning
