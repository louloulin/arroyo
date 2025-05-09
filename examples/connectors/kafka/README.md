# Kafka 连接器示例

本目录包含了 Arroyo 的 Kafka 连接器示例，展示了如何使用 Kafka 作为数据源和数据目标。

## 前提条件

运行这些示例前，您需要有一个可用的 Kafka 集群。您可以使用以下方法启动一个本地 Kafka 集群：

### 使用 Docker Compose

创建一个 `docker-compose.yml` 文件：

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.3.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

然后运行：

```bash
docker-compose up -d
```

## 示例列表

### 1. Kafka 源 (kafka_source.sql)

展示如何从 Kafka 主题读取数据。

### 2. Kafka 目标 (kafka_sink.sql)

展示如何将数据写入 Kafka 主题。

### 3. Kafka 到 Kafka 处理 (kafka_to_kafka.sql)

展示如何从 Kafka 读取数据，处理后再写回 Kafka。

### 4. Kafka 与模式注册表 (kafka_with_schema_registry.sql)

展示如何使用带有模式注册表的 Kafka 连接器。

### 5. Kafka 流连接 (kafka_stream_join.sql)

展示如何连接多个 Kafka 主题的数据流。

## 运行示例

### 步骤 1：创建测试主题

```bash
# 创建输入主题
docker exec -it kafka kafka-topics --create --topic input-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 创建输出主题
docker exec -it kafka kafka-topics --create --topic output-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 步骤 2：生成测试数据

```bash
# 向输入主题发送测试数据
docker exec -it kafka kafka-console-producer --topic input-topic --bootstrap-server localhost:9092 << EOF
{"id": 1, "name": "Product 1", "price": 10.99}
{"id": 2, "name": "Product 2", "price": 20.49}
{"id": 3, "name": "Product 3", "price": 5.99}
{"id": 4, "name": "Product 4", "price": 15.99}
{"id": 5, "name": "Product 5", "price": 25.99}
EOF
```

### 步骤 3：运行 Arroyo 查询

使用 Web UI 或命令行运行示例查询：

```bash
arroyo run examples/connectors/kafka/kafka_source.sql
```

### 步骤 4：查看结果

如果使用了 Kafka 目标连接器，可以查看输出主题的数据：

```bash
docker exec -it kafka kafka-console-consumer --topic output-topic --bootstrap-server localhost:9092 --from-beginning
```
