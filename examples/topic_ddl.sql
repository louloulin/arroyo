-- Topic DDL 示例
-- 本示例展示如何使用 Arroyo 的 Topic DDL 语句

-- 创建 Topic
CREATE TOPIC IF NOT EXISTS sensor_data WITH (
    partitions = 3,
    replication_factor = 2,
    retention_ms = 86400000,
    cleanup_policy = 'delete',
    max_message_bytes = 1048576,
    description = '传感器数据主题'
);

-- 修改 Topic
ALTER TOPIC sensor_data SET 
    retention_ms = 172800000,
    cleanup_policy = 'compact',
    description = '传感器数据主题 - 已更新';

-- 创建另一个 Topic
CREATE TOPIC IF NOT EXISTS alerts WITH (
    partitions = 1,
    replication_factor = 3,
    retention_ms = 604800000,
    cleanup_policy = 'delete',
    description = '警报数据主题'
);

-- 列出所有 Topic
SHOW TOPICS;

-- 删除 Topic
DROP TOPIC IF EXISTS test_topic;

-- 使用创建的 Topic 进行流处理
CREATE TABLE sensor_stream (
    sensor_id STRING,
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE,
    event_time TIMESTAMP,
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    connector = 'kafka',
    bootstrap.servers = 'localhost:9092',
    topic = 'sensor_data',
    format = 'json'
);

CREATE TABLE alert_sink (
    sensor_id STRING,
    temperature DOUBLE,
    alert_message STRING,
    alert_time TIMESTAMP
) WITH (
    connector = 'kafka',
    bootstrap.servers = 'localhost:9092',
    topic = 'alerts',
    format = 'json'
);

-- 高温警报流处理
INSERT INTO alert_sink
SELECT 
    sensor_id,
    temperature,
    CONCAT('High temperature alert: ', CAST(temperature AS STRING), '°C') AS alert_message,
    event_time AS alert_time
FROM sensor_stream
WHERE temperature > 30.0;
