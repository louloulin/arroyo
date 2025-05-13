-- 消息队列 DML 示例
-- 本示例展示如何使用 Arroyo 的消息队列 DML 语句

-- 创建 Topic
CREATE TOPIC IF NOT EXISTS sensor_data WITH (
    partitions = 3,
    replication_factor = 2,
    retention_ms = 86400000,
    cleanup_policy = 'delete',
    description = '传感器数据主题'
);

-- 生产消息（直接指定值）
WITH PRODUCE("sensor_data", columns=["sensor_id", "temperature", "humidity", "timestamp"]) AS (
    VALUES
        ('sensor1', 25.5, 60.0, CURRENT_TIMESTAMP),
        ('sensor2', 26.2, 58.5, CURRENT_TIMESTAMP),
        ('sensor3', 24.8, 62.3, CURRENT_TIMESTAMP)
)
SELECT * FROM VALUES;

-- 生产消息（带键）
WITH PRODUCE("sensor_data", columns=["sensor_id", "temperature", "humidity", "timestamp"], key="sensor_id") AS (
    VALUES
        ('sensor4', 27.1, 59.2, CURRENT_TIMESTAMP),
        ('sensor5', 25.9, 61.5, CURRENT_TIMESTAMP)
)
SELECT * FROM VALUES;

-- 从查询结果生产消息
CREATE TABLE sensor_readings (
    sensor_id STRING,
    temperature DOUBLE,
    humidity DOUBLE,
    reading_time TIMESTAMP,
    WATERMARK FOR reading_time AS reading_time - INTERVAL '30' SECOND
) WITH (
    connector = 'kafka',
    bootstrap.servers = 'localhost:9092',
    topic = 'raw_sensor_readings',
    format = 'json'
);

WITH PRODUCE("sensor_data", columns=["sensor_id", "temperature", "humidity", "timestamp"], key="sensor_id") AS (
    SELECT 
        sensor_id,
        temperature,
        humidity,
        reading_time AS timestamp
    FROM sensor_readings
    WHERE temperature > 30.0
)
SELECT * FROM VALUES;

-- 消费消息
WITH CONSUME("sensor_data") AS (
    SELECT * FROM table
)
LIMIT 10;

-- 消费消息（指定消费者组）
WITH CONSUME("sensor_data", group_id="monitoring-group") AS (
    SELECT * FROM table
)
WHERE temperature > 26.0
LIMIT 5;

-- 创建另一个 Topic
CREATE TOPIC IF NOT EXISTS alerts WITH (
    partitions = 1,
    replication_factor = 3,
    retention_ms = 604800000,
    cleanup_policy = 'delete',
    description = '警报数据主题'
);

-- 生产警报消息
WITH PRODUCE("alerts", columns=["alert_id", "sensor_id", "alert_type", "alert_message", "timestamp"], key="alert_id") AS (
    VALUES
        ('alert1', 'sensor1', 'HIGH_TEMP', 'Temperature too high', CURRENT_TIMESTAMP),
        ('alert2', 'sensor3', 'LOW_HUMIDITY', 'Humidity too low', CURRENT_TIMESTAMP)
)
SELECT * FROM VALUES;

-- 消费警报消息
WITH CONSUME("alerts", group_id="alert-processor") AS (
    SELECT * FROM table
)
WHERE alert_type = 'HIGH_TEMP'
LIMIT 10;

-- 使用流处理将高温数据转发到警报主题
CREATE TABLE high_temp_alerts AS
SELECT 
    CONCAT('alert-', CAST(RAND() * 1000000 AS INT)) AS alert_id,
    sensor_id,
    'HIGH_TEMP' AS alert_type,
    CONCAT('High temperature alert: ', CAST(temperature AS STRING), '°C') AS alert_message,
    reading_time AS timestamp
FROM sensor_readings
WHERE temperature > 30.0;

WITH PRODUCE("alerts", columns=["alert_id", "sensor_id", "alert_type", "alert_message", "timestamp"], key="alert_id") AS (
    SELECT * FROM high_temp_alerts
)
SELECT * FROM VALUES;
