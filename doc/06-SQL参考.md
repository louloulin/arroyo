# 6. SQL 参考

Arroyo 提供了强大的 SQL 支持，允许用户使用熟悉的 SQL 语法编写流处理查询。本章节提供了 Arroyo SQL 的详细参考。

## 6.1 SQL 语法

Arroyo SQL 基于标准 SQL，但添加了流处理特定的扩展。

### 6.1.1 基本查询结构

```sql
SELECT [列1], [列2], ...
FROM [源表]
WHERE [条件]
GROUP BY [分组列]
HAVING [分组条件]
```

### 6.1.2 数据定义语言 (DDL)

创建表：

```sql
CREATE TABLE [表名] (
  [列1] [类型1],
  [列2] [类型2],
  ...
)
```

创建视图：

```sql
CREATE VIEW [视图名] AS
SELECT ...
```

### 6.1.3 数据操作语言 (DML)

插入数据：

```sql
INSERT INTO [目标表]
SELECT ...
```

## 6.2 数据类型

Arroyo 支持以下数据类型：

### 6.2.1 基本类型

- **整数类型**：`TINYINT`, `SMALLINT`, `INT`, `BIGINT`
- **浮点类型**：`FLOAT`, `DOUBLE`
- **定点数类型**：`DECIMAL(p, s)`
- **字符串类型**：`CHAR(n)`, `VARCHAR(n)`, `STRING`
- **布尔类型**：`BOOLEAN`
- **时间类型**：`DATE`, `TIME`, `TIMESTAMP`
- **二进制类型**：`BINARY`, `VARBINARY`

### 6.2.2 复合类型

- **数组类型**：`ARRAY<T>`
- **映射类型**：`MAP<K, V>`
- **结构类型**：`STRUCT<f1: T1, f2: T2, ...>`

## 6.3 函数和操作符

Arroyo 支持丰富的函数和操作符。

### 6.3.1 算术操作符

- `+`：加法
- `-`：减法
- `*`：乘法
- `/`：除法
- `%`：取模

### 6.3.2 比较操作符

- `=`：等于
- `<>`：不等于
- `<`：小于
- `<=`：小于等于
- `>`：大于
- `>=`：大于等于
- `IS NULL`：是否为空
- `IS NOT NULL`：是否非空
- `BETWEEN`：范围比较
- `IN`：集合包含
- `LIKE`：模式匹配

### 6.3.3 逻辑操作符

- `AND`：逻辑与
- `OR`：逻辑或
- `NOT`：逻辑非

### 6.3.4 聚合函数

- `COUNT`：计数
- `SUM`：求和
- `AVG`：平均值
- `MIN`：最小值
- `MAX`：最大值
- `STDDEV`：标准差
- `VARIANCE`：方差

### 6.3.5 字符串函数

- `CONCAT`：字符串连接
- `SUBSTRING`：子字符串
- `UPPER`：转大写
- `LOWER`：转小写
- `TRIM`：去除空格
- `LENGTH`：字符串长度
- `REGEXP_EXTRACT`：正则表达式提取

### 6.3.6 时间函数

- `CURRENT_TIMESTAMP`：当前时间戳
- `EXTRACT`：提取时间部分
- `DATE_FORMAT`：格式化日期
- `DATE_ADD`：日期加法
- `DATE_SUB`：日期减法
- `TIMESTAMPDIFF`：时间差

### 6.3.7 JSON 函数

- `JSON_VALUE`：提取 JSON 值
- `JSON_QUERY`：查询 JSON
- `JSON_EXISTS`：检查 JSON 路径是否存在
- `JSON_OBJECT`：创建 JSON 对象
- `JSON_ARRAY`：创建 JSON 数组

## 6.4 窗口函数

Arroyo 支持多种窗口函数，用于在流数据上执行有界计算。

### 6.4.1 滚动窗口 (Tumbling Window)

滚动窗口将数据分割成固定大小、不重叠的窗口。

```sql
SELECT 
  window_start, 
  window_end, 
  COUNT(*) as count
FROM TABLE(
  TUMBLE(TABLE source_table, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end
```

### 6.4.2 滑动窗口 (Sliding Window)

滑动窗口将数据分割成固定大小、可重叠的窗口。

```sql
SELECT 
  window_start, 
  window_end, 
  COUNT(*) as count
FROM TABLE(
  HOP(TABLE source_table, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end
```

### 6.4.3 会话窗口 (Session Window)

会话窗口根据活动会话将数据分组，会话之间的间隔超过指定时间则认为是新会话。

```sql
SELECT 
  window_start, 
  window_end, 
  COUNT(*) as count
FROM TABLE(
  SESSION(TABLE source_table, DESCRIPTOR(event_time), INTERVAL '10' MINUTE)
)
GROUP BY window_start, window_end
```

### 6.4.4 即时窗口 (Instant Window)

即时窗口针对单个事件创建窗口。

```sql
SELECT 
  event_time as window_time, 
  value
FROM TABLE(
  INSTANT(TABLE source_table, DESCRIPTOR(event_time))
)
```

## 6.5 连接操作

Arroyo 支持多种连接操作，用于关联不同的数据流。

### 6.5.1 流-流连接 (Stream-Stream Join)

```sql
SELECT a.id, a.name, b.value
FROM stream_a AS a
JOIN stream_b AS b
ON a.id = b.id
WHERE a.event_time BETWEEN b.event_time - INTERVAL '5' MINUTE AND b.event_time + INTERVAL '5' MINUTE
```

### 6.5.2 流-表连接 (Stream-Table Join)

```sql
SELECT a.id, a.value, b.name
FROM stream_a AS a
JOIN table_b AS b
ON a.id = b.id
```

### 6.5.3 时间窗口连接 (Windowed Join)

```sql
SELECT 
  a.id, 
  a.value as a_value, 
  b.value as b_value,
  window_start,
  window_end
FROM TABLE(
  TUMBLE(TABLE stream_a, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
) AS a
JOIN TABLE(
  TUMBLE(TABLE stream_b, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
) AS b
ON a.id = b.id AND a.window_start = b.window_start AND a.window_end = b.window_end
GROUP BY a.id, a.value, b.value, window_start, window_end
```

### 6.5.4 查找连接 (Lookup Join)

```sql
SELECT a.id, a.value, b.name
FROM stream_a AS a
JOIN lookup_table FOR SYSTEM_TIME AS OF a.proc_time AS b
ON a.id = b.id
```

## 6.6 时间属性

Arroyo 支持两种时间属性：事件时间和处理时间。

### 6.6.1 事件时间 (Event Time)

事件时间是事件实际发生的时间，通常由数据中的时间戳表示。

```sql
-- 定义事件时间
CREATE TABLE events (
  id INT,
  event_time TIMESTAMP(3),
  value DOUBLE,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
```

### 6.6.2 处理时间 (Processing Time)

处理时间是事件被系统处理的时间。

```sql
-- 使用处理时间
SELECT 
  id, 
  CURRENT_TIMESTAMP as proc_time, 
  value
FROM events
```

## 6.7 水印 (Watermark)

水印用于跟踪事件时间的进度，处理延迟数据。

```sql
-- 定义水印
CREATE TABLE events (
  id INT,
  event_time TIMESTAMP(3),
  value DOUBLE,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
```

## 6.8 用户定义函数 (UDF)

Arroyo 支持用户定义函数，允许用户扩展 SQL 功能。

### 6.8.1 标量函数 (Scalar Function)

```sql
-- 使用标量 UDF
SELECT id, multiply(value1, value2) as product
FROM source_table
```

### 6.8.2 表值函数 (Table Function)

```sql
-- 使用表值 UDF
SELECT id, value
FROM TABLE(explode(array_column))
```

### 6.8.3 聚合函数 (Aggregate Function)

```sql
-- 使用聚合 UDF
SELECT id, custom_agg(value) as result
FROM source_table
GROUP BY id
```
