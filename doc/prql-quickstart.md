# Arroyo PRQL 快速入门

本文档提供了在 Arroyo 中使用 PRQL 的快速入门指南。

## 什么是 PRQL？

PRQL（Pipeline Relational Query Language）是一种现代化的数据转换语言，旨在替代 SQL，提供更简洁、更强大的管道式查询能力。PRQL 的主要特点包括：

- **管道式语法**：每一行代表对前一行结果的转换，形成逻辑流水线
- **简洁明了**：语法简洁，易于阅读和编写
- **强大的抽象**：支持变量、函数等抽象机制
- **类型安全**：提供类型检查和推断
- **数据库无关**：可编译为多种 SQL 方言

## 在 Arroyo 中使用 PRQL

### 1. 通过 Web UI 使用 PRQL

1. 打开 Arroyo Web UI
2. 创建新管道
3. 在查询编辑器中输入 PRQL 查询
4. 系统会自动检测查询类型，或者您可以显式选择 PRQL 作为查询类型
5. 点击"创建管道"按钮

### 2. 通过 API 使用 PRQL

您可以通过 Arroyo API 提交 PRQL 查询：

```json
POST /api/v1/pipelines
{
  "name": "my_prql_pipeline",
  "query": "from events | filter event_type == 'click' | select {user_id, page_id, timestamp}",
  "query_type": "prql",
  "parallelism": 1
}
```

### 3. 通过命令行使用 PRQL

您可以使用 Arroyo CLI 提交 PRQL 查询：

```bash
arroyo run --query-type prql "from events | filter event_type == 'click' | select {user_id, page_id, timestamp}"
```

或者从文件中读取 PRQL 查询：

```bash
arroyo run --query-type prql -f query.prql
```

## PRQL 基础语法

### 1. 基本查询结构

PRQL 查询由一系列转换组成，每个转换都对前一个转换的结果进行操作：

```prql
from employees       # 数据源
filter age > 30      # 过滤条件
select {name, age}   # 选择列
```

### 2. 管道操作符

您可以使用管道操作符 `|` 连接转换：

```prql
from employees | filter age > 30 | select {name, age}
```

### 3. 常用转换

- **from**：指定数据源
- **filter**：过滤行
- **select**：选择或重命名列
- **derive**：添加新列
- **aggregate**：聚合计算
- **sort**：排序
- **take**：限制结果数量
- **join**：连接表
- **group**：分组

### 4. 变量和函数

PRQL 支持变量和函数定义：

```prql
let min_age = 30

let is_eligible = age -> age > min_age

from employees
filter is_eligible age
select {name, age}
```

## Arroyo 特定功能

### 1. 窗口操作

```prql
from events
window tumbling 5m (
  group user_id (
    aggregate {
      event_count = count
    }
  )
)
```

### 2. 水印

```prql
from events
# -- WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
filter event_time > s.timestamp - interval 1 hour
```

### 3. 连接器配置

```prql
# -- KAFKA_SOURCE: topic=events, bootstrap.servers=localhost:9092, format=json
from events
filter value > 10
# -- KAFKA_SINK: topic=filtered_events, bootstrap.servers=localhost:9092, format=json
```

## 示例

### 1. 基本过滤和投影

```prql
from events
filter event_type == "click"
select {user_id, page_id, timestamp}
```

### 2. 添加派生列

```prql
from events
derive {
  event_hour = extract_hour timestamp,
  event_date = date_trunc "day" timestamp,
  is_weekend = day_of_week timestamp in [6, 7]
}
```

### 3. 聚合计算

```prql
from events
group user_id (
  aggregate {
    event_count = count,
    first_event = min timestamp,
    last_event = max timestamp
  }
)
```

### 4. 窗口聚合

```prql
from events
window tumbling 1h (
  group {user_id, page_id} (
    aggregate {
      view_count = count,
      avg_duration = average duration
    }
  )
)
```

### 5. 连接操作

```prql
from events
join users (==user_id)
select {
  events.timestamp,
  events.event_type,
  users.username,
  users.country
}
```

## 进一步学习

- 查看 `examples/prql` 目录中的示例
- 阅读 [PRQL 官方文档](https://prql-lang.org/)
- 参考 [Arroyo PRQL 支持文档](doc/prql-support.md)
