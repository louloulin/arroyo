# Arroyo PRQL 指南

本文档详细介绍了如何在 Arroyo 流处理系统中使用 PRQL 查询语言。

## 1. 什么是 PRQL？

PRQL（Pipeline Relational Query Language）是一种现代化的数据转换语言，旨在替代 SQL，提供更简洁、更强大的管道式查询能力。PRQL 的主要特点包括：

- **管道式语法**：每一行代表对前一行结果的转换，形成逻辑流水线
- **简洁明了**：语法简洁，易于阅读和编写
- **强大的抽象**：支持变量、函数等抽象机制
- **类型安全**：提供类型检查和推断
- **数据库无关**：可编译为多种 SQL 方言

## 2. Arroyo 中的 PRQL 支持

Arroyo 通过 `arroyo-prql` 模块提供了对 PRQL 的支持，允许用户使用 PRQL 编写流处理查询。Arroyo 的 PRQL 支持包括：

- **基本 PRQL 语法**：支持标准 PRQL 语法，如过滤、选择、聚合等
- **Arroyo 特定扩展**：支持 Arroyo 特定的功能，如连接器、窗口函数和水印
- **自动转换**：自动将 PRQL 查询转换为 Arroyo SQL 查询

## 3. 基本语法

### 3.1 基本查询结构

PRQL 查询由一系列转换组成，每个转换都对前一个转换的结果进行操作：

```prql
from employees       # 数据源
filter age > 30      # 过滤条件
select {name, age}   # 选择列
```

### 3.2 管道操作符

您可以使用管道操作符 `|` 连接转换：

```prql
from employees | filter age > 30 | select {name, age}
```

或者使用换行来隐式连接转换：

```prql
from employees
filter age > 30
select {name, age}
```

### 3.3 常用转换

- **from**：指定数据源
- **filter**：过滤行
- **select**：选择或重命名列
- **derive**：添加新列
- **aggregate**：聚合计算
- **sort**：排序
- **take**：限制结果数量
- **join**：连接表
- **group**：分组

### 3.4 变量和函数

PRQL 支持变量和函数定义：

```prql
let min_age = 30

let is_eligible = age -> age > min_age

from employees
filter is_eligible age
select {name, age}
```

## 4. Arroyo 特定扩展

### 4.1 连接器语法

#### 4.1.1 Kafka 源连接器

```prql
from kafka (
  topic = "events",
  bootstrap.servers = "localhost:9092",
  format = "json"
) as events

filter event_type == "click"
select {user_id, page_id, event_time}
```

#### 4.1.2 Kafka 目标连接器

```prql
from events
filter event_type == "click"
select {user_id, page_id, event_time}

into kafka (
  topic = "filtered_events",
  bootstrap.servers = "localhost:9092",
  format = "json"
)
```

#### 4.1.3 文件源连接器

```prql
from file (
  path = "/tmp/arroyo/input",
  format = "json",
  pattern = "*.json"
) as input_data

filter value > 10
select {id, name, value}
```

#### 4.1.4 文件目标连接器

```prql
from data
filter value > 10
select {id, name, value}

into file (
  path = "/tmp/arroyo/output",
  format = "json",
  write_mode = "append"
)
```

### 4.2 水印语法

水印用于处理延迟数据，在 Arroyo PRQL 中可以使用以下语法定义：

```prql
from events
watermark (
  field = event_time,
  delay = 5s
)
filter event_type == "click"
```

支持的时间单位包括：
- `s`：秒
- `m`：分钟
- `h`：小时
- `d`：天

### 4.3 窗口函数

Arroyo PRQL 支持多种窗口函数，用于时间相关的聚合操作：

#### 4.3.1 滚动窗口

```prql
from events
window tumbling (
  size = 5m,
  time_field = event_time
) (
  group user_id (
    aggregate {
      event_count = count
    }
  )
)
```

#### 4.3.2 滑动窗口

```prql
from events
window sliding (
  size = 10m,
  slide = 1m,
  time_field = event_time
) (
  group user_id (
    aggregate {
      event_count = count
    }
  )
)
```

#### 4.3.3 会话窗口

```prql
from events
window session (
  gap = 30m,
  time_field = event_time
) (
  group user_id (
    aggregate {
      event_count = count
    }
  )
)
```

### 4.4 时间函数

Arroyo PRQL 支持多种时间函数，用于处理时间相关的操作：

```prql
from events
derive {
  processing_ts = processing_time(),
  event_hour = extract_hour(event_time),
  event_minute = extract_minute(event_time),
  event_day = extract_day(event_time),
  event_month = extract_month(event_time),
  event_year = extract_year(event_time)
}
```

## 5. 完整示例

### 5.1 实时点击流分析

```prql
from kafka (
  topic = "clickstream",
  bootstrap.servers = "localhost:9092",
  format = "json"
) as clicks

watermark (
  field = event_time,
  delay = 5s
)

filter event_type == "click"

window tumbling (
  size = 5m,
  time_field = event_time
) (
  group {user_id, page_id} (
    aggregate {
      click_count = count
    }
  )
)

filter click_count > 10

into kafka (
  topic = "high_activity_pages",
  bootstrap.servers = "localhost:9092",
  format = "json"
)
```

### 5.2 异常交易检测

```prql
from kafka (
  topic = "transactions",
  bootstrap.servers = "localhost:9092",
  format = "json"
) as transactions

watermark (
  field = transaction_time,
  delay = 1m
)

window sliding (
  size = 1h,
  slide = 5m,
  time_field = transaction_time
) (
  group card_number (
    aggregate {
      tx_count = count,
      total_amount = sum amount,
      max_amount = max amount
    }
  )
)

derive {
  is_suspicious = tx_count > 10 || max_amount > 1000 || total_amount > 5000
}

filter is_suspicious

into kafka (
  topic = "suspicious_transactions",
  bootstrap.servers = "localhost:9092",
  format = "json"
)
```

## 6. 最佳实践

### 6.1 查询结构

- 使用换行而不是管道操作符，提高可读性
- 使用缩进表示查询的层次结构
- 将复杂查询拆分为多个变量和函数

### 6.2 命名约定

- 使用小写字母和下划线命名变量和函数
- 使用描述性名称，避免缩写
- 保持命名风格一致

### 6.3 性能优化

- 尽早进行过滤，减少数据量
- 只选择需要的列，避免不必要的数据传输
- 合理设置窗口大小和滑动步长

## 7. 故障排除

### 7.1 常见错误

- **语法错误**：检查 PRQL 语法是否正确
- **类型错误**：检查变量和函数的类型是否匹配
- **连接器配置错误**：检查连接器配置是否正确

### 7.2 调试技巧

- 使用简单查询验证数据源
- 逐步添加转换，确保每一步都正确
- 检查生成的 SQL 查询，了解转换过程

## 8. 参考资料

- [PRQL 官方文档](https://prql-lang.org/)
- [Arroyo 文档](https://arroyo.dev/)
- [Arroyo PRQL 示例](https://github.com/ArroyoSystems/arroyo/tree/master/examples/prql)
