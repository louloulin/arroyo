# PRQL 语法扩展计划

## 1. 概述

本文档详细描述了在 Arroyo 流处理系统中扩展 PRQL (Pipelined Relational Query Language) 语法的计划，以支持 Arroyo 特定功能，而不是使用注释方式。通过扩展 PRQL 语法，我们可以为用户提供更一致、更直观的查询体验，同时保持 PRQL 简洁、易读的特点。

### 1.1 当前状态

目前，Arroyo 的 PRQL 支持通过注释方式实现特定功能，例如：

```prql
# -- KAFKA_SOURCE: topic=events, bootstrap.servers=localhost:9092, format=json
from events

# -- WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

这种方式存在以下问题：

1. **语法不正式**：注释本应用于代码说明，而非功能实现
2. **解析困难**：依赖正则表达式解析，容易出现边缘情况
3. **错误处理不友好**：注释中的错误难以定位和报告
4. **与 IDE 集成困难**：无法提供语法高亮和自动完成
5. **学习曲线陡峭**：用户需要学习两套语法

### 1.2 目标

我们的目标是扩展 PRQL 语法，使其能够原生支持 Arroyo 的特定功能，包括：

1. 连接器配置（Kafka、文件系统等）
2. 窗口函数（滚动窗口、滑动窗口、会话窗口）
3. 水印定义
4. 时间函数和操作

## 2. 语法扩展设计

### 2.1 连接器语法

#### 2.1.1 源连接器

**当前（注释方式）**：
```prql
# -- KAFKA_SOURCE: topic=events, bootstrap.servers=localhost:9092, format=json
from events
```

**扩展语法**：
```prql
from kafka (
  topic = "events",
  bootstrap.servers = "localhost:9092",
  format = "json"
) as events
```

#### 2.1.2 目标连接器

**当前（注释方式）**：
```prql
# -- KAFKA_SINK: topic=output, bootstrap.servers=localhost:9092, format=json
```

**扩展语法**：
```prql
into kafka (
  topic = "output",
  bootstrap.servers = "localhost:9092",
  format = "json"
)
```

#### 2.1.3 文件系统连接器

**扩展语法**：
```prql
from file (
  path = "/tmp/arroyo/input",
  format = "json",
  pattern = "*.json"
) as input_data

into file (
  path = "/tmp/arroyo/output",
  format = "parquet",
  write_mode = "append"
)
```

### 2.2 窗口函数语法

#### 2.2.1 滚动窗口

**当前（PRQL 内置）**：
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

**扩展语法**（保持兼容，增加更多选项）：
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

#### 2.2.2 滑动窗口

**扩展语法**：
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

#### 2.2.3 会话窗口

**扩展语法**：
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

### 2.3 水印语法

**当前（注释方式）**：
```prql
# -- WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

**扩展语法**：
```prql
from events
watermark (
  field = event_time,
  delay = 5s
)
```

### 2.4 时间函数和操作

**当前**：
```prql
derive {
  processing_ts = processing_time(),
  event_hour = extract_hour event_time
}
```

**扩展语法**（保持兼容，增加更多函数）：
```prql
derive {
  processing_ts = processing_time(),
  event_hour = extract_hour(event_time),
  window_start = window_start(),
  window_end = window_end()
}
```

## 3. 语法规范

### 3.1 连接器语法规范

```
from_connector_expr ::= "from" connector_type "(" connector_options ")" ["as" alias]
into_connector_expr ::= "into" connector_type "(" connector_options ")"
connector_type ::= "kafka" | "file" | "jdbc" | "http" | ...
connector_options ::= option_name "=" option_value ["," connector_options]
option_name ::= identifier
option_value ::= string_literal | number_literal | boolean_literal
```

### 3.2 窗口函数语法规范

```
window_expr ::= "window" window_type "(" window_options ")" "(" window_body ")"
window_type ::= "tumbling" | "sliding" | "session"
window_options ::= window_option ["," window_options]
window_option ::= option_name "=" option_value
window_body ::= group_expr | aggregate_expr | ...
```

### 3.3 水印语法规范

```
watermark_expr ::= "watermark" "(" watermark_options ")"
watermark_options ::= watermark_option ["," watermark_options]
watermark_option ::= option_name "=" option_value
```

## 4. 实现计划

### 4.1 阶段一：语法解析器扩展 ✅

1. **扩展 PRQL 解析器**：✅
   - 创建自定义解析器模块 `parser.rs`
   - 添加对新语法结构的支持
   - 实现语法验证和错误报告

2. **AST 扩展**：✅
   - 定义新的数据结构表示扩展语法
   - 实现 `ConnectorConfig`、`WindowConfig` 和 `WatermarkConfig` 类型

3. **语义分析**：✅
   - 实现新语法结构的语义检查
   - 验证连接器选项和窗口参数

### 4.2 阶段二：SQL 生成器扩展 ✅

1. **连接器转换**：✅
   - 实现从扩展 PRQL 语法到 Arroyo SQL 连接器语法的转换
   - 处理连接器特定的选项和参数
   - 支持 Kafka 和文件连接器

2. **窗口函数转换**：✅
   - 实现从 PRQL 窗口语法到 Arroyo SQL 窗口函数的转换
   - 支持滚动窗口、滑动窗口和会话窗口

3. **水印转换**：✅
   - 实现从 PRQL 水印语法到 Arroyo SQL 水印定义的转换
   - 支持延迟参数和时间单位转换

4. **时间函数转换**：✅
   - 实现 PRQL 时间函数到 Arroyo SQL 时间函数的映射
   - 支持提取时间部分和处理时间函数

### 4.3 阶段三：集成和测试 ✅

1. **集成到 Arroyo**：✅
   - 将扩展的 PRQL 解析器和 SQL 生成器集成到 Arroyo
   - 更新 lib.rs 以支持新的解析器和转换器

2. **测试套件**：✅
   - 创建测试用例覆盖所有新语法
   - 验证转换的正确性
   - 测试边缘情况和错误处理

3. **文档和示例**：✅
   - 更新 PRQL 文档以包含新语法
   - 创建示例查询展示新功能

## 5. 具体实现示例

### 5.1 Kafka 连接器示例

**PRQL 查询**：
```prql
from kafka (
  topic = "orders",
  bootstrap.servers = "localhost:9092",
  format = "json"
) as orders

filter total_amount > 100

window tumbling (
  size = 10m,
  time_field = order_time
) (
  group customer_id (
    aggregate {
      order_count = count,
      total_spent = sum total_amount
    }
  )
)

filter order_count > 1

into kafka (
  topic = "high_value_customers",
  bootstrap.servers = "localhost:9092",
  format = "json"
)
```

**转换后的 SQL**：
```sql
SELECT
  customer_id,
  COUNT(*) AS order_count,
  SUM(total_amount) AS total_spent,
  TUMBLE_START(order_time, INTERVAL '10' MINUTE) AS window_start,
  TUMBLE_END(order_time, INTERVAL '10' MINUTE) AS window_end
FROM TABLE(
  TUMBLE(
    TABLE KAFKA(
      topic => 'orders',
      properties => (
        'bootstrap.servers' => 'localhost:9092'
      ),
      format => json
    ),
    DESCRIPTOR(order_time),
    INTERVAL '10' MINUTE
  )
)
WHERE total_amount > 100
GROUP BY customer_id, TUMBLE_START(order_time, INTERVAL '10' MINUTE), TUMBLE_END(order_time, INTERVAL '10' MINUTE)
HAVING COUNT(*) > 1
INSERT INTO KAFKA(
  topic => 'high_value_customers',
  properties => (
    'bootstrap.servers' => 'localhost:9092'
  ),
  format => json
)
```

### 5.2 文件系统连接器示例

**PRQL 查询**：
```prql
from file (
  path = "/tmp/arroyo/input",
  format = "csv",
  field_delimiter = ",",
  quote_character = "\""
) as users

derive {
  age_group = if age < 18 then "minor"
              else if age < 65 then "adult"
              else "senior"
}

group age_group (
  aggregate {
    user_count = count,
    avg_age = average age
  }
)

into file (
  path = "/tmp/arroyo/output",
  format = "json",
  write_mode = "overwrite"
)
```

## 6. 兼容性考虑

### 6.1 向后兼容性

为了确保向后兼容性，我们将：

1. 保留对现有 PRQL 语法的支持
2. 继续支持注释方式，但将其标记为已弃用
3. 提供迁移工具，帮助用户从注释方式迁移到新语法

### 6.2 与 PRQL 社区协作

我们计划：

1. 与 PRQL 社区分享我们的扩展计划
2. 探讨将流处理特定功能纳入 PRQL 标准的可能性
3. 贡献我们的实现到 PRQL 项目

## 7. 时间线

### 7.1 阶段一（语法解析器扩展）- 4周

- 周 1-2：设计语法规范和 AST 扩展
- 周 3-4：实现解析器扩展和语义分析

### 7.2 阶段二（SQL 生成器扩展）- 6周

- 周 1-2：实现连接器转换
- 周 3-4：实现窗口函数和水印转换
- 周 5-6：实现时间函数转换和优化

### 7.3 阶段三（集成和测试）- 4周

- 周 1-2：集成到 Arroyo 和初步测试
- 周 3-4：全面测试、文档和示例

## 8. 结论

通过扩展 PRQL 语法以原生支持 Arroyo 特定功能，我们可以为用户提供更一致、更直观的查询体验。这种方法比使用注释方式更加正式、更易于使用，同时保持了 PRQL 简洁、易读的特点。

扩展计划分为三个阶段，从语法解析器扩展到 SQL 生成器扩展再到集成和测试，确保了平稳的开发过程和高质量的最终产品。通过与 PRQL 社区协作，我们还可以探索将这些扩展纳入 PRQL 标准的可能性，为更广泛的流处理用例提供支持。
