# Arroyo PRQL 示例

本目录包含了 Arroyo 的 PRQL 示例，展示了如何使用 PRQL 语言编写流处理查询。

## 基础示例

### 1. 基本查询 (basic_query.prql)

展示基本的 PRQL 查询语法，包括过滤、选择和排序。

### 2. 聚合操作 (aggregation.prql)

展示如何使用 PRQL 进行聚合操作，如计数、求和和平均值。

### 3. 窗口操作 (window_operations.prql)

展示如何使用 PRQL 进行窗口操作，如滚动窗口和滑动窗口。

### 4. 连接操作 (joins.prql)

展示如何使用 PRQL 连接多个数据流。

### 5. 变量和函数 (variables_and_functions.prql)

展示如何在 PRQL 中使用变量和函数。

### 6. 嵌套查询 (nested_queries.prql)

展示如何在 PRQL 中使用嵌套查询和复杂数据转换。

## Arroyo 特定功能

### 7. Arroyo 特定功能 (arroyo_specific.prql)

展示 PRQL 中 Arroyo 特定的功能概览，包括连接器、窗口函数和时间函数。

### 8. Kafka 连接器 (kafka_connector.prql)

展示如何在 PRQL 中使用 Kafka 源连接器和目标连接器。

### 9. 文件连接器 (file_connector.prql)

展示如何在 PRQL 中使用文件源连接器和目标连接器。

### 10. 窗口函数 (window_functions.prql)

展示如何在 PRQL 中使用窗口函数进行时间相关的聚合操作。

### 11. 时间函数 (time_functions.prql)

展示如何在 PRQL 中使用时间函数处理时间相关的操作。

### 12. 复杂管道 (complex_pipeline.prql)

展示如何在 PRQL 中构建复杂的流处理管道，结合多种功能。

## 使用方法

1. 确保已安装 Arroyo
2. 使用以下命令运行示例：

```bash
arroyo run --query-type prql -f examples/prql/basic_query.prql
```

或者通过 Web UI 上传并运行示例。

## 运行示例

每个示例都可以通过 Web UI 或命令行运行。例如，要运行基本查询示例：

```bash
arroyo run examples/prql/basic_query.prql
```

或者在 Web UI 中创建新管道，复制 basic_query.prql 的内容到查询编辑器中，并选择 PRQL 作为查询类型。
