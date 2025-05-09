# Arroyo 基础示例

本目录包含了 Arroyo 的基础示例，展示了基本的 SQL 查询和操作。

## 示例列表

### 1. 简单过滤 (simple_filter.sql)

展示如何使用 WHERE 子句过滤数据流中的记录。

### 2. 列投影 (projection.sql)

展示如何选择和转换数据流中的列。

### 3. 基本聚合 (aggregation.sql)

展示如何使用聚合函数对数据流进行聚合操作。

### 4. 水印定义和使用 (watermark.sql)

展示如何定义和使用水印处理事件时间。

### 5. 简单转换 (transformation.sql)

展示如何使用各种函数和表达式转换数据。

### 6. 数据生成 (data_generation.sql)

展示如何使用 impulse() 和其他函数生成测试数据。

## 运行示例

每个示例都可以通过 Web UI 或命令行运行。例如，要运行简单过滤示例：

```bash
arroyo run examples/basic/simple_filter.sql
```

或者在 Web UI 中创建新管道，复制 simple_filter.sql 的内容到查询编辑器中。
