# Arroyo 用户定义函数示例

本目录包含了 Arroyo 的用户定义函数 (UDF) 示例，展示了如何创建和使用 UDF。

## UDF 概述

用户定义函数 (UDF) 允许您扩展 Arroyo 的功能，实现自定义的数据处理逻辑。Arroyo 支持以下类型的 UDF：

1. **标量 UDF**：处理单个输入行，返回单个输出值
2. **表值 UDF**：处理单个输入行，返回多个输出行
3. **聚合 UDF**：处理多个输入行，返回单个聚合值
4. **异步 UDF**：支持异步操作的 UDF

## 支持的语言

Arroyo 支持使用以下语言编写 UDF：

1. **Rust**：获得最佳性能
2. **Python**：提供更高的灵活性

## 示例列表

### 1. 标量 UDF (scalar_udf.sql)

展示如何创建和使用标量 UDF。

### 2. 表值 UDF (table_udf.sql)

展示如何创建和使用表值 UDF。

### 3. 聚合 UDF (aggregate_udf.sql)

展示如何创建和使用聚合 UDF。

### 4. 异步 UDF (async_udf.sql)

展示如何创建和使用异步 UDF。

## 运行示例

每个示例都可以通过 Web UI 或命令行运行。例如，要运行标量 UDF 示例：

```bash
arroyo run examples/udfs/scalar_udf.sql
```

或者在 Web UI 中创建新管道，复制 scalar_udf.sql 的内容到查询编辑器中。
