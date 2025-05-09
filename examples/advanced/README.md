# Arroyo 高级功能示例

本目录包含了 Arroyo 的高级功能示例，展示了更复杂的流处理场景。

## 示例列表

### 1. 复杂 ETL 流程 (complex_etl.sql)

展示如何实现复杂的 ETL（提取、转换、加载）流程。

### 2. 欺诈检测 (fraud_detection.sql)

展示如何使用流处理进行实时欺诈检测。

### 3. 异常检测 (anomaly_detection.sql)

展示如何使用流处理进行实时异常检测。

### 4. 实时分析 (real_time_analytics.sql)

展示如何使用流处理进行实时数据分析。

### 5. 模式匹配 (pattern_matching.sql)

展示如何使用模式匹配功能检测复杂事件模式。

### 6. 多流处理 (multi_stream_processing.sql)

展示如何同时处理和关联多个数据流。

## 运行示例

每个示例都可以通过 Web UI 或命令行运行。例如，要运行欺诈检测示例：

```bash
arroyo run examples/advanced/fraud_detection.sql
```

或者在 Web UI 中创建新管道，复制 fraud_detection.sql 的内容到查询编辑器中。
