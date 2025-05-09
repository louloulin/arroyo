# Arroyo 示例集合

本目录包含了各种 Arroyo 流处理系统的示例，帮助用户快速了解和使用 Arroyo 的各种功能。

## 目录结构

- **basic/** - 基础示例，展示基本的 SQL 查询和操作
- **windows/** - 窗口操作示例，展示各种窗口函数的使用
- **joins/** - 连接操作示例，展示各种连接操作的使用
- **connectors/** - 连接器示例，展示各种连接器的配置和使用
- **udfs/** - 用户定义函数示例，展示如何创建和使用 UDF
- **advanced/** - 高级功能示例，展示更复杂的流处理场景

## 如何运行示例

每个示例目录中都包含一个或多个 SQL 文件和相应的说明文档。要运行示例，可以使用以下方法：

### 方法一：使用 Web UI

1. 启动 Arroyo 集群：
   ```bash
   arroyo cluster
   ```

2. 打开浏览器访问 Web UI：
   ```
   http://localhost:5115
   ```

3. 创建新管道，复制示例 SQL 到查询编辑器中，然后点击"创建"按钮。

### 方法二：使用命令行

1. 使用 `arroyo run` 命令运行 SQL 文件：
   ```bash
   arroyo run examples/basic/simple_filter.sql
   ```

## 示例概览

### 基础示例 (basic/)

- **simple_filter.sql** - 简单的过滤操作
- **projection.sql** - 列投影操作
- **aggregation.sql** - 基本聚合操作
- **watermark.sql** - 水印定义和使用

### 窗口操作示例 (windows/)

- **tumbling_window.sql** - 滚动窗口示例
- **sliding_window.sql** - 滑动窗口示例
- **session_window.sql** - 会话窗口示例
- **global_window.sql** - 全局窗口示例

### 连接操作示例 (joins/)

- **stream_stream_join.sql** - 流-流连接示例
- **stream_table_join.sql** - 流-表连接示例
- **windowed_join.sql** - 窗口连接示例
- **temporal_join.sql** - 时态连接示例

### 连接器示例 (connectors/)

- **kafka/** - Kafka 连接器示例
- **filesystem/** - 文件系统连接器示例
- **kinesis/** - Kinesis 连接器示例
- **redis/** - Redis 连接器示例
- **mqtt/** - MQTT 连接器示例
- **http/** - HTTP 连接器示例

### 用户定义函数示例 (udfs/)

- **scalar_udf.sql** - 标量 UDF 示例
- **table_udf.sql** - 表值 UDF 示例
- **aggregate_udf.sql** - 聚合 UDF 示例
- **async_udf.sql** - 异步 UDF 示例

### 高级功能示例 (advanced/)

- **complex_etl.sql** - 复杂 ETL 流程示例
- **fraud_detection.sql** - 欺诈检测示例
- **anomaly_detection.sql** - 异常检测示例
- **real_time_analytics.sql** - 实时分析示例
- **pattern_matching.sql** - 模式匹配示例

## 贡献

欢迎贡献新的示例！如果您有好的示例想要分享，请提交 Pull Request。
