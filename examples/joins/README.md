# Arroyo 连接操作示例

本目录包含了 Arroyo 的连接操作示例，展示了各种连接操作的使用。

## 示例列表

### 1. 流-流连接 (stream_stream_join.sql)

展示如何连接两个数据流，处理两个流中的相关事件。

### 2. 流-表连接 (stream_table_join.sql)

展示如何将流数据与静态表数据连接，丰富流数据的上下文。

### 3. 窗口连接 (windowed_join.sql)

展示如何在时间窗口内连接多个数据流，处理时间上相关的事件。

### 4. 时态连接 (temporal_join.sql)

展示如何使用时态连接（Temporal Join）处理随时间变化的维度表。

### 5. 间隔连接 (interval_join.sql)

展示如何使用间隔连接（Interval Join）处理在一定时间范围内的相关事件。

### 6. 查找连接 (lookup_join.sql)

展示如何使用查找连接（Lookup Join）从外部系统获取数据。

## 运行示例

每个示例都可以通过 Web UI 或命令行运行。例如，要运行流-流连接示例：

```bash
arroyo run examples/joins/stream_stream_join.sql
```

或者在 Web UI 中创建新管道，复制 stream_stream_join.sql 的内容到查询编辑器中。
