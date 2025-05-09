# Arroyo 窗口操作示例

本目录包含了 Arroyo 的窗口操作示例，展示了各种窗口函数的使用。

## 示例列表

### 1. 滚动窗口 (tumbling_window.sql)

展示如何使用滚动窗口（固定大小、不重叠的窗口）进行聚合操作。

### 2. 滑动窗口 (sliding_window.sql)

展示如何使用滑动窗口（固定大小、可重叠的窗口）进行聚合操作。

### 3. 会话窗口 (session_window.sql)

展示如何使用会话窗口（由活动会话定义的窗口）进行聚合操作。

### 4. 全局窗口 (global_window.sql)

展示如何使用全局窗口（处理整个数据流）进行聚合操作。

### 5. 窗口函数 (window_functions.sql)

展示如何使用窗口函数（如 ROW_NUMBER、RANK、LEAD、LAG 等）进行分析操作。

### 6. 窗口连接 (window_join.sql)

展示如何在窗口内连接多个数据流。

## 运行示例

每个示例都可以通过 Web UI 或命令行运行。例如，要运行滚动窗口示例：

```bash
arroyo run examples/windows/tumbling_window.sql
```

或者在 Web UI 中创建新管道，复制 tumbling_window.sql 的内容到查询编辑器中。
