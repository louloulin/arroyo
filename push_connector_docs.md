# Arroyo 主动推送连接器文档

本文档详细描述了 Arroyo 主动推送连接器的设计、实现和使用方法。

## 1. 概述

Arroyo 主动推送连接器允许外部系统直接将数据推送到 Arroyo 流处理系统，而不是由 Arroyo 主动从外部系统拉取数据。这种模式在某些场景下具有明显优势，如实时事件处理、IoT 数据收集等。

主动推送连接器支持多种协议（HTTP、QUIC、gRPC、WebSocket），用户可以根据自己的需求选择最适合的协议。

## 2. 功能特性

- **多协议支持**：HTTP、QUIC、gRPC、WebSocket
- **SQL 配置**：通过 SQL 语法简化配置
- **高性能**：低延迟、高吞吐量
- **可靠性**：数据不丢失、至少一次处理语义
- **可扩展性**：支持水平扩展
- **多语言 SDK**：Rust、Python、Java、JavaScript/TypeScript、Go

## 3. 安装与配置

### 3.1 前提条件

- Arroyo 版本 >= X.Y.Z
- Rust 版本 >= 1.70.0
- 其他依赖...

### 3.2 安装步骤

1. 确保 Arroyo 已正确安装
2. 安装主动推送连接器：

```bash
# 安装命令
```

### 3.3 配置选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `buffer_size` | 内存缓冲区大小 | `10MB` |
| `batch_size` | 批处理大小 | `1000` |
| ... | ... | ... |

## 4. 使用方法

### 4.1 创建推送源

#### 4.1.1 HTTP 协议

```sql
CREATE TABLE http_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    protocol = 'http',
    topic = 'user_events',
    format = 'json',
    buffer_size = '10MB'
);
```

#### 4.1.2 QUIC 协议

```sql
CREATE TABLE quic_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    protocol = 'quic',
    topic = 'device_events',
    format = 'json',
    quic.max_concurrent_streams = '100'
);
```

#### 4.1.3 gRPC 协议

```sql
CREATE TABLE grpc_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    protocol = 'grpc',
    topic = 'service_events',
    format = 'json',
    grpc.max_message_size = '5MB'
);
```

#### 4.1.4 WebSocket 协议

```sql
CREATE TABLE ws_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    protocol = 'websocket',
    topic = 'browser_events',
    format = 'json'
);
```

### 4.2 使用 SDK 推送数据

#### 4.2.1 Rust SDK

```rust
use arroyo_push_sdk::{PushClient, PushClientConfig, TransportProtocol};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建客户端
    let config = PushClientConfig {
        protocol: TransportProtocol::Http,
        base_url: "http://localhost:8000".to_string(),
        api_key: Some("your-api-key".to_string()),
        ..Default::default()
    };
    let client = PushClient::new(config);
    
    // 推送数据
    client.push("user_events", b"{\"id\":\"123\",\"data\":\"example\"}".to_vec(), None).await?;
    
    Ok(())
}
```

#### 4.2.2 Python SDK

```python
from arroyo_push_sdk import PushClient, TransportProtocol

# 创建客户端
client = PushClient(
    protocol=TransportProtocol.HTTP,
    base_url="http://localhost:8000",
    api_key="your-api-key"
)

# 推送数据
client.push("user_events", b'{"id":"123","data":"example"}')
```

## 5. 性能优化

### 5.1 批处理

为了提高性能，建议使用批处理 API：

```rust
let batch = vec![
    b"{\"id\":\"123\",\"data\":\"example1\"}".to_vec(),
    b"{\"id\":\"124\",\"data\":\"example2\"}".to_vec(),
    b"{\"id\":\"125\",\"data\":\"example3\"}".to_vec(),
];
client.push_batch("user_events", batch, None).await?;
```

### 5.2 压缩

对于大量数据，建议启用压缩：

```rust
let config = PushClientConfig {
    protocol: TransportProtocol::Http,
    base_url: "http://localhost:8000".to_string(),
    compression: Some(CompressionType::Zstd),
    ..Default::default()
};
```

## 6. 监控与故障排除

### 6.1 指标

主动推送连接器暴露以下 Prometheus 指标：

- `arroyo_push_messages_total`：接收的消息总数
- `arroyo_push_bytes_total`：接收的字节总数
- `arroyo_push_errors_total`：错误总数
- `arroyo_push_latency_ms`：消息处理延迟（毫秒）

### 6.2 日志

日志位于 `logs/push_connector.log`，包含以下级别：

- ERROR：严重错误
- WARN：警告
- INFO：一般信息
- DEBUG：调试信息

### 6.3 常见问题

1. **连接失败**
   - 检查网络连接
   - 验证认证信息
   - 确认服务器状态

2. **性能问题**
   - 增加批处理大小
   - 启用压缩
   - 调整缓冲区大小

## 7. 安全考虑

### 7.1 认证

支持以下认证方式：

- API 密钥
- OAuth 2.0
- 客户端证书

### 7.2 授权

基于主题的访问控制，可以限制对特定主题的访问。

### 7.3 数据加密

- 传输层加密 (TLS)
- 可选的负载加密

## 8. 示例应用

### 8.1 实时仪表板

```sql
-- 创建推送源
CREATE TABLE user_events (
    user_id STRING,
    event_type STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    protocol = 'http',
    topic = 'user_events',
    format = 'json'
);

-- 创建实时计数视图
CREATE VIEW event_counts AS
SELECT
    TUMBLE_START(timestamp, INTERVAL '1' MINUTE) as window_start,
    event_type,
    COUNT(*) as event_count
FROM user_events
GROUP BY TUMBLE(timestamp, INTERVAL '1' MINUTE), event_type;
```

## 9. API 参考

### 9.1 REST API

#### 9.1.1 推送数据

```
POST /api/v1/push/{topic}
```

请求体：JSON 或二进制数据

响应：

```json
{
  "success": true,
  "message": "Data received",
  "offset": 12345
}
```

### 9.2 gRPC API

详见 `push.proto` 文件。

## 10. 版本历史

| 版本 | 日期 | 变更内容 |
|------|------|---------|
| 0.1.0 | YYYY-MM-DD | 初始版本 |
| ... | ... | ... |
