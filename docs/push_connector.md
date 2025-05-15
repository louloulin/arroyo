# Arroyo 主动推送连接器

## 概述

Arroyo 主动推送连接器允许外部系统直接将数据推送到 Arroyo 流处理系统，而不是由 Arroyo 主动从外部系统拉取数据。这种模式在某些场景下具有明显优势，如实时事件处理、IoT 数据收集等。

主动推送连接器支持多种协议（HTTP、QUIC、gRPC、WebSocket），用户可以根据自己的需求选择最适合的协议。

## 功能特性

- **多协议支持**：HTTP、QUIC、gRPC、WebSocket
- **SQL 配置**：通过 SQL 语法简化配置
- **高性能**：低延迟、高吞吐量
- **可靠性**：数据不丢失、至少一次处理语义
- **可扩展性**：支持水平扩展

## 架构设计

主动推送连接器由以下组件组成：

1. **连接器框架**：实现 `Connector` trait 的 `PushConnector` 结构体
2. **配置结构**：定义连接器配置的 `PushConfig` 结构体
3. **表配置结构**：定义连接到特定资源的 `PushTable` 结构体
4. **源操作符**：实现 `SourceOperator` trait 的 `PushSourceFunc` 结构体
5. **协议服务器**：根据配置启动相应的协议服务器（HTTP、QUIC、gRPC、WebSocket）

## 使用方法

### SQL 语法

```sql
-- 创建使用 HTTP 协议的推送源
CREATE TABLE http_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    protocol = 'http',  -- 指定使用 HTTP 协议
    topic = 'user_events',
    format = 'json',
    buffer_size = '10MB'  -- 可选配置
);

-- 创建使用 QUIC 协议的推送源
CREATE TABLE quic_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    protocol = 'quic',  -- 指定使用 QUIC 协议
    topic = 'device_events',
    format = 'json',
    quic.max_concurrent_streams = '100'  -- QUIC 特定配置
);
```

### 配置选项

#### 通用配置选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `protocol` | 使用的协议（http、quic、grpc、websocket） | `http` |
| `topic` | 接收数据的主题 | 必填 |
| `buffer_size` | 内存缓冲区大小（字节） | `10485760` (10MB) |
| `batch_size` | 批处理大小 | `1000` |
| `compression` | 压缩算法（none、gzip、lz4、zstd） | `none` |

#### HTTP 特定配置选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `http.timeout` | 请求超时时间（秒） | `30` |
| `http.max_connections` | 最大连接数 | `100` |

#### QUIC 特定配置选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `quic.max_concurrent_streams` | 最大并发流数 | `100` |
| `quic.idle_timeout` | 空闲超时时间（秒） | `30` |

#### gRPC 特定配置选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `grpc.max_message_size` | 最大消息大小（字节） | `4194304` (4MB) |
| `grpc.keepalive_time` | 保活时间（秒） | `60` |

#### WebSocket 特定配置选项

| 选项 | 描述 | 默认值 |
|------|------|--------|
| `ws.max_frame_size` | 最大帧大小（字节） | `1048576` (1MB) |
| `ws.heartbeat_interval` | 心跳间隔（秒） | `30` |

## HTTP 协议支持

HTTP 协议是主动推送连接器支持的第一个协议，它允许外部系统通过 HTTP POST 请求将数据推送到 Arroyo。

### HTTP API 端点

HTTP API 提供以下端点：

- **`POST /api/v1/push/{topic}`**：推送数据到指定主题
- **`GET /api/v1/push/topics`**：获取可用主题列表
- **`POST /api/v1/push/topics`**：创建新主题
- **`DELETE /api/v1/push/topics/{topic}`**：删除主题
- **`GET /api/v1/push/health`**：健康检查端点

### 认证和授权

HTTP API 支持以下认证方式：

- **API 密钥**：通过 `Authorization: Bearer <api_key>` 头进行认证
- **OAuth**：支持 OAuth 2.0 认证（尚未完全实现）

### 使用示例

#### 使用 curl 推送数据

```bash
curl -X POST http://localhost:8000/api/v1/push/my-topic \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer my-api-key" \
  -d '{"id":"123","data":"example"}'
```

#### 使用 Rust SDK 推送数据

```rust
use arroyo_push_sdk::{PushClient, PushClientConfig, TransportProtocol};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建客户端
    let config = PushClientConfig {
        protocol: TransportProtocol::Http,
        base_url: "http://localhost:8000".to_string(),
        api_key: Some("my-api-key".to_string()),
        ..Default::default()
    };
    let client = PushClient::new(config)?;

    // 推送数据
    let response = client.push(
        "my-topic",
        b"{\"id\":\"123\",\"data\":\"example\"}".to_vec(),
        None
    ).await?;

    println!("Response: {:?}", response);

    Ok(())
}
```

## 内部缓冲区

内部缓冲区是主动推送连接器的核心组件之一，它负责缓存接收到的数据，实现背压机制和批处理逻辑，以提高性能和可靠性。

### 内存缓冲区

内存缓冲区使用 `MemoryBuffer` 结构体实现，它提供以下功能：

- **高效的数据存储**：使用 `VecDeque` 实现高效的先进先出队列
- **线程安全**：使用 `Mutex` 和 `RwLock` 确保线程安全
- **异步接口**：提供 `push` 和 `pop` 等异步方法
- **超时处理**：支持带超时的操作，如 `push_timeout` 和 `pop_timeout`
- **统计信息**：提供详细的缓冲区统计信息，如当前大小、总推送消息数等

### 背压机制

背压机制使用 `BackpressureController` 结构体实现，它提供以下功能：

- **空间管理**：管理缓冲区空间的分配和释放
- **多种回退策略**：支持常数、指数和线性回退策略
- **异步获取空间**：提供 `acquire` 方法异步获取缓冲区空间
- **通知机制**：当空间可用时通知等待的任务
- **利用率监控**：提供缓冲区利用率信息

### 批处理逻辑

批处理逻辑使用 `BatchProcessor` 结构体实现，它提供以下功能：

- **消息批处理**：将多个消息组合成批次进行处理
- **多主题支持**：支持按主题分组批处理
- **自动刷新**：基于大小和时间的自动刷新策略
- **手动刷新**：提供 `flush` 方法手动刷新批次
- **批处理统计**：提供批处理大小和主题分布等统计信息

### 使用示例

```rust
// 创建内存缓冲区
let buffer = MemoryBuffer::new(1024 * 1024); // 1MB 缓冲区

// 创建背压控制器
let controller = BackpressureController::new(1024 * 1024);

// 创建批处理器
let mut processor = BatchProcessor::new(1000, Duration::from_millis(100));

// 推送消息到缓冲区
let message = PushMessage {
    topic: "my-topic".to_string(),
    data: vec![1, 2, 3, 4],
    timestamp: SystemTime::now(),
};

// 获取空间并推送消息
controller.acquire(message.data.len()).await?;
buffer.push(message.clone()).await?;

// 从缓冲区获取消息
let message = buffer.pop().await?;

// 添加到批处理器
if let Some(batch) = processor.add(message) {
    // 处理批次
    for (topic, messages) in batch {
        // 处理每个主题的消息
    }
}

// 释放空间
controller.release(message.data.len());
```

## 源操作符

源操作符是主动推送连接器的核心组件，负责接收外部系统推送的数据，并将其转换为 Arroyo 内部格式进行处理。

### PushSourceFunc 结构体

`PushSourceFunc` 结构体实现了 `SourceOperator` trait，提供以下功能：

- **协议服务器管理**：启动和停止相应的协议服务器（HTTP、QUIC、gRPC、WebSocket）
- **消息接收**：接收外部系统推送的消息
- **状态管理**：维护源操作符的状态，如接收的消息数量、字节数等
- **检查点处理**：支持状态的保存和恢复
- **控制消息处理**：处理停止、检查点等控制消息

### 数据转换为 Arrow 格式

数据转换使用 `PushMessageConverter` 结构体实现，它提供以下功能：

- **Schema 转换**：将连接器 Schema 转换为 Arrow Schema
- **消息转换**：将接收到的消息转换为 Arrow RecordBatch
- **字段映射**：支持字段名称和类型的映射
- **自动添加字段**：自动添加时间戳和主题字段（如果不存在）
- **类型转换**：支持多种数据类型的转换

### 使用示例

```rust
// 创建源操作符
let source_func = PushSourceFunc::new_operator(
    config,
    table,
    operator_config,
)?;

// 运行源操作符
let finish_type = source_func.run(ctx, collector).await;
```

### 数据流程

1. 外部系统通过 HTTP、QUIC、gRPC 或 WebSocket 协议推送数据
2. 协议服务器接收数据并发送到内部通道
3. 内部任务将数据放入内存缓冲区
4. 主循环从缓冲区获取数据
5. 数据转换器将数据转换为 Arrow 格式
6. 数据被发送到下游操作符进行处理

## SQL 支持

SQL 支持是主动推送连接器的重要组成部分，使用户能够通过 SQL 语句配置和使用推送连接器。

### SQL 语法

用户可以使用标准的 SQL `CREATE TABLE` 语句创建推送连接器：

```sql
CREATE TABLE push_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    topic = 'user_events',
    protocol = 'http',
    http.timeout = '60',
    http.max_connections = '200'
);
```

### 协议选项

SQL 支持允许用户指定以下协议选项：

#### 通用选项

- `connector`：必须为 `'push'`
- `topic`：必填，指定接收数据的主题
- `protocol`：可选，指定使用的协议，支持 `'http'`、`'quic'`、`'grpc'` 和 `'websocket'`，默认为 `'http'`
- `compression`：可选，指定压缩算法，支持 `'none'`、`'gzip'`、`'lz4'` 和 `'zstd'`，默认为 `'none'`
- `batch_size`：可选，指定批处理大小，默认为 `1000`
- `buffer_size`：可选，指定缓冲区大小（字节），默认为 `10485760`（10MB）
- `max_batch_size`：可选，指定最大批处理大小，默认为 `1000`

#### HTTP 协议选项

- `http.timeout`：可选，指定请求超时时间（秒），默认为 `30`
- `http.max_connections`：可选，指定最大连接数，默认为 `100`

#### QUIC 协议选项

- `quic.max_concurrent_streams`：可选，指定最大并发流数，默认为 `100`
- `quic.idle_timeout`：可选，指定空闲超时时间（秒），默认为 `30`

#### gRPC 协议选项

- `grpc.max_message_size`：可选，指定最大消息大小（字节），默认为 `4194304`（4MB）
- `grpc.keepalive_time`：可选，指定保活时间（秒），默认为 `60`

#### WebSocket 协议选项

- `ws.max_frame_size`：可选，指定最大帧大小（字节），默认为 `1048576`（1MB）
- `ws.heartbeat_interval`：可选，指定心跳间隔（秒），默认为 `30`

### 使用示例

#### HTTP 协议示例

```sql
CREATE TABLE http_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    topic = 'user_events',
    protocol = 'http',
    http.timeout = '60',
    http.max_connections = '200'
);
```

#### QUIC 协议示例

```sql
CREATE TABLE quic_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    topic = 'device_events',
    protocol = 'quic',
    quic.max_concurrent_streams = '200',
    quic.idle_timeout = '60'
);
```

#### gRPC 协议示例

```sql
CREATE TABLE grpc_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    topic = 'api_events',
    protocol = 'grpc',
    grpc.max_message_size = '8388608',
    grpc.keepalive_time = '120'
);
```

#### WebSocket 协议示例

```sql
CREATE TABLE ws_events (
    id STRING,
    data STRING,
    timestamp TIMESTAMP
) WITH (
    connector = 'push',
    topic = 'realtime_events',
    protocol = 'websocket',
    ws.max_frame_size = '2097152',
    ws.heartbeat_interval = '15'
);
```

## 实现状态

目前已完成的功能：

- [x] 基础连接器框架
  - [x] 实现 `PushConnector` 结构体和 `Connector` trait 实现
  - [x] 定义配置结构体和表配置结构体
  - [x] 实现连接器注册机制

- [x] HTTP 协议支持
  - [x] 实现 HTTP API 服务
  - [x] 实现基本认证和授权
  - [x] 实现 HTTP 端点处理逻辑
  - [x] 实现 HTTP 客户端 SDK (Rust)

- [x] 内部缓冲区
  - [x] 实现内存缓冲区
  - [x] 实现背压机制
  - [x] 实现批处理逻辑

- [x] 源操作符
  - [x] 实现 `PushSourceFunc` 结构体
  - [x] 实现 `SourceOperator` trait
  - [x] 实现数据转换为 Arrow 格式

- [x] SQL 支持
  - [x] 实现 SQL 解析器对 `protocol` 选项的支持
  - [x] 实现协议特定配置选项的解析
  - [x] 集成到 SQL 执行计划生成

待实现的功能：

- [ ] QUIC 协议支持
- [ ] gRPC 协议支持
- [ ] WebSocket 协议支持
- [ ] 安全增强
- [ ] 监控与指标

## 开发计划

请参考 [plan2.md](../plan2.md) 文件了解详细的开发计划和进度。
