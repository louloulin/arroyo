# Arroyo 主动推送连接器实施指南

本文档提供了按照 plan2.md 实施 Arroyo 主动推送连接器的详细指南。

## 1. 开发流程

### 1.1 实施流程

1. **选择任务**：从 plan2.md 中选择一个待实施的任务
2. **实现功能**：编写代码实现功能
3. **编写测试**：编写单元测试和集成测试
4. **运行测试**：验证功能是否正常工作
5. **更新文档**：更新相关文档
6. **更新计划**：在 plan2.md 中标记任务为已完成
7. **提交代码**：提交代码到版本控制系统

### 1.2 代码风格和最佳实践

- 遵循 Rust 代码风格指南
- 使用有意义的变量名和函数名
- 添加适当的注释
- 处理所有可能的错误
- 编写全面的测试

## 2. 实施指南

### 2.1 基础连接器框架

#### 2.1.1 实现 `PushConnector` 结构体和 `Connector` trait 实现

1. 在 `arroyo-connectors` 项目中创建新的模块 `push`
2. 实现 `PushConnector` 结构体
3. 实现 `Connector` trait

```rust
// arroyo-connectors/src/push/mod.rs

use arroyo_rpc::api_types::connections::{Connector as ConnectorMetadata};
use arroyo_types::formats::{Format, Framing};
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub struct PushConnector {}

impl Connector for PushConnector {
    type ProfileT = PushConfig;
    type TableT = PushTable;

    fn name(&self) -> &'static str {
        "push"
    }

    fn metadata(&self) -> ConnectorMetadata {
        ConnectorMetadata {
            id: "push".to_string(),
            name: "Push".to_string(),
            icon: ICON.to_string(),
            description: "Receive data pushed from external systems".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    // 实现其他必要方法...
}
```

#### 2.1.2 定义配置结构体和表配置结构体

```rust
// arroyo-connectors/src/push/config.rs

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PushConfig {
    pub buffer_size: Option<usize>,
    pub max_batch_size: Option<usize>,
    pub authentication: Option<AuthConfig>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PushTable {
    pub topic: String,
    pub protocol: Protocol,
    pub retention_period: Option<Duration>,
    pub http_config: Option<HttpConfig>,
    pub quic_config: Option<QuicConfig>,
    pub grpc_config: Option<GrpcConfig>,
    pub websocket_config: Option<WebSocketConfig>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub enum Protocol {
    Http,
    Quic,
    Grpc,
    WebSocket,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub enum AuthConfig {
    ApiKey(String),
    OAuth(OAuthConfig),
    None,
}

// 各协议特定配置...
```

#### 2.1.3 实现连接器注册机制

```rust
// arroyo-connectors/src/lib.rs

// 添加 push 模块
pub mod push;

// 在 register_all 函数中注册 PushConnector
pub fn register_all(registry: &mut ConnectorRegistry) {
    // 其他连接器注册...
    registry.register(Box::new(push::PushConnector {}));
}
```

### 2.2 HTTP 协议支持

#### 2.2.1 实现 HTTP API 服务

```rust
// arroyo-connectors/src/push/http.rs

use axum::{
    routing::{get, post},
    Router, extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use tokio::sync::mpsc;
use std::sync::Arc;

pub struct HttpServer {
    // 服务器配置和状态
}

impl HttpServer {
    pub fn new(config: HttpConfig) -> Self {
        // 初始化服务器
    }

    pub async fn start(&self, tx: mpsc::Sender<PushMessage>) -> Result<(), Error> {
        // 启动服务器
        let app = Router::new()
            .route("/api/v1/push/:topic", post(Self::handle_push))
            .route("/api/v1/push/topics", get(Self::handle_get_topics))
            .route("/api/v1/push/topics", post(Self::handle_create_topic))
            .route("/api/v1/push/topics/:topic", delete(Self::handle_delete_topic))
            .with_state(Arc::new(ServerState { tx }));

        // 启动服务器
        axum::Server::bind(&self.addr)
            .serve(app.into_make_service())
            .await?;

        Ok(())
    }

    // 处理函数
    async fn handle_push(
        State(state): State<Arc<ServerState>>,
        Path(topic): Path<String>,
        body: Bytes,
    ) -> impl IntoResponse {
        // 处理推送请求
    }

    // 其他处理函数...
}
```

#### 2.2.2 实现基本认证和授权

```rust
// arroyo-connectors/src/push/auth.rs

use axum::{
    extract::TypedHeader,
    headers::{Authorization, HeaderValue},
    http::Request,
    middleware::{Next, self},
    response::Response,
};

pub async fn auth_middleware<B>(
    TypedHeader(auth): TypedHeader<Authorization<HeaderValue>>,
    request: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
    // 验证认证信息
    if !is_valid_auth(&auth) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // 继续处理请求
    Ok(next.run(request).await)
}

fn is_valid_auth(auth: &Authorization<HeaderValue>) -> bool {
    // 验证逻辑
}
```

#### 2.2.3 实现 HTTP 客户端 SDK (Rust)

```rust
// arroyo-push-sdk/src/http.rs

use reqwest::{Client, header};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub struct HttpPushClient {
    base_url: String,
    client: Client,
    api_key: Option<String>,
}

impl HttpPushClient {
    pub fn new(base_url: &str, api_key: Option<String>) -> Self {
        let mut headers = header::HeaderMap::new();
        if let Some(key) = &api_key {
            headers.insert(
                header::AUTHORIZATION,
                header::HeaderValue::from_str(&format!("Bearer {}", key)).unwrap(),
            );
        }

        let client = Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();

        Self {
            base_url: base_url.to_string(),
            client,
            api_key,
        }
    }

    pub async fn push(&self, topic: &str, data: Vec<u8>) -> Result<PushResponse, Error> {
        let url = format!("{}/api/v1/push/{}", self.base_url, topic);
        let response = self.client.post(&url)
            .body(data)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Error::RequestFailed(response.status().as_u16()));
        }

        let push_response = response.json::<PushResponse>().await?;
        Ok(push_response)
    }

    pub async fn push_batch(&self, topic: &str, batch: Vec<Vec<u8>>) -> Result<BatchPushResponse, Error> {
        // 批量推送实现
    }
}
```

### 2.3 内部缓冲区

#### 2.3.1 实现内存缓冲区

```rust
// arroyo-connectors/src/push/buffer.rs

use std::collections::VecDeque;
use tokio::sync::{Mutex, Notify};
use std::sync::Arc;

pub struct MemoryBuffer<T> {
    buffer: Mutex<VecDeque<T>>,
    capacity: usize,
    notify: Arc<Notify>,
}

impl<T> MemoryBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn push(&self, item: T) -> Result<(), BufferError> {
        let mut buffer = self.buffer.lock().await;
        if buffer.len() >= self.capacity {
            return Err(BufferError::Full);
        }
        buffer.push_back(item);
        self.notify.notify_one();
        Ok(())
    }

    pub async fn pop(&self) -> T {
        loop {
            let mut buffer = self.buffer.lock().await;
            if let Some(item) = buffer.pop_front() {
                return item;
            }
            drop(buffer);
            self.notify.notified().await;
        }
    }

    // 其他方法...
}
```

#### 2.3.2 实现背压机制

```rust
// arroyo-connectors/src/push/backpressure.rs

use std::time::{Duration, Instant};
use tokio::time::sleep;

pub struct BackpressureController {
    max_buffer_size: usize,
    current_buffer_size: AtomicUsize,
    backoff_strategy: BackoffStrategy,
}

impl BackpressureController {
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            max_buffer_size,
            current_buffer_size: AtomicUsize::new(0),
            backoff_strategy: BackoffStrategy::default(),
        }
    }

    pub async fn acquire(&self, size: usize) -> Result<(), BackpressureError> {
        loop {
            let current = self.current_buffer_size.load(Ordering::Relaxed);
            if current + size <= self.max_buffer_size {
                match self.current_buffer_size.compare_exchange(
                    current,
                    current + size,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Ok(()),
                    Err(_) => continue,
                }
            }

            // 应用背压
            let backoff = self.backoff_strategy.next_backoff();
            sleep(backoff).await;
        }
    }

    pub fn release(&self, size: usize) {
        self.current_buffer_size.fetch_sub(size, Ordering::Relaxed);
        self.backoff_strategy.reset();
    }
}
```

#### 2.3.3 实现批处理逻辑

```rust
// arroyo-connectors/src/push/batch.rs

use std::time::{Duration, Instant};
use tokio::time::sleep;

pub struct BatchProcessor<T> {
    max_batch_size: usize,
    max_wait_time: Duration,
    buffer: Vec<T>,
    last_flush: Instant,
}

impl<T> BatchProcessor<T> {
    pub fn new(max_batch_size: usize, max_wait_time: Duration) -> Self {
        Self {
            max_batch_size,
            max_wait_time,
            buffer: Vec::with_capacity(max_batch_size),
            last_flush: Instant::now(),
        }
    }

    pub fn add(&mut self, item: T) -> Option<Vec<T>> {
        self.buffer.push(item);
        
        if self.buffer.len() >= self.max_batch_size {
            return self.flush();
        }
        
        None
    }

    pub fn flush(&mut self) -> Option<Vec<T>> {
        if self.buffer.is_empty() {
            return None;
        }
        
        let batch = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.max_batch_size));
        self.last_flush = Instant::now();
        
        Some(batch)
    }

    pub fn should_flush(&self) -> bool {
        !self.buffer.is_empty() && self.last_flush.elapsed() >= self.max_wait_time
    }
}
```

## 3. 测试指南

### 3.1 单元测试

为每个组件编写单元测试，确保其功能正常工作。

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_connector_name() {
        let connector = PushConnector {};
        assert_eq!(connector.name(), "push");
    }

    #[test]
    fn test_push_connector_metadata() {
        let connector = PushConnector {};
        let metadata = connector.metadata();
        assert_eq!(metadata.id, "push");
        assert_eq!(metadata.name, "Push");
        assert!(metadata.source);
        assert!(!metadata.sink);
    }

    // 更多测试...
}
```

### 3.2 集成测试

编写集成测试，测试组件之间的交互。

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_http_end_to_end() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // 设置测试环境
            let (tx, rx) = mpsc::channel(100);
            let server = HttpServer::new(HttpConfig::default());
            let server_handle = tokio::spawn(async move {
                server.start(tx).await.unwrap();
            });

            // 创建客户端
            let client = HttpPushClient::new("http://localhost:8000", None);

            // 推送数据
            let data = b"{\"id\":\"123\",\"data\":\"test\"}".to_vec();
            let response = client.push("test_topic", data).await.unwrap();
            assert!(response.success);

            // 验证数据
            let message = rx.recv().await.unwrap();
            assert_eq!(message.topic, "test_topic");
            assert_eq!(message.data, b"{\"id\":\"123\",\"data\":\"test\"}");

            // 清理
            server_handle.abort();
        });
    }

    // 更多测试...
}
```

## 4. 文档指南

### 4.1 代码文档

为所有公共 API 添加文档注释。

```rust
/// 主动推送连接器，允许外部系统直接将数据推送到 Arroyo。
///
/// # Examples
///
/// ```
/// use arroyo_connectors::push::PushConnector;
/// use arroyo_connectors::Connector;
///
/// let connector = PushConnector {};
/// assert_eq!(connector.name(), "push");
/// ```
pub struct PushConnector {}
```

### 4.2 用户文档

更新 push_connector_docs.md 文件，添加详细的用户文档。

## 5. 更新计划

在完成任务后，更新 plan2.md 文件中的任务状态。

```markdown
1. **基础连接器框架** ✅
   - [x] 实现 `PushConnector` 结构体和 `Connector` trait 实现
   - [x] 定义配置结构体和表配置结构体
   - [x] 实现连接器注册机制
   - 📝 **文档状态**：已完成
   - 🧪 **测试状态**：已完成
```

## 6. 提交代码

提交代码到版本控制系统，包括实现、测试和文档。

```bash
git add .
git commit -m "Implement push connector framework"
git push
```

## 7. 下一步

按照 plan2.md 中的优先级顺序，继续实施下一个任务。
