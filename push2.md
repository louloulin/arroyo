# Push Connector 集成到 API 服务改进计划

## 1. 问题背景

当前 Push Connector 实现存在以下问题：

1. **服务分离**：Push Connector 是一个独立的服务，有自己的 HTTP 服务器和 API 路由，但它没有被集成到主 API 服务中。
2. **路由不匹配**：前端代码假设 Push API 是通过 `/api/v1/push/topics` 路径访问的，但这个路径在 API 服务中并不存在。
3. **404 错误**：由于上述问题，前端请求 Push Topics API 时会收到 404 错误。

## 2. 改进目标

将 Push Connector 集成到 API 服务中，使前端可以通过统一的 API 路径访问 Push 功能，具体目标包括：

1. 将 Push Connector 的 API 路由集成到主 API 服务中
2. 确保 Push Connector 的功能在 API 服务启动时自动可用
3. 统一 API 路径和参数命名
4. 改进错误处理和文档

## 3. 实施计划

### 3.1 重构 Push Connector 模块

#### 3.1.1 修改 Push Connector 结构

将 Push Connector 从独立服务改为可集成的模块：

```rust
// 在 crates/arroyo-connectors/src/push/mod.rs 中
pub struct PushConnector {
    topic_manager: Arc<TopicManager>,
    metrics_manager: Arc<MetricsManager>,
    message_store: Arc<MessageStore>,
    // 其他字段...
}

impl PushConnector {
    pub fn new() -> Self {
        Self {
            topic_manager: Arc::new(TopicManager::new()),
            metrics_manager: Arc::new(MetricsManager::new()),
            message_store: Arc::new(MessageStore::new(1000)),
            // 初始化其他字段...
        }
    }
    
    // 添加获取内部组件的方法
    pub fn topic_manager(&self) -> Arc<TopicManager> {
        self.topic_manager.clone()
    }
    
    // 其他方法...
}
```

#### 3.1.2 提取 API 处理函数

将 HTTP 处理函数从 HTTP 服务器中提取出来，使其可以被 API 服务使用：

```rust
// 在 crates/arroyo-connectors/src/push/api.rs 中
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::push::{PushConnector, topic::{CreateTopicRequest, TopicError}};

// API 处理函数
pub async fn handle_get_topics(
    State(connector): State<Arc<PushConnector>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 获取 connectionId 参数（可选）
    let _connection_id = params.get("connectionId");

    // 获取主题列表
    match connector.topic_manager().get_topics() {
        Ok(topics) => {
            (StatusCode::OK, Json(topics))
        },
        Err(e) => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to get topics: {}", e)
                })),
            )
        }
    }
}

pub async fn handle_create_topic(
    State(connector): State<Arc<PushConnector>>,
    Json(payload): Json<CreateTopicRequest>,
) -> impl IntoResponse {
    // 创建主题
    match connector.topic_manager().create_topic(payload) {
        Ok(topic) => {
            (StatusCode::CREATED, Json(topic))
        },
        Err(e) => {
            let status = match e {
                TopicError::TopicAlreadyExists(_) => StatusCode::CONFLICT,
                TopicError::InvalidTopicName(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            (
                status,
                Json(serde_json::json!({
                    "error": format!("{}", e)
                })),
            )
        }
    }
}

// 其他 API 处理函数...
```

### 3.2 集成到 API 服务

#### 3.2.1 创建 Push 模块

在 API 服务中创建 Push 模块，用于集成 Push Connector：

```rust
// 在 crates/arroyo-api/src/push.rs 中
use axum::{
    routing::{get, post, delete},
    Router,
};
use std::sync::Arc;

use arroyo_connectors::push::{PushConnector, api};

pub fn create_push_routes() -> (Router, Arc<PushConnector>) {
    // 创建 Push Connector 实例
    let connector = Arc::new(PushConnector::new());
    
    // 创建路由
    let routes = Router::new()
        .route("/push/:topic", post(api::handle_push))
        .route("/push/topics", get(api::handle_get_topics))
        .route("/push/topics", post(api::handle_create_topic))
        .route("/push/topics/:topic", get(api::handle_get_topic_info))
        .route("/push/topics/:topic", delete(api::handle_delete_topic))
        .with_state(connector.clone());
    
    (routes, connector)
}
```

#### 3.2.2 修改 API 服务的路由定义

将 Push 路由集成到 API 服务中：

```rust
// 在 crates/arroyo-api/src/rest.rs 中
pub fn create_rest_app(database: DatabaseSource, controller_addr: &str) -> Router {
    // ...
    
    // 创建 Push 路由
    let (push_routes, _push_connector) = push::create_push_routes();
    
    let api_routes = Router::new()
        // 现有路由
        .route("/ping", get(ping))
        .route("/connectors", get(get_connectors))
        // ...
        
        // 合并 Push 路由
        .merge(push_routes)
        
        .fallback(api_fallback);
    
    // ...
}
```

#### 3.2.3 修改 API 服务的启动代码

确保 API 服务启动时 Push Connector 也被正确初始化：

```rust
// 在 crates/arroyo-api/src/main.rs 中
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ...
    
    // 创建 API 服务
    let app = create_rest_app(database, &controller_addr);
    
    // 启动 API 服务
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;
    
    Ok(())
}
```

### 3.3 前端适配

#### 3.3.1 确认 API 路径

确认前端使用的 API 路径与后端一致：

```typescript
// 在 webui/src/lib/data_fetching.ts 中
const pushTopicsFetcher = () => {
  return async (params: { key: string; connectionId: string }) => {
    try {
      // 确保路径与后端一致
      const response = await fetch(`/api/v1/push/topics?connectionId=${params.connectionId}`);
      // ...
    } catch (err) {
      // ...
    }
  };
};
```

#### 3.3.2 添加错误处理

改进前端错误处理，提供更友好的错误消息：

```typescript
// 在 webui/src/lib/data_fetching.ts 中
const pushTopicsFetcher = () => {
  return async (params: { key: string; connectionId: string }) => {
    try {
      const response = await fetch(`/api/v1/push/topics?connectionId=${params.connectionId}`);

      if (!response.ok) {
        // 尝试解析错误消息
        let errorMessage = `Failed to fetch push topics: ${response.statusText}`;
        try {
          const errorData = await response.json();
          if (errorData.error) {
            errorMessage = errorData.error;
          }
        } catch (e) {
          // 忽略解析错误
        }
        throw new Error(errorMessage);
      }

      const data = await response.json();
      return data as PushTopic[];
    } catch (err) {
      console.error('Failed to fetch push topics:', err);
      throw err;
    }
  };
};
```

## 4. 测试计划

### 4.1 单元测试

1. 为 Push Connector 的 API 处理函数编写单元测试
2. 测试 Topic 管理功能（创建、删除、查询）
3. 测试错误处理

### 4.2 集成测试

1. 测试 Push API 路由是否正确注册
2. 测试 API 服务是否能正确处理 Push API 请求
3. 测试前端是否能正确调用 Push API

### 4.3 端到端测试

1. 启动完整的系统（API 服务和前端）
2. 测试创建、查询和删除 Topic 的完整流程
3. 测试错误情况下的用户体验

## 5. 文档计划

### 5.1 API 文档

1. 编写详细的 Push API 文档，包括每个端点的路径、参数和返回值
2. 记录错误码和错误消息

### 5.2 架构文档

1. 记录 Push Connector 的架构和设计
2. 说明 Push Connector 如何集成到 API 服务中

### 5.3 用户文档

1. 编写 Push 功能的用户指南
2. 提供使用示例和最佳实践

## 6. 实施时间表

1. **第 1 周**：重构 Push Connector 模块
2. **第 2 周**：集成到 API 服务
3. **第 3 周**：前端适配和测试
4. **第 4 周**：文档编写和最终测试

## 7. 风险和缓解措施

1. **风险**：集成过程中可能引入新的 bug
   **缓解**：编写全面的测试，采用渐进式集成

2. **风险**：API 路径变更可能影响现有用户
   **缓解**：保持向后兼容，或提供明确的迁移指南

3. **风险**：性能可能受到影响
   **缓解**：进行性能测试，优化关键路径

## 8. 结论

通过将 Push Connector 集成到 API 服务中，我们可以解决当前的 404 错误问题，并提供更统一的 API 体验。这个改进计划涵盖了代码重构、集成、测试和文档等方面，确保 Push 功能能够可靠地工作。
