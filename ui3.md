# Arroyo Push Connector Web UI 实现计划

## 1. 概述

本文档详细描述了 Arroyo Push Connector 的 Web UI 实现计划。Push Connector 允许外部系统直接将数据推送到 Arroyo 流处理系统，而不是由 Arroyo 主动从外部系统拉取数据。Web UI 将提供直观的界面来创建、配置和管理 Push Connector 连接和主题。

## 2. 当前状态分析

### 2.1 后端实现状态

Push Connector 的后端已经实现了基本框架，包括：

- `PushConnector` 结构体实现了 `Connector` trait
- 定义了 `PushConfig` 和 `PushTable` 配置结构体
- 实现了 `PushSourceFunc` 源操作符
- 支持多种协议（HTTP、QUIC、gRPC、WebSocket）
- 提供了基本的 HTTP API 服务

然而，主题管理 API 目前只有占位符实现：

```rust
/// Handle get topics request
async fn handle_get_topics(
    State(_state): State<Arc<HttpServerState>>,
) -> impl IntoResponse {
    // TODO: Implement get topics functionality
    // ...
}

/// Handle create topic request
async fn handle_create_topic(
    State(_state): State<Arc<HttpServerState>>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    // TODO: Implement create topic functionality
    // ...
}

/// Handle delete topic request
async fn handle_delete_topic(
    State(_state): State<Arc<HttpServerState>>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    // TODO: Implement delete topic functionality
    // ...
}
```

### 2.2 前端实现状态

目前，Web UI 已经有了基本的连接管理功能，但缺少 Push Connector 特定的界面和功能。

## 3. 实现目标

1. 创建 Push Connector 的专用界面
2. 实现主题管理功能
3. 提供连接配置和监控界面
4. 添加示例代码和文档

## 4. 实现计划

### 4.1 组件结构

```
webui/src/routes/connections/push/
├── PushConnectorIcon.tsx         # Push Connector 图标
├── PushConnectionForm.tsx        # 连接配置表单
├── PushConnectionDetails.tsx     # 连接详情页面
├── PushConnectionConfig.tsx      # 连接配置组件
├── PushTopicManager.tsx          # 主题管理组件
├── TopicList.tsx                 # 主题列表组件
├── CreateTopic.tsx               # 创建主题组件
├── TopicDetails.tsx              # 主题详情组件
└── PushConnectorDocs.tsx         # 文档组件
```

### 4.2 API 集成

由于后端 API 尚未完全实现，我们将在前端使用模拟数据进行开发，并在 `data_fetching.ts` 中添加以下函数：

```typescript
// Push Connector API 函数
export const usePushTopics = (connectionId: string) => {
  // 模拟数据
  const topics = [
    {
      name: 'events',
      messages: 1245,
      created_at: Date.now() / 1000 - 86400 * 3,
      last_activity: Date.now() / 1000 - 3600,
      retention_period: 7 * 86400,
      compression: true,
    },
    // ...
  ];
  
  return {
    topics,
    topicsLoading: false,
    topicsError: null,
    mutateTopics: () => {},
  };
};

export const usePushTopicDetails = (connectionId: string, topicName: string) => {
  // 模拟数据
  const topicDetails = {
    name: topicName,
    messages: 1245,
    created_at: Date.now() / 1000 - 86400 * 3,
    last_activity: Date.now() / 1000 - 3600,
    retention_period: 7 * 86400,
    compression: true,
  };
  
  return {
    topicDetails,
    topicDetailsLoading: false,
    topicDetailsError: null,
  };
};

export const createPushTopic = async (
  connectionId: string,
  topicName: string,
  options?: {
    retention_period?: number;
    compression?: boolean;
  }
) => {
  // 模拟 API 调用
  await new Promise(resolve => setTimeout(resolve, 1000));
  return { success: true };
};

export const deletePushTopic = async (connectionId: string, topicName: string) => {
  // 模拟 API 调用
  await new Promise(resolve => setTimeout(resolve, 1000));
  return { success: true };
};
```

### 4.3 路由配置

在 `router.tsx` 中添加 Push Connector 的路由：

```typescript
{
  path: 'connections/push/:connectionId',
  element: <PushConnectionDetails />,
},
```

### 4.4 连接列表修改

在 `Connections.tsx` 中添加 Push Connector 的特殊处理：

```typescript
{table.connector === 'push' ? (
  <Button
    size="sm"
    variant="outline"
    colorScheme="blue"
    onClick={() => navigate(`/connections/push/${table.id}`)}
    mr={2}
  >
    管理主题
  </Button>
) : null}
```

### 4.5 连接配置表单

修改 `ConfigureConnection.tsx` 以使用自定义表单：

```typescript
// 如果是 Push Connector，使用自定义表单
if (connector.id === 'push') {
  return (
    <PushConnectionForm
      connector={connector}
      state={state}
      setState={setState}
      onSubmit={onSubmit}
    />
  );
}
```

## 5. 用户界面设计

### 5.1 连接详情页面

连接详情页面将包含以下标签页：

1. **主题** - 显示主题列表，允许创建和删除主题
2. **主题详情** - 显示选定主题的详细信息
3. **配置** - 显示连接配置和示例代码
4. **文档** - 显示 Push Connector 的文档

### 5.2 主题管理界面

主题管理界面将包含：

1. 主题列表表格，显示主题名称、消息数量、创建时间等信息
2. 创建主题按钮和对话框
3. 删除主题按钮和确认对话框
4. 查看主题详情按钮

### 5.3 主题详情界面

主题详情界面将显示：

1. 主题基本信息（名称、创建时间、消息数量等）
2. 推送端点 URL 和复制按钮
3. 主题配置信息（保留期限、压缩设置等）
4. 监控指标（如果可用）

### 5.4 连接配置界面

连接配置界面将显示：

1. 连接基本信息
2. 推送端点 URL 和复制按钮
3. 示例代码（cURL、Python、JavaScript 等）
4. 协议特定配置

## 6. 后端 API 需求

为了完全支持 Push Connector 的 Web UI 功能，后端需要实现以下 API：

1. **主题管理 API**：
   - `GET /api/v1/push/topics` - 获取主题列表
   - `POST /api/v1/push/topics` - 创建新主题
   - `DELETE /api/v1/push/topics/{topic}` - 删除主题

2. **主题详情 API**：
   - `GET /api/v1/push/topics/{topic}` - 获取主题详情

这些 API 需要在 `crates/arroyo-api/src/rest.rs` 中添加路由，并在 `crates/arroyo-connectors/src/push/http.rs` 中实现处理函数。

## 7. 实现步骤

1. 创建基本组件结构
2. 实现模拟 API 函数
3. 实现连接配置表单
4. 实现主题管理界面
5. 实现主题详情界面
6. 实现连接配置界面
7. 添加路由配置
8. 修改连接列表页面
9. 添加示例代码和文档
10. 测试和优化

## 8. 后续工作

1. 实现后端 API
2. 将模拟 API 函数替换为实际 API 调用
3. 添加更多功能（监控、消息浏览等）
4. 改进用户体验

## 9. 结论

Push Connector 的 Web UI 实现将为用户提供直观的界面来创建、配置和管理 Push Connector 连接和主题。通过模拟数据进行前端开发，我们可以在后端 API 完成之前提供基本功能，并在后端 API 完成后无缝集成。
