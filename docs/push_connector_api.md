# Push Connector API 文档

本文档描述了 Arroyo Push Connector 的 API，包括主题管理和数据推送功能。

## 1. 主题管理 API

### 1.1 获取主题列表

获取所有可用的主题。

**请求**：

```
GET /api/v1/push/topics
```

**参数**：

- `connectionId`（可选）：连接 ID，用于筛选特定连接的主题

**响应**：

```json
[
  {
    "name": "events",
    "messages": 1245,
    "created_at": 1620000000,
    "last_activity": 1620100000,
    "retention_period": 604800,
    "compression": true
  },
  {
    "name": "logs",
    "messages": 5678,
    "created_at": 1620000000,
    "last_activity": 1620100000,
    "retention_period": 1209600,
    "compression": false
  }
]
```

### 1.2 创建主题

创建一个新的主题。

**请求**：

```
POST /api/v1/push/topics
```

**请求体**：

```json
{
  "name": "events",
  "retention_period": 604800,
  "compression": true
}
```

**参数**：

- `name`（必填）：主题名称，只能包含字母、数字、下划线和连字符
- `retention_period`（可选）：数据保留期限，单位为秒，默认为 7 天（604800 秒）
- `compression`（可选）：是否启用压缩，默认为 false

**响应**：

```json
{
  "name": "events",
  "messages": 0,
  "created_at": 1620000000,
  "last_activity": null,
  "retention_period": 604800,
  "compression": true
}
```

### 1.3 获取主题详情

获取特定主题的详细信息。

**请求**：

```
GET /api/v1/push/topics/{topic}
```

**参数**：

- `topic`（路径参数）：主题名称

**响应**：

```json
{
  "name": "events",
  "messages": 1245,
  "created_at": 1620000000,
  "last_activity": 1620100000,
  "retention_period": 604800,
  "compression": true
}
```

### 1.4 删除主题

删除特定的主题。

**请求**：

```
DELETE /api/v1/push/topics/{topic}
```

**参数**：

- `topic`（路径参数）：主题名称

**响应**：

```json
{
  "success": true,
  "message": "Topic events deleted"
}
```

## 2. 数据推送 API

### 2.1 推送数据

向特定主题推送数据。

**请求**：

```
POST /api/v1/push/{topic}
```

**参数**：

- `topic`（路径参数）：主题名称

**请求体**：

请求体可以是任何格式的数据，取决于您的应用程序需求。例如，JSON 格式的数据：

```json
{
  "id": "123",
  "event": "user_login",
  "timestamp": 1620000000,
  "data": {
    "user_id": "user123",
    "ip": "192.168.1.1"
  }
}
```

**响应**：

```json
{
  "success": true,
  "message": "Message received",
  "timestamp": 1620000000
}
```

## 3. 错误处理

API 使用标准的 HTTP 状态码来表示请求的结果：

- `200 OK`：请求成功
- `201 Created`：资源创建成功
- `400 Bad Request`：请求参数无效
- `404 Not Found`：请求的资源不存在
- `409 Conflict`：资源已存在
- `500 Internal Server Error`：服务器内部错误

错误响应的格式如下：

```json
{
  "error": "错误信息"
}
```

## 4. 示例

### 4.1 创建主题并推送数据

```bash
# 创建主题
curl -X POST http://localhost:8000/api/v1/push/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "events", "retention_period": 604800, "compression": true}'

# 推送数据
curl -X POST http://localhost:8000/api/v1/push/events \
  -H "Content-Type: application/json" \
  -d '{"id": "123", "event": "user_login", "timestamp": 1620000000, "data": {"user_id": "user123", "ip": "192.168.1.1"}}'
```

### 4.2 获取主题列表和详情

```bash
# 获取主题列表
curl -X GET http://localhost:8000/api/v1/push/topics

# 获取主题详情
curl -X GET http://localhost:8000/api/v1/push/topics/events
```

### 4.3 删除主题

```bash
# 删除主题
curl -X DELETE http://localhost:8000/api/v1/push/topics/events
```

## 5. 注意事项

1. 主题名称只能包含字母、数字、下划线和连字符
2. 主题名称区分大小写
3. 数据保留期限的单位为秒
4. 如果推送数据到不存在的主题，系统会自动创建该主题
5. 推送的数据大小没有严格限制，但建议不要超过 10MB
