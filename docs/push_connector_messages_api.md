# Push Connector 消息浏览 API 文档

本文档描述了 Arroyo Push Connector 的消息浏览 API，用于查询和浏览主题中的消息。

## 1. 消息数据结构

### 1.1 MessageData

```json
{
  "id": 1234,
  "topic": "events",
  "content": "SGVsbG8sIHdvcmxkIQ==",
  "timestamp": 1620100000,
  "size": 13
}
```

**字段说明**：

- `id`：消息 ID（唯一标识符）
- `topic`：主题名称
- `content`：消息内容（Base64 编码的字节数组）
- `timestamp`：消息时间戳（Unix 时间戳，秒）
- `size`：消息大小（字节）

## 2. 消息浏览 API

### 2.1 查询消息

查询主题中的消息，支持分页和时间范围过滤。

**请求**：

```
GET /api/v1/push/messages/{topic}?limit=100&offset=0&start_time=1620000000&end_time=1620100000
```

**参数**：

- `topic`（路径参数）：主题名称
- `limit`（查询参数，可选）：返回的最大消息数，默认为 100
- `offset`（查询参数，可选）：分页偏移量，默认为 0
- `start_time`（查询参数，可选）：开始时间戳（Unix 时间戳，秒），包含该时间
- `end_time`（查询参数，可选）：结束时间戳（Unix 时间戳，秒），包含该时间

**响应**：

```json
[
  {
    "id": 1234,
    "topic": "events",
    "content": "SGVsbG8sIHdvcmxkIQ==",
    "timestamp": 1620050000,
    "size": 13
  },
  {
    "id": 1235,
    "topic": "events",
    "content": "SGVsbG8sIEFycm95byE=",
    "timestamp": 1620060000,
    "size": 14
  }
]
```

### 2.2 获取单个消息

获取主题中的单个消息。

**请求**：

```
GET /api/v1/push/messages/{topic}/{id}
```

**参数**：

- `topic`（路径参数）：主题名称
- `id`（路径参数）：消息 ID

**响应**：

```json
{
  "id": 1234,
  "topic": "events",
  "content": "SGVsbG8sIHdvcmxkIQ==",
  "timestamp": 1620050000,
  "size": 13
}
```

### 2.3 清除消息

清除主题中的所有消息。

**请求**：

```
DELETE /api/v1/push/messages/{topic}
```

**参数**：

- `topic`（路径参数）：主题名称

**响应**：

```json
{
  "success": true,
  "message": "Messages cleared for topic: events"
}
```

## 3. 错误处理

API 使用标准的 HTTP 状态码来表示请求的结果：

- `200 OK`：请求成功
- `400 Bad Request`：请求参数无效
- `404 Not Found`：请求的主题或消息不存在
- `500 Internal Server Error`：服务器内部错误

错误响应的格式如下：

```json
{
  "error": "错误信息"
}
```

## 4. 示例

### 4.1 查询消息

```bash
curl -X GET "http://localhost:8000/api/v1/push/messages/events?limit=10&offset=0"
```

### 4.2 获取单个消息

```bash
curl -X GET http://localhost:8000/api/v1/push/messages/events/1234
```

### 4.3 清除消息

```bash
curl -X DELETE http://localhost:8000/api/v1/push/messages/events
```

## 5. 前端集成

### 5.1 查询消息

```typescript
async function fetchMessages(
  topicName: string,
  limit = 100,
  offset = 0,
  startTime?: number,
  endTime?: number
) {
  // 构建查询参数
  const params = new URLSearchParams();
  params.append('limit', limit.toString());
  params.append('offset', offset.toString());
  if (startTime) params.append('start_time', startTime.toString());
  if (endTime) params.append('end_time', endTime.toString());
  
  const response = await fetch(`/api/v1/push/messages/${topicName}?${params.toString()}`);
  
  if (!response.ok) {
    throw new Error(`Failed to fetch messages: ${response.statusText}`);
  }
  
  return await response.json();
}
```

### 5.2 获取单个消息

```typescript
async function fetchMessage(topicName: string, messageId: number) {
  const response = await fetch(`/api/v1/push/messages/${topicName}/${messageId}`);
  
  if (!response.ok) {
    throw new Error(`Failed to fetch message: ${response.statusText}`);
  }
  
  return await response.json();
}
```

### 5.3 清除消息

```typescript
async function clearMessages(topicName: string) {
  const response = await fetch(`/api/v1/push/messages/${topicName}`, {
    method: 'DELETE',
  });
  
  if (!response.ok) {
    throw new Error(`Failed to clear messages: ${response.statusText}`);
  }
  
  return await response.json();
}
```

### 5.4 使用 React Hook 获取消息

```typescript
import { useState, useEffect } from 'react';

function useTopicMessages(
  topicName: string,
  limit = 100,
  offset = 0,
  startTime?: number,
  endTime?: number,
  refreshInterval = 5000
) {
  const [messages, setMessages] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  useEffect(() => {
    let isMounted = true;
    
    const fetchMessages = async () => {
      try {
        setLoading(true);
        
        // 构建查询参数
        const params = new URLSearchParams();
        params.append('limit', limit.toString());
        params.append('offset', offset.toString());
        if (startTime) params.append('start_time', startTime.toString());
        if (endTime) params.append('end_time', endTime.toString());
        
        const response = await fetch(`/api/v1/push/messages/${topicName}?${params.toString()}`);
        
        if (!response.ok) {
          throw new Error(`Failed to fetch messages: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        if (isMounted) {
          setMessages(data);
          setError(null);
        }
      } catch (err) {
        if (isMounted) {
          setError(err.message);
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };
    
    fetchMessages();
    
    const intervalId = setInterval(fetchMessages, refreshInterval);
    
    return () => {
      isMounted = false;
      clearInterval(intervalId);
    };
  }, [topicName, limit, offset, startTime, endTime, refreshInterval]);
  
  return { messages, loading, error };
}
```

## 6. 注意事项

1. 消息内容以 Base64 编码的字节数组形式返回，需要解码后使用
2. 消息存储在内存中，重启服务器后将丢失
3. 每个主题默认最多存储 1000 条消息，超过限制后将删除最旧的消息
4. 消息 ID 是全局唯一的，按照消息接收顺序递增
