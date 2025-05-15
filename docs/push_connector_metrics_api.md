# Push Connector 监控 API 文档

本文档描述了 Arroyo Push Connector 的监控 API，用于获取主题的实时监控数据。

## 1. 监控数据结构

### 1.1 TopicMetrics

```json
{
  "name": "events",
  "total_messages": 1245,
  "messages_per_second": 12.5,
  "avg_message_size": 256.7,
  "total_bytes": 319587,
  "bytes_per_second": 3208.5,
  "last_update": 1620100000
}
```

**字段说明**：

- `name`：主题名称
- `total_messages`：总消息数
- `messages_per_second`：每秒消息数（基于最近一分钟的数据计算）
- `avg_message_size`：平均消息大小（字节，基于最近一分钟的数据计算）
- `total_bytes`：总字节数
- `bytes_per_second`：每秒字节数（基于最近一分钟的数据计算）
- `last_update`：最后更新时间（Unix 时间戳，秒）

## 2. 监控 API

### 2.1 获取所有主题的监控数据

获取所有主题的监控数据。

**请求**：

```
GET /api/v1/push/metrics
```

**响应**：

```json
[
  {
    "name": "events",
    "total_messages": 1245,
    "messages_per_second": 12.5,
    "avg_message_size": 256.7,
    "total_bytes": 319587,
    "bytes_per_second": 3208.5,
    "last_update": 1620100000
  },
  {
    "name": "logs",
    "total_messages": 5678,
    "messages_per_second": 8.3,
    "avg_message_size": 512.0,
    "total_bytes": 2907136,
    "bytes_per_second": 4249.6,
    "last_update": 1620100000
  }
]
```

### 2.2 获取特定主题的监控数据

获取特定主题的监控数据。

**请求**：

```
GET /api/v1/push/metrics/{topic}
```

**参数**：

- `topic`（路径参数）：主题名称

**响应**：

```json
{
  "name": "events",
  "total_messages": 1245,
  "messages_per_second": 12.5,
  "avg_message_size": 256.7,
  "total_bytes": 319587,
  "bytes_per_second": 3208.5,
  "last_update": 1620100000
}
```

## 3. 错误处理

API 使用标准的 HTTP 状态码来表示请求的结果：

- `200 OK`：请求成功
- `404 Not Found`：请求的主题不存在
- `500 Internal Server Error`：服务器内部错误

错误响应的格式如下：

```json
{
  "error": "错误信息"
}
```

## 4. 示例

### 4.1 获取所有主题的监控数据

```bash
curl -X GET http://localhost:8000/api/v1/push/metrics
```

### 4.2 获取特定主题的监控数据

```bash
curl -X GET http://localhost:8000/api/v1/push/metrics/events
```

## 5. 前端集成

### 5.1 获取所有主题的监控数据

```typescript
async function fetchAllMetrics() {
  const response = await fetch('/api/v1/push/metrics');
  
  if (!response.ok) {
    throw new Error(`Failed to fetch metrics: ${response.statusText}`);
  }
  
  return await response.json();
}
```

### 5.2 获取特定主题的监控数据

```typescript
async function fetchTopicMetrics(topicName: string) {
  const response = await fetch(`/api/v1/push/metrics/${topicName}`);
  
  if (!response.ok) {
    throw new Error(`Failed to fetch topic metrics: ${response.statusText}`);
  }
  
  return await response.json();
}
```

### 5.3 使用 React Hook 获取监控数据

```typescript
import { useState, useEffect } from 'react';

function useTopicMetrics(topicName: string, refreshInterval = 5000) {
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  useEffect(() => {
    let isMounted = true;
    
    const fetchMetrics = async () => {
      try {
        setLoading(true);
        const response = await fetch(`/api/v1/push/metrics/${topicName}`);
        
        if (!response.ok) {
          throw new Error(`Failed to fetch topic metrics: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        if (isMounted) {
          setMetrics(data);
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
    
    fetchMetrics();
    
    const intervalId = setInterval(fetchMetrics, refreshInterval);
    
    return () => {
      isMounted = false;
      clearInterval(intervalId);
    };
  }, [topicName, refreshInterval]);
  
  return { metrics, loading, error };
}
```

## 6. 注意事项

1. 监控数据是实时计算的，可能会有轻微的延迟
2. 消息速率和字节速率是基于最近一分钟的数据计算的
3. 如果主题没有收到任何消息，监控数据中的速率字段将为 0
4. 监控数据不会持久化，重启服务器后将重置
