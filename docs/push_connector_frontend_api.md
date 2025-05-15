# Push Connector 前端 API 文档

本文档描述了 Arroyo Push Connector 的前端 API，包括主题管理和数据推送功能。

## 1. 数据类型

### 1.1 PushTopic

```typescript
interface PushTopic {
  name: string;           // 主题名称
  messages: number;       // 消息数量
  created_at: number;     // 创建时间（Unix 时间戳，秒）
  last_activity?: number; // 最后活动时间（Unix 时间戳，秒）
  retention_period: number; // 数据保留期限（秒）
  compression: boolean;   // 是否启用压缩
}
```

## 2. API 函数

### 2.1 usePushTopics

获取主题列表的 Hook。

```typescript
function usePushTopics(connectionId: string): {
  topics: PushTopic[] | undefined;
  topicsLoading: boolean;
  topicsError: Error | undefined;
  mutateTopics: () => Promise<any>;
}
```

**参数**：

- `connectionId`：连接 ID

**返回值**：

- `topics`：主题列表
- `topicsLoading`：是否正在加载
- `topicsError`：错误信息
- `mutateTopics`：刷新数据的函数

**示例**：

```typescript
import { usePushTopics } from '../lib/data_fetching';

function TopicList({ connectionId }) {
  const { topics, topicsLoading, topicsError } = usePushTopics(connectionId);
  
  if (topicsLoading) {
    return <div>Loading...</div>;
  }
  
  if (topicsError) {
    return <div>Error: {topicsError.message}</div>;
  }
  
  return (
    <ul>
      {topics?.map(topic => (
        <li key={topic.name}>{topic.name}</li>
      ))}
    </ul>
  );
}
```

### 2.2 usePushTopicDetails

获取主题详情的 Hook。

```typescript
function usePushTopicDetails(connectionId: string, topicName: string): {
  topicDetails: PushTopic | undefined;
  topicDetailsLoading: boolean;
  topicDetailsError: Error | undefined;
}
```

**参数**：

- `connectionId`：连接 ID
- `topicName`：主题名称

**返回值**：

- `topicDetails`：主题详情
- `topicDetailsLoading`：是否正在加载
- `topicDetailsError`：错误信息

**示例**：

```typescript
import { usePushTopicDetails } from '../lib/data_fetching';

function TopicDetails({ connectionId, topicName }) {
  const { topicDetails, topicDetailsLoading, topicDetailsError } = usePushTopicDetails(connectionId, topicName);
  
  if (topicDetailsLoading) {
    return <div>Loading...</div>;
  }
  
  if (topicDetailsError) {
    return <div>Error: {topicDetailsError.message}</div>;
  }
  
  return (
    <div>
      <h2>{topicDetails?.name}</h2>
      <p>Messages: {topicDetails?.messages}</p>
      <p>Created: {new Date(topicDetails?.created_at * 1000).toLocaleString()}</p>
      {topicDetails?.last_activity && (
        <p>Last Activity: {new Date(topicDetails.last_activity * 1000).toLocaleString()}</p>
      )}
      <p>Retention Period: {topicDetails?.retention_period / 86400} days</p>
      <p>Compression: {topicDetails?.compression ? 'Enabled' : 'Disabled'}</p>
    </div>
  );
}
```

### 2.3 createPushTopic

创建新主题。

```typescript
async function createPushTopic(
  connectionId: string,
  topicName: string,
  options?: {
    retention_period?: number;
    compression?: boolean;
  }
): Promise<{ success: boolean }>
```

**参数**：

- `connectionId`：连接 ID
- `topicName`：主题名称
- `options`：可选参数
  - `retention_period`：数据保留期限（秒），默认为 7 天
  - `compression`：是否启用压缩，默认为 false

**返回值**：

- `success`：是否成功

**示例**：

```typescript
import { createPushTopic } from '../lib/data_fetching';

async function handleCreateTopic() {
  try {
    await createPushTopic('connection-123', 'new-topic', {
      retention_period: 14 * 86400, // 14 天
      compression: true,
    });
    console.log('Topic created successfully');
  } catch (error) {
    console.error('Failed to create topic:', error);
  }
}
```

### 2.4 deletePushTopic

删除主题。

```typescript
async function deletePushTopic(
  connectionId: string,
  topicName: string
): Promise<{ success: boolean }>
```

**参数**：

- `connectionId`：连接 ID
- `topicName`：主题名称

**返回值**：

- `success`：是否成功

**示例**：

```typescript
import { deletePushTopic } from '../lib/data_fetching';

async function handleDeleteTopic(topicName) {
  try {
    await deletePushTopic('connection-123', topicName);
    console.log('Topic deleted successfully');
  } catch (error) {
    console.error('Failed to delete topic:', error);
  }
}
```

## 3. 错误处理

所有 API 函数都会在发生错误时抛出异常，可以使用 try/catch 捕获异常。

```typescript
try {
  await createPushTopic('connection-123', 'new-topic');
} catch (error) {
  console.error('Failed to create topic:', error);
}
```

## 4. 刷新数据

可以使用 `mutateTopics` 函数手动刷新主题列表。

```typescript
const { topics, mutateTopics } = usePushTopics('connection-123');

// 创建主题后刷新列表
await createPushTopic('connection-123', 'new-topic');
await mutateTopics();
```

## 5. 注意事项

1. 主题名称只能包含字母、数字、下划线和连字符
2. 主题名称区分大小写
3. 数据保留期限的单位为秒
4. 如果推送数据到不存在的主题，系统会自动创建该主题
