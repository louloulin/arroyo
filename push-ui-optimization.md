# Push UI 优化方案

## 1. 问题概述

当前 Push UI 存在以下问题：

1. **Topic 配置重复**：在 "Configure profile" 和 "Configure table" 步骤中都需要配置 Topic，这可能导致数据不一致。
2. **状态管理不完善**：虽然已经实现了 `PushGlobalContext` 来共享 Topic 信息，但它只在前端生效，没有与后端正确集成。
3. **错误处理不完善**：当 API 请求失败时，错误提示不够友好和详细。
4. **用户体验不佳**：缺乏实时反馈和指导，用户可能不清楚如何正确配置和使用 Push 功能。

## 2. 优化方案

### 2.1 统一 Topic 配置

#### 2.1.1 修改 UI 流程

将 Topic 配置统一放在 "Configure table" 步骤中，从 "Configure profile" 步骤中移除 Topic 配置：

```tsx
// 在 webui/src/routes/connections/ConfigureProfile.tsx 中
// 移除 Topic 相关的表单字段和状态管理代码
```

#### 2.1.2 改进状态共享

改进 `PushGlobalContext` 的实现，确保它能够正确地在不同步骤之间共享状态：

```tsx
// 在 webui/src/contexts/PushGlobalContext.tsx 中
import React, { createContext, useContext, useState, ReactNode, useEffect } from 'react';

// 全局 Push 上下文类型
type PushGlobalContextType = {
  topic: string;
  updateTopic: (topic: string) => void;
  protocol: string;
  updateProtocol: (protocol: string) => void;
  httpConfig: {
    timeout: string;
    maxConnections: string;
  };
  updateHttpConfig: (config: { timeout: string; maxConnections: string }) => void;
  // 其他配置...
};

// 创建上下文
const PushGlobalContext = createContext<PushGlobalContextType | undefined>(undefined);

// 全局状态提供者组件
export const PushGlobalProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  // 尝试从 localStorage 恢复状态
  const getInitialState = () => {
    try {
      const savedState = localStorage.getItem('pushGlobalState');
      if (savedState) {
        return JSON.parse(savedState);
      }
    } catch (error) {
      console.error('Failed to parse saved state:', error);
    }
    return {
      topic: '',
      protocol: 'http',
      httpConfig: {
        timeout: '30',
        maxConnections: '100',
      },
    };
  };

  const [state, setState] = useState<{
    topic: string;
    protocol: string;
    httpConfig: {
      timeout: string;
      maxConnections: string;
    };
  }>(getInitialState);

  // 当状态变化时保存到 localStorage
  useEffect(() => {
    try {
      localStorage.setItem('pushGlobalState', JSON.stringify(state));
    } catch (error) {
      console.error('Failed to save state:', error);
    }
  }, [state]);

  // 更新函数
  const updateTopic = (newTopic: string) => {
    setState((prevState) => ({
      ...prevState,
      topic: newTopic,
    }));
  };

  const updateProtocol = (newProtocol: string) => {
    setState((prevState) => ({
      ...prevState,
      protocol: newProtocol,
    }));
  };

  const updateHttpConfig = (config: { timeout: string; maxConnections: string }) => {
    setState((prevState) => ({
      ...prevState,
      httpConfig: config,
    }));
  };

  return (
    <PushGlobalContext.Provider
      value={{
        topic: state.topic,
        updateTopic,
        protocol: state.protocol,
        updateProtocol,
        httpConfig: state.httpConfig,
        updateHttpConfig,
      }}
    >
      {children}
    </PushGlobalContext.Provider>
  );
};

// 使用上下文的钩子
export const useGlobalPush = () => {
  const context = useContext(PushGlobalContext);
  if (context === undefined) {
    throw new Error('useGlobalPush must be used within a PushGlobalProvider');
  }
  return context;
};
```

### 2.2 改进错误处理

#### 2.2.1 创建统一的错误处理组件

```tsx
// 在 webui/src/components/ErrorBoundary.tsx 中
import React, { Component, ErrorInfo, ReactNode } from 'react';
import {
  Alert,
  AlertIcon,
  AlertTitle,
  AlertDescription,
  Box,
  Button,
  Code,
  Collapse,
} from '@chakra-ui/react';

interface Props {
  children: ReactNode;
  fallback?: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
  errorInfo: ErrorInfo | null;
  showDetails: boolean;
}

class ErrorBoundary extends Component<Props, State> {
  public state: State = {
    hasError: false,
    error: null,
    errorInfo: null,
    showDetails: false,
  };

  public static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error, errorInfo: null, showDetails: false };
  }

  public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    this.setState({ error, errorInfo });
    console.error('Uncaught error:', error, errorInfo);
  }

  public render() {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      return (
        <Box p={4}>
          <Alert status="error" variant="solid" flexDirection="column" alignItems="start" mb={4}>
            <AlertIcon />
            <AlertTitle mt={0} mb={2}>
              Something went wrong
            </AlertTitle>
            <AlertDescription>
              {this.state.error?.message || 'An unexpected error occurred'}
            </AlertDescription>
            <Button
              size="sm"
              variant="outline"
              colorScheme="red"
              mt={2}
              onClick={() => this.setState({ showDetails: !this.state.showDetails })}
            >
              {this.state.showDetails ? 'Hide' : 'Show'} Details
            </Button>
          </Alert>

          <Collapse in={this.state.showDetails} animateOpacity>
            <Code p={4} borderRadius="md" width="100%" whiteSpace="pre-wrap">
              {this.state.error?.stack}
              {this.state.errorInfo?.componentStack}
            </Code>
          </Collapse>

          <Button
            mt={4}
            colorScheme="blue"
            onClick={() => {
              this.setState({ hasError: false, error: null, errorInfo: null });
              window.location.reload();
            }}
          >
            Reload Page
          </Button>
        </Box>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
```

#### 2.2.2 改进 API 错误处理

```tsx
// 在 webui/src/lib/data_fetching.ts 中
// 改进 API 错误处理
const handleApiError = (error: any): Error => {
  if (error instanceof Error) {
    return error;
  }

  if (typeof error === 'string') {
    return new Error(error);
  }

  if (error && typeof error === 'object') {
    if (error.message) {
      return new Error(error.message);
    }
    if (error.error) {
      return new Error(error.error);
    }
  }

  return new Error('An unknown error occurred');
};

// 使用改进的错误处理
export function usePushTopics(connectionId: string) {
  const { data, error, isLoading, mutate } = useSWR<PushTopic[]>(
    pushTopicsKey(connectionId),
    pushTopicsFetcher(),
    {
      refreshInterval: 10000,
      onError: (err) => {
        console.error('Error fetching push topics:', err);
        // 可以在这里添加全局错误处理，如显示 toast 通知
      },
    }
  );

  return {
    topics: data,
    topicsLoading: isLoading,
    topicsError: error ? handleApiError(error) : undefined,
    mutateTopics: mutate,
  };
}
```

### 2.3 改进用户体验

#### 2.3.1 添加实时验证和反馈

```tsx
// 在 webui/src/routes/connections/push/PushConnectionForm.tsx 中
// 添加实时验证和反馈
const validateTopic = (topic: string): string | null => {
  if (!topic.trim()) {
    return 'Topic name is required';
  }

  const validNameRegex = /^[a-zA-Z0-9_-]+$/;
  if (!validNameRegex.test(topic)) {
    return 'Topic name can only contain letters, numbers, underscores and hyphens';
  }

  return null;
};

// 在组件中使用
const [topicError, setTopicError] = useState<string | null>(null);

const handleTopicChange = (e: React.ChangeEvent<HTMLInputElement>) => {
  const newTopic = e.target.value;
  
  // 更新本地状态
  setState({
    ...state,
    table: {
      ...state.table,
      topic: newTopic,
    } as PushTableConfig,
  });
  
  // 同时更新全局状态
  updateTopic(newTopic);
  
  // 实时验证
  setTopicError(validateTopic(newTopic));
};

// 在 JSX 中显示错误
<FormControl isRequired isInvalid={!!topicError}>
  <FormLabel>Topic Name</FormLabel>
  <Input
    placeholder="my-topic"
    value={state.table?.topic || ''}
    onChange={handleTopicChange}
  />
  {topicError ? (
    <FormErrorMessage>{topicError}</FormErrorMessage>
  ) : (
    <FormHelperText>
      The topic name for the push connector. This will be used in the URL path.
    </FormHelperText>
  )}
</FormControl>
```

#### 2.3.2 添加引导和提示

```tsx
// 在 webui/src/routes/connections/push/PushConnectionConfig.tsx 中
// 添加引导和提示
<Alert status="info" mb={4}>
  <AlertIcon />
  <Box>
    <AlertTitle>Getting Started</AlertTitle>
    <AlertDescription>
      <Text mb={2}>
        Follow these steps to start receiving data:
      </Text>
      <OrderedList spacing={1} pl={4}>
        <ListItem>Create a topic using the "Topics" tab</ListItem>
        <ListItem>Use the provided endpoint to push data to your topic</ListItem>
        <ListItem>Check the "Examples" tab for code samples</ListItem>
      </OrderedList>
    </AlertDescription>
  </Box>
</Alert>
```

## 3. 测试计划

### 3.1 单元测试

1. 为 `PushGlobalContext` 编写单元测试
2. 测试表单验证逻辑
3. 测试错误处理组件

### 3.2 集成测试

1. 测试不同步骤之间的状态共享
2. 测试 API 错误处理
3. 测试表单提交和验证

### 3.3 用户测试

1. 进行可用性测试，收集用户反馈
2. 测试不同浏览器和设备上的兼容性
3. 测试错误情况下的用户体验

## 4. 实施时间表

1. **第 1 天**：统一 Topic 配置
2. **第 2 天**：改进错误处理
3. **第 3 天**：改进用户体验
4. **第 4 天**：测试和修复问题

## 5. 风险和缓解措施

1. **风险**：状态管理的改变可能影响现有功能
   **缓解**：编写全面的测试，确保向后兼容

2. **风险**：新的 UI 组件可能影响性能
   **缓解**：进行性能测试，优化组件渲染

3. **风险**：用户可能不适应新的 UI 流程
   **缓解**：提供清晰的文档和引导，收集用户反馈
