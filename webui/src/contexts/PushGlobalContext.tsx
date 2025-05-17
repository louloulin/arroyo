import React, { createContext, useContext, useState, ReactNode, useEffect } from 'react';
import { Protocol } from '../types/push';

// HTTP 配置类型
type HttpConfig = {
  timeout: string;
  maxConnections: string;
};

// gRPC 配置类型
type GrpcConfig = {
  maxMessageSize: string;
  keepaliveTime: string;
};

// WebSocket 配置类型
type WebsocketConfig = {
  maxFrameSize: string;
  heartbeatInterval: string;
};

// QUIC 配置类型
type QuicConfig = {
  maxConcurrentStreams: string;
  idleTimeout: string;
};

// 全局状态类型
type PushGlobalState = {
  topic: string;
  protocol: Protocol;
  httpConfig: HttpConfig;
  grpcConfig: GrpcConfig;
  websocketConfig: WebsocketConfig;
  quicConfig: QuicConfig;
};

// 全局 Push 上下文类型
type PushGlobalContextType = {
  topic: string;
  updateTopic: (topic: string) => void;
  protocol: Protocol;
  updateProtocol: (protocol: Protocol) => void;
  httpConfig: HttpConfig;
  updateHttpConfig: (config: HttpConfig) => void;
  grpcConfig: GrpcConfig;
  updateGrpcConfig: (config: GrpcConfig) => void;
  websocketConfig: WebsocketConfig;
  updateWebsocketConfig: (config: WebsocketConfig) => void;
  quicConfig: QuicConfig;
  updateQuicConfig: (config: QuicConfig) => void;
};

// 创建上下文
const PushGlobalContext = createContext<PushGlobalContextType | undefined>(undefined);

// 全局状态提供者组件
export const PushGlobalProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  // 尝试从 localStorage 恢复状态
  const getInitialState = (): PushGlobalState => {
    try {
      const savedState = localStorage.getItem('pushGlobalState');
      if (savedState) {
        return JSON.parse(savedState) as PushGlobalState;
      }
    } catch (error) {
      console.error('Failed to parse saved state:', error);
    }
    return {
      topic: '',
      protocol: 'http' as Protocol,
      httpConfig: {
        timeout: '30',
        maxConnections: '100',
      },
      grpcConfig: {
        maxMessageSize: '5',
        keepaliveTime: '60',
      },
      websocketConfig: {
        maxFrameSize: '1',
        heartbeatInterval: '30',
      },
      quicConfig: {
        maxConcurrentStreams: '100',
        idleTimeout: '30',
      },
    };
  };

  const [state, setState] = useState<PushGlobalState>(getInitialState());

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
    setState((prevState: PushGlobalState) => ({
      ...prevState,
      topic: newTopic,
    }));
  };

  const updateProtocol = (newProtocol: Protocol) => {
    setState((prevState: PushGlobalState) => ({
      ...prevState,
      protocol: newProtocol,
    }));
  };

  const updateHttpConfig = (config: HttpConfig) => {
    setState((prevState: PushGlobalState) => ({
      ...prevState,
      httpConfig: config,
    }));
  };

  const updateGrpcConfig = (config: GrpcConfig) => {
    setState((prevState: PushGlobalState) => ({
      ...prevState,
      grpcConfig: config,
    }));
  };

  const updateWebsocketConfig = (config: WebsocketConfig) => {
    setState((prevState: PushGlobalState) => ({
      ...prevState,
      websocketConfig: config,
    }));
  };

  const updateQuicConfig = (config: QuicConfig) => {
    setState((prevState: PushGlobalState) => ({
      ...prevState,
      quicConfig: config,
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
        grpcConfig: state.grpcConfig,
        updateGrpcConfig,
        websocketConfig: state.websocketConfig,
        updateWebsocketConfig,
        quicConfig: state.quicConfig,
        updateQuicConfig,
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
