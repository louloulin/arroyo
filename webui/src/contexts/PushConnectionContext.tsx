import React, { createContext, useContext, useState, ReactNode, useEffect } from 'react';
import { PushConnectionState, PushTableConfig } from '../types/push';
import { ConnectionSchema } from '../lib/data_fetching';

// 默认表配置
const defaultTableConfig: PushTableConfig = {
  topic: '',
  protocol: 'http',
  http_config: {
    timeout: '30',
    max_connections: '100'
  }
};

// 默认状态
const defaultState: PushConnectionState = {
  name: undefined,
  connectionProfileId: null,
  table: defaultTableConfig,
  schema: null
};

// 上下文类型
type PushConnectionContextType = {
  state: PushConnectionState;
  setState: React.Dispatch<React.SetStateAction<PushConnectionState>>;
  updateName: (name: string) => void;
  updateConnectionProfileId: (id: string | null) => void;
  updateTable: (table: Partial<PushTableConfig>) => void;
  updateSchema: (schema: ConnectionSchema | null) => void;
  resetState: () => void;
};

// 创建上下文
const PushConnectionContext = createContext<PushConnectionContextType | undefined>(undefined);

// 状态提供者组件
export const PushConnectionProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  // 尝试从 localStorage 恢复状态
  const getInitialState = (): PushConnectionState => {
    try {
      const savedState = localStorage.getItem('pushConnectionState');
      if (savedState) {
        return JSON.parse(savedState);
      }
    } catch (error) {
      console.error('Failed to parse saved state:', error);
    }
    return defaultState;
  };

  const [state, setState] = useState<PushConnectionState>(getInitialState);

  // 当状态变化时保存到 localStorage
  useEffect(() => {
    try {
      localStorage.setItem('pushConnectionState', JSON.stringify(state));
    } catch (error) {
      console.error('Failed to save state:', error);
    }
  }, [state]);

  // 更新名称
  const updateName = (name: string) => {
    setState(prev => ({ ...prev, name }));
  };

  // 更新连接配置文件 ID
  const updateConnectionProfileId = (connectionProfileId: string | null) => {
    setState(prev => ({ ...prev, connectionProfileId }));
  };

  // 更新表配置
  const updateTable = (table: Partial<PushTableConfig>) => {
    setState(prev => ({
      ...prev,
      table: { ...prev.table, ...table } as PushTableConfig
    }));
  };

  // 更新 schema
  const updateSchema = (schema: ConnectionSchema | null) => {
    setState(prev => ({ ...prev, schema }));
  };

  // 重置状态
  const resetState = () => {
    setState(defaultState);
    localStorage.removeItem('pushConnectionState');
  };

  return (
    <PushConnectionContext.Provider
      value={{
        state,
        setState,
        updateName,
        updateConnectionProfileId,
        updateTable,
        updateSchema,
        resetState
      }}
    >
      {children}
    </PushConnectionContext.Provider>
  );
};

// 使用上下文的钩子
export const usePushConnection = () => {
  const context = useContext(PushConnectionContext);
  if (context === undefined) {
    throw new Error('usePushConnection must be used within a PushConnectionProvider');
  }
  return context;
};
