import React, { createContext, useContext, useState, ReactNode, useEffect } from 'react';

// 全局 Push 上下文类型
type PushGlobalContextType = {
  topic: string;
  updateTopic: (topic: string) => void;
  // 添加其他可能需要在步骤之间共享的字段
};

// 创建上下文
const PushGlobalContext = createContext<PushGlobalContextType | undefined>(undefined);

// 全局状态提供者组件
export const PushGlobalProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  // 尝试从 localStorage 恢复状态
  const getInitialTopic = (): string => {
    try {
      const savedTopic = localStorage.getItem('pushGlobalTopic');
      if (savedTopic) {
        return savedTopic;
      }
    } catch (error) {
      console.error('Failed to parse saved topic:', error);
    }
    return '';
  };

  const [topic, setTopic] = useState<string>(getInitialTopic);

  // 当 topic 变化时保存到 localStorage
  useEffect(() => {
    try {
      localStorage.setItem('pushGlobalTopic', topic);
    } catch (error) {
      console.error('Failed to save topic:', error);
    }
  }, [topic]);

  // 更新 topic
  const updateTopic = (newTopic: string) => {
    setTopic(newTopic);
  };

  return (
    <PushGlobalContext.Provider value={{ topic, updateTopic }}>
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
