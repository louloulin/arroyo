import React, { createContext, useContext, useState, ReactNode } from 'react';
import {
  Alert,
  AlertIcon,
  AlertTitle,
  AlertDescription,
  CloseButton,
  Box,
  VStack,
} from '@chakra-ui/react';

// 错误类型
export type ErrorType = 'validation' | 'network' | 'server' | 'unknown';

// 错误对象
export type ErrorObject = {
  type: ErrorType;
  title: string;
  message: string;
  details?: string[];
};

// 错误上下文类型
type ErrorContextType = {
  error: ErrorObject | null;
  setError: (error: ErrorObject | null) => void;
  clearError: () => void;
  addValidationError: (message: string, details?: string[]) => void;
  addNetworkError: (message: string, details?: string[]) => void;
  addServerError: (message: string, details?: string[]) => void;
};

// 创建错误上下文
const ErrorContext = createContext<ErrorContextType | undefined>(undefined);

// 错误提供者组件
export const ErrorProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [error, setError] = useState<ErrorObject | null>(null);

  const clearError = () => setError(null);

  const addValidationError = (message: string, details?: string[]) => {
    setError({
      type: 'validation',
      title: 'Validation Error',
      message,
      details,
    });
  };

  const addNetworkError = (message: string, details?: string[]) => {
    setError({
      type: 'network',
      title: 'Network Error',
      message,
      details,
    });
  };

  const addServerError = (message: string, details?: string[]) => {
    setError({
      type: 'server',
      title: 'Server Error',
      message,
      details,
    });
  };

  return (
    <ErrorContext.Provider
      value={{
        error,
        setError,
        clearError,
        addValidationError,
        addNetworkError,
        addServerError,
      }}
    >
      {children}
    </ErrorContext.Provider>
  );
};

// 使用错误上下文的钩子
export const useError = () => {
  const context = useContext(ErrorContext);
  if (context === undefined) {
    throw new Error('useError must be used within an ErrorProvider');
  }
  return context;
};

// 错误显示组件
export const ErrorDisplay: React.FC = () => {
  const { error, clearError } = useError();

  if (!error) return null;

  return (
    <Box my={4}>
      <Alert status={error.type === 'validation' ? 'warning' : 'error'} variant="solid" borderRadius="md">
        <AlertIcon />
        <Box flex="1">
          <AlertTitle>{error.title}</AlertTitle>
          <AlertDescription display="block">
            {error.message}
            {error.details && error.details.length > 0 && (
              <VStack align="start" mt={2} spacing={1}>
                {error.details.map((detail, index) => (
                  <Box key={index} fontSize="sm">
                    • {detail}
                  </Box>
                ))}
              </VStack>
            )}
          </AlertDescription>
        </Box>
        <CloseButton position="absolute" right="8px" top="8px" onClick={clearError} />
      </Alert>
    </Box>
  );
};
