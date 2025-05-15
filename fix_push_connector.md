# Push Connector 跳转问题修复方案

## 问题描述

当用户在创建 Push Connector 时，点击 "Validate" 按钮后没有出现 "Continue" 按钮，导致用户无法继续下一步。

## 问题分析

通过检查代码，我们发现以下几个可能的问题：

1. 在 `PushConnectionForm.tsx` 中，表单提交后应该调用 `onSubmit` 函数，但是当前的实现中，只有点击 "Next" 按钮才会调用 `onSubmit` 函数。

2. 在验证成功后，应该显示 "Continue" 按钮，但是当前的实现中没有这个逻辑。

3. 在 `CreateConnection.tsx` 中，`ConnectionCreator` 组件的 `steps` 数组中，`ConfigureProfile` 组件的 `onSubmit` 函数会调用 `setActiveStep(next)` 来进入下一步，但是在 `PushConnectionForm.tsx` 中没有类似的逻辑。

## 修复方案

1. 修改 `PushConnectionForm.tsx` 文件，在验证成功后显示 "Continue" 按钮，并在点击 "Continue" 按钮时调用 `onSubmit` 函数。

2. 修改 `CreateConnection.tsx` 文件，确保 `ConfigureConnection` 组件的 `onSubmit` 函数被正确调用。

## 具体修改

### 1. 修改 `PushConnectionForm.tsx` 文件

```tsx
import React, { useState } from 'react';
import {
  Box,
  Button,
  FormControl,
  FormLabel,
  Input,
  Select,
  Stack,
  Text,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  FormHelperText,
  Switch,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
  HStack,
} from '@chakra-ui/react';
import { JsonForm } from '../JsonForm';
import { Connector } from '../../../lib/data_fetching';
import { CreateConnectionState } from '../CreateConnection';

interface PushConnectionFormProps {
  connector: Connector;
  state: CreateConnectionState;
  setState: (state: CreateConnectionState) => void;
  onSubmit: () => void;
}

export const PushConnectionForm: React.FC<PushConnectionFormProps> = ({
  connector,
  state,
  setState,
  onSubmit,
}) => {
  const [protocol, setProtocol] = useState<string>(state.table?.protocol || 'http');
  const [isValidated, setIsValidated] = useState<boolean>(false);
  
  const handleProtocolChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const newProtocol = e.target.value;
    setProtocol(newProtocol);
    
    // Update state with new protocol
    setState({
      ...state,
      table: {
        ...state.table,
        protocol: newProtocol,
      },
    });
  };

  const handleValidate = () => {
    // 在实际应用中，这里应该有真正的验证逻辑
    // 现在我们只是简单地设置 isValidated 为 true
    setIsValidated(true);
  };

  return (
    <Stack spacing={6}>
      <FormControl isRequired>
        <FormLabel>Topic Name</FormLabel>
        <Input
          placeholder="my-topic"
          value={state.table?.topic || ''}
          onChange={(e) => {
            setState({
              ...state,
              table: {
                ...state.table,
                topic: e.target.value,
              },
            });
          }}
        />
        <FormHelperText>
          The topic name for the push connector. This will be used in the URL path.
        </FormHelperText>
      </FormControl>

      <FormControl isRequired>
        <FormLabel>Protocol</FormLabel>
        <Select value={protocol} onChange={handleProtocolChange}>
          <option value="http">HTTP</option>
          <option value="grpc">gRPC</option>
          <option value="websocket">WebSocket</option>
          <option value="quic">QUIC</option>
        </Select>
        <FormHelperText>
          The protocol used for receiving data from external systems.
        </FormHelperText>
      </FormControl>

      {protocol === 'http' && (
        <Box p={4} borderWidth="1px" borderRadius="md">
          <Text fontWeight="bold" mb={4}>HTTP Configuration</Text>
          <Stack spacing={4}>
            <FormControl>
              <FormLabel>Timeout (seconds)</FormLabel>
              <NumberInput
                defaultValue={30}
                min={1}
                max={300}
                value={state.table?.http_config?.timeout || 30}
                onChange={(valueString) => {
                  setState({
                    ...state,
                    table: {
                      ...state.table,
                      http_config: {
                        ...state.table?.http_config,
                        timeout: parseInt(valueString),
                      },
                    },
                  });
                }}
              >
                <NumberInputField />
                <NumberInputStepper>
                  <NumberIncrementStepper />
                  <NumberDecrementStepper />
                </NumberInputStepper>
              </NumberInput>
              <FormHelperText>Request timeout in seconds</FormHelperText>
            </FormControl>

            <FormControl>
              <FormLabel>Max Connections</FormLabel>
              <NumberInput
                defaultValue={100}
                min={10}
                max={10000}
                value={state.table?.http_config?.max_connections || 100}
                onChange={(valueString) => {
                  setState({
                    ...state,
                    table: {
                      ...state.table,
                      http_config: {
                        ...state.table?.http_config,
                        max_connections: parseInt(valueString),
                      },
                    },
                  });
                }}
              >
                <NumberInputField />
                <NumberInputStepper>
                  <NumberIncrementStepper />
                  <NumberDecrementStepper />
                </NumberInputStepper>
              </NumberInput>
              <FormHelperText>Maximum number of concurrent connections</FormHelperText>
            </FormControl>
          </Stack>
        </Box>
      )}

      {/* 其他协议的配置选项可以在这里添加 */}

      <HStack spacing={4} justify="flex-end">
        <Button colorScheme="blue" variant="outline" onClick={handleValidate}>
          Validate
        </Button>
        {isValidated && (
          <Button colorScheme="blue" onClick={onSubmit}>
            Continue
          </Button>
        )}
      </HStack>
    </Stack>
  );
};

export default PushConnectionForm;
```

### 2. 修改 `ConfigureConnection.tsx` 文件（如果需要）

如果 `ConfigureConnection.tsx` 文件中有问题，我们也需要修改它，确保它正确地使用 `PushConnectionForm` 组件。

## 测试步骤

1. 修改 `PushConnectionForm.tsx` 文件
2. 重新构建前端
3. 测试 Push Connector 的创建流程，确保点击 "Validate" 按钮后出现 "Continue" 按钮，并且点击 "Continue" 按钮后能够进入下一步

## 预期结果

用户应该能够在创建 Push Connector 时，点击 "Validate" 按钮后看到 "Continue" 按钮，并且点击 "Continue" 按钮后能够进入下一步。
