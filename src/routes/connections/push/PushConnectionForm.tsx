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
  Spacer,
} from '@chakra-ui/react';
import { ArrowRightIcon } from '@chakra-ui/icons';
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
  const [topicName, setTopicName] = useState<string>(state.table?.topic || '');

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

  const handleTopicNameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newTopicName = e.target.value;
    setTopicName(newTopicName);

    setState({
      ...state,
      table: {
        ...state.table,
        topic: newTopicName,
      },
    });
  };

  const handleValidate = () => {
    // 在实际应用中，这里应该有真正的验证逻辑
    // 现在我们只是简单地设置 isValidated 为 true
    setIsValidated(true);
    console.log('Validation successful, isValidated set to true');
  };

  const handleSubmit = () => {
    // 确保所有必要的字段都已填写
    if (topicName) {
      // 更新状态
      setState({
        ...state,
        table: {
          ...state.table,
          topic: topicName,
          protocol: protocol,
        },
      });

      // 调用 onSubmit 回调
      onSubmit();
    }
  };

  return (
    <Stack spacing={6}>
      <FormControl isRequired>
        <FormLabel>Topic Name</FormLabel>
        <Input
          placeholder="my-topic"
          value={topicName}
          onChange={handleTopicNameChange}
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

      <HStack>
        <Button colorScheme="blue" variant="outline" onClick={handleValidate}>
          Validate
        </Button>

        <Spacer />

        <Button
          colorScheme="blue"
          onClick={handleSubmit}
          isDisabled={!topicName}
        >
          Continue
          <ArrowRightIcon w={3} h={3} ml={2} />
        </Button>
      </HStack>
    </Stack>
  );
};

export default PushConnectionForm;
