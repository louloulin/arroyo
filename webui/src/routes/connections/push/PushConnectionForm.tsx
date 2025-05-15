import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  FormControl,
  FormLabel,
  Input,
  Select,
  Stack,
  Text,
  FormHelperText,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
} from '@chakra-ui/react';
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
  // 初始化 state.table 如果它不存在
  useEffect(() => {
    if (!state.table) {
      setState({
        ...state,
        table: {
          protocol: 'http',
          topic: '',
          http_config: {
            timeout: '30',
            max_connections: '100'
          }
        }
      });
    }
  }, [state, setState]);

  const [protocol, setProtocol] = useState<string>(state.table?.protocol || 'http');

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
                defaultValue="30"
                min={1}
                max={300}
                value={state.table?.http_config?.timeout || '30'}
                onChange={(valueString) => {
                  setState({
                    ...state,
                    table: {
                      ...state.table,
                      http_config: {
                        ...state.table?.http_config,
                        timeout: valueString, // 保持为字符串类型
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
                defaultValue="100"
                min={10}
                max={10000}
                value={state.table?.http_config?.max_connections || '100'}
                onChange={(valueString) => {
                  setState({
                    ...state,
                    table: {
                      ...state.table,
                      http_config: {
                        ...state.table?.http_config,
                        max_connections: valueString, // 保持为字符串类型
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

      <Button colorScheme="blue" onClick={onSubmit}>
        Next
      </Button>
    </Stack>
  );
};

export default PushConnectionForm;
