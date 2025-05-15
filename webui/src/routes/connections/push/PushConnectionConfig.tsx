import React, { useState } from 'react';
import {
  Box,
  Button,
  Flex,
  Heading,
  Stack,
  Text,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  Code,
  useClipboard,
  Alert,
  AlertIcon,
  AlertTitle,
  AlertDescription,
  Divider,
  Badge,
} from '@chakra-ui/react';
import { CheckIcon, CopyIcon } from '@chakra-ui/icons';
import { PushTopicManager } from './PushTopicManager';

interface PushConnectionConfigProps {
  connectionId: string;
  connectionName: string;
  protocol: string;
  topic: string;
  host: string;
  port: number;
}

export const PushConnectionConfig: React.FC<PushConnectionConfigProps> = ({
  connectionId,
  connectionName,
  protocol,
  topic,
  host,
  port,
}) => {
  const [activeTab, setActiveTab] = useState(0);
  
  // 构建 URL
  const baseUrl = `${protocol}://${host}:${port}`;
  const pushUrl = `${baseUrl}/api/v1/push/${topic}`;
  
  // 复制功能
  const { hasCopied: hasUrlCopied, onCopy: onUrlCopy } = useClipboard(pushUrl);
  
  // 示例代码
  const curlExample = `curl -X POST ${pushUrl} \\
  -H "Content-Type: application/json" \\
  -d '{"id":"123","data":"example"}'`;
  
  const { hasCopied: hasCurlCopied, onCopy: onCurlCopy } = useClipboard(curlExample);
  
  const pythonExample = `import requests
import json

url = "${pushUrl}"
data = {
    "id": "123",
    "data": "example"
}

response = requests.post(url, json=data)
print(response.json())`;
  
  const { hasCopied: hasPythonCopied, onCopy: onPythonCopy } = useClipboard(pythonExample);
  
  const nodeExample = `const fetch = require('node-fetch');

const url = "${pushUrl}";
const data = {
    id: "123",
    data: "example"
};

fetch(url, {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
    },
    body: JSON.stringify(data),
})
.then(response => response.json())
.then(data => console.log(data))
.catch(error => console.error('Error:', error));`;
  
  const { hasCopied: hasNodeCopied, onCopy: onNodeCopy } = useClipboard(nodeExample);

  return (
    <Box>
      <Stack spacing={6}>
        <Heading size="md">{connectionName} Configuration</Heading>
        
        <Alert status="info" variant="subtle">
          <AlertIcon />
          <Box>
            <AlertTitle>Push Endpoint Information</AlertTitle>
            <AlertDescription>
              Use the following endpoint to push data to this connection:
              <Flex mt={2} alignItems="center">
                <Code p={2} borderRadius="md" fontSize="sm" flex="1">
                  {pushUrl}
                </Code>
                <Button
                  size="sm"
                  ml={2}
                  leftIcon={hasUrlCopied ? <CheckIcon /> : <CopyIcon />}
                  onClick={onUrlCopy}
                >
                  {hasUrlCopied ? 'Copied' : 'Copy'}
                </Button>
              </Flex>
            </AlertDescription>
          </Box>
        </Alert>
        
        <Tabs variant="enclosed" index={activeTab} onChange={setActiveTab}>
          <TabList>
            <Tab>Topics</Tab>
            <Tab>Examples</Tab>
            <Tab>Documentation</Tab>
          </TabList>
          
          <TabPanels>
            <TabPanel>
              <PushTopicManager connectionId={connectionId} />
            </TabPanel>
            
            <TabPanel>
              <Stack spacing={6}>
                <Box>
                  <Heading size="sm" mb={2}>cURL Example</Heading>
                  <Flex direction="column">
                    <Code p={4} borderRadius="md" fontSize="sm" whiteSpace="pre">
                      {curlExample}
                    </Code>
                    <Button
                      size="sm"
                      mt={2}
                      alignSelf="flex-end"
                      leftIcon={hasCurlCopied ? <CheckIcon /> : <CopyIcon />}
                      onClick={onCurlCopy}
                    >
                      {hasCurlCopied ? 'Copied' : 'Copy'}
                    </Button>
                  </Flex>
                </Box>
                
                <Divider />
                
                <Box>
                  <Heading size="sm" mb={2}>Python Example</Heading>
                  <Flex direction="column">
                    <Code p={4} borderRadius="md" fontSize="sm" whiteSpace="pre">
                      {pythonExample}
                    </Code>
                    <Button
                      size="sm"
                      mt={2}
                      alignSelf="flex-end"
                      leftIcon={hasPythonCopied ? <CheckIcon /> : <CopyIcon />}
                      onClick={onPythonCopy}
                    >
                      {hasPythonCopied ? 'Copied' : 'Copy'}
                    </Button>
                  </Flex>
                </Box>
                
                <Divider />
                
                <Box>
                  <Heading size="sm" mb={2}>Node.js Example</Heading>
                  <Flex direction="column">
                    <Code p={4} borderRadius="md" fontSize="sm" whiteSpace="pre">
                      {nodeExample}
                    </Code>
                    <Button
                      size="sm"
                      mt={2}
                      alignSelf="flex-end"
                      leftIcon={hasNodeCopied ? <CheckIcon /> : <CopyIcon />}
                      onClick={onNodeCopy}
                    >
                      {hasNodeCopied ? 'Copied' : 'Copy'}
                    </Button>
                  </Flex>
                </Box>
              </Stack>
            </TabPanel>
            
            <TabPanel>
              <Stack spacing={4}>
                <Heading size="sm">Push Connector Documentation</Heading>
                <Text>
                  The Push Connector allows external systems to push data directly to Arroyo.
                  It supports multiple protocols including HTTP, gRPC, WebSocket, and QUIC.
                </Text>
                
                <Heading size="sm" mt={2}>Supported Protocols</Heading>
                <Stack>
                  <Flex alignItems="center">
                    <Badge colorScheme="green" mr={2}>HTTP</Badge>
                    <Text>Simple REST API for pushing data</Text>
                  </Flex>
                  <Flex alignItems="center">
                    <Badge colorScheme="blue" mr={2}>gRPC</Badge>
                    <Text>High-performance RPC framework</Text>
                  </Flex>
                  <Flex alignItems="center">
                    <Badge colorScheme="purple" mr={2}>WebSocket</Badge>
                    <Text>Bidirectional communication channel</Text>
                  </Flex>
                  <Flex alignItems="center">
                    <Badge colorScheme="orange" mr={2}>QUIC</Badge>
                    <Text>Next-generation transport protocol</Text>
                  </Flex>
                </Stack>
                
                <Button
                  colorScheme="blue"
                  variant="outline"
                  mt={4}
                  onClick={() => window.open('https://doc.arroyo.dev/connectors/push', '_blank')}
                >
                  View Full Documentation
                </Button>
              </Stack>
            </TabPanel>
          </TabPanels>
        </Tabs>
      </Stack>
    </Box>
  );
};

export default PushConnectionConfig;
