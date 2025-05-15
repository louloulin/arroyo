import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  Heading,
  Stack,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  Text,
  Flex,
  Spinner,
  Alert,
  AlertIcon,
  useDisclosure,
} from '@chakra-ui/react';
import { useParams, useNavigate } from 'react-router-dom';
import { ConnectionTable, useConnectionTables } from '../../../lib/data_fetching';
import { PushConnectionConfig } from './PushConnectionConfig';
import { PushTopicManager } from './PushTopicManager';
import { TopicDetails } from './TopicDetails';
import { CreateTopic } from './CreateTopic';
import { PushConnectorDocs } from './PushConnectorDocs';

export const PushConnectionDetails: React.FC = () => {
  const { connectionId } = useParams<{ connectionId: string }>();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState(0);
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);
  const { isOpen, onOpen, onClose } = useDisclosure();

  // 获取连接表信息
  const { connectionTablePages, connectionTablesLoading } = useConnectionTables(100);

  // 查找当前连接
  const connection = connectionTablePages?.flatMap(page => page.data).find(
    table => table.id === connectionId
  );

  // 如果连接不存在，重定向到连接列表页面
  useEffect(() => {
    if (!connectionTablesLoading && !connection) {
      navigate('/connections');
    }
  }, [connection, connectionTablesLoading, navigate]);

  // 从连接配置中提取协议、主题、主机和端口信息
  const config = connection?.config as any;
  const protocol = config?.protocol || 'http';
  const topic = config?.topic || '';
  const host = window.location.hostname;
  const port = 8000; // 默认端口，实际应从配置中获取

  // 处理主题选择
  const handleTopicSelect = (topicName: string) => {
    setSelectedTopic(topicName);
    setActiveTab(1); // 切换到主题详情标签
  };

  // 处理返回到主题列表
  const handleBackToTopics = () => {
    setSelectedTopic(null);
    setActiveTab(0); // 切换回主题列表标签
  };

  if (connectionTablesLoading) {
    return (
      <Flex justifyContent="center" alignItems="center" height="200px">
        <Spinner size="xl" />
      </Flex>
    );
  }

  if (!connection) {
    return (
      <Alert status="error">
        <AlertIcon />
        Connection not found
      </Alert>
    );
  }

  return (
    <Container maxW="container.xl" py={8}>
      <Stack spacing={6}>
        <Heading size="lg">{connection.name}</Heading>
        <Text color="gray.500">Push Connector - {protocol.toUpperCase()}</Text>

        <Tabs index={activeTab} onChange={setActiveTab} variant="enclosed">
          <TabList>
            <Tab>Topics</Tab>
            {selectedTopic && <Tab>Topic Details</Tab>}
            <Tab>Configuration</Tab>
            <Tab>Documentation</Tab>
          </TabList>

          <TabPanels>
            <TabPanel>
              <PushTopicManager
                connectionId={connectionId || ''}
                onViewDetails={handleTopicSelect}
                onCreateTopic={onOpen}
              />
            </TabPanel>

            {selectedTopic && (
              <TabPanel>
                <TopicDetails
                  connectionId={connectionId || ''}
                  topicName={selectedTopic}
                  protocol={protocol}
                  host={host}
                  port={port}
                  onBack={handleBackToTopics}
                />
              </TabPanel>
            )}

            <TabPanel>
              <PushConnectionConfig
                connectionId={connectionId || ''}
                connectionName={connection.name}
                protocol={protocol}
                topic={topic}
                host={host}
                port={port}
              />
            </TabPanel>

            <TabPanel>
              <PushConnectorDocs />
            </TabPanel>
          </TabPanels>
        </Tabs>
      </Stack>

      {/* 创建主题对话框 */}
      <CreateTopic
        isOpen={isOpen}
        onClose={onClose}
        connectionId={connectionId || ''}
        onSuccess={() => {
          // 刷新主题列表
          onClose();
        }}
      />
    </Container>
  );
};

export default PushConnectionDetails;
