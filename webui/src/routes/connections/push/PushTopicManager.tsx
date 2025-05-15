import React, { useState } from 'react';
import {
  Box,
  Button,
  Flex,
  Heading,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  IconButton,
  useDisclosure,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalFooter,
  ModalBody,
  ModalCloseButton,
  FormControl,
  FormLabel,
  Input,
  FormHelperText,
  useToast,
  Text,
  Spinner,
  Alert,
  AlertIcon,
} from '@chakra-ui/react';
import { AddIcon, DeleteIcon, InfoIcon } from '@chakra-ui/icons';
import { PushTopic } from '../../../lib/data_fetching';

interface PushTopicManagerProps {
  connectionId: string;
  onViewDetails?: (topicName: string) => void;
  onCreateTopic?: () => void;
}

export const PushTopicManager: React.FC<PushTopicManagerProps> = ({
  connectionId,
  onViewDetails,
  onCreateTopic
}) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [newTopicName, setNewTopicName] = useState('');
  const [isCreating, setIsCreating] = useState(false);
  const [isDeleting, setIsDeleting] = useState<string | null>(null);
  const toast = useToast();

  // 模拟数据 - 在实际实现中，这些数据应该从 API 获取
  const [topics, setTopics] = useState<PushTopic[]>([
    {
      name: 'events',
      messages: 1245,
      created_at: Date.now() / 1000 - 86400 * 3,
      last_activity: Date.now() / 1000 - 3600,
      retention_period: 7 * 86400,
      compression: true,
    },
    {
      name: 'logs',
      messages: 5678,
      created_at: Date.now() / 1000 - 86400 * 5,
      last_activity: Date.now() / 1000 - 1800,
      retention_period: 14 * 86400,
      compression: false,
    },
  ]);
  const topicsLoading = false;
  const topicsError = null;
  const mutateTopics = () => {};

  const handleCreateTopic = async () => {
    if (!newTopicName.trim()) {
      toast({
        title: 'Error',
        description: 'Topic name cannot be empty',
        status: 'error',
        duration: 3000,
        isClosable: true,
      });
      return;
    }

    setIsCreating(true);
    try {
      // 模拟创建主题
      setTimeout(() => {
        // 添加新主题到列表
        setTopics([
          ...topics,
          {
            name: newTopicName,
            messages: 0,
            created_at: Date.now() / 1000,
            retention_period: 7 * 86400,
            compression: false,
          }
        ]);

        toast({
          title: 'Success',
          description: `Topic "${newTopicName}" created successfully`,
          status: 'success',
          duration: 3000,
          isClosable: true,
        });
        setNewTopicName('');
        onClose();

        // 如果提供了回调函数，则调用它
        if (onCreateTopic) {
          onCreateTopic();
        }
      }, 1000);
    } catch (error) {
      toast({
        title: 'Error',
        description: `Failed to create topic: ${error}`,
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
    } finally {
      setIsCreating(false);
    }
  };

  const handleDeleteTopic = async (topicName: string) => {
    setIsDeleting(topicName);
    try {
      // 模拟删除主题
      setTimeout(() => {
        // 从列表中移除主题
        setTopics(topics.filter(topic => topic.name !== topicName));

        toast({
          title: 'Success',
          description: `Topic "${topicName}" deleted successfully`,
          status: 'success',
          duration: 3000,
          isClosable: true,
        });
      }, 1000);
    } catch (error) {
      toast({
        title: 'Error',
        description: `Failed to delete topic: ${error}`,
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
    } finally {
      setIsDeleting(null);
    }
  };

  return (
    <Box>
      <Flex justifyContent="space-between" alignItems="center" mb={4}>
        <Heading size="md">Push Topics</Heading>
        <Button
          leftIcon={<AddIcon />}
          colorScheme="blue"
          onClick={onCreateTopic ? onCreateTopic : onOpen}
        >
          Create Topic
        </Button>
      </Flex>

      {topicsError && (
        <Alert status="error" mb={4}>
          <AlertIcon />
          Failed to load topics: {topicsError}
        </Alert>
      )}

      {topicsLoading ? (
        <Flex justifyContent="center" alignItems="center" height="200px">
          <Spinner size="xl" />
        </Flex>
      ) : topics && topics.length > 0 ? (
        <Table variant="simple">
          <Thead>
            <Tr>
              <Th>Topic Name</Th>
              <Th>Messages</Th>
              <Th>Created At</Th>
              <Th>Actions</Th>
            </Tr>
          </Thead>
          <Tbody>
            {topics.map((topic) => (
              <Tr key={topic.name}>
                <Td>{topic.name}</Td>
                <Td>{topic.messages}</Td>
                <Td>{new Date(topic.created_at * 1000).toLocaleString()}</Td>
                <Td>
                  <Flex>
                    <IconButton
                      aria-label="View topic details"
                      icon={<InfoIcon />}
                      variant="ghost"
                      mr={2}
                      onClick={() => onViewDetails && onViewDetails(topic.name)}
                    />
                    <IconButton
                      aria-label="Delete topic"
                      icon={<DeleteIcon />}
                      colorScheme="red"
                      variant="ghost"
                      isLoading={isDeleting === topic.name}
                      onClick={() => handleDeleteTopic(topic.name)}
                    />
                  </Flex>
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      ) : (
        <Box p={4} textAlign="center">
          <Text>No topics found. Create a new topic to get started.</Text>
        </Box>
      )}

      {/* Create Topic Modal */}
      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Create New Topic</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <FormControl isRequired>
              <FormLabel>Topic Name</FormLabel>
              <Input
                placeholder="my-topic"
                value={newTopicName}
                onChange={(e) => setNewTopicName(e.target.value)}
              />
              <FormHelperText>
                Enter a unique name for your topic. This will be used in the URL path.
              </FormHelperText>
            </FormControl>
          </ModalBody>

          <ModalFooter>
            <Button variant="ghost" mr={3} onClick={onClose}>
              Cancel
            </Button>
            <Button
              colorScheme="blue"
              onClick={handleCreateTopic}
              isLoading={isCreating}
              loadingText="Creating"
            >
              Create
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </Box>
  );
};

export default PushTopicManager;
