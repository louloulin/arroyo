import React, { useState, useEffect } from 'react';
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
  Badge,
  Spinner,
  Alert,
  AlertIcon,
} from '@chakra-ui/react';
import { AddIcon, DeleteIcon, InfoIcon } from '@chakra-ui/icons';
import { usePushTopics, createPushTopic, deletePushTopic } from '../../../lib/data_fetching';

interface PushTopicManagerProps {
  connectionId: string;
}

export const PushTopicManager: React.FC<PushTopicManagerProps> = ({ connectionId }) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [newTopicName, setNewTopicName] = useState('');
  const [isCreating, setIsCreating] = useState(false);
  const [isDeleting, setIsDeleting] = useState<string | null>(null);
  const toast = useToast();

  // 这里应该使用实际的 API 调用来获取主题列表
  // 目前使用模拟数据
  const { topics, topicsLoading, topicsError, mutateTopics } = usePushTopics(connectionId);

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
      await createPushTopic(connectionId, newTopicName);
      toast({
        title: 'Success',
        description: `Topic "${newTopicName}" created successfully`,
        status: 'success',
        duration: 3000,
        isClosable: true,
      });
      setNewTopicName('');
      onClose();
      mutateTopics();
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
      await deletePushTopic(connectionId, topicName);
      toast({
        title: 'Success',
        description: `Topic "${topicName}" deleted successfully`,
        status: 'success',
        duration: 3000,
        isClosable: true,
      });
      mutateTopics();
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
        <Button leftIcon={<AddIcon />} colorScheme="blue" onClick={onOpen}>
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
                  <IconButton
                    aria-label="Delete topic"
                    icon={<DeleteIcon />}
                    colorScheme="red"
                    variant="ghost"
                    isLoading={isDeleting === topic.name}
                    onClick={() => handleDeleteTopic(topic.name)}
                  />
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
