import React, { useState, useRef } from 'react';
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
  AlertDialog,
  AlertDialogBody,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogContent,
  AlertDialogOverlay,
  Switch,
  FormErrorMessage,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
  Stack,
} from '@chakra-ui/react';
import { AddIcon, DeleteIcon, InfoIcon } from '@chakra-ui/icons';
import { PushTopic, usePushTopics, createPushTopic, deletePushTopic } from '../../../lib/data_fetching';

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
  const [retentionPeriod, setRetentionPeriod] = useState(7); // 默认保留期为7天
  const [enableCompression, setEnableCompression] = useState(false);
  const [isCreating, setIsCreating] = useState(false);
  const [isDeleting, setIsDeleting] = useState<string | null>(null);
  const [topicToDelete, setTopicToDelete] = useState<string | null>(null);
  const cancelRef = useRef<HTMLButtonElement>(null);
  const { isOpen: isDeleteOpen, onOpen: onDeleteOpen, onClose: onDeleteClose } = useDisclosure();
  const toast = useToast();

  // 使用真实的 API 调用获取主题列表
  const { topics, topicsLoading, topicsError, mutateTopics } = usePushTopics(connectionId);

  const validateTopicName = (name: string): boolean => {
    if (!name.trim()) {
      toast({
        title: 'Error',
        description: 'Topic name cannot be empty',
        status: 'error',
        duration: 3000,
        isClosable: true,
      });
      return false;
    }

    // 验证主题名称只包含字母、数字、下划线和连字符
    const validNameRegex = /^[a-zA-Z0-9_-]+$/;
    if (!validNameRegex.test(name)) {
      toast({
        title: 'Error',
        description: 'Topic name can only contain letters, numbers, underscores and hyphens',
        status: 'error',
        duration: 3000,
        isClosable: true,
      });
      return false;
    }

    return true;
  };

  const handleCreateTopic = async () => {
    if (!validateTopicName(newTopicName)) {
      return;
    }

    setIsCreating(true);
    try {
      // 使用真实的 API 调用创建主题
      await createPushTopic(connectionId, newTopicName, {
        retention_period: retentionPeriod * 24 * 60 * 60, // 将天数转换为秒
        compression: enableCompression,
      });

      toast({
        title: 'Success',
        description: `Topic "${newTopicName}" created successfully`,
        status: 'success',
        duration: 3000,
        isClosable: true,
      });

      // 刷新主题列表
      mutateTopics();

      // 重置表单
      setNewTopicName('');
      setRetentionPeriod(7);
      setEnableCompression(false);
      onClose();

      // 如果提供了回调函数，则调用它
      if (onCreateTopic) {
        onCreateTopic();
      }
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

  const handleDeleteClick = (topicName: string) => {
    setTopicToDelete(topicName);
    onDeleteOpen();
  };

  const handleDeleteTopic = async () => {
    if (!topicToDelete) return;

    setIsDeleting(topicToDelete);
    try {
      // 使用真实的 API 调用删除主题
      await deletePushTopic(connectionId, topicToDelete);

      toast({
        title: 'Success',
        description: `Topic "${topicToDelete}" deleted successfully`,
        status: 'success',
        duration: 3000,
        isClosable: true,
      });

      // 刷新主题列表
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
      setTopicToDelete(null);
      onDeleteClose();
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
          Failed to load topics: {topicsError.message}
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
                      onClick={() => handleDeleteClick(topic.name)}
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
            <Stack spacing={4}>
              <FormControl isRequired>
                <FormLabel>Topic Name</FormLabel>
                <Input
                  placeholder="my-topic"
                  value={newTopicName}
                  onChange={(e) => setNewTopicName(e.target.value)}
                />
                <FormHelperText>
                  Enter a unique name for your topic. This will be used in the URL path.
                  Only letters, numbers, underscores and hyphens are allowed.
                </FormHelperText>
              </FormControl>

              <FormControl>
                <FormLabel>Retention Period (days)</FormLabel>
                <NumberInput
                  min={1}
                  max={365}
                  value={retentionPeriod}
                  onChange={(valueString) => setRetentionPeriod(parseInt(valueString))}
                >
                  <NumberInputField />
                  <NumberInputStepper>
                    <NumberIncrementStepper />
                    <NumberDecrementStepper />
                  </NumberInputStepper>
                </NumberInput>
                <FormHelperText>
                  How long to keep messages in this topic (in days).
                </FormHelperText>
              </FormControl>

              <FormControl>
                <FormLabel>Enable Compression</FormLabel>
                <Switch
                  isChecked={enableCompression}
                  onChange={(e) => setEnableCompression(e.target.checked)}
                />
                <FormHelperText>
                  Enable compression to reduce storage size.
                </FormHelperText>
              </FormControl>
            </Stack>
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

      {/* Delete Confirmation Dialog */}
      <AlertDialog
        isOpen={isDeleteOpen}
        leastDestructiveRef={cancelRef}
        onClose={onDeleteClose}
      >
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Delete Topic
            </AlertDialogHeader>

            <AlertDialogBody>
              Are you sure you want to delete the topic "{topicToDelete}"? This action cannot be undone.
            </AlertDialogBody>

            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={onDeleteClose}>
                Cancel
              </Button>
              <Button
                colorScheme="red"
                onClick={handleDeleteTopic}
                ml={3}
                isLoading={isDeleting === topicToDelete}
                loadingText="Deleting"
              >
                Delete
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Box>
  );
};

export default PushTopicManager;
