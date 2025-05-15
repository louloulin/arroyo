import React, { useState } from 'react';
import {
  Box,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  IconButton,
  Flex,
  Spinner,
  Text,
  useToast,
  AlertDialog,
  AlertDialogBody,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogContent,
  AlertDialogOverlay,
  Button,
  useDisclosure,
} from '@chakra-ui/react';
import { DeleteIcon, InfoIcon } from '@chakra-ui/icons';
import { usePushTopics, deletePushTopic } from '../../../lib/data_fetching';

interface TopicListProps {
  connectionId: string;
  onViewDetails: (topicName: string) => void;
}

export const TopicList: React.FC<TopicListProps> = ({ connectionId, onViewDetails }) => {
  const { topics, topicsLoading, topicsError, mutateTopics } = usePushTopics(connectionId);
  const [topicToDelete, setTopicToDelete] = useState<string | null>(null);
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = React.useRef<HTMLButtonElement>(null);
  const toast = useToast();
  const [isDeleting, setIsDeleting] = useState(false);

  const handleDeleteClick = (topicName: string) => {
    setTopicToDelete(topicName);
    onOpen();
  };

  const handleDeleteConfirm = async () => {
    if (!topicToDelete) return;

    setIsDeleting(true);
    try {
      await deletePushTopic(connectionId, topicToDelete);
      toast({
        title: 'Topic deleted',
        description: `Topic "${topicToDelete}" has been deleted successfully.`,
        status: 'success',
        duration: 5000,
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
      setIsDeleting(false);
      onClose();
      setTopicToDelete(null);
    }
  };

  if (topicsLoading) {
    return (
      <Flex justifyContent="center" alignItems="center" height="200px">
        <Spinner size="xl" />
      </Flex>
    );
  }

  if (topicsError) {
    return (
      <Box p={4} textAlign="center">
        <Text color="red.500">Error loading topics: {topicsError}</Text>
      </Box>
    );
  }

  if (!topics || topics.length === 0) {
    return (
      <Box p={4} textAlign="center">
        <Text>No topics found. Create a new topic to get started.</Text>
      </Box>
    );
  }

  return (
    <>
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
                    onClick={() => onViewDetails(topic.name)}
                  />
                  <IconButton
                    aria-label="Delete topic"
                    icon={<DeleteIcon />}
                    colorScheme="red"
                    variant="ghost"
                    onClick={() => handleDeleteClick(topic.name)}
                  />
                </Flex>
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>

      <AlertDialog
        isOpen={isOpen}
        leastDestructiveRef={cancelRef}
        onClose={onClose}
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
              <Button ref={cancelRef} onClick={onClose}>
                Cancel
              </Button>
              <Button 
                colorScheme="red" 
                onClick={handleDeleteConfirm} 
                ml={3}
                isLoading={isDeleting}
                loadingText="Deleting"
              >
                Delete
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </>
  );
};

export default TopicList;
