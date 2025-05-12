import React, { useState } from 'react';
import {
  Box,
  Button,
  Container,
  Flex,
  Heading,
  HStack,
  Icon,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  Text,
  useDisclosure,
  useToast,
  Badge,
  Spinner,
  AlertDialog,
  AlertDialogBody,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogContent,
  AlertDialogOverlay,
} from '@chakra-ui/react';
import { FiPlus, FiTrash2, FiEdit } from 'react-icons/fi';
import { Link, useNavigate } from 'react-router-dom';
import { useNavbar } from '../../App';
import { formatError } from '../../lib/util';
import { useTopics, deleteTopic } from '../../lib/data_fetching';
import CreateTopicModal from '../../components/CreateTopicModal';

export function TopicsIndex() {
  useNavbar({ title: 'Topics' });
  const navigate = useNavigate();
  const toast = useToast();
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [deleteTopicName, setDeleteTopicName] = useState<string | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const cancelRef = React.useRef<HTMLButtonElement>(null);

  // Fetch topics
  const { data: topics, isLoading, error, refetch } = useTopics();

  // Handle topic creation success
  const handleTopicCreated = () => {
    toast({
      title: 'Topic created',
      description: 'The topic was created successfully',
      status: 'success',
      duration: 5000,
      isClosable: true,
    });
    refetch();
    onClose();
  };

  // Handle topic deletion
  const confirmDeleteTopic = (topicName: string) => {
    setDeleteTopicName(topicName);
  };

  const handleDeleteTopic = async () => {
    if (!deleteTopicName) return;

    setIsDeleting(true);
    try {
      await deleteTopic(deleteTopicName);
      toast({
        title: 'Topic deleted',
        description: `Topic "${deleteTopicName}" was deleted successfully`,
        status: 'success',
        duration: 5000,
        isClosable: true,
      });
      refetch();
    } catch (err) {
      toast({
        title: 'Error deleting topic',
        description: formatError(err),
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
    } finally {
      setIsDeleting(false);
      setDeleteTopicName(null);
    }
  };

  const closeDeleteDialog = () => {
    setDeleteTopicName(null);
  };

  return (
    <Container maxW="container.xl" py={8}>
      <Flex justifyContent="space-between" alignItems="center" mb={6}>
        <Heading size="lg">Topics</Heading>
        <Button leftIcon={<Icon as={FiPlus} />} colorScheme="blue" onClick={onOpen}>
          Create Topic
        </Button>
      </Flex>

      {isLoading ? (
        <Flex justifyContent="center" alignItems="center" height="200px">
          <Spinner size="xl" />
        </Flex>
      ) : error ? (
        <Box p={4} bg="red.50" color="red.800" borderRadius="md">
          <Text>Error loading topics: {formatError(error)}</Text>
        </Box>
      ) : topics && topics.length > 0 ? (
        <Box overflowX="auto">
          <Table variant="simple">
            <Thead>
              <Tr>
                <Th>Name</Th>
                <Th>Partitions</Th>
                <Th>Replication Factor</Th>
                <Th>Retention</Th>
                <Th>Created</Th>
                <Th>Actions</Th>
              </Tr>
            </Thead>
            <Tbody>
              {topics.map((topic) => (
                <Tr key={topic.name}>
                  <Td>
                    <Link to={`/topics/${topic.name}`}>
                      <Text color="blue.500" fontWeight="medium">
                        {topic.name}
                      </Text>
                    </Link>
                  </Td>
                  <Td>{topic.partitions}</Td>
                  <Td>{topic.replicationFactor}</Td>
                  <Td>
                    {topic.retentionMs ? (
                      `${Math.floor(topic.retentionMs / (1000 * 60 * 60 * 24))} days`
                    ) : (
                      <Badge colorScheme="green">Forever</Badge>
                    )}
                  </Td>
                  <Td>{new Date(topic.createdAt * 1000).toLocaleString()}</Td>
                  <Td>
                    <HStack spacing={2}>
                      <Button
                        size="sm"
                        colorScheme="blue"
                        variant="ghost"
                        leftIcon={<Icon as={FiEdit} />}
                        onClick={() => navigate(`/topics/${topic.name}/edit`)}
                      >
                        Edit
                      </Button>
                      <Button
                        size="sm"
                        colorScheme="red"
                        variant="ghost"
                        leftIcon={<Icon as={FiTrash2} />}
                        onClick={() => confirmDeleteTopic(topic.name)}
                      >
                        Delete
                      </Button>
                    </HStack>
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        </Box>
      ) : (
        <Box p={6} textAlign="center" bg="gray.50" borderRadius="md">
          <Text fontSize="lg" mb={4}>
            No topics found
          </Text>
          <Text mb={4}>Create your first topic to get started</Text>
          <Button colorScheme="blue" onClick={onOpen}>
            Create Topic
          </Button>
        </Box>
      )}

      {/* Create Topic Modal */}
      <CreateTopicModal isOpen={isOpen} onClose={onClose} onSuccess={handleTopicCreated} />

      {/* Delete Confirmation Dialog */}
      <AlertDialog
        isOpen={deleteTopicName !== null}
        leastDestructiveRef={cancelRef}
        onClose={closeDeleteDialog}
      >
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Delete Topic
            </AlertDialogHeader>

            <AlertDialogBody>
              Are you sure you want to delete the topic "{deleteTopicName}"? This action cannot be
              undone.
            </AlertDialogBody>

            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={closeDeleteDialog}>
                Cancel
              </Button>
              <Button
                colorScheme="red"
                onClick={handleDeleteTopic}
                ml={3}
                isLoading={isDeleting}
              >
                Delete
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Container>
  );
}
