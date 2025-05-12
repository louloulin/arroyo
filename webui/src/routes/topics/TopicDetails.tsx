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
  Spinner,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  Badge,
  Stat,
  StatLabel,
  StatNumber,
  StatGroup,
  SimpleGrid,
  useToast,
  AlertDialog,
  AlertDialogBody,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogContent,
  AlertDialogOverlay,
} from '@chakra-ui/react';
import { FiArrowLeft, FiTrash2, FiEdit } from 'react-icons/fi';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { useNavbar } from '../../App';
import { formatError, formatBytes } from '../../lib/util';
import { useTopicDetails, deleteTopic } from '../../lib/data_fetching';

export function TopicDetails() {
  const { topicName } = useParams<{ topicName: string }>();
  const navigate = useNavigate();
  const toast = useToast();
  const [isDeleting, setIsDeleting] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const cancelRef = React.useRef<HTMLButtonElement>(null);

  useNavbar({ title: `Topic: ${topicName}` });

  // Fetch topic details
  const { data: topic, isLoading, error } = useTopicDetails(topicName || '');

  // Handle topic deletion
  const handleDeleteTopic = async () => {
    if (!topicName) return;

    setIsDeleting(true);
    try {
      await deleteTopic(topicName);
      toast({
        title: 'Topic deleted',
        description: `Topic "${topicName}" was deleted successfully`,
        status: 'success',
        duration: 5000,
        isClosable: true,
      });
      navigate('/topics');
    } catch (err) {
      toast({
        title: 'Error deleting topic',
        description: formatError(err),
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
      setIsDeleting(false);
      setShowDeleteConfirm(false);
    }
  };

  if (isLoading) {
    return (
      <Container maxW="container.xl" py={8}>
        <Flex justifyContent="center" alignItems="center" height="200px">
          <Spinner size="xl" />
        </Flex>
      </Container>
    );
  }

  if (error || !topic) {
    return (
      <Container maxW="container.xl" py={8}>
        <Box p={4} bg="red.50" color="red.800" borderRadius="md">
          <Text>Error loading topic details: {formatError(error)}</Text>
        </Box>
      </Container>
    );
  }

  return (
    <Container maxW="container.xl" py={8}>
      <Flex justifyContent="space-between" alignItems="center" mb={6}>
        <HStack>
          <Button
            as={Link}
            to="/topics"
            variant="ghost"
            leftIcon={<Icon as={FiArrowLeft} />}
            mr={2}
          >
            Back
          </Button>
          <Heading size="lg">{topic.info.name}</Heading>
        </HStack>
        <HStack>
          <Button
            colorScheme="blue"
            leftIcon={<Icon as={FiEdit} />}
            onClick={() => navigate(`/topics/${topicName}/edit`)}
          >
            Edit
          </Button>
          <Button
            colorScheme="red"
            variant="outline"
            leftIcon={<Icon as={FiTrash2} />}
            onClick={() => setShowDeleteConfirm(true)}
          >
            Delete
          </Button>
        </HStack>
      </Flex>

      {topic.info.description && (
        <Box mb={6} p={4} bg="gray.50" borderRadius="md">
          <Text>{topic.info.description}</Text>
        </Box>
      )}

      <SimpleGrid columns={{ base: 1, md: 4 }} spacing={6} mb={6}>
        <Stat bg="white" p={4} borderRadius="md" boxShadow="sm">
          <StatLabel>Partitions</StatLabel>
          <StatNumber>{topic.info.partitions}</StatNumber>
        </Stat>
        <Stat bg="white" p={4} borderRadius="md" boxShadow="sm">
          <StatLabel>Replication Factor</StatLabel>
          <StatNumber>{topic.info.replicationFactor}</StatNumber>
        </Stat>
        <Stat bg="white" p={4} borderRadius="md" boxShadow="sm">
          <StatLabel>Message Count</StatLabel>
          <StatNumber>{topic.messageCount.toLocaleString()}</StatNumber>
        </Stat>
        <Stat bg="white" p={4} borderRadius="md" boxShadow="sm">
          <StatLabel>Size</StatLabel>
          <StatNumber>{formatBytes(topic.sizeBytes)}</StatNumber>
        </Stat>
      </SimpleGrid>

      <Tabs variant="enclosed" colorScheme="blue">
        <TabList>
          <Tab>Overview</Tab>
          <Tab>Partitions</Tab>
        </TabList>

        <TabPanels>
          <TabPanel>
            <Box bg="white" p={4} borderRadius="md" boxShadow="sm">
              <Heading size="md" mb={4}>
                Configuration
              </Heading>
              <Table variant="simple">
                <Tbody>
                  <Tr>
                    <Th>Cleanup Policy</Th>
                    <Td>{topic.info.cleanupPolicy}</Td>
                  </Tr>
                  <Tr>
                    <Th>Retention Time</Th>
                    <Td>
                      {topic.info.retentionMs ? (
                        `${Math.floor(topic.info.retentionMs / (1000 * 60 * 60 * 24))} days`
                      ) : (
                        <Badge colorScheme="green">Forever</Badge>
                      )}
                    </Td>
                  </Tr>
                  <Tr>
                    <Th>Retention Size</Th>
                    <Td>
                      {topic.info.retentionBytes ? (
                        formatBytes(topic.info.retentionBytes)
                      ) : (
                        <Badge colorScheme="green">Unlimited</Badge>
                      )}
                    </Td>
                  </Tr>
                  <Tr>
                    <Th>Max Message Size</Th>
                    <Td>
                      {topic.info.maxMessageBytes ? (
                        formatBytes(topic.info.maxMessageBytes)
                      ) : (
                        <Badge colorScheme="green">Default</Badge>
                      )}
                    </Td>
                  </Tr>
                  <Tr>
                    <Th>Created At</Th>
                    <Td>{new Date(topic.info.createdAt * 1000).toLocaleString()}</Td>
                  </Tr>
                  <Tr>
                    <Th>Updated At</Th>
                    <Td>{new Date(topic.info.updatedAt * 1000).toLocaleString()}</Td>
                  </Tr>
                </Tbody>
              </Table>
            </Box>
          </TabPanel>
          <TabPanel>
            <Box bg="white" p={4} borderRadius="md" boxShadow="sm">
              <Heading size="md" mb={4}>
                Partitions
              </Heading>
              <Table variant="simple">
                <Thead>
                  <Tr>
                    <Th>Partition ID</Th>
                    <Th>Leader</Th>
                    <Th>Replicas</Th>
                    <Th>In-Sync Replicas</Th>
                  </Tr>
                </Thead>
                <Tbody>
                  {topic.partitions.map((partition) => (
                    <Tr key={partition.id}>
                      <Td>{partition.id}</Td>
                      <Td>{partition.leader}</Td>
                      <Td>{partition.replicas.join(', ')}</Td>
                      <Td>{partition.isr.join(', ')}</Td>
                    </Tr>
                  ))}
                </Tbody>
              </Table>
            </Box>
          </TabPanel>
        </TabPanels>
      </Tabs>

      {/* Delete Confirmation Dialog */}
      <AlertDialog
        isOpen={showDeleteConfirm}
        leastDestructiveRef={cancelRef}
        onClose={() => setShowDeleteConfirm(false)}
      >
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Delete Topic
            </AlertDialogHeader>

            <AlertDialogBody>
              Are you sure you want to delete the topic "{topicName}"? This action cannot be undone.
            </AlertDialogBody>

            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={() => setShowDeleteConfirm(false)}>
                Cancel
              </Button>
              <Button colorScheme="red" onClick={handleDeleteTopic} ml={3} isLoading={isDeleting}>
                Delete
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </Container>
  );
}
