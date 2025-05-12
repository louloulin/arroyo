import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  Container,
  Flex,
  Heading,
  HStack,
  Icon,
  FormControl,
  FormLabel,
  FormHelperText,
  Input,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
  Select,
  Textarea,
  useToast,
  Spinner,
} from '@chakra-ui/react';
import { FiArrowLeft, FiSave } from 'react-icons/fi';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { useNavbar } from '../../App';
import { formatError } from '../../lib/util';
import { useTopicDetails, updateTopic } from '../../lib/data_fetching';

export function EditTopic() {
  const { topicName } = useParams<{ topicName: string }>();
  const navigate = useNavigate();
  const toast = useToast();
  const [isSubmitting, setIsSubmitting] = useState(false);

  useNavbar({ title: `Edit Topic: ${topicName}` });

  // Fetch topic details
  const { data: topic, isLoading, error } = useTopicDetails(topicName || '');

  // Form state
  const [formData, setFormData] = useState({
    name: '',
    partitions: 1,
    replicationFactor: 1,
    retentionMs: null as number | null,
    retentionBytes: null as number | null,
    cleanupPolicy: 'delete',
    maxMessageBytes: null as number | null,
    description: '',
  });

  // Initialize form with topic data
  useEffect(() => {
    if (topic) {
      setFormData({
        name: topic.info.name,
        partitions: topic.info.partitions,
        replicationFactor: topic.info.replicationFactor,
        retentionMs: topic.info.retentionMs,
        retentionBytes: topic.info.retentionBytes,
        cleanupPolicy: topic.info.cleanupPolicy,
        maxMessageBytes: topic.info.maxMessageBytes,
        description: topic.info.description || '',
      });
    }
  }, [topic]);

  // Handle form changes
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleNumberChange = (name: string, value: number | null) => {
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  // Handle form submission
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsSubmitting(true);

    try {
      await updateTopic({
        config: {
          name: formData.name,
          partitions: formData.partitions,
          replicationFactor: formData.replicationFactor,
          retentionMs: formData.retentionMs,
          retentionBytes: formData.retentionBytes,
          cleanupPolicy: formData.cleanupPolicy,
          maxMessageBytes: formData.maxMessageBytes,
          description: formData.description || undefined,
        }
      });

      toast({
        title: 'Topic updated',
        description: `Topic "${formData.name}" was updated successfully`,
        status: 'success',
        duration: 5000,
        isClosable: true,
      });

      navigate(`/topics/${formData.name}`);
    } catch (err) {
      toast({
        title: 'Error updating topic',
        description: formatError(err),
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
      setIsSubmitting(false);
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
          <Heading size="md" mb={2}>Error loading topic</Heading>
          <pre>{formatError(error)}</pre>
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
            to={`/topics/${topicName}`}
            variant="ghost"
            leftIcon={<Icon as={FiArrowLeft} />}
            mr={2}
          >
            Back
          </Button>
          <Heading size="lg">Edit Topic: {topicName}</Heading>
        </HStack>
      </Flex>

      <Box bg="white" p={6} borderRadius="md" boxShadow="sm">
        <form onSubmit={handleSubmit}>
          <FormControl id="name" isRequired mb={4} isDisabled>
            <FormLabel>Topic Name</FormLabel>
            <Input
              name="name"
              value={formData.name}
              onChange={handleChange}
              placeholder="Enter topic name"
            />
            <FormHelperText>Topic name cannot be changed after creation</FormHelperText>
          </FormControl>

          <FormControl id="partitions" isRequired mb={4} isDisabled>
            <FormLabel>Partitions</FormLabel>
            <NumberInput
              min={1}
              value={formData.partitions}
              onChange={(_, value) => handleNumberChange('partitions', value)}
            >
              <NumberInputField />
              <NumberInputStepper>
                <NumberIncrementStepper />
                <NumberDecrementStepper />
              </NumberInputStepper>
            </NumberInput>
            <FormHelperText>Number of partitions cannot be changed after creation</FormHelperText>
          </FormControl>

          <FormControl id="replicationFactor" isRequired mb={4} isDisabled>
            <FormLabel>Replication Factor</FormLabel>
            <NumberInput
              min={1}
              value={formData.replicationFactor}
              onChange={(_, value) => handleNumberChange('replicationFactor', value)}
            >
              <NumberInputField />
              <NumberInputStepper>
                <NumberIncrementStepper />
                <NumberDecrementStepper />
              </NumberInputStepper>
            </NumberInput>
            <FormHelperText>Replication factor cannot be changed after creation</FormHelperText>
          </FormControl>

          <FormControl id="cleanupPolicy" mb={4}>
            <FormLabel>Cleanup Policy</FormLabel>
            <Select
              name="cleanupPolicy"
              value={formData.cleanupPolicy}
              onChange={handleChange}
            >
              <option value="delete">Delete</option>
              <option value="compact">Compact</option>
              <option value="compact,delete">Compact and Delete</option>
            </Select>
            <FormHelperText>
              Delete: Remove old messages based on retention settings
              <br />
              Compact: Keep only the latest message for each key
            </FormHelperText>
          </FormControl>

          <FormControl id="retentionMs" mb={4}>
            <FormLabel>Retention Time (ms)</FormLabel>
            <NumberInput
              min={0}
              value={formData.retentionMs === null ? '' : formData.retentionMs}
              onChange={(_, value) => handleNumberChange('retentionMs', value || null)}
            >
              <NumberInputField />
              <NumberInputStepper>
                <NumberIncrementStepper />
                <NumberDecrementStepper />
              </NumberInputStepper>
            </NumberInput>
            <FormHelperText>
              How long messages should be retained (in milliseconds). Leave empty for unlimited.
            </FormHelperText>
          </FormControl>

          <FormControl id="retentionBytes" mb={4}>
            <FormLabel>Retention Size (bytes)</FormLabel>
            <NumberInput
              min={0}
              value={formData.retentionBytes === null ? '' : formData.retentionBytes}
              onChange={(_, value) => handleNumberChange('retentionBytes', value || null)}
            >
              <NumberInputField />
              <NumberInputStepper>
                <NumberIncrementStepper />
                <NumberDecrementStepper />
              </NumberInputStepper>
            </NumberInput>
            <FormHelperText>
              Maximum size of the topic (in bytes). Leave empty for unlimited.
            </FormHelperText>
          </FormControl>

          <FormControl id="maxMessageBytes" mb={4}>
            <FormLabel>Max Message Size (bytes)</FormLabel>
            <NumberInput
              min={0}
              value={formData.maxMessageBytes === null ? '' : formData.maxMessageBytes}
              onChange={(_, value) => handleNumberChange('maxMessageBytes', value || null)}
            >
              <NumberInputField />
              <NumberInputStepper>
                <NumberIncrementStepper />
                <NumberDecrementStepper />
              </NumberInputStepper>
            </NumberInput>
            <FormHelperText>
              Maximum size of a message. Leave empty for default.
            </FormHelperText>
          </FormControl>

          <FormControl id="description" mb={4}>
            <FormLabel>Description</FormLabel>
            <Textarea
              name="description"
              value={formData.description}
              onChange={handleChange}
              placeholder="Enter topic description"
              rows={3}
            />
          </FormControl>

          <Flex justifyContent="flex-end">
            <Button
              type="submit"
              colorScheme="blue"
              leftIcon={<Icon as={FiSave} />}
              isLoading={isSubmitting}
            >
              Save Changes
            </Button>
          </Flex>
        </form>
      </Box>
    </Container>
  );
}
