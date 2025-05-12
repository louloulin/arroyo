import React, { useState } from 'react';
import {
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalFooter,
  ModalBody,
  ModalCloseButton,
  Button,
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
  Box,
} from '@chakra-ui/react';
import { createTopic } from '../lib/data_fetching';
import { formatError } from '../lib/util';

interface CreateTopicModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: () => void;
}

export default function CreateTopicModal({ isOpen, onClose, onSuccess }: CreateTopicModalProps) {
  const toast = useToast();
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

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

  // Reset form when modal opens/closes
  React.useEffect(() => {
    if (!isOpen) {
      setFormData({
        name: '',
        partitions: 1,
        replicationFactor: 1,
        retentionMs: null,
        retentionBytes: null,
        cleanupPolicy: 'delete',
        maxMessageBytes: null,
        description: '',
      });
      setError(null);
    }
  }, [isOpen]);

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
    setError(null);

    try {
      await createTopic({
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
        title: 'Topic created',
        description: `Topic "${formData.name}" was created successfully`,
        status: 'success',
        duration: 5000,
        isClosable: true,
      });

      onSuccess();
    } catch (err) {
      setError(formatError(err));
      setIsSubmitting(false);
    }
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} size="lg">
      <ModalOverlay />
      <ModalContent>
        <form onSubmit={handleSubmit}>
          <ModalHeader>Create New Topic</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            {error && (
              <Box p={3} mb={4} bg="red.50" color="red.800" borderRadius="md">
                {error}
              </Box>
            )}

            <FormControl id="name" isRequired mb={4}>
              <FormLabel>Topic Name</FormLabel>
              <Input
                name="name"
                value={formData.name}
                onChange={handleChange}
                placeholder="Enter topic name"
              />
              <FormHelperText>
                Topic name must be unique and cannot be changed after creation
              </FormHelperText>
            </FormControl>

            <FormControl id="partitions" isRequired mb={4}>
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
              <FormHelperText>
                Number of partitions for the topic. More partitions allow for higher parallelism.
              </FormHelperText>
            </FormControl>

            <FormControl id="replicationFactor" isRequired mb={4}>
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
              <FormHelperText>
                Number of replicas for each partition. Higher values increase fault tolerance.
              </FormHelperText>
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
          </ModalBody>

          <ModalFooter>
            <Button variant="ghost" mr={3} onClick={onClose}>
              Cancel
            </Button>
            <Button
              type="submit"
              colorScheme="blue"
              isLoading={isSubmitting}
            >
              Create Topic
            </Button>
          </ModalFooter>
        </form>
      </ModalContent>
    </Modal>
  );
}
