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
  Input,
  FormHelperText,
  useToast,
  Stack,
  Switch,
  FormErrorMessage,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
} from '@chakra-ui/react';
import { createPushTopic } from '../../../lib/data_fetching';

interface CreateTopicProps {
  isOpen: boolean;
  onClose: () => void;
  connectionId: string;
  onSuccess: () => void;
}

export const CreateTopic: React.FC<CreateTopicProps> = ({
  isOpen,
  onClose,
  connectionId,
  onSuccess,
}) => {
  const [topicName, setTopicName] = useState('');
  const [retentionPeriod, setRetentionPeriod] = useState(7);
  const [enableCompression, setEnableCompression] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [nameError, setNameError] = useState('');
  const toast = useToast();

  const validateTopicName = (name: string) => {
    if (!name.trim()) {
      setNameError('Topic name is required');
      return false;
    }
    
    if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
      setNameError('Topic name can only contain letters, numbers, underscores, and hyphens');
      return false;
    }
    
    setNameError('');
    return true;
  };

  const handleSubmit = async () => {
    if (!validateTopicName(topicName)) {
      return;
    }

    setIsSubmitting(true);
    try {
      await createPushTopic(connectionId, topicName, {
        retention_period: retentionPeriod * 24 * 60 * 60, // Convert days to seconds
        compression: enableCompression,
      });
      
      toast({
        title: 'Topic created',
        description: `Topic "${topicName}" has been created successfully.`,
        status: 'success',
        duration: 5000,
        isClosable: true,
      });
      
      // Reset form
      setTopicName('');
      setRetentionPeriod(7);
      setEnableCompression(false);
      
      // Close modal and notify parent
      onClose();
      onSuccess();
    } catch (error) {
      toast({
        title: 'Error',
        description: `Failed to create topic: ${error}`,
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleClose = () => {
    setTopicName('');
    setRetentionPeriod(7);
    setEnableCompression(false);
    setNameError('');
    onClose();
  };

  return (
    <Modal isOpen={isOpen} onClose={handleClose}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>Create New Topic</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <Stack spacing={4}>
            <FormControl isRequired isInvalid={!!nameError}>
              <FormLabel>Topic Name</FormLabel>
              <Input
                placeholder="my-topic"
                value={topicName}
                onChange={(e) => {
                  setTopicName(e.target.value);
                  validateTopicName(e.target.value);
                }}
              />
              {!nameError ? (
                <FormHelperText>
                  Enter a unique name for your topic. This will be used in the URL path.
                </FormHelperText>
              ) : (
                <FormErrorMessage>{nameError}</FormErrorMessage>
              )}
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
                How long messages should be retained in this topic.
              </FormHelperText>
            </FormControl>

            <FormControl display="flex" alignItems="center">
              <FormLabel htmlFor="enable-compression" mb="0">
                Enable Compression
              </FormLabel>
              <Switch
                id="enable-compression"
                isChecked={enableCompression}
                onChange={(e) => setEnableCompression(e.target.checked)}
              />
            </FormControl>
          </Stack>
        </ModalBody>

        <ModalFooter>
          <Button variant="ghost" mr={3} onClick={handleClose}>
            Cancel
          </Button>
          <Button
            colorScheme="blue"
            onClick={handleSubmit}
            isLoading={isSubmitting}
            loadingText="Creating"
          >
            Create
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default CreateTopic;
