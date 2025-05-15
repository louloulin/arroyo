import React, { useState, useEffect } from 'react';
import {
  Box,
  Heading,
  Text,
  Stat,
  StatLabel,
  StatNumber,
  StatHelpText,
  StatGroup,
  Flex,
  Spinner,
  Button,
  useToast,
  SimpleGrid,
  Card,
  CardHeader,
  CardBody,
  Badge,
  Divider,
  Code,
  useClipboard,
  Alert,
  AlertIcon,
} from '@chakra-ui/react';
import { ArrowBackIcon, CopyIcon, CheckIcon } from '@chakra-ui/icons';
import { usePushTopicDetails } from '../../../lib/data_fetching';

interface TopicDetailsProps {
  connectionId: string;
  topicName: string;
  protocol: string;
  host: string;
  port: number;
  onBack: () => void;
}

export const TopicDetails: React.FC<TopicDetailsProps> = ({
  connectionId,
  topicName,
  protocol,
  host,
  port,
  onBack,
}) => {
  const { topicDetails, topicDetailsLoading, topicDetailsError } = usePushTopicDetails(
    connectionId,
    topicName
  );
  
  const toast = useToast();
  
  // 构建 URL
  const baseUrl = `${protocol}://${host}:${port}`;
  const pushUrl = `${baseUrl}/api/v1/push/${topicName}`;
  
  // 复制功能
  const { hasCopied, onCopy } = useClipboard(pushUrl);

  if (topicDetailsLoading) {
    return (
      <Flex justifyContent="center" alignItems="center" height="200px">
        <Spinner size="xl" />
      </Flex>
    );
  }

  if (topicDetailsError) {
    return (
      <Alert status="error">
        <AlertIcon />
        Error loading topic details: {topicDetailsError}
      </Alert>
    );
  }

  if (!topicDetails) {
    return (
      <Alert status="warning">
        <AlertIcon />
        Topic not found
      </Alert>
    );
  }

  return (
    <Box>
      <Button leftIcon={<ArrowBackIcon />} onClick={onBack} mb={6}>
        Back to Topics
      </Button>

      <Heading size="md" mb={4}>
        Topic: {topicName}
      </Heading>

      <SimpleGrid columns={{ base: 1, md: 3 }} spacing={6} mb={6}>
        <Card>
          <CardHeader>
            <Heading size="sm">Messages</Heading>
          </CardHeader>
          <CardBody>
            <Stat>
              <StatNumber>{topicDetails.messages.toLocaleString()}</StatNumber>
              <StatHelpText>Total messages</StatHelpText>
            </Stat>
          </CardBody>
        </Card>

        <Card>
          <CardHeader>
            <Heading size="sm">Created</Heading>
          </CardHeader>
          <CardBody>
            <Stat>
              <StatNumber>
                {new Date(topicDetails.created_at * 1000).toLocaleDateString()}
              </StatNumber>
              <StatHelpText>
                {new Date(topicDetails.created_at * 1000).toLocaleTimeString()}
              </StatHelpText>
            </Stat>
          </CardBody>
        </Card>

        <Card>
          <CardHeader>
            <Heading size="sm">Status</Heading>
          </CardHeader>
          <CardBody>
            <Flex alignItems="center">
              <Badge colorScheme="green" fontSize="1.2em" p={1}>
                Active
              </Badge>
            </Flex>
          </CardBody>
        </Card>
      </SimpleGrid>

      <Card mb={6}>
        <CardHeader>
          <Heading size="sm">Push Endpoint</Heading>
        </CardHeader>
        <CardBody>
          <Text mb={2}>Use this endpoint to push data to this topic:</Text>
          <Flex alignItems="center">
            <Code p={2} borderRadius="md" flex="1">
              {pushUrl}
            </Code>
            <Button
              size="sm"
              ml={2}
              leftIcon={hasCopied ? <CheckIcon /> : <CopyIcon />}
              onClick={onCopy}
            >
              {hasCopied ? 'Copied' : 'Copy'}
            </Button>
          </Flex>
        </CardBody>
      </Card>

      <Card>
        <CardHeader>
          <Heading size="sm">Configuration</Heading>
        </CardHeader>
        <CardBody>
          <SimpleGrid columns={{ base: 1, md: 2 }} spacing={4}>
            <Box>
              <Text fontWeight="bold">Retention Period:</Text>
              <Text>{topicDetails.retention_period / (24 * 60 * 60)} days</Text>
            </Box>
            <Box>
              <Text fontWeight="bold">Compression:</Text>
              <Text>{topicDetails.compression ? 'Enabled' : 'Disabled'}</Text>
            </Box>
            <Box>
              <Text fontWeight="bold">Protocol:</Text>
              <Text>{protocol.toUpperCase()}</Text>
            </Box>
            <Box>
              <Text fontWeight="bold">Last Activity:</Text>
              <Text>
                {topicDetails.last_activity
                  ? new Date(topicDetails.last_activity * 1000).toLocaleString()
                  : 'No activity yet'}
              </Text>
            </Box>
          </SimpleGrid>
        </CardBody>
      </Card>
    </Box>
  );
};

export default TopicDetails;
