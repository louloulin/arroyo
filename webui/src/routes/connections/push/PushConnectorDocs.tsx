import React from 'react';
import {
  Box,
  Heading,
  Text,
  Link,
  List,
  ListItem,
  ListIcon,
  Code,
  Divider,
  Button,
  Flex,
  Badge,
} from '@chakra-ui/react';
import { ExternalLinkIcon, InfoIcon, CheckCircleIcon } from '@chakra-ui/icons';

export const PushConnectorDocs: React.FC = () => {
  return (
    <Box>
      <Heading size="md" mb={4}>
        Push Connector Documentation
      </Heading>
      
      <Text mb={4}>
        The Push Connector allows external systems to push data directly to Arroyo.
        It supports multiple protocols and provides a flexible way to ingest data from various sources.
      </Text>
      
      <Heading size="sm" mb={2}>
        Supported Protocols
      </Heading>
      
      <List spacing={3} mb={4}>
        <ListItem>
          <ListIcon as={CheckCircleIcon} color="green.500" />
          <Badge colorScheme="green" mr={2}>HTTP</Badge>
          Simple REST API for pushing data
        </ListItem>
        <ListItem>
          <ListIcon as={CheckCircleIcon} color="green.500" />
          <Badge colorScheme="blue" mr={2}>gRPC</Badge>
          High-performance RPC framework
        </ListItem>
        <ListItem>
          <ListIcon as={CheckCircleIcon} color="green.500" />
          <Badge colorScheme="purple" mr={2}>WebSocket</Badge>
          Bidirectional communication channel
        </ListItem>
        <ListItem>
          <ListIcon as={CheckCircleIcon} color="green.500" />
          <Badge colorScheme="orange" mr={2}>QUIC</Badge>
          Next-generation transport protocol
        </ListItem>
      </List>
      
      <Heading size="sm" mb={2}>
        SQL Example
      </Heading>
      
      <Code p={4} borderRadius="md" mb={4} display="block" whiteSpace="pre">
{`CREATE TABLE http_events (
    id STRING,
    data STRING,
    _timestamp TIMESTAMP
) WITH (
    connector = 'push',
    protocol = 'http',
    topic = 'events',
    format = 'json'
);`}
      </Code>
      
      <Divider my={6} />
      
      <Heading size="sm" mb={2}>
        Client Libraries
      </Heading>
      
      <List spacing={3} mb={4}>
        <ListItem>
          <ListIcon as={InfoIcon} color="blue.500" />
          <Link href="https://doc.arroyo.dev/connectors/push/rust-client" isExternal>
            Rust Client <ExternalLinkIcon mx="2px" />
          </Link>
        </ListItem>
        <ListItem>
          <ListIcon as={InfoIcon} color="blue.500" />
          <Link href="https://doc.arroyo.dev/connectors/push/python-client" isExternal>
            Python Client <ExternalLinkIcon mx="2px" />
          </Link>
        </ListItem>
        <ListItem>
          <ListIcon as={InfoIcon} color="blue.500" />
          <Link href="https://doc.arroyo.dev/connectors/push/js-client" isExternal>
            JavaScript/TypeScript Client <ExternalLinkIcon mx="2px" />
          </Link>
        </ListItem>
      </List>
      
      <Flex justifyContent="center" mt={6}>
        <Button
          colorScheme="blue"
          rightIcon={<ExternalLinkIcon />}
          onClick={() => window.open('https://doc.arroyo.dev/connectors/push', '_blank')}
        >
          View Full Documentation
        </Button>
      </Flex>
    </Box>
  );
};

export default PushConnectorDocs;
