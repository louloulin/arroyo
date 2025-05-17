#!/bin/bash

# Test script for Push connector

# Create a topic
echo "Creating topic 'test-topic'..."
curl -X POST http://localhost:8000/api/v1/push/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"test-topic","schema":{"fields":[{"name":"message","type":"string"}]}}'

# Send a message to the topic
echo -e "\nSending message to 'test-topic'..."
curl -X POST http://localhost:8000/api/v1/push/test-topic \
  -H "Content-Type: application/json" \
  -d '{"message":"Hello, world!"}'

# Get topic info
echo -e "\nGetting topic info..."
curl -X GET http://localhost:8000/api/v1/push/topics/test-topic

# Get messages from the topic
echo -e "\nGetting messages from 'test-topic'..."
curl -X GET http://localhost:8000/api/v1/push/messages/test-topic

# Get metrics for the topic
echo -e "\nGetting metrics for 'test-topic'..."
curl -X GET http://localhost:8000/api/v1/push/metrics/test-topic

# Delete the topic
echo -e "\nDeleting topic 'test-topic'..."
curl -X DELETE http://localhost:8000/api/v1/push/topics/test-topic
