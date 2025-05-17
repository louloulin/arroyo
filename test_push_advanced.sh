#!/bin/bash

# Test script for Push connector with message confirmation and retry

# Create a topic
echo "Creating topic 'test-topic'..."
curl -X POST http://localhost:8000/api/v1/push/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"test-topic","schema":{"fields":[{"name":"message","type":"string"}]}}'

# Send a message to the topic
echo -e "\nSending message to 'test-topic'..."
RESPONSE=$(curl -s -X POST http://localhost:8000/api/v1/push/test-topic \
  -H "Content-Type: application/json" \
  -d '{"message":"Hello, world!"}')

echo "Response: $RESPONSE"

# Extract message ID from response
MESSAGE_ID=$(echo $RESPONSE | jq -r '.message_id // "unknown"')
echo "Message ID: $MESSAGE_ID"

# Get message status
echo -e "\nGetting message status..."
curl -X GET http://localhost:8000/api/v1/push/messages/test-topic \
  | jq ".[] | select(.id == $MESSAGE_ID)"

# Send a message that will fail validation (too large)
echo -e "\nSending a message that will fail validation (too large)..."
LARGE_MESSAGE=$(printf '{"message":"%s"}' $(printf 'x%.0s' {1..2000000}))
RESPONSE=$(curl -s -X POST http://localhost:8000/api/v1/push/test-topic \
  -H "Content-Type: application/json" \
  -d "$LARGE_MESSAGE")

echo "Response: $RESPONSE"

# Extract message ID from response
MESSAGE_ID=$(echo $RESPONSE | jq -r '.message_id // "unknown"')
echo "Message ID: $MESSAGE_ID"

# Get message status
echo -e "\nGetting message status for failed message..."
curl -X GET http://localhost:8000/api/v1/push/messages/test-topic \
  | jq ".[] | select(.id == $MESSAGE_ID)"

# Get metrics for the topic
echo -e "\nGetting metrics for 'test-topic'..."
curl -X GET http://localhost:8000/api/v1/push/metrics/test-topic

# Delete the topic
echo -e "\nDeleting topic 'test-topic'..."
curl -X DELETE http://localhost:8000/api/v1/push/topics/test-topic
