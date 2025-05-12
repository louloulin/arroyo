// This file contains manually added types for the topic API endpoints
// These types should be generated from the OpenAPI spec in the future

import { paths } from './api-types';

// Extend the paths type to include the topic endpoints
declare module './api-types' {
  export interface paths {
    '/v1/topics': {
      get: {
        responses: {
          200: {
            content: {
              'application/json': {
                topics: TopicInfo[];
              };
            };
          };
        };
      };
      post: {
        requestBody: {
          content: {
            'application/json': CreateTopicRequest;
          };
        };
        responses: {
          201: {
            content: {
              'application/json': TopicInfo;
            };
          };
        };
      };
    };
    '/v1/topics/{name}': {
      get: {
        parameters: {
          path: {
            name: string;
          };
        };
        responses: {
          200: {
            content: {
              'application/json': TopicDetails;
            };
          };
        };
      };
      patch: {
        parameters: {
          path: {
            name: string;
          };
        };
        requestBody: {
          content: {
            'application/json': UpdateTopicRequest;
          };
        };
        responses: {
          200: {
            content: {
              'application/json': TopicInfo;
            };
          };
        };
      };
      delete: {
        parameters: {
          path: {
            name: string;
          };
        };
        responses: {
          204: {
            content: {
              'application/json': null;
            };
          };
        };
      };
    };
    '/v1/topics/health': {
      post: {
        requestBody: {
          content: {
            'application/json': TopicHealthCheckRequest;
          };
        };
        responses: {
          200: {
            content: {
              'application/json': TopicHealthCheckResponse;
            };
          };
        };
      };
    };
  }
}

// Topic types
export interface TopicInfo {
  name: string;
  partitions: number;
  replicationFactor: number;
  retentionMs: number | null;
  retentionBytes: number | null;
  cleanupPolicy: string;
  maxMessageBytes: number | null;
  description: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface TopicPartitionInfo {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
}

export interface TopicDetails {
  info: TopicInfo;
  partitions: TopicPartitionInfo[];
  messageCount: number;
  sizeBytes: number;
}

export interface CreateTopicRequest {
  config: {
    name: string;
    partitions: number;
    replicationFactor: number;
    retentionMs?: number | null;
    retentionBytes?: number | null;
    cleanupPolicy: string;
    maxMessageBytes?: number | null;
    description?: string;
  };
}

export interface UpdateTopicRequest {
  config: {
    name: string;
    partitions: number;
    replicationFactor: number;
    retentionMs?: number | null;
    retentionBytes?: number | null;
    cleanupPolicy: string;
    maxMessageBytes?: number | null;
    description?: string;
  };
}

export interface TopicHealthCheckRequest {
  topics: string[];
}

export interface TopicHealthCheckResponse {
  healthy: boolean;
  details: string;
}
