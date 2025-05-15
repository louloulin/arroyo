import { ConnectionSchema } from "../lib/data_fetching";

// 认证配置类型
export type AuthenticationNone = {
  type: 'none';
};

export type AuthenticationApiKey = {
  type: 'api_key';
  api_key: string;
};

export type AuthenticationOAuth = {
  type: 'oauth';
  client_id: string;
  client_secret: string;
  token_url: string;
};

export type Authentication = 
  | AuthenticationNone
  | AuthenticationApiKey
  | AuthenticationOAuth;

// HTTP 配置类型
export type HttpConfig = {
  timeout: string;
  max_connections: string;
};

// gRPC 配置类型
export type GrpcConfig = {
  max_message_size: string;
  keepalive_time: string;
};

// WebSocket 配置类型
export type WebSocketConfig = {
  max_frame_size: string;
  heartbeat_interval: string;
};

// QUIC 配置类型
export type QuicConfig = {
  max_concurrent_streams: string;
  idle_timeout: string;
};

// 协议类型
export type Protocol = 'http' | 'grpc' | 'websocket' | 'quic';

// Push 表配置类型
export type PushTableConfig = {
  topic: string;
  protocol: Protocol;
  retention_period?: string;
  http_config?: HttpConfig;
  grpc_config?: GrpcConfig;
  websocket_config?: WebSocketConfig;
  quic_config?: QuicConfig;
  compression?: 'none' | 'gzip' | 'lz4' | 'zstd';
  batch_size?: string;
};

// Push 配置类型
export type PushConfig = {
  name: string;
  type: 'push';
  buffer_size: string;
  max_batch_size: string;
  authentication: Authentication;
};

// 创建连接状态类型
export type PushConnectionState = {
  name?: string;
  connectionProfileId: string | null;
  table: PushTableConfig | null;
  schema: ConnectionSchema | null;
};
