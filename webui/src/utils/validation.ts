import { z } from 'zod';
import { 
  Authentication, 
  PushConfig, 
  PushTableConfig 
} from '../types/push';

// 认证验证模式
export const authenticationSchema = z.discriminatedUnion('type', [
  z.object({
    type: z.literal('none'),
  }),
  z.object({
    type: z.literal('api_key'),
    api_key: z.string().min(1, 'API Key is required'),
  }),
  z.object({
    type: z.literal('oauth'),
    client_id: z.string().min(1, 'Client ID is required'),
    client_secret: z.string().min(1, 'Client Secret is required'),
    token_url: z.string().url('Token URL must be a valid URL'),
  }),
]);

// HTTP 配置验证模式
export const httpConfigSchema = z.object({
  timeout: z.string().regex(/^\d+$/, 'Timeout must be a number').refine(
    (val) => parseInt(val) >= 1 && parseInt(val) <= 300,
    { message: 'Timeout must be between 1 and 300 seconds' }
  ),
  max_connections: z.string().regex(/^\d+$/, 'Max connections must be a number').refine(
    (val) => parseInt(val) >= 10 && parseInt(val) <= 10000,
    { message: 'Max connections must be between 10 and 10000' }
  ),
});

// Push 表配置验证模式
export const pushTableConfigSchema = z.object({
  topic: z.string().min(1, 'Topic name is required').regex(
    /^[a-zA-Z0-9_-]+$/,
    'Topic name can only contain letters, numbers, underscores and hyphens'
  ),
  protocol: z.enum(['http', 'grpc', 'websocket', 'quic']),
  http_config: httpConfigSchema.optional(),
  // 其他协议配置...
});

// Push 配置验证模式
export const pushConfigSchema = z.object({
  name: z.string().min(1, 'Name is required'),
  type: z.literal('push'),
  buffer_size: z.string().regex(/^\d+$/, 'Buffer size must be a number').refine(
    (val) => parseInt(val) >= 1,
    { message: 'Buffer size must be at least 1' }
  ),
  max_batch_size: z.string().regex(/^\d+$/, 'Max batch size must be a number').refine(
    (val) => parseInt(val) >= 1,
    { message: 'Max batch size must be at least 1' }
  ),
  authentication: authenticationSchema,
});

// 验证 Push 配置
export function validatePushConfig(config: any): { valid: boolean; errors?: string[] } {
  try {
    pushConfigSchema.parse(config);
    return { valid: true };
  } catch (error) {
    if (error instanceof z.ZodError) {
      return {
        valid: false,
        errors: error.errors.map(e => `${e.path.join('.')}: ${e.message}`),
      };
    }
    return { valid: false, errors: ['Unknown validation error'] };
  }
}

// 验证 Push 表配置
export function validatePushTableConfig(config: any): { valid: boolean; errors?: string[] } {
  try {
    pushTableConfigSchema.parse(config);
    return { valid: true };
  } catch (error) {
    if (error instanceof z.ZodError) {
      return {
        valid: false,
        errors: error.errors.map(e => `${e.path.join('.')}: ${e.message}`),
      };
    }
    return { valid: false, errors: ['Unknown validation error'] };
  }
}
