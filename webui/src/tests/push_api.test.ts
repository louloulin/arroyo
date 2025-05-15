import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { usePushTopics, usePushTopicDetails, createPushTopic, deletePushTopic } from '../lib/data_fetching';

// 模拟 fetch
global.fetch = vi.fn();

describe('Push API Functions', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('usePushTopics', () => {
    it('should fetch topics from the API', async () => {
      // 模拟 fetch 响应
      const mockTopics = [
        {
          name: 'events',
          messages: 1245,
          created_at: 1620000000,
          last_activity: 1620100000,
          retention_period: 604800,
          compression: true,
        },
      ];

      // 设置 fetch 模拟
      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => mockTopics,
      });

      // 调用 API 函数
      const fetcher = usePushTopics('connection-123');
      
      // 验证 fetch 调用
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/push/topics?connectionId=connection-123');
    });

    it('should handle API errors', async () => {
      // 设置 fetch 模拟
      (global.fetch as any).mockResolvedValueOnce({
        ok: false,
        statusText: 'Not Found',
      });

      // 调用 API 函数
      const fetcher = usePushTopics('connection-123');
      
      // 验证 fetch 调用
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/push/topics?connectionId=connection-123');
    });
  });

  describe('usePushTopicDetails', () => {
    it('should fetch topic details from the API', async () => {
      // 模拟 fetch 响应
      const mockTopic = {
        name: 'events',
        messages: 1245,
        created_at: 1620000000,
        last_activity: 1620100000,
        retention_period: 604800,
        compression: true,
      };

      // 设置 fetch 模拟
      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => mockTopic,
      });

      // 调用 API 函数
      const fetcher = usePushTopicDetails('connection-123', 'events');
      
      // 验证 fetch 调用
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/push/topics/events');
    });
  });

  describe('createPushTopic', () => {
    it('should create a topic via the API', async () => {
      // 设置 fetch 模拟
      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => ({ success: true }),
      });

      // 调用 API 函数
      await createPushTopic('connection-123', 'new-topic', {
        retention_period: 604800,
        compression: true,
      });
      
      // 验证 fetch 调用
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/push/topics', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          name: 'new-topic',
          retention_period: 604800,
          compression: true,
        }),
      });
    });
  });

  describe('deletePushTopic', () => {
    it('should delete a topic via the API', async () => {
      // 设置 fetch 模拟
      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => ({ success: true }),
      });

      // 调用 API 函数
      await deletePushTopic('connection-123', 'topic-to-delete');
      
      // 验证 fetch 调用
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/push/topics/topic-to-delete', {
        method: 'DELETE',
      });
    });
  });
});
