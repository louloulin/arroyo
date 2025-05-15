import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createPushTopic, deletePushTopic } from '../lib/data_fetching';

// 模拟 fetch
global.fetch = vi.fn();

describe('Push API Functions', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('createPushTopic', () => {
    it('should create a topic via the API', async () => {
      // 设置 fetch 模拟
      (global.fetch as any).mockResolvedValueOnce({
        ok: true,
        json: async () => ({ success: true }),
      });

      // 模拟 globalMutate
      vi.stubGlobal('globalMutate', vi.fn());

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

      // 模拟 globalMutate
      vi.stubGlobal('globalMutate', vi.fn());

      // 调用 API 函数
      await deletePushTopic('connection-123', 'topic-to-delete');

      // 验证 fetch 调用
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/push/topics/topic-to-delete', {
        method: 'DELETE',
      });
    });
  });
});
