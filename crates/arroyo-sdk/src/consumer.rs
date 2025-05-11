use crate::client::ArroyoClient;
use crate::error::Result;
use crate::models::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// 消费者配置选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerOptions {
    /// 消费者组 ID
    pub group_id: String,
    /// 客户端 ID
    pub client_id: String,
    /// 自动提交
    pub auto_commit: bool,
    /// 自动提交间隔（毫秒）
    pub auto_commit_interval_ms: u64,
    /// 会话超时（毫秒）
    pub session_timeout_ms: u64,
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: u64,
    /// 偏移量重置策略
    pub auto_offset_reset: String,
    /// 最大拉取字节数
    pub max_partition_fetch_bytes: u32,
    /// 最大拉取等待时间（毫秒）
    pub fetch_max_wait_ms: u64,
    /// 其他配置选项
    pub config: Option<HashMap<String, String>>,
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            group_id: format!("arroyo-consumer-{}", uuid::Uuid::new_v4()),
            client_id: format!("arroyo-consumer-{}", uuid::Uuid::new_v4()),
            auto_commit: true,
            auto_commit_interval_ms: 5000,
            session_timeout_ms: 30000,
            heartbeat_interval_ms: 3000,
            auto_offset_reset: "latest".to_string(),
            max_partition_fetch_bytes: 1048576,
            fetch_max_wait_ms: 500,
            config: None,
        }
    }
}

/// 消费者构建器，用于流畅的 API 设计
pub struct ConsumerBuilder {
    options: ConsumerOptions,
}

impl ConsumerBuilder {
    /// 创建新的消费者构建器
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            options: ConsumerOptions {
                group_id: group_id.into(),
                ..Default::default()
            },
        }
    }

    /// 设置客户端 ID
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.options.client_id = client_id.into();
        self
    }

    /// 设置自动提交
    pub fn auto_commit(mut self, auto_commit: bool) -> Self {
        self.options.auto_commit = auto_commit;
        self
    }

    /// 设置自动提交间隔（毫秒）
    pub fn auto_commit_interval_ms(mut self, auto_commit_interval_ms: u64) -> Self {
        self.options.auto_commit_interval_ms = auto_commit_interval_ms;
        self
    }

    /// 设置会话超时（毫秒）
    pub fn session_timeout_ms(mut self, session_timeout_ms: u64) -> Self {
        self.options.session_timeout_ms = session_timeout_ms;
        self
    }

    /// 设置心跳间隔（毫秒）
    pub fn heartbeat_interval_ms(mut self, heartbeat_interval_ms: u64) -> Self {
        self.options.heartbeat_interval_ms = heartbeat_interval_ms;
        self
    }

    /// 设置偏移量重置策略
    pub fn auto_offset_reset(mut self, auto_offset_reset: impl Into<String>) -> Self {
        self.options.auto_offset_reset = auto_offset_reset.into();
        self
    }

    /// 设置最大拉取字节数
    pub fn max_partition_fetch_bytes(mut self, max_partition_fetch_bytes: u32) -> Self {
        self.options.max_partition_fetch_bytes = max_partition_fetch_bytes;
        self
    }

    /// 设置最大拉取等待时间（毫秒）
    pub fn fetch_max_wait_ms(mut self, fetch_max_wait_ms: u64) -> Self {
        self.options.fetch_max_wait_ms = fetch_max_wait_ms;
        self
    }

    /// 添加配置选项
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let config = self.options.config.get_or_insert_with(HashMap::new);
        config.insert(key.into(), value.into());
        self
    }

    /// 构建 ConsumerOptions
    pub fn build(self) -> ConsumerOptions {
        self.options
    }
}

/// 消费者，用于从 Topic 消费消息
pub struct Consumer {
    /// 客户端
    client: ArroyoClient,
    /// Topic 名称
    topic: String,
    /// 消费者配置
    options: ConsumerOptions,
}

impl Consumer {
    /// 创建新的消费者
    pub fn new(client: ArroyoClient, topic: impl Into<String>, options: ConsumerOptions) -> Self {
        Self {
            client,
            topic: topic.into(),
            options,
        }
    }

    /// 拉取消息
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<Message>> {
        let response = self
            .client
            .client
            .get(&format!(
                "{}/api/topics/{}/messages",
                self.client.base_url, self.topic
            ))
            .query(&[
                ("group_id", &self.options.group_id),
                ("timeout_ms", &timeout.as_millis().to_string()),
                ("max_bytes", &self.options.max_partition_fetch_bytes.to_string()),
            ])
            .send()
            .await?;

        self.client.handle_response(response).await
    }

    /// 提交偏移量
    pub async fn commit(&self, partition: u32, offset: u64) -> Result<()> {
        let response = self
            .client
            .client
            .post(&format!(
                "{}/api/topics/{}/offsets",
                self.client.base_url, self.topic
            ))
            .json(&serde_json::json!({
                "group_id": self.options.group_id,
                "partition": partition,
                "offset": offset
            }))
            .send()
            .await?;

        self.client.handle_empty_response(response).await
    }

    /// 批量提交偏移量
    pub async fn commit_batch(&self, offsets: HashMap<u32, u64>) -> Result<()> {
        let mut batch = Vec::new();
        for (partition, offset) in offsets {
            batch.push(serde_json::json!({
                "group_id": self.options.group_id,
                "partition": partition,
                "offset": offset
            }));
        }

        let response = self
            .client
            .client
            .post(&format!(
                "{}/api/topics/{}/offsets/batch",
                self.client.base_url, self.topic
            ))
            .json(&batch)
            .send()
            .await?;

        self.client.handle_empty_response(response).await
    }

    /// 获取当前偏移量
    pub async fn get_offsets(&self) -> Result<HashMap<u32, u64>> {
        let response = self
            .client
            .client
            .get(&format!(
                "{}/api/topics/{}/offsets",
                self.client.base_url, self.topic
            ))
            .query(&[("group_id", &self.options.group_id)])
            .send()
            .await?;

        self.client.handle_response(response).await
    }
}
