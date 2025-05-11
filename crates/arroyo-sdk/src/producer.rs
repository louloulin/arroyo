use crate::client::ArroyoClient;
use crate::error::Result;
use crate::models::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// 生产者配置选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerOptions {
    /// 客户端 ID
    pub client_id: String,
    /// 批处理大小
    pub batch_size: usize,
    /// 批处理延迟（毫秒）
    pub batch_delay_ms: u64,
    /// 压缩类型
    pub compression_type: Option<String>,
    /// 确认级别
    pub acks: String,
    /// 重试次数
    pub retries: u32,
    /// 重试间隔（毫秒）
    pub retry_backoff_ms: u64,
    /// 其他配置选项
    pub config: Option<HashMap<String, String>>,
}

impl Default for ProducerOptions {
    fn default() -> Self {
        Self {
            client_id: format!("arroyo-producer-{}", uuid::Uuid::new_v4()),
            batch_size: 16384,
            batch_delay_ms: 5,
            compression_type: None,
            acks: "all".to_string(),
            retries: 3,
            retry_backoff_ms: 100,
            config: None,
        }
    }
}

/// 生产者构建器，用于流畅的 API 设计
pub struct ProducerBuilder {
    options: ProducerOptions,
}

impl ProducerBuilder {
    /// 创建新的生产者构建器
    pub fn new() -> Self {
        Self {
            options: ProducerOptions::default(),
        }
    }

    /// 设置客户端 ID
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.options.client_id = client_id.into();
        self
    }

    /// 设置批处理大小
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.options.batch_size = batch_size;
        self
    }

    /// 设置批处理延迟（毫秒）
    pub fn batch_delay_ms(mut self, batch_delay_ms: u64) -> Self {
        self.options.batch_delay_ms = batch_delay_ms;
        self
    }

    /// 设置压缩类型
    pub fn compression_type(mut self, compression_type: impl Into<String>) -> Self {
        self.options.compression_type = Some(compression_type.into());
        self
    }

    /// 设置确认级别
    pub fn acks(mut self, acks: impl Into<String>) -> Self {
        self.options.acks = acks.into();
        self
    }

    /// 设置重试次数
    pub fn retries(mut self, retries: u32) -> Self {
        self.options.retries = retries;
        self
    }

    /// 设置重试间隔（毫秒）
    pub fn retry_backoff_ms(mut self, retry_backoff_ms: u64) -> Self {
        self.options.retry_backoff_ms = retry_backoff_ms;
        self
    }

    /// 添加配置选项
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let config = self.options.config.get_or_insert_with(HashMap::new);
        config.insert(key.into(), value.into());
        self
    }

    /// 构建 ProducerOptions
    pub fn build(self) -> ProducerOptions {
        self.options
    }
}

/// 生产者，用于向 Topic 发送消息
pub struct Producer {
    /// 客户端
    client: ArroyoClient,
    /// Topic 名称
    topic: String,
    /// 生产者配置
    options: ProducerOptions,
}

impl Producer {
    /// 创建新的生产者
    pub fn new(client: ArroyoClient, topic: impl Into<String>, options: ProducerOptions) -> Self {
        Self {
            client,
            topic: topic.into(),
            options,
        }
    }

    /// 发送消息
    pub async fn send(&self, key: Option<Vec<u8>>, value: Vec<u8>) -> Result<()> {
        let message = Message {
            key,
            value,
            headers: None,
            timestamp: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            ),
            partition: None,
            offset: None,
        };

        self.send_message(message).await
    }

    /// 发送消息（带头部）
    pub async fn send_with_headers(
        &self,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        headers: HashMap<String, Vec<u8>>,
    ) -> Result<()> {
        let message = Message {
            key,
            value,
            headers: Some(headers),
            timestamp: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            ),
            partition: None,
            offset: None,
        };

        self.send_message(message).await
    }

    /// 发送消息对象
    pub async fn send_message(&self, message: Message) -> Result<()> {
        let response = self
            .client
            .client
            .post(&format!(
                "{}/api/topics/{}/messages",
                self.client.base_url, self.topic
            ))
            .json(&message)
            .send()
            .await?;

        self.client.handle_empty_response(response).await
    }

    /// 批量发送消息
    pub async fn send_batch(&self, messages: Vec<Message>) -> Result<()> {
        let response = self
            .client
            .client
            .post(&format!(
                "{}/api/topics/{}/messages/batch",
                self.client.base_url, self.topic
            ))
            .json(&messages)
            .send()
            .await?;

        self.client.handle_empty_response(response).await
    }
}
