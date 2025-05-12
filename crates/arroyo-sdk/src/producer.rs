use crate::client::ArroyoClient;
use crate::error::Result;
use crate::models::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;

/// 发送结果
#[derive(Debug, Clone)]
pub struct SendResult {
    /// 消息是否发送成功
    pub success: bool,
    /// 错误信息（如果发送失败）
    pub error: Option<String>,
    /// 发送耗时（毫秒）
    pub latency_ms: u64,
    /// 消息大小（字节）
    pub size_bytes: usize,
    /// 分区 ID
    pub partition: Option<u32>,
    /// 偏移量
    pub offset: Option<u64>,
}

/// 回调函数类型
pub type SendCallback = Box<dyn FnOnce(SendResult) + Send + 'static>;

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
    /// 发送超时（毫秒）
    pub send_timeout_ms: u64,
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
            send_timeout_ms: 30000, // 默认 30 秒超时
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

    /// 设置发送超时（毫秒）
    pub fn send_timeout_ms(mut self, send_timeout_ms: u64) -> Self {
        self.options.send_timeout_ms = send_timeout_ms;
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

    /// 同步发送消息
    ///
    /// 此方法会阻塞直到消息发送成功或失败
    pub async fn send(&self, key: Option<Vec<u8>>, value: Vec<u8>) -> Result<SendResult> {
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

    /// 同步发送消息（带头部）
    ///
    /// 此方法会阻塞直到消息发送成功或失败
    pub async fn send_with_headers(
        &self,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        headers: HashMap<String, Vec<u8>>,
    ) -> Result<SendResult> {
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

    /// 异步发送消息
    ///
    /// 此方法不会阻塞，而是立即返回。可以通过回调函数获取发送结果。
    pub fn send_async(
        &self,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        callback: Option<SendCallback>,
    ) {
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

        self.send_message_async(message, callback);
    }

    /// 异步发送消息（带头部）
    ///
    /// 此方法不会阻塞，而是立即返回。可以通过回调函数获取发送结果。
    pub fn send_with_headers_async(
        &self,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        headers: HashMap<String, Vec<u8>>,
        callback: Option<SendCallback>,
    ) {
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

        self.send_message_async(message, callback);
    }

    /// 同步发送消息对象
    pub async fn send_message(&self, message: Message) -> Result<SendResult> {
        let start_time = SystemTime::now();
        let message_size = message.value.len() + message.key.as_ref().map_or(0, |k| k.len());

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

        let result = self.client.handle_empty_response(response).await;
        let end_time = SystemTime::now();
        let latency = end_time.duration_since(start_time).unwrap_or(Duration::from_secs(0));

        match result {
            Ok(()) => Ok(SendResult {
                success: true,
                error: None,
                latency_ms: latency.as_millis() as u64,
                size_bytes: message_size,
                partition: None, // 服务器没有返回分区信息
                offset: None,    // 服务器没有返回偏移量信息
            }),
            Err(e) => Ok(SendResult {
                success: false,
                error: Some(e.to_string()),
                latency_ms: latency.as_millis() as u64,
                size_bytes: message_size,
                partition: None,
                offset: None,
            }),
        }
    }

    /// 异步发送消息对象
    pub fn send_message_async(&self, message: Message, callback: Option<SendCallback>) {
        let client = self.client.clone();
        let topic = self.topic.clone();
        let message_size = message.value.len() + message.key.as_ref().map_or(0, |k| k.len());
        let start_time = SystemTime::now();

        tokio::spawn(async move {
            let response = client
                .client
                .post(&format!("{}/api/topics/{}/messages", client.base_url, topic))
                .json(&message)
                .send()
                .await;

            let end_time = SystemTime::now();
            let latency = end_time.duration_since(start_time).unwrap_or(Duration::from_secs(0));

            let result = match response {
                Ok(resp) => client.handle_empty_response(resp).await,
                Err(e) => Err(e.into()),
            };

            let send_result = match result {
                Ok(()) => SendResult {
                    success: true,
                    error: None,
                    latency_ms: latency.as_millis() as u64,
                    size_bytes: message_size,
                    partition: None,
                    offset: None,
                },
                Err(e) => SendResult {
                    success: false,
                    error: Some(e.to_string()),
                    latency_ms: latency.as_millis() as u64,
                    size_bytes: message_size,
                    partition: None,
                    offset: None,
                },
            };

            if let Some(cb) = callback {
                cb(send_result);
            }
        });
    }

    /// 同步批量发送消息
    pub async fn send_batch(&self, messages: Vec<Message>) -> Result<Vec<SendResult>> {
        let start_time = SystemTime::now();

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

        let result = self.client.handle_empty_response(response).await;
        let end_time = SystemTime::now();
        let latency = end_time.duration_since(start_time).unwrap_or(Duration::from_secs(0));

        match result {
            Ok(()) => {
                // 创建每条消息的发送结果
                let results = messages
                    .iter()
                    .map(|m| {
                        let message_size = m.value.len() + m.key.as_ref().map_or(0, |k| k.len());
                        SendResult {
                            success: true,
                            error: None,
                            latency_ms: latency.as_millis() as u64,
                            size_bytes: message_size,
                            partition: None,
                            offset: None,
                        }
                    })
                    .collect();
                Ok(results)
            }
            Err(e) => {
                // 所有消息都失败
                let error_message = e.to_string();
                let results = messages
                    .iter()
                    .map(|m| {
                        let message_size = m.value.len() + m.key.as_ref().map_or(0, |k| k.len());
                        SendResult {
                            success: false,
                            error: Some(error_message.clone()),
                            latency_ms: latency.as_millis() as u64,
                            size_bytes: message_size,
                            partition: None,
                            offset: None,
                        }
                    })
                    .collect();
                Ok(results)
            }
        }
    }

    /// 异步批量发送消息
    pub fn send_batch_async(
        &self,
        messages: Vec<Message>,
        callback: Option<Box<dyn FnOnce(Vec<SendResult>) + Send + 'static>>,
    ) {
        let client = self.client.clone();
        let topic = self.topic.clone();
        let start_time = SystemTime::now();

        tokio::spawn(async move {

            let response = client
                .client
                .post(&format!(
                    "{}/api/topics/{}/messages/batch",
                    client.base_url, topic
                ))
                .json(&messages)
                .send()
                .await;

            let end_time = SystemTime::now();
            let latency = end_time.duration_since(start_time).unwrap_or(Duration::from_secs(0));

            let result = match response {
                Ok(resp) => client.handle_empty_response(resp).await,
                Err(e) => Err(e.into()),
            };

            let send_results = match result {
                Ok(()) => {
                    // 创建每条消息的发送结果
                    messages
                        .iter()
                        .map(|m| {
                            let message_size = m.value.len() + m.key.as_ref().map_or(0, |k| k.len());
                            SendResult {
                                success: true,
                                error: None,
                                latency_ms: latency.as_millis() as u64,
                                size_bytes: message_size,
                                partition: None,
                                offset: None,
                            }
                        })
                        .collect()
                }
                Err(e) => {
                    // 所有消息都失败
                    let error_message = e.to_string();
                    messages
                        .iter()
                        .map(|m| {
                            let message_size = m.value.len() + m.key.as_ref().map_or(0, |k| k.len());
                            SendResult {
                                success: false,
                                error: Some(error_message.clone()),
                                latency_ms: latency.as_millis() as u64,
                                size_bytes: message_size,
                                partition: None,
                                offset: None,
                            }
                        })
                        .collect()
                }
            };

            if let Some(cb) = callback {
                cb(send_results);
            }
        });
    }
}
