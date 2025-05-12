use crate::client::ArroyoClient;
use crate::error::{Error, Result};
use crate::models::Message;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// 订阅类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionType {
    /// 单个 Topic 订阅
    Single(String),
    /// 多个 Topic 订阅
    Multiple(Vec<String>),
    /// 正则表达式订阅
    Pattern(String),
}

impl SubscriptionType {
    /// 创建单个 Topic 订阅
    pub fn single(topic: impl Into<String>) -> Self {
        Self::Single(topic.into())
    }

    /// 创建多个 Topic 订阅
    pub fn multiple(topics: Vec<impl Into<String>>) -> Self {
        Self::Multiple(topics.into_iter().map(|t| t.into()).collect())
    }

    /// 创建正则表达式订阅
    pub fn pattern(pattern: impl Into<String>) -> Self {
        Self::Pattern(pattern.into())
    }

    /// 检查 Topic 是否匹配订阅
    pub fn matches(&self, topic: &str) -> bool {
        match self {
            Self::Single(t) => t == topic,
            Self::Multiple(topics) => topics.contains(&topic.to_string()),
            Self::Pattern(pattern) => {
                if let Ok(regex) = Regex::new(pattern) {
                    regex.is_match(topic)
                } else {
                    false
                }
            }
        }
    }

    /// 获取所有匹配的 Topic
    pub fn get_matching_topics(&self, available_topics: &[String]) -> Vec<String> {
        match self {
            Self::Single(topic) => {
                if available_topics.contains(&topic.to_string()) {
                    vec![topic.clone()]
                } else {
                    vec![]
                }
            }
            Self::Multiple(topics) => {
                topics
                    .iter()
                    .filter(|t| available_topics.contains(t))
                    .cloned()
                    .collect()
            }
            Self::Pattern(pattern) => {
                if let Ok(regex) = Regex::new(pattern) {
                    available_topics
                        .iter()
                        .filter(|t| regex.is_match(t))
                        .cloned()
                        .collect()
                } else {
                    vec![]
                }
            }
        }
    }
}

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
    /// 订阅类型
    pub subscription_type: Option<SubscriptionType>,
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
            subscription_type: None,
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

    /// 设置单个 Topic 订阅
    pub fn subscribe(mut self, topic: impl Into<String>) -> Self {
        self.options.subscription_type = Some(SubscriptionType::single(topic));
        self
    }

    /// 设置多个 Topic 订阅
    pub fn subscribe_to(mut self, topics: Vec<impl Into<String>>) -> Self {
        self.options.subscription_type = Some(SubscriptionType::multiple(topics));
        self
    }

    /// 设置正则表达式订阅
    pub fn subscribe_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.options.subscription_type = Some(SubscriptionType::pattern(pattern));
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
    /// Topic 名称（单个订阅时使用）
    topic: Option<String>,
    /// 订阅的 Topics（多个订阅时使用）
    topics: Vec<String>,
    /// 消费者配置
    options: ConsumerOptions,
    /// 订阅类型
    subscription_type: Option<SubscriptionType>,
}

impl Consumer {
    /// 创建新的消费者
    pub fn new(client: ArroyoClient, topic: impl Into<String>, options: ConsumerOptions) -> Self {
        let topic_str = topic.into();
        let mut subscription_type = options.subscription_type.clone();

        // 如果没有设置订阅类型，则默认为单个 Topic 订阅
        if subscription_type.is_none() {
            subscription_type = Some(SubscriptionType::Single(topic_str.clone()));
        }

        // 根据订阅类型设置 topic 和 topics
        let (topic_opt, topics) = match &subscription_type {
            Some(SubscriptionType::Single(t)) => (Some(t.clone()), vec![t.clone()]),
            Some(SubscriptionType::Multiple(ts)) => (None, ts.clone()),
            Some(SubscriptionType::Pattern(_)) => (None, vec![]),
            None => (Some(topic_str.clone()), vec![topic_str.clone()]),
        };

        Self {
            client,
            topic: topic_opt,
            topics,
            options,
            subscription_type,
        }
    }

    /// 创建新的消费者（使用订阅）
    pub fn new_with_subscription(client: ArroyoClient, subscription: SubscriptionType, options: ConsumerOptions) -> Self {
        let mut options_with_subscription = options;
        options_with_subscription.subscription_type = Some(subscription.clone());

        // 根据订阅类型设置 topic 和 topics
        let (topic_opt, topics) = match &subscription {
            SubscriptionType::Single(t) => (Some(t.clone()), vec![t.clone()]),
            SubscriptionType::Multiple(ts) => (None, ts.clone()),
            SubscriptionType::Pattern(_) => (None, vec![]),
        };

        Self {
            client,
            topic: topic_opt,
            topics,
            options: options_with_subscription,
            subscription_type: Some(subscription),
        }
    }

    /// 拉取消息
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<Message>> {
        // 检查是否有单个 Topic
        if let Some(topic) = &self.topic {
            let response = self
                .client
                .client
                .get(&format!(
                    "{}/api/topics/{}/messages",
                    self.client.base_url, topic
                ))
                .query(&[
                    ("group_id", &self.options.group_id),
                    ("timeout_ms", &timeout.as_millis().to_string()),
                    ("max_bytes", &self.options.max_partition_fetch_bytes.to_string()),
                ])
                .send()
                .await?;

            return self.client.handle_response(response).await;
        }

        // 如果是多个 Topic 或正则表达式订阅，则需要获取所有匹配的 Topic
        let available_topics = self.list_topics().await?;
        let matching_topics = match &self.subscription_type {
            Some(subscription) => subscription.get_matching_topics(&available_topics),
            None => vec![],
        };

        if matching_topics.is_empty() {
            return Ok(vec![]);
        }

        // 从所有匹配的 Topic 中拉取消息
        let mut all_messages = Vec::new();
        for topic in matching_topics {
            let response = self
                .client
                .client
                .get(&format!(
                    "{}/api/topics/{}/messages",
                    self.client.base_url, topic
                ))
                .query(&[
                    ("group_id", &self.options.group_id),
                    ("timeout_ms", &timeout.as_millis().to_string()),
                    ("max_bytes", &self.options.max_partition_fetch_bytes.to_string()),
                ])
                .send()
                .await?;

            let messages: Vec<Message> = self.client.handle_response(response).await?;
            all_messages.extend(messages);
        }

        Ok(all_messages)
    }

    /// 获取所有可用的 Topic
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        let response = self
            .client
            .client
            .get(&format!("{}/api/topics", self.client.base_url))
            .send()
            .await?;

        self.client.handle_response(response).await
    }

    /// 提交偏移量
    pub async fn commit(&self, topic: impl Into<String>, partition: u32, offset: u64) -> Result<()> {
        let topic_str = topic.into();
        let response = self
            .client
            .client
            .post(&format!(
                "{}/api/topics/{}/offsets",
                self.client.base_url, topic_str
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

    /// 提交单个 Topic 的偏移量（仅适用于单个 Topic 订阅）
    pub async fn commit_single(&self, partition: u32, offset: u64) -> Result<()> {
        if let Some(topic) = &self.topic {
            self.commit(topic, partition, offset).await
        } else {
            Err(Error::ValidationError("No single topic available for this consumer".to_string()))
        }
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
