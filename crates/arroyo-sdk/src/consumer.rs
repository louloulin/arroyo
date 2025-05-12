use crate::client::ArroyoClient;
use crate::consumer_group::{ConsumerGroupManager, PartitionAssignmentStrategy};
use crate::error::{Error, Result};
use crate::models::Message;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

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
    /// 启用消费者组
    pub enable_consumer_group: bool,
    /// 分区分配策略
    pub partition_assignment_strategy: Option<PartitionAssignmentStrategy>,
    /// 其他配置选项
    pub config: Option<HashMap<String, String>>,
    /// 流量控制配置
    pub flow_control_config: Option<crate::flow_control::FlowControlConfig>,
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
            enable_consumer_group: false,
            partition_assignment_strategy: Some(PartitionAssignmentStrategy::RoundRobin),
            config: None,
            flow_control_config: None,
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

    /// 启用消费者组
    pub fn enable_consumer_group(mut self, enable: bool) -> Self {
        self.options.enable_consumer_group = enable;
        self
    }

    /// 设置分区分配策略
    pub fn partition_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.options.partition_assignment_strategy = Some(strategy);
        self
    }

    /// 添加配置选项
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let config = self.options.config.get_or_insert_with(HashMap::new);
        config.insert(key.into(), value.into());
        self
    }

    /// 设置流量控制配置
    pub fn flow_control(mut self, config: crate::flow_control::FlowControlConfig) -> Self {
        self.options.flow_control_config = Some(config);
        self
    }

    /// 启用固定速率限流
    pub fn enable_rate_limiting(mut self, messages_per_second: u32) -> Self {
        use crate::flow_control::{FlowControlConfig, FlowControlStrategy, BackpressureStrategy};

        let config = FlowControlConfig {
            strategy: FlowControlStrategy::FixedRate,
            max_messages_per_second: messages_per_second,
            backpressure_strategy: BackpressureStrategy::Block,
            buffer_size: 10000,
            backpressure_threshold: 0.8,
        };

        self.options.flow_control_config = Some(config);
        self
    }

    /// 启用自适应速率限流
    pub fn enable_adaptive_rate_limiting(mut self, initial_messages_per_second: u32) -> Self {
        use crate::flow_control::{FlowControlConfig, FlowControlStrategy, BackpressureStrategy};

        let config = FlowControlConfig {
            strategy: FlowControlStrategy::Adaptive,
            max_messages_per_second: initial_messages_per_second,
            backpressure_strategy: BackpressureStrategy::Block,
            buffer_size: 10000,
            backpressure_threshold: 0.8,
        };

        self.options.flow_control_config = Some(config);
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
    /// 消费者组管理器
    group_manager: Option<Arc<RwLock<crate::consumer_group::ConsumerGroupManager>>>,
    /// 流量控制器
    flow_controller: Option<Arc<crate::flow_control::FlowController>>,
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

        // 创建消费者组管理器（如果启用了消费者组）
        let group_manager = if options.enable_consumer_group {
            let manager = ConsumerGroupManager::new(
                client.clone(),
                options.group_id.clone(),
                options.client_id.clone(),
                options.session_timeout_ms,
                options.heartbeat_interval_ms,
                PartitionAssignmentStrategy::RoundRobin,
            );

            Some(Arc::new(RwLock::new(manager)))
        } else {
            None
        };

        // 创建流量控制器（如果配置了流量控制）
        let flow_controller = if let Some(flow_config) = &options.flow_control_config {
            Some(Arc::new(crate::flow_control::FlowController::new(flow_config.clone())))
        } else {
            None
        };

        Self {
            client,
            topic: topic_opt,
            topics,
            options,
            subscription_type,
            group_manager,
            flow_controller,
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

        // 创建消费者组管理器（如果启用了消费者组）
        let group_manager = if options_with_subscription.enable_consumer_group {
            let manager = ConsumerGroupManager::new(
                client.clone(),
                options_with_subscription.group_id.clone(),
                options_with_subscription.client_id.clone(),
                options_with_subscription.session_timeout_ms,
                options_with_subscription.heartbeat_interval_ms,
                PartitionAssignmentStrategy::RoundRobin,
            );

            Some(Arc::new(RwLock::new(manager)))
        } else {
            None
        };

        // 创建流量控制器（如果配置了流量控制）
        let flow_controller = if let Some(flow_config) = &options_with_subscription.flow_control_config {
            Some(Arc::new(crate::flow_control::FlowController::new(flow_config.clone())))
        } else {
            None
        };

        Self {
            client,
            topic: topic_opt,
            topics,
            options: options_with_subscription,
            subscription_type: Some(subscription),
            group_manager,
            flow_controller,
        }
    }

    /// 拉取消息
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<Message>> {
        // 应用流量控制
        if let Some(flow_controller) = &self.flow_controller {
            // 检查是否应该应用流量控制
            if !flow_controller.apply_flow_control().await {
                // 如果流量控制器决定丢弃消息，则返回空结果
                return Ok(vec![]);
            }
        }

        // 如果启用了消费者组，确保消费者组管理器已启动
        if self.options.enable_consumer_group {
            if let Some(group_manager) = &self.group_manager {
                let manager = group_manager.read().await;
                if !manager.is_joined().await {
                    // 启动消费者组管理器
                    drop(manager);
                    let mut manager = group_manager.write().await;

                    // 设置订阅的 Topic
                    let topics = match &self.subscription_type {
                        Some(SubscriptionType::Single(t)) => vec![t.clone()],
                        Some(SubscriptionType::Multiple(ts)) => ts.clone(),
                        Some(SubscriptionType::Pattern(_)) => {
                            let available_topics = self.list_topics().await?;
                            self.subscription_type
                                .as_ref()
                                .unwrap()
                                .get_matching_topics(&available_topics)
                        }
                        None => {
                            if let Some(topic) = &self.topic {
                                vec![topic.clone()]
                            } else {
                                vec![]
                            }
                        }
                    };

                    manager.subscribe(topics);
                    manager.start().await?;
                }
            }
        }

        // 如果启用了消费者组，只拉取分配给该消费者的分区
        if self.options.enable_consumer_group {
            if let Some(group_manager) = &self.group_manager {
                let manager = group_manager.read().await;
                let assigned_partitions = manager.get_assigned_partitions().await;

                if assigned_partitions.is_empty() {
                    return Ok(vec![]);
                }

                // 从分配的分区中拉取消息
                let mut all_messages = Vec::new();
                for (topic, partitions) in assigned_partitions {
                    let partitions_str = partitions
                        .iter()
                        .map(|p| p.to_string())
                        .collect::<Vec<_>>()
                        .join(",");

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
                            ("partitions", &partitions_str),
                        ])
                        .send()
                        .await?;

                    let messages: Vec<Message> = self.client.handle_response(response).await?;
                    all_messages.extend(messages);
                }

                // 更新流量控制器的缓冲区使用量
                if let Some(flow_controller) = &self.flow_controller {
                    flow_controller.update_buffer_usage(all_messages.len()).await;

                    // 如果有消息，调整流量控制参数
                    if !all_messages.is_empty() {
                        flow_controller.adjust_parameters(all_messages.len() as f64, timeout).await;
                    }
                }

                return Ok(all_messages);
            }
        }

        // 如果没有启用消费者组，使用普通的拉取逻辑
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

            let messages: Vec<Message> = self.client.handle_response(response).await?;

            // 更新流量控制器的缓冲区使用量
            if let Some(flow_controller) = &self.flow_controller {
                flow_controller.update_buffer_usage(messages.len()).await;

                // 如果有消息，调整流量控制参数
                if !messages.is_empty() {
                    flow_controller.adjust_parameters(messages.len() as f64, timeout).await;
                }
            }

            return Ok(messages);
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

        // 更新流量控制器的缓冲区使用量
        if let Some(flow_controller) = &self.flow_controller {
            flow_controller.update_buffer_usage(all_messages.len()).await;

            // 如果有消息，调整流量控制参数
            if !all_messages.is_empty() {
                flow_controller.adjust_parameters(all_messages.len() as f64, timeout).await;
            }
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
    pub async fn commit_batch(&self, topic: impl Into<String>, offsets: HashMap<u32, u64>) -> Result<()> {
        let topic_str = topic.into();
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
                self.client.base_url, topic_str
            ))
            .json(&batch)
            .send()
            .await?;

        self.client.handle_empty_response(response).await
    }

    /// 批量提交单个 Topic 的偏移量（仅适用于单个 Topic 订阅）
    pub async fn commit_batch_single(&self, offsets: HashMap<u32, u64>) -> Result<()> {
        if let Some(topic) = &self.topic {
            self.commit_batch(topic, offsets).await
        } else {
            Err(Error::ValidationError("No single topic available for this consumer".to_string()))
        }
    }

    /// 获取当前偏移量
    pub async fn get_offsets(&self, topic: impl Into<String>) -> Result<HashMap<u32, u64>> {
        let topic_str = topic.into();
        let response = self
            .client
            .client
            .get(&format!(
                "{}/api/topics/{}/offsets",
                self.client.base_url, topic_str
            ))
            .query(&[("group_id", &self.options.group_id)])
            .send()
            .await?;

        self.client.handle_response(response).await
    }

    /// 获取单个 Topic 的当前偏移量（仅适用于单个 Topic 订阅）
    pub async fn get_offsets_single(&self) -> Result<HashMap<u32, u64>> {
        if let Some(topic) = &self.topic {
            self.get_offsets(topic).await
        } else {
            Err(Error::ValidationError("No single topic available for this consumer".to_string()))
        }
    }

    /// 获取所有订阅 Topic 的当前偏移量
    pub async fn get_all_offsets(&self) -> Result<HashMap<String, HashMap<u32, u64>>> {
        let mut result = HashMap::new();

        // 如果是单个 Topic 订阅
        if let Some(topic) = &self.topic {
            let offsets = self.get_offsets(topic).await?;
            result.insert(topic.clone(), offsets);
            return Ok(result);
        }

        // 如果是多个 Topic 或正则表达式订阅
        let available_topics = self.list_topics().await?;
        let matching_topics = match &self.subscription_type {
            Some(subscription) => subscription.get_matching_topics(&available_topics),
            None => vec![],
        };

        for topic in matching_topics {
            let offsets = self.get_offsets(&topic).await?;
            result.insert(topic, offsets);
        }

        Ok(result)
    }

    /// 关闭消费者
    pub async fn close(&self) -> Result<()> {
        // 如果启用了消费者组，停止消费者组管理器
        if self.options.enable_consumer_group {
            if let Some(group_manager) = &self.group_manager {
                let manager = group_manager.read().await;
                manager.stop().await?;
            }
        }

        Ok(())
    }
}
