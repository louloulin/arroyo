use crate::client::ArroyoClient;
use crate::compression::{create_compressor, Compressor, CompressionType};
use crate::error::Result;
use crate::models::Message;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, Mutex as TokioMutex};

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

/// 批处理统计信息
#[derive(Debug, Clone)]
struct BatchStats {
    /// 批处理大小（消息数）
    batch_size: usize,
    /// 批处理延迟（毫秒）
    batch_delay_ms: u64,
    /// 发送时间
    send_time: Instant,
    /// 批处理总大小（字节）
    total_bytes: usize,
    /// 延迟（毫秒）
    latency_ms: u64,
    /// 是否成功
    success: bool,
}

/// 自适应批处理策略
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AdaptiveBatchingStrategy {
    /// 禁用自适应批处理
    Disabled,
    /// 基于吞吐量的自适应批处理
    Throughput,
    /// 基于延迟的自适应批处理
    Latency,
    /// 平衡吞吐量和延迟的自适应批处理
    Balanced,
}

impl Default for AdaptiveBatchingStrategy {
    fn default() -> Self {
        AdaptiveBatchingStrategy::Disabled
    }
}

/// 自适应批处理配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveBatchingConfig {
    /// 自适应批处理策略
    pub strategy: AdaptiveBatchingStrategy,
    /// 最小批处理大小
    pub min_batch_size: usize,
    /// 最大批处理大小
    pub max_batch_size: usize,
    /// 最小批处理延迟（毫秒）
    pub min_batch_delay_ms: u64,
    /// 最大批处理延迟（毫秒）
    pub max_batch_delay_ms: u64,
    /// 调整间隔（毫秒）
    pub adjustment_interval_ms: u64,
    /// 目标延迟（毫秒）
    pub target_latency_ms: u64,
    /// 目标吞吐量（消息/秒）
    pub target_throughput: f64,
}

impl Default for AdaptiveBatchingConfig {
    fn default() -> Self {
        Self {
            strategy: AdaptiveBatchingStrategy::default(),
            min_batch_size: 1,
            max_batch_size: 100000,
            min_batch_delay_ms: 0,
            max_batch_delay_ms: 1000,
            adjustment_interval_ms: 5000,
            target_latency_ms: 100,
            target_throughput: 10000.0,
        }
    }
}

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
    #[serde(default)]
    pub compression_type: CompressionType,
    /// 确认级别
    pub acks: String,
    /// 重试次数
    pub retries: u32,
    /// 重试间隔（毫秒）
    pub retry_backoff_ms: u64,
    /// 发送超时（毫秒）
    pub send_timeout_ms: u64,
    /// 自适应批处理配置
    pub adaptive_batching: Option<AdaptiveBatchingConfig>,
    /// 其他配置选项
    pub config: Option<HashMap<String, String>>,
}

impl Default for ProducerOptions {
    fn default() -> Self {
        Self {
            client_id: format!("arroyo-producer-{}", uuid::Uuid::new_v4()),
            batch_size: 16384,
            batch_delay_ms: 5,
            compression_type: CompressionType::None,
            acks: "all".to_string(),
            retries: 3,
            retry_backoff_ms: 100,
            send_timeout_ms: 30000, // 默认 30 秒超时
            adaptive_batching: None, // 默认禁用自适应批处理
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
        let compression_type_str = compression_type.into();
        match CompressionType::from_str(&compression_type_str) {
            Ok(ct) => self.options.compression_type = ct,
            Err(_) => {
                // 如果无法解析，则使用默认值
                eprintln!("警告: 无效的压缩类型 '{}', 使用默认值 'none'", compression_type_str);
            }
        }
        self
    }

    /// 设置压缩类型（枚举）
    pub fn compression_type_enum(mut self, compression_type: CompressionType) -> Self {
        self.options.compression_type = compression_type;
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

    /// 启用自适应批处理
    pub fn enable_adaptive_batching(mut self, strategy: AdaptiveBatchingStrategy) -> Self {
        let mut config = self.options.adaptive_batching.unwrap_or_default();
        config.strategy = strategy;
        self.options.adaptive_batching = Some(config);
        self
    }

    /// 设置自适应批处理配置
    pub fn adaptive_batching_config(mut self, config: AdaptiveBatchingConfig) -> Self {
        self.options.adaptive_batching = Some(config);
        self
    }

    /// 设置自适应批处理的最小批处理大小
    pub fn min_batch_size(mut self, min_batch_size: usize) -> Self {
        let mut config = self.options.adaptive_batching.unwrap_or_default();
        config.min_batch_size = min_batch_size;
        self.options.adaptive_batching = Some(config);
        self
    }

    /// 设置自适应批处理的最大批处理大小
    pub fn max_batch_size(mut self, max_batch_size: usize) -> Self {
        let mut config = self.options.adaptive_batching.unwrap_or_default();
        config.max_batch_size = max_batch_size;
        self.options.adaptive_batching = Some(config);
        self
    }

    /// 设置自适应批处理的最小批处理延迟（毫秒）
    pub fn min_batch_delay_ms(mut self, min_batch_delay_ms: u64) -> Self {
        let mut config = self.options.adaptive_batching.unwrap_or_default();
        config.min_batch_delay_ms = min_batch_delay_ms;
        self.options.adaptive_batching = Some(config);
        self
    }

    /// 设置自适应批处理的最大批处理延迟（毫秒）
    pub fn max_batch_delay_ms(mut self, max_batch_delay_ms: u64) -> Self {
        let mut config = self.options.adaptive_batching.unwrap_or_default();
        config.max_batch_delay_ms = max_batch_delay_ms;
        self.options.adaptive_batching = Some(config);
        self
    }

    /// 设置自适应批处理的调整间隔（毫秒）
    pub fn adjustment_interval_ms(mut self, adjustment_interval_ms: u64) -> Self {
        let mut config = self.options.adaptive_batching.unwrap_or_default();
        config.adjustment_interval_ms = adjustment_interval_ms;
        self.options.adaptive_batching = Some(config);
        self
    }

    /// 设置自适应批处理的目标延迟（毫秒）
    pub fn target_latency_ms(mut self, target_latency_ms: u64) -> Self {
        let mut config = self.options.adaptive_batching.unwrap_or_default();
        config.target_latency_ms = target_latency_ms;
        self.options.adaptive_batching = Some(config);
        self
    }

    /// 设置自适应批处理的目标吞吐量（消息/秒）
    pub fn target_throughput(mut self, target_throughput: f64) -> Self {
        let mut config = self.options.adaptive_batching.unwrap_or_default();
        config.target_throughput = target_throughput;
        self.options.adaptive_batching = Some(config);
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

/// 自适应批处理器
struct AdaptiveBatcher {
    /// 当前批处理大小
    current_batch_size: usize,
    /// 当前批处理延迟（毫秒）
    current_batch_delay_ms: u64,
    /// 批处理统计信息
    stats: VecDeque<BatchStats>,
    /// 上次调整时间
    last_adjustment_time: Instant,
    /// 配置
    config: AdaptiveBatchingConfig,
}

impl AdaptiveBatcher {
    /// 创建新的自适应批处理器
    fn new(config: AdaptiveBatchingConfig, initial_batch_size: usize, initial_batch_delay_ms: u64) -> Self {
        Self {
            current_batch_size: initial_batch_size,
            current_batch_delay_ms: initial_batch_delay_ms,
            stats: VecDeque::with_capacity(100),
            last_adjustment_time: Instant::now(),
            config,
        }
    }

    /// 添加批处理统计信息
    fn add_stats(&mut self, stats: BatchStats) {
        // 保持最多 100 条统计信息
        if self.stats.len() >= 100 {
            self.stats.pop_front();
        }
        self.stats.push_back(stats);
    }

    /// 调整批处理参数
    fn adjust_parameters(&mut self) -> (usize, u64) {
        // 如果没有足够的统计信息或者还没到调整时间，则不调整
        if self.stats.is_empty() ||
           self.last_adjustment_time.elapsed().as_millis() < self.config.adjustment_interval_ms as u128 {
            return (self.current_batch_size, self.current_batch_delay_ms);
        }

        // 计算平均延迟和吞吐量
        let mut total_latency = 0;
        let mut total_messages = 0;
        let mut success_count = 0;

        for stat in &self.stats {
            if stat.success {
                total_latency += stat.latency_ms;
                total_messages += stat.batch_size;
                success_count += 1;
            }
        }

        // 如果没有成功的批处理，则不调整
        if success_count == 0 {
            return (self.current_batch_size, self.current_batch_delay_ms);
        }

        let avg_latency = total_latency as f64 / success_count as f64;
        let elapsed_ms = self.last_adjustment_time.elapsed().as_millis() as f64;
        let throughput = (total_messages as f64 / elapsed_ms) * 1000.0; // 消息/秒

        // 根据策略调整参数
        match self.config.strategy {
            AdaptiveBatchingStrategy::Disabled => {
                // 不调整
                (self.current_batch_size, self.current_batch_delay_ms)
            }
            AdaptiveBatchingStrategy::Throughput => {
                // 优先考虑吞吐量
                let mut new_batch_size = self.current_batch_size;
                let mut new_batch_delay_ms = self.current_batch_delay_ms;

                // 如果吞吐量低于目标，增加批处理大小，减少批处理延迟
                if throughput < self.config.target_throughput {
                    new_batch_size = (new_batch_size as f64 * 1.2) as usize;
                    new_batch_delay_ms = (new_batch_delay_ms as f64 * 0.8) as u64;
                } else {
                    // 如果吞吐量高于目标，可以适当增加批处理延迟以减少 CPU 使用
                    new_batch_delay_ms = (new_batch_delay_ms as f64 * 1.1) as u64;
                }

                // 确保在配置范围内
                new_batch_size = new_batch_size.clamp(self.config.min_batch_size, self.config.max_batch_size);
                new_batch_delay_ms = new_batch_delay_ms.clamp(self.config.min_batch_delay_ms, self.config.max_batch_delay_ms);

                self.current_batch_size = new_batch_size;
                self.current_batch_delay_ms = new_batch_delay_ms;
                self.last_adjustment_time = Instant::now();
                self.stats.clear();

                (new_batch_size, new_batch_delay_ms)
            }
            AdaptiveBatchingStrategy::Latency => {
                // 优先考虑延迟
                let mut new_batch_size = self.current_batch_size;
                let mut new_batch_delay_ms = self.current_batch_delay_ms;

                // 如果延迟高于目标，减少批处理大小和延迟
                if avg_latency > self.config.target_latency_ms as f64 {
                    new_batch_size = (new_batch_size as f64 * 0.8) as usize;
                    new_batch_delay_ms = (new_batch_delay_ms as f64 * 0.8) as u64;
                } else {
                    // 如果延迟低于目标，可以适当增加批处理大小以提高吞吐量
                    new_batch_size = (new_batch_size as f64 * 1.1) as usize;
                }

                // 确保在配置范围内
                new_batch_size = new_batch_size.clamp(self.config.min_batch_size, self.config.max_batch_size);
                new_batch_delay_ms = new_batch_delay_ms.clamp(self.config.min_batch_delay_ms, self.config.max_batch_delay_ms);

                self.current_batch_size = new_batch_size;
                self.current_batch_delay_ms = new_batch_delay_ms;
                self.last_adjustment_time = Instant::now();
                self.stats.clear();

                (new_batch_size, new_batch_delay_ms)
            }
            AdaptiveBatchingStrategy::Balanced => {
                // 平衡吞吐量和延迟
                let mut new_batch_size = self.current_batch_size;
                let mut new_batch_delay_ms = self.current_batch_delay_ms;

                // 计算延迟和吞吐量的偏差
                let latency_ratio = avg_latency / self.config.target_latency_ms as f64;
                let throughput_ratio = self.config.target_throughput / throughput;

                // 根据偏差调整参数
                if latency_ratio > 1.2 {
                    // 延迟太高，减少批处理大小和延迟
                    new_batch_size = (new_batch_size as f64 * 0.9) as usize;
                    new_batch_delay_ms = (new_batch_delay_ms as f64 * 0.8) as u64;
                } else if throughput_ratio > 1.2 {
                    // 吞吐量太低，增加批处理大小
                    new_batch_size = (new_batch_size as f64 * 1.1) as usize;
                    // 如果延迟允许，可以适当减少批处理延迟
                    if latency_ratio < 0.8 {
                        new_batch_delay_ms = (new_batch_delay_ms as f64 * 0.9) as u64;
                    }
                } else {
                    // 在目标范围内，微调
                    if latency_ratio < 0.8 && throughput_ratio < 0.8 {
                        // 性能很好，可以适当增加批处理大小以提高吞吐量
                        new_batch_size = (new_batch_size as f64 * 1.05) as usize;
                    }
                }

                // 确保在配置范围内
                new_batch_size = new_batch_size.clamp(self.config.min_batch_size, self.config.max_batch_size);
                new_batch_delay_ms = new_batch_delay_ms.clamp(self.config.min_batch_delay_ms, self.config.max_batch_delay_ms);

                self.current_batch_size = new_batch_size;
                self.current_batch_delay_ms = new_batch_delay_ms;
                self.last_adjustment_time = Instant::now();
                self.stats.clear();

                (new_batch_size, new_batch_delay_ms)
            }
        }
    }
}

/// 批处理器
#[derive(Clone)]
enum Batcher {
    /// 固定批处理
    Fixed {
        batch_size: usize,
        batch_delay_ms: u64,
    },
    /// 自适应批处理
    Adaptive(Arc<TokioMutex<AdaptiveBatcher>>),
}

/// 生产者，用于向 Topic 发送消息
#[derive(Clone)]
pub struct Producer {
    /// 客户端
    client: ArroyoClient,
    /// Topic 名称
    topic: String,
    /// 生产者配置
    options: ProducerOptions,
    /// 批处理器
    batcher: Batcher,
    /// 压缩器
    compressor: Arc<dyn Compressor + Send + Sync>,
}

impl Producer {
    /// 创建新的生产者
    pub fn new(client: ArroyoClient, topic: impl Into<String>, options: ProducerOptions) -> Self {
        // 创建批处理器
        let batcher = match &options.adaptive_batching {
            Some(config) if config.strategy != AdaptiveBatchingStrategy::Disabled => {
                // 创建自适应批处理器
                let adaptive_batcher = AdaptiveBatcher::new(
                    config.clone(),
                    options.batch_size,
                    options.batch_delay_ms,
                );
                Batcher::Adaptive(Arc::new(TokioMutex::new(adaptive_batcher)))
            }
            _ => {
                // 创建固定批处理器
                Batcher::Fixed {
                    batch_size: options.batch_size,
                    batch_delay_ms: options.batch_delay_ms,
                }
            }
        };

        // 创建压缩器
        let compressor = create_compressor(options.compression_type);

        Self {
            client,
            topic: topic.into(),
            options,
            batcher,
            compressor,
        }
    }

    /// 获取当前批处理参数
    pub async fn get_batch_parameters(&self) -> (usize, u64) {
        match &self.batcher {
            Batcher::Fixed { batch_size, batch_delay_ms } => (*batch_size, *batch_delay_ms),
            Batcher::Adaptive(batcher) => {
                let batcher = batcher.lock().await;
                (batcher.current_batch_size, batcher.current_batch_delay_ms)
            }
        }
    }

    /// 更新批处理统计信息
    async fn update_batch_stats(&self, stats: BatchStats) {
        if let Batcher::Adaptive(batcher) = &self.batcher {
            let mut batcher = batcher.lock().await;
            batcher.add_stats(stats);
            batcher.adjust_parameters();
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
        let start_time = Instant::now();
        let (_batch_size, batch_delay_ms) = self.get_batch_parameters().await;

        // 计算批处理总大小
        let total_bytes: usize = messages
            .iter()
            .map(|m| m.value.len() + m.key.as_ref().map_or(0, |k| k.len()))
            .sum();

        // 如果启用了压缩，则压缩消息
        let compressed_messages = if self.options.compression_type != CompressionType::None {
            let mut compressed_messages = Vec::with_capacity(messages.len());
            for message in &messages {
                let mut compressed_message = message.clone();

                // 压缩消息值
                compressed_message.value = self.compressor.compress(&message.value)?;

                // 如果有消息键，也压缩它
                if let Some(key) = &message.key {
                    compressed_message.key = Some(self.compressor.compress(key)?);
                }

                // 添加压缩类型头部
                let mut headers = message.headers.clone().unwrap_or_default();
                headers.insert(
                    "compression-type".to_string(),
                    self.options.compression_type.to_string().into_bytes(),
                );
                compressed_message.headers = Some(headers);

                compressed_messages.push(compressed_message);
            }
            compressed_messages
        } else {
            messages.clone()
        };

        let response = self
            .client
            .client
            .post(&format!(
                "{}/api/topics/{}/messages/batch",
                self.client.base_url, self.topic
            ))
            .json(&compressed_messages)
            .send()
            .await?;

        let result = self.client.handle_empty_response(response).await;
        let latency = start_time.elapsed();
        let latency_ms = latency.as_millis() as u64;

        // 更新批处理统计信息
        self.update_batch_stats(BatchStats {
            batch_size: messages.len(),
            batch_delay_ms,
            send_time: start_time,
            total_bytes,
            latency_ms,
            success: result.is_ok(),
        })
        .await;

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
                            latency_ms,
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
                            latency_ms,
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
        let producer_clone = self.clone();

        tokio::spawn(async move {
            let start_time = Instant::now();
            let (_batch_size, batch_delay_ms) = producer_clone.get_batch_parameters().await;

            // 计算批处理总大小
            let total_bytes: usize = messages
                .iter()
                .map(|m| m.value.len() + m.key.as_ref().map_or(0, |k| k.len()))
                .sum();

            // 如果启用了压缩，则压缩消息
            let compressed_messages = if producer_clone.options.compression_type != CompressionType::None {
                let mut compressed_messages = Vec::with_capacity(messages.len());
                for message in &messages {
                    let mut compressed_message = message.clone();

                    // 压缩消息值
                    match producer_clone.compressor.compress(&message.value) {
                        Ok(compressed_value) => compressed_message.value = compressed_value,
                        Err(e) => {
                            // 如果压缩失败，则使用原始消息
                            eprintln!("压缩消息失败: {}", e);
                            compressed_messages.push(message.clone());
                            continue;
                        }
                    }

                    // 如果有消息键，也压缩它
                    if let Some(key) = &message.key {
                        match producer_clone.compressor.compress(key) {
                            Ok(compressed_key) => compressed_message.key = Some(compressed_key),
                            Err(e) => {
                                // 如果压缩失败，则使用原始键
                                eprintln!("压缩消息键失败: {}", e);
                                compressed_message.key = message.key.clone();
                            }
                        }
                    }

                    // 添加压缩类型头部
                    let mut headers = message.headers.clone().unwrap_or_default();
                    headers.insert(
                        "compression-type".to_string(),
                        producer_clone.options.compression_type.to_string().into_bytes(),
                    );
                    compressed_message.headers = Some(headers);

                    compressed_messages.push(compressed_message);
                }
                compressed_messages
            } else {
                messages.clone()
            };

            let response = client
                .client
                .post(&format!(
                    "{}/api/topics/{}/messages/batch",
                    client.base_url, topic
                ))
                .json(&compressed_messages)
                .send()
                .await;

            let latency = start_time.elapsed();
            let latency_ms = latency.as_millis() as u64;

            let result = match response {
                Ok(resp) => client.handle_empty_response(resp).await,
                Err(e) => Err(e.into()),
            };

            // 更新批处理统计信息
            producer_clone.update_batch_stats(BatchStats {
                batch_size: messages.len(),
                batch_delay_ms,
                send_time: start_time,
                total_bytes,
                latency_ms,
                success: result.is_ok(),
            })
            .await;

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
                                latency_ms,
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
                                latency_ms,
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
