use crate::client::ArroyoClient;
use crate::compression::{create_compressor, Compressor, CompressionType};
use crate::error::{Error, Result};
use crate::models::Message;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as TokioMutex;
use lru::LruCache;

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

/// 消息 ID，用于幂等性发送
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId {
    /// 生产者 ID
    pub producer_id: String,
    /// 序列号
    pub sequence_number: u64,
    /// 消息哈希
    pub message_hash: u64,
}

impl MessageId {
    /// 创建新的消息 ID
    pub fn new(producer_id: impl Into<String>, sequence_number: u64, message_hash: u64) -> Self {
        Self {
            producer_id: producer_id.into(),
            sequence_number,
            message_hash,
        }
    }

    /// 计算消息哈希
    pub fn calculate_message_hash(key: Option<&[u8]>, value: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        if let Some(k) = key {
            k.hash(&mut hasher);
        }
        value.hash(&mut hasher);
        hasher.finish()
    }
}

/// 事务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// 未初始化
    Uninitialized,
    /// 已初始化
    Initialized,
    /// 已开始
    Started,
    /// 已提交
    Committed,
    /// 已中止
    Aborted,
    /// 已关闭
    Closed,
}

/// 事务操作结果
#[derive(Debug, Clone)]
pub struct TransactionResult {
    /// 操作是否成功
    pub success: bool,
    /// 错误信息（如果操作失败）
    pub error: Option<String>,
    /// 操作耗时（毫秒）
    pub latency_ms: u64,
    /// 事务 ID
    pub transaction_id: String,
}

/// 事务配置选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOptions {
    /// 事务 ID 前缀
    pub transaction_id_prefix: String,
    /// 事务超时（毫秒）
    pub transaction_timeout_ms: u64,
    /// 事务重试次数
    pub transaction_retries: u32,
    /// 事务重试间隔（毫秒）
    pub transaction_retry_backoff_ms: u64,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            transaction_id_prefix: "arroyo-tx".to_string(),
            transaction_timeout_ms: 60000, // 默认 60 秒超时
            transaction_retries: 3,
            transaction_retry_backoff_ms: 100,
        }
    }
}

/// 事务
#[derive(Debug, Clone)]
pub struct Transaction {
    /// 事务 ID
    pub id: String,
    /// 事务状态
    pub state: TransactionState,
    /// 事务开始时间
    pub start_time: Option<SystemTime>,
    /// 事务结束时间
    pub end_time: Option<SystemTime>,
    /// 缓冲的消息
    pub buffered_messages: Vec<Message>,
    /// 事务配置
    pub options: TransactionOptions,
}

impl Transaction {
    /// 创建新的事务
    pub fn new(id: impl Into<String>, options: TransactionOptions) -> Self {
        Self {
            id: id.into(),
            state: TransactionState::Uninitialized,
            start_time: None,
            end_time: None,
            buffered_messages: Vec::new(),
            options,
        }
    }

    /// 初始化事务
    pub fn initialize(&mut self) {
        if self.state == TransactionState::Uninitialized {
            self.state = TransactionState::Initialized;
        }
    }

    /// 开始事务
    pub fn begin(&mut self) {
        if self.state == TransactionState::Initialized {
            self.state = TransactionState::Started;
            self.start_time = Some(SystemTime::now());
        }
    }

    /// 添加消息到事务
    pub fn add_message(&mut self, message: Message) {
        if self.state == TransactionState::Started {
            self.buffered_messages.push(message);
        }
    }

    /// 提交事务
    pub fn commit(&mut self) {
        if self.state == TransactionState::Started {
            self.state = TransactionState::Committed;
            self.end_time = Some(SystemTime::now());
        }
    }

    /// 中止事务
    pub fn abort(&mut self) {
        if self.state == TransactionState::Started {
            self.state = TransactionState::Aborted;
            self.end_time = Some(SystemTime::now());
            self.buffered_messages.clear();
        }
    }

    /// 关闭事务
    pub fn close(&mut self) {
        if self.state == TransactionState::Committed || self.state == TransactionState::Aborted {
            self.state = TransactionState::Closed;
        }
    }

    /// 获取事务持续时间（毫秒）
    pub fn duration_ms(&self) -> Option<u64> {
        match (self.start_time, self.end_time) {
            (Some(start), Some(end)) => {
                end.duration_since(start).ok().map(|d| d.as_millis() as u64)
            }
            _ => None,
        }
    }

    /// 检查事务是否已超时
    pub fn is_timed_out(&self) -> bool {
        if let Some(start) = self.start_time {
            if self.state == TransactionState::Started {
                if let Ok(elapsed) = SystemTime::now().duration_since(start) {
                    return elapsed.as_millis() as u64 > self.options.transaction_timeout_ms;
                }
            }
        }
        false
    }

    /// 获取缓冲的消息数量
    pub fn message_count(&self) -> usize {
        self.buffered_messages.len()
    }

    /// 清空缓冲的消息
    pub fn clear_messages(&mut self) {
        self.buffered_messages.clear();
    }

    /// 获取缓冲的消息
    pub fn take_messages(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.buffered_messages)
    }
}

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

/// 幂等性配置选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotenceOptions {
    /// 生产者 ID
    pub producer_id: String,
    /// 是否启用序列号
    pub enable_sequence_numbers: bool,
    /// 缓存大小
    pub deduplication_cache_size: usize,
    /// 缓存过期时间（毫秒）
    pub deduplication_cache_expiry_ms: u64,
}

impl Default for IdempotenceOptions {
    fn default() -> Self {
        Self {
            producer_id: format!("arroyo-producer-{}", uuid::Uuid::new_v4()),
            enable_sequence_numbers: true,
            deduplication_cache_size: 1000,
            deduplication_cache_expiry_ms: 60000, // 默认 60 秒过期
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
    /// 事务配置
    pub transaction_options: Option<TransactionOptions>,
    /// 是否启用事务
    pub transactional: bool,
    /// 幂等性配置
    pub idempotence_options: Option<IdempotenceOptions>,
    /// 是否启用幂等性
    pub idempotent: bool,
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
            transaction_options: None, // 默认禁用事务
            transactional: false, // 默认禁用事务
            idempotence_options: None, // 默认禁用幂等性
            idempotent: false, // 默认禁用幂等性
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

    /// 启用事务
    pub fn enable_transactions(mut self) -> Self {
        self.options.transactional = true;
        if self.options.transaction_options.is_none() {
            self.options.transaction_options = Some(TransactionOptions::default());
        }
        self
    }

    /// 设置事务 ID 前缀
    pub fn transaction_id_prefix(mut self, prefix: impl Into<String>) -> Self {
        let mut options = self.options.transaction_options.unwrap_or_default();
        options.transaction_id_prefix = prefix.into();
        self.options.transaction_options = Some(options);
        self.options.transactional = true;
        self
    }

    /// 设置事务超时（毫秒）
    pub fn transaction_timeout_ms(mut self, timeout_ms: u64) -> Self {
        let mut options = self.options.transaction_options.unwrap_or_default();
        options.transaction_timeout_ms = timeout_ms;
        self.options.transaction_options = Some(options);
        self.options.transactional = true;
        self
    }

    /// 设置事务重试次数
    pub fn transaction_retries(mut self, retries: u32) -> Self {
        let mut options = self.options.transaction_options.unwrap_or_default();
        options.transaction_retries = retries;
        self.options.transaction_options = Some(options);
        self.options.transactional = true;
        self
    }

    /// 设置事务重试间隔（毫秒）
    pub fn transaction_retry_backoff_ms(mut self, retry_backoff_ms: u64) -> Self {
        let mut options = self.options.transaction_options.unwrap_or_default();
        options.transaction_retry_backoff_ms = retry_backoff_ms;
        self.options.transaction_options = Some(options);
        self.options.transactional = true;
        self
    }

    /// 启用幂等性
    pub fn enable_idempotence(mut self) -> Self {
        self.options.idempotent = true;
        if self.options.idempotence_options.is_none() {
            self.options.idempotence_options = Some(IdempotenceOptions::default());
        }
        self
    }

    /// 设置生产者 ID
    pub fn producer_id(mut self, producer_id: impl Into<String>) -> Self {
        let mut options = self.options.idempotence_options.unwrap_or_default();
        options.producer_id = producer_id.into();
        self.options.idempotence_options = Some(options);
        self.options.idempotent = true;
        self
    }

    /// 设置是否启用序列号
    pub fn enable_sequence_numbers(mut self, enable: bool) -> Self {
        let mut options = self.options.idempotence_options.unwrap_or_default();
        options.enable_sequence_numbers = enable;
        self.options.idempotence_options = Some(options);
        self.options.idempotent = true;
        self
    }

    /// 设置去重缓存大小
    pub fn deduplication_cache_size(mut self, size: usize) -> Self {
        let mut options = self.options.idempotence_options.unwrap_or_default();
        options.deduplication_cache_size = size;
        self.options.idempotence_options = Some(options);
        self.options.idempotent = true;
        self
    }

    /// 设置去重缓存过期时间（毫秒）
    pub fn deduplication_cache_expiry_ms(mut self, expiry_ms: u64) -> Self {
        let mut options = self.options.idempotence_options.unwrap_or_default();
        options.deduplication_cache_expiry_ms = expiry_ms;
        self.options.idempotence_options = Some(options);
        self.options.idempotent = true;
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
    /// 当前事务
    current_transaction: Option<Arc<TokioMutex<Transaction>>>,
    /// 事务序号
    transaction_sequence: Arc<TokioMutex<u64>>,
    /// 序列号计数器
    sequence_counter: Arc<TokioMutex<u64>>,
    /// 消息去重缓存
    deduplication_cache: Option<Arc<TokioMutex<LruCache<MessageId, bool>>>>,
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

        // 创建事务（如果启用）
        let current_transaction = if options.transactional {
            let tx_options = options.transaction_options.clone().unwrap_or_default();
            let tx_id = format!(
                "{}-{}-{}",
                tx_options.transaction_id_prefix,
                uuid::Uuid::new_v4(),
                0
            );
            let transaction = Transaction::new(tx_id, tx_options);
            Some(Arc::new(TokioMutex::new(transaction)))
        } else {
            None
        };

        // 创建去重缓存（如果启用幂等性）
        let deduplication_cache = if options.idempotent {
            let cache_size = options
                .idempotence_options
                .as_ref()
                .map(|opts| opts.deduplication_cache_size)
                .unwrap_or(1000);
            Some(Arc::new(TokioMutex::new(LruCache::new(
                std::num::NonZeroUsize::new(cache_size).unwrap(),
            ))))
        } else {
            None
        };

        Self {
            client,
            topic: topic.into(),
            options,
            batcher,
            compressor,
            current_transaction,
            transaction_sequence: Arc::new(TokioMutex::new(0)),
            sequence_counter: Arc::new(TokioMutex::new(0)),
            deduplication_cache,
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

        if self.options.idempotent {
            self.send_message_idempotent(message).await
        } else {
            self.send_message(message).await
        }
    }

    /// 同步幂等性发送消息
    ///
    /// 此方法确保消息只会被发送一次，即使在重试的情况下
    async fn send_message_idempotent(&self, message: Message) -> Result<SendResult> {
        // 如果未启用幂等性，则使用普通发送
        if !self.options.idempotent {
            return self.send_message(message).await;
        }

        let start_time = Instant::now();
        let message_size = message.value.len() + message.key.as_ref().map_or(0, |k| k.len());

        // 计算消息哈希
        let message_hash = MessageId::calculate_message_hash(
            message.key.as_ref().map(|k| k.as_slice()),
            &message.value,
        );

        // 获取生产者 ID
        let producer_id = self
            .options
            .idempotence_options
            .as_ref()
            .map(|opts| opts.producer_id.clone())
            .unwrap_or_else(|| format!("arroyo-producer-{}", uuid::Uuid::new_v4()));

        // 获取并递增序列号
        let sequence_number = {
            let mut counter = self.sequence_counter.lock().await;
            *counter += 1;
            *counter
        };

        // 创建消息 ID
        let message_id = MessageId::new(producer_id, sequence_number, message_hash);

        // 检查消息是否已经发送过
        if let Some(cache) = &self.deduplication_cache {
            let cache = cache.lock().await;
            if cache.contains(&message_id) {
                // 消息已经发送过，返回成功
                let latency = start_time.elapsed();
                return Ok(SendResult {
                    success: true,
                    error: None,
                    latency_ms: 1, // 确保大于0
                    size_bytes: message_size,
                    partition: None,
                    offset: None,
                });
            }
        }

        // 添加消息 ID 到消息头部
        let mut headers = message.headers.unwrap_or_default();
        headers.insert(
            "producer-id".to_string(),
            message_id.producer_id.clone().into_bytes(),
        );
        headers.insert(
            "sequence-number".to_string(),
            sequence_number.to_string().into_bytes(),
        );
        headers.insert(
            "message-hash".to_string(),
            message_hash.to_string().into_bytes(),
        );

        // 创建带有头部的消息
        let message_with_headers = Message {
            key: message.key,
            value: message.value,
            headers: Some(headers),
            timestamp: message.timestamp,
            partition: message.partition,
            offset: message.offset,
        };

        // 发送消息
        let result = self.send_message(message_with_headers).await;

        // 如果发送成功，将消息 ID 添加到缓存
        if let Ok(ref send_result) = result {
            if send_result.success {
                if let Some(cache) = &self.deduplication_cache {
                    let mut cache = cache.lock().await;
                    cache.put(message_id, true);
                }
            }
        }

        result
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

        if self.options.idempotent {
            self.send_message_idempotent(message).await
        } else {
            self.send_message(message).await
        }
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

        if self.options.idempotent {
            let producer = self.clone();
            let callback_clone = callback;

            tokio::spawn(async move {
                let result = producer.send_message_idempotent(message).await;
                if let Some(cb) = callback_clone {
                    match result {
                        Ok(send_result) => cb(send_result),
                        Err(e) => {
                            let send_result = SendResult {
                                success: false,
                                error: Some(e.to_string()),
                                latency_ms: 0,
                                size_bytes: 0,
                                partition: None,
                                offset: None,
                            };
                            cb(send_result);
                        }
                    }
                }
            });
        } else {
            self.send_message_async(message, callback);
        }
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

        if self.options.idempotent {
            let producer = self.clone();
            let callback_clone = callback;

            tokio::spawn(async move {
                let result = producer.send_message_idempotent(message).await;
                if let Some(cb) = callback_clone {
                    match result {
                        Ok(send_result) => cb(send_result),
                        Err(e) => {
                            let send_result = SendResult {
                                success: false,
                                error: Some(e.to_string()),
                                latency_ms: 0,
                                size_bytes: 0,
                                partition: None,
                                offset: None,
                            };
                            cb(send_result);
                        }
                    }
                }
            });
        } else {
            self.send_message_async(message, callback);
        }
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

    /// 初始化事务
    pub async fn init_transaction(&self) -> Result<TransactionResult> {
        if !self.options.transactional {
            return Err(Error::ValidationError("Producer is not configured for transactions".to_string()));
        }

        let start_time = Instant::now();
        let mut tx = match &self.current_transaction {
            Some(tx) => tx.lock().await,
            None => return Err(Error::ValidationError("No transaction available".to_string())),
        };

        if tx.state != TransactionState::Uninitialized {
            return Err(Error::ValidationError(format!("Transaction is already initialized, current state: {:?}", tx.state)));
        }

        // 初始化事务
        tx.initialize();

        let latency = start_time.elapsed();
        Ok(TransactionResult {
            success: true,
            error: None,
            latency_ms: latency.as_millis() as u64,
            transaction_id: tx.id.clone(),
        })
    }

    /// 开始事务
    pub async fn begin_transaction(&self) -> Result<TransactionResult> {
        if !self.options.transactional {
            return Err(Error::ValidationError("Producer is not configured for transactions".to_string()));
        }

        let start_time = Instant::now();
        let mut tx = match &self.current_transaction {
            Some(tx) => tx.lock().await,
            None => return Err(Error::ValidationError("No transaction available".to_string())),
        };

        if tx.state != TransactionState::Initialized {
            return Err(Error::ValidationError(format!("Transaction is not initialized, current state: {:?}", tx.state)));
        }

        // 开始事务
        tx.begin();

        let latency = start_time.elapsed();
        Ok(TransactionResult {
            success: true,
            error: None,
            latency_ms: latency.as_millis() as u64,
            transaction_id: tx.id.clone(),
        })
    }

    /// 在事务中发送消息
    pub async fn send_in_transaction(&self, key: Option<Vec<u8>>, value: Vec<u8>) -> Result<SendResult> {
        if !self.options.transactional {
            return Err(Error::ValidationError("Producer is not configured for transactions".to_string()));
        }

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

        self.send_message_in_transaction(message).await
    }

    /// 在事务中发送消息（带头部）
    pub async fn send_with_headers_in_transaction(
        &self,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        headers: HashMap<String, Vec<u8>>,
    ) -> Result<SendResult> {
        if !self.options.transactional {
            return Err(Error::ValidationError("Producer is not configured for transactions".to_string()));
        }

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

        self.send_message_in_transaction(message).await
    }

    /// 在事务中发送消息对象
    async fn send_message_in_transaction(&self, message: Message) -> Result<SendResult> {
        if !self.options.transactional {
            return Err(Error::ValidationError("Producer is not configured for transactions".to_string()));
        }

        let start_time = Instant::now();
        let message_size = message.value.len() + message.key.as_ref().map_or(0, |k| k.len());

        // 获取当前事务
        let mut tx = match &self.current_transaction {
            Some(tx) => tx.lock().await,
            None => return Err(Error::ValidationError("No transaction available".to_string())),
        };

        if tx.state != TransactionState::Started {
            return Err(Error::ValidationError(format!("Transaction is not started, current state: {:?}", tx.state)));
        }

        // 添加消息到事务
        tx.add_message(message);

        let latency = start_time.elapsed();
        Ok(SendResult {
            success: true,
            error: None,
            latency_ms: latency.as_millis() as u64,
            size_bytes: message_size,
            partition: None,
            offset: None,
        })
    }

    /// 提交事务
    pub async fn commit_transaction(&self) -> Result<TransactionResult> {
        if !self.options.transactional {
            return Err(Error::ValidationError("Producer is not configured for transactions".to_string()));
        }

        let start_time = Instant::now();
        let mut tx = match &self.current_transaction {
            Some(tx) => tx.lock().await,
            None => return Err(Error::ValidationError("No transaction available".to_string())),
        };

        if tx.state != TransactionState::Started {
            return Err(Error::ValidationError(format!("Transaction is not started, current state: {:?}", tx.state)));
        }

        // 获取事务中的所有消息
        let messages = tx.take_messages();
        if messages.is_empty() {
            // 如果没有消息，直接提交
            tx.commit();
            let latency = start_time.elapsed();
            return Ok(TransactionResult {
                success: true,
                error: None,
                latency_ms: latency.as_millis() as u64,
                transaction_id: tx.id.clone(),
            });
        }

        // 发送所有消息
        let result = self.send_batch(messages).await;

        match result {
            Ok(results) => {
                // 检查是否所有消息都发送成功
                let all_success = results.iter().all(|r| r.success);
                if all_success {
                    // 提交事务
                    tx.commit();
                    let latency = start_time.elapsed();
                    Ok(TransactionResult {
                        success: true,
                        error: None,
                        latency_ms: latency.as_millis() as u64,
                        transaction_id: tx.id.clone(),
                    })
                } else {
                    // 如果有消息发送失败，中止事务
                    tx.abort();
                    let errors: Vec<String> = results
                        .iter()
                        .filter_map(|r| r.error.clone())
                        .collect();
                    let error_message = format!("Failed to send messages: {}", errors.join(", "));
                    let latency = start_time.elapsed();
                    Ok(TransactionResult {
                        success: false,
                        error: Some(error_message),
                        latency_ms: latency.as_millis() as u64,
                        transaction_id: tx.id.clone(),
                    })
                }
            }
            Err(e) => {
                // 如果发送失败，中止事务
                tx.abort();
                let latency = start_time.elapsed();
                Ok(TransactionResult {
                    success: false,
                    error: Some(e.to_string()),
                    latency_ms: latency.as_millis() as u64,
                    transaction_id: tx.id.clone(),
                })
            }
        }
    }

    /// 中止事务
    pub async fn abort_transaction(&self) -> Result<TransactionResult> {
        if !self.options.transactional {
            return Err(Error::ValidationError("Producer is not configured for transactions".to_string()));
        }

        let start_time = Instant::now();
        let mut tx = match &self.current_transaction {
            Some(tx) => tx.lock().await,
            None => return Err(Error::ValidationError("No transaction available".to_string())),
        };

        if tx.state != TransactionState::Started {
            return Err(Error::ValidationError(format!("Transaction is not started, current state: {:?}", tx.state)));
        }

        // 中止事务
        tx.abort();

        let latency = start_time.elapsed();
        Ok(TransactionResult {
            success: true,
            error: None,
            latency_ms: latency.as_millis() as u64,
            transaction_id: tx.id.clone(),
        })
    }

    /// 创建新事务
    pub async fn new_transaction(&self) -> Result<TransactionResult> {
        if !self.options.transactional {
            return Err(Error::ValidationError("Producer is not configured for transactions".to_string()));
        }

        let start_time = Instant::now();

        // 获取并递增事务序号
        let mut seq = self.transaction_sequence.lock().await;
        *seq += 1;
        let sequence = *seq;

        // 创建新事务
        let tx_options = self.options.transaction_options.clone().unwrap_or_default();
        let tx_id = format!(
            "{}-{}-{}",
            tx_options.transaction_id_prefix,
            uuid::Uuid::new_v4(),
            sequence
        );
        let transaction = Transaction::new(tx_id.clone(), tx_options);

        // 替换当前事务
        *self.current_transaction.as_ref().unwrap().lock().await = transaction;

        let latency = start_time.elapsed();
        Ok(TransactionResult {
            success: true,
            error: None,
            latency_ms: latency.as_millis() as u64,
            transaction_id: tx_id,
        })
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
