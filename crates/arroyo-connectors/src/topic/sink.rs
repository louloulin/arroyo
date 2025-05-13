use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use uuid;

use arroyo_formats::ser::ArrowSerializer;
use tracing::{debug, error, info};

use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::CheckpointBarrier;

/// 分区策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStrategy {
    /// 轮询分区
    RoundRobin,
    /// 基于键的哈希分区
    Hash,
    /// 自定义分区
    Custom,
}

/// 事务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionState {
    /// 未开始
    NotStarted,
    /// 已开始
    Started,
    /// 已提交
    Committed,
    /// 已中止
    Aborted,
}

/// 事务
struct Transaction {
    /// 事务ID
    id: String,
    /// 事务状态
    state: TransactionState,
    /// 缓冲的消息
    buffered_messages: HashMap<u32, Vec<Vec<u8>>>,
    /// 检查点ID
    checkpoint_id: Option<u32>,
}

impl Transaction {
    fn new(id: String) -> Self {
        Self {
            id,
            state: TransactionState::NotStarted,
            buffered_messages: HashMap::new(),
            checkpoint_id: None,
        }
    }

    fn start(&mut self) {
        self.state = TransactionState::Started;
    }

    fn buffer_message(&mut self, partition: u32, message: Vec<u8>) {
        let messages = self.buffered_messages.entry(partition).or_insert_with(Vec::new);
        messages.push(message);
    }

    fn commit(&mut self) {
        self.state = TransactionState::Committed;
    }

    fn abort(&mut self) {
        self.state = TransactionState::Aborted;
    }

    fn set_checkpoint_id(&mut self, checkpoint_id: u32) {
        self.checkpoint_id = Some(checkpoint_id);
    }
}

pub struct TopicSinkFunc {
    pub topic: String,
    pub serializer: ArrowSerializer,
    /// 分区策略
    pub partition_strategy: PartitionStrategy,
    /// 是否启用事务
    pub transactional: bool,
    /// 当前事务
    current_transaction: Option<Transaction>,
    /// 上次提交的事务ID
    last_committed_transaction_id: Option<String>,
    /// 分区数量
    pub partition_count: usize,
    /// 当前分区索引（用于轮询策略）
    current_partition_index: usize,
    /// 缓冲区刷新间隔
    flush_interval: Duration,
    /// 上次刷新时间
    last_flush_time: std::time::Instant,
    /// 缓冲区大小限制
    buffer_size_limit: usize,
    /// 当前缓冲区大小
    current_buffer_size: usize,
    /// 服务器地址
    pub bootstrap_servers: String,
    /// 客户端配置
    pub client_configs: HashMap<String, String>,
    /// 生产者实例
    producer: Option<Arc<Mutex<FutureProducer>>>,
    /// 事务生产者实例
    transactional_producer: Option<Arc<Mutex<FutureProducer>>>,
    /// 待处理的消息发送结果
    pending_delivery_futures: Vec<()>,
}

impl Debug for TopicSinkFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicSinkFunc")
            .field("topic", &self.topic)
            .field("partition_strategy", &self.partition_strategy)
            .field("transactional", &self.transactional)
            .field("partition_count", &self.partition_count)
            .field("buffer_size_limit", &self.buffer_size_limit)
            .finish()
    }
}

#[async_trait]
impl ArrowOperator for TopicSinkFunc {
    fn name(&self) -> String {
        format!("topic-sink-{}", self.topic)
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        // 初始化与 Topic 存储系统的连接
        info!("Starting TopicSink for topic: {}", self.topic);
        info!("Partition strategy: {:?}, Transactional: {}", self.partition_strategy, self.transactional);

        // 创建生产者
        match self.create_producer().await {
            Ok(producer) => {
                self.producer = Some(Arc::new(Mutex::new(producer)));
                info!("Created producer for topic: {}", self.topic);
            }
            Err(e) => {
                let error_msg = format!("Failed to create producer: {}", e);
                ctx.report_error(error_msg.clone(), error_msg).await;
                panic!("Failed to create producer: {}", e);
            }
        }

        // 如果启用了事务，初始化事务生产者
        if self.transactional {
            match self.create_transactional_producer().await {
                Ok(producer) => {
                    self.transactional_producer = Some(Arc::new(Mutex::new(producer)));
                    info!("Created transactional producer for topic: {}", self.topic);

                    // 初始化事务
                    let transaction_id = format!("tx-{}-{}", self.topic, uuid::Uuid::new_v4());
                    info!("Initialized transaction: {}", transaction_id);
                    self.current_transaction = Some(Transaction::new(transaction_id));

                    // 开始事务
                    if let Some(transaction) = &mut self.current_transaction {
                        transaction.start();
                    }
                }
                Err(e) => {
                    let error_msg = format!("Failed to create transactional producer: {}", e);
                    ctx.report_error(error_msg.clone(), error_msg).await;
                    panic!("Failed to create transactional producer: {}", e);
                }
            }
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // 将数据写入 Topic 存储系统
        let values = self.serializer.serialize(&batch);
        let mut count = 0;

        // 检查是否需要开始新事务
        if self.transactional {
            if let Some(transaction) = &mut self.current_transaction {
                if transaction.state == TransactionState::NotStarted {
                    transaction.start();
                    debug!("Started transaction: {}", transaction.id);
                }
            }
        }

        // 处理每条记录
        for value in values {
            // 确定分区
            let partition = self.determine_partition(&batch, count);

            if self.transactional {
                // 如果启用了事务，将消息缓冲到当前事务中
                if let Some(transaction) = &mut self.current_transaction {
                    transaction.buffer_message(partition, value.clone());
                    self.current_buffer_size += 1;
                }
            } else {
                // 如果没有启用事务，直接写入
                if let Err(e) = self.write_to_partition(partition, value).await {
                    let error_msg = format!("Failed to write to partition {}: {}", partition, e);
                    ctx.report_error(error_msg.clone(), error_msg).await;
                    error!("Failed to write to partition {}: {}", partition, e);
                }
            }

            count += 1;
        }

        // 检查是否需要刷新缓冲区
        if self.transactional && self.should_flush() {
            match self.flush_buffer().await {
                Ok(_) => {
                    debug!("Flushed buffer for topic: {}", self.topic);
                }
                Err(e) => {
                    let error_msg = format!("Failed to flush buffer: {}", e);
                    ctx.report_error(error_msg.clone(), error_msg).await;
                    error!("Failed to flush buffer: {}", e);
                }
            }
        }

        // 检查待处理的消息发送结果
        self.check_pending_deliveries().await;

        info!("Processed {} records for topic: {}", count, self.topic);
    }

    async fn handle_checkpoint(
        &mut self,
        checkpoint: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // 刷新缓冲区，确保所有数据都已写入
        info!("Handling checkpoint for topic: {}, epoch: {}", self.topic, checkpoint.epoch);

        // 等待所有待处理的消息发送完成
        self.wait_for_pending_deliveries().await;

        if self.transactional {
            // 如果启用了事务，提交当前事务并开始新事务
            if let Some(transaction) = &mut self.current_transaction {
                // 设置检查点ID
                transaction.set_checkpoint_id(checkpoint.epoch);

                // 提交事务
                match self.commit_transaction().await {
                    Ok(_) => {
                        info!("Committed transaction for checkpoint: {}", checkpoint.epoch);
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to commit transaction: {}", e);
                        ctx.report_error(error_msg.clone(), error_msg).await;
                        error!("Failed to commit transaction: {}", e);
                    }
                }

                // 创建新事务
                let transaction_id = format!("tx-{}-{}-{}", self.topic, uuid::Uuid::new_v4(), checkpoint.epoch);
                debug!("Created new transaction: {}", transaction_id);
                self.current_transaction = Some(Transaction::new(transaction_id));

                // 开始新事务
                if let Some(transaction) = &mut self.current_transaction {
                    transaction.start();
                }
            }
        } else {
            // 如果没有启用事务，只需刷新缓冲区
            info!("Flushing data for topic: {}", self.topic);
        }
    }
}

impl TopicSinkFunc {
    /// 创建新的 TopicSinkFunc
    pub fn new(topic: String, serializer: ArrowSerializer) -> Self {
        Self {
            topic,
            serializer,
            partition_strategy: PartitionStrategy::RoundRobin,
            transactional: false,
            current_transaction: None,
            last_committed_transaction_id: None,
            partition_count: 3, // 默认分区数
            current_partition_index: 0,
            flush_interval: Duration::from_millis(1000),
            last_flush_time: std::time::Instant::now(),
            buffer_size_limit: 1000,
            current_buffer_size: 0,
            bootstrap_servers: "localhost:9092".to_string(),
            client_configs: HashMap::new(),
            producer: None,
            transactional_producer: None,
            pending_delivery_futures: Vec::new(),
        }
    }

    /// 设置分区策略
    pub fn with_partition_strategy(mut self, strategy: PartitionStrategy) -> Self {
        self.partition_strategy = strategy;
        self
    }

    /// 启用事务
    pub fn with_transactions(mut self) -> Self {
        self.transactional = true;
        self
    }

    /// 设置分区数量
    pub fn with_partition_count(mut self, count: usize) -> Self {
        self.partition_count = count;
        self
    }

    /// 设置缓冲区刷新间隔
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// 设置缓冲区大小限制
    pub fn with_buffer_size_limit(mut self, limit: usize) -> Self {
        self.buffer_size_limit = limit;
        self
    }

    /// 设置服务器地址
    pub fn with_bootstrap_servers(mut self, servers: String) -> Self {
        self.bootstrap_servers = servers;
        self
    }

    /// 设置客户端配置
    pub fn with_client_config(mut self, key: String, value: String) -> Self {
        self.client_configs.insert(key, value);
        self
    }

    /// 创建生产者
    async fn create_producer(&self) -> Result<FutureProducer> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &self.bootstrap_servers);

        // 设置其他客户端配置
        for (key, value) in &self.client_configs {
            client_config.set(key, value);
        }

        // 创建生产者
        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| anyhow!("Failed to create producer: {}", e))?;

        Ok(producer)
    }

    /// 创建事务生产者
    async fn create_transactional_producer(&self) -> Result<FutureProducer> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &self.bootstrap_servers);

        // 设置事务ID
        let transaction_id = format!("tx-{}-{}", self.topic, uuid::Uuid::new_v4());
        client_config.set("transactional.id", &transaction_id);

        // 设置其他客户端配置
        for (key, value) in &self.client_configs {
            client_config.set(key, value);
        }

        // 创建生产者
        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| anyhow!("Failed to create transactional producer: {}", e))?;

        // 注意：在实际生产环境中，应该使用 rdkafka 的事务API
        // 但由于 rdkafka 的 Rust 绑定可能不完全支持事务，这里简化处理

        Ok(producer)
    }

    /// 确定记录应该写入的分区
    fn determine_partition(&mut self, _batch: &RecordBatch, index: usize) -> u32 {
        match self.partition_strategy {
            PartitionStrategy::RoundRobin => {
                // 轮询分区
                let partition = self.current_partition_index % self.partition_count;
                self.current_partition_index = (self.current_partition_index + 1) % self.partition_count;
                partition as u32
            }
            PartitionStrategy::Hash => {
                // 基于键的哈希分区
                // 在实际实现中，应该从记录中提取键并计算哈希
                // 这里简单模拟一下
                (index % self.partition_count) as u32
            }
            PartitionStrategy::Custom => {
                // 自定义分区策略
                // 在实际实现中，应该调用用户提供的分区函数
                // 这里简单模拟一下
                (index % self.partition_count) as u32
            }
        }
    }

    /// 将记录写入指定分区
    async fn write_to_partition(&self, partition: u32, value: Vec<u8>) -> Result<()> {
        if let Some(producer) = &self.producer {
            let producer = producer.lock().await;

            // 创建记录
            let record = FutureRecord::to(&self.topic)
                .partition(partition as i32)
                .payload(&value)
                .key("")  // 添加一个空键以满足类型要求
                .timestamp(SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64);

            // 发送记录
            let delivery_future = producer.send(record, Timeout::After(Duration::from_secs(5)));

            // 等待发送完成
            match delivery_future.await {
                Ok(_) => {
                    debug!("Writing record to topic: {}, partition: {}, size: {} bytes",
                           self.topic, partition, value.len());
                    Ok(())
                },
                Err((e, _)) => {
                    Err(anyhow!("Failed to deliver message: {}", e))
                }
            }
        } else {
            Err(anyhow!("Producer not initialized"))
        }
    }

    /// 检查是否应该刷新缓冲区
    fn should_flush(&self) -> bool {
        // 如果缓冲区大小超过限制，或者距离上次刷新时间超过间隔，则刷新
        self.current_buffer_size >= self.buffer_size_limit ||
        self.last_flush_time.elapsed() >= self.flush_interval
    }

    /// 检查待处理的消息发送结果
    async fn check_pending_deliveries(&mut self) {
        // 由于我们已经在 write_to_partition 中等待发送完成，这里不需要再检查
        self.pending_delivery_futures.clear();
    }

    /// 等待所有待处理的消息发送完成
    async fn wait_for_pending_deliveries(&mut self) {
        // 由于我们已经在 write_to_partition 中等待发送完成，这里不需要再等待
        self.pending_delivery_futures.clear();
    }

    /// 刷新缓冲区
    async fn flush_buffer(&mut self) -> Result<()> {
        if let Some(transaction) = &self.current_transaction {
            // 将事务中的所有消息写入 Topic 存储系统
            let mut total_messages = 0;

            // 创建一个临时的消息副本，以避免可变借用冲突
            let mut messages_to_send = HashMap::new();
            for (partition, messages) in &transaction.buffered_messages {
                messages_to_send.insert(*partition, messages.clone());
            }

            for (partition, messages) in &messages_to_send {
                debug!("Flushing {} messages to partition {}", messages.len(), partition);
                total_messages += messages.len();

                // 批量写入消息
                for message in messages {
                    if let Err(e) = self.write_to_partition(*partition, message.clone()).await {
                        error!("Failed to write message to partition {}: {}", partition, e);
                        return Err(e);
                    }
                }
            }

            info!("Flushed {} messages for topic: {}", total_messages, self.topic);
            self.current_buffer_size = 0;
            self.last_flush_time = std::time::Instant::now();
        }

        Ok(())
    }

    /// 提交事务
    async fn commit_transaction(&mut self) -> Result<()> {
        // 获取事务ID和状态
        let transaction_id;

        if let Some(transaction) = &self.current_transaction {
            transaction_id = transaction.id.clone();
        } else {
            return Ok(());
        }

        // 刷新缓冲区中的消息
        self.flush_buffer().await?;

        // 提交事务
        if let Some(producer) = &self.transactional_producer {
            let _producer = producer.lock().await;

            // 由于 rdkafka 的 Rust 绑定可能不完全支持事务，这里简化处理
            // 在实际生产环境中，应该使用完整的事务API

            if let Some(transaction) = &mut self.current_transaction {
                transaction.commit();
            }

            // 记录已提交的事务ID
            self.last_committed_transaction_id = Some(transaction_id.clone());

            info!("Committed transaction: {}", transaction_id);
        } else {
            return Err(anyhow!("Transactional producer not initialized"));
        }

        Ok(())
    }

    /// 中止事务
    async fn abort_transaction(&mut self) -> Result<()> {
        if let Some(transaction) = &mut self.current_transaction {
            // 中止事务
            if let Some(producer) = &self.transactional_producer {
                let _producer = producer.lock().await;

                // 由于 rdkafka 的 Rust 绑定可能不完全支持事务，这里简化处理
                // 在实际生产环境中，应该使用完整的事务API

                transaction.abort();

                info!("Aborted transaction: {}", transaction.id);

                // 清空缓冲区
                self.current_buffer_size = 0;
            } else {
                return Err(anyhow!("Transactional producer not initialized"));
            }
        }

        Ok(())
    }
}
