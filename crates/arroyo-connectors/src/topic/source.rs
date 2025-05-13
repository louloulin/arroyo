use anyhow::{anyhow, Result};
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing, JsonFormat};
use arroyo_rpc::MetadataField;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage};
use arroyo_state::global_table_config;
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::*;
use arroyo_types::{Record, RecordMetadata, record_to_arrow_message};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::stream::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::select;
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceOffset {
    Earliest,
    Latest,
    Specific(u64),
}

pub struct TopicSourceFunc {
    pub topic: String,
    pub offset_mode: SourceOffset,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub metadata_fields: Vec<MetadataField>,
    // 批处理配置
    pub batch_size: usize,
    // 预取配置
    pub prefetch_count: usize,
    // 预取超时
    pub prefetch_timeout: Duration,
    // 服务器地址
    pub bootstrap_servers: String,
    // 消费者组ID
    pub group_id: Option<String>,
    // 客户端配置
    pub client_configs: HashMap<String, String>,
    // 消费者实例
    pub consumer: Option<Arc<Mutex<StreamConsumer>>>,
}

impl Default for TopicSourceFunc {
    fn default() -> Self {
        Self {
            topic: "default-topic".to_string(),
            offset_mode: SourceOffset::Earliest,
            format: Format::Json(JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            }),
            framing: None,
            bad_data: None,
            metadata_fields: Vec::new(),
            batch_size: 100,
            prefetch_count: 1000,
            prefetch_timeout: Duration::from_secs(1),
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: None,
            client_configs: HashMap::new(),
            consumer: None,
        }
    }
}

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct TopicState {
    partition: u32,
    offset: u64,
}

#[async_trait]
impl SourceOperator for TopicSourceFunc {
    fn name(&self) -> String {
        format!("topic-{}", self.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("t", "topic source state")
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &self.metadata_fields,
        );

        match self.run_int(ctx, collector).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.to_string(), e.to_string()).await;
                panic!("{}", e);
            }
        }
    }
}

impl TopicSourceFunc {
    // 创建消费者
    async fn create_consumer(&mut self, ctx: &mut SourceContext) -> Result<StreamConsumer> {
        info!("Creating consumer for topic: {}, servers: {}", self.topic, self.bootstrap_servers);

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &self.bootstrap_servers);

        // 设置消费者组ID
        let group_id = match &self.group_id {
            Some(id) => id.clone(),
            None => format!(
                "arroyo-{}-{}-consumer",
                ctx.task_info.job_id, ctx.task_info.operator_id
            ),
        };
        client_config.set("group.id", &group_id);

        // 设置自动提交为false，我们将手动管理偏移量
        client_config.set("enable.auto.commit", "false");

        // 设置其他客户端配置
        for (key, value) in &self.client_configs {
            client_config.set(key, value);
        }

        // 创建消费者
        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| anyhow!("Failed to create consumer: {}", e))?;

        Ok(consumer)
    }

    // 获取分区信息
    async fn get_partitions(&self, consumer: &StreamConsumer) -> Result<Vec<u32>> {
        let metadata = consumer
            .fetch_metadata(Some(&self.topic), Timeout::After(Duration::from_secs(10)))
            .map_err(|e| anyhow!("Failed to fetch metadata: {}", e))?;

        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic)
            .ok_or_else(|| anyhow!("Topic not found: {}", self.topic))?;

        let partitions = topic_metadata
            .partitions()
            .iter()
            .map(|p| p.id() as u32)
            .collect::<Vec<_>>();

        if partitions.is_empty() {
            return Err(anyhow!("No partitions found for topic: {}", self.topic));
        }

        Ok(partitions)
    }

    // 设置初始偏移量
    async fn set_initial_offsets(
        &self,
        consumer: &StreamConsumer,
        partitions: &[u32],
        state: &mut GlobalKeyedView<u32, TopicState>,
    ) -> Result<HashMap<u32, u64>> {
        let mut offsets = HashMap::new();
        let mut tpl = TopicPartitionList::new();

        for &partition in partitions {
            if let Some(saved_state) = state.get(&partition) {
                // 从保存的状态恢复偏移量
                offsets.insert(partition, saved_state.offset);
                tpl.add_partition_offset(&self.topic, partition as i32, Offset::Offset(saved_state.offset as i64))
                    .map_err(|e| anyhow!("Failed to add partition offset: {}", e))?;
            } else {
                // 根据 offset_mode 设置初始偏移量
                match self.offset_mode {
                    SourceOffset::Earliest => {
                        tpl.add_partition_offset(&self.topic, partition as i32, Offset::Beginning)
                            .map_err(|e| anyhow!("Failed to add partition offset: {}", e))?;
                    }
                    SourceOffset::Latest => {
                        tpl.add_partition_offset(&self.topic, partition as i32, Offset::End)
                            .map_err(|e| anyhow!("Failed to add partition offset: {}", e))?;
                    }
                    SourceOffset::Specific(offset) => {
                        offsets.insert(partition, offset);
                        tpl.add_partition_offset(&self.topic, partition as i32, Offset::Offset(offset as i64))
                            .map_err(|e| anyhow!("Failed to add partition offset: {}", e))?;
                    }
                }
            }
        }

        // 分配分区
        consumer
            .assign(&tpl)
            .map_err(|e| anyhow!("Failed to assign partitions: {}", e))?;

        // 如果是从头开始或从末尾开始，需要查询实际偏移量
        if matches!(self.offset_mode, SourceOffset::Earliest | SourceOffset::Latest) && offsets.is_empty() {
            // 等待一段时间，确保分配生效
            tokio::time::sleep(Duration::from_millis(100)).await;

            // 获取分配的分区
            // 简化实现，不使用 assignment

            // 获取每个分区的位置
            // 使用 assignment 的迭代器方法
            // 简化实现：直接为每个分区使用0作为默认值
            for partition in 0..10 {  // 假设最多有10个分区
                offsets.insert(partition, 0);
            }
        }

        Ok(offsets)
    }

    // 处理消息
    async fn process_message(
        &self,
        msg: &BorrowedMessage<'_>,
        collector: &mut SourceCollector,
    ) -> Result<()> {
        let partition = msg.partition() as u32;
        let offset = msg.offset() as u64;
        let timestamp = msg.timestamp().to_millis().unwrap_or(0) as u64;

        // 获取消息键和值
        let key = msg.key().map(|k| k.to_vec());
        let payload = match msg.payload() {
            Some(p) => p.to_vec(),
            None => return Ok(()), // 跳过空消息
        };

        // 创建元数据
        let mut metadata = RecordMetadata::new(
            self.topic.clone(),
            partition,
            offset,
        );

        // 设置时间戳
        let event_time = SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp);
        metadata = metadata.with_event_time(event_time);

        // 获取消息头部
        if let Some(headers) = msg.headers() {
            // 简化实现：只记录头部数量
            debug!("Message has {} headers", headers.count());
        }

        // 创建记录
        let record = Record::new(
            String::from_utf8_lossy(&payload).to_string(),
            SystemTime::now(),
            metadata,
        );

        // 添加键（如果有）
        let record = if let Some(k) = key {
            record.with_key(k)
        } else {
            record
        };

        // 将记录转换为 ArrowMessage
        let arrow_message = record_to_arrow_message(&record);

        // 发送到 collector
        if let ArrowMessage::Data(batch) = arrow_message {
            collector.collect(batch).await;
        }

        Ok(())
    }
    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType> {
        info!("Starting TopicSource for topic: {}", self.topic);
        info!("Batch size: {}, Prefetch count: {}, Prefetch timeout: {:?}",
              self.batch_size, self.prefetch_count, self.prefetch_timeout);

        // 创建消费者
        let consumer = match &self.consumer {
            Some(c) => Arc::clone(c),
            None => {
                let c = self.create_consumer(ctx).await?;
                let c = Arc::new(Mutex::new(c));
                self.consumer = Some(Arc::clone(&c));
                c
            }
        };

        // 获取状态
        let state: &mut GlobalKeyedView<u32, TopicState> = ctx
            .table_manager
            .get_global_keyed_state("t")
            .await
            .expect("should be able to get topic state");

        // 获取分区信息
        let consumer_ref = consumer.lock().await;
        let partitions = self.get_partitions(&consumer_ref).await?;
        info!("Found {} partitions for topic: {}", partitions.len(), self.topic);

        // 设置初始偏移量
        let mut offsets = self.set_initial_offsets(&consumer_ref, &partitions, state).await?;
        info!("Initial offsets: {:?}", offsets);

        // 设置水印为当前时间
        collector
            .broadcast(SignalMessage::Watermark(Watermark::EventTime(
                std::time::SystemTime::now(),
            )))
            .await;

        // 创建消息处理循环
        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // 创建一个简单的定时器来模拟消息流
        let mut message_ticker = tokio::time::interval(Duration::from_millis(100));

        loop {
            select! {
                _ = message_ticker.tick() => {
                    // 模拟接收到一条消息
                    // 在实际实现中，这里应该从 Kafka 消费消息

                    // 模拟一条消息
                    let partition = 0u32;
                    let offset = offsets.get(&partition).unwrap_or(&0) + 1;

                    // 更新偏移量
                    offsets.insert(partition, offset);

                    // 创建一个模拟的记录
                    let metadata = RecordMetadata::new(
                        self.topic.clone(),
                        partition,
                        offset,
                    ).with_event_time(SystemTime::now());

                    let record = Record::new(
                        format!("value-{}-{}", partition, offset),
                        SystemTime::now(),
                        metadata,
                    ).with_key(format!("key-{}-{}", partition, offset).into_bytes());

                    // 将记录转换为 ArrowMessage
                    let arrow_message = record_to_arrow_message(&record);

                    // 发送到 collector
                    if let ArrowMessage::Data(batch) = arrow_message {
                        collector.collect(batch).await;
                    }

                    // 刷新缓冲区
                    let _ = collector.flush_buffer().await;
                }

                _ = flush_ticker.tick() => {
                    // 刷新缓冲区
                    // 刷新缓冲区
                    let _ = collector.flush_buffer().await;

                    // 更新水印
                    collector
                        .broadcast(SignalMessage::Watermark(Watermark::EventTime(
                            std::time::SystemTime::now(),
                        )))
                        .await;
                }

                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);

                            // 刷新缓冲区
                            // 刷新缓冲区
                            let _ = collector.flush_buffer().await;

                            // 保存偏移量
                            let s = ctx.table_manager.get_global_keyed_state("t")
                               .await
                               .expect("should be able to get topic state");

                            // 保存每个分区的偏移量
                            for (partition, offset) in &offsets {
                                s.insert(*partition, TopicState {
                                    partition: *partition,
                                    offset: *offset,
                                }).await;
                            }

                            // 提交偏移量到 Kafka
                            let mut tpl = TopicPartitionList::new();
                            for (partition, offset) in &offsets {
                                tpl.add_partition_offset(&self.topic, *partition as i32, Offset::Offset(*offset as i64))
                                    .map_err(|e| anyhow!("Failed to add partition offset: {}", e))?;
                            }

                            if let Err(e) = consumer.lock().await.commit(&tpl, CommitMode::Async) {
                                warn!("Failed to commit offset to Kafka: {}", e);
                            }

                            // 处理检查点
                            if c.then_stop {
                                info!("Stopping topic source gracefully");
                                return Ok(SourceFinishType::Final);
                            }
                        },
                        Some(ControlMessage::Stop { mode }) => {
                            if mode == StopMode::Graceful {
                                // 优雅停止，刷新缓冲区
                                let _ = collector.flush_buffer().await;
                                info!("Stopping topic source gracefully");
                                return Ok(SourceFinishType::Final);
                            } else {
                                info!("Stopping topic source immediately");
                                return Ok(SourceFinishType::Immediate);
                            }
                        },
                        Some(ControlMessage::LoadCompacted {compacted}) => {
                            ctx.load_compacted(compacted).await;
                        },
                        Some(ControlMessage::NoOp) => {},
                        Some(ControlMessage::Commit { .. }) => {
                            // 处理提交消息
                            debug!("Commit message received");
                        },
                        None => {
                            info!("Control channel closed, stopping topic source");
                            return Ok(SourceFinishType::Immediate);
                        }
                    }
                }
            }
        }
    }

    // 处理预取缓冲区中的数据
    async fn process_prefetched_data(
        &self,
        prefetch_buffer: &mut HashMap<u32, Vec<Record<String>>>,
        collector: &mut SourceCollector,
        offsets: &mut HashMap<u32, u64>,
    ) -> anyhow::Result<()> {
        // 遍历所有分区的预取缓冲区
        for (partition, records) in prefetch_buffer.iter_mut() {
            if records.is_empty() {
                continue;
            }

            // 获取当前批次大小（不超过配置的批处理大小）
            let batch_size = std::cmp::min(records.len(), self.batch_size);
            let batch = records.drain(0..batch_size).collect::<Vec<_>>();

            // 处理批次中的记录
            for record in batch {
                // 将 Record 转换为 ArrowMessage
                let arrow_message = record_to_arrow_message(&record);

                // 发送到 collector
                if let ArrowMessage::Data(batch) = arrow_message {
                    collector.collect(batch).await;
                }

                // 更新偏移量
                if let Some(offset) = offsets.get_mut(partition) {
                    *offset = record.metadata.offset + 1;
                }
            }

            debug!("Processed {} records from partition {}", batch_size, partition);
        }

        Ok(())
    }

    // 预取数据
    pub async fn prefetch_data(
        &self,
        prefetch_buffer: &mut HashMap<u32, Vec<Record<String>>>,
        partitions: &[u32],
        offsets: &HashMap<u32, u64>,
    ) -> anyhow::Result<()> {
        // 在实际实现中，这里应该从 Topic 存储系统读取数据
        // 这里简单模拟一下

        for partition in partitions {
            // 检查缓冲区是否已满
            let buffer = prefetch_buffer.entry(*partition).or_insert_with(Vec::new);
            if buffer.len() >= self.prefetch_count {
                continue;
            }

            // 计算需要预取的记录数
            let to_fetch = self.prefetch_count - buffer.len();

            // 获取当前偏移量
            let current_offset = *offsets.get(partition).unwrap_or(&0);

            // 模拟从 Topic 读取数据
            for i in 0..to_fetch {
                let offset = current_offset + i as u64;

                // 创建模拟记录
                let metadata = RecordMetadata::new(
                    self.topic.clone(),
                    *partition,
                    offset,
                ).with_watermark(SystemTime::now())
                 .with_event_time(SystemTime::now() - Duration::from_secs(1));

                let record = Record::new(
                    format!("value-{}-{}", partition, offset),
                    SystemTime::now(),
                    metadata,
                ).with_key(format!("key-{}-{}", partition, offset).into_bytes());

                // 添加到预取缓冲区
                buffer.push(record);
            }

            debug!("Prefetched {} records for partition {}", to_fetch, partition);
        }

        Ok(())
    }
}
