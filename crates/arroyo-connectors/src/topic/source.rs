use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::MetadataField;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage};
use arroyo_state::global_table_config;
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::*;
use arroyo_types::{Record, RecordMetadata, arrow_message_to_record};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceOffset {
    Earliest,
    Latest,
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
    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> anyhow::Result<SourceFinishType> {
        // 这里是一个简单的模拟实现，实际上需要连接到 Topic 存储系统
        // 在完整实现中，我们需要：
        // 1. 连接到 Topic 存储系统
        // 2. 获取分区信息
        // 3. 根据 offset_mode 确定起始偏移量
        // 4. 读取数据并发送到 collector

        info!("Starting TopicSource for topic: {}", self.topic);
        info!("Batch size: {}, Prefetch count: {}, Prefetch timeout: {:?}",
              self.batch_size, self.prefetch_count, self.prefetch_timeout);

        // 模拟从状态恢复
        let state: &mut GlobalKeyedView<u32, TopicState> = ctx
            .table_manager
            .get_global_keyed_state("t")
            .await
            .expect("should be able to get topic state");

        // 模拟分区和偏移量
        let mut offsets = HashMap::new();
        let partitions = vec![0u32, 1u32]; // 假设有两个分区

        for partition in &partitions {
            if let Some(saved_state) = state.get(partition) {
                offsets.insert(*partition, saved_state.offset);
            } else {
                // 根据 offset_mode 设置初始偏移量
                let initial_offset = match self.offset_mode {
                    SourceOffset::Earliest => 0,
                    SourceOffset::Latest => {
                        // 在实际实现中，这里应该查询最新的偏移量
                        // 这里简单模拟为 100
                        100
                    }
                };
                offsets.insert(*partition, initial_offset);
            }
        }

        // 设置水印为当前时间
        collector
            .broadcast(SignalMessage::Watermark(Watermark::EventTime(
                std::time::SystemTime::now(),
            )))
            .await;

        // 创建预取缓冲区
        let mut prefetch_buffer: HashMap<u32, Vec<Record<String>>> = HashMap::new();

        // 模拟消息处理循环
        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // 预取超时计时器
        let mut prefetch_timer = tokio::time::interval(self.prefetch_timeout);
        prefetch_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            select! {
                _ = flush_ticker.tick() => {
                    // 在实际实现中，这里应该从 Topic 存储系统读取数据
                    // 这里简单模拟一下

                    // 处理预取缓冲区中的数据
                    self.process_prefetched_data(&mut prefetch_buffer, collector, &mut offsets).await?;

                    // 预取新数据
                    self.prefetch_data(&mut prefetch_buffer, &partitions, &offsets).await?;

                    // 更新水印
                    collector
                        .broadcast(SignalMessage::Watermark(Watermark::EventTime(
                            std::time::SystemTime::now(),
                        )))
                        .await;
                }

                _ = prefetch_timer.tick() => {
                    // 预取超时，强制处理缓冲区中的数据
                    if !prefetch_buffer.is_empty() {
                        debug!("Prefetch timeout, processing buffered data");
                        self.process_prefetched_data(&mut prefetch_buffer, collector, &mut offsets).await?;
                    }
                }

                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);

                            // 处理缓冲区中的所有数据
                            self.process_prefetched_data(&mut prefetch_buffer, collector, &mut offsets).await?;

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

                            // 处理检查点
                            if c.then_stop {
                                info!("Stopping topic source gracefully");
                                return Ok(SourceFinishType::Final);
                            }
                        },
                        Some(ControlMessage::Stop { mode }) => {
                            if mode == StopMode::Graceful {
                                // 优雅停止，处理缓冲区中的所有数据
                                self.process_prefetched_data(&mut prefetch_buffer, collector, &mut offsets).await?;
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
