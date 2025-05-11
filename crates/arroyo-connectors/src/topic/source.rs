use anyhow::bail;
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
use async_trait::async_trait;
use bincode::{Decode, Encode};
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

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

        // 模拟消息处理循环
        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            select! {
                _ = flush_ticker.tick() => {
                    // 在实际实现中，这里应该从 Topic 存储系统读取数据
                    // 这里简单模拟一下

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
}
