use arrow::array::RecordBatch;
use async_trait::async_trait;
use std::fmt::Debug;

use arroyo_formats::ser::ArrowSerializer;
use tracing::info;

use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::CheckpointBarrier;

pub struct TopicSinkFunc {
    pub topic: String,
    pub serializer: ArrowSerializer,
}

impl Debug for TopicSinkFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicSinkFunc")
            .field("topic", &self.topic)
            .finish()
    }
}

#[async_trait]
impl ArrowOperator for TopicSinkFunc {
    fn name(&self) -> String {
        format!("topic-sink-{}", self.topic)
    }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) {
        // 在实际实现中，这里应该初始化与 Topic 存储系统的连接
        info!("Starting TopicSink for topic: {}", self.topic);
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // 在实际实现中，这里应该将数据写入 Topic 存储系统
        let values = self.serializer.serialize(&batch);
        let mut count = 0;

        // 模拟写入操作
        for _v in values {
            // 实际写入操作
            count += 1;
        }

        info!("Writing {} records to topic: {}", count, self.topic);
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // 在实际实现中，这里应该刷新缓冲区，确保所有数据都已写入
        info!("Flushing data for topic: {}", self.topic);
    }
}
