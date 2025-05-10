use crate::mongodb::MongoDBClient;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::StopMode;
use arroyo_rpc::ControlMessage;
use arroyo_types::*;
use async_trait::async_trait;
use futures::stream::StreamExt;
use mongodb::bson::{self, Bson, Document, doc};
use mongodb::options::FindOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

pub struct MongoDBSourceFunc {
    pub client: Arc<MongoDBClient>,
    pub database: String,
    pub collection: String,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
}

#[async_trait]
impl SourceOperator for MongoDBSourceFunc {
    fn name(&self) -> String {
        format!("mongodb-{}-{}", self.database, self.collection)
    }

    fn tables(&self) -> HashMap<String, arroyo_rpc::grpc::rpc::TableConfig> {
        HashMap::new()
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
            &[],
        );

        match self.run_int(ctx, collector).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;
                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}

impl MongoDBSourceFunc {
    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        let db = self.client.get_client().database(&self.database);
        let collection = db.collection::<Document>(&self.collection);

        // 设置轮询间隔，默认为1秒
        let poll_interval = Duration::from_millis(1000);
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // 设置查询选项
        let options = FindOptions::builder().batch_size(100).build();

        // 设置初始时间戳
        let mut last_timestamp = chrono::Utc::now().timestamp_millis();

        loop {
            select! {
                _ = interval.tick() => {
                    // 构建查询条件，获取上次查询后的新文档
                    let filter = doc! {
                        "timestamp": { "$gt": last_timestamp }
                    };

                    // 执行查询
                    let mut cursor = collection.find(filter, options.clone()).await
                        .map_err(|e| UserError::new("MongoDB query error", format!("{:?}", e)))?;

                    let mut count = 0;
                    while let Some(result) = cursor.next().await {
                        match result {
                            Ok(document) => {
                                // 更新最后时间戳
                                if let Some(ts) = document.get("timestamp") {
                                    if let Bson::Int64(ts) = ts {
                                        last_timestamp = *ts;
                                    }
                                }

                                // 将文档转换为JSON
                                let json = bson::to_document(&document)
                                    .map_err(|e| UserError::new("BSON conversion error", format!("{:?}", e)))?;

                                let json_string = serde_json::to_string(&json)
                                    .map_err(|e| UserError::new("JSON conversion error", format!("{:?}", e)))?;

                                // 处理文档
                                collector.deserialize_slice(
                                    json_string.as_bytes(),
                                    from_millis(last_timestamp as u64),
                                    None,
                                ).await?;

                                count += 1;
                                if collector.should_flush() {
                                    collector.flush_buffer().await?;
                                }
                            }
                            Err(e) => {
                                error!("Error reading MongoDB document: {:?}", e);
                            }
                        }
                    }

                    if count > 0 {
                        debug!("Read {} documents from MongoDB", count);
                    }

                    // 刷新缓冲区
                    if collector.should_flush() {
                        collector.flush_buffer().await?;
                    }
                }
                msg = ctx.control_rx.recv() => {
                    match msg {
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Received stop message with mode {:?}", mode);
                            match mode {
                                StopMode::Graceful => {
                                    // 刷新缓冲区并退出
                                    collector.flush_buffer().await?;
                                    return Ok(SourceFinishType::Graceful);
                                }
                                StopMode::Immediate => {
                                    return Ok(SourceFinishType::Immediate);
                                }
                            }
                        }
                        Some(ControlMessage::Checkpoint { .. }) => {
                            // 处理检查点
                            collector.flush_buffer().await?;
                        }
                        Some(ControlMessage::Commit { .. }) => {
                            // 忽略提交消息
                        }
                        Some(ControlMessage::LoadCompacted { .. }) => {
                            // 忽略加载压缩消息
                        }
                        Some(ControlMessage::NoOp) => {
                            // 忽略空操作
                        }
                        None => {
                            warn!("Control channel closed");
                            return Ok(SourceFinishType::Immediate);
                        }
                    }
                }
            }
        }
    }
}
