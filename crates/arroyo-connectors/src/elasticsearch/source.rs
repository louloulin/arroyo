use crate::elasticsearch::ElasticsearchClient;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::StopMode;
use arroyo_rpc::ControlMessage;
use arroyo_types::*;
use async_trait::async_trait;
use ::elasticsearch::SearchParts;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

pub struct ElasticsearchSourceFunc {
    pub client: Arc<ElasticsearchClient>,
    pub index: String,
    pub query: Option<String>,
    pub batch_size: u32,
    pub poll_interval: u32,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
}

#[async_trait]
impl SourceOperator for ElasticsearchSourceFunc {
    fn name(&self) -> String {
        format!("elasticsearch-source-{}", self.index)
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

impl ElasticsearchSourceFunc {
    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        // 设置轮询间隔
        let poll_interval = Duration::from_millis(self.poll_interval as u64);
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // 设置初始时间戳
        let mut last_timestamp = chrono::Utc::now().timestamp_millis();

        // 构建基本查询
        let base_query = match &self.query {
            Some(q) => serde_json::from_str::<Value>(q)
                .map_err(|e| UserError::new("Invalid Elasticsearch query", format!("{:?}", e)))?,
            None => json!({"match_all": {}}),
        };

        loop {
            select! {
                _ = interval.tick() => {
                    // 构建查询条件，获取上次查询后的新文档
                    let query = json!({
                        "bool": {
                            "must": base_query,
                            "filter": {
                                "range": {
                                    "@timestamp": {
                                        "gt": last_timestamp
                                    }
                                }
                            }
                        }
                    });

                    // 执行查询
                    let response = self.client.get_client()
                        .search(SearchParts::Index(&[&self.index]))
                        .body(json!({
                            "query": query,
                            "sort": [
                                {"@timestamp": {"order": "asc"}}
                            ],
                            "size": self.batch_size
                        }))
                        .send()
                        .await
                        .map_err(|e| UserError::new("Elasticsearch query error", format!("{:?}", e)))?;

                    // 解析响应
                    let search_response = response.json::<Value>().await
                        .map_err(|e| UserError::new("Failed to parse Elasticsearch response", format!("{:?}", e)))?;

                    let hits = search_response["hits"]["hits"].as_array();

                    if let Some(hits) = hits {
                        let count = hits.len();

                        for hit in hits {
                            // 获取文档
                            let source = &hit["_source"];

                            // 获取时间戳
                            if let Some(timestamp) = source["@timestamp"].as_str() {
                                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(timestamp) {
                                    last_timestamp = dt.timestamp_millis();
                                }
                            }

                            // 处理文档
                            let json_string = serde_json::to_string(source)
                                .map_err(|e| UserError::new("JSON conversion error", format!("{:?}", e)))?;

                            collector.deserialize_slice(
                                json_string.as_bytes(),
                                from_millis(last_timestamp as u64),
                                None,
                            ).await?;

                            if collector.should_flush() {
                                collector.flush_buffer().await?;
                            }
                        }

                        if count > 0 {
                            debug!("Read {} documents from Elasticsearch", count);
                        }
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
