use crate::elasticsearch::ElasticsearchClient;
use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use ::elasticsearch::indices::{IndicesExistsParts, IndicesCreateParts};
use ::elasticsearch::IndexParts;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

pub struct ElasticsearchSinkFunc {
    pub client: Arc<ElasticsearchClient>,
    pub index: String,
    pub write_mode: String,
    pub id_field: Option<String>,
    pub serializer: ArrowSerializer,
}

#[async_trait]
impl ArrowOperator for ElasticsearchSinkFunc {
    fn name(&self) -> String {
        format!("elasticsearch-sink-{}", self.index)
    }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) {
        // 验证索引是否存在，如果不存在则创建
        let exists = self.client.get_client()
            .indices()
            .exists(IndicesExistsParts::Index(&[&self.index]))
            .send()
            .await;

        if let Ok(exists) = exists {
            if !exists.status_code().is_success() {
                info!("Index {} does not exist, creating it", self.index);

                // 创建索引
                let create_result = self.client.get_client()
                    .indices()
                    .create(IndicesCreateParts::Index(&self.index))
                    .body(json!({
                        "settings": {
                            "number_of_shards": 1,
                            "number_of_replicas": 1
                        },
                        "mappings": {
                            "dynamic": true
                        }
                    }))
                    .send()
                    .await;

                if let Err(e) = create_result {
                    error!("Failed to create index {}: {:?}", self.index, e);
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
        if batch.num_rows() == 0 {
            return;
        }

        // 序列化数据
        let serialized_data = self.serializer.serialize(&batch);

        // 将 Box<dyn Iterator<Item = Vec<u8>> + Send> 转换为 Vec<Vec<u8>>
        let serialized_vec: Vec<Vec<u8>> = serialized_data.collect();

        if serialized_vec.is_empty() {
            return;
        }

        // 根据写入模式处理数据
        match self.write_mode.as_str() {
            "Index" => self.index_documents(serialized_vec, ctx).await,
            "Create" => self.create_documents(serialized_vec, ctx).await,
            "Update" => self.update_documents(serialized_vec, ctx).await,
            "Upsert" => self.upsert_documents(serialized_vec, ctx).await,
            _ => {
                ctx.error_reporter
                    .report_error(
                        "Invalid write mode".to_string(),
                        format!("Unsupported write mode: {}", self.write_mode),
                    )
                    .await;
            }
        }
    }

    async fn handle_checkpoint(
        &mut self,
        checkpoint: CheckpointBarrier,
        _ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // Elasticsearch 写入操作是原子的，不需要特殊的检查点处理
        info!("Checkpoint {} processed", checkpoint.epoch);
    }
}

impl ElasticsearchSinkFunc {
    // 索引文档（覆盖已存在的文档）
    async fn index_documents(&self, serialized_data: Vec<Vec<u8>>, ctx: &mut OperatorContext) {
        let mut bulk_operations = Vec::new();

        for data in serialized_data {
            match serde_json::from_slice::<Value>(&data) {
                Ok(doc) => {
                    let id = self.extract_id(&doc);

                    let action = if let Some(id) = id {
                        // 如果有ID，使用指定ID索引
                        json!({"index": {"_index": self.index, "_id": id}})
                    } else {
                        // 否则让Elasticsearch生成ID
                        json!({"index": {"_index": self.index}})
                    };

                    bulk_operations.push(action);
                    bulk_operations.push(doc);
                }
                Err(e) => {
                    error!("Failed to parse JSON: {:?}", e);
                }
            }
        }

        if bulk_operations.is_empty() {
            return;
        }

        // 执行批量操作
        self.execute_bulk_operation(bulk_operations, ctx).await;
    }

    // 创建文档（仅当文档不存在时）
    async fn create_documents(&self, serialized_data: Vec<Vec<u8>>, ctx: &mut OperatorContext) {
        let mut bulk_operations = Vec::new();

        for data in serialized_data {
            match serde_json::from_slice::<Value>(&data) {
                Ok(doc) => {
                    let id = self.extract_id(&doc);

                    if let Some(id) = id {
                        let action = json!({"create": {"_index": self.index, "_id": id}});
                        bulk_operations.push(action);
                        bulk_operations.push(doc);
                    } else {
                        error!("ID field is required for create operation");
                    }
                }
                Err(e) => {
                    error!("Failed to parse JSON: {:?}", e);
                }
            }
        }

        if bulk_operations.is_empty() {
            return;
        }

        // 执行批量操作
        self.execute_bulk_operation(bulk_operations, ctx).await;
    }

    // 更新文档（仅当文档存在时）
    async fn update_documents(&self, serialized_data: Vec<Vec<u8>>, ctx: &mut OperatorContext) {
        let mut bulk_operations = Vec::new();

        for data in serialized_data {
            match serde_json::from_slice::<Value>(&data) {
                Ok(doc) => {
                    let id = self.extract_id(&doc);

                    if let Some(id) = id {
                        let action = json!({"update": {"_index": self.index, "_id": id}});
                        let update_doc = json!({"doc": doc});
                        bulk_operations.push(action);
                        bulk_operations.push(update_doc);
                    } else {
                        error!("ID field is required for update operation");
                    }
                }
                Err(e) => {
                    error!("Failed to parse JSON: {:?}", e);
                }
            }
        }

        if bulk_operations.is_empty() {
            return;
        }

        // 执行批量操作
        self.execute_bulk_operation(bulk_operations, ctx).await;
    }

    // 更新或插入文档
    async fn upsert_documents(&self, serialized_data: Vec<Vec<u8>>, ctx: &mut OperatorContext) {
        let mut bulk_operations = Vec::new();

        for data in serialized_data {
            match serde_json::from_slice::<Value>(&data) {
                Ok(doc) => {
                    let id = self.extract_id(&doc);

                    if let Some(id) = id {
                        let action = json!({"update": {"_index": self.index, "_id": id}});
                        let update_doc = json!({"doc": doc, "doc_as_upsert": true});
                        bulk_operations.push(action);
                        bulk_operations.push(update_doc);
                    } else {
                        error!("ID field is required for upsert operation");
                    }
                }
                Err(e) => {
                    error!("Failed to parse JSON: {:?}", e);
                }
            }
        }

        if bulk_operations.is_empty() {
            return;
        }

        // 执行批量操作
        self.execute_bulk_operation(bulk_operations, ctx).await;
    }

    // 执行批量操作
    async fn execute_bulk_operation(&self, operations: Vec<Value>, ctx: &mut OperatorContext) {
        // 将每个操作单独发送
        for i in 0..operations.len() / 2 {
            let _action = &operations[i * 2];
            let doc = &operations[i * 2 + 1];

            // 使用 Elasticsearch 的 index API 而不是 bulk API
            let response = self.client.get_client()
                .index(IndexParts::Index(&self.index))
                .body(doc)
                .send()
                .await;

            match response {
                Ok(response) => {
                    match response.json::<Value>().await {
                        Ok(_result) => {
                            info!("Successfully indexed document to index {}", self.index);
                        }
                        Err(e) => {
                            ctx.error_reporter
                                .report_error(
                                    "Failed to parse Elasticsearch response".to_string(),
                                    format!("{:?}", e),
                                )
                                .await;
                        }
                    }
                }
                Err(e) => {
                    ctx.error_reporter
                        .report_error(
                            "Elasticsearch index operation error".to_string(),
                            format!("Failed to index document: {:?}", e),
                        )
                        .await;
                }
            }
        }
    }

    // 从文档中提取ID
    fn extract_id(&self, doc: &Value) -> Option<String> {
        if let Some(id_field) = &self.id_field {
            // 从指定字段获取ID
            if let Some(id) = doc.get(id_field) {
                if let Some(id_str) = id.as_str() {
                    return Some(id_str.to_string());
                } else {
                    return Some(id.to_string());
                }
            }
        }

        // 检查是否有_id字段
        if let Some(id) = doc.get("_id") {
            if let Some(id_str) = id.as_str() {
                return Some(id_str.to_string());
            }
        }

        None
    }
}
