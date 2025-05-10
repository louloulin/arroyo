use crate::mongodb::MongoDBClient;
use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use mongodb::bson::{self, Document};
use mongodb::options::InsertManyOptions;
use std::sync::Arc;
use tracing::{error, info};

pub struct MongoDBSinkFunc {
    pub client: Arc<MongoDBClient>,
    pub database: String,
    pub collection: String,
    pub serializer: ArrowSerializer,
}

#[async_trait]
impl ArrowOperator for MongoDBSinkFunc {
    fn name(&self) -> String {
        format!("mongodb-sink-{}-{}", self.database, self.collection)
    }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) {
        // 验证数据库和集合是否存在，如果不存在则创建
        let db = self.client.get_client().database(&self.database);
        let collection_names = db.list_collection_names(None).await.unwrap_or_default();

        if !collection_names.contains(&self.collection) {
            info!("Collection {} does not exist, it will be created automatically", self.collection);
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

        let db = self.client.get_client().database(&self.database);
        let collection = db.collection::<Document>(&self.collection);

        // 序列化数据
        let serialized_data = self.serializer.serialize(&batch);

        // 将 Box<dyn Iterator<Item = Vec<u8>> + Send> 转换为 Vec<Vec<u8>>
        let serialized_vec: Vec<Vec<u8>> = serialized_data.collect();

        if serialized_vec.is_empty() {
            return;
        }

        // 将序列化的数据转换为MongoDB文档
        let documents: Vec<Document> = serialized_vec
            .into_iter()
            .filter_map(|data| {
                match serde_json::from_slice::<serde_json::Value>(&data) {
                    Ok(json) => {
                        match bson::to_document(&json) {
                            Ok(doc) => Some(doc),
                            Err(e) => {
                                error!("Failed to convert JSON to BSON document: {:?}", e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse JSON: {:?}", e);
                        None
                    }
                }
            })
            .collect();

        if documents.is_empty() {
            return;
        }

        // 插入文档
        let options = InsertManyOptions::builder().ordered(false).build();
        match collection.insert_many(documents, options).await {
            Ok(result) => {
                info!(
                    "Successfully inserted {} documents into MongoDB collection {}.{}",
                    result.inserted_ids.len(),
                    self.database,
                    self.collection
                );
            }
            Err(e) => {
                ctx.error_reporter
                    .report_error(
                        "MongoDB insert error".to_string(),
                        format!("Failed to insert documents: {:?}", e),
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
        // MongoDB 插入操作是原子的，不需要特殊的检查点处理
        info!("Checkpoint {} processed", checkpoint.epoch);
    }
}
