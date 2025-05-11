use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, Result};
use arrow::compute::{partition, sort_to_indices, take};
use arrow_array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, OperatorConstructor,
    Registry,
};
use arroyo_rpc::grpc::api;
use arroyo_types::ArroyoSchema;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, RwLock};
use tracing::info;

use crate::arrow::sync::streams::KeyedCloneableStreamFuture;
use crate::arrow::table_config::timestamp_table_config;
use crate::arrow::utils::TableConfig;

type NextBatchFuture<K> = KeyedCloneableStreamFuture<K, RecordBatch>;

/// Count Window Aggregating Function
/// 
/// This window aggregates data based on count rather than time.
/// It triggers when a specified number of elements have been received.
pub struct CountAggregatingWindowFunc<K: Copy> {
    /// The size of the window (number of elements)
    size: usize,
    /// The aggregation execution plan
    aggregation_plan: Arc<dyn ExecutionPlan>,
    /// The schema of the input data
    input_schema: Arc<ArroyoSchema>,
    /// The current count of elements for each key
    counts: HashMap<K, usize>,
    /// The batches for each key
    batches: HashMap<K, Vec<RecordBatch>>,
    /// The receiver for the aggregation results
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    /// The futures for the aggregation results
    futures: Arc<Mutex<FuturesUnordered<NextBatchFuture<K>>>>,
}

impl<K: Copy + std::hash::Hash + Eq + Send + Sync + 'static> CountAggregatingWindowFunc<K> {
    /// Create a new CountAggregatingWindowFunc
    pub fn new(
        size: usize,
        aggregation_plan: Arc<dyn ExecutionPlan>,
        input_schema: Arc<ArroyoSchema>,
    ) -> Self {
        Self {
            size,
            aggregation_plan,
            input_schema,
            counts: HashMap::new(),
            batches: HashMap::new(),
            receiver: Arc::new(RwLock::new(None)),
            futures: Arc::new(Mutex::new(FuturesUnordered::new())),
        }
    }

    /// Process a batch for a specific key
    async fn process_key_batch(&mut self, key: K, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        // Get or create the entry for this key
        let count = self.counts.entry(key).or_insert(0);
        let key_batches = self.batches.entry(key).or_insert_with(Vec::new);
        
        // Add the batch to the key's batches
        key_batches.push(batch);
        *count += 1;
        
        // If we've reached the window size, trigger the aggregation
        if *count >= self.size {
            // Create a channel for the aggregation results
            let (sender, receiver) = unbounded_channel();
            *self.receiver.write().await = Some(receiver);
            
            // Create a single batch from all the batches for this key
            let combined_batch = RecordBatch::concat(
                key_batches[0].schema(),
                key_batches.as_slice(),
            )?;
            
            // Send the batch to the aggregation plan
            sender.send(combined_batch)?;
            
            // Reset the count and batches for this key
            *count = 0;
            key_batches.clear();
            
            // Execute the aggregation plan
            let result = self.aggregation_plan.execute(0)?;
            
            // Return the first batch from the result
            let batch = result.next().await.transpose()?.unwrap();
            return Ok(Some(batch));
        }
        
        Ok(None)
    }
}

#[async_trait::async_trait]
impl<K: Copy + std::hash::Hash + Eq + Send + Sync + 'static> ArrowOperator for CountAggregatingWindowFunc<K> {
    fn name(&self) -> String {
        "count_window".to_string()
    }
    
    fn display(&self) -> DisplayableOperator {
        DisplayableOperator {
            name: std::borrow::Cow::Borrowed("CountAggregatingWindowFunc"),
            fields: vec![
                ("size", self.size.to_string().into()),
                ("aggregation_plan", (&*self.aggregation_plan).into()),
            ],
        }
    }
    
    async fn on_start(&mut self, _ctx: &mut OperatorContext) {
        // Nothing to do on start
    }
    
    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        if batch.num_rows() == 0 {
            return;
        }
        
        // Partition the batch by key
        let partition_indices = if !self.input_schema.has_routing_keys() {
            // If there are no keys, treat the whole batch as one partition
            vec![0..batch.num_rows()]
        } else {
            // Otherwise, partition by the routing keys
            let key_columns = self.input_schema
                .routing_keys()
                .as_ref()
                .unwrap()
                .iter()
                .map(|key| {
                    batch.column_by_name(key)
                        .unwrap_or_else(|| panic!("Key column {} not found", key))
                        .clone()
                })
                .collect::<Vec<_>>();
                
            partition(key_columns.as_slice()).unwrap().ranges()
        };
        
        // Process each partition
        for range in partition_indices {
            if range.is_empty() {
                continue;
            }
            
            // Extract the key for this partition
            let key = if self.input_schema.has_routing_keys() {
                // Use the first row of the partition to get the key
                let key_row = range.start;
                let key_columns = self.input_schema
                    .routing_keys()
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|key| {
                        batch.column_by_name(key)
                            .unwrap_or_else(|| panic!("Key column {} not found", key))
                            .clone()
                    })
                    .collect::<Vec<_>>();
                
                // TODO: Extract the key from the key columns
                // For now, just use a dummy key
                0 as K
            } else {
                // If there are no keys, use a dummy key
                0 as K
            };
            
            // Extract the partition batch
            let columns = batch
                .columns()
                .iter()
                .map(|c| take(c, &arrow::array::Int64Array::from_iter_values(range.clone().map(|i| i as i64)), None).unwrap())
                .collect::<Vec<_>>();
                
            let partition_batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
            
            // Process the partition batch
            if let Ok(Some(result_batch)) = self.process_key_batch(key, partition_batch).await {
                collector.collect(result_batch).await.unwrap();
            }
        }
    }
    
    async fn handle_checkpoint(
        &mut self,
        _: arroyo_operator::context::CheckpointBarrier,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) {
        // Nothing to do for checkpoints
    }
    
    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }
}

/// Constructor for CountAggregatingWindowFunc
pub struct CountAggregatingWindowConstructor;

impl OperatorConstructor for CountAggregatingWindowConstructor {
    type ConfigT = api::CountWindowAggregateOperator;
    
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        // Parse the configuration
        let size = config.size as usize;
        let input_schema = Arc::new(ArroyoSchema::try_from(
            config.input_schema.ok_or_else(|| anyhow!("missing input schema"))?,
        )?);
        
        // Create the aggregation plan
        let aggregation_plan = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(false, input_schema.schema.clone()));
        
        // Create the operator
        let operator = CountAggregatingWindowFunc::<u64>::new(
            size,
            aggregation_plan,
            input_schema,
        );
        
        Ok(ConstructedOperator::from_operator(Box::new(operator)))
    }
}
