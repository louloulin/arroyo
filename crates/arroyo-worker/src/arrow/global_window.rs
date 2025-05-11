use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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

/// Trigger type for Global Window
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerType {
    /// Trigger based on element count
    Count { size: usize },
    /// Trigger based on processing time
    ProcessingTime { interval: Duration },
    /// Trigger based on watermark
    Watermark,
    /// Trigger based on punctuation (specific elements in the stream)
    Punctuation,
}

/// Global Window Aggregating Function
/// 
/// This window aggregates all data into a single global window.
/// It triggers based on various conditions defined by triggers.
pub struct GlobalWindowFunc<K: Copy> {
    /// The trigger for this window
    trigger: TriggerType,
    /// The aggregation execution plan
    aggregation_plan: Arc<dyn ExecutionPlan>,
    /// The schema of the input data
    input_schema: Arc<ArroyoSchema>,
    /// The batches for each key
    batches: HashMap<K, Vec<RecordBatch>>,
    /// The counts for each key (used for Count trigger)
    counts: HashMap<K, usize>,
    /// The last trigger time for each key (used for ProcessingTime trigger)
    last_trigger_times: HashMap<K, SystemTime>,
    /// The receiver for the aggregation results
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    /// The futures for the aggregation results
    futures: Arc<Mutex<FuturesUnordered<NextBatchFuture<K>>>>,
}

impl<K: Copy + std::hash::Hash + Eq + Send + Sync + 'static> GlobalWindowFunc<K> {
    /// Create a new GlobalWindowFunc
    pub fn new(
        trigger: TriggerType,
        aggregation_plan: Arc<dyn ExecutionPlan>,
        input_schema: Arc<ArroyoSchema>,
    ) -> Self {
        Self {
            trigger,
            aggregation_plan,
            input_schema,
            batches: HashMap::new(),
            counts: HashMap::new(),
            last_trigger_times: HashMap::new(),
            receiver: Arc::new(RwLock::new(None)),
            futures: Arc::new(Mutex::new(FuturesUnordered::new())),
        }
    }

    /// Process a batch for a specific key
    async fn process_key_batch(&mut self, key: K, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        // Get or create the entry for this key
        let key_batches = self.batches.entry(key).or_insert_with(Vec::new);
        
        // Add the batch to the key's batches
        key_batches.push(batch);
        
        // Check if we should trigger based on the trigger type
        let should_trigger = match self.trigger {
            TriggerType::Count { size } => {
                let count = self.counts.entry(key).or_insert(0);
                *count += 1;
                *count >= size
            },
            TriggerType::ProcessingTime { interval } => {
                let now = SystemTime::now();
                let last_time = self.last_trigger_times.entry(key).or_insert(now);
                if now.duration_since(*last_time).unwrap_or(Duration::ZERO) >= interval {
                    *last_time = now;
                    true
                } else {
                    false
                }
            },
            TriggerType::Watermark => {
                // Watermark trigger is handled in the on_watermark method
                false
            },
            TriggerType::Punctuation => {
                // Punctuation trigger is not implemented yet
                false
            },
        };
        
        if should_trigger {
            // Reset the count for this key if using Count trigger
            if let TriggerType::Count { .. } = self.trigger {
                self.counts.insert(key, 0);
            }
            
            // If there are no batches, return None
            if key_batches.is_empty() {
                return Ok(None);
            }
            
            // Create a channel for the aggregation results
            let (sender, receiver) = unbounded_channel();
            *self.receiver.write().await = Some(receiver);
            
            // Create a single batch from all the batches for this key
            let combined_batch = RecordBatch::concat(
                key_batches[0].schema(),
                key_batches.as_slice(),
            )?;
            
            // Send the batch to the aggregation plan
            sender.send(combined_batch.clone())?;
            
            // Execute the aggregation plan
            let result = self.aggregation_plan.execute(0)?;
            
            // Return the first batch from the result
            let batch = result.next().await.transpose()?.unwrap();
            
            // For global windows, we keep the data for future triggers
            // We don't clear the batches here
            
            return Ok(Some(batch));
        }
        
        Ok(None)
    }
    
    /// Handle a watermark
    async fn on_watermark(&mut self, watermark: SystemTime, collector: &mut dyn Collector) -> Result<()> {
        // Only trigger if we're using a Watermark trigger
        if self.trigger != TriggerType::Watermark {
            return Ok(());
        }
        
        // Trigger for each key
        for (key, batches) in &self.batches {
            if batches.is_empty() {
                continue;
            }
            
            // Create a channel for the aggregation results
            let (sender, receiver) = unbounded_channel();
            *self.receiver.write().await = Some(receiver);
            
            // Create a single batch from all the batches for this key
            let combined_batch = RecordBatch::concat(
                batches[0].schema(),
                batches.as_slice(),
            )?;
            
            // Send the batch to the aggregation plan
            sender.send(combined_batch.clone())?;
            
            // Execute the aggregation plan
            let result = self.aggregation_plan.execute(0)?;
            
            // Collect the result
            if let Some(batch) = result.next().await.transpose()? {
                collector.collect(batch).await?;
            }
        }
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl<K: Copy + std::hash::Hash + Eq + Send + Sync + 'static> ArrowOperator for GlobalWindowFunc<K> {
    fn name(&self) -> String {
        "global_window".to_string()
    }
    
    fn display(&self) -> DisplayableOperator {
        DisplayableOperator {
            name: std::borrow::Cow::Borrowed("GlobalWindowFunc"),
            fields: vec![
                ("trigger", format!("{:?}", self.trigger).into()),
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
        ctx: &mut OperatorContext,
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
        
        // Check for watermark trigger
        if let Some(watermark) = ctx.last_present_watermark() {
            if let Err(e) = self.on_watermark(watermark, collector).await {
                info!("Error processing watermark: {:?}", e);
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

/// Constructor for GlobalWindowFunc
pub struct GlobalWindowConstructor;

impl OperatorConstructor for GlobalWindowConstructor {
    type ConfigT = api::GlobalWindowAggregateOperator;
    
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        // Parse the configuration
        let input_schema = Arc::new(ArroyoSchema::try_from(
            config.input_schema.ok_or_else(|| anyhow!("missing input schema"))?,
        )?);
        
        // Determine the trigger type
        let trigger = match config.trigger_type {
            0 => TriggerType::Count { size: config.count_size as usize },
            1 => TriggerType::ProcessingTime { interval: Duration::from_micros(config.time_interval_micros) },
            2 => TriggerType::Watermark,
            3 => TriggerType::Punctuation,
            _ => return Err(anyhow!("Invalid trigger type: {}", config.trigger_type)),
        };
        
        // Create the aggregation plan
        let aggregation_plan = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(false, input_schema.schema.clone()));
        
        // Create the operator
        let operator = GlobalWindowFunc::<u64>::new(
            trigger,
            aggregation_plan,
            input_schema,
        );
        
        Ok(ConstructedOperator::from_operator(Box::new(operator)))
    }
}
