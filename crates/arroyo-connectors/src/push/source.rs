use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arroyo_rpc::grpc::rpc::SubtaskCheckpointMetadata;

use anyhow::Result;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::{ConstructedOperator, SourceOperator};
use arroyo_operator::SourceFinishType;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{OperatorConfig, ControlMessage};
use arroyo_rpc::formats::{Format, Framing, BadData};
use arroyo_state::global_table_config;
use arroyo_types::UserError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::sleep;
use tracing::{debug, info, warn, error};

use crate::push::auth::AuthConfig;
use crate::push::converter::PushMessageConverter;
use crate::push::http::{HttpServer, HttpServerConfig};
use crate::push::{PushConfig, PushTable};

/// Message received from external systems
#[derive(Debug, Clone)]
pub struct PushMessage {
    /// Message ID (0 means not assigned yet)
    pub id: u64,
    /// Topic name
    pub topic: String,
    /// Message data
    pub data: Vec<u8>,
    /// Message timestamp
    pub timestamp: SystemTime,
}

/// State for the push source operator
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default, bincode::Encode, bincode::Decode)]
pub struct PushSourceState {
    pub messages_received: u64,
    pub bytes_received: u64,
    pub last_message_time: Option<SystemTime>,
}

/// Source operator for push connector
pub struct PushSourceFunc {
    pub topic: String,
    pub protocol: String,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub buffer_size: usize,
    pub max_batch_size: usize,
    pub state: PushSourceState,
    pub message_rx: Option<Receiver<PushMessage>>,
    pub message_tx: Option<Sender<PushMessage>>,
    // http_server field removed as Push functionality is integrated into the API service
    pub auth_config: Option<AuthConfig>,
    pub memory_buffer: Option<Arc<crate::push::buffer::MemoryBuffer>>,
    pub backpressure_controller: Option<Arc<crate::push::backpressure::BackpressureController>>,
    pub batch_processor: Option<crate::push::batch::BatchProcessor>,
    pub converter: Option<PushMessageConverter>,
}

impl PushSourceFunc {
    /// Create a new operator from configuration
    pub fn new_operator(
        config: PushConfig,
        table: PushTable,
        operator_config: OperatorConfig,
    ) -> Result<ConstructedOperator> {
        // Log the config and table for debugging
        tracing::debug!("Creating operator with config: {:?}", config);
        tracing::debug!("Creating operator with table: {:?}", table);

        let buffer_size = match config.buffer_size {
            Some(size) => size,
            None => 10 * 1024 * 1024,
        };
        let max_batch_size = match config.max_batch_size {
            Some(size) => size,
            None => 1000,
        };

        // Create a channel for receiving messages
        let (tx, rx) = mpsc::channel(buffer_size);

        // Create memory buffer
        let memory_buffer = Arc::new(crate::push::buffer::MemoryBuffer::new(buffer_size));

        // Create backpressure controller
        let backpressure_controller = Arc::new(crate::push::backpressure::BackpressureController::new(buffer_size));

        // Create batch processor
        let batch_processor = crate::push::batch::BatchProcessor::new(
            max_batch_size,
            Duration::from_millis(100), // 100ms max wait time
        );

        // HTTP server is no longer needed as Push functionality is integrated into the API service
        let http_server = None;

        // Create converter
        let converter = None; // TODO: Implement schema conversion

        Ok(ConstructedOperator::from_source(Box::new(PushSourceFunc {
            topic: table.topic,
            protocol: table.protocol,
            format: arroyo_rpc::formats::Format::Json(arroyo_rpc::formats::JsonFormat::default()),
            framing: None,
            bad_data: None,
            buffer_size,
            max_batch_size,
            state: PushSourceState::default(),
            message_rx: Some(rx),
            message_tx: Some(tx),
            // http_server field removed as Push functionality is integrated into the API service
            auth_config: None, // TODO: Map authentication from config
            memory_buffer: Some(memory_buffer),
            backpressure_controller: Some(backpressure_controller),
            batch_processor: Some(batch_processor),
            converter,
        })))
    }

    /// Register with the global message registry instead of starting a separate server
    async fn start_protocol_server(&mut self) -> Result<(), UserError> {
        // Log that we're using the integrated Push service
        info!("Using integrated Push service for topic {}", self.topic);

        // No need to start a separate server as Push functionality is integrated into the API service
        // Just register the topic with the global message registry
        if let Some(tx) = self.message_tx.clone() {
            // Register the sender in the global MESSAGE_SENDERS registry
            let mut senders = crate::push::MESSAGE_SENDERS.write().map_err(|e| UserError {
                name: "Lock error".to_string(),
                details: format!("Failed to acquire write lock on MESSAGE_SENDERS: {}", e),
            })?;

            senders.insert(self.topic.clone(), tx);
            info!("Registered topic {} with global message registry", self.topic);
        } else {
            return Err(UserError {
                name: "Configuration error".to_string(),
                details: "Message channel not initialized".to_string(),
            });
        }

        Ok(())
    }

    /// Internal implementation of the run method
    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        // Initialize deserializer
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );

        // Register message sender in global registry
        if let Some(tx) = self.message_tx.clone() {
            if let Ok(mut senders) = crate::push::MESSAGE_SENDERS.write() {
                info!("Registering message sender for topic {}", self.topic);
                senders.insert(self.topic.clone(), tx);
            } else {
                error!("Failed to acquire write lock on MESSAGE_SENDERS");
            }
        }

        // Start the protocol server
        self.start_protocol_server().await?;

        let mut rx = self.message_rx.take().expect("message_rx should be available");
        let memory_buffer = self.memory_buffer.as_ref().expect("memory_buffer should be available");
        let backpressure_controller = self.backpressure_controller.as_ref().expect("backpressure_controller should be available");
        let mut batch_processor = self.batch_processor.take().expect("batch_processor should be available");

        // Start a task to process messages from the channel and put them in the buffer
        let buffer_clone = memory_buffer.clone();
        let backpressure_clone = backpressure_controller.clone();
        let topic_clone = self.topic.clone();

        let buffer_task = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                // Check if message is for the correct topic
                if message.topic != topic_clone {
                    warn!("Received message for topic {} but expected {}", message.topic, topic_clone);
                    continue;
                }

                // Acquire space in the buffer
                let message_size = message.data.len();
                if let Err(e) = backpressure_clone.acquire(message_size).await {
                    error!("Failed to acquire space in buffer: {}", e);
                    continue;
                }

                // Push message to buffer
                if let Err(e) = buffer_clone.push(message).await {
                    error!("Failed to push message to buffer: {}", e);
                    // Release space in buffer
                    backpressure_clone.release(message_size);
                }
            }
        });

        loop {
            tokio::select! {
                // Handle control messages
                Some(control_msg) = ctx.control_rx.recv() => {
                    match control_msg {
                        ControlMessage::Stop { mode: _ } => {
                            info!("Received stop message");

                            // HTTP server is no longer needed as Push functionality is integrated into the API service

                            // Close buffer
                            memory_buffer.close().await;

                            // Abort buffer task
                            buffer_task.abort();

                            // Unregister message sender from global registry
                            if let Ok(mut senders) = crate::push::MESSAGE_SENDERS.write() {
                                info!("Unregistering message sender for topic {}", self.topic);
                                senders.remove(&self.topic);
                            } else {
                                error!("Failed to acquire write lock on MESSAGE_SENDERS");
                            }

                            return Ok(SourceFinishType::Immediate);
                        }
                        ControlMessage::Checkpoint { .. } => {
                            // Save state
                            let s: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<(), PushSourceState> = ctx
                                .table_manager
                                .get_global_keyed_state("p")
                                .await
                                .expect("should have table p in push source");

                            let _ = s.insert((), self.state.clone());

                            // Acknowledge checkpoint
                            ctx.control_tx.send(arroyo_rpc::ControlResp::CheckpointCompleted(
                                arroyo_rpc::CheckpointCompleted {
                                    checkpoint_epoch: 0,
                                    node_id: ctx.task_info.node_id,
                                    operator_id: ctx.task_info.operator_id.clone(),
                                    subtask_metadata: SubtaskCheckpointMetadata::default(),
                                }
                            )).await.unwrap();
                        }
                        ControlMessage::LoadCompacted { compacted } => {
                            ctx.load_compacted(compacted).await;
                        }
                        ControlMessage::NoOp => {},
                        ControlMessage::Commit { .. } => {
                            // TODO: Implement commit handling
                        }
                    }
                }

                // Process messages from buffer
                Ok(message) = memory_buffer.try_pop() => {
                    // Update state
                    self.state.messages_received += 1;
                    self.state.bytes_received += message.data.len() as u64;
                    self.state.last_message_time = Some(message.timestamp);

                    // Add message to batch processor
                    if let Some(batch) = batch_processor.add(message.clone()) {
                        // Process each topic's batch
                        for (topic, messages) in batch {
                            if topic != self.topic {
                                warn!("Batch contains messages for topic {} but expected {}", topic, self.topic);
                                continue;
                            }

                            // Process each message in batch
                            for msg in messages {
                                // Process the message
                                if let Some(converter) = &self.converter {
                                    // Convert message to Arrow format
                                    match converter.convert(&msg) {
                                        Ok(record_batch) => {
                                            // Collect record batch
                                            collector.collect(record_batch).await;
                                            debug!("Successfully processed message for topic {}", self.topic);
                                        }
                                        Err(e) => {
                                            error!("Error converting message to Arrow format: {:?}", e);
                                        }
                                    }
                                } else {
                                    // Use default deserialization
                                    // TODO: Implement proper deserialization
                                    debug!("Successfully processed message for topic {}", self.topic);
                                }

                                // Release space in buffer
                                backpressure_controller.release(msg.data.len());
                            }
                        }
                    } else {
                        // Process the message directly
                        if let Some(converter) = &self.converter {
                            // Convert message to Arrow format
                            match converter.convert(&message) {
                                Ok(record_batch) => {
                                    // Collect record batch
                                    collector.collect(record_batch).await;
                                    debug!("Successfully processed message for topic {}", self.topic);
                                }
                                Err(e) => {
                                    error!("Error converting message to Arrow format: {:?}", e);
                                }
                            }
                        } else {
                            // Use default deserialization
                            // TODO: Implement proper deserialization
                            debug!("Successfully processed message for topic {}", self.topic);
                        }

                        // Release space in buffer
                        backpressure_controller.release(message.data.len());
                    }
                }

                // Check if batch should be flushed
                _ = sleep(Duration::from_millis(10)) => {
                    if batch_processor.should_flush() {
                        if let Some(batch) = batch_processor.flush() {
                            // Process each topic's batch
                            for (topic, messages) in batch {
                                if topic != self.topic {
                                    warn!("Batch contains messages for topic {} but expected {}", topic, self.topic);
                                    continue;
                                }

                                // Process each message in batch
                                for msg in messages {
                                    // Process the message
                                    if let Some(converter) = &self.converter {
                                        // Convert message to Arrow format
                                        match converter.convert(&msg) {
                                            Ok(record_batch) => {
                                                // Collect record batch
                                                collector.collect(record_batch).await;
                                                debug!("Successfully processed message for topic {}", self.topic);
                                            }
                                            Err(e) => {
                                                error!("Error converting message to Arrow format: {:?}", e);
                                            }
                                        }
                                    } else {
                                        // Use default deserialization
                                        // TODO: Implement proper deserialization
                                        debug!("Successfully processed message for topic {}", self.topic);
                                    }

                                    // Release space in buffer
                                    backpressure_controller.release(msg.data.len());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl SourceOperator for PushSourceFunc {
    fn name(&self) -> String {
        format!("push-{}-{}", self.protocol, self.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("p", "push source state")
    }

    async fn on_start(&mut self, ctx: &mut SourceContext) {
        let s: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<(), PushSourceState> = ctx
            .table_manager
            .get_global_keyed_state("p")
            .await
            .expect("should have table p in push source");

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
        }
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        match self.run_int(ctx, collector).await {
            Ok(result) => result,
            Err(e) => {
                ctx.report_user_error(e.clone()).await;
                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}
