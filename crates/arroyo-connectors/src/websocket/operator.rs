use async_trait::async_trait;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::SystemTime;

use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage};
use arroyo_state::global_table_config;
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::{SignalMessage, UserError, Watermark};
use bincode::{Decode, Encode};
use futures::{SinkExt, StreamExt};
use tokio::select;
// use tokio_tungstenite::tungstenite::handshake::client::generate_key;
// use tokio_tungstenite::tungstenite::http::Uri;
// use tokio_tungstenite::{connect_async, tungstenite};
use tracing::{debug, info};
// use tungstenite::http::Request;

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct WebsocketSourceState {}

pub struct WebsocketSourceFunc {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub subscription_messages: Vec<String>,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub state: WebsocketSourceState,
}

#[async_trait]
impl SourceOperator for WebsocketSourceFunc {
    fn name(&self) -> String {
        "WebsocketSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("e", "websocket source state")
    }

    async fn on_start(&mut self, ctx: &mut SourceContext) {
        let s: &mut GlobalKeyedView<(), WebsocketSourceState> = ctx
            .table_manager
            .get_global_keyed_state("e")
            .await
            .expect("couldn't get state for websocket");

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
        }
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

impl WebsocketSourceFunc {
    async fn our_handle_control_message(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
        msg: Option<ControlMessage>,
    ) -> Option<SourceFinishType> {
        match msg? {
            ControlMessage::Checkpoint(c) => {
                debug!("starting checkpointing {}", ctx.task_info.task_index);
                let s: &mut GlobalKeyedView<(), WebsocketSourceState> = ctx
                    .table_manager
                    .get_global_keyed_state("e")
                    .await
                    .expect("couldn't get state for websocket");
                s.insert((), self.state.clone()).await;

                if self.start_checkpoint(c, ctx, collector).await {
                    return Some(SourceFinishType::Immediate);
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping websocket source: {:?}", mode);

                match mode {
                    StopMode::Graceful => {
                        return Some(SourceFinishType::Graceful);
                    }
                    StopMode::Immediate => {
                        return Some(SourceFinishType::Immediate);
                    }
                }
            }
            ControlMessage::Commit { .. } => {
                unreachable!("sources shouldn't receive commit messages");
            }
            ControlMessage::LoadCompacted { compacted } => {
                ctx.load_compacted(compacted).await;
            }
            ControlMessage::NoOp => {}
        }
        None
    }

    async fn handle_message(
        &mut self,
        msg: &[u8],
        collector: &mut SourceCollector,
    ) -> Result<(), UserError> {
        collector
            .deserialize_slice(msg, SystemTime::now(), None)
            .await?;

        if collector.should_flush() {
            collector.flush_buffer().await?;
        }

        Ok(())
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        // WebSocket connection is temporarily disabled
        ctx.report_error(
            "WebSocket connection is temporarily disabled".to_string(),
            "WebSocket support is not available in this build".to_string(),
        )
        .await;
        return Err(UserError::new(
            "WebSocket connection is temporarily disabled",
            "WebSocket support is not available in this build",
        ));

        // This code is unreachable due to the early return above
        // It's kept here as a reference for when WebSocket support is re-enabled
    }
}
