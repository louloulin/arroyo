//! # Arroyo SDK
//!
//! Arroyo SDK 是一个 Rust 库，用于简化与 Arroyo 流处理平台的交互。
//! 它提供了一组简单的 API，使开发者能够轻松地连接到 Arroyo 服务器并管理流处理作业。
//!
//! ## 主要组件
//!
//! - `client`: 提供与 Arroyo 服务器交互的主要客户端
//! - `error`: 定义 SDK 使用的错误类型
//! - `models`: 定义与 Arroyo API 交互的数据模型
//! - `connection`: 管理数据源和目标连接
//! - `job`: 管理流处理作业
//! - `pipeline`: 管理流处理管道
//! - `sql`: 提供 SQL 查询支持
//!
//! ## 示例
//!
//! ```rust,no_run
//! use arroyo_sdk::client::ArroyoClient;
//! use arroyo_sdk::error::Result;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // 创建客户端
//!     let client = ArroyoClient::new("http://localhost:8000")?;
//!
//!     // 获取所有作业
//!     let jobs = client.get_jobs().await?;
//!     println!("当前作业: {:?}", jobs);
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod compression;
pub mod consumer;
pub mod consumer_group;
pub mod error;
pub mod failover;
pub mod flow_control;
pub mod models;
pub mod connection;
pub mod job;
pub mod pipeline;
pub mod producer;
pub mod retry;
pub mod sql;
pub mod topic;
pub mod util;

// Re-export commonly used types
pub use client::ArroyoClient;
pub use compression::CompressionType;
pub use connection::{
    ConnectionConfig, ConnectionPool, ConnectionPoolStats, ConnectionState, PooledArroyoClient,
    SessionInfo,
};
pub use consumer::{Consumer, ConsumerBuilder, ConsumerOptions, SubscriptionType};
pub use consumer_group::{
    ConsumerGroupManager, GroupMember, GroupState, HeartbeatResponse, JoinGroupResponse,
    PartitionAssignmentStrategy, RebalanceResponse,
};
pub use error::{Error, Result};
pub use failover::{FailoverConfig, FailoverManager, FailoverState};
pub use producer::{
    AdaptiveBatchingConfig, AdaptiveBatchingStrategy, Producer, ProducerBuilder, ProducerOptions,
    SendCallback, SendResult, Transaction, TransactionOptions, TransactionResult, TransactionState,
};
pub use retry::{RetryConfig, RetryStrategy, RetryableErrorType};
pub use topic::{TopicBuilder, TopicOptions};
