use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Topic 模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    /// Topic 名称
    pub name: String,
    /// 分区数量
    pub partitions: u32,
    /// 复制因子
    pub replication_factor: u32,
    /// 创建时间
    pub created_at: String,
    /// 配置选项
    pub config: HashMap<String, String>,
}

/// Topic 分区信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPartition {
    /// 分区 ID
    pub id: u32,
    /// 分区 Leader
    pub leader: u32,
    /// 分区副本
    pub replicas: Vec<u32>,
    /// 同步副本
    pub isr: Vec<u32>,
}

/// Topic 分区统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPartitionStats {
    /// 分区 ID
    pub id: u32,
    /// 最早偏移量
    pub earliest_offset: u64,
    /// 最新偏移量
    pub latest_offset: u64,
    /// 消息数量
    pub message_count: u64,
    /// 分区大小（字节）
    pub size_bytes: u64,
}

/// Topic 统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicStats {
    /// Topic 名称
    pub name: String,
    /// 分区统计信息
    pub partitions: Vec<TopicPartitionStats>,
    /// 总消息数量
    pub total_message_count: u64,
    /// 总大小（字节）
    pub total_size_bytes: u64,
}

/// 消息模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// 消息键
    pub key: Option<Vec<u8>>,
    /// 消息值
    pub value: Vec<u8>,
    /// 消息头部
    pub headers: Option<HashMap<String, Vec<u8>>>,
    /// 消息时间戳
    pub timestamp: Option<i64>,
    /// 分区
    pub partition: Option<u32>,
    /// 偏移量
    pub offset: Option<u64>,
}

/// 消费者组
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroup {
    /// 消费者组 ID
    pub id: String,
    /// 消费者组状态
    pub state: String,
    /// 消费者数量
    pub members: u32,
    /// 分配策略
    pub protocol: String,
    /// 协调器 ID
    pub coordinator_id: u32,
}

/// 消费者组成员
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMember {
    /// 成员 ID
    pub id: String,
    /// 客户端 ID
    pub client_id: String,
    /// 客户端主机
    pub client_host: String,
    /// 分配的分区
    pub assignments: HashMap<String, Vec<u32>>,
}

/// 消费者组偏移量
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupOffset {
    /// Topic 名称
    pub topic: String,
    /// 分区 ID
    pub partition: u32,
    /// 当前偏移量
    pub offset: u64,
    /// 最新偏移量
    pub latest_offset: u64,
    /// 偏移量差距
    pub lag: u64,
    /// 最后提交时间
    pub last_commit_time: String,
}
