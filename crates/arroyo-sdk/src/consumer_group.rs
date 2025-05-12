use crate::client::ArroyoClient;
use crate::error::Result;
use crate::models::ConsumerGroup;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info};
use uuid::Uuid;

/// 加入组响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinGroupResponse {
    /// 成员 ID
    pub member_id: String,
    /// 消费者组 ID
    pub group_id: String,
    /// 消费者组状态
    pub state: GroupState,
    /// 分配的分区
    pub assigned_partitions: HashMap<String, Vec<u32>>,
    /// 生成 ID
    pub generation_id: u32,
}

/// 心跳响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// 是否需要再平衡
    pub rebalance_needed: bool,
    /// 消费者组状态
    pub state: GroupState,
}

/// 再平衡响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceResponse {
    /// 分配的分区
    pub assigned_partitions: HashMap<String, Vec<u32>>,
    /// 生成 ID
    pub generation_id: u32,
}

/// 分区分配策略
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionAssignmentStrategy {
    /// 轮询分配策略
    RoundRobin,
    /// 范围分配策略
    Range,
    /// 一致性哈希分配策略
    ConsistentHash,
    /// 粘性分配策略（尽量保持现有分配）
    Sticky,
}

impl Default for PartitionAssignmentStrategy {
    fn default() -> Self {
        Self::RoundRobin
    }
}

/// 消费者组成员信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMember {
    /// 成员 ID
    pub member_id: String,
    /// 客户端 ID
    pub client_id: String,
    /// 订阅的 Topic
    pub topics: Vec<String>,
    /// 会话超时（毫秒）
    pub session_timeout_ms: u64,
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: u64,
    /// 上次心跳时间
    pub last_heartbeat: SystemTime,
    /// 分配的分区
    pub assigned_partitions: HashMap<String, Vec<u32>>,
}

/// 消费者组状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupState {
    /// 准备中
    Preparing,
    /// 完成分区分配
    CompletingRebalance,
    /// 稳定状态
    Stable,
    /// 正在进行再平衡
    PreparingRebalance,
    /// 已关闭
    Dead,
}

impl Default for GroupState {
    fn default() -> Self {
        Self::Preparing
    }
}

/// 消费者组管理器
#[derive(Debug, Clone)]
pub struct ConsumerGroupManager {
    /// 客户端
    client: ArroyoClient,
    /// 消费者组 ID
    group_id: String,
    /// 成员 ID
    member_id: String,
    /// 客户端 ID
    client_id: String,
    /// 会话超时（毫秒）
    session_timeout_ms: u64,
    /// 心跳间隔（毫秒）
    heartbeat_interval_ms: u64,
    /// 分区分配策略
    assignment_strategy: PartitionAssignmentStrategy,
    /// 订阅的 Topic
    topics: Vec<String>,
    /// 分配的分区
    assigned_partitions: Arc<RwLock<HashMap<String, Vec<u32>>>>,
    /// 是否已加入组
    joined: Arc<RwLock<bool>>,
    /// 是否正在运行
    running: Arc<RwLock<bool>>,
    /// 再平衡锁
    rebalance_lock: Arc<Mutex<()>>,
}

impl ConsumerGroupManager {
    /// 创建新的消费者组管理器
    pub fn new(
        client: ArroyoClient,
        group_id: impl Into<String>,
        client_id: impl Into<String>,
        session_timeout_ms: u64,
        heartbeat_interval_ms: u64,
        assignment_strategy: PartitionAssignmentStrategy,
    ) -> Self {
        Self {
            client,
            group_id: group_id.into(),
            member_id: format!("member-{}", Uuid::new_v4()),
            client_id: client_id.into(),
            session_timeout_ms,
            heartbeat_interval_ms,
            assignment_strategy,
            topics: Vec::new(),
            assigned_partitions: Arc::new(RwLock::new(HashMap::new())),
            joined: Arc::new(RwLock::new(false)),
            running: Arc::new(RwLock::new(false)),
            rebalance_lock: Arc::new(Mutex::new(())),
        }
    }

    /// 设置订阅的 Topic
    pub fn subscribe(&mut self, topics: Vec<impl Into<String>>) {
        self.topics = topics.into_iter().map(|t| t.into()).collect();
    }

    /// 启动消费者组管理器
    pub async fn start(&self) -> Result<()> {
        // 设置运行状态
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        // 加入消费者组
        self.join_group().await?;

        // 启动心跳线程
        self.start_heartbeat_thread().await;

        Ok(())
    }

    /// 停止消费者组管理器
    pub async fn stop(&self) -> Result<()> {
        // 设置运行状态
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }
        *running = false;
        drop(running);

        // 离开消费者组
        self.leave_group().await?;

        Ok(())
    }

    /// 加入消费者组
    async fn join_group(&self) -> Result<()> {
        // 获取再平衡锁
        let _lock = self.rebalance_lock.lock().await;

        // 检查是否已加入组
        let joined = *self.joined.read().await;
        if joined {
            return Ok(());
        }

        info!("Joining consumer group: {}", self.group_id);

        // 发送加入组请求
        let response = self
            .client
            .client
            .post(&format!(
                "{}/api/consumer-groups/{}/members",
                self.client.base_url, self.group_id
            ))
            .json(&serde_json::json!({
                "member_id": self.member_id,
                "client_id": self.client_id,
                "topics": self.topics,
                "session_timeout_ms": self.session_timeout_ms,
                "heartbeat_interval_ms": self.heartbeat_interval_ms,
                "assignment_strategy": self.assignment_strategy,
            }))
            .send()
            .await?;

        // 处理响应
        let join_response: JoinGroupResponse = self.client.handle_response(response).await?;

        // 更新分配的分区
        let mut assigned_partitions = self.assigned_partitions.write().await;
        *assigned_partitions = join_response.assigned_partitions;
        drop(assigned_partitions);

        // 设置已加入组
        let mut joined = self.joined.write().await;
        *joined = true;
        drop(joined);

        info!(
            "Joined consumer group: {}, assigned partitions: {:?}",
            self.group_id,
            self.assigned_partitions.read().await
        );

        Ok(())
    }

    /// 离开消费者组
    async fn leave_group(&self) -> Result<()> {
        // 获取再平衡锁
        let _lock = self.rebalance_lock.lock().await;

        // 检查是否已加入组
        let joined = *self.joined.read().await;
        if !joined {
            return Ok(());
        }

        info!("Leaving consumer group: {}", self.group_id);

        // 发送离开组请求
        let response = self
            .client
            .client
            .delete(&format!(
                "{}/api/consumer-groups/{}/members/{}",
                self.client.base_url, self.group_id, self.member_id
            ))
            .send()
            .await?;

        // 处理响应
        self.client.handle_empty_response(response).await?;

        // 清空分配的分区
        let mut assigned_partitions = self.assigned_partitions.write().await;
        assigned_partitions.clear();
        drop(assigned_partitions);

        // 设置未加入组
        let mut joined = self.joined.write().await;
        *joined = false;
        drop(joined);

        info!("Left consumer group: {}", self.group_id);

        Ok(())
    }

    /// 发送心跳
    pub async fn send_heartbeat(&self) -> Result<()> {
        // 检查是否已加入组
        let joined = *self.joined.read().await;
        if !joined {
            return Ok(());
        }

        debug!("Sending heartbeat to consumer group: {}", self.group_id);

        // 发送心跳请求
        let response = self
            .client
            .client
            .post(&format!(
                "{}/api/consumer-groups/{}/members/{}/heartbeat",
                self.client.base_url, self.group_id, self.member_id
            ))
            .send()
            .await?;

        // 处理响应
        let heartbeat_response: HeartbeatResponse = self.client.handle_response(response).await?;

        // 如果需要再平衡，触发再平衡
        if heartbeat_response.rebalance_needed {
            info!("Rebalance needed, triggering rebalance");
            self.trigger_rebalance().await?;
        }

        Ok(())
    }

    /// 触发再平衡
    pub async fn trigger_rebalance(&self) -> Result<()> {
        // 获取再平衡锁
        let _lock = self.rebalance_lock.lock().await;

        info!("Starting rebalance for consumer group: {}", self.group_id);

        // 发送再平衡请求
        let response = self
            .client
            .client
            .post(&format!(
                "{}/api/consumer-groups/{}/rebalance",
                self.client.base_url, self.group_id
            ))
            .json(&serde_json::json!({
                "member_id": self.member_id,
            }))
            .send()
            .await?;

        // 处理响应
        let rebalance_response: RebalanceResponse = self.client.handle_response(response).await?;

        // 更新分配的分区
        let mut assigned_partitions = self.assigned_partitions.write().await;
        *assigned_partitions = rebalance_response.assigned_partitions;
        drop(assigned_partitions);

        info!(
            "Rebalance completed for consumer group: {}, new assigned partitions: {:?}",
            self.group_id,
            self.assigned_partitions.read().await
        );

        Ok(())
    }

    /// 启动心跳线程
    async fn start_heartbeat_thread(&self) {
        let client = self.client.clone();
        let group_id = self.group_id.clone();
        let member_id = self.member_id.clone();
        let heartbeat_interval_ms = self.heartbeat_interval_ms;
        let running = self.running.clone();
        let joined = self.joined.clone();
        let rebalance_lock = self.rebalance_lock.clone();
        let assigned_partitions = self.assigned_partitions.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(heartbeat_interval_ms));

            loop {
                interval.tick().await;

                // 检查是否正在运行
                if !*running.read().await {
                    break;
                }

                // 检查是否已加入组
                if !*joined.read().await {
                    continue;
                }

                // 发送心跳
                let result = Self::send_heartbeat_internal(
                    &client,
                    &group_id,
                    &member_id,
                    rebalance_lock.clone(),
                    assigned_partitions.clone(),
                )
                .await;

                if let Err(e) = result {
                    error!("Failed to send heartbeat: {}", e);
                    // 如果心跳失败，尝试重新加入组
                    if let Err(e) = Self::rejoin_group_internal(
                        &client,
                        &group_id,
                        &member_id,
                        rebalance_lock.clone(),
                        assigned_partitions.clone(),
                        joined.clone(),
                    )
                    .await
                    {
                        error!("Failed to rejoin group: {}", e);
                    }
                }
            }
        });
    }

    /// 内部方法：发送心跳
    async fn send_heartbeat_internal(
        client: &ArroyoClient,
        group_id: &str,
        member_id: &str,
        rebalance_lock: Arc<Mutex<()>>,
        assigned_partitions: Arc<RwLock<HashMap<String, Vec<u32>>>>,
    ) -> Result<()> {
        debug!("Sending heartbeat to consumer group: {}", group_id);

        // 发送心跳请求
        let response = client
            .client
            .post(&format!(
                "{}/api/consumer-groups/{}/members/{}/heartbeat",
                client.base_url, group_id, member_id
            ))
            .send()
            .await?;

        // 处理响应
        let heartbeat_response: HeartbeatResponse = client.handle_response(response).await?;

        // 如果需要再平衡，触发再平衡
        if heartbeat_response.rebalance_needed {
            info!("Rebalance needed, triggering rebalance");
            Self::trigger_rebalance_internal(
                client,
                group_id,
                member_id,
                rebalance_lock,
                assigned_partitions,
            )
            .await?;
        }

        Ok(())
    }

    /// 内部方法：触发再平衡
    async fn trigger_rebalance_internal(
        client: &ArroyoClient,
        group_id: &str,
        member_id: &str,
        rebalance_lock: Arc<Mutex<()>>,
        assigned_partitions: Arc<RwLock<HashMap<String, Vec<u32>>>>,
    ) -> Result<()> {
        // 获取再平衡锁
        let _lock = rebalance_lock.lock().await;

        info!("Starting rebalance for consumer group: {}", group_id);

        // 发送再平衡请求
        let response = client
            .client
            .post(&format!(
                "{}/api/consumer-groups/{}/rebalance",
                client.base_url, group_id
            ))
            .json(&serde_json::json!({
                "member_id": member_id,
            }))
            .send()
            .await?;

        // 处理响应
        let rebalance_response: RebalanceResponse = client.handle_response(response).await?;

        // 更新分配的分区
        let mut partitions = assigned_partitions.write().await;
        *partitions = rebalance_response.assigned_partitions;
        drop(partitions);

        info!(
            "Rebalance completed for consumer group: {}, new assigned partitions: {:?}",
            group_id,
            assigned_partitions.read().await
        );

        Ok(())
    }

    /// 内部方法：重新加入组
    async fn rejoin_group_internal(
        client: &ArroyoClient,
        group_id: &str,
        member_id: &str,
        rebalance_lock: Arc<Mutex<()>>,
        assigned_partitions: Arc<RwLock<HashMap<String, Vec<u32>>>>,
        joined: Arc<RwLock<bool>>,
    ) -> Result<()> {
        // 获取再平衡锁
        let _lock = rebalance_lock.lock().await;

        // 设置未加入组
        let mut joined_guard = joined.write().await;
        *joined_guard = false;
        drop(joined_guard);

        // 清空分配的分区
        let mut partitions = assigned_partitions.write().await;
        partitions.clear();
        drop(partitions);

        // TODO: 实现重新加入组的逻辑

        Ok(())
    }

    /// 获取分配的分区
    pub async fn get_assigned_partitions(&self) -> HashMap<String, Vec<u32>> {
        self.assigned_partitions.read().await.clone()
    }

    /// 检查是否已加入组
    pub async fn is_joined(&self) -> bool {
        *self.joined.read().await
    }

    /// 获取消费者组信息
    pub async fn get_group_info(&self) -> Result<ConsumerGroup> {
        let response = self
            .client
            .client
            .get(&format!(
                "{}/api/consumer-groups/{}",
                self.client.base_url, self.group_id
            ))
            .send()
            .await?;

        self.client.handle_response(response).await
    }
}
