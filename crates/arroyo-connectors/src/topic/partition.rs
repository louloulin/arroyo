use anyhow::{anyhow, bail, Result};
use arroyo_rpc::api_types::partitions::{
    ClusterBalanceStatus, MigrationDetails, MigrationStatus, NodeLoad, PartitionAssignment,
    PartitionAssignmentConfig, PartitionAssignmentStrategy, PartitionDetails, PartitionMigrationPlan,
    PartitionMetrics, PartitionRebalanceStrategy, PartitionRole, PartitionStatus, RebalanceTriggerConfig,
};
use arroyo_rpc::api_types::topics::TopicConfig;
// 注意：这里使用模拟的 Metadata 结构体，实际应该使用 rdkafka 的 Metadata
// 在实际实现中，应该使用 rdkafka 的 AdminClient 和 Metadata

/// 模拟的 Broker 结构体
#[derive(Clone, Debug)]
struct Broker {
    id: i32,
}

impl Broker {
    fn id(&self) -> i32 {
        self.id
    }
}

/// 模拟的 Partition 结构体
#[derive(Clone, Debug)]
struct Partition {
    id: i32,
    leader: Option<Broker>,
    replicas: Vec<Broker>,
}

impl Partition {
    fn id(&self) -> i32 {
        self.id
    }

    fn leader(&self) -> Option<&Broker> {
        self.leader.as_ref()
    }

    fn replicas(&self) -> &[Broker] {
        &self.replicas
    }
}

/// 模拟的 Topic 结构体
#[derive(Clone, Debug)]
struct Topic {
    name: String,
    partitions: Vec<Partition>,
}

impl Topic {
    fn name(&self) -> &str {
        &self.name
    }

    fn partitions(&self) -> &[Partition] {
        &self.partitions
    }
}

/// 模拟的 Metadata 结构体
#[derive(Clone, Debug)]
struct Metadata {
    brokers: Vec<Broker>,
    topics: Vec<Topic>,
}

impl Metadata {
    fn brokers(&self) -> &[Broker] {
        &self.brokers
    }

    fn topics(&self) -> &[Topic] {
        &self.topics
    }
}

/// 模拟的 AdminClient 结构体
struct AdminClient {
    server: String,
}

impl AdminClient {
    fn new(server: &str) -> Self {
        Self {
            server: server.to_string(),
        }
    }

    fn inner(&self) -> &Self {
        self
    }

    fn fetch_metadata(&self, topic: Option<&str>, _timeout: Duration) -> Result<Metadata> {
        // 模拟获取元数据
        // 在实际实现中，应该使用 rdkafka 的 AdminClient 获取真实的元数据
        let brokers = vec![
            Broker { id: 1 },
            Broker { id: 2 },
            Broker { id: 3 },
        ];

        let mut topics = Vec::new();

        // 如果指定了 Topic，只返回该 Topic 的元数据
        if let Some(topic_name) = topic {
            let partitions = vec![
                Partition {
                    id: 0,
                    leader: Some(Broker { id: 1 }),
                    replicas: vec![Broker { id: 1 }, Broker { id: 2 }],
                },
                Partition {
                    id: 1,
                    leader: Some(Broker { id: 2 }),
                    replicas: vec![Broker { id: 2 }, Broker { id: 3 }],
                },
                Partition {
                    id: 2,
                    leader: Some(Broker { id: 3 }),
                    replicas: vec![Broker { id: 3 }, Broker { id: 1 }],
                },
            ];

            topics.push(Topic {
                name: topic_name.to_string(),
                partitions,
            });
        } else {
            // 返回所有 Topic 的元数据
            topics.push(Topic {
                name: "topic1".to_string(),
                partitions: vec![
                    Partition {
                        id: 0,
                        leader: Some(Broker { id: 1 }),
                        replicas: vec![Broker { id: 1 }, Broker { id: 2 }],
                    },
                    Partition {
                        id: 1,
                        leader: Some(Broker { id: 2 }),
                        replicas: vec![Broker { id: 2 }, Broker { id: 3 }],
                    },
                ],
            });

            topics.push(Topic {
                name: "topic2".to_string(),
                partitions: vec![
                    Partition {
                        id: 0,
                        leader: Some(Broker { id: 3 }),
                        replicas: vec![Broker { id: 3 }, Broker { id: 1 }],
                    },
                    Partition {
                        id: 1,
                        leader: Some(Broker { id: 1 }),
                        replicas: vec![Broker { id: 1 }, Broker { id: 2 }],
                    },
                ],
            });
        }

        Ok(Metadata { brokers, topics })
    }
}
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

/// 分区管理器
pub struct PartitionManager {
    /// 服务器地址
    server: String,
    /// 管理客户端
    admin_client: AdminClient,
    /// 分区分配配置
    assignment_config: RwLock<PartitionAssignmentConfig>,
    /// 节点负载信息
    node_loads: RwLock<HashMap<String, NodeLoad>>,
    /// 分区详情缓存
    partition_details_cache: RwLock<HashMap<String, HashMap<i32, PartitionDetails>>>,
    /// 迁移详情
    migrations: RwLock<HashMap<String, MigrationDetails>>,
    /// 上次再平衡时间
    last_rebalance_time: RwLock<Option<u64>>,
    /// 缓存过期时间（秒）
    cache_ttl: u64,
    /// 上次缓存更新时间
    last_cache_update: RwLock<Instant>,
}

impl PartitionManager {
    /// 创建分区管理器
    pub fn new(server: &str, cache_ttl: Option<u64>) -> Result<Self> {
        let admin_client = AdminClient::new(server);

        // 默认分区分配配置
        let default_assignment_config = PartitionAssignmentConfig {
            assignment_strategy: PartitionAssignmentStrategy::RoundRobin,
            rebalance_strategy: PartitionRebalanceStrategy::LoadBased,
            rebalance_trigger: RebalanceTriggerConfig {
                enabled: true,
                load_imbalance_threshold: 10.0, // 10% 不平衡阈值
                min_rebalance_interval_sec: 300, // 5 分钟
                max_rebalance_interval_sec: 86400, // 1 天
                rebalance_on_node_join: true,
                rebalance_on_node_leave: true,
                max_partitions_to_move: 10,
            },
        };

        Ok(Self {
            server: server.to_string(),
            admin_client,
            assignment_config: RwLock::new(default_assignment_config),
            node_loads: RwLock::new(HashMap::new()),
            partition_details_cache: RwLock::new(HashMap::new()),
            migrations: RwLock::new(HashMap::new()),
            last_rebalance_time: RwLock::new(None),
            cache_ttl: cache_ttl.unwrap_or(60),
            last_cache_update: RwLock::new(Instant::now()),
        })
    }

    /// 获取分区分配配置
    pub async fn get_assignment_config(&self) -> PartitionAssignmentConfig {
        self.assignment_config.read().await.clone()
    }

    /// 更新分区分配配置
    pub async fn update_assignment_config(&self, config: PartitionAssignmentConfig) -> Result<()> {
        let mut assignment_config = self.assignment_config.write().await;
        *assignment_config = config;
        Ok(())
    }

    /// 获取集群元数据
    async fn get_metadata(&self, topic: Option<&str>) -> Result<Metadata> {
        let metadata = self
            .admin_client
            .inner()
            .fetch_metadata(topic, Duration::from_secs(30))
            .map_err(|e| anyhow!("Failed to fetch metadata: {}", e))?;
        Ok(metadata)
    }

    /// 获取可用节点列表
    async fn get_available_nodes(&self) -> Result<Vec<String>> {
        let metadata = self.get_metadata(None).await?;
        let nodes = metadata
            .brokers()
            .iter()
            .map(|broker| broker.id().to_string())
            .collect();
        Ok(nodes)
    }

    /// 初始分区分配
    pub async fn assign_partitions(
        &self,
        topic_config: &TopicConfig,
        nodes: Option<Vec<String>>,
    ) -> Result<Vec<PartitionAssignment>> {
        // 获取可用节点
        let available_nodes = match nodes {
            Some(n) => n,
            None => self.get_available_nodes().await?,
        };

        if available_nodes.is_empty() {
            bail!("No available nodes for partition assignment");
        }

        let assignment_config = self.assignment_config.read().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 根据分配策略分配分区
        let assignments = match assignment_config.assignment_strategy {
            PartitionAssignmentStrategy::RoundRobin => {
                self.assign_partitions_round_robin(topic_config, &available_nodes, now)
            }
            PartitionAssignmentStrategy::Random => {
                self.assign_partitions_random(topic_config, &available_nodes, now)
            }
            PartitionAssignmentStrategy::LoadBased => {
                self.assign_partitions_load_based(topic_config, &available_nodes, now)
                    .await?
            }
            PartitionAssignmentStrategy::RackAware => {
                self.assign_partitions_rack_aware(topic_config, &available_nodes, now)
                    .await?
            }
        };

        Ok(assignments)
    }

    /// 轮询分配分区
    fn assign_partitions_round_robin(
        &self,
        topic_config: &TopicConfig,
        nodes: &[String],
        now: u64,
    ) -> Vec<PartitionAssignment> {
        let mut assignments = Vec::new();
        let node_count = nodes.len();

        for partition_id in 0..topic_config.partitions {
            // 为每个分区分配一个领导者
            let leader_node_idx = (partition_id as usize) % node_count;
            let leader_node_id = nodes[leader_node_idx].clone();

            assignments.push(PartitionAssignment {
                partition_id,
                node_id: leader_node_id,
                role: PartitionRole::Leader,
                assigned_at: now,
            });

            // 为每个分区分配副本（如果有）
            if topic_config.replication_factor > 1 {
                for replica_idx in 1..topic_config.replication_factor {
                    let follower_node_idx = (leader_node_idx + replica_idx as usize) % node_count;
                    let follower_node_id = nodes[follower_node_idx].clone();

                    assignments.push(PartitionAssignment {
                        partition_id,
                        node_id: follower_node_id,
                        role: PartitionRole::Follower,
                        assigned_at: now,
                    });
                }
            }
        }

        assignments
    }

    /// 随机分配分区
    fn assign_partitions_random(
        &self,
        topic_config: &TopicConfig,
        nodes: &[String],
        now: u64,
    ) -> Vec<PartitionAssignment> {
        let mut assignments = Vec::new();
        let node_count = nodes.len();
        // 在实际实现中，应该使用 rand::thread_rng() 获取随机数生成器
        // 这里简化实现，不使用真正的随机数
        // 初始化

        for partition_id in 0..topic_config.partitions {
            // 在实际实现中，应该随机选择节点
            // 这里简化实现，使用轮询方式
            let selected_nodes = nodes.to_vec();

            // 为每个分区分配一个领导者
            let leader_node_id = selected_nodes[0].clone();
            assignments.push(PartitionAssignment {
                partition_id,
                node_id: leader_node_id,
                role: PartitionRole::Leader,
                assigned_at: now,
            });

            // 为每个分区分配副本（如果有）
            if topic_config.replication_factor > 1 {
                for replica_idx in 1..std::cmp::min(topic_config.replication_factor, node_count as i16) {
                    let follower_node_id = selected_nodes[replica_idx as usize].clone();
                    assignments.push(PartitionAssignment {
                        partition_id,
                        node_id: follower_node_id,
                        role: PartitionRole::Follower,
                        assigned_at: now,
                    });
                }
            }
        }

        assignments
    }

    /// 基于负载的分区分配
    async fn assign_partitions_load_based(
        &self,
        topic_config: &TopicConfig,
        nodes: &[String],
        now: u64,
    ) -> Result<Vec<PartitionAssignment>> {
        // 更新节点负载信息
        self.update_node_loads().await?;

        let mut assignments = Vec::new();
        let node_loads = self.node_loads.read().await;

        // 按照分区数量排序节点
        let mut sorted_nodes: Vec<_> = nodes
            .iter()
            .map(|node_id| {
                let load = node_loads.get(node_id).cloned().unwrap_or_else(|| NodeLoad {
                    node_id: node_id.clone(),
                    partition_count: 0,
                    leader_partition_count: 0,
                    total_size_bytes: 0,
                    total_message_count: 0,
                    total_bytes_in_per_sec: 0.0,
                    total_bytes_out_per_sec: 0.0,
                    cpu_usage_percent: 0.0,
                    memory_usage_percent: 0.0,
                    disk_usage_percent: 0.0,
                    network_usage_percent: 0.0,
                });
                (node_id, load)
            })
            .collect();

        // 按照领导者分区数量排序
        sorted_nodes.sort_by(|a, b| a.1.leader_partition_count.cmp(&b.1.leader_partition_count));

        for partition_id in 0..topic_config.partitions {
            // 为每个分区分配一个领导者（选择负载最低的节点）
            let leader_node_id = sorted_nodes[0].0.clone();
            assignments.push(PartitionAssignment {
                partition_id,
                node_id: leader_node_id.clone(),
                role: PartitionRole::Leader,
                assigned_at: now,
            });

            // 更新节点负载
            if let Some(node_load) = sorted_nodes.iter_mut().find(|n| n.0 == &leader_node_id) {
                node_load.1.leader_partition_count += 1;
                node_load.1.partition_count += 1;
            }

            // 重新排序节点
            sorted_nodes.sort_by(|a, b| a.1.leader_partition_count.cmp(&b.1.leader_partition_count));

            // 为每个分区分配副本（如果有）
            if topic_config.replication_factor > 1 {
                // 按照总分区数量排序
                sorted_nodes.sort_by(|a, b| a.1.partition_count.cmp(&b.1.partition_count));

                for _replica_idx in 1..std::cmp::min(topic_config.replication_factor, nodes.len() as i16) {
                    let follower_node_id = sorted_nodes[0].0.clone();

                    // 确保不会将副本分配给同一个节点
                    if follower_node_id != leader_node_id {
                        assignments.push(PartitionAssignment {
                            partition_id,
                            node_id: follower_node_id.clone(),
                            role: PartitionRole::Follower,
                            assigned_at: now,
                        });

                        // 更新节点负载
                        if let Some(node_load) = sorted_nodes.iter_mut().find(|n| n.0 == &follower_node_id) {
                            node_load.1.partition_count += 1;
                        }

                        // 重新排序节点
                        sorted_nodes.sort_by(|a, b| a.1.partition_count.cmp(&b.1.partition_count));
                    }
                }
            }
        }

        Ok(assignments)
    }

    /// 基于机架感知的分区分配
    async fn assign_partitions_rack_aware(
        &self,
        topic_config: &TopicConfig,
        nodes: &[String],
        now: u64,
    ) -> Result<Vec<PartitionAssignment>> {
        // 注意：这里简化实现，实际应该从配置或元数据中获取机架信息
        // 这里假设节点 ID 的第一个字符代表机架 ID
        let mut rack_nodes: HashMap<String, Vec<String>> = HashMap::new();

        for node_id in nodes {
            let rack_id = if node_id.is_empty() {
                "default".to_string()
            } else {
                node_id.chars().next().unwrap().to_string()
            };

            rack_nodes.entry(rack_id).or_default().push(node_id.clone());
        }

        let mut assignments = Vec::new();
        let racks: Vec<_> = rack_nodes.keys().cloned().collect();

        if racks.is_empty() {
            bail!("No racks available for partition assignment");
        }

        for partition_id in 0..topic_config.partitions {
            // 为每个分区选择不同机架上的节点
            let leader_rack_idx = (partition_id as usize) % racks.len();
            let leader_rack = &racks[leader_rack_idx];

            if let Some(rack_node_list) = rack_nodes.get(leader_rack) {
                if !rack_node_list.is_empty() {
                    // 在该机架上选择一个节点作为领导者
                    let leader_node_idx = (partition_id as usize) % rack_node_list.len();
                    let leader_node_id = rack_node_list[leader_node_idx].clone();

                    assignments.push(PartitionAssignment {
                        partition_id,
                        node_id: leader_node_id.clone(),
                        role: PartitionRole::Leader,
                        assigned_at: now,
                    });

                    // 为每个分区分配副本（如果有）
                    if topic_config.replication_factor > 1 {
                        let mut replica_count = 1;

                        // 尝试在不同机架上分配副本
                        for replica_idx in 1..topic_config.replication_factor {
                            let follower_rack_idx = (leader_rack_idx + replica_idx as usize) % racks.len();
                            let follower_rack = &racks[follower_rack_idx];

                            if let Some(follower_rack_nodes) = rack_nodes.get(follower_rack) {
                                if !follower_rack_nodes.is_empty() {
                                    let follower_node_idx = ((partition_id + replica_idx as i32) as usize) % follower_rack_nodes.len();
                                    let follower_node_id = follower_rack_nodes[follower_node_idx].clone();

                                    assignments.push(PartitionAssignment {
                                        partition_id,
                                        node_id: follower_node_id,
                                        role: PartitionRole::Follower,
                                        assigned_at: now,
                                    });

                                    replica_count += 1;
                                }
                            }

                            // 如果没有足够的机架，在同一机架上分配剩余副本
                            if replica_count < topic_config.replication_factor as u32 {
                                for same_rack_replica_idx in replica_count..topic_config.replication_factor as u32 {
                                    if let Some(leader_rack_nodes) = rack_nodes.get(leader_rack) {
                                        if leader_rack_nodes.len() > 1 {
                                            let same_rack_node_idx = (leader_node_idx + same_rack_replica_idx as usize) % leader_rack_nodes.len();
                                            let same_rack_node_id = leader_rack_nodes[same_rack_node_idx].clone();

                                            // 确保不会将副本分配给同一个节点
                                            if same_rack_node_id != leader_node_id {
                                                assignments.push(PartitionAssignment {
                                                    partition_id,
                                                    node_id: same_rack_node_id,
                                                    role: PartitionRole::Follower,
                                                    assigned_at: now,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(assignments)
    }

    /// 更新节点负载信息
    async fn update_node_loads(&self) -> Result<()> {
        // 检查缓存是否过期
        let mut last_cache_update = self.last_cache_update.write().await;
        if last_cache_update.elapsed().as_secs() < self.cache_ttl {
            return Ok(());
        }
        *last_cache_update = Instant::now();

        // 获取集群元数据
        let metadata = self.get_metadata(None).await?;
        let mut node_loads = HashMap::new();

        // 初始化节点负载信息
        for broker in metadata.brokers() {
            let node_id = broker.id().to_string();
            node_loads.insert(
                node_id.clone(),
                NodeLoad {
                    node_id,
                    partition_count: 0,
                    leader_partition_count: 0,
                    total_size_bytes: 0,
                    total_message_count: 0,
                    total_bytes_in_per_sec: 0.0,
                    total_bytes_out_per_sec: 0.0,
                    cpu_usage_percent: 0.0,
                    memory_usage_percent: 0.0,
                    disk_usage_percent: 0.0,
                    network_usage_percent: 0.0,
                },
            );
        }

        // 统计分区信息
        for topic in metadata.topics() {
            for partition in topic.partitions() {
                if let Some(leader) = partition.leader() {
                    let leader_id = leader.id().to_string();
                    if let Some(node_load) = node_loads.get_mut(&leader_id) {
                        node_load.partition_count += 1;
                        node_load.leader_partition_count += 1;
                    }
                }

                for replica in partition.replicas() {
                    let replica_id = replica.id().to_string();
                    if let Some(node_load) = node_loads.get_mut(&replica_id) {
                        node_load.partition_count += 1;
                    }
                }
            }
        }

        // 更新节点负载信息
        let mut node_loads_lock = self.node_loads.write().await;
        *node_loads_lock = node_loads;

        Ok(())
    }

    /// 获取分区详情
    pub async fn get_partition_details(&self, topic_name: &str, partition_id: i32) -> Result<PartitionDetails> {
        // 检查缓存
        let partition_details_cache = self.partition_details_cache.read().await;
        if let Some(topic_partitions) = partition_details_cache.get(topic_name) {
            if let Some(partition_details) = topic_partitions.get(&partition_id) {
                return Ok(partition_details.clone());
            }
        }
        drop(partition_details_cache);

        // 获取 Topic 元数据
        let metadata = self.get_metadata(Some(topic_name)).await?;
        let topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic_name)
            .ok_or_else(|| anyhow!("Topic not found: {}", topic_name))?;

        let partition = topic
            .partitions()
            .iter()
            .find(|p| p.id() == partition_id)
            .ok_or_else(|| anyhow!("Partition not found: {}", partition_id))?;

        // 构建分区详情
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut replicas = Vec::new();

        // 添加领导者
        if let Some(leader) = partition.leader() {
            replicas.push(PartitionAssignment {
                partition_id,
                node_id: leader.id().to_string(),
                role: PartitionRole::Leader,
                assigned_at: now, // 这里使用当前时间，实际应该从元数据中获取
            });
        }

        // 添加跟随者
        for replica in partition.replicas() {
            if partition.leader().map_or(true, |leader| leader.id() != replica.id()) {
                replicas.push(PartitionAssignment {
                    partition_id,
                    node_id: replica.id().to_string(),
                    role: PartitionRole::Follower,
                    assigned_at: now, // 这里使用当前时间，实际应该从元数据中获取
                });
            }
        }

        // 构建分区指标（这里使用模拟数据，实际应该从监控系统获取）
        let metrics = PartitionMetrics {
            bytes_in_per_sec: 0.0,
            bytes_out_per_sec: 0.0,
            messages_in_per_sec: 0.0,
            replica_lag: HashMap::new(),
        };

        let partition_details = PartitionDetails {
            id: partition_id,
            topic_name: topic_name.to_string(),
            status: PartitionStatus::Normal,
            size_bytes: 0, // 这里使用模拟数据，实际应该从存储系统获取
            message_count: 0, // 这里使用模拟数据，实际应该从存储系统获取
            earliest_offset: 0, // 这里使用模拟数据，实际应该从存储系统获取
            latest_offset: 0, // 这里使用模拟数据，实际应该从存储系统获取
            replicas,
            metrics,
        };

        // 更新缓存
        let mut partition_details_cache = self.partition_details_cache.write().await;
        let topic_partitions = partition_details_cache
            .entry(topic_name.to_string())
            .or_insert_with(HashMap::new);
        topic_partitions.insert(partition_id, partition_details.clone());

        Ok(partition_details)
    }

    /// 获取 Topic 的所有分区详情
    pub async fn get_topic_partition_details(&self, topic_name: &str) -> Result<Vec<PartitionDetails>> {
        // 获取 Topic 元数据
        let metadata = self.get_metadata(Some(topic_name)).await?;
        let topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic_name)
            .ok_or_else(|| anyhow!("Topic not found: {}", topic_name))?;

        let mut partition_details = Vec::new();
        for partition in topic.partitions() {
            partition_details.push(self.get_partition_details(topic_name, partition.id()).await?);
        }

        Ok(partition_details)
    }

    /// 获取集群平衡状态
    pub async fn get_cluster_balance_status(&self, include_migration_plan: bool) -> Result<ClusterBalanceStatus> {
        // 更新节点负载信息
        self.update_node_loads().await?;

        let node_loads = self.node_loads.read().await;
        let node_loads_vec: Vec<_> = node_loads.values().cloned().collect();

        // 计算不平衡程度
        let mut is_balanced = true;
        let mut imbalance_percent = 0.0;

        if !node_loads_vec.is_empty() {
            // 计算平均分区数
            let total_partitions: u32 = node_loads_vec.iter().map(|load| load.partition_count).sum();
            let avg_partitions = total_partitions as f64 / node_loads_vec.len() as f64;

            // 计算标准差
            let variance: f64 = node_loads_vec
                .iter()
                .map(|load| {
                    let diff = load.partition_count as f64 - avg_partitions;
                    diff * diff
                })
                .sum::<f64>()
                / node_loads_vec.len() as f64;
            let std_dev = variance.sqrt();

            // 计算不平衡程度（变异系数）
            imbalance_percent = if avg_partitions > 0.0 {
                (std_dev / avg_partitions) * 100.0
            } else {
                0.0
            };

            // 获取配置的不平衡阈值
            let assignment_config = self.assignment_config.read().await;
            is_balanced = imbalance_percent <= assignment_config.rebalance_trigger.load_imbalance_threshold;
        }

        // 生成迁移计划
        let suggested_migrations = if include_migration_plan && !is_balanced {
            self.generate_migration_plan().await?
        } else {
            Vec::new()
        };

        let last_rebalance_time = *self.last_rebalance_time.read().await;

        // 计算下次再平衡时间
        let next_rebalance_time = if !is_balanced {
            let assignment_config = self.assignment_config.read().await;
            if assignment_config.rebalance_trigger.enabled {
                last_rebalance_time.map(|last_time| {
                    last_time + assignment_config.rebalance_trigger.min_rebalance_interval_sec
                })
            } else {
                None
            }
        } else {
            None
        };

        Ok(ClusterBalanceStatus {
            is_balanced,
            imbalance_percent,
            node_loads: node_loads_vec,
            suggested_migrations,
            last_rebalance_time,
            next_rebalance_time,
        })
    }

    /// 生成迁移计划
    async fn generate_migration_plan(&self) -> Result<Vec<PartitionMigrationPlan>> {
        let node_loads = self.node_loads.read().await;
        let mut node_loads_vec: Vec<_> = node_loads.values().cloned().collect();

        // 如果节点数量不足，无法生成迁移计划
        if node_loads_vec.len() < 2 {
            return Ok(Vec::new());
        }

        // 按照分区数量排序节点
        node_loads_vec.sort_by(|a, b| b.partition_count.cmp(&a.partition_count));

        // 计算平均分区数
        let total_partitions: u32 = node_loads_vec.iter().map(|load| load.partition_count).sum();
        let avg_partitions = (total_partitions as f64 / node_loads_vec.len() as f64).ceil() as u32;

        let mut migration_plans = Vec::new();
        let assignment_config = self.assignment_config.read().await;
        let max_partitions_to_move = assignment_config.rebalance_trigger.max_partitions_to_move;

        // 获取所有 Topic 的元数据
        let metadata = self.get_metadata(None).await?;

        // 从负载最高的节点迁移分区到负载最低的节点
        let mut source_idx = 0;
        let mut target_idx = node_loads_vec.len() - 1;

        while source_idx < target_idx && migration_plans.len() < max_partitions_to_move as usize {
            let source_node = &node_loads_vec[source_idx];
            let target_node = &node_loads_vec[target_idx];

            // 如果源节点的分区数不超过平均值，或目标节点的分区数已达到平均值，停止迁移
            if source_node.partition_count <= avg_partitions || target_node.partition_count >= avg_partitions {
                break;
            }

            // 查找可以从源节点迁移到目标节点的分区
            for topic in metadata.topics() {
                for partition in topic.partitions() {
                    if let Some(leader) = partition.leader() {
                        if leader.id().to_string() == source_node.node_id {
                            // 检查目标节点是否已经是该分区的副本
                            let is_target_replica = partition
                                .replicas()
                                .iter()
                                .any(|r| r.id().to_string() == target_node.node_id);

                            if !is_target_replica {
                                // 添加迁移计划
                                migration_plans.push(PartitionMigrationPlan {
                                    partition_id: partition.id(),
                                    topic_name: topic.name().to_string(),
                                    source_node_id: source_node.node_id.clone(),
                                    target_node_id: target_node.node_id.clone(),
                                    priority: 1, // 优先级，可以根据负载差异设置
                                    estimated_size_bytes: 0, // 这里使用模拟数据，实际应该从存储系统获取
                                    estimated_duration_sec: 60, // 这里使用模拟数据，实际应该根据大小和网络带宽估算
                                });

                                // 如果已经达到最大迁移数量，停止添加
                                if migration_plans.len() >= max_partitions_to_move as usize {
                                    break;
                                }
                            }
                        }
                    }
                }

                // 如果已经达到最大迁移数量，停止添加
                if migration_plans.len() >= max_partitions_to_move as usize {
                    break;
                }
            }

            // 更新索引
            source_idx += 1;
            target_idx -= 1;
        }

        Ok(migration_plans)
    }

    /// 触发分区再平衡
    pub async fn trigger_rebalance(
        &self,
        execute: bool,
        max_partitions_to_move: Option<u32>,
        topics: Option<Vec<String>>,
    ) -> Result<Vec<PartitionMigrationPlan>> {
        // 检查是否可以触发再平衡
        let assignment_config = self.assignment_config.read().await;
        if !assignment_config.rebalance_trigger.enabled {
            bail!("Rebalance is disabled in the configuration");
        }

        // 检查最小再平衡间隔
        let last_rebalance_time = *self.last_rebalance_time.read().await;
        if let Some(last_time) = last_rebalance_time {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let elapsed = now - last_time;
            if elapsed < assignment_config.rebalance_trigger.min_rebalance_interval_sec {
                bail!(
                    "Minimum rebalance interval not reached. Please wait {} seconds.",
                    assignment_config.rebalance_trigger.min_rebalance_interval_sec - elapsed
                );
            }
        }

        // 获取集群平衡状态
        let balance_status = self.get_cluster_balance_status(true).await?;

        // 如果集群已经平衡，不需要再平衡
        if balance_status.is_balanced {
            info!("Cluster is already balanced. No rebalance needed.");
            return Ok(Vec::new());
        }

        // 获取迁移计划
        let mut migration_plans = balance_status.suggested_migrations;

        // 如果指定了特定的 Topic，过滤迁移计划
        if let Some(topic_list) = topics {
            migration_plans.retain(|plan| topic_list.contains(&plan.topic_name));
        }

        // 如果指定了最大迁移分区数量，限制迁移计划
        if let Some(max_partitions) = max_partitions_to_move {
            if migration_plans.len() > max_partitions as usize {
                migration_plans.truncate(max_partitions as usize);
            }
        }

        // 如果需要执行迁移
        if execute && !migration_plans.is_empty() {
            // 执行迁移计划
            self.execute_migration_plans(&migration_plans).await?;

            // 更新上次再平衡时间
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let mut last_rebalance_time = self.last_rebalance_time.write().await;
            *last_rebalance_time = Some(now);
        }

        Ok(migration_plans)
    }

    /// 执行迁移计划
    async fn execute_migration_plans(&self, plans: &[PartitionMigrationPlan]) -> Result<()> {
        for plan in plans {
            // 创建迁移详情
            let migration_id = Uuid::new_v4().to_string();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let migration_details = MigrationDetails {
                migration_id: migration_id.clone(),
                topic_name: plan.topic_name.clone(),
                partition_id: plan.partition_id,
                source_node_id: plan.source_node_id.clone(),
                target_node_id: plan.target_node_id.clone(),
                status: MigrationStatus::InProgress,
                progress_percent: 0.0,
                bytes_transferred: 0,
                total_bytes: plan.estimated_size_bytes,
                start_time: now,
                end_time: None,
                error_message: None,
            };

            // 保存迁移详情
            let mut migrations = self.migrations.write().await;
            migrations.insert(migration_id.clone(), migration_details);

            // 执行迁移（这里简化实现，实际应该异步执行）
            info!(
                "Starting migration of partition {} of topic {} from node {} to node {}",
                plan.partition_id, plan.topic_name, plan.source_node_id, plan.target_node_id
            );

            // 在实际实现中，这里应该启动一个异步任务来执行迁移
            // 为了简化，这里只是模拟迁移完成
            let migration = migrations.get_mut(&migration_id).unwrap();
            migration.status = MigrationStatus::Completed;
            migration.progress_percent = 100.0;
            migration.bytes_transferred = plan.estimated_size_bytes;
            migration.end_time = Some(now + plan.estimated_duration_sec);
        }

        Ok(())
    }

    /// 迁移分区
    pub async fn migrate_partition(
        &self,
        topic_name: &str,
        partition_id: i32,
        target_node_id: &str,
    ) -> Result<MigrationDetails> {
        // 获取分区详情
        let partition_details = self.get_partition_details(topic_name, partition_id).await?;

        // 检查目标节点是否存在
        let available_nodes = self.get_available_nodes().await?;
        if !available_nodes.contains(&target_node_id.to_string()) {
            bail!("Target node {} does not exist", target_node_id);
        }

        // 检查分区是否已经在目标节点上
        let leader_node_id = partition_details
            .replicas
            .iter()
            .find(|r| r.role == PartitionRole::Leader)
            .map(|r| r.node_id.clone())
            .ok_or_else(|| anyhow!("Partition has no leader"))?;

        if leader_node_id == target_node_id {
            bail!(
                "Partition {} of topic {} is already on node {}",
                partition_id,
                topic_name,
                target_node_id
            );
        }

        // 创建迁移计划
        let plan = PartitionMigrationPlan {
            partition_id,
            topic_name: topic_name.to_string(),
            source_node_id: leader_node_id,
            target_node_id: target_node_id.to_string(),
            priority: 1,
            estimated_size_bytes: partition_details.size_bytes,
            estimated_duration_sec: 60, // 这里使用模拟数据，实际应该根据大小和网络带宽估算
        };

        // 执行迁移计划
        self.execute_migration_plans(&[plan]).await?;

        // 获取迁移详情
        let migrations = self.migrations.read().await;
        let migration_details = migrations
            .values()
            .find(|m| {
                m.topic_name == topic_name
                    && m.partition_id == partition_id
                    && m.target_node_id == target_node_id
            })
            .cloned()
            .ok_or_else(|| anyhow!("Migration details not found"))?;

        Ok(migration_details)
    }

    /// 获取迁移详情
    pub async fn get_migration_details(&self, migration_id: &str) -> Result<MigrationDetails> {
        let migrations = self.migrations.read().await;
        let migration_details = migrations
            .get(migration_id)
            .cloned()
            .ok_or_else(|| anyhow!("Migration {} not found", migration_id))?;

        Ok(migration_details)
    }

    /// 获取所有迁移详情
    pub async fn get_all_migrations(&self) -> Result<Vec<MigrationDetails>> {
        let migrations = self.migrations.read().await;
        let migration_details = migrations.values().cloned().collect();

        Ok(migration_details)
    }

    /// 检查是否需要再平衡
    pub async fn check_rebalance_needed(&self) -> Result<bool> {
        // 获取配置
        let assignment_config = self.assignment_config.read().await;
        if !assignment_config.rebalance_trigger.enabled {
            return Ok(false);
        }

        // 检查最小再平衡间隔
        let last_rebalance_time = *self.last_rebalance_time.read().await;
        if let Some(last_time) = last_rebalance_time {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let elapsed = now - last_time;
            if elapsed < assignment_config.rebalance_trigger.min_rebalance_interval_sec {
                return Ok(false);
            }
        }

        // 获取集群平衡状态
        let balance_status = self.get_cluster_balance_status(false).await?;

        Ok(!balance_status.is_balanced)
    }

    /// 处理节点加入事件
    pub async fn handle_node_join(&self, node_id: &str) -> Result<bool> {
        // 获取配置
        let assignment_config = self.assignment_config.read().await;
        if !assignment_config.rebalance_trigger.enabled || !assignment_config.rebalance_trigger.rebalance_on_node_join {
            return Ok(false);
        }

        // 更新节点负载信息
        self.update_node_loads().await?;

        // 检查是否需要再平衡
        let rebalance_needed = self.check_rebalance_needed().await?;
        if rebalance_needed {
            info!("Node {} joined, triggering rebalance", node_id);
            // 这里可以触发再平衡，或者只返回需要再平衡的标志
            return Ok(true);
        }

        Ok(false)
    }

    /// 处理节点离开事件
    pub async fn handle_node_leave(&self, node_id: &str) -> Result<bool> {
        // 获取配置
        let assignment_config = self.assignment_config.read().await;
        if !assignment_config.rebalance_trigger.enabled || !assignment_config.rebalance_trigger.rebalance_on_node_leave {
            return Ok(false);
        }

        // 更新节点负载信息
        self.update_node_loads().await?;

        // 检查是否需要再平衡
        let rebalance_needed = self.check_rebalance_needed().await?;
        if rebalance_needed {
            info!("Node {} left, triggering rebalance", node_id);
            // 这里可以触发再平衡，或者只返回需要再平衡的标志
            return Ok(true);
        }

        Ok(false)
    }
}
