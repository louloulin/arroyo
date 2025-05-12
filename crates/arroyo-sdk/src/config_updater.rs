use crate::connection::{ConnectionConfig, PooledArroyoClient};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tracing::{debug, error, info};

/// 配置更新策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigUpdateStrategy {
    /// 立即应用配置更新
    Immediate,
    /// 延迟应用配置更新，等待当前操作完成
    Delayed,
    /// 仅通知配置更新，不自动应用
    NotifyOnly,
}

/// 配置更新事件类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigUpdateEventType {
    /// 连接池配置更新
    ConnectionPool,
    /// 重试配置更新
    Retry,
    /// 故障转移配置更新
    Failover,
    /// 缓存配置更新
    Cache,
    /// 预取配置更新
    Prefetch,
    /// 指标配置更新
    Metrics,
    /// 全局配置更新
    Global,
}

/// 配置更新事件
#[derive(Debug, Clone)]
pub struct ConfigUpdateEvent {
    /// 事件类型
    pub event_type: ConfigUpdateEventType,
    /// 事件时间
    pub timestamp: Instant,
    /// 旧配置
    pub old_config: Option<ConnectionConfig>,
    /// 新配置
    pub new_config: ConnectionConfig,
}

/// 配置更新监听器
pub type ConfigUpdateListener = Box<dyn Fn(ConfigUpdateEvent) + Send + Sync>;

/// 配置更新器配置
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigUpdaterConfig {
    /// 是否启用配置自动更新
    pub enabled: bool,
    /// 更新检查间隔（秒）
    pub check_interval_secs: u64,
    /// 更新策略
    pub update_strategy: ConfigUpdateStrategy,
    /// 配置端点
    pub config_endpoint: String,
    /// 配置更新重试次数
    pub retry_attempts: u32,
    /// 配置更新重试间隔（毫秒）
    pub retry_interval_ms: u64,
    /// 是否启用配置变更通知
    pub enable_notifications: bool,
    /// 自定义配置参数
    pub custom_params: HashMap<String, String>,
}

impl Default for ConfigUpdaterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_secs: 300, // 5分钟
            update_strategy: ConfigUpdateStrategy::Delayed,
            config_endpoint: "/api/client/config".to_string(),
            retry_attempts: 3,
            retry_interval_ms: 1000,
            enable_notifications: true,
            custom_params: HashMap::new(),
        }
    }
}

/// 服务器端配置响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfigResponse {
    /// 配置版本
    pub version: String,
    /// 连接池配置
    pub connection_pool: Option<ConnectionPoolConfig>,
    /// 重试配置
    pub retry: Option<RetryConfig>,
    /// 故障转移配置
    pub failover: Option<FailoverConfig>,
    /// 缓存配置
    pub cache: Option<CacheConfig>,
    /// 预取配置
    pub prefetch: Option<PrefetchConfig>,
    /// 指标配置
    pub metrics: Option<MetricsConfig>,
    /// 全局配置
    pub global: Option<GlobalConfig>,
}

/// 连接池配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolConfig {
    /// 最大连接数
    pub max_connections: Option<usize>,
    /// 最小连接数
    pub min_connections: Option<usize>,
    /// 连接超时时间（秒）
    pub connection_timeout_secs: Option<u64>,
    /// 空闲连接超时时间（秒）
    pub idle_timeout_secs: Option<u64>,
    /// 最大空闲连接数
    pub max_idle_connections: Option<usize>,
    /// 连接验证间隔（秒）
    pub validation_interval_secs: Option<u64>,
}

/// 重试配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// 重试次数
    pub retry_attempts: Option<u32>,
    /// 重试间隔（毫秒）
    pub retry_interval_ms: Option<u64>,
}

/// 故障转移配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    /// 是否启用故障转移
    pub enabled: Option<bool>,
    /// 故障转移目标
    pub failover_targets: Option<Vec<String>>,
}

/// 缓存配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// 是否启用缓存
    pub enabled: Option<bool>,
    /// 最大缓存条目数
    pub max_entries: Option<usize>,
    /// 默认 TTL（秒）
    pub default_ttl_secs: Option<u64>,
    /// 清理间隔（秒）
    pub cleanup_interval_secs: Option<u64>,
}

/// 预取配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefetchConfig {
    /// 是否启用预取
    pub enabled: Option<bool>,
    /// 预取阈值
    pub threshold: Option<f64>,
    /// 最大预取数量
    pub max_prefetch_count: Option<usize>,
}

/// 指标配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// 是否启用指标收集
    pub enabled: Option<bool>,
    /// 收集间隔（秒）
    pub collection_interval_secs: Option<u64>,
}

/// 全局配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    /// 客户端 ID
    pub client_id: Option<String>,
    /// 客户端版本
    pub client_version: Option<String>,
    /// 其他配置参数
    pub params: Option<HashMap<String, String>>,
}

/// 配置更新器
#[derive(Clone)]
pub struct ConfigUpdater {
    /// 配置
    config: ConfigUpdaterConfig,
    /// 客户端
    client: PooledArroyoClient,
    /// 当前配置
    current_config: Arc<RwLock<ConnectionConfig>>,
    /// 配置版本
    config_version: Arc<RwLock<String>>,
    /// 上次更新时间
    last_update: Arc<RwLock<Instant>>,
    /// 是否正在更新
    is_updating: Arc<Mutex<bool>>,
    /// 监听器
    listeners: Arc<RwLock<Vec<ConfigUpdateListener>>>,
    /// 是否正在运行
    is_running: Arc<RwLock<bool>>,
}

impl ConfigUpdater {
    /// 创建新的配置更新器
    pub fn new(client: PooledArroyoClient, config: ConfigUpdaterConfig) -> Self {
        let current_config = client.pool.get_config();

        let updater = Self {
            config,
            client,
            current_config: Arc::new(RwLock::new(current_config)),
            config_version: Arc::new(RwLock::new("0".to_string())),
            last_update: Arc::new(RwLock::new(Instant::now())),
            is_updating: Arc::new(Mutex::new(false)),
            listeners: Arc::new(RwLock::new(Vec::new())),
            is_running: Arc::new(RwLock::new(false)),
        };

        updater
    }

    /// 启动配置更新器
    pub fn start(&self) {
        if !self.config.enabled {
            return;
        }

        let updater = self.clone();
        tokio::spawn(async move {
            let mut is_running = updater.is_running.write().await;
            if *is_running {
                return;
            }
            *is_running = true;
            drop(is_running);

            updater.run_update_loop().await;
        });
    }

    /// 停止配置更新器
    pub async fn stop(&self) {
        let mut is_running = self.is_running.write().await;
        *is_running = false;
    }

    /// 运行更新循环
    async fn run_update_loop(&self) {
        let interval = Duration::from_secs(self.config.check_interval_secs);

        loop {
            // 检查是否应该停止
            let is_running = self.is_running.read().await;
            if !*is_running {
                break;
            }
            drop(is_running);

            // 检查配置更新
            if let Err(e) = self.check_for_updates().await {
                error!("检查配置更新失败: {}", e);
            }

            // 等待下一次检查
            sleep(interval).await;
        }
    }

    /// 检查配置更新
    pub async fn check_for_updates(&self) -> Result<bool> {
        // 如果正在更新，跳过
        let mut is_updating = self.is_updating.lock().await;
        if *is_updating {
            return Ok(false);
        }
        *is_updating = true;

        // 获取当前配置版本
        let current_version = self.config_version.read().await.clone();

        // 构建请求参数
        let mut params = vec![("version", current_version.as_str())];

        // 添加自定义参数
        for (key, value) in &self.config.custom_params {
            params.push((key, value));
        }

        // 发送请求
        let response = match self.client.get(&self.config.config_endpoint).await {
            Ok(response) => response,
            Err(e) => {
                *is_updating = false;
                return Err(e);
            }
        };

        // 解析响应
        let server_config: ServerConfigResponse = match self.client.handle_response(response).await {
            Ok(config) => config,
            Err(e) => {
                *is_updating = false;
                return Err(e);
            }
        };

        // 检查版本是否变更
        if server_config.version == current_version {
            debug!("配置未变更，当前版本: {}", current_version);
            *is_updating = false;
            return Ok(false);
        }

        // 应用配置更新
        let updated = self.apply_config_update(server_config.clone()).await?;

        // 更新版本和时间
        if updated {
            let mut version = self.config_version.write().await;
            *version = server_config.version;

            let mut last_update = self.last_update.write().await;
            *last_update = Instant::now();

            info!("配置已更新到版本: {}", version);
        }

        *is_updating = false;
        Ok(updated)
    }

    /// 应用配置更新
    async fn apply_config_update(&self, server_config: ServerConfigResponse) -> Result<bool> {
        // 获取当前配置
        let current_config = self.current_config.read().await.clone();

        // 创建新配置
        let mut new_config = current_config.clone();

        // 更新连接池配置
        if let Some(pool_config) = server_config.connection_pool {
            if let Some(max_connections) = pool_config.max_connections {
                new_config.max_connections = max_connections;
            }
            if let Some(min_connections) = pool_config.min_connections {
                new_config.min_connections = min_connections;
            }
            if let Some(timeout) = pool_config.connection_timeout_secs {
                new_config.connection_timeout_secs = timeout;
            }
            if let Some(idle_timeout) = pool_config.idle_timeout_secs {
                new_config.idle_timeout_secs = idle_timeout;
            }
            if let Some(max_idle) = pool_config.max_idle_connections {
                new_config.max_idle_connections = max_idle;
            }
            if let Some(validation_interval) = pool_config.validation_interval_secs {
                new_config.validation_interval_secs = validation_interval;
            }
        }

        // 更新重试配置
        if let Some(retry_config) = server_config.retry {
            if let Some(attempts) = retry_config.retry_attempts {
                new_config.retry_attempts = attempts;
            }
            if let Some(interval) = retry_config.retry_interval_ms {
                new_config.retry_interval_ms = interval;
            }
        }

        // 检查配置是否有变化
        let has_changes = new_config != current_config;

        if has_changes {
            // 根据更新策略应用配置
            match self.config.update_strategy {
                ConfigUpdateStrategy::Immediate => {
                    // 立即应用配置
                    self.update_client_config(new_config.clone()).await?;
                }
                ConfigUpdateStrategy::Delayed => {
                    // 保存新配置，等待下次操作时应用
                    let mut config = self.current_config.write().await;
                    *config = new_config.clone();
                }
                ConfigUpdateStrategy::NotifyOnly => {
                    // 不自动应用，仅通知
                }
            }

            // 触发配置更新事件
            if self.config.enable_notifications {
                self.notify_listeners(ConfigUpdateEvent {
                    event_type: ConfigUpdateEventType::Global,
                    timestamp: Instant::now(),
                    old_config: Some(current_config),
                    new_config,
                }).await;
            }
        }

        Ok(has_changes)
    }

    /// 更新客户端配置
    async fn update_client_config(&self, new_config: ConnectionConfig) -> Result<()> {
        // 这里需要实现实际的客户端配置更新逻辑
        // 由于 PooledArroyoClient 不直接支持更新配置，
        // 我们可能需要创建一个新的客户端或者扩展现有客户端

        // 更新当前配置
        let mut config = self.current_config.write().await;
        *config = new_config;

        Ok(())
    }

    /// 添加配置更新监听器
    pub async fn add_listener(&self, listener: ConfigUpdateListener) {
        let mut listeners = self.listeners.write().await;
        listeners.push(listener);
    }

    /// 通知所有监听器
    async fn notify_listeners(&self, event: ConfigUpdateEvent) {
        let listeners = self.listeners.read().await;
        for listener in listeners.iter() {
            listener(event.clone());
        }
    }

    /// 获取当前配置
    pub async fn get_current_config(&self) -> ConnectionConfig {
        self.current_config.read().await.clone()
    }

    /// 获取上次更新时间
    pub async fn get_last_update_time(&self) -> Instant {
        *self.last_update.read().await
    }

    /// 获取当前配置版本
    pub async fn get_config_version(&self) -> String {
        self.config_version.read().await.clone()
    }

    /// 手动触发配置更新检查
    pub async fn force_check_update(&self) -> Result<bool> {
        self.check_for_updates().await
    }
}

impl std::fmt::Debug for ConfigUpdater {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigUpdater")
            .field("config", &self.config)
            .field("config_version", &self.config_version)
            .field("last_update", &self.last_update)
            .field("is_updating", &self.is_updating)
            .field("is_running", &self.is_running)
            .finish_non_exhaustive()
    }
}
