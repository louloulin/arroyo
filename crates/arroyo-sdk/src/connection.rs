use crate::cache::{CacheConfig, CacheKey, CacheStrategy, ClientCache};
use crate::error::{Error, Result};
use crate::failover::{FailoverConfig, FailoverManager, FailoverState};
use crate::prefetch::{PrefetchConfig, PrefetchManager, PrefetchStrategy};
use crate::retry::{RetryConfig, retry_async};
use reqwest::{Client, ClientBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// 连接状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// 空闲状态
    Idle,
    /// 正在使用
    InUse,
    /// 正在验证
    Validating,
    /// 已关闭
    Closed,
}

/// 连接配置
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// 最大连接数
    pub max_connections: usize,
    /// 最小连接数
    pub min_connections: usize,
    /// 连接超时时间（秒）
    pub connection_timeout_secs: u64,
    /// 空闲连接超时时间（秒）
    pub idle_timeout_secs: u64,
    /// 最大空闲连接数
    pub max_idle_connections: usize,
    /// 连接验证间隔（秒）
    pub validation_interval_secs: u64,
    /// 重试次数
    pub retry_attempts: u32,
    /// 重试间隔（毫秒）
    pub retry_interval_ms: u64,
    /// 是否启用连接池
    pub enable_connection_pooling: bool,
    /// 是否启用会话管理
    pub enable_session_management: bool,
    /// 重试配置
    pub retry_config: Option<crate::retry::RetryConfig>,
    /// 故障转移配置
    pub failover_config: Option<crate::failover::FailoverConfig>,
    /// 缓存配置
    pub cache_config: Option<crate::cache::CacheConfig>,
    /// 预取配置
    pub prefetch_config: Option<crate::prefetch::PrefetchConfig>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_connections: 1,
            connection_timeout_secs: 30,
            idle_timeout_secs: 300,
            max_idle_connections: 5,
            validation_interval_secs: 60,
            retry_attempts: 3,
            retry_interval_ms: 500,
            enable_connection_pooling: true,
            enable_session_management: true,
            retry_config: None,
            failover_config: None,
            cache_config: None,
            prefetch_config: None,
        }
    }
}

/// 连接信息
#[derive(Debug)]
struct ConnectionInfo {
    /// HTTP 客户端
    client: Client,
    /// 连接状态
    state: ConnectionState,
    /// 最后使用时间
    last_used: Instant,
    /// 创建时间
    created_at: Instant,
    /// 使用次数
    usage_count: u64,
}

/// 会话信息
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// 会话 ID
    pub session_id: String,
    /// 创建时间
    pub created_at: Instant,
    /// 最后活动时间
    pub last_activity: Instant,
    /// 用户 ID
    pub user_id: Option<String>,
    /// 会话数据
    pub data: HashMap<String, String>,
}

/// 连接池管理器
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    /// 基础 URL
    base_url: String,
    /// 连接配置
    config: ConnectionConfig,
    /// 连接池
    pool: Arc<RwLock<Vec<ConnectionInfo>>>,
    /// 会话管理器
    session_manager: Arc<RwLock<HashMap<String, SessionInfo>>>,
    /// 是否正在清理
    is_cleaning: Arc<Mutex<bool>>,
}

impl ConnectionPool {
    /// 创建新的连接池
    pub fn new(base_url: impl Into<String>, config: ConnectionConfig) -> Self {
        let pool = Self {
            base_url: base_url.into(),
            config,
            pool: Arc::new(RwLock::new(Vec::new())),
            session_manager: Arc::new(RwLock::new(HashMap::new())),
            is_cleaning: Arc::new(Mutex::new(false)),
        };

        // 启动后台清理任务
        if pool.config.enable_connection_pooling {
            let pool_clone = pool.clone();
            tokio::spawn(async move {
                pool_clone.start_cleanup_task().await;
            });
        }

        pool
    }

    /// 获取连接
    pub async fn get_connection(&self) -> Result<Client> {
        if !self.config.enable_connection_pooling {
            // 如果未启用连接池，直接创建新连接
            return self.create_client();
        }

        // 尝试从池中获取空闲连接
        let mut pool = self.pool.write().await;

        // 查找空闲连接
        for conn_info in pool.iter_mut() {
            if conn_info.state == ConnectionState::Idle {
                conn_info.state = ConnectionState::InUse;
                conn_info.last_used = Instant::now();
                conn_info.usage_count += 1;
                return Ok(conn_info.client.clone());
            }
        }

        // 如果没有空闲连接且未达到最大连接数，创建新连接
        if pool.len() < self.config.max_connections {
            let client = self.create_client()?;

            pool.push(ConnectionInfo {
                client: client.clone(),
                state: ConnectionState::InUse,
                last_used: Instant::now(),
                created_at: Instant::now(),
                usage_count: 1,
            });

            return Ok(client);
        }

        // 如果达到最大连接数，等待连接释放或超时
        drop(pool);

        let start = Instant::now();
        let timeout = Duration::from_secs(self.config.connection_timeout_secs);

        while start.elapsed() < timeout {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut pool = self.pool.write().await;

            // 再次尝试获取空闲连接
            for conn_info in pool.iter_mut() {
                if conn_info.state == ConnectionState::Idle {
                    conn_info.state = ConnectionState::InUse;
                    conn_info.last_used = Instant::now();
                    conn_info.usage_count += 1;
                    return Ok(conn_info.client.clone());
                }
            }

            drop(pool);
        }

        Err(Error::ConnectionError("连接池已满，无法获取连接".to_string()))
    }

    /// 释放连接
    pub async fn release_connection(&self, client: &Client) {
        if !self.config.enable_connection_pooling {
            return;
        }

        let mut pool = self.pool.write().await;

        // 查找对应的连接并标记为空闲
        for conn_info in pool.iter_mut() {
            // 由于 Client 不能直接比较，我们使用客户端的地址作为标识
            let client_ptr = client as *const Client;
            let conn_client_ptr = &conn_info.client as *const Client;

            if std::ptr::eq(client_ptr, conn_client_ptr) {
                conn_info.state = ConnectionState::Idle;
                conn_info.last_used = Instant::now();
                break;
            }
        }
    }

    /// 创建新的 HTTP 客户端
    fn create_client(&self) -> Result<Client> {
        let client = ClientBuilder::new()
            .timeout(Duration::from_secs(self.config.connection_timeout_secs))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_max_idle_per_host(self.config.max_idle_connections)
            .pool_idle_timeout(Duration::from_secs(self.config.idle_timeout_secs))
            .build()
            .map_err(|e| Error::ClientError(e.to_string()))?;

        Ok(client)
    }

    /// 验证连接
    async fn validate_connection(&self, client: &Client) -> bool {
        // 简单的连接验证：发送 HEAD 请求到基础 URL
        match client
            .head(&self.base_url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// 清理过期连接
    pub async fn cleanup_connections(&self) {
        // 获取清理锁
        let mut is_cleaning = self.is_cleaning.lock().await;
        if *is_cleaning {
            return;
        }
        *is_cleaning = true;

        let mut pool = self.pool.write().await;
        let now = Instant::now();

        // 标记要移除的连接索引
        let mut to_remove = Vec::new();

        for (i, conn_info) in pool.iter_mut().enumerate() {
            // 跳过正在使用的连接
            if conn_info.state == ConnectionState::InUse {
                continue;
            }

            // 检查空闲超时
            if conn_info.state == ConnectionState::Idle
                && now.duration_since(conn_info.last_used).as_secs() > self.config.idle_timeout_secs
            {
                to_remove.push(i);
                continue;
            }

            // 定期验证空闲连接
            if conn_info.state == ConnectionState::Idle
                && now.duration_since(conn_info.last_used).as_secs() > self.config.validation_interval_secs
            {
                conn_info.state = ConnectionState::Validating;

                // 克隆连接信息以便在外部验证
                let client = conn_info.client.clone();
                let pool_clone = self.clone();

                // 在单独的任务中验证连接
                tokio::spawn(async move {
                    let is_valid = pool_clone.validate_connection(&client).await;

                    let mut pool = pool_clone.pool.write().await;

                    // 查找对应的连接
                    for conn in pool.iter_mut() {
                        // 由于 Client 不能直接比较，我们使用客户端的地址作为标识
                        let client_ptr = &client as *const Client;
                        let conn_client_ptr = &conn.client as *const Client;

                        if std::ptr::eq(client_ptr, conn_client_ptr) {
                            if is_valid {
                                conn.state = ConnectionState::Idle;
                            } else {
                                conn.state = ConnectionState::Closed;
                            }
                            break;
                        }
                    }
                });
            }

            // 移除已关闭的连接
            if conn_info.state == ConnectionState::Closed {
                to_remove.push(i);
            }
        }

        // 从后向前移除连接，避免索引失效
        for i in to_remove.into_iter().rev() {
            pool.remove(i);
        }

        // 确保至少有最小连接数
        while pool.len() < self.config.min_connections {
            match self.create_client() {
                Ok(client) => {
                    pool.push(ConnectionInfo {
                        client,
                        state: ConnectionState::Idle,
                        last_used: Instant::now(),
                        created_at: Instant::now(),
                        usage_count: 0,
                    });
                }
                Err(e) => {
                    error!("创建连接失败: {}", e);
                    break;
                }
            }
        }

        *is_cleaning = false;
    }

    /// 启动清理任务
    async fn start_cleanup_task(&self) {
        let interval = Duration::from_secs(self.config.validation_interval_secs);

        loop {
            tokio::time::sleep(interval).await;
            self.cleanup_connections().await;
        }
    }

    /// 创建会话
    pub async fn create_session(&self, user_id: Option<String>) -> String {
        if !self.config.enable_session_management {
            return "".to_string();
        }

        let session_id = uuid::Uuid::new_v4().to_string();
        let now = Instant::now();

        let session = SessionInfo {
            session_id: session_id.clone(),
            created_at: now,
            last_activity: now,
            user_id,
            data: HashMap::new(),
        };

        let mut sessions = self.session_manager.write().await;
        sessions.insert(session_id.clone(), session);

        session_id
    }

    /// 获取会话
    pub async fn get_session(&self, session_id: &str) -> Option<SessionInfo> {
        if !self.config.enable_session_management {
            return None;
        }

        let mut sessions = self.session_manager.write().await;

        if let Some(session) = sessions.get_mut(session_id) {
            session.last_activity = Instant::now();
            Some(session.clone())
        } else {
            None
        }
    }

    /// 更新会话数据
    pub async fn update_session_data(
        &self,
        session_id: &str,
        key: &str,
        value: &str,
    ) -> Result<()> {
        if !self.config.enable_session_management {
            return Ok(());
        }

        let mut sessions = self.session_manager.write().await;

        if let Some(session) = sessions.get_mut(session_id) {
            session.last_activity = Instant::now();
            session.data.insert(key.to_string(), value.to_string());
            Ok(())
        } else {
            Err(Error::SessionError("会话不存在".to_string()))
        }
    }

    /// 删除会话
    pub async fn delete_session(&self, session_id: &str) -> Result<()> {
        if !self.config.enable_session_management {
            return Ok(());
        }

        let mut sessions = self.session_manager.write().await;

        if sessions.remove(session_id).is_some() {
            Ok(())
        } else {
            Err(Error::SessionError("会话不存在".to_string()))
        }
    }

    /// 清理过期会话
    pub async fn cleanup_sessions(&self, max_age_secs: u64) {
        if !self.config.enable_session_management {
            return;
        }

        let mut sessions = self.session_manager.write().await;
        let now = Instant::now();

        // 收集过期会话的 ID
        let expired_sessions: Vec<String> = sessions
            .iter()
            .filter(|(_, session)| {
                now.duration_since(session.last_activity).as_secs() > max_age_secs
            })
            .map(|(id, _)| id.clone())
            .collect();

        // 移除过期会话
        for id in expired_sessions {
            sessions.remove(&id);
        }
    }

    /// 获取连接池配置
    pub fn get_config(&self) -> ConnectionConfig {
        self.config.clone()
    }

    /// 获取连接池统计信息
    pub async fn get_pool_stats(&self) -> ConnectionPoolStats {
        if !self.config.enable_connection_pooling {
            return ConnectionPoolStats::default();
        }

        let pool = self.pool.read().await;
        let sessions = self.session_manager.read().await;

        let mut idle_count = 0;
        let mut in_use_count = 0;
        let mut validating_count = 0;
        let mut closed_count = 0;

        for conn in pool.iter() {
            match conn.state {
                ConnectionState::Idle => idle_count += 1,
                ConnectionState::InUse => in_use_count += 1,
                ConnectionState::Validating => validating_count += 1,
                ConnectionState::Closed => closed_count += 1,
            }
        }

        ConnectionPoolStats {
            total_connections: pool.len(),
            idle_connections: idle_count,
            in_use_connections: in_use_count,
            validating_connections: validating_count,
            closed_connections: closed_count,
            total_sessions: sessions.len(),
            max_connections: self.config.max_connections,
            min_connections: self.config.min_connections,
        }
    }
}

/// 连接池统计信息
#[derive(Debug, Clone, Default)]
pub struct ConnectionPoolStats {
    /// 总连接数
    pub total_connections: usize,
    /// 空闲连接数
    pub idle_connections: usize,
    /// 使用中的连接数
    pub in_use_connections: usize,
    /// 验证中的连接数
    pub validating_connections: usize,
    /// 已关闭的连接数
    pub closed_connections: usize,
    /// 总会话数
    pub total_sessions: usize,
    /// 最大连接数
    pub max_connections: usize,
    /// 最小连接数
    pub min_connections: usize,
}

/// 增强的 Arroyo 客户端，支持连接池、会话管理、重试、故障转移、缓存和预取
#[derive(Debug, Clone)]
pub struct PooledArroyoClient {
    /// 基础 URL
    pub(crate) base_url: String,
    /// 连接池
    pub(crate) pool: ConnectionPool,
    /// 当前会话 ID
    pub(crate) session_id: Option<String>,
    /// 重试管理器
    pub(crate) retry_config: Option<crate::retry::RetryConfig>,
    /// 故障转移管理器
    pub(crate) failover_manager: Option<crate::failover::FailoverManager>,
    /// 缓存管理器
    pub(crate) cache: Option<crate::cache::ClientCache>,
    /// 预取管理器
    pub(crate) prefetch_manager: Option<crate::prefetch::PrefetchManager>,
}

impl PooledArroyoClient {
    /// 创建新的 Arroyo 客户端
    pub fn new(base_url: impl Into<String>, config: Option<ConnectionConfig>) -> Result<Self> {
        let base_url = base_url.into();
        let config = config.unwrap_or_default();

        let pool = ConnectionPool::new(&base_url, config.clone());

        // 创建故障转移管理器（如果配置了）
        let failover_manager = if let Some(failover_config) = config.failover_config {
            if failover_config.enabled && !failover_config.failover_targets.is_empty() {
                Some(crate::failover::FailoverManager::new(&base_url, failover_config))
            } else {
                None
            }
        } else {
            None
        };

        // 创建缓存管理器（如果配置了）
        let cache = if let Some(cache_config) = config.cache_config {
            if cache_config.enabled {
                Some(crate::cache::ClientCache::new(cache_config))
            } else {
                None
            }
        } else {
            None
        };

        // 创建预取管理器（如果配置了）
        let prefetch_manager = if let Some(prefetch_config) = config.prefetch_config {
            if prefetch_config.enabled {
                Some(crate::prefetch::PrefetchManager::new(prefetch_config))
            } else {
                None
            }
        } else {
            None
        };

        let client = Self {
            base_url,
            pool,
            session_id: None,
            retry_config: config.retry_config,
            failover_manager,
            cache,
            prefetch_manager,
        };

        // 如果启用了预取，启动预取处理循环
        if client.prefetch_manager.is_some() && client.cache.is_some() {
            client.start_prefetch_processing();
        }

        Ok(client)
    }

    /// 创建会话
    pub async fn create_session(&mut self, user_id: Option<String>) -> String {
        let session_id = self.pool.create_session(user_id).await;
        self.session_id = Some(session_id.clone());
        session_id
    }

    /// 设置会话 ID
    pub fn set_session_id(&mut self, session_id: impl Into<String>) {
        self.session_id = Some(session_id.into());
    }

    /// 获取会话信息
    pub async fn get_session_info(&self) -> Option<SessionInfo> {
        match &self.session_id {
            Some(id) => self.pool.get_session(id).await,
            None => None,
        }
    }

    /// 更新会话数据
    pub async fn update_session_data(&self, key: &str, value: &str) -> Result<()> {
        match &self.session_id {
            Some(id) => self.pool.update_session_data(id, key, value).await,
            None => Err(Error::SessionError("未设置会话 ID".to_string())),
        }
    }

    /// 关闭会话
    pub async fn close_session(&mut self) -> Result<()> {
        match &self.session_id {
            Some(id) => {
                let result = self.pool.delete_session(id).await;
                self.session_id = None;
                result
            }
            None => Ok(()),
        }
    }

    /// 获取连接池统计信息
    pub async fn get_pool_stats(&self) -> ConnectionPoolStats {
        self.pool.get_pool_stats().await
    }

    /// 发送 GET 请求
    pub async fn get(&self, path: &str) -> Result<Response> {
        // 检查缓存
        if let Some(cache) = &self.cache {
            // 创建缓存键
            let cache_key = crate::cache::CacheKey {
                method: "GET".to_string(),
                path: path.to_string(),
                query_params: None,
                body: None,
            };

            // 尝试从缓存获取
            if let Some(response) = cache.get_response(&cache_key).await {
                // 如果启用了预取，记录访问
                if let Some(prefetch) = &self.prefetch_manager {
                    prefetch.record_access(path).await;

                    // 预测并预取下一个可能的访问
                    let predicted_paths = prefetch.predict_next_accesses(path).await;
                    for predicted_path in predicted_paths {
                        let prefetch_key = crate::cache::CacheKey {
                            method: "GET".to_string(),
                            path: predicted_path.clone(),
                            query_params: None,
                            body: None,
                        };

                        prefetch.add_to_queue(prefetch_key, 5).await;
                    }
                }

                return Ok(response);
            }
        }

        // 如果启用了预取，记录访问
        if let Some(prefetch) = &self.prefetch_manager {
            prefetch.record_access(path).await;
        }

        // 如果启用了故障转移，获取当前端点
        let base_url = if let Some(failover) = &self.failover_manager {
            failover.get_current_endpoint().await
        } else {
            self.base_url.clone()
        };

        // 获取连接
        let client = self.pool.get_connection().await?;

        // 构建 URL
        let url = format!("{}{}", base_url, path);

        // 如果启用了重试，使用重试机制
        if let Some(retry_config) = &self.retry_config {
            let failover_manager = self.failover_manager.clone();
            let session_id = self.session_id.clone();
            let client_ref = &client;
            let pool_ref = &self.pool;
            let cache_ref = self.cache.clone();

            let result = crate::retry::retry_async(
                || async {
                    let mut builder = client_ref.get(&url);

                    // 如果有会话 ID，添加到请求头
                    if let Some(session_id) = &session_id {
                        builder = builder.header("X-Session-ID", session_id);
                    }

                    let response = builder
                        .send()
                        .await
                        .map_err(|e| Error::RequestError(e.to_string()))?;

                    // 检查响应状态码，如果是服务器错误，转换为 ApiError
                    if response.status().is_server_error() {
                        return Err(Error::ApiError(
                            response.status().as_u16(),
                            response.text().await.unwrap_or_default(),
                        ));
                    }

                    // 如果启用了故障转移，报告成功
                    if let Some(failover) = &failover_manager {
                        failover.report_success().await?;
                    }

                    // 如果启用了缓存，将响应添加到缓存
                    if let Some(cache) = &cache_ref {
                        // 创建缓存键
                        let cache_key = crate::cache::CacheKey {
                            method: "GET".to_string(),
                            path: path.to_string(),
                            query_params: None,
                            body: None,
                        };

                        // 缓存响应
                        let _ = cache.cache_response(cache_key, &response, None).await;
                    }

                    Ok(response)
                },
                retry_config,
            ).await;

            // 释放连接
            pool_ref.release_connection(client_ref).await;

            // 处理结果
            match result {
                Ok(response) => Ok(response),
                Err(error) => {
                    // 如果启用了故障转移，尝试故障转移
                    if let Some(failover) = &self.failover_manager {
                        let should_failover = failover.report_failure(&error).await?;
                        if should_failover {
                            failover.perform_failover().await?;
                            // 使用新的端点重试
                            let client = self.pool.get_connection().await?;
                            let new_url = format!("{}{}", failover.get_current_endpoint().await, path);

                            let mut builder = client.get(&new_url);

                            // 如果有会话 ID，添加到请求头
                            if let Some(session_id) = &self.session_id {
                                builder = builder.header("X-Session-ID", session_id);
                            }

                            let response = builder
                                .send()
                                .await
                                .map_err(|e| Error::RequestError(e.to_string()))?;

                            // 释放连接
                            self.pool.release_connection(&client).await;

                            // 如果启用了缓存，将响应添加到缓存
                            if let Some(cache) = &self.cache {
                                // 创建缓存键
                                let cache_key = crate::cache::CacheKey {
                                    method: "GET".to_string(),
                                    path: path.to_string(),
                                    query_params: None,
                                    body: None,
                                };

                                // 缓存响应
                                let _ = cache.cache_response(cache_key, &response, None).await;
                            }

                            return Ok(response);
                        }
                    }
                    Err(error)
                }
            }
        } else {
            // 不使用重试机制
            let mut builder = client.get(&url);

            // 如果有会话 ID，添加到请求头
            if let Some(session_id) = &self.session_id {
                builder = builder.header("X-Session-ID", session_id);
            }

            let response = builder
                .send()
                .await
                .map_err(|e| {
                    // 如果启用了故障转移，报告失败
                    if let Some(failover) = &self.failover_manager {
                        let error = Error::RequestError(e.to_string());
                        let _ = futures::executor::block_on(async {
                            let should_failover = failover.report_failure(&error).await?;
                            if should_failover {
                                failover.perform_failover().await?;
                            }
                            Ok::<_, Error>(())
                        });
                    }
                    Error::RequestError(e.to_string())
                })?;

            // 如果启用了故障转移，报告成功
            if let Some(failover) = &self.failover_manager {
                let _ = failover.report_success().await;
            }

            // 如果启用了缓存，将响应添加到缓存
            if let Some(cache) = &self.cache {
                // 创建缓存键
                let cache_key = crate::cache::CacheKey {
                    method: "GET".to_string(),
                    path: path.to_string(),
                    query_params: None,
                    body: None,
                };

                // 缓存响应
                let _ = cache.cache_response(cache_key, &response, None).await;
            }

            // 释放连接
            self.pool.release_connection(&client).await;

            Ok(response)
        }
    }

    /// 发送 POST 请求
    pub async fn post<T: serde::Serialize + ?Sized>(
        &self,
        path: &str,
        json: &T,
    ) -> Result<Response> {
        // 如果启用了故障转移，获取当前端点
        let base_url = if let Some(failover) = &self.failover_manager {
            failover.get_current_endpoint().await
        } else {
            self.base_url.clone()
        };

        // 获取连接
        let client = self.pool.get_connection().await?;

        // 构建 URL
        let url = format!("{}{}", base_url, path);

        // 如果启用了重试，使用重试机制
        if let Some(retry_config) = &self.retry_config {
            let failover_manager = self.failover_manager.clone();
            let session_id = self.session_id.clone();
            let client_ref = &client;
            let pool_ref = &self.pool;
            let json_ref = json;

            let result = crate::retry::retry_async(
                || async {
                    let mut builder = client_ref.post(&url);

                    // 如果有会话 ID，添加到请求头
                    if let Some(session_id) = &session_id {
                        builder = builder.header("X-Session-ID", session_id);
                    }

                    let response = builder
                        .json(json_ref)
                        .send()
                        .await
                        .map_err(|e| Error::RequestError(e.to_string()))?;

                    // 检查响应状态码，如果是服务器错误，转换为 ApiError
                    if response.status().is_server_error() {
                        return Err(Error::ApiError(
                            response.status().as_u16(),
                            response.text().await.unwrap_or_default(),
                        ));
                    }

                    // 如果启用了故障转移，报告成功
                    if let Some(failover) = &failover_manager {
                        failover.report_success().await?;
                    }

                    Ok(response)
                },
                retry_config,
            ).await;

            // 释放连接
            pool_ref.release_connection(client_ref).await;

            // 处理结果
            match result {
                Ok(response) => Ok(response),
                Err(error) => {
                    // 如果启用了故障转移，尝试故障转移
                    if let Some(failover) = &self.failover_manager {
                        let should_failover = failover.report_failure(&error).await?;
                        if should_failover {
                            failover.perform_failover().await?;
                            // 使用新的端点重试
                            let client = self.pool.get_connection().await?;
                            let new_url = format!("{}{}", failover.get_current_endpoint().await, path);

                            let mut builder = client.post(&new_url);

                            // 如果有会话 ID，添加到请求头
                            if let Some(session_id) = &self.session_id {
                                builder = builder.header("X-Session-ID", session_id);
                            }

                            let response = builder
                                .json(json)
                                .send()
                                .await
                                .map_err(|e| Error::RequestError(e.to_string()))?;

                            // 释放连接
                            self.pool.release_connection(&client).await;

                            return Ok(response);
                        }
                    }
                    Err(error)
                }
            }
        } else {
            // 不使用重试机制
            let mut builder = client.post(&url);

            // 如果有会话 ID，添加到请求头
            if let Some(session_id) = &self.session_id {
                builder = builder.header("X-Session-ID", session_id);
            }

            let response = builder
                .json(json)
                .send()
                .await
                .map_err(|e| {
                    // 如果启用了故障转移，报告失败
                    if let Some(failover) = &self.failover_manager {
                        let error = Error::RequestError(e.to_string());
                        let _ = futures::executor::block_on(async {
                            let should_failover = failover.report_failure(&error).await?;
                            if should_failover {
                                failover.perform_failover().await?;
                            }
                            Ok::<_, Error>(())
                        });
                    }
                    Error::RequestError(e.to_string())
                })?;

            // 如果启用了故障转移，报告成功
            if let Some(failover) = &self.failover_manager {
                let _ = failover.report_success().await;
            }

            // 释放连接
            self.pool.release_connection(&client).await;

            Ok(response)
        }
    }

    /// 发送 PUT 请求
    pub async fn put<T: serde::Serialize + ?Sized>(
        &self,
        path: &str,
        json: &T,
    ) -> Result<Response> {
        // 如果启用了故障转移，获取当前端点
        let base_url = if let Some(failover) = &self.failover_manager {
            failover.get_current_endpoint().await
        } else {
            self.base_url.clone()
        };

        // 获取连接
        let client = self.pool.get_connection().await?;

        // 构建 URL
        let url = format!("{}{}", base_url, path);

        // 如果启用了重试，使用重试机制
        if let Some(retry_config) = &self.retry_config {
            let failover_manager = self.failover_manager.clone();
            let session_id = self.session_id.clone();
            let client_ref = &client;
            let pool_ref = &self.pool;
            let json_ref = json;

            let result = crate::retry::retry_async(
                || async {
                    let mut builder = client_ref.put(&url);

                    // 如果有会话 ID，添加到请求头
                    if let Some(session_id) = &session_id {
                        builder = builder.header("X-Session-ID", session_id);
                    }

                    let response = builder
                        .json(json_ref)
                        .send()
                        .await
                        .map_err(|e| Error::RequestError(e.to_string()))?;

                    // 检查响应状态码，如果是服务器错误，转换为 ApiError
                    if response.status().is_server_error() {
                        return Err(Error::ApiError(
                            response.status().as_u16(),
                            response.text().await.unwrap_or_default(),
                        ));
                    }

                    // 如果启用了故障转移，报告成功
                    if let Some(failover) = &failover_manager {
                        failover.report_success().await?;
                    }

                    Ok(response)
                },
                retry_config,
            ).await;

            // 释放连接
            pool_ref.release_connection(client_ref).await;

            // 处理结果
            match result {
                Ok(response) => Ok(response),
                Err(error) => {
                    // 如果启用了故障转移，尝试故障转移
                    if let Some(failover) = &self.failover_manager {
                        let should_failover = failover.report_failure(&error).await?;
                        if should_failover {
                            failover.perform_failover().await?;
                            // 使用新的端点重试
                            let client = self.pool.get_connection().await?;
                            let new_url = format!("{}{}", failover.get_current_endpoint().await, path);

                            let mut builder = client.put(&new_url);

                            // 如果有会话 ID，添加到请求头
                            if let Some(session_id) = &self.session_id {
                                builder = builder.header("X-Session-ID", session_id);
                            }

                            let response = builder
                                .json(json)
                                .send()
                                .await
                                .map_err(|e| Error::RequestError(e.to_string()))?;

                            // 释放连接
                            self.pool.release_connection(&client).await;

                            return Ok(response);
                        }
                    }
                    Err(error)
                }
            }
        } else {
            // 不使用重试机制
            let mut builder = client.put(&url);

            // 如果有会话 ID，添加到请求头
            if let Some(session_id) = &self.session_id {
                builder = builder.header("X-Session-ID", session_id);
            }

            let response = builder
                .json(json)
                .send()
                .await
                .map_err(|e| {
                    // 如果启用了故障转移，报告失败
                    if let Some(failover) = &self.failover_manager {
                        let error = Error::RequestError(e.to_string());
                        let _ = futures::executor::block_on(async {
                            let should_failover = failover.report_failure(&error).await?;
                            if should_failover {
                                failover.perform_failover().await?;
                            }
                            Ok::<_, Error>(())
                        });
                    }
                    Error::RequestError(e.to_string())
                })?;

            // 如果启用了故障转移，报告成功
            if let Some(failover) = &self.failover_manager {
                let _ = failover.report_success().await;
            }

            // 释放连接
            self.pool.release_connection(&client).await;

            Ok(response)
        }
    }

    /// 发送 DELETE 请求
    pub async fn delete(&self, path: &str) -> Result<Response> {
        // 如果启用了故障转移，获取当前端点
        let base_url = if let Some(failover) = &self.failover_manager {
            failover.get_current_endpoint().await
        } else {
            self.base_url.clone()
        };

        // 获取连接
        let client = self.pool.get_connection().await?;

        // 构建 URL
        let url = format!("{}{}", base_url, path);

        // 如果启用了重试，使用重试机制
        if let Some(retry_config) = &self.retry_config {
            let failover_manager = self.failover_manager.clone();
            let session_id = self.session_id.clone();
            let client_ref = &client;
            let pool_ref = &self.pool;

            let result = crate::retry::retry_async(
                || async {
                    let mut builder = client_ref.delete(&url);

                    // 如果有会话 ID，添加到请求头
                    if let Some(session_id) = &session_id {
                        builder = builder.header("X-Session-ID", session_id);
                    }

                    let response = builder
                        .send()
                        .await
                        .map_err(|e| Error::RequestError(e.to_string()))?;

                    // 检查响应状态码，如果是服务器错误，转换为 ApiError
                    if response.status().is_server_error() {
                        return Err(Error::ApiError(
                            response.status().as_u16(),
                            response.text().await.unwrap_or_default(),
                        ));
                    }

                    // 如果启用了故障转移，报告成功
                    if let Some(failover) = &failover_manager {
                        failover.report_success().await?;
                    }

                    Ok(response)
                },
                retry_config,
            ).await;

            // 释放连接
            pool_ref.release_connection(client_ref).await;

            // 处理结果
            match result {
                Ok(response) => Ok(response),
                Err(error) => {
                    // 如果启用了故障转移，尝试故障转移
                    if let Some(failover) = &self.failover_manager {
                        let should_failover = failover.report_failure(&error).await?;
                        if should_failover {
                            failover.perform_failover().await?;
                            // 使用新的端点重试
                            let client = self.pool.get_connection().await?;
                            let new_url = format!("{}{}", failover.get_current_endpoint().await, path);

                            let mut builder = client.delete(&new_url);

                            // 如果有会话 ID，添加到请求头
                            if let Some(session_id) = &self.session_id {
                                builder = builder.header("X-Session-ID", session_id);
                            }

                            let response = builder
                                .send()
                                .await
                                .map_err(|e| Error::RequestError(e.to_string()))?;

                            // 释放连接
                            self.pool.release_connection(&client).await;

                            return Ok(response);
                        }
                    }
                    Err(error)
                }
            }
        } else {
            // 不使用重试机制
            let mut builder = client.delete(&url);

            // 如果有会话 ID，添加到请求头
            if let Some(session_id) = &self.session_id {
                builder = builder.header("X-Session-ID", session_id);
            }

            let response = builder
                .send()
                .await
                .map_err(|e| {
                    // 如果启用了故障转移，报告失败
                    if let Some(failover) = &self.failover_manager {
                        let error = Error::RequestError(e.to_string());
                        let _ = futures::executor::block_on(async {
                            let should_failover = failover.report_failure(&error).await?;
                            if should_failover {
                                failover.perform_failover().await?;
                            }
                            Ok::<_, Error>(())
                        });
                    }
                    Error::RequestError(e.to_string())
                })?;

            // 如果启用了故障转移，报告成功
            if let Some(failover) = &self.failover_manager {
                let _ = failover.report_success().await;
            }

            // 释放连接
            self.pool.release_connection(&client).await;

            Ok(response)
        }
    }

    /// 处理 API 响应
    pub async fn handle_response<T: DeserializeOwned>(&self, response: Response) -> Result<T> {
        match response.status() {
            StatusCode::OK | StatusCode::CREATED => {
                response
                    .json::<T>()
                    .await
                    .map_err(|e| Error::DeserializationError(e.to_string()))
            }
            status => {
                let error_text = response.text().await.unwrap_or_default();
                Err(Error::ApiError(status.as_u16(), error_text))
            }
        }
    }

    /// 处理无返回值的 API 响应
    pub async fn handle_empty_response(&self, response: Response) -> Result<()> {
        match response.status() {
            StatusCode::OK | StatusCode::CREATED | StatusCode::NO_CONTENT => Ok(()),
            status => {
                let error_text = response.text().await.unwrap_or_default();
                Err(Error::ApiError(status.as_u16(), error_text))
            }
        }
    }

    /// 处理预取请求
    pub async fn process_prefetch(&self) -> Result<()> {
        if let Some(prefetch_manager) = &self.prefetch_manager {
            if let Some(cache) = &self.cache {
                // 获取下一个预取项
                if let Some(key) = prefetch_manager.get_next_item().await {
                    // 检查是否已在缓存中
                    if cache.get_response(&key).await.is_some() {
                        // 已在缓存中，标记为完成
                        prefetch_manager.complete_item(&key, true).await;
                        return Ok(());
                    }

                    // 执行预取
                    let result = match key.method.as_str() {
                        "GET" => self.get(&key.path).await,
                        // 其他方法可以根据需要添加
                        _ => return Ok(()),
                    };

                    // 标记预取完成
                    prefetch_manager.complete_item(&key, result.is_ok()).await;
                }
            }
        }

        Ok(())
    }

    /// 启动预取处理循环
    pub fn start_prefetch_processing(&self) {
        if let Some(prefetch_manager) = &self.prefetch_manager {
            if let Some(cache) = &self.cache {
                let client = self.clone();

                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                        if let Err(e) = client.process_prefetch().await {
                            tracing::warn!("预取处理错误: {}", e);
                        }
                    }
                });
            }
        }
    }

    /// 清除缓存
    pub async fn clear_cache(&self) -> Result<()> {
        if let Some(cache) = &self.cache {
            cache.clear().await?;
        }

        Ok(())
    }

    /// 清除特定路径的缓存
    pub async fn clear_cache_path(&self, path_prefix: &str) -> Result<()> {
        if let Some(cache) = &self.cache {
            cache.clear_path(path_prefix).await?;
        }

        Ok(())
    }
}
