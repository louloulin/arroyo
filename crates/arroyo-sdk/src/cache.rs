use crate::error::{Error, Result};
use lru::LruCache;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, warn};

/// 缓存键
#[derive(Debug, Clone, Eq)]
pub struct CacheKey {
    /// 请求方法
    pub method: String,
    /// 请求路径
    pub path: String,
    /// 请求查询参数
    pub query_params: Option<HashMap<String, String>>,
    /// 请求体
    pub body: Option<Vec<u8>>,
}

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.method == other.method
            && self.path == other.path
            && self.query_params == other.query_params
            && self.body == other.body
    }
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.method.hash(state);
        self.path.hash(state);
        if let Some(params) = &self.query_params {
            // 对参数进行排序，确保相同参数的不同顺序产生相同的哈希值
            let mut sorted_params: Vec<_> = params.iter().collect();
            sorted_params.sort_by(|a, b| a.0.cmp(b.0));
            for (k, v) in sorted_params {
                k.hash(state);
                v.hash(state);
            }
        }
        if let Some(body) = &self.body {
            body.hash(state);
        }
    }
}

/// 缓存条目
#[derive(Debug, Clone)]
pub struct CacheEntry<T> {
    /// 缓存的数据
    pub data: T,
    /// 过期时间
    pub expires_at: Instant,
    /// 创建时间
    pub created_at: Instant,
    /// 最后访问时间
    pub last_accessed: Instant,
    /// 访问次数
    pub access_count: u64,
}

/// 响应缓存条目
#[derive(Debug, Clone)]
pub struct ResponseCacheEntry {
    /// 状态码
    pub status: u16,
    /// 响应头
    pub headers: HashMap<String, String>,
    /// 响应体
    pub body: Vec<u8>,
    /// 过期时间
    pub expires_at: Instant,
    /// 创建时间
    pub created_at: Instant,
    /// 最后访问时间
    pub last_accessed: Instant,
    /// 访问次数
    pub access_count: u64,
}

/// 缓存策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStrategy {
    /// 不缓存
    NoCache,
    /// 使用缓存，但每次都验证
    ValidateAlways,
    /// 使用缓存，直到过期
    UseUntilExpired,
}

/// 缓存配置
#[derive(Debug, Clone, PartialEq)]
pub struct CacheConfig {
    /// 是否启用缓存
    pub enabled: bool,
    /// 缓存策略
    pub strategy: CacheStrategy,
    /// 缓存大小（条目数）
    pub max_entries: usize,
    /// 默认过期时间（秒）
    pub default_ttl_secs: u64,
    /// 缓存清理间隔（秒）
    pub cleanup_interval_secs: u64,
    /// 是否启用预取
    pub enable_prefetch: bool,
    /// 预取阈值（0.0-1.0，表示剩余 TTL 的比例）
    pub prefetch_threshold: f64,
    /// 可缓存的路径前缀
    pub cacheable_paths: Vec<String>,
    /// 不可缓存的路径前缀
    pub non_cacheable_paths: Vec<String>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            strategy: CacheStrategy::UseUntilExpired,
            max_entries: 1000,
            default_ttl_secs: 60,
            cleanup_interval_secs: 300,
            enable_prefetch: true,
            prefetch_threshold: 0.8,
            cacheable_paths: vec!["/api/topics".to_string(), "/api/jobs".to_string()],
            non_cacheable_paths: vec![],
        }
    }
}

/// 客户端缓存管理器
#[derive(Debug, Clone)]
pub struct ClientCache {
    /// 缓存配置
    config: CacheConfig,
    /// 缓存数据
    cache: Arc<Mutex<LruCache<CacheKey, Arc<Vec<u8>>>>>,
    /// 缓存元数据
    metadata: Arc<RwLock<HashMap<CacheKey, CacheEntry<()>>>>,
    /// 响应缓存
    response_cache: Arc<Mutex<LruCache<CacheKey, Arc<ResponseCacheEntry>>>>,
    /// 预取队列
    prefetch_queue: Arc<Mutex<Vec<CacheKey>>>,
    /// 是否正在清理
    is_cleaning: Arc<Mutex<bool>>,
}

impl ClientCache {
    /// 创建新的客户端缓存
    pub fn new(config: CacheConfig) -> Self {
        let cache_size = NonZeroUsize::new(config.max_entries).unwrap_or(NonZeroUsize::new(1).unwrap());
        let cache = Arc::new(Mutex::new(LruCache::new(cache_size)));
        let metadata = Arc::new(RwLock::new(HashMap::new()));
        let response_cache = Arc::new(Mutex::new(LruCache::new(cache_size)));
        let prefetch_queue = Arc::new(Mutex::new(Vec::new()));
        let is_cleaning = Arc::new(Mutex::new(false));

        let client_cache = Self {
            config,
            cache,
            metadata,
            response_cache,
            prefetch_queue,
            is_cleaning,
        };

        // 启动后台清理任务
        if client_cache.config.enabled {
            let cache_clone = client_cache.clone();
            tokio::spawn(async move {
                cache_clone.start_cleanup_task().await;
            });

            // 启动预取任务
            if client_cache.config.enable_prefetch {
                let cache_clone = client_cache.clone();
                tokio::spawn(async move {
                    cache_clone.start_prefetch_task().await;
                });
            }
        }

        client_cache
    }

    /// 检查路径是否可缓存
    pub fn is_cacheable(&self, path: &str) -> bool {
        if !self.config.enabled {
            return false;
        }

        // 检查不可缓存路径
        for prefix in &self.config.non_cacheable_paths {
            if path.starts_with(prefix) {
                return false;
            }
        }

        // 如果没有指定可缓存路径，则所有路径都可缓存
        if self.config.cacheable_paths.is_empty() {
            return true;
        }

        // 检查可缓存路径
        for prefix in &self.config.cacheable_paths {
            if path.starts_with(prefix) {
                return true;
            }
        }

        false
    }

    /// 获取缓存数据
    pub async fn get<T: DeserializeOwned>(&self, key: &CacheKey) -> Option<T> {
        if !self.config.enabled || !self.is_cacheable(&key.path) {
            return None;
        }

        let mut cache = self.cache.lock().await;
        let data = cache.get(key)?;

        // 更新元数据
        let mut metadata = self.metadata.write().await;
        if let Some(entry) = metadata.get_mut(key) {
            entry.last_accessed = Instant::now();
            entry.access_count += 1;

            // 检查是否需要预取
            if self.config.enable_prefetch {
                let elapsed = Instant::now().duration_since(entry.created_at);
                let ttl = entry.expires_at.duration_since(entry.created_at);
                if ttl.as_secs() > 0 && elapsed.as_secs_f64() / ttl.as_secs_f64() > self.config.prefetch_threshold {
                    let mut prefetch_queue = self.prefetch_queue.lock().await;
                    prefetch_queue.push(key.clone());
                }
            }
        }

        // 反序列化数据
        match serde_json::from_slice::<T>(&data) {
            Ok(value) => Some(value),
            Err(e) => {
                warn!("Failed to deserialize cached data: {}", e);
                None
            }
        }
    }

    /// 设置缓存数据
    pub async fn set<T: Serialize>(&self, key: CacheKey, value: &T, ttl_secs: Option<u64>) -> Result<()> {
        if !self.config.enabled || !self.is_cacheable(&key.path) {
            return Ok(());
        }

        // 序列化数据
        let data = serde_json::to_vec(value).map_err(|e| Error::SerializationError(e.to_string()))?;
        let data_arc = Arc::new(data);

        // 计算过期时间
        let ttl = Duration::from_secs(ttl_secs.unwrap_or(self.config.default_ttl_secs));
        let now = Instant::now();
        let expires_at = now + ttl;

        // 更新缓存
        let mut cache = self.cache.lock().await;
        cache.put(key.clone(), data_arc.clone());

        // 更新元数据
        let mut metadata = self.metadata.write().await;
        metadata.insert(
            key,
            CacheEntry {
                data: (),
                expires_at,
                created_at: now,
                last_accessed: now,
                access_count: 1,
            },
        );

        Ok(())
    }

    /// 清除缓存
    pub async fn clear(&self) -> Result<()> {
        let mut cache = self.cache.lock().await;
        cache.clear();

        let mut metadata = self.metadata.write().await;
        metadata.clear();

        let mut prefetch_queue = self.prefetch_queue.lock().await;
        prefetch_queue.clear();

        Ok(())
    }

    /// 清除特定路径的缓存
    pub async fn clear_path(&self, path_prefix: &str) -> Result<()> {
        let mut cache = self.cache.lock().await;
        let mut metadata = self.metadata.write().await;

        // 收集要删除的键
        let keys_to_remove: Vec<CacheKey> = metadata
            .keys()
            .filter(|k| k.path.starts_with(path_prefix))
            .cloned()
            .collect();

        // 从缓存和元数据中删除
        for key in keys_to_remove {
            cache.pop(&key);
            metadata.remove(&key);
        }

        // 清理预取队列
        let mut prefetch_queue = self.prefetch_queue.lock().await;
        prefetch_queue.retain(|k| !k.path.starts_with(path_prefix));

        Ok(())
    }

    /// 清理过期缓存
    pub async fn cleanup_expired(&self) -> Result<()> {
        let mut is_cleaning = self.is_cleaning.lock().await;
        if *is_cleaning {
            return Ok(());
        }
        *is_cleaning = true;

        let now = Instant::now();
        let mut metadata = self.metadata.write().await;
        let mut cache = self.cache.lock().await;
        let mut response_cache = self.response_cache.lock().await;

        // 收集过期的键
        let expired_keys: Vec<CacheKey> = metadata
            .iter()
            .filter(|(_, entry)| entry.expires_at <= now)
            .map(|(k, _)| k.clone())
            .collect();

        // 从缓存和元数据中删除
        for key in expired_keys {
            cache.pop(&key);
            response_cache.pop(&key);
            metadata.remove(&key);
        }

        *is_cleaning = false;
        Ok(())
    }

    /// 缓存 HTTP 响应
    pub async fn cache_response(&self, key: CacheKey, response: &reqwest::Response, ttl_secs: Option<u64>) -> Result<()> {
        if !self.config.enabled || !self.is_cacheable(&key.path) {
            return Ok(());
        }

        // 克隆响应数据
        let status = response.status().as_u16();
        let headers = response.headers().iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or_default().to_string()))
            .collect::<HashMap<String, String>>();

        // 由于 reqwest::Response 不允许克隆或获取响应体而不消耗响应，
        // 我们在这里只缓存状态码和头信息，不缓存响应体
        // 在实际应用中，我们可能需要修改 reqwest 库或使用其他方法
        let body_bytes = Vec::new();

        // 计算过期时间
        let ttl = Duration::from_secs(ttl_secs.unwrap_or(self.config.default_ttl_secs));
        let now = Instant::now();
        let expires_at = now + ttl;

        // 创建缓存条目
        let entry = ResponseCacheEntry {
            status,
            headers,
            body: body_bytes,
            expires_at,
            created_at: now,
            last_accessed: now,
            access_count: 1,
        };

        // 更新缓存
        let mut response_cache = self.response_cache.lock().await;
        response_cache.put(key.clone(), Arc::new(entry));

        // 更新元数据
        let mut metadata = self.metadata.write().await;
        metadata.insert(
            key,
            CacheEntry {
                data: (),
                expires_at,
                created_at: now,
                last_accessed: now,
                access_count: 1,
            },
        );

        Ok(())
    }

    /// 获取缓存的 HTTP 响应
    pub async fn get_response(&self, key: &CacheKey) -> Option<reqwest::Response> {
        if !self.config.enabled || !self.is_cacheable(&key.path) {
            return None;
        }

        let mut response_cache = self.response_cache.lock().await;
        let entry = response_cache.get(key)?;

        // 更新元数据
        let mut metadata = self.metadata.write().await;
        if let Some(meta_entry) = metadata.get_mut(key) {
            meta_entry.last_accessed = Instant::now();
            meta_entry.access_count += 1;
        }

        // 创建响应对象 - 由于 reqwest::Response 不能直接构造，我们使用一个变通方法
        // 创建一个新的客户端和请求，然后使用缓存的数据构造响应
        let client = reqwest::Client::new();
        let url = format!("http://localhost/{}", key.path);
        let request = client.get(&url).build().ok()?;

        // 创建响应
        let mut headers = HeaderMap::new();
        for (name, value) in &entry.headers {
            if let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) {
                if let Ok(header_value) = HeaderValue::from_str(value) {
                    headers.insert(header_name, header_value);
                }
            }
        }

        // 由于 reqwest::Response 不能直接构造，我们需要使用其他方法
        // 在实际应用中，我们可以使用以下方法之一：
        // 1. 使用 reqwest 的内部 API（不推荐，因为它们可能会更改）
        // 2. 使用 mock 库创建模拟响应
        // 3. 修改我们的缓存策略，缓存解析后的数据而不是原始响应

        // 这里我们简单返回 None，表示无法从缓存中恢复响应
        // 在实际实现中，我们会使用上述方法之一
        None
    }

    /// 启动清理任务
    async fn start_cleanup_task(&self) {
        let interval = Duration::from_secs(self.config.cleanup_interval_secs);

        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = self.cleanup_expired().await {
                warn!("Failed to cleanup expired cache entries: {}", e);
            }
        }
    }

    /// 启动预取任务
    async fn start_prefetch_task(&self) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            // 处理预取队列
            let mut prefetch_queue = self.prefetch_queue.lock().await;
            if prefetch_queue.is_empty() {
                continue;
            }

            let keys_to_prefetch: Vec<CacheKey> = prefetch_queue.drain(..).collect();
            drop(prefetch_queue);

            for key in keys_to_prefetch {
                debug!("Prefetching data for key: {:?}", key);
                // 实际的预取逻辑将在 PooledArroyoClient 中实现
                // 这里只是将键添加到预取队列中
            }
        }
    }
}
