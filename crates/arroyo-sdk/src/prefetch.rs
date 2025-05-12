use crate::cache::CacheKey;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

/// 预取策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchStrategy {
    /// 不预取
    None,
    /// 基于访问模式预取
    AccessPattern,
    /// 基于关联关系预取
    Relationship,
    /// 基于时间预取
    TimeBased,
}

/// 预取配置
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// 是否启用预取
    pub enabled: bool,
    /// 预取策略
    pub strategy: PrefetchStrategy,
    /// 预取阈值（0.0-1.0，表示剩余 TTL 的比例）
    pub threshold: f64,
    /// 最大预取数量
    pub max_prefetch_count: usize,
    /// 预取间隔（毫秒）
    pub interval_ms: u64,
    /// 预取超时（毫秒）
    pub timeout_ms: u64,
    /// 预取优先级（0-10，越高越优先）
    pub priority: u8,
    /// 预取关系配置
    pub relationships: HashMap<String, Vec<String>>,
    /// 预取时间配置（路径 -> 时间间隔（秒））
    pub time_based_configs: HashMap<String, u64>,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            strategy: PrefetchStrategy::AccessPattern,
            threshold: 0.8,
            max_prefetch_count: 10,
            interval_ms: 1000,
            timeout_ms: 5000,
            priority: 5,
            relationships: HashMap::new(),
            time_based_configs: HashMap::new(),
        }
    }
}

/// 预取项
#[derive(Debug, Clone)]
struct PrefetchItem {
    /// 缓存键
    key: CacheKey,
    /// 优先级
    priority: u8,
    /// 添加时间
    added_at: Instant,
    /// 预取尝试次数
    attempts: u32,
}

/// 访问模式记录
#[derive(Debug, Clone)]
struct AccessPattern {
    /// 源路径
    source_path: String,
    /// 目标路径
    target_path: String,
    /// 访问次数
    count: u64,
    /// 最后访问时间
    last_accessed: Instant,
    /// 平均时间间隔（毫秒）
    avg_interval_ms: u64,
}

/// 预取管理器
#[derive(Debug, Clone)]
pub struct PrefetchManager {
    /// 预取配置
    config: PrefetchConfig,
    /// 预取队列
    queue: Arc<Mutex<VecDeque<PrefetchItem>>>,
    /// 正在预取的键集合
    in_progress: Arc<Mutex<HashSet<CacheKey>>>,
    /// 访问模式记录
    access_patterns: Arc<RwLock<HashMap<String, HashMap<String, AccessPattern>>>>,
    /// 最近访问的路径
    recent_accesses: Arc<Mutex<VecDeque<(String, Instant)>>>,
    /// 是否正在处理
    is_processing: Arc<Mutex<bool>>,
}

impl PrefetchManager {
    /// 创建新的预取管理器
    pub fn new(config: PrefetchConfig) -> Self {
        let manager = Self {
            config,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            in_progress: Arc::new(Mutex::new(HashSet::new())),
            access_patterns: Arc::new(RwLock::new(HashMap::new())),
            recent_accesses: Arc::new(Mutex::new(VecDeque::with_capacity(100))),
            is_processing: Arc::new(Mutex::new(false)),
        };

        // 初始化预定义的关系
        if manager.config.enabled && manager.config.strategy == PrefetchStrategy::Relationship {
            let manager_clone = manager.clone();
            tokio::spawn(async move {
                manager_clone.initialize_relationships().await;
            });
        }

        // 启动时间预取任务
        if manager.config.enabled && manager.config.strategy == PrefetchStrategy::TimeBased {
            let manager_clone = manager.clone();
            tokio::spawn(async move {
                manager_clone.start_time_based_prefetch().await;
            });
        }

        manager
    }

    /// 初始化预定义的关系
    async fn initialize_relationships(&self) {
        let mut patterns = self.access_patterns.write().await;

        for (source, targets) in &self.config.relationships {
            let source_patterns = patterns.entry(source.clone()).or_insert_with(HashMap::new);

            for target in targets {
                source_patterns.insert(
                    target.clone(),
                    AccessPattern {
                        source_path: source.clone(),
                        target_path: target.clone(),
                        count: 1, // 初始计数
                        last_accessed: Instant::now(),
                        avg_interval_ms: 1000, // 默认间隔
                    },
                );
            }
        }
    }

    /// 启动时间预取任务
    async fn start_time_based_prefetch(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(self.config.interval_ms)).await;

            if !self.config.enabled || self.config.strategy != PrefetchStrategy::TimeBased {
                continue;
            }

            let now = Instant::now();

            // 检查每个时间配置
            for (path, interval_secs) in &self.config.time_based_configs {
                // 创建预取键
                let key = CacheKey {
                    method: "GET".to_string(),
                    path: path.clone(),
                    query_params: None,
                    body: None,
                };

                // 添加到预取队列
                self.add_to_queue(key, self.config.priority).await;
            }
        }
    }

    /// 记录访问
    pub async fn record_access(&self, path: &str) {
        if !self.config.enabled {
            return;
        }

        let now = Instant::now();

        // 记录最近访问
        let mut recent = self.recent_accesses.lock().await;
        recent.push_back((path.to_string(), now));

        // 限制队列大小
        while recent.len() > 100 {
            recent.pop_front();
        }

        // 如果使用访问模式策略，更新访问模式
        if self.config.strategy == PrefetchStrategy::AccessPattern {
            // 获取前一个访问
            if let Some((prev_path, prev_time)) = recent.iter().rev().nth(1) {
                let interval = now.duration_since(*prev_time).as_millis() as u64;

                // 更新访问模式
                let mut patterns = self.access_patterns.write().await;
                let source_patterns = patterns.entry(prev_path.clone()).or_insert_with(HashMap::new);

                if let Some(pattern) = source_patterns.get_mut(path) {
                    // 更新现有模式
                    pattern.count += 1;
                    pattern.last_accessed = now;
                    pattern.avg_interval_ms = (pattern.avg_interval_ms * (pattern.count - 1) + interval) / pattern.count;
                } else {
                    // 创建新模式
                    source_patterns.insert(
                        path.to_string(),
                        AccessPattern {
                            source_path: prev_path.clone(),
                            target_path: path.to_string(),
                            count: 1,
                            last_accessed: now,
                            avg_interval_ms: interval,
                        },
                    );
                }
            }
        }
    }

    /// 预测下一个访问
    pub async fn predict_next_accesses(&self, path: &str) -> Vec<String> {
        if !self.config.enabled || self.config.strategy != PrefetchStrategy::AccessPattern {
            return Vec::new();
        }

        let patterns = self.access_patterns.read().await;

        if let Some(source_patterns) = patterns.get(path) {
            // 按访问次数排序
            let mut sorted_patterns: Vec<_> = source_patterns.values().collect();
            sorted_patterns.sort_by(|a, b| b.count.cmp(&a.count));

            // 返回前 N 个预测
            return sorted_patterns
                .iter()
                .take(self.config.max_prefetch_count)
                .map(|p| p.target_path.clone())
                .collect();
        }

        Vec::new()
    }

    /// 添加预取项到队列
    pub async fn add_to_queue(&self, key: CacheKey, priority: u8) {
        if !self.config.enabled {
            return;
        }

        let mut queue = self.queue.lock().await;
        let in_progress = self.in_progress.lock().await;

        // 检查是否已在队列或正在处理
        let already_queued = queue.iter().any(|item| item.key == key);
        let already_in_progress = in_progress.contains(&key);

        if !already_queued && !already_in_progress {
            // 添加到队列
            queue.push_back(PrefetchItem {
                key,
                priority,
                added_at: Instant::now(),
                attempts: 0,
            });

            // 按优先级排序
            let mut vec: Vec<_> = queue.drain(..).collect();
            vec.sort_by(|a, b| b.priority.cmp(&a.priority));
            queue.extend(vec);
        }
    }

    /// 获取下一个预取项
    pub async fn get_next_item(&self) -> Option<CacheKey> {
        if !self.config.enabled {
            return None;
        }

        let mut queue = self.queue.lock().await;
        let mut in_progress = self.in_progress.lock().await;

        // 查找下一个未处理的项
        while let Some(item) = queue.pop_front() {
            // 检查是否已在处理
            if in_progress.contains(&item.key) {
                continue;
            }

            // 检查尝试次数
            if item.attempts >= 3 {
                continue;
            }

            // 检查超时
            if item.added_at.elapsed() > Duration::from_millis(self.config.timeout_ms) {
                continue;
            }

            // 标记为正在处理
            in_progress.insert(item.key.clone());

            return Some(item.key);
        }

        None
    }

    /// 完成预取项
    pub async fn complete_item(&self, key: &CacheKey, success: bool) {
        let mut in_progress = self.in_progress.lock().await;
        in_progress.remove(key);

        if !success {
            // 如果失败，可以选择重新添加到队列
            let mut queue = self.queue.lock().await;

            // 查找是否有相同的项
            for item in queue.iter_mut() {
                if item.key == *key {
                    item.attempts += 1;
                    return;
                }
            }

            // 添加新项
            queue.push_back(PrefetchItem {
                key: key.clone(),
                priority: self.config.priority,
                added_at: Instant::now(),
                attempts: 1,
            });
        }
    }
}
