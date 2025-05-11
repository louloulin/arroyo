use std::collections::HashMap;
use std::fmt::Debug;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::{Record, Watermark};

/// 水印策略类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatermarkStrategyType {
    /// 周期性水印：固定延迟
    Periodic,
    /// 标记水印：基于特定事件
    Punctuated,
    /// 自适应水印：根据数据特性动态调整
    Adaptive,
    /// 边界水印：基于窗口边界
    Bounded,
    /// 对齐水印：多源场景下的水印对齐
    Aligned,
}

/// 水印策略配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkStrategyConfig {
    /// 策略类型
    pub strategy_type: WatermarkStrategyType,
    /// 水印延迟
    pub max_delay: Duration,
    /// 水印生成间隔
    pub interval: Duration,
    /// 空闲超时
    pub idle_timeout: Option<Duration>,
    /// 自定义配置
    pub custom_config: Option<HashMap<String, String>>,
}

impl Default for WatermarkStrategyConfig {
    fn default() -> Self {
        Self {
            strategy_type: WatermarkStrategyType::Periodic,
            max_delay: Duration::from_secs(5),
            interval: Duration::from_secs(1),
            idle_timeout: Some(Duration::from_secs(60)),
            custom_config: None,
        }
    }
}

/// 水印策略特性
pub trait WatermarkStrategy: Send + Sync + Debug {
    /// 获取策略类型
    fn strategy_type(&self) -> WatermarkStrategyType;

    /// 获取策略配置
    fn config(&self) -> &WatermarkStrategyConfig;

    /// 处理字符串记录，更新内部状态
    fn on_event(&mut self, record: &Record<String>) -> Option<Watermark>;

    /// 周期性调用，生成水印
    fn on_periodic_watermark(&mut self) -> Option<Watermark>;

    /// 处理空闲状态
    fn on_idle(&mut self) -> Option<Watermark>;

    /// 获取当前水印
    fn current_watermark(&self) -> Option<Watermark>;
}

/// 周期性水印策略
#[derive(Debug)]
pub struct PeriodicWatermarkStrategy {
    /// 策略配置
    config: WatermarkStrategyConfig,
    /// 最大事件时间
    max_event_time: Option<SystemTime>,
    /// 上次水印时间
    last_watermark_time: Option<SystemTime>,
    /// 上次事件时间
    last_event_time: SystemTime,
    /// 是否空闲
    is_idle: bool,
}

impl PeriodicWatermarkStrategy {
    /// 创建新的周期性水印策略
    pub fn new(config: WatermarkStrategyConfig) -> Self {
        Self {
            config,
            max_event_time: None,
            last_watermark_time: None,
            last_event_time: SystemTime::now(),
            is_idle: false,
        }
    }
}

impl WatermarkStrategy for PeriodicWatermarkStrategy {
    fn strategy_type(&self) -> WatermarkStrategyType {
        WatermarkStrategyType::Periodic
    }

    fn config(&self) -> &WatermarkStrategyConfig {
        &self.config
    }

    fn on_event(&mut self, record: &Record<String>) -> Option<Watermark> {
        // 更新最大事件时间
        if let Some(event_time) = record.metadata.event_time {
            self.max_event_time = Some(match self.max_event_time {
                Some(max_time) => max_time.max(event_time),
                None => event_time,
            });
        }

        // 更新上次事件时间
        self.last_event_time = SystemTime::now();

        // 如果之前是空闲状态，现在有数据了，重置空闲状态
        if self.is_idle {
            self.is_idle = false;
            return Some(self.current_watermark()?);
        }

        None
    }

    fn on_periodic_watermark(&mut self) -> Option<Watermark> {
        let now = SystemTime::now();

        // 检查是否应该生成新的水印
        let should_emit = match self.last_watermark_time {
            Some(last_time) => now.duration_since(last_time).unwrap_or_default() >= self.config.interval,
            None => true,
        };

        if should_emit {
            self.last_watermark_time = Some(now);
            return self.current_watermark();
        }

        None
    }

    fn on_idle(&mut self) -> Option<Watermark> {
        // 检查是否应该进入空闲状态
        if let Some(idle_timeout) = self.config.idle_timeout {
            let now = SystemTime::now();
            if now.duration_since(self.last_event_time).unwrap_or_default() >= idle_timeout && !self.is_idle {
                self.is_idle = true;
                return Some(Watermark::Idle);
            }
        }

        None
    }

    fn current_watermark(&self) -> Option<Watermark> {
        // 如果没有事件时间，返回 None
        let max_time = self.max_event_time?;

        // 计算水印时间（最大事件时间减去最大延迟）
        let watermark_time = max_time.checked_sub(self.config.max_delay)?;

        Some(Watermark::EventTime(watermark_time))
    }
}

/// 标记水印策略
pub struct PunctuatedWatermarkStrategy {
    /// 策略配置
    config: WatermarkStrategyConfig,
    /// 最大事件时间
    max_event_time: Option<SystemTime>,
    /// 上次水印时间
    last_watermark_time: Option<SystemTime>,
    /// 上次事件时间
    last_event_time: SystemTime,
    /// 是否空闲
    is_idle: bool,
    /// 标记条件（自定义函数）
    is_punctuation: Box<dyn Fn(&Record<String>) -> bool + Send + Sync>,
}

impl Debug for PunctuatedWatermarkStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PunctuatedWatermarkStrategy")
            .field("config", &self.config)
            .field("max_event_time", &self.max_event_time)
            .field("last_watermark_time", &self.last_watermark_time)
            .field("last_event_time", &self.last_event_time)
            .field("is_idle", &self.is_idle)
            .field("is_punctuation", &"<function>")
            .finish()
    }
}

impl PunctuatedWatermarkStrategy {
    /// 创建新的标记水印策略
    pub fn new<F>(config: WatermarkStrategyConfig, is_punctuation: F) -> Self
    where
        F: Fn(&Record<String>) -> bool + Send + Sync + 'static,
    {
        Self {
            config,
            max_event_time: None,
            last_watermark_time: None,
            last_event_time: SystemTime::now(),
            is_idle: false,
            is_punctuation: Box::new(is_punctuation),
        }
    }
}

impl WatermarkStrategy for PunctuatedWatermarkStrategy {
    fn strategy_type(&self) -> WatermarkStrategyType {
        WatermarkStrategyType::Punctuated
    }

    fn config(&self) -> &WatermarkStrategyConfig {
        &self.config
    }

    fn on_event(&mut self, record: &Record<String>) -> Option<Watermark> {
        // 更新最大事件时间
        if let Some(event_time) = record.metadata.event_time {
            self.max_event_time = Some(match self.max_event_time {
                Some(max_time) => max_time.max(event_time),
                None => event_time,
            });
        }

        // 更新上次事件时间
        self.last_event_time = SystemTime::now();

        // 如果之前是空闲状态，现在有数据了，重置空闲状态
        if self.is_idle {
            self.is_idle = false;
        }

        // 检查是否是标记记录
        if (self.is_punctuation)(record) {
            return self.current_watermark();
        }

        None
    }

    fn on_periodic_watermark(&mut self) -> Option<Watermark> {
        // 标记水印策略不生成周期性水印
        None
    }

    fn on_idle(&mut self) -> Option<Watermark> {
        // 检查是否应该进入空闲状态
        if let Some(idle_timeout) = self.config.idle_timeout {
            let now = SystemTime::now();
            if now.duration_since(self.last_event_time).unwrap_or_default() >= idle_timeout && !self.is_idle {
                self.is_idle = true;
                return Some(Watermark::Idle);
            }
        }

        None
    }

    fn current_watermark(&self) -> Option<Watermark> {
        // 如果没有事件时间，返回 None
        let max_time = self.max_event_time?;

        // 计算水印时间（最大事件时间减去最大延迟）
        let watermark_time = max_time.checked_sub(self.config.max_delay)?;

        Some(Watermark::EventTime(watermark_time))
    }
}

/// 自适应水印策略
#[derive(Debug)]
pub struct AdaptiveWatermarkStrategy {
    /// 策略配置
    config: WatermarkStrategyConfig,
    /// 最大事件时间
    max_event_time: Option<SystemTime>,
    /// 上次水印时间
    last_watermark_time: Option<SystemTime>,
    /// 上次事件时间
    last_event_time: SystemTime,
    /// 是否空闲
    is_idle: bool,
    /// 事件时间分布窗口
    time_distribution: Vec<Duration>,
    /// 当前动态延迟
    current_delay: Duration,
    /// 分位数阈值（默认为 0.95，即 95% 的事件在此延迟内）
    quantile: f64,
}

impl AdaptiveWatermarkStrategy {
    /// 创建新的自适应水印策略
    pub fn new(config: WatermarkStrategyConfig) -> Self {
        // 从自定义配置中获取分位数，默认为 0.95
        let quantile = config
            .custom_config
            .as_ref()
            .and_then(|cfg| cfg.get("quantile"))
            .and_then(|q| q.parse::<f64>().ok())
            .unwrap_or(0.95);

        // 从自定义配置中获取窗口大小，默认为 1000
        let window_size = config
            .custom_config
            .as_ref()
            .and_then(|cfg| cfg.get("window_size"))
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1000);

        let max_delay = config.max_delay;

        Self {
            config,
            max_event_time: None,
            last_watermark_time: None,
            last_event_time: SystemTime::now(),
            is_idle: false,
            time_distribution: Vec::with_capacity(window_size),
            current_delay: max_delay,
            quantile,
        }
    }

    /// 更新动态延迟
    fn update_delay(&mut self) {
        if self.time_distribution.is_empty() {
            return;
        }

        // 对时间分布进行排序
        self.time_distribution.sort_unstable();

        // 计算分位数索引
        let idx = ((self.time_distribution.len() as f64) * self.quantile) as usize;
        let idx = idx.min(self.time_distribution.len() - 1);

        // 获取分位数延迟
        let quantile_delay = self.time_distribution[idx];

        // 更新当前延迟（不超过最大延迟）
        self.current_delay = quantile_delay.min(self.config.max_delay);

        // 如果分布窗口已满，清空一半
        if self.time_distribution.len() >= self.time_distribution.capacity() {
            let new_len = self.time_distribution.len() / 2;
            self.time_distribution.truncate(new_len);
        }
    }
}

impl WatermarkStrategy for AdaptiveWatermarkStrategy {
    fn strategy_type(&self) -> WatermarkStrategyType {
        WatermarkStrategyType::Adaptive
    }

    fn config(&self) -> &WatermarkStrategyConfig {
        &self.config
    }

    fn on_event(&mut self, record: &Record<String>) -> Option<Watermark> {
        // 获取事件时间
        if let Some(event_time) = record.metadata.event_time {
            // 计算事件时间与处理时间的差值
            let now = SystemTime::now();
            if let Ok(lateness) = now.duration_since(event_time) {
                // 添加到时间分布窗口
                self.time_distribution.push(lateness);

                // 更新最大事件时间
                self.max_event_time = Some(match self.max_event_time {
                    Some(max_time) => max_time.max(event_time),
                    None => event_time,
                });
            }
        }

        // 更新上次事件时间
        self.last_event_time = SystemTime::now();

        // 如果之前是空闲状态，现在有数据了，重置空闲状态
        if self.is_idle {
            self.is_idle = false;

            // 更新动态延迟
            self.update_delay();

            return self.current_watermark();
        }

        None
    }

    fn on_periodic_watermark(&mut self) -> Option<Watermark> {
        let now = SystemTime::now();

        // 检查是否应该生成新的水印
        let should_emit = match self.last_watermark_time {
            Some(last_time) => now.duration_since(last_time).unwrap_or_default() >= self.config.interval,
            None => true,
        };

        if should_emit {
            // 更新动态延迟
            self.update_delay();

            self.last_watermark_time = Some(now);
            return self.current_watermark();
        }

        None
    }

    fn on_idle(&mut self) -> Option<Watermark> {
        // 检查是否应该进入空闲状态
        if let Some(idle_timeout) = self.config.idle_timeout {
            let now = SystemTime::now();
            if now.duration_since(self.last_event_time).unwrap_or_default() >= idle_timeout && !self.is_idle {
                self.is_idle = true;
                return Some(Watermark::Idle);
            }
        }

        None
    }

    fn current_watermark(&self) -> Option<Watermark> {
        // 如果没有事件时间，返回 None
        let max_time = self.max_event_time?;

        // 计算水印时间（最大事件时间减去当前动态延迟）
        let watermark_time = max_time.checked_sub(self.current_delay)?;

        Some(Watermark::EventTime(watermark_time))
    }
}

/// 水印对齐器
#[derive(Debug)]
pub struct WatermarkAligner {
    /// 源水印
    source_watermarks: HashMap<String, Option<Watermark>>,
    /// 当前对齐水印
    current_aligned_watermark: Option<Watermark>,
    /// 是否所有源都空闲
    all_idle: bool,
}

impl WatermarkAligner {
    /// 创建新的水印对齐器
    pub fn new() -> Self {
        Self {
            source_watermarks: HashMap::new(),
            current_aligned_watermark: None,
            all_idle: false,
        }
    }

    /// 添加源
    pub fn add_source(&mut self, source_id: String) {
        self.source_watermarks.insert(source_id, None);
        self.all_idle = false;
    }

    /// 移除源
    pub fn remove_source(&mut self, source_id: &str) {
        self.source_watermarks.remove(source_id);
        self.update_aligned_watermark();
    }

    /// 更新源水印
    pub fn update_source_watermark(&mut self, source_id: &str, watermark: Option<Watermark>) -> Option<Watermark> {
        if let Some(current) = self.source_watermarks.get_mut(source_id) {
            *current = watermark;
            self.update_aligned_watermark()
        } else {
            None
        }
    }

    /// 更新对齐水印
    fn update_aligned_watermark(&mut self) -> Option<Watermark> {
        if self.source_watermarks.is_empty() {
            self.current_aligned_watermark = None;
            return None;
        }

        // 检查是否所有源都空闲
        let all_idle = self.source_watermarks.values().all(|w| {
            matches!(w, Some(Watermark::Idle))
        });

        if all_idle && !self.all_idle {
            self.all_idle = true;
            self.current_aligned_watermark = Some(Watermark::Idle);
            return self.current_aligned_watermark;
        }

        // 如果有任何源不是空闲状态，重置空闲标志
        if !all_idle {
            self.all_idle = false;
        }

        // 找出所有源的最小事件时间水印
        let min_watermark = self.source_watermarks.values()
            .filter_map(|w| match w {
                Some(Watermark::EventTime(time)) => Some(*time),
                _ => None,
            })
            .min();

        // 更新当前对齐水印
        if let Some(time) = min_watermark {
            self.current_aligned_watermark = Some(Watermark::EventTime(time));
        } else if self.all_idle {
            // 如果没有事件时间水印但所有源都空闲，保持空闲水印
            self.current_aligned_watermark = Some(Watermark::Idle);
        } else if !self.source_watermarks.is_empty() {
            // 如果有源但没有水印，使用默认水印
            let now = SystemTime::now();
            self.current_aligned_watermark = Some(Watermark::EventTime(now));
        } else {
            // 没有源，没有水印
            self.current_aligned_watermark = None;
        }

        self.current_aligned_watermark
    }

    /// 获取当前对齐水印
    pub fn current_watermark(&self) -> Option<Watermark> {
        self.current_aligned_watermark
    }
}

/// 边界水印策略
#[derive(Debug)]
pub struct BoundedWatermarkStrategy {
    /// 策略配置
    config: WatermarkStrategyConfig,
    /// 最大事件时间
    max_event_time: Option<SystemTime>,
    /// 上次水印时间
    last_watermark_time: Option<SystemTime>,
    /// 上次事件时间
    last_event_time: SystemTime,
    /// 是否空闲
    is_idle: bool,
    /// 窗口大小
    window_size: Duration,
    /// 窗口边界
    window_boundaries: Vec<SystemTime>,
    /// 当前窗口索引
    current_window_index: usize,
}

impl BoundedWatermarkStrategy {
    /// 创建新的边界水印策略
    pub fn new(config: WatermarkStrategyConfig) -> Self {
        // 从自定义配置中获取窗口大小，默认为 1 分钟
        let window_size = config
            .custom_config
            .as_ref()
            .and_then(|cfg| cfg.get("window_size"))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(60_000);

        Self {
            config,
            max_event_time: None,
            last_watermark_time: None,
            last_event_time: SystemTime::now(),
            is_idle: false,
            window_size: Duration::from_millis(window_size),
            window_boundaries: Vec::new(),
            current_window_index: 0,
        }
    }

    /// 更新窗口边界
    fn update_window_boundaries(&mut self, event_time: SystemTime) {
        if self.window_boundaries.is_empty() {
            // 初始化窗口边界
            let first_boundary = event_time
                .checked_add(self.window_size)
                .unwrap_or(event_time);

            self.window_boundaries.push(first_boundary);
            return;
        }

        // 检查是否需要添加新的窗口边界
        let last_boundary = *self.window_boundaries.last().unwrap();
        if event_time >= last_boundary {
            // 添加新的窗口边界
            let new_boundary = last_boundary
                .checked_add(self.window_size)
                .unwrap_or(last_boundary);

            self.window_boundaries.push(new_boundary);
        }
    }

    /// 获取当前窗口边界
    fn current_window_boundary(&self) -> Option<SystemTime> {
        self.window_boundaries.get(self.current_window_index).copied()
    }

    /// 前进到下一个窗口
    fn advance_window(&mut self) -> bool {
        if self.current_window_index + 1 < self.window_boundaries.len() {
            self.current_window_index += 1;
            true
        } else {
            false
        }
    }
}

impl WatermarkStrategy for BoundedWatermarkStrategy {
    fn strategy_type(&self) -> WatermarkStrategyType {
        WatermarkStrategyType::Bounded
    }

    fn config(&self) -> &WatermarkStrategyConfig {
        &self.config
    }

    fn on_event(&mut self, record: &Record<String>) -> Option<Watermark> {
        // 获取事件时间
        if let Some(event_time) = record.metadata.event_time {
            // 更新最大事件时间
            self.max_event_time = Some(match self.max_event_time {
                Some(max_time) => max_time.max(event_time),
                None => event_time,
            });

            // 更新窗口边界
            self.update_window_boundaries(event_time);
        }

        // 更新上次事件时间
        self.last_event_time = SystemTime::now();

        // 如果之前是空闲状态，现在有数据了，重置空闲状态
        if self.is_idle {
            self.is_idle = false;
            return self.current_watermark();
        }

        None
    }

    fn on_periodic_watermark(&mut self) -> Option<Watermark> {
        let now = SystemTime::now();

        // 检查是否应该生成新的水印
        let should_emit = match self.last_watermark_time {
            Some(last_time) => now.duration_since(last_time).unwrap_or_default() >= self.config.interval,
            None => true,
        };

        if should_emit {
            self.last_watermark_time = Some(now);

            // 检查是否应该前进到下一个窗口
            if let Some(max_time) = self.max_event_time {
                if let Some(current_boundary) = self.current_window_boundary() {
                    if max_time >= current_boundary {
                        // 前进到下一个窗口
                        if self.advance_window() {
                            return self.current_watermark();
                        }
                    }
                }
            }
        }

        None
    }

    fn on_idle(&mut self) -> Option<Watermark> {
        // 检查是否应该进入空闲状态
        if let Some(idle_timeout) = self.config.idle_timeout {
            let now = SystemTime::now();
            if now.duration_since(self.last_event_time).unwrap_or_default() >= idle_timeout && !self.is_idle {
                self.is_idle = true;
                return Some(Watermark::Idle);
            }
        }

        None
    }

    fn current_watermark(&self) -> Option<Watermark> {
        // 如果没有窗口边界，返回 None
        let current_boundary = self.current_window_boundary()?;

        // 计算水印时间（当前窗口边界减去最大延迟）
        let watermark_time = current_boundary.checked_sub(self.config.max_delay)?;

        Some(Watermark::EventTime(watermark_time))
    }
}

/// 水印策略工厂
#[derive(Debug, Default)]
pub struct WatermarkStrategyFactory;

impl WatermarkStrategyFactory {
    /// 创建新的水印策略工厂
    pub fn new() -> Self {
        Self
    }

    /// 创建周期性水印策略
    pub fn create_periodic(&self, config: WatermarkStrategyConfig) -> Box<dyn WatermarkStrategy> {
        Box::new(PeriodicWatermarkStrategy::new(config))
    }

    /// 创建标记水印策略
    pub fn create_punctuated<F>(&self, config: WatermarkStrategyConfig, is_punctuation: F) -> Box<dyn WatermarkStrategy>
    where
        F: Fn(&Record<String>) -> bool + Send + Sync + 'static,
    {
        Box::new(PunctuatedWatermarkStrategy::new(config, is_punctuation))
    }

    /// 创建自适应水印策略
    pub fn create_adaptive(&self, config: WatermarkStrategyConfig) -> Box<dyn WatermarkStrategy> {
        Box::new(AdaptiveWatermarkStrategy::new(config))
    }

    /// 创建边界水印策略
    pub fn create_bounded(&self, config: WatermarkStrategyConfig) -> Box<dyn WatermarkStrategy> {
        Box::new(BoundedWatermarkStrategy::new(config))
    }

    /// 根据策略类型创建水印策略
    pub fn create(&self, config: WatermarkStrategyConfig) -> Box<dyn WatermarkStrategy> {
        match config.strategy_type {
            WatermarkStrategyType::Periodic => self.create_periodic(config),
            WatermarkStrategyType::Adaptive => self.create_adaptive(config),
            WatermarkStrategyType::Bounded => self.create_bounded(config),
            WatermarkStrategyType::Punctuated => {
                // 默认的标记条件：总是返回 false
                self.create_punctuated(config, |_| false)
            },
            WatermarkStrategyType::Aligned => {
                // 对齐水印策略需要特殊处理，这里返回周期性水印策略作为默认行为
                self.create_periodic(config)
            },
        }
    }
}
