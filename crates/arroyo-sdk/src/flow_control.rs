use governor::{Quota, RateLimiter as GovernorRateLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};
use serde::{Deserialize, Serialize};

/// 流量控制策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowControlStrategy {
    /// 无流量控制
    None,
    /// 固定速率限制
    FixedRate,
    /// 自适应速率限制
    Adaptive,
}

/// 背压策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackpressureStrategy {
    /// 无背压处理
    None,
    /// 阻塞等待
    Block,
    /// 丢弃消息
    Drop,
}

/// 流量控制配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowControlConfig {
    /// 流量控制策略
    pub strategy: FlowControlStrategy,
    /// 每秒最大消息数
    pub max_messages_per_second: u32,
    /// 背压策略
    pub backpressure_strategy: BackpressureStrategy,
    /// 缓冲区大小
    pub buffer_size: usize,
    /// 背压阈值（0.0-1.0）
    pub backpressure_threshold: f64,
}

impl Default for FlowControlConfig {
    fn default() -> Self {
        Self {
            strategy: FlowControlStrategy::None,
            max_messages_per_second: 1000,
            backpressure_strategy: BackpressureStrategy::Block,
            buffer_size: 10000,
            backpressure_threshold: 0.8,
        }
    }
}

use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::RateLimiter;

/// 流量控制器
pub struct FlowController {
    /// 配置
    config: FlowControlConfig,
    /// 限流器
    rate_limiter: Option<Arc<GovernorRateLimiter<NotKeyed, InMemoryState, DefaultClock>>>,
    /// 当前缓冲区使用量
    buffer_usage: Arc<Mutex<usize>>,
    /// 最后一次调整时间
    last_adjustment: Arc<Mutex<Instant>>,
}

impl FlowController {
    /// 创建新的流量控制器
    pub fn new(config: FlowControlConfig) -> Self {
        let rate_limiter = if config.strategy != FlowControlStrategy::None {
            let quota = Quota::per_second(NonZeroU32::new(config.max_messages_per_second).unwrap_or(NonZeroU32::new(1).unwrap()));
            Some(Arc::new(RateLimiter::direct(quota)))
        } else {
            None
        };

        Self {
            config,
            rate_limiter,
            buffer_usage: Arc::new(Mutex::new(0)),
            last_adjustment: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// 更新缓冲区使用量
    pub async fn update_buffer_usage(&self, usage: usize) {
        let mut buffer_usage = self.buffer_usage.lock().await;
        *buffer_usage = usage;
    }

    /// 获取当前缓冲区使用率
    pub async fn get_buffer_usage_ratio(&self) -> f64 {
        let buffer_usage = *self.buffer_usage.lock().await;
        buffer_usage as f64 / self.config.buffer_size as f64
    }

    /// 检查是否需要应用背压
    pub async fn should_apply_backpressure(&self) -> bool {
        let usage_ratio = self.get_buffer_usage_ratio().await;
        usage_ratio >= self.config.backpressure_threshold
    }

    /// 应用流量控制
    pub async fn apply_flow_control(&self) -> bool {
        // 如果没有启用流量控制，直接返回
        if self.config.strategy == FlowControlStrategy::None {
            return true;
        }

        // 检查是否需要应用背压
        if self.should_apply_backpressure().await {
            match self.config.backpressure_strategy {
                BackpressureStrategy::None => {}
                BackpressureStrategy::Block => {
                    // 等待直到缓冲区使用率低于阈值
                    while self.should_apply_backpressure().await {
                        sleep(Duration::from_millis(10)).await;
                    }
                }
                BackpressureStrategy::Drop => {
                    // 丢弃消息
                    return false;
                }
            }
        }

        // 应用速率限制
        if let Some(limiter) = &self.rate_limiter {
            limiter.until_ready().await;
        }

        true
    }

    /// 调整流量控制参数（仅适用于自适应策略）
    pub async fn adjust_parameters(&self, throughput: f64, latency: Duration) {
        if self.config.strategy != FlowControlStrategy::Adaptive {
            return;
        }

        let mut last_adjustment = self.last_adjustment.lock().await;
        if last_adjustment.elapsed() < Duration::from_secs(30) {
            return;
        }

        // 根据吞吐量和延迟调整参数
        // 这里可以实现更复杂的自适应算法
        if let Some(limiter) = &self.rate_limiter {
            let current_rate = self.config.max_messages_per_second;
            let buffer_usage = self.get_buffer_usage_ratio().await;

            let new_rate = if buffer_usage > 0.9 {
                // 缓冲区接近满，减少速率
                (current_rate as f64 * 0.8) as u32
            } else if buffer_usage < 0.3 && latency < Duration::from_millis(100) {
                // 缓冲区使用率低且延迟小，增加速率
                (current_rate as f64 * 1.2) as u32
            } else {
                current_rate
            };

            // 确保速率在合理范围内
            let new_rate = new_rate.max(100).min(10000);

            // 更新限流器
            if new_rate != current_rate {
                // 注意：由于 governor 的限制，我们不能直接更新现有的限流器
                // 在实际应用中，可能需要创建一个新的限流器并替换旧的
            }
        }

        *last_adjustment = Instant::now();
    }
}
