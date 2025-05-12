use crate::error::{Error, Result};
use rand::Rng;
use std::future::Future;
use std::time::Duration;
use tracing::{debug, error, warn};

/// 重试策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryStrategy {
    /// 固定间隔重试
    Fixed,
    /// 指数退避重试
    Exponential,
    /// 指数退避重试，带抖动
    ExponentialWithJitter,
}

/// 重试配置
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// 最大重试次数
    pub max_retries: u32,
    /// 基础重试间隔（毫秒）
    pub base_delay_ms: u64,
    /// 最大重试间隔（毫秒）
    pub max_delay_ms: u64,
    /// 重试策略
    pub strategy: RetryStrategy,
    /// 可重试的错误类型
    pub retryable_errors: Vec<RetryableErrorType>,
}

/// 可重试的错误类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryableErrorType {
    /// 连接错误
    Connection,
    /// 请求错误
    Request,
    /// 服务器错误（5xx）
    Server,
    /// 限流错误（429）
    RateLimit,
    /// 冲突错误（409）
    Conflict,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 10000,
            strategy: RetryStrategy::ExponentialWithJitter,
            retryable_errors: vec![
                RetryableErrorType::Connection,
                RetryableErrorType::Request,
                RetryableErrorType::Server,
                RetryableErrorType::RateLimit,
            ],
        }
    }
}

impl RetryConfig {
    /// 创建新的重试配置
    pub fn new(
        max_retries: u32,
        base_delay_ms: u64,
        max_delay_ms: u64,
        strategy: RetryStrategy,
    ) -> Self {
        Self {
            max_retries,
            base_delay_ms,
            max_delay_ms,
            strategy,
            retryable_errors: vec![
                RetryableErrorType::Connection,
                RetryableErrorType::Request,
                RetryableErrorType::Server,
                RetryableErrorType::RateLimit,
            ],
        }
    }

    /// 添加可重试的错误类型
    pub fn with_retryable_error(mut self, error_type: RetryableErrorType) -> Self {
        if !self.retryable_errors.contains(&error_type) {
            self.retryable_errors.push(error_type);
        }
        self
    }

    /// 设置可重试的错误类型
    pub fn with_retryable_errors(mut self, error_types: Vec<RetryableErrorType>) -> Self {
        self.retryable_errors = error_types;
        self
    }

    /// 计算重试延迟
    pub fn calculate_delay(&self, retry_count: u32) -> Duration {
        let delay_ms = match self.strategy {
            RetryStrategy::Fixed => self.base_delay_ms,
            RetryStrategy::Exponential => {
                let exp_delay = self.base_delay_ms * 2u64.pow(retry_count);
                std::cmp::min(exp_delay, self.max_delay_ms)
            }
            RetryStrategy::ExponentialWithJitter => {
                let exp_delay = self.base_delay_ms * 2u64.pow(retry_count);
                let max_delay = std::cmp::min(exp_delay, self.max_delay_ms);
                let min_delay = max_delay / 2;
                let jitter = rand::thread_rng().gen_range(0..=min_delay);
                min_delay + jitter
            }
        };

        Duration::from_millis(delay_ms)
    }

    /// 检查错误是否可重试
    pub fn is_retryable(&self, error: &Error) -> bool {
        match error {
            Error::ConnectionError(_) => self.retryable_errors.contains(&RetryableErrorType::Connection),
            Error::RequestError(_) => self.retryable_errors.contains(&RetryableErrorType::Request),
            Error::ApiError(status, _) => {
                if (500..600).contains(status) {
                    self.retryable_errors.contains(&RetryableErrorType::Server)
                } else if *status == 429 {
                    self.retryable_errors.contains(&RetryableErrorType::RateLimit)
                } else if *status == 409 {
                    self.retryable_errors.contains(&RetryableErrorType::Conflict)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

/// 执行带重试的异步操作
pub async fn retry_async<F, Fut, T>(operation: F, config: &RetryConfig) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut retries = 0;

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if retries >= config.max_retries || !config.is_retryable(&error) {
                    return Err(error);
                }

                retries += 1;
                let delay = config.calculate_delay(retries);

                match &error {
                    Error::ConnectionError(msg) => {
                        warn!("连接错误: {}. 重试 {}/{}...", msg, retries, config.max_retries);
                    }
                    Error::RequestError(msg) => {
                        warn!("请求错误: {}. 重试 {}/{}...", msg, retries, config.max_retries);
                    }
                    Error::ApiError(status, msg) => {
                        warn!(
                            "API 错误 ({}): {}. 重试 {}/{}...",
                            status, msg, retries, config.max_retries
                        );
                    }
                    _ => {
                        warn!("错误: {}. 重试 {}/{}...", error, retries, config.max_retries);
                    }
                }

                debug!("等待 {:?} 后重试...", delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// 执行带重试的异步操作，带有自定义错误处理
pub async fn retry_async_with_handler<F, Fut, T, H>(
    operation: F,
    config: &RetryConfig,
    error_handler: H,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
    H: Fn(&Error, u32, u32),
{
    let mut retries = 0;

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if retries >= config.max_retries || !config.is_retryable(&error) {
                    return Err(error);
                }

                retries += 1;
                error_handler(&error, retries, config.max_retries);
                
                let delay = config.calculate_delay(retries);
                debug!("等待 {:?} 后重试...", delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
}
