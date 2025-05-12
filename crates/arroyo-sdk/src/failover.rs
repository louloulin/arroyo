use crate::error::{Error, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// 故障转移状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverState {
    /// 正常状态
    Normal,
    /// 故障状态
    Failed,
    /// 恢复中状态
    Recovering,
}

/// 故障转移配置
#[derive(Debug, Clone, PartialEq)]
pub struct FailoverConfig {
    /// 是否启用故障转移
    pub enabled: bool,
    /// 故障检测阈值（连续失败次数）
    pub failure_threshold: u32,
    /// 故障恢复阈值（连续成功次数）
    pub recovery_threshold: u32,
    /// 故障检测窗口（秒）
    pub detection_window_secs: u64,
    /// 故障恢复间隔（秒）
    pub recovery_interval_secs: u64,
    /// 故障恢复超时（秒）
    pub recovery_timeout_secs: u64,
    /// 故障转移目标
    pub failover_targets: Vec<String>,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            failure_threshold: 3,
            recovery_threshold: 2,
            detection_window_secs: 60,
            recovery_interval_secs: 30,
            recovery_timeout_secs: 300,
            failover_targets: Vec::new(),
        }
    }
}

/// 故障转移管理器
#[derive(Debug, Clone)]
pub struct FailoverManager {
    /// 故障转移配置
    config: FailoverConfig,
    /// 主要端点
    primary_endpoint: String,
    /// 当前端点
    current_endpoint: Arc<RwLock<String>>,
    /// 当前状态
    state: Arc<RwLock<FailoverState>>,
    /// 连续失败次数
    failure_count: Arc<RwLock<u32>>,
    /// 连续成功次数
    success_count: Arc<RwLock<u32>>,
    /// 最后一次故障时间
    last_failure_time: Arc<RwLock<Option<Instant>>>,
    /// 最后一次恢复尝试时间
    last_recovery_attempt: Arc<RwLock<Option<Instant>>>,
    /// 当前故障转移目标索引
    current_target_index: Arc<RwLock<usize>>,
}

impl FailoverManager {
    /// 创建新的故障转移管理器
    pub fn new(primary_endpoint: impl Into<String>, config: FailoverConfig) -> Self {
        let primary = primary_endpoint.into();
        Self {
            config,
            primary_endpoint: primary.clone(),
            current_endpoint: Arc::new(RwLock::new(primary)),
            state: Arc::new(RwLock::new(FailoverState::Normal)),
            failure_count: Arc::new(RwLock::new(0)),
            success_count: Arc::new(RwLock::new(0)),
            last_failure_time: Arc::new(RwLock::new(None)),
            last_recovery_attempt: Arc::new(RwLock::new(None)),
            current_target_index: Arc::new(RwLock::new(0)),
        }
    }

    /// 获取当前端点
    pub async fn get_current_endpoint(&self) -> String {
        self.current_endpoint.read().await.clone()
    }

    /// 获取当前状态
    pub async fn get_state(&self) -> FailoverState {
        *self.state.read().await
    }

    /// 报告请求成功
    pub async fn report_success(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let mut state = self.state.write().await;
        let mut failure_count = self.failure_count.write().await;
        let mut success_count = self.success_count.write().await;

        // 重置失败计数
        *failure_count = 0;

        match *state {
            FailoverState::Normal => {
                // 正常状态下，不需要特殊处理
            }
            FailoverState::Failed | FailoverState::Recovering => {
                // 增加成功计数
                *success_count += 1;

                // 如果当前端点是主端点，并且达到恢复阈值，切换回正常状态
                if self.current_endpoint.read().await.as_str() == self.primary_endpoint
                    && *success_count >= self.config.recovery_threshold
                {
                    *state = FailoverState::Normal;
                    *success_count = 0;
                    info!("系统已恢复正常状态");
                } else if *success_count >= self.config.recovery_threshold {
                    // 如果不是主端点，但达到恢复阈值，尝试恢复到主端点
                    *state = FailoverState::Recovering;
                    *success_count = 0;
                    let mut last_recovery_attempt = self.last_recovery_attempt.write().await;
                    *last_recovery_attempt = Some(Instant::now());
                    debug!("尝试恢复到主端点");
                }
            }
        }

        Ok(())
    }

    /// 报告请求失败
    pub async fn report_failure(&self, error: &Error) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }

        let mut state = self.state.write().await;
        let mut failure_count = self.failure_count.write().await;
        let mut last_failure_time = self.last_failure_time.write().await;

        // 增加失败计数
        *failure_count += 1;
        *last_failure_time = Some(Instant::now());

        // 检查是否需要故障转移
        let should_failover = match *state {
            FailoverState::Normal => {
                if *failure_count >= self.config.failure_threshold {
                    *state = FailoverState::Failed;
                    warn!("检测到故障，切换到故障状态: {}", error);
                    true
                } else {
                    false
                }
            }
            FailoverState::Failed => {
                // 已经处于故障状态，不需要再次故障转移
                false
            }
            FailoverState::Recovering => {
                // 恢复过程中出现故障，回到故障状态
                *state = FailoverState::Failed;
                warn!("恢复过程中检测到故障，回到故障状态: {}", error);
                true
            }
        };

        Ok(should_failover)
    }

    /// 执行故障转移
    pub async fn perform_failover(&self) -> Result<String> {
        if !self.config.enabled || self.config.failover_targets.is_empty() {
            return Err(Error::ConnectionError("故障转移未启用或没有可用的故障转移目标".to_string()));
        }

        let mut current_target_index = self.current_target_index.write().await;
        let targets_count = self.config.failover_targets.len();

        // 选择下一个故障转移目标
        *current_target_index = (*current_target_index + 1) % targets_count;
        let new_endpoint = self.config.failover_targets[*current_target_index].clone();

        // 更新当前端点
        let mut current_endpoint = self.current_endpoint.write().await;
        *current_endpoint = new_endpoint.clone();

        info!("故障转移到新端点: {}", new_endpoint);
        Ok(new_endpoint)
    }

    /// 尝试恢复到主端点
    pub async fn try_recover(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }

        let state = self.state.read().await;
        if *state != FailoverState::Recovering {
            return Ok(false);
        }

        let last_recovery_attempt = self.last_recovery_attempt.read().await;
        if let Some(attempt_time) = *last_recovery_attempt {
            let elapsed = attempt_time.elapsed();
            if elapsed < Duration::from_secs(self.config.recovery_interval_secs) {
                // 恢复间隔未到，不尝试恢复
                return Ok(false);
            }
        }

        // 更新当前端点为主端点
        let mut current_endpoint = self.current_endpoint.write().await;
        *current_endpoint = self.primary_endpoint.clone();

        info!("尝试恢复到主端点: {}", self.primary_endpoint);
        Ok(true)
    }

    /// 检查是否应该尝试恢复
    pub async fn should_try_recover(&self) -> bool {
        if !self.config.enabled {
            return false;
        }

        let state = self.state.read().await;
        if *state != FailoverState::Failed {
            return false;
        }

        let last_failure_time = self.last_failure_time.read().await;
        if let Some(failure_time) = *last_failure_time {
            let elapsed = failure_time.elapsed();
            if elapsed >= Duration::from_secs(self.config.recovery_interval_secs) {
                // 恢复间隔已到，可以尝试恢复
                return true;
            }
        }

        false
    }
}
