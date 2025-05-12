use std::collections::HashMap;
use std::time::Instant;

use anyhow::Result;
use tracing::{info, warn};
use arroyo_rpc;

use crate::dynamic_scaling::{ScalingOperation, ScalingPlan, StateRedistributionPlan};
use crate::{states::stop_if_desired_non_running, JobMessage};

use super::{running::Running, scheduling::Scheduling, JobContext, State, StateError, StateHolder, Transition};

/// 扩展模式
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingMode {
    /// 传统模式：停止作业，重新调度
    Traditional,
    /// 动态模式：无需停止作业，动态调整并行度
    Dynamic,
}

/// 扩展阶段
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingPhase {
    /// 准备阶段：创建扩展计划
    Preparing,
    /// 检查点阶段：创建检查点
    Checkpointing,
    /// 状态重分配阶段：重新分配状态
    StateRedistribution,
    /// 任务调整阶段：调整任务分配
    TaskAdjustment,
    /// 完成阶段：扩展完成
    Completed,
}

#[derive(Debug)]
pub struct Rescaling {
    /// 扩展模式
    pub mode: ScalingMode,
    /// 扩展阶段
    pub phase: ScalingPhase,
    /// 扩展计划
    pub scaling_plan: Option<ScalingPlan>,
    /// 状态重分配计划
    pub redistribution_plans: Vec<StateRedistributionPlan>,
    /// 阶段开始时间
    pub phase_start_time: Instant,
    /// 检查点已启动
    pub checkpoint_started: bool,
    /// 检查点已完成
    pub checkpoint_completed: bool,
}

impl Rescaling {
    /// 创建新的扩展状态
    pub fn new(mode: ScalingMode) -> Self {
        Self {
            mode,
            phase: ScalingPhase::Preparing,
            scaling_plan: None,
            redistribution_plans: Vec::new(),
            phase_start_time: Instant::now(),
            checkpoint_started: false,
            checkpoint_completed: false,
        }
    }



    /// 执行状态重分配
    async fn execute_state_redistribution(&self) -> Result<()> {
        for plan in &self.redistribution_plans {
            plan.execute().await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl State for Rescaling {
    fn name(&self) -> &'static str {
        "Rescaling"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        // 根据扩展模式选择不同的扩展策略
        match self.mode {
            ScalingMode::Traditional => {
                // 传统模式：停止作业，重新调度
                let job_controller = ctx.job_controller.as_mut().unwrap();

                if !self.checkpoint_started {
                    match job_controller.checkpoint(true).await {
                        Ok(started) => {
                            self.checkpoint_started = started;
                            info!("Started final checkpoint for traditional rescaling");
                        }
                        Err(e) => {
                            return Err(ctx.retryable(
                                self,
                                "failed to initiate final checkpoint",
                                e,
                                10,
                            ));
                        }
                    }
                }

                match job_controller.checkpoint_finished().await {
                    Ok(done) => {
                        if done && job_controller.finished() && self.checkpoint_started {
                            info!("Traditional rescaling completed, transitioning to Scheduling");
                            return Ok(Transition::next(*self, Scheduling {}));
                        }
                    }
                    Err(e) => {
                        return Err(ctx.retryable(
                            self,
                            "failed while monitoring final checkpoint",
                            e,
                            10,
                        ));
                    }
                }
            }
            ScalingMode::Dynamic => {
                // 动态模式：无需停止作业，动态调整并行度
                match self.phase {
                    ScalingPhase::Preparing => {
                        // 创建扩展计划
                        // 这里我们不再传递 ctx，而是直接在方法内部获取所需信息
                        let job_id = ctx.config.id.to_string();
                        let parallelism_overrides = ctx.config.parallelism_overrides.clone();

                        // 模拟获取当前epoch
                        let epoch = 1; // 简化处理，实际应该从作业控制器获取

                        // 模拟获取当前并行度
                        let mut current_parallelism = HashMap::new();
                        current_parallelism.insert(1, 2); // 假设操作符1的并行度为2
                        current_parallelism.insert(2, 4); // 假设操作符2的并行度为4

                        // 获取目标并行度
                        let target_parallelism: HashMap<u32, usize> = parallelism_overrides
                            .iter()
                            .map(|(k, v)| (*k, *v))
                            .collect();

                        // 如果目标并行度为空，使用模拟数据
                        let target_parallelism = if target_parallelism.is_empty() {
                            let mut tp = HashMap::new();
                            tp.insert(1, 4); // 假设操作符1的目标并行度为4
                            tp.insert(2, 2); // 假设操作符2的目标并行度为2
                            tp
                        } else {
                            target_parallelism
                        };

                        // 创建扩展计划
                        let plan = ScalingPlan::new(
                            job_id,
                            epoch,
                            &current_parallelism,
                            &target_parallelism,
                        );

                        if plan.has_operations() {
                            info!(
                                "Created scaling plan with {} operations for job {}",
                                plan.operations.len(),
                                plan.job_id
                            );
                            self.scaling_plan = Some(plan);

                            info!("Scaling plan created, moving to Checkpointing phase");
                            self.phase = ScalingPhase::Checkpointing;
                            self.phase_start_time = Instant::now();
                        } else {
                            warn!("No scaling operations needed, skipping rescaling");
                            info!("No scaling operations needed, returning to Running state");
                            return Ok(Transition::next(*self, Running {}));
                        }
                    }
                    ScalingPhase::Checkpointing => {
                        // 创建检查点
                        let job_controller = ctx.job_controller.as_mut().unwrap();

                        if !self.checkpoint_started {
                            match job_controller.checkpoint(false).await {
                                Ok(started) => {
                                    self.checkpoint_started = started;
                                    info!("Started checkpoint for dynamic rescaling");
                                }
                                Err(e) => {
                                    return Err(ctx.retryable(
                                        self,
                                        "failed to initiate checkpoint",
                                        e,
                                        10,
                                    ));
                                }
                            }
                        }

                        match job_controller.checkpoint_finished().await {
                            Ok(done) => {
                                if done && self.checkpoint_started {
                                    info!("Checkpoint completed, moving to StateRedistribution phase");
                                    self.checkpoint_completed = true;
                                    self.phase = ScalingPhase::StateRedistribution;
                                    self.phase_start_time = Instant::now();

                                    // 创建状态重分配计划
                                    let plan = self.scaling_plan.as_ref().unwrap();

                                    for operation in &plan.operations {
                                        match operation {
                                            ScalingOperation::ScaleOut { operator_id, original_parallelism, new_parallelism } |
                                            ScalingOperation::ScaleIn { operator_id, original_parallelism, new_parallelism } => {
                                                // 创建状态重分配计划
                                                let mut redistribution_plan = StateRedistributionPlan::new(
                                                    plan.job_id.clone(),
                                                    plan.epoch,
                                                    operator_id.clone(),
                                                    *original_parallelism,
                                                    *new_parallelism,
                                                );

                                                // 获取操作符的表名列表
                                                // 这里简化处理，实际应该从检查点元数据中获取
                                                let table_names = vec!["default".to_string()];

                                                // 计算状态映射
                                                if let Err(e) = redistribution_plan.compute_state_mapping(&table_names) {
                                                    return Err(ctx.retryable(
                                                        self,
                                                        "failed to compute state mapping",
                                                        e,
                                                        10,
                                                    ));
                                                }

                                                self.redistribution_plans.push(redistribution_plan);
                                            }
                                        }
                                    }

                                    info!(
                                        "Created {} state redistribution plans",
                                        self.redistribution_plans.len()
                                    );
                                }
                            }
                            Err(e) => {
                                return Err(ctx.retryable(
                                    self,
                                    "failed while monitoring checkpoint",
                                    e,
                                    10,
                                ));
                            }
                        }
                    }
                    ScalingPhase::StateRedistribution => {
                        // 执行状态重分配
                        match self.execute_state_redistribution().await {
                            Ok(()) => {
                                info!("State redistribution completed, moving to TaskAdjustment phase");
                                self.phase = ScalingPhase::TaskAdjustment;
                                self.phase_start_time = Instant::now();
                            }
                            Err(e) => {
                                return Err(ctx.retryable(
                                    self,
                                    "failed to execute state redistribution",
                                    e,
                                    10,
                                ));
                            }
                        }
                    }
                    ScalingPhase::TaskAdjustment => {
                        // 实现任务调整逻辑
                        // 1. 获取扩展计划
                        let plan = self.scaling_plan.as_ref().unwrap();

                        // 3. 记录并行度更新
                        for operation in &plan.operations {
                            match operation {
                                ScalingOperation::ScaleOut { operator_id, new_parallelism, original_parallelism, .. } |
                                ScalingOperation::ScaleIn { operator_id, new_parallelism, original_parallelism, .. } => {
                                    info!(
                                        "Would update parallelism for operator {} from {} to {}",
                                        operator_id, original_parallelism, new_parallelism
                                    );
                                }
                            }
                        }

                        // 4. 任务分配调整
                        // 这里简化处理，实际实现中需要:
                        // - 更新作业模型中的并行度
                        // - 计算新的任务分配
                        // - 更新作业模型中的任务分配
                        // - 向工作节点发送更新

                        info!("Task adjustment simulation completed, moving to Completed phase");
                        self.phase = ScalingPhase::Completed;
                        self.phase_start_time = Instant::now();
                    }
                    ScalingPhase::Completed => {
                        // 扩展完成，返回到Running状态
                        info!("Dynamic rescaling completed, transitioning to Scheduling for task redistribution");
                        return Ok(Transition::next(*self, Scheduling {}));
                    }
                }
            }
        }

        // 处理消息
        match ctx.rx.recv().await.expect("channel closed while receiving") {
            JobMessage::RunningMessage(msg) => {
                let job_controller = ctx.job_controller.as_mut().unwrap();
                if let Err(e) = job_controller.handle_message(msg).await {
                    return Err(ctx.retryable(
                        self,
                        "failed while processing message",
                        e,
                        10,
                    ));
                }
            }
            JobMessage::ConfigUpdate(c) => {
                stop_if_desired_non_running!(self, &c);
            }
            _ => {
                // ignore other messages
            }
        }

        Ok(Transition::Advance(StateHolder {
            state: self,
            update_fn: Box::new(|_| {}),
        }))
    }
}
