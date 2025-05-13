use crate::ArroyoSchemaProvider;
use datafusion::common::{Result, tree_node::{Transformed, TreeNodeRewriter}};
use datafusion::logical_expr::{LogicalPlan, Projection, Filter, TableScan, JoinType, Join};
use std::sync::Arc;

pub mod predicate_pushdown;
pub mod projection_pushdown;
pub mod join_optimizer;
pub mod join_rewriter;

#[cfg(test)]
mod tests;

/// 优化器特征，定义了优化器的基本行为
pub trait Optimizer {
    /// 优化逻辑计划
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan>;
}

/// 组合优化器，按顺序应用多个优化器
pub struct CombinedOptimizer {
    optimizers: Vec<Box<dyn Optimizer>>,
}

impl CombinedOptimizer {
    /// 创建新的组合优化器
    pub fn new() -> Self {
        let mut optimizers: Vec<Box<dyn Optimizer>> = Vec::new();

        // 添加 JOIN 重写器
        optimizers.push(Box::new(join_rewriter::JoinRewriter::new()));

        // 添加 JOIN 优化器
        optimizers.push(Box::new(join_optimizer::JoinOptimizer::new()));

        // 添加谓词下推优化器
        optimizers.push(Box::new(predicate_pushdown::PredicatePushdown::new()));

        // 添加投影下推优化器
        optimizers.push(Box::new(projection_pushdown::ProjectionPushdown::new()));

        Self { optimizers }
    }
}

impl Optimizer for CombinedOptimizer {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut optimized_plan = plan;

        // 按顺序应用所有优化器
        for optimizer in &self.optimizers {
            optimized_plan = optimizer.optimize(optimized_plan)?;
        }

        Ok(optimized_plan)
    }
}

/// 优化查询计划
///
/// 应用一系列优化规则，包括谓词下推和投影下推
pub fn optimize_plan(plan: LogicalPlan, schema_provider: &ArroyoSchemaProvider) -> Result<LogicalPlan> {
    // 首先应用 Arroyo 特定的重写规则
    let rewritten_plan = crate::rewrite_plan(plan, schema_provider)?;

    // 然后应用通用优化规则
    let optimizer = CombinedOptimizer::new();
    let optimized_plan = optimizer.optimize(rewritten_plan)?;

    Ok(optimized_plan)
}

/// 检查计划是否包含特定类型的节点
pub fn plan_contains_node_type(plan: &LogicalPlan, node_type: &str) -> bool {
    match plan {
        LogicalPlan::TableScan(_) if node_type == "TableScan" => true,
        LogicalPlan::Filter(_) if node_type == "Filter" => true,
        LogicalPlan::Projection(_) if node_type == "Projection" => true,
        LogicalPlan::Aggregate(_) if node_type == "Aggregate" => true,
        LogicalPlan::Join(_) if node_type == "Join" => true,
        LogicalPlan::Sort(_) if node_type == "Sort" => true,
        LogicalPlan::Limit(_) if node_type == "Limit" => true,
        LogicalPlan::Extension(_) if node_type == "Extension" => true,
        _ => {
            // 递归检查子节点
            plan.inputs().iter().any(|input| plan_contains_node_type(input, node_type))
        }
    }
}

/// 获取计划中的表扫描节点
pub fn get_table_scans(plan: &LogicalPlan) -> Vec<&TableScan> {
    let mut table_scans = Vec::new();

    match plan {
        LogicalPlan::TableScan(scan) => {
            table_scans.push(scan);
        }
        _ => {
            // 递归检查子节点
            for input in plan.inputs() {
                table_scans.extend(get_table_scans(input));
            }
        }
    }

    table_scans
}

/// 获取计划中的过滤器节点
pub fn get_filters(plan: &LogicalPlan) -> Vec<&Filter> {
    let mut filters = Vec::new();

    match plan {
        LogicalPlan::Filter(filter) => {
            filters.push(filter);
        }
        _ => {
            // 递归检查子节点
            for input in plan.inputs() {
                filters.extend(get_filters(input));
            }
        }
    }

    filters
}

/// 获取计划中的投影节点
pub fn get_projections(plan: &LogicalPlan) -> Vec<&Projection> {
    let mut projections = Vec::new();

    match plan {
        LogicalPlan::Projection(projection) => {
            projections.push(projection);
        }
        _ => {
            // 递归检查子节点
            for input in plan.inputs() {
                projections.extend(get_projections(input));
            }
        }
    }

    projections
}
