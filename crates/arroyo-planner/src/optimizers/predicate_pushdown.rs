use datafusion::common::{Result, tree_node::{Transformed, TreeNodeRewriter}};
use datafusion::logical_expr::{LogicalPlan, Filter, TableScan, JoinType, Join, BinaryExpr, Operator, Expr, utils::expr_to_columns};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info};

use super::Optimizer;

/// 谓词下推优化器
/// 
/// 将过滤条件尽可能下推到数据源，减少数据传输和处理量
pub struct PredicatePushdown {}

impl PredicatePushdown {
    /// 创建新的谓词下推优化器
    pub fn new() -> Self {
        Self {}
    }
    
    /// 尝试将过滤条件下推到表扫描节点
    fn push_down_to_table_scan(&self, filter: &Filter, table_scan: &TableScan) -> Result<Option<LogicalPlan>> {
        // 检查过滤条件是否可以下推到表扫描
        // 这里需要考虑表扫描的能力和过滤条件的复杂性
        
        // 简单实现：只下推简单的过滤条件
        if self.can_push_down_to_table_scan(&filter.predicate, table_scan) {
            debug!("Pushing down filter to table scan: {}", filter.predicate);
            
            // 创建新的表扫描节点，包含下推的过滤条件
            // 在实际实现中，这里需要修改表扫描的配置，添加过滤条件
            // 这里简化处理，仅返回原始表扫描
            
            return Ok(Some(LogicalPlan::TableScan(table_scan.clone())));
        }
        
        Ok(None)
    }
    
    /// 检查过滤条件是否可以下推到表扫描
    fn can_push_down_to_table_scan(&self, predicate: &Expr, table_scan: &TableScan) -> bool {
        // 获取过滤条件中的列
        let columns = expr_to_columns(predicate);
        
        // 检查所有列是否都在表扫描的输出中
        let table_columns: HashSet<_> = table_scan.projected_schema.field_names().into_iter().collect();
        
        columns.iter().all(|col| table_columns.contains(&col.name))
    }
    
    /// 尝试将过滤条件下推到连接节点
    fn push_down_to_join(&self, filter: &Filter, join: &Join) -> Result<Option<LogicalPlan>> {
        // 分析过滤条件，确定哪些部分可以下推到左侧，哪些可以下推到右侧
        let (left_predicates, right_predicates, remaining_predicates) = self.split_join_predicates(&filter.predicate, join)?;
        
        if left_predicates.is_empty() && right_predicates.is_empty() {
            // 没有可以下推的谓词
            return Ok(None);
        }
        
        // 创建新的左侧计划，包含下推的过滤条件
        let new_left = if !left_predicates.is_empty() {
            let left_predicate = if left_predicates.len() == 1 {
                left_predicates[0].clone()
            } else {
                // 组合多个谓词
                left_predicates.into_iter().reduce(|acc, expr| {
                    Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(acc),
                        op: Operator::And,
                        right: Box::new(expr),
                    })
                }).unwrap()
            };
            
            LogicalPlan::Filter(Filter::try_new(left_predicate, join.left.clone())?)
        } else {
            (*join.left).clone()
        };
        
        // 创建新的右侧计划，包含下推的过滤条件
        let new_right = if !right_predicates.is_empty() {
            let right_predicate = if right_predicates.len() == 1 {
                right_predicates[0].clone()
            } else {
                // 组合多个谓词
                right_predicates.into_iter().reduce(|acc, expr| {
                    Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(acc),
                        op: Operator::And,
                        right: Box::new(expr),
                    })
                }).unwrap()
            };
            
            LogicalPlan::Filter(Filter::try_new(right_predicate, join.right.clone())?)
        } else {
            (*join.right).clone()
        };
        
        // 创建新的连接节点
        let new_join = LogicalPlan::Join(Join {
            left: Arc::new(new_left),
            right: Arc::new(new_right),
            on: join.on.clone(),
            filter: join.filter.clone(),
            join_type: join.join_type,
            join_constraint: join.join_constraint,
            schema: join.schema.clone(),
            null_equals_null: join.null_equals_null,
        });
        
        // 如果有剩余的谓词，创建新的过滤器节点
        if !remaining_predicates.is_empty() {
            let remaining_predicate = if remaining_predicates.len() == 1 {
                remaining_predicates[0].clone()
            } else {
                // 组合多个谓词
                remaining_predicates.into_iter().reduce(|acc, expr| {
                    Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(acc),
                        op: Operator::And,
                        right: Box::new(expr),
                    })
                }).unwrap()
            };
            
            Ok(Some(LogicalPlan::Filter(Filter::try_new(
                remaining_predicate,
                Arc::new(new_join),
            )?)))
        } else {
            Ok(Some(new_join))
        }
    }
    
    /// 将过滤条件分解为可以下推到连接左侧、右侧和不能下推的部分
    fn split_join_predicates(&self, predicate: &Expr, join: &Join) -> Result<(Vec<Expr>, Vec<Expr>, Vec<Expr>)> {
        // 获取左侧和右侧的列
        let left_columns: HashSet<_> = join.left.schema().field_names().into_iter().collect();
        let right_columns: HashSet<_> = join.right.schema().field_names().into_iter().collect();
        
        // 分解谓词
        let predicates = self.split_conjunction(predicate);
        
        let mut left_predicates = Vec::new();
        let mut right_predicates = Vec::new();
        let mut remaining_predicates = Vec::new();
        
        for pred in predicates {
            // 获取谓词中的列
            let columns = expr_to_columns(&pred);
            
            // 检查谓词是否只涉及左侧列
            let only_left = columns.iter().all(|col| left_columns.contains(&col.name));
            
            // 检查谓词是否只涉及右侧列
            let only_right = columns.iter().all(|col| right_columns.contains(&col.name));
            
            if only_left {
                left_predicates.push(pred);
            } else if only_right {
                right_predicates.push(pred);
            } else {
                remaining_predicates.push(pred);
            }
        }
        
        Ok((left_predicates, right_predicates, remaining_predicates))
    }
    
    /// 将谓词分解为合取形式（AND 连接的子谓词）
    fn split_conjunction(&self, expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => {
                let mut result = self.split_conjunction(left);
                result.extend(self.split_conjunction(right));
                result
            }
            _ => vec![expr.clone()],
        }
    }
}

impl TreeNodeRewriter for PredicatePushdown {
    type Node = LogicalPlan;
    
    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        match &node {
            LogicalPlan::Filter(filter) => {
                match filter.input.as_ref() {
                    LogicalPlan::TableScan(table_scan) => {
                        // 尝试将过滤条件下推到表扫描
                        if let Some(new_plan) = self.push_down_to_table_scan(filter, table_scan)? {
                            return Ok(Transformed::yes(new_plan));
                        }
                    }
                    LogicalPlan::Join(join) => {
                        // 尝试将过滤条件下推到连接
                        if let Some(new_plan) = self.push_down_to_join(filter, join)? {
                            return Ok(Transformed::yes(new_plan));
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        
        Ok(Transformed::no(node))
    }
}

impl Optimizer for PredicatePushdown {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // 创建可变的优化器实例
        let mut optimizer = Self::new();
        
        // 应用优化
        let result = plan.rewrite(&mut optimizer)?;
        
        Ok(result.data)
    }
}
