use datafusion::common::{Result, tree_node::{Transformed, TreeNodeRewriter}};
use datafusion::logical_expr::{LogicalPlan, Projection, TableScan, JoinType, Join, Expr, Column, utils::expr_to_columns};
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tracing::{debug, info};

use super::Optimizer;

/// 投影下推优化器
/// 
/// 将投影操作尽可能下推到数据源，减少数据传输和处理量
pub struct ProjectionPushdown {}

impl ProjectionPushdown {
    /// 创建新的投影下推优化器
    pub fn new() -> Self {
        Self {}
    }
    
    /// 尝试将投影下推到表扫描节点
    fn push_down_to_table_scan(&self, projection: &Projection, table_scan: &TableScan) -> Result<Option<LogicalPlan>> {
        // 检查投影是否可以下推到表扫描
        // 这里需要考虑表扫描的能力和投影的复杂性
        
        // 获取投影中使用的列
        let mut required_columns = HashSet::new();
        for expr in &projection.expr {
            let columns = expr_to_columns(expr);
            for col in columns {
                required_columns.insert(col.name.clone());
            }
        }
        
        // 检查表扫描是否已经有投影
        if let Some(existing_projection) = &table_scan.projection {
            // 获取表的所有列
            let table_columns: Vec<_> = (0..table_scan.table_schema.fields().len()).collect();
            
            // 创建新的投影，只包含需要的列
            let mut new_projection = Vec::new();
            let mut column_mapping = HashMap::new();
            
            for (i, &col_idx) in existing_projection.iter().enumerate() {
                let col_name = table_scan.table_schema.field(col_idx).name();
                if required_columns.contains(col_name) {
                    new_projection.push(col_idx);
                    column_mapping.insert(col_name.clone(), i);
                }
            }
            
            // 创建新的表扫描节点，包含下推的投影
            let new_table_scan = TableScan {
                table_name: table_scan.table_name.clone(),
                source: table_scan.source.clone(),
                projection: Some(new_projection),
                projected_schema: table_scan.projected_schema.clone(),
                filters: table_scan.filters.clone(),
                fetch: table_scan.fetch,
                table_schema: table_scan.table_schema.clone(),
            };
            
            // 创建新的投影表达式，使用新的列索引
            let new_exprs = projection.expr.iter().map(|expr| {
                self.rewrite_expr(expr, &column_mapping)
            }).collect::<Result<Vec<_>>>()?;
            
            // 创建新的投影节点
            return Ok(Some(LogicalPlan::Projection(Projection::try_new(
                new_exprs,
                Arc::new(LogicalPlan::TableScan(new_table_scan)),
            )?)));
        }
        
        // 如果表扫描没有投影，创建新的投影
        let table_columns: Vec<_> = table_scan.table_schema.fields().iter().enumerate()
            .filter_map(|(i, field)| {
                if required_columns.contains(field.name()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();
        
        // 创建新的表扫描节点，包含下推的投影
        let new_table_scan = TableScan {
            table_name: table_scan.table_name.clone(),
            source: table_scan.source.clone(),
            projection: Some(table_columns),
            projected_schema: table_scan.projected_schema.clone(),
            filters: table_scan.filters.clone(),
            fetch: table_scan.fetch,
            table_schema: table_scan.table_schema.clone(),
        };
        
        Ok(Some(LogicalPlan::Projection(Projection::try_new(
            projection.expr.clone(),
            Arc::new(LogicalPlan::TableScan(new_table_scan)),
        )?)))
    }
    
    /// 重写表达式，使用新的列索引
    fn rewrite_expr(&self, expr: &Expr, column_mapping: &HashMap<String, usize>) -> Result<Expr> {
        match expr {
            Expr::Column(Column { relation, name }) => {
                if let Some(&idx) = column_mapping.get(name) {
                    Ok(Expr::Column(Column {
                        relation: relation.clone(),
                        name: name.clone(),
                    }))
                } else {
                    Ok(expr.clone())
                }
            }
            // 处理其他类型的表达式
            _ => Ok(expr.clone()),
        }
    }
    
    /// 尝试将投影下推到连接节点
    fn push_down_to_join(&self, projection: &Projection, join: &Join) -> Result<Option<LogicalPlan>> {
        // 获取投影中使用的列
        let mut required_columns = HashSet::new();
        for expr in &projection.expr {
            let columns = expr_to_columns(expr);
            for col in columns {
                required_columns.insert(col.name.clone());
            }
        }
        
        // 获取连接条件中使用的列
        let mut join_columns = HashSet::new();
        for (left_col, right_col) in &join.on {
            join_columns.insert(left_col.name.clone());
            join_columns.insert(right_col.name.clone());
        }
        
        // 合并所有需要的列
        required_columns.extend(join_columns);
        
        // 获取左侧和右侧的列
        let left_columns: HashSet<_> = join.left.schema().field_names().into_iter().collect();
        let right_columns: HashSet<_> = join.right.schema().field_names().into_iter().collect();
        
        // 确定左侧和右侧需要的列
        let left_required: HashSet<_> = required_columns.iter()
            .filter(|col| left_columns.contains(*col))
            .cloned()
            .collect();
        
        let right_required: HashSet<_> = required_columns.iter()
            .filter(|col| right_columns.contains(*col))
            .cloned()
            .collect();
        
        // 创建左侧投影
        let new_left = if left_required.len() < left_columns.len() {
            let left_exprs = left_required.iter()
                .map(|col| Expr::Column(Column::new_unqualified(col)))
                .collect::<Vec<_>>();
            
            LogicalPlan::Projection(Projection::try_new(
                left_exprs,
                join.left.clone(),
            )?)
        } else {
            (*join.left).clone()
        };
        
        // 创建右侧投影
        let new_right = if right_required.len() < right_columns.len() {
            let right_exprs = right_required.iter()
                .map(|col| Expr::Column(Column::new_unqualified(col)))
                .collect::<Vec<_>>();
            
            LogicalPlan::Projection(Projection::try_new(
                right_exprs,
                join.right.clone(),
            )?)
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
        
        // 创建新的投影节点
        Ok(Some(LogicalPlan::Projection(Projection::try_new(
            projection.expr.clone(),
            Arc::new(new_join),
        )?)))
    }
}

impl TreeNodeRewriter for ProjectionPushdown {
    type Node = LogicalPlan;
    
    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        match &node {
            LogicalPlan::Projection(projection) => {
                match projection.input.as_ref() {
                    LogicalPlan::TableScan(table_scan) => {
                        // 尝试将投影下推到表扫描
                        if let Some(new_plan) = self.push_down_to_table_scan(projection, table_scan)? {
                            return Ok(Transformed::yes(new_plan));
                        }
                    }
                    LogicalPlan::Join(join) => {
                        // 尝试将投影下推到连接
                        if let Some(new_plan) = self.push_down_to_join(projection, join)? {
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

impl Optimizer for ProjectionPushdown {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // 创建可变的优化器实例
        let mut optimizer = Self::new();
        
        // 应用优化
        let result = plan.rewrite(&mut optimizer)?;
        
        Ok(result.data)
    }
}
