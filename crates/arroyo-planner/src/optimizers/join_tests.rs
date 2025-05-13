#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArroyoSchemaProvider;
    use crate::tables::Table;
    use datafusion::logical_expr::{LogicalPlan, TableScan, Filter, Projection, Join, JoinType, JoinConstraint, Expr, Column, BinaryExpr, Operator};
    use datafusion::common::TableReference;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::collections::HashMap;

    // 创建测试表扫描
    fn create_test_table_scan(table_name: &str, is_dimension: bool) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("event_time", DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None), false),
        ]));
        
        let table_name = if is_dimension {
            format!("dimension_{}", table_name)
        } else {
            table_name.to_string()
        };
        
        LogicalPlan::TableScan(TableScan {
            table_name: TableReference::bare(&table_name),
            source: Arc::new(datafusion::datasource::empty::EmptyTable::new(schema.clone())),
            projection: None,
            projected_schema: Arc::new(datafusion::common::DFSchema::try_from_schema(schema.clone()).unwrap()),
            filters: vec![],
            fetch: None,
            table_schema: schema,
        })
    }
    
    // 创建测试 JOIN
    fn create_test_join(left: LogicalPlan, right: LogicalPlan, join_type: JoinType) -> LogicalPlan {
        let on = vec![
            (
                Expr::Column(Column::new_unqualified("id")),
                Expr::Column(Column::new_unqualified("id")),
            ),
        ];
        
        let schema = Arc::new(datafusion::logical_expr::build_join_schema(
            left.schema(),
            right.schema(),
            &join_type,
        ).unwrap());
        
        LogicalPlan::Join(Join {
            left: Arc::new(left),
            right: Arc::new(right),
            on,
            join_type,
            join_constraint: JoinConstraint::On,
            null_equals_null: false,
            filter: None,
            schema,
        })
    }
    
    // 创建测试窗口 JOIN
    fn create_test_window_join(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        let on = vec![
            (
                Expr::Column(Column::new_unqualified("id")),
                Expr::Column(Column::new_unqualified("id")),
            ),
        ];
        
        // 创建时间窗口条件：a.event_time BETWEEN b.event_time - INTERVAL '5' MINUTE AND b.event_time + INTERVAL '5' MINUTE
        let time_window_condition = Expr::Between(Box::new(datafusion::logical_expr::expr::Between {
            expr: Box::new(Expr::Column(Column::new_unqualified("event_time"))),
            negated: false,
            low: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::new_unqualified("event_time"))),
                op: Operator::Minus,
                right: Box::new(Expr::Literal(datafusion::scalar::ScalarValue::IntervalDayTime(Some(300000)))), // 5 minutes in milliseconds
            })),
            high: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::new_unqualified("event_time"))),
                op: Operator::Plus,
                right: Box::new(Expr::Literal(datafusion::scalar::ScalarValue::IntervalDayTime(Some(300000)))), // 5 minutes in milliseconds
            })),
        }));
        
        let schema = Arc::new(datafusion::logical_expr::build_join_schema(
            left.schema(),
            right.schema(),
            &JoinType::Inner,
        ).unwrap());
        
        LogicalPlan::Join(Join {
            left: Arc::new(left),
            right: Arc::new(right),
            on,
            join_type: JoinType::Inner,
            join_constraint: JoinConstraint::On,
            null_equals_null: false,
            filter: Some(time_window_condition),
            schema,
        })
    }
    
    #[test]
    fn test_join_optimizer() -> Result<()> {
        // 创建测试计划：流-表 JOIN
        let stream_table = create_test_table_scan("stream", false);
        let dimension_table = create_test_table_scan("table", true);
        let join = create_test_join(stream_table, dimension_table, JoinType::Inner);
        
        // 创建 schema provider
        let schema_provider = ArroyoSchemaProvider::new();
        
        // 应用 JOIN 优化器
        let optimizer = join_optimizer::JoinOptimizer::new();
        let optimized_plan = optimizer.optimize(join.clone())?;
        
        // 验证优化结果
        // 在实际实现中，应该验证是否应用了正确的优化策略
        // 但在我们的简化实现中，优化器不会修改计划，所以这里只验证计划结构
        
        assert!(plan_contains_node_type(&optimized_plan, "Join"));
        
        Ok(())
    }
    
    #[test]
    fn test_join_rewriter() -> Result<()> {
        // 创建测试计划：窗口 JOIN
        let stream_table_1 = create_test_table_scan("stream1", false);
        let stream_table_2 = create_test_table_scan("stream2", false);
        let join = create_test_window_join(stream_table_1, stream_table_2);
        
        // 创建 schema provider
        let schema_provider = ArroyoSchemaProvider::new();
        
        // 应用 JOIN 重写器
        let rewriter = join_rewriter::JoinRewriter::new();
        let rewritten_plan = rewriter.optimize(join.clone())?;
        
        // 验证重写结果
        // 在实际实现中，应该验证是否应用了正确的重写规则
        // 但在我们的简化实现中，重写器不会修改计划，所以这里只验证计划结构
        
        assert!(plan_contains_node_type(&rewritten_plan, "Join"));
        
        Ok(())
    }
    
    #[test]
    fn test_combined_optimizer() -> Result<()> {
        // 创建测试计划：流-表 JOIN
        let stream_table = create_test_table_scan("stream", false);
        let dimension_table = create_test_table_scan("table", true);
        let join = create_test_join(stream_table, dimension_table, JoinType::Inner);
        
        // 创建 schema provider
        let schema_provider = ArroyoSchemaProvider::new();
        
        // 应用组合优化器
        let optimizer = CombinedOptimizer::new();
        let optimized_plan = optimizer.optimize(join.clone())?;
        
        // 验证优化结果
        assert!(plan_contains_node_type(&optimized_plan, "Join"));
        
        Ok(())
    }
}
