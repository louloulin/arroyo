#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_dml::{try_parse_queue_dml, QueueDmlStatement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test_parse_produce() {
        let sql = r#"
        WITH PRODUCE("my_topic", columns=["id", "name", "value"], key="id") AS (
            VALUES
                (1, 'item1', 100),
                (2, 'item2', 200),
                (3, 'item3', 300)
        )
        SELECT * FROM VALUES;
        "#;
        
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_queue_dml(&statements[0]).unwrap();
        assert!(result.is_some());
        
        if let Some(QueueDmlStatement::Produce { topic, columns, key_column_index, values, query }) = result {
            assert_eq!(topic, "my_topic");
            assert_eq!(columns, vec!["id", "name", "value"]);
            assert_eq!(key_column_index, Some(0)); // "id" is at index 0
            assert!(values.is_some());
            assert!(query.is_none());
            
            let values = values.unwrap();
            assert_eq!(values.len(), 3);
        } else {
            panic!("Expected Produce statement");
        }
    }
    
    #[test]
    fn test_parse_produce_without_key() {
        let sql = r#"
        WITH PRODUCE("my_topic", columns=["id", "name", "value"]) AS (
            VALUES
                (1, 'item1', 100),
                (2, 'item2', 200)
        )
        SELECT * FROM VALUES;
        "#;
        
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_queue_dml(&statements[0]).unwrap();
        assert!(result.is_some());
        
        if let Some(QueueDmlStatement::Produce { topic, columns, key_column_index, values, query }) = result {
            assert_eq!(topic, "my_topic");
            assert_eq!(columns, vec!["id", "name", "value"]);
            assert_eq!(key_column_index, None); // No key specified
            assert!(values.is_some());
            assert!(query.is_none());
            
            let values = values.unwrap();
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected Produce statement");
        }
    }
    
    #[test]
    fn test_parse_produce_with_query() {
        let sql = r#"
        WITH PRODUCE("my_topic", columns=["id", "name", "value"], key="id") AS (
            SELECT id, name, value FROM my_table WHERE value > 100
        )
        SELECT * FROM VALUES;
        "#;
        
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_queue_dml(&statements[0]).unwrap();
        assert!(result.is_some());
        
        if let Some(QueueDmlStatement::Produce { topic, columns, key_column_index, values, query }) = result {
            assert_eq!(topic, "my_topic");
            assert_eq!(columns, vec!["id", "name", "value"]);
            assert_eq!(key_column_index, Some(0)); // "id" is at index 0
            assert!(values.is_none());
            assert!(query.is_some());
        } else {
            panic!("Expected Produce statement");
        }
    }
    
    #[test]
    fn test_parse_consume() {
        let sql = r#"
        WITH CONSUME("my_topic", group_id="my_group") AS (
            SELECT * FROM table
        )
        WHERE value > 100
        LIMIT 10;
        "#;
        
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_queue_dml(&statements[0]).unwrap();
        assert!(result.is_some());
        
        if let Some(QueueDmlStatement::Consume { topic, group_id, filter, limit }) = result {
            assert_eq!(topic, "my_topic");
            assert_eq!(group_id, Some("my_group".to_string()));
            assert!(filter.is_some());
            assert_eq!(limit, Some(10));
        } else {
            panic!("Expected Consume statement");
        }
    }
    
    #[test]
    fn test_parse_consume_without_options() {
        let sql = r#"
        WITH CONSUME("my_topic") AS (
            SELECT * FROM table
        );
        "#;
        
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_queue_dml(&statements[0]).unwrap();
        assert!(result.is_some());
        
        if let Some(QueueDmlStatement::Consume { topic, group_id, filter, limit }) = result {
            assert_eq!(topic, "my_topic");
            assert_eq!(group_id, None);
            assert_eq!(filter, None);
            assert_eq!(limit, None);
        } else {
            panic!("Expected Consume statement");
        }
    }
    
    #[test]
    fn test_parse_non_queue_dml() {
        let sql = "SELECT * FROM my_table";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_queue_dml(&statements[0]).unwrap();
        assert!(result.is_none());
    }
}
