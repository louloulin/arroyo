#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_ddl::{try_parse_topic_ddl, TopicDdlStatement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test_parse_create_topic() {
        let sql = "CREATE TOPIC IF NOT EXISTS my_topic WITH (partitions = 3, replication_factor = 2, retention_ms = 86400000, cleanup_policy = 'delete')";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_topic_ddl(&statements[0]).unwrap();
        assert!(result.is_some());
        
        if let Some(TopicDdlStatement::CreateTopic { name, if_not_exists, config }) = result {
            assert_eq!(name, "my_topic");
            assert!(if_not_exists);
            assert_eq!(config.partitions, 3);
            assert_eq!(config.replication_factor, 2);
            assert_eq!(config.retention_ms, Some(86400000));
            assert_eq!(config.cleanup_policy, "delete");
        } else {
            panic!("Expected CreateTopic statement");
        }
    }
    
    #[test]
    fn test_parse_alter_topic() {
        let sql = "ALTER TOPIC my_topic SET retention_ms = 172800000, cleanup_policy = 'compact'";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_topic_ddl(&statements[0]).unwrap();
        assert!(result.is_some());
        
        if let Some(TopicDdlStatement::AlterTopic { name, config }) = result {
            assert_eq!(name, "my_topic");
            assert_eq!(config.retention_ms, Some(172800000));
            assert_eq!(config.cleanup_policy, "compact");
        } else {
            panic!("Expected AlterTopic statement");
        }
    }
    
    #[test]
    fn test_parse_drop_topic() {
        let sql = "DROP TOPIC IF EXISTS my_topic";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_topic_ddl(&statements[0]).unwrap();
        assert!(result.is_some());
        
        if let Some(TopicDdlStatement::DropTopic { name, if_exists }) = result {
            assert_eq!(name, "my_topic");
            assert!(if_exists);
        } else {
            panic!("Expected DropTopic statement");
        }
    }
    
    #[test]
    fn test_parse_show_topics() {
        let sql = "SHOW TOPICS";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_topic_ddl(&statements[0]).unwrap();
        assert!(result.is_some());
        
        if let Some(TopicDdlStatement::ShowTopics) = result {
            // 成功
        } else {
            panic!("Expected ShowTopics statement");
        }
    }
    
    #[test]
    fn test_parse_non_topic_ddl() {
        let sql = "SELECT * FROM my_table";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        
        assert_eq!(statements.len(), 1);
        
        let result = try_parse_topic_ddl(&statements[0]).unwrap();
        assert!(result.is_none());
    }
}
