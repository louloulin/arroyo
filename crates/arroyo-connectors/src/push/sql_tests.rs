#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;
    use arroyo_rpc::ConnectorOptions;
    use datafusion::sql::sqlparser::ast::{Expr, Value};
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::{Parser, ParserError};

    use crate::push::sql;

    #[test]
    fn test_parse_create_table_with_protocol() -> Result<(), ParserError> {
        // Create SQL statement with protocol option
        let sql = r#"
        CREATE TABLE push_test (
            id STRING,
            data STRING,
            timestamp TIMESTAMP
        ) WITH (
            connector = 'push',
            topic = 'test_topic',
            protocol = 'http'
        )
        "#;

        // Parse SQL statement
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;

        // Check that we have one statement
        assert_eq!(statements.len(), 1);

        // Check that it's a CREATE TABLE statement
        match &statements[0] {
            datafusion::sql::sqlparser::ast::Statement::CreateTable(create_table) => {
                // Check table name
                assert_eq!(create_table.name.to_string(), "push_test");

                // Check columns
                assert_eq!(create_table.columns.len(), 3);
                assert_eq!(create_table.columns[0].name.to_string(), "id");
                assert_eq!(create_table.columns[1].name.to_string(), "data");
                assert_eq!(create_table.columns[2].name.to_string(), "timestamp");

                // Check WITH options
                assert!(!create_table.with_options.is_empty());

                // Find connector option
                let connector_option = create_table.with_options.iter().find(|opt| {
                    opt.name.to_string() == "connector"
                });
                assert!(connector_option.is_some());

                // Find protocol option
                let protocol_option = create_table.with_options.iter().find(|opt| {
                    opt.name.to_string() == "protocol"
                });
                assert!(protocol_option.is_some());

                // Check protocol value
                if let Some(protocol_option) = protocol_option {
                    if let datafusion::sql::sqlparser::ast::Value::SingleQuotedString(value) = &protocol_option.value {
                        assert_eq!(value, "http");
                    } else {
                        panic!("Protocol option value is not a string");
                    }
                }
            }
            _ => panic!("Not a CREATE TABLE statement"),
        }

        Ok(())
    }

    #[test]
    fn test_parse_protocol_options() -> Result<(), anyhow::Error> {
        // Create options with HTTP-specific options
        let mut options = ConnectorOptions::new();
        options.insert_str("protocol", "http");
        options.insert_str("topic", "test_topic");
        options.insert_str("http.timeout", "60");
        options.insert_str("http.max_connections", "200");

        // Parse protocol options
        sql::parse_protocol_options(&mut options)?;

        // Check that HTTP config was extracted
        assert!(options.has_key("http_config"));

        // Check HTTP config values
        let http_config_str = options.get_str("http_config").unwrap();
        let http_config: HashMap<String, String> = serde_json::from_str(&http_config_str)?;
        assert_eq!(http_config.get("timeout"), Some(&"60".to_string()));
        assert_eq!(http_config.get("max_connections"), Some(&"200".to_string()));

        // Check that original options were removed
        assert!(!options.has_key("http.timeout"));
        assert!(!options.has_key("http.max_connections"));

        Ok(())
    }

    #[test]
    fn test_parse_multiple_protocol_options() -> Result<(), anyhow::Error> {
        // Create options with multiple protocol options
        let mut options = ConnectorOptions::new();
        options.insert_str("protocol", "quic");
        options.insert_str("topic", "test_topic");
        options.insert_str("quic.max_concurrent_streams", "200");
        options.insert_str("quic.idle_timeout", "60");

        // Parse protocol options
        sql::parse_protocol_options(&mut options)?;

        // Check that QUIC config was extracted
        assert!(options.has_key("quic_config"));

        // Check QUIC config values
        let quic_config_str = options.get_str("quic_config").unwrap();
        let quic_config: HashMap<String, String> = serde_json::from_str(&quic_config_str)?;
        assert_eq!(quic_config.get("max_concurrent_streams"), Some(&"200".to_string()));
        assert_eq!(quic_config.get("idle_timeout"), Some(&"60".to_string()));

        Ok(())
    }

    #[test]
    fn test_validate_protocol_options() -> Result<(), anyhow::Error> {
        // Create valid options
        let mut options = ConnectorOptions::new();
        options.insert_str("protocol", "http");
        options.insert_str("topic", "test_topic");

        // Validate options
        assert!(sql::validate_protocol_options(&options).is_ok());

        // Create invalid options (missing topic)
        let mut invalid_options = ConnectorOptions::new();
        invalid_options.insert_str("protocol", "http");

        // Validate options
        assert!(sql::validate_protocol_options(&invalid_options).is_err());

        // Create invalid options (invalid protocol)
        let mut invalid_protocol = ConnectorOptions::new();
        invalid_protocol.insert_str("protocol", "invalid");
        invalid_protocol.insert_str("topic", "test_topic");

        // Validate options
        assert!(sql::validate_protocol_options(&invalid_protocol).is_err());

        Ok(())
    }

    #[test]
    fn test_parse_create_table_with_multiple_protocols() -> Result<(), ParserError> {
        // Test with different protocols
        let protocols = vec!["http", "quic", "grpc", "websocket"];

        for protocol in protocols {
            let sql = format!(r#"
            CREATE TABLE push_test (
                id STRING,
                data STRING,
                timestamp TIMESTAMP
            ) WITH (
                connector = 'push',
                topic = 'test_topic',
                protocol = '{}'
            )
            "#, protocol);

            // Parse SQL statement
            let dialect = PostgreSqlDialect {};
            let statements = Parser::parse_sql(&dialect, &sql)?;

            // Check protocol value
            match &statements[0] {
                datafusion::sql::sqlparser::ast::Statement::CreateTable(create_table) => {
                    let protocol_option = create_table.with_options.iter().find(|opt| {
                        opt.name.to_string() == "protocol"
                    });

                    if let Some(protocol_option) = protocol_option {
                        if let datafusion::sql::sqlparser::ast::Value::SingleQuotedString(value) = &protocol_option.value {
                            assert_eq!(value, protocol);
                        } else {
                            panic!("Protocol option value is not a string");
                        }
                    } else {
                        panic!("Protocol option not found");
                    }
                }
                _ => panic!("Not a CREATE TABLE statement"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_parse_create_table_with_protocol_specific_options() -> Result<(), ParserError> {
        // Create SQL statement with HTTP-specific options
        let sql = r#"
        CREATE TABLE push_test (
            id STRING,
            data STRING,
            timestamp TIMESTAMP
        ) WITH (
            connector = 'push',
            topic = 'test_topic',
            protocol = 'http',
            http.timeout = '60',
            http.max_connections = '200'
        )
        "#;

        // Parse SQL statement
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;

        // Check HTTP-specific options
        match &statements[0] {
            datafusion::sql::sqlparser::ast::Statement::CreateTable(create_table) => {
                // Find HTTP timeout option
                let timeout_option = create_table.with_options.iter().find(|opt| {
                    opt.name.to_string() == "http.timeout"
                });
                assert!(timeout_option.is_some());

                // Find HTTP max connections option
                let max_connections_option = create_table.with_options.iter().find(|opt| {
                    opt.name.to_string() == "http.max_connections"
                });
                assert!(max_connections_option.is_some());

                // Check option values
                if let Some(timeout_option) = timeout_option {
                    if let datafusion::sql::sqlparser::ast::Value::SingleQuotedString(value) = &timeout_option.value {
                        assert_eq!(value, "60");
                    } else {
                        panic!("HTTP timeout option value is not a string");
                    }
                }

                if let Some(max_connections_option) = max_connections_option {
                    if let datafusion::sql::sqlparser::ast::Value::SingleQuotedString(value) = &max_connections_option.value {
                        assert_eq!(value, "200");
                    } else {
                        panic!("HTTP max connections option value is not a string");
                    }
                }
            }
            _ => panic!("Not a CREATE TABLE statement"),
        }

        Ok(())
    }
}
