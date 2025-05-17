#[cfg(test)]
mod tests {
    use anyhow::Result;
    use arroyo_rpc::ConnectorOptions;
    use datafusion::sql::sqlparser::ast::{Expr, Ident, SqlOption, Value};

    use crate::push::sql;

    #[test]
    fn test_parse_protocol_options() -> Result<()> {
        // Create options with HTTP-specific options
        let sql_options = vec![
            SqlOption::KeyValue {
                key: Ident {
                    value: "protocol".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("http".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "topic".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("test_topic".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "http.timeout".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("60".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "http.max_connections".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("200".to_string())),
            },
        ];

        let mut options = ConnectorOptions::try_from(&sql_options).unwrap();

        // Parse protocol options
        sql::parse_protocol_options(&mut options)?;

        // Check that HTTP config was extracted
        assert!(options.contains_key("http_config"));

        // Check that original options were removed
        assert!(!options.contains_key("http.timeout"));
        assert!(!options.contains_key("http.max_connections"));

        Ok(())
    }

    #[test]
    fn test_parse_multiple_protocol_options() -> Result<()> {
        // Create options with multiple protocol options
        let sql_options = vec![
            SqlOption::KeyValue {
                key: Ident {
                    value: "protocol".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("quic".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "topic".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("test_topic".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "quic.max_concurrent_streams".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("200".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "quic.idle_timeout".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("60".to_string())),
            },
        ];

        let mut options = ConnectorOptions::try_from(&sql_options).unwrap();

        // Parse protocol options
        sql::parse_protocol_options(&mut options)?;

        // Check that QUIC config was extracted
        assert!(options.contains_key("quic_config"));

        Ok(())
    }

    #[test]
    fn test_validate_protocol_options() -> Result<()> {
        // Create valid options
        let sql_options = vec![
            SqlOption::KeyValue {
                key: Ident {
                    value: "protocol".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("http".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "topic".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("test_topic".to_string())),
            },
        ];

        let mut options = ConnectorOptions::try_from(&sql_options).unwrap();

        // Validate options
        assert!(sql::validate_protocol_options(&mut options).is_ok());

        // Check that topic is still present after validation
        assert!(options.contains_key("topic"), "Topic should still be present after validation");

        // Check that protocol is still present after validation
        assert!(options.contains_key("protocol"), "Protocol should still be present after validation");

        // Create invalid options (missing topic)
        let invalid_sql_options = vec![
            SqlOption::KeyValue {
                key: Ident {
                    value: "protocol".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("http".to_string())),
            },
        ];

        let mut invalid_options = ConnectorOptions::try_from(&invalid_sql_options).unwrap();

        // Validate options
        let err = sql::validate_protocol_options(&mut invalid_options);
        assert!(err.is_err());
        let err_msg = err.unwrap_err().to_string();
        assert!(err_msg.contains("Missing required option: topic"),
                "Error message should mention missing topic: {}", err_msg);
        assert!(err_msg.contains("WITH (topic = "),
                "Error message should provide usage hint: {}", err_msg);

        // Create invalid options (invalid protocol)
        let invalid_protocol_sql_options = vec![
            SqlOption::KeyValue {
                key: Ident {
                    value: "protocol".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("invalid".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "topic".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("test_topic".to_string())),
            },
        ];

        let mut invalid_protocol = ConnectorOptions::try_from(&invalid_protocol_sql_options).unwrap();

        // Validate options
        let err = sql::validate_protocol_options(&mut invalid_protocol);
        assert!(err.is_err());
        let err_msg = err.unwrap_err().to_string();
        assert!(err_msg.contains("Unsupported protocol"),
                "Error message should mention unsupported protocol: {}", err_msg);
        assert!(err_msg.contains("Supported protocols are"),
                "Error message should list supported protocols: {}", err_msg);

        Ok(())
    }

    #[test]
    fn test_topic_preserved_after_validation() -> Result<()> {
        // Create options with topic and other parameters
        let sql_options = vec![
            SqlOption::KeyValue {
                key: Ident {
                    value: "protocol".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("http".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "topic".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("test_topic".to_string())),
            },
            SqlOption::KeyValue {
                key: Ident {
                    value: "http.timeout".to_string(),
                    quote_style: None,
                },
                value: Expr::Value(Value::SingleQuotedString("30".to_string())),
            },
        ];

        let mut options = ConnectorOptions::try_from(&sql_options).unwrap();

        // Parse protocol options
        sql::parse_protocol_options(&mut options)?;

        // Validate options
        sql::validate_protocol_options(&mut options)?;

        // Check that topic is still present after both operations
        assert!(options.contains_key("topic"), "Topic should still be present after validation");

        // Now try to pull the topic and verify it works
        let topic = options.pull_opt_str("topic")?;
        assert_eq!(topic, Some("test_topic".to_string()), "Topic value should be preserved");

        Ok(())
    }
}
