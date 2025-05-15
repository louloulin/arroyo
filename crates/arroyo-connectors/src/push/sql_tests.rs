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
        assert!(sql::validate_protocol_options(&mut invalid_options).is_err());

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
        assert!(sql::validate_protocol_options(&mut invalid_protocol).is_err());

        Ok(())
    }
}
