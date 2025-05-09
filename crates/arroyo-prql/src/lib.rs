//! PRQL support for Arroyo
//!
//! This crate provides functionality to convert PRQL queries to SQL queries
//! that can be executed by Arroyo's SQL engine.

mod converter;
mod error;
mod parser;

use anyhow::Result;
pub use error::PrqlError;
pub use parser::{ConnectorConfig, WindowConfig, WatermarkConfig};

/// Converts a PRQL query to an Arroyo-compatible SQL query
///
/// # Arguments
///
/// * `prql_query` - The PRQL query to convert
///
/// # Returns
///
/// The converted SQL query
///
/// # Errors
///
/// Returns an error if the PRQL query is invalid or cannot be converted to SQL
pub fn prql_to_sql(prql_query: &str) -> Result<String, PrqlError> {
    // Parse extended syntax
    let (modified_query, connectors, windows, watermarks) = parser::parse_extended_syntax(prql_query)?;

    // Use the prqlc library to compile PRQL to SQL
    let sql = prqlc::compile(
        &modified_query,
        &prqlc::Options {
            format: false,
            target: prqlc::Target::Sql(Some(prqlc::sql::Dialect::Postgres)),
            signature_comment: false,
            ..Default::default()
        },
    )
    .map_err(|e| PrqlError::CompilationError(e.to_string()))?;

    // Apply any Arroyo-specific transformations to the SQL
    let sql = converter::post_process_sql(&sql)?;

    // Apply connector configurations
    let sql = apply_connector_configs(sql, &connectors)?;

    // Apply window configurations
    let sql = apply_window_configs(sql, &windows)?;

    // Apply watermark configurations
    let sql = apply_watermark_configs(sql, &watermarks)?;

    Ok(sql)
}

/// Applies connector configurations to the SQL
///
/// # Arguments
///
/// * `sql` - The SQL to modify
/// * `connectors` - The connector configurations to apply
///
/// # Returns
///
/// The modified SQL
fn apply_connector_configs(sql: String, connectors: &[ConnectorConfig]) -> Result<String, PrqlError> {
    let mut modified_sql = sql;

    for config in connectors {
        if config.is_source {
            // For source connectors, replace table references
            let table_name = config.alias.clone().unwrap_or_else(|| format!("{}_source", config.connector_type));
            let connector_sql = build_connector_sql(config)?;
            modified_sql = modified_sql.replace(&format!("FROM {}", table_name), &format!("FROM {}", connector_sql));
        } else {
            // For sink connectors, add INSERT INTO statement
            let connector_sql = build_connector_sql(config)?;
            modified_sql = format!("INSERT INTO {} {}", connector_sql, modified_sql);
        }
    }

    Ok(modified_sql)
}

/// Builds SQL for a connector configuration
///
/// # Arguments
///
/// * `config` - The connector configuration
///
/// # Returns
///
/// The SQL for the connector
fn build_connector_sql(config: &ConnectorConfig) -> Result<String, PrqlError> {
    match config.connector_type.as_str() {
        "kafka" => {
            let topic = config.options.iter()
                .find(|(name, _)| name == "topic")
                .map(|(_, value)| value.clone())
                .ok_or_else(|| PrqlError::PostProcessingError("Missing 'topic' in Kafka configuration".to_string()))?;

            let bootstrap_servers = config.options.iter()
                .find(|(name, _)| name == "bootstrap.servers")
                .map(|(_, value)| value.clone())
                .ok_or_else(|| PrqlError::PostProcessingError("Missing 'bootstrap.servers' in Kafka configuration".to_string()))?;

            let format = config.options.iter()
                .find(|(name, _)| name == "format")
                .map(|(_, value)| value.clone())
                .unwrap_or_else(|| "json".to_string());

            Ok(format!(
                "KAFKA(
                    topic => '{}',
                    properties => (
                        'bootstrap.servers' => '{}'
                    ),
                    format => {}
                )",
                topic, bootstrap_servers, format
            ))
        },
        "file" => {
            let path = config.options.iter()
                .find(|(name, _)| name == "path")
                .map(|(_, value)| value.clone())
                .ok_or_else(|| PrqlError::PostProcessingError("Missing 'path' in File configuration".to_string()))?;

            let format = config.options.iter()
                .find(|(name, _)| name == "format")
                .map(|(_, value)| value.clone())
                .unwrap_or_else(|| "csv".to_string());

            let write_mode = if !config.is_source {
                config.options.iter()
                    .find(|(name, _)| name == "write_mode")
                    .map(|(_, value)| format!(", write_mode => '{}'", value))
                    .unwrap_or_default()
            } else {
                String::new()
            };

            Ok(format!(
                "FILE(
                    path => '{}',
                    format => {}{}
                )",
                path, format, write_mode
            ))
        },
        _ => Err(PrqlError::PostProcessingError(format!("Unsupported connector type: {}", config.connector_type))),
    }
}

/// Applies window configurations to the SQL
///
/// # Arguments
///
/// * `sql` - The SQL to modify
/// * `windows` - The window configurations to apply
///
/// # Returns
///
/// The modified SQL
fn apply_window_configs(sql: String, _windows: &[WindowConfig]) -> Result<String, PrqlError> {
    // Window configurations are already handled by the parser
    // This function is a placeholder for future enhancements
    Ok(sql)
}

/// Applies watermark configurations to the SQL
///
/// # Arguments
///
/// * `sql` - The SQL to modify
/// * `watermarks` - The watermark configurations to apply
///
/// # Returns
///
/// The modified SQL
fn apply_watermark_configs(sql: String, _watermarks: &[WatermarkConfig]) -> Result<String, PrqlError> {
    // Watermark configurations are already handled by the parser
    // This function is a placeholder for future enhancements
    Ok(sql)
}

/// Determines if a query is a PRQL query
///
/// This is a simple heuristic based on common PRQL syntax patterns.
///
/// # Arguments
///
/// * `query` - The query to check
///
/// # Returns
///
/// `true` if the query appears to be a PRQL query, `false` otherwise
pub fn is_prql_query(query: &str) -> bool {
    let query = query.trim();

    // Check for common PRQL starting keywords
    if query.starts_with("from") || query.starts_with("let") || query.starts_with("prql") {
        return true;
    }

    // Check for pipe operator usage
    if query.contains("|") && !query.contains("SELECT") && !query.contains("FROM") {
        return true;
    }

    // Check for PRQL-style function calls (no parentheses)
    if query.contains("filter ") || query.contains("derive ") || query.contains("group ") {
        return true;
    }

    false
}

#[cfg(test)]
mod test_basic;
#[cfg(test)]
mod test_extended;
