//! PRQL support for Arroyo
//!
//! This crate provides functionality to convert PRQL queries to SQL queries
//! that can be executed by Arroyo's SQL engine.

mod converter;
mod error;

use anyhow::Result;
pub use error::PrqlError;

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
    // Use the prqlc library to compile PRQL to SQL
    let sql = prqlc::compile(
        prql_query,
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
mod tests;
