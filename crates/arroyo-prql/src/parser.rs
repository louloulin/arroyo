//! Parser module for extended PRQL syntax
//!
//! This module provides functionality to parse extended PRQL syntax
//! for Arroyo-specific features like connectors, window functions, and watermarks.

use crate::error::PrqlError;
use anyhow::Result;
use regex::Regex;

/// Represents a connector configuration
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    /// The type of connector (e.g., "kafka", "file")
    pub connector_type: String,
    /// The options for the connector
    pub options: Vec<(String, String)>,
    /// The alias for the connector (if any)
    pub alias: Option<String>,
    /// Whether this is a source or sink connector
    pub is_source: bool,
}

/// Represents a window configuration
#[derive(Debug, Clone)]
pub struct WindowConfig {
    /// The type of window (e.g., "tumbling", "sliding", "session")
    pub window_type: String,
    /// The options for the window
    pub options: Vec<(String, String)>,
}

/// Represents a watermark configuration
#[derive(Debug, Clone)]
pub struct WatermarkConfig {
    /// The field to use for the watermark
    pub field: String,
    /// The delay for the watermark
    pub delay: String,
}

/// Parses a PRQL query and extracts extended syntax elements
///
/// # Arguments
///
/// * `prql_query` - The PRQL query to parse
///
/// # Returns
///
/// A tuple containing the modified PRQL query and any extracted configurations
pub fn parse_extended_syntax(prql_query: &str) -> Result<(String, Vec<ConnectorConfig>, Vec<WindowConfig>, Vec<WatermarkConfig>), PrqlError> {
    let mut modified_query = prql_query.to_string();
    let mut connectors = Vec::new();
    let mut windows = Vec::new();
    let mut watermarks = Vec::new();

    // Parse source connectors
    modified_query = parse_source_connectors(&modified_query, &mut connectors)?;

    // Parse sink connectors
    modified_query = parse_sink_connectors(&modified_query, &mut connectors)?;

    // Parse window configurations
    modified_query = parse_window_configs(&modified_query, &mut windows)?;

    // Parse watermark configurations
    modified_query = parse_watermark_configs(&modified_query, &mut watermarks)?;

    Ok((modified_query, connectors, windows, watermarks))
}

/// Parses source connector syntax from a PRQL query
///
/// # Arguments
///
/// * `query` - The PRQL query to parse
/// * `connectors` - A vector to store extracted connector configurations
///
/// # Returns
///
/// The modified PRQL query with source connector syntax replaced
fn parse_source_connectors(query: &str, connectors: &mut Vec<ConnectorConfig>) -> Result<String, PrqlError> {
    // Pattern for source connectors: from kafka ( ... ) as alias
    let source_pattern = Regex::new(r"from\s+(\w+)\s*\(\s*((?:[^()]*|\([^()]*\))*)\s*\)(?:\s+as\s+(\w+))?")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    let mut modified_query = query.to_string();
    let mut offset: i64 = 0;

    // Find all matches and process them
    for cap in source_pattern.captures_iter(query) {
        let whole_match = cap.get(0).unwrap();
        let connector_type = cap.get(1).unwrap().as_str().to_lowercase();
        let options_str = cap.get(2).unwrap().as_str();
        let alias = cap.get(3).map(|m| m.as_str().to_string());

        // Skip if it's not a connector type we recognize
        if !is_connector_type(&connector_type) {
            continue;
        }

        // Parse options
        let options = parse_options(options_str)?;

        // Replace with standard PRQL syntax
        let table_name = alias.clone().unwrap_or_else(|| format!("{}_source", connector_type.clone()));
        let replacement = format!("from {}", table_name);

        // Create connector config
        let config = ConnectorConfig {
            connector_type,
            options,
            alias: alias.clone(),
            is_source: true,
        };

        // Add to connectors list
        connectors.push(config);

        // Adjust for changes in string length
        let start = (whole_match.start() as i64 + offset) as usize;
        let end = (whole_match.end() as i64 + offset) as usize;
        modified_query.replace_range(start..end, &replacement);
        offset += replacement.len() as i64 - (end - start) as i64;
    }

    Ok(modified_query)
}

/// Parses sink connector syntax from a PRQL query
///
/// # Arguments
///
/// * `query` - The PRQL query to parse
/// * `connectors` - A vector to store extracted connector configurations
///
/// # Returns
///
/// The modified PRQL query with sink connector syntax replaced
fn parse_sink_connectors(query: &str, connectors: &mut Vec<ConnectorConfig>) -> Result<String, PrqlError> {
    // Pattern for sink connectors: into kafka ( ... )
    let sink_pattern = Regex::new(r"into\s+(\w+)\s*\(\s*((?:[^()]*|\([^()]*\))*)\s*\)")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    let mut modified_query = query.to_string();
    let mut offset: i64 = 0;

    // Find all matches and process them
    for cap in sink_pattern.captures_iter(query) {
        let whole_match = cap.get(0).unwrap();
        let connector_type = cap.get(1).unwrap().as_str().to_lowercase();
        let options_str = cap.get(2).unwrap().as_str();

        // Skip if it's not a connector type we recognize
        if !is_connector_type(&connector_type) {
            continue;
        }

        // Parse options
        let options = parse_options(options_str)?;

        // Create connector config
        let config = ConnectorConfig {
            connector_type,
            options,
            alias: None,
            is_source: false,
        };

        // Add to connectors list
        connectors.push(config);

        // Replace with empty string (will be handled in post-processing)
        let replacement = "";

        // Adjust for changes in string length
        let start = (whole_match.start() as i64 + offset) as usize;
        let end = (whole_match.end() as i64 + offset) as usize;
        modified_query.replace_range(start..end, replacement);
        offset += replacement.len() as i64 - (end - start) as i64;
    }

    Ok(modified_query)
}

/// Parses window configurations from a PRQL query
///
/// # Arguments
///
/// * `query` - The PRQL query to parse
/// * `windows` - A vector to store extracted window configurations
///
/// # Returns
///
/// The modified PRQL query with extended window syntax replaced
fn parse_window_configs(query: &str, windows: &mut Vec<WindowConfig>) -> Result<String, PrqlError> {
    // Pattern for extended window syntax: window tumbling ( size = 5m, time_field = event_time ) ( ... )
    let window_pattern = Regex::new(r"window\s+(\w+)\s*\(\s*((?:[^()]*|\([^()]*\))*)\s*\)\s*\(")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    let mut modified_query = query.to_string();
    let mut offset: i64 = 0;

    // Find all matches and process them
    for cap in window_pattern.captures_iter(query) {
        let whole_match = cap.get(0).unwrap();
        let window_type = cap.get(1).unwrap().as_str().to_lowercase();
        let options_str = cap.get(2).unwrap().as_str();

        // Parse options
        let options = parse_options(options_str)?;

        // Get window size from options
        let size = options.iter()
            .find(|(name, _)| name == "size")
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| "5m".to_string());

        // Create window config
        let config = WindowConfig {
            window_type: window_type.clone(),
            options,
        };

        // Add to windows list
        windows.push(config);

        // Replace with standard PRQL syntax
        let replacement = format!("window {} {} (", window_type, size);

        // Adjust for changes in string length
        let start = (whole_match.start() as i64 + offset) as usize;
        let end = (whole_match.end() as i64 + offset) as usize;
        modified_query.replace_range(start..end, &replacement);
        offset += replacement.len() as i64 - (end - start) as i64;
    }

    Ok(modified_query)
}

/// Parses watermark configurations from a PRQL query
///
/// # Arguments
///
/// * `query` - The PRQL query to parse
/// * `watermarks` - A vector to store extracted watermark configurations
///
/// # Returns
///
/// The modified PRQL query with watermark syntax replaced
fn parse_watermark_configs(query: &str, watermarks: &mut Vec<WatermarkConfig>) -> Result<String, PrqlError> {
    // Pattern for watermark syntax: watermark ( field = event_time, delay = 5s )
    let watermark_pattern = Regex::new(r"watermark\s*\(\s*((?:[^()]*|\([^()]*\))*)\s*\)")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    let mut modified_query = query.to_string();
    let mut offset: i64 = 0;

    // Find all matches and process them
    for cap in watermark_pattern.captures_iter(query) {
        let whole_match = cap.get(0).unwrap();
        let options_str = cap.get(1).unwrap().as_str();

        // Parse options
        let options = parse_options(options_str)?;

        // Extract field and delay
        let field = options.iter()
            .find(|(name, _)| name == "field")
            .map(|(_, value)| value.clone())
            .ok_or_else(|| PrqlError::PostProcessingError("Missing 'field' in watermark configuration".to_string()))?;

        let delay = options.iter()
            .find(|(name, _)| name == "delay")
            .map(|(_, value)| value.clone())
            .ok_or_else(|| PrqlError::PostProcessingError("Missing 'delay' in watermark configuration".to_string()))?;

        // Parse delay to seconds
        let delay_seconds = parse_time_unit(&delay)?;

        // Create watermark config
        let config = WatermarkConfig {
            field: field.clone(),
            delay: delay.clone(),
        };

        // Add to watermarks list
        watermarks.push(config);

        // Replace with comment (will be handled in post-processing)
        let replacement = format!("# -- WATERMARK FOR {} AS {} - INTERVAL '{}' SECOND",
                                 field, field, delay_seconds);

        // Adjust for changes in string length
        let start = (whole_match.start() as i64 + offset) as usize;
        let end = (whole_match.end() as i64 + offset) as usize;
        modified_query.replace_range(start..end, &replacement);

        // For debugging
        println!("Watermark replacement: {}", replacement);
        offset += replacement.len() as i64 - (end - start) as i64;
    }

    Ok(modified_query)
}

/// Parses options from a string
///
/// # Arguments
///
/// * `options_str` - The string containing options
///
/// # Returns
///
/// A vector of (name, value) pairs
fn parse_options(options_str: &str) -> Result<Vec<(String, String)>, PrqlError> {
    let mut options = Vec::new();
    // Updated pattern to handle property names with dots (e.g., bootstrap.servers)
    let option_pattern = Regex::new(r#"([\w.]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^,\s]+))"#)
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    for cap in option_pattern.captures_iter(options_str) {
        let name = cap.get(1).unwrap().as_str().to_string();
        let value = cap.get(2).or(cap.get(3)).or(cap.get(4)).unwrap().as_str().to_string();
        options.push((name, value));
    }

    Ok(options)
}

/// Checks if a string is a recognized connector type
///
/// # Arguments
///
/// * `connector_type` - The connector type to check
///
/// # Returns
///
/// `true` if the connector type is recognized, `false` otherwise
fn is_connector_type(connector_type: &str) -> bool {
    matches!(connector_type, "kafka" | "file" | "jdbc" | "http")
}

/// Parses a time unit string and returns the value in seconds
///
/// # Arguments
///
/// * `time_str` - The time string to parse (e.g., "5s", "10m", "1h")
///
/// # Returns
///
/// The time value in seconds
fn parse_time_unit(time_str: &str) -> Result<String, PrqlError> {
    let time_pattern = Regex::new(r"(\d+)([smhd])")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    if let Some(cap) = time_pattern.captures(time_str) {
        let value: u64 = cap.get(1).unwrap().as_str().parse()
            .map_err(|e| PrqlError::PostProcessingError(format!("Failed to parse time value: {}", e)))?;
        let unit = cap.get(2).unwrap().as_str();

        let seconds = match unit {
            "s" => value,
            "m" => value * 60,
            "h" => value * 3600,
            "d" => value * 86400,
            _ => return Err(PrqlError::PostProcessingError(format!("Unknown time unit: {}", unit))),
        };

        Ok(seconds.to_string())
    } else {
        Err(PrqlError::PostProcessingError(format!("Invalid time format: {}", time_str)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_source_connector() {
        let query = r#"from kafka (
            topic = "events",
            bootstrap.servers = "localhost:9092",
            format = "json"
        ) as events
        filter event_type == "click""#;

        let (modified, connectors, _, _) = parse_extended_syntax(query).unwrap();

        assert_eq!(connectors.len(), 1);
        assert_eq!(connectors[0].connector_type, "kafka");
        assert_eq!(connectors[0].options.len(), 3);
        assert_eq!(connectors[0].alias, Some("events".to_string()));
        assert!(connectors[0].is_source);

        assert!(modified.contains("from events"));
        assert!(!modified.contains("from kafka"));
    }

    #[test]
    fn test_parse_sink_connector() {
        let query = r#"from events
        filter event_type == "click"
        into kafka (
            topic = "output",
            bootstrap.servers = "localhost:9092",
            format = "json"
        )"#;

        let (modified, connectors, _, _) = parse_extended_syntax(query).unwrap();

        assert_eq!(connectors.len(), 1);
        assert_eq!(connectors[0].connector_type, "kafka");
        assert_eq!(connectors[0].options.len(), 3);
        assert_eq!(connectors[0].alias, None);
        assert!(!connectors[0].is_source);

        assert!(!modified.contains("into kafka"));
    }

    #[test]
    fn test_parse_window_config() {
        let query = r#"from events
        window tumbling (
            size = 5m,
            time_field = event_time
        ) (
            group user_id (
                aggregate {
                    event_count = count
                }
            )
        )"#;

        let (modified, _, windows, _) = parse_extended_syntax(query).unwrap();

        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].window_type, "tumbling");
        assert_eq!(windows[0].options.len(), 2);

        assert!(modified.contains("window tumbling 5m ("));
    }

    #[test]
    fn test_parse_watermark_config() {
        let query = r#"from events
        watermark (
            field = event_time,
            delay = 5s
        )
        filter event_type == "click""#;

        let (modified, _, _, watermarks) = parse_extended_syntax(query).unwrap();

        assert_eq!(watermarks.len(), 1);
        assert_eq!(watermarks[0].field, "event_time");
        assert_eq!(watermarks[0].delay, "5s");

        assert!(modified.contains("# -- WATERMARK FOR event_time"));
    }

    #[test]
    fn test_parse_time_unit() {
        assert_eq!(parse_time_unit("5s").unwrap(), "5");
        assert_eq!(parse_time_unit("10m").unwrap(), "600");
        assert_eq!(parse_time_unit("1h").unwrap(), "3600");
        assert_eq!(parse_time_unit("2d").unwrap(), "172800");
    }
}
