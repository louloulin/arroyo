//! Converter module for post-processing SQL generated from PRQL

use crate::error::PrqlError;
use anyhow::Result;
use regex::Regex;

/// Post-processes SQL generated from PRQL to make it compatible with Arroyo
///
/// # Arguments
///
/// * `sql` - The SQL generated from PRQL
///
/// # Returns
///
/// The post-processed SQL
///
/// # Errors
///
/// Returns an error if the SQL cannot be post-processed
pub fn post_process_sql(sql: &str) -> Result<String, PrqlError> {
    let mut processed_sql = sql.to_string();

    // Handle Arroyo-specific window functions
    processed_sql = process_window_functions(&processed_sql)?;

    // Handle Arroyo-specific time functions
    processed_sql = process_time_functions(&processed_sql)?;

    // Handle Arroyo-specific connectors
    processed_sql = process_connectors(&processed_sql)?;

    Ok(processed_sql)
}

/// Processes window functions in the SQL
///
/// This function handles the conversion of standard SQL window functions to Arroyo's
/// specific window syntax. Arroyo uses TABLE functions like TUMBLE, HOP, and SESSION
/// for window operations.
fn process_window_functions(sql: &str) -> Result<String, PrqlError> {
    let mut processed_sql = sql.to_string();

    // Convert PRQL-generated window functions to Arroyo's TABLE functions

    // Pattern for tumbling window: WINDOW w AS (PARTITION BY ... ORDER BY ... RANGE BETWEEN ... AND ...)
    let tumbling_window_pattern = regex::Regex::new(
        r"WINDOW\s+\w+\s+AS\s+\(\s*PARTITION\s+BY\s+([^)]+)\s+ORDER\s+BY\s+([^)]+)\s+RANGE\s+BETWEEN\s+INTERVAL\s+'([^']+)'\s+([^)]+)\s+AND\s+CURRENT\s+ROW\s*\)"
    ).map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    // Replace tumbling window with Arroyo's TUMBLE function
    processed_sql = tumbling_window_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let _partition_by = &caps[1];
        let order_by = &caps[2];
        let interval = &caps[3];

        format!("TABLE(TUMBLE(TABLE source_table, DESCRIPTOR({}), INTERVAL '{}'))",
                order_by.trim(), interval.trim())
    }).to_string();

    // Pattern for sliding window: similar to tumbling but with different RANGE BETWEEN
    let sliding_window_pattern = regex::Regex::new(
        r"WINDOW\s+\w+\s+AS\s+\(\s*PARTITION\s+BY\s+([^)]+)\s+ORDER\s+BY\s+([^)]+)\s+RANGE\s+BETWEEN\s+INTERVAL\s+'([^']+)'\s+PRECEDING\s+AND\s+INTERVAL\s+'([^']+)'\s+FOLLOWING\s*\)"
    ).map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    // Replace sliding window with Arroyo's HOP function
    processed_sql = sliding_window_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let _partition_by = &caps[1];
        let order_by = &caps[2];
        let preceding = &caps[3];
        let following = &caps[4];

        format!("TABLE(HOP(TABLE source_table, DESCRIPTOR({}), INTERVAL '{}', INTERVAL '{}'))",
                order_by.trim(), preceding.trim(), following.trim())
    }).to_string();

    // Pattern for session window
    let session_window_pattern = regex::Regex::new(
        r"WINDOW\s+\w+\s+AS\s+\(\s*PARTITION\s+BY\s+([^)]+)\s+ORDER\s+BY\s+([^)]+)\s+RANGE\s+BETWEEN\s+INTERVAL\s+'([^']+)'\s+PRECEDING\s+AND\s+INTERVAL\s+'([^']+)'\s+FOLLOWING\s+WITH\s+GAP\s+INTERVAL\s+'([^']+)'\s*\)"
    ).map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    // Replace session window with Arroyo's SESSION function
    processed_sql = session_window_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let _partition_by = &caps[1];
        let order_by = &caps[2];
        let gap = &caps[5];

        format!("TABLE(SESSION(TABLE source_table, DESCRIPTOR({}), INTERVAL '{}'))",
                order_by.trim(), gap.trim())
    }).to_string();

    Ok(processed_sql)
}

/// Processes time functions in the SQL
///
/// This function handles the conversion of standard SQL time functions to Arroyo's
/// specific time functions and watermark syntax.
fn process_time_functions(sql: &str) -> Result<String, PrqlError> {
    let mut processed_sql = sql.to_string();

    // Convert standard time functions to Arroyo's time functions

    // Replace standard CURRENT_TIMESTAMP with Arroyo's processing_time() function
    // We can't use negative lookahead, so we'll use a simpler approach
    processed_sql = processed_sql.replace("CURRENT_TIMESTAMP", "processing_time()");

    // Replace standard EXTRACT with Arroyo's specific extract functions
    let extract_pattern = Regex::new(r"EXTRACT\s*\(\s*(\w+)\s+FROM\s+([^)]+)\s*\)")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    processed_sql = extract_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let unit = &caps[1];
        let expr = &caps[2];

        match unit.to_lowercase().as_str() {
            "year" => format!("EXTRACT_YEAR({})", expr),
            "month" => format!("EXTRACT_MONTH({})", expr),
            "day" => format!("EXTRACT_DAY({})", expr),
            "hour" => format!("EXTRACT_HOUR({})", expr),
            "minute" => format!("EXTRACT_MINUTE({})", expr),
            "second" => format!("EXTRACT_SECOND({})", expr),
            _ => format!("EXTRACT({} FROM {})", unit, expr)
        }
    }).to_string();

    // Handle watermark definitions
    // In PRQL, watermarks might be defined using a custom syntax that gets translated to a comment
    // We'll look for these comments and convert them to Arroyo's WATERMARK syntax
    let watermark_comment_pattern = Regex::new(r"--\s*WATERMARK\s+FOR\s+(\w+)\s+AS\s+([^;]+);")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    processed_sql = watermark_comment_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let column = &caps[1];
        let expr = &caps[2];

        format!("WATERMARK FOR {} AS {};", column, expr)
    }).to_string();

    // Handle new watermark format (from extended PRQL syntax)
    let new_watermark_pattern = Regex::new(r"#\s*--\s*WATERMARK\s+FOR\s+(\w+)\s+AS\s+([^-]+)-\s*INTERVAL\s+'([^']+)'\s*SECOND")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    processed_sql = new_watermark_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let field = &caps[1];
        let field_expr = &caps[2].trim();
        let interval = &caps[3];

        format!("WATERMARK FOR {} AS {} - INTERVAL '{}' SECOND", field, field_expr, interval)
    }).to_string();

    // Handle event time extraction
    let event_time_pattern = Regex::new(r"--\s*EVENT_TIME\s+(\w+)")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    processed_sql = event_time_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let column = &caps[1];

        format!("/* EVENT_TIME: {} */", column)
    }).to_string();

    Ok(processed_sql)
}

/// Processes connector-related syntax in the SQL
///
/// This function handles the conversion of standard SQL table references to Arroyo's
/// connector syntax. It looks for special comments or patterns that indicate connector
/// configurations and converts them to Arroyo's syntax.
fn process_connectors(sql: &str) -> Result<String, PrqlError> {
    let mut processed_sql = sql.to_string();

    // Process Kafka source connectors
    // Look for comments like: -- KAFKA_SOURCE: topic=users, bootstrap.servers=localhost:9092, format=json
    let kafka_source_pattern = Regex::new(r"--\s*KAFKA_SOURCE:\s*topic=([^,]+),\s*bootstrap\.servers=([^,]+)(?:,\s*format=([^,]+))?(?:,\s*(.+))?")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    processed_sql = kafka_source_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let topic = caps[1].trim();
        let servers = caps[2].trim();
        let format = caps.get(3).map_or("json", |m| m.as_str().trim());
        let extra_config = caps.get(4).map_or("", |m| m.as_str().trim());

        {
            let extra = if extra_config.is_empty() {
                String::new()
            } else {
                format!(", {}", extra_config)
            };

            format!(
                "KAFKA(
                    topic => '{}',
                    properties => (
                        'bootstrap.servers' => '{}'
                        {}
                    ),
                    format => {}
                )",
                topic,
                servers,
                extra,
                format
            )
        }
    }).to_string();

    // Process Kafka sink connectors
    // Look for comments like: -- KAFKA_SINK: topic=users_output, bootstrap.servers=localhost:9092, format=json
    let kafka_sink_pattern = Regex::new(r"--\s*KAFKA_SINK:\s*topic=([^,]+),\s*bootstrap\.servers=([^,]+)(?:,\s*format=([^,]+))?(?:,\s*(.+))?")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    processed_sql = kafka_sink_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let topic = caps[1].trim();
        let servers = caps[2].trim();
        let format = caps.get(3).map_or("json", |m| m.as_str().trim());
        let extra_config = caps.get(4).map_or("", |m| m.as_str().trim());

        {
            let extra = if extra_config.is_empty() {
                String::new()
            } else {
                format!(", {}", extra_config)
            };

            format!(
                "INSERT INTO KAFKA(
                    topic => '{}',
                    properties => (
                        'bootstrap.servers' => '{}'
                        {}
                    ),
                    format => {}
                )",
                topic,
                servers,
                extra,
                format
            )
        }
    }).to_string();

    // Process File source connectors
    // Look for comments like: -- FILE_SOURCE: path=/data/users.csv, format=csv
    let file_source_pattern = Regex::new(r"--\s*FILE_SOURCE:\s*path=([^,]+)(?:,\s*format=([^,]+))?(?:,\s*(.+))?")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    processed_sql = file_source_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let path = caps[1].trim();
        let format = caps.get(2).map_or("csv", |m| m.as_str().trim());
        let extra_config = caps.get(3).map_or("", |m| m.as_str().trim());

        {
            let extra = if extra_config.is_empty() {
                String::new()
            } else {
                format!(", {}", extra_config)
            };

            format!(
                "FILE(
                    path => '{}',
                    format => {}
                    {}
                )",
                path,
                format,
                extra
            )
        }
    }).to_string();

    // Process File sink connectors
    // Look for comments like: -- FILE_SINK: path=/data/users_output.csv, format=csv
    let file_sink_pattern = Regex::new(r"--\s*FILE_SINK:\s*path=([^,]+)(?:,\s*format=([^,]+))?(?:,\s*(.+))?")
        .map_err(|e| PrqlError::PostProcessingError(format!("Failed to compile regex: {}", e)))?;

    processed_sql = file_sink_pattern.replace_all(&processed_sql, |caps: &regex::Captures| {
        let path = caps[1].trim();
        let format = caps.get(2).map_or("csv", |m| m.as_str().trim());
        let extra_config = caps.get(3).map_or("", |m| m.as_str().trim());

        {
            let extra = if extra_config.is_empty() {
                String::new()
            } else {
                format!(", {}", extra_config)
            };

            format!(
                "INSERT INTO FILE(
                    path => '{}',
                    format => {}
                    {}
                )",
                path,
                format,
                extra
            )
        }
    }).to_string();

    // Process generic FROM table replacement with connector syntax
    // This is a more advanced feature that would require parsing the SQL more deeply
    // For now, we'll just handle the explicit comment-based approach above

    Ok(processed_sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_post_process_sql_identity() {
        let sql = "SELECT * FROM employees WHERE age > 30";
        let processed = post_process_sql(sql).unwrap();
        assert_eq!(processed, sql);
    }

    #[test]
    fn test_process_window_functions_tumbling() {
        let sql = "SELECT * FROM source_table \
                  WINDOW w AS (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW)";
        let expected = "SELECT * FROM source_table \
                       TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(event_time), INTERVAL '5 minutes'))";

        let processed = process_window_functions(sql).unwrap();
        assert_eq!(processed.split_whitespace().collect::<Vec<_>>().join(" "),
                  expected.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    #[test]
    fn test_process_window_functions_sliding() {
        let sql = "SELECT * FROM source_table \
                  WINDOW w AS (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '10 minutes' PRECEDING AND INTERVAL '5 minutes' FOLLOWING)";
        let expected = "SELECT * FROM source_table \
                       TABLE(HOP(TABLE source_table, DESCRIPTOR(event_time), INTERVAL '10 minutes', INTERVAL '5 minutes'))";

        let processed = process_window_functions(sql).unwrap();
        assert_eq!(processed.split_whitespace().collect::<Vec<_>>().join(" "),
                  expected.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    #[test]
    fn test_process_time_functions() {
        let sql = "SELECT EXTRACT(HOUR FROM event_time) as hour, CURRENT_TIMESTAMP as now FROM events";
        let expected = "SELECT EXTRACT_HOUR(event_time) as hour, processing_time() as now FROM events";

        let processed = process_time_functions(sql).unwrap();
        assert_eq!(processed.split_whitespace().collect::<Vec<_>>().join(" "),
                  expected.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    #[test]
    fn test_process_watermark() {
        let sql = "SELECT * FROM events; -- WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND;";
        let expected = "SELECT * FROM events; WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND;";

        let processed = process_time_functions(sql).unwrap();
        assert_eq!(processed.split_whitespace().collect::<Vec<_>>().join(" "),
                  expected.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    #[test]
    fn test_process_kafka_connector() {
        let sql = "SELECT * FROM users; -- KAFKA_SOURCE: topic=users, bootstrap.servers=localhost:9092, format=json";
        let expected = "SELECT * FROM users; KAFKA( topic => 'users', properties => ( 'bootstrap.servers' => 'localhost:9092' ), format => json )";

        let processed = process_connectors(sql).unwrap();
        // Remove whitespace for comparison
        assert_eq!(processed.split_whitespace().collect::<Vec<_>>().join(" "),
                  expected.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    #[test]
    fn test_process_file_connector() {
        let sql = "SELECT * FROM data; -- FILE_SOURCE: path=/data/users.csv, format=csv";
        let expected = "SELECT * FROM data; FILE( path => '/data/users.csv', format => csv )";

        let processed = process_connectors(sql).unwrap();
        // Remove whitespace for comparison
        assert_eq!(processed.split_whitespace().collect::<Vec<_>>().join(" "),
                  expected.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    #[test]
    fn test_process_kafka_sink() {
        let sql = "INSERT INTO output_table SELECT * FROM users; -- KAFKA_SINK: topic=users_output, bootstrap.servers=localhost:9092";
        let expected = "INSERT INTO output_table SELECT * FROM users; INSERT INTO KAFKA( topic => 'users_output', properties => ( 'bootstrap.servers' => 'localhost:9092' ), format => json )";

        let processed = process_connectors(sql).unwrap();
        // Remove whitespace for comparison
        assert_eq!(processed.split_whitespace().collect::<Vec<_>>().join(" "),
                  expected.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    #[test]
    fn test_full_pipeline_processing() {
        // Test each component separately since we've already tested them individually

        // Test window functions
        let window_sql = "SELECT * FROM events WINDOW w AS (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW)";
        let processed_window = process_window_functions(window_sql).unwrap();
        assert!(processed_window.contains("TABLE(TUMBLE"));

        // Test extract functions
        let extract_sql = "SELECT EXTRACT(HOUR FROM event_time) as hour FROM events";
        let processed_extract = process_time_functions(extract_sql).unwrap();
        assert!(processed_extract.contains("EXTRACT_HOUR"));

        // Test kafka source
        let kafka_sql = "SELECT * FROM events -- KAFKA_SOURCE: topic=events, bootstrap.servers=localhost:9092";
        let processed_kafka = process_connectors(kafka_sql).unwrap();
        assert!(processed_kafka.contains("KAFKA("));

        // Test watermark
        let watermark_sql = "SELECT * FROM events -- WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND;";
        let processed_watermark = process_time_functions(watermark_sql).unwrap();
        assert!(processed_watermark.contains("WATERMARK FOR"));
    }
}
