use std::collections::HashMap;

use anyhow::Result;
use arroyo_rpc::ConnectorOptions;
use datafusion::sql::sqlparser::ast::{Expr, Value};
use tracing::{debug, warn};

/// Parse protocol-specific options from SQL WITH clause
pub fn parse_protocol_options(options: &mut ConnectorOptions) -> Result<(), anyhow::Error> {
    // Get protocol
    let protocol = options
        .pull_opt_str("protocol")?
        .unwrap_or_else(|| "http".to_string());

    // Parse protocol-specific options
    match protocol.as_str() {
        "http" => parse_http_options(options)?,
        "quic" => parse_quic_options(options)?,
        "grpc" => parse_grpc_options(options)?,
        "websocket" => parse_websocket_options(options)?,
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported protocol: {}. Supported protocols are: http, quic, grpc, websocket",
                protocol
            ));
        }
    }

    Ok(())
}

/// Parse HTTP-specific options
fn parse_http_options(options: &mut ConnectorOptions) -> Result<(), anyhow::Error> {
    let mut http_config = HashMap::new();

    // Extract HTTP-specific options
    for key in options.keys().cloned().collect::<Vec<_>>() {
        if key.starts_with("http.") {
            let option_name = key.strip_prefix("http.").unwrap();
            if let Some(value) = options.pull_opt_str(&key)? {
                let value_clone = value.clone();
                http_config.insert(option_name.to_string(), value);
                debug!("Found HTTP option: {} = {}", option_name, value_clone);
            }
        }
    }

    // Add HTTP config to options if any options were found
    if !http_config.is_empty() {
        options.insert_str("http_config", &serde_json::to_string(&http_config)?);
    }

    Ok(())
}

/// Parse QUIC-specific options
fn parse_quic_options(options: &mut ConnectorOptions) -> Result<(), anyhow::Error> {
    let mut quic_config = HashMap::new();

    // Extract QUIC-specific options
    for key in options.keys().cloned().collect::<Vec<_>>() {
        if key.starts_with("quic.") {
            let option_name = key.strip_prefix("quic.").unwrap();
            if let Some(value) = options.pull_opt_str(&key)? {
                let value_clone = value.clone();
                quic_config.insert(option_name.to_string(), value);
                debug!("Found QUIC option: {} = {}", option_name, value_clone);
            }
        }
    }

    // Add QUIC config to options if any options were found
    if !quic_config.is_empty() {
        options.insert_str("quic_config", &serde_json::to_string(&quic_config)?);
    }

    Ok(())
}

/// Parse gRPC-specific options
fn parse_grpc_options(options: &mut ConnectorOptions) -> Result<(), anyhow::Error> {
    let mut grpc_config = HashMap::new();

    // Extract gRPC-specific options
    for key in options.keys().cloned().collect::<Vec<_>>() {
        if key.starts_with("grpc.") {
            let option_name = key.strip_prefix("grpc.").unwrap();
            if let Some(value) = options.pull_opt_str(&key)? {
                let value_clone = value.clone();
                grpc_config.insert(option_name.to_string(), value);
                debug!("Found gRPC option: {} = {}", option_name, value_clone);
            }
        }
    }

    // Add gRPC config to options if any options were found
    if !grpc_config.is_empty() {
        options.insert_str("grpc_config", &serde_json::to_string(&grpc_config)?);
    }

    Ok(())
}

/// Parse WebSocket-specific options
fn parse_websocket_options(options: &mut ConnectorOptions) -> Result<(), anyhow::Error> {
    let mut websocket_config = HashMap::new();

    // Extract WebSocket-specific options
    for key in options.keys().cloned().collect::<Vec<_>>() {
        if key.starts_with("ws.") {
            let option_name = key.strip_prefix("ws.").unwrap();
            if let Some(value) = options.pull_opt_str(&key)? {
                let value_clone = value.clone();
                websocket_config.insert(option_name.to_string(), value);
                debug!("Found WebSocket option: {} = {}", option_name, value_clone);
            }
        }
    }

    // Add WebSocket config to options if any options were found
    if !websocket_config.is_empty() {
        options.insert_str("websocket_config", &serde_json::to_string(&websocket_config)?);
    }

    Ok(())
}

/// Validate protocol options
pub fn validate_protocol_options(options: &mut ConnectorOptions) -> Result<(), anyhow::Error> {
    // Get protocol
    let protocol = match options.pull_opt_str("protocol").map_err(|e| anyhow::anyhow!("{}", e))? {
        Some(s) => s,
        None => "http".to_string(),
    };

    // Check if topic exists, but don't remove it
    let has_topic = options.contains_key("topic");
    if !has_topic {
        return Err(anyhow::anyhow!("Missing required option: topic"));
    }

    // Validate protocol-specific options
    match protocol.as_str() {
        "http" | "quic" | "grpc" | "websocket" => {
            // No additional validation needed
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported protocol: {}. Supported protocols are: http, quic, grpc, websocket",
                protocol
            ));
        }
    }

    Ok(())
}
