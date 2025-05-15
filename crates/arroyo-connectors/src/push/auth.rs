use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::Response,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthConfig {
    /// No authentication
    None,
    /// API key authentication
    ApiKey(String),
    /// OAuth authentication
    OAuth(OAuthConfig),
}

/// OAuth configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthConfig {
    pub client_id: String,
    pub client_secret: String,
    pub token_url: String,
}

/// Authentication service
pub struct AuthService {
    pub config: AuthConfig,
}

impl AuthService {
    /// Create a new authentication service
    pub fn new(config: AuthConfig) -> Self {
        Self { config }
    }

    /// Validate authentication
    pub fn validate(&self, auth_header: Option<&str>) -> bool {
        match &self.config {
            AuthConfig::None => true,
            AuthConfig::ApiKey(api_key) => {
                if let Some(header_value) = auth_header {
                    if let Some(token) = header_value.strip_prefix("Bearer ") {
                        return token == api_key;
                    }
                }
                false
            }
            AuthConfig::OAuth(_) => {
                // TODO: Implement OAuth validation
                warn!("OAuth validation not implemented yet");
                false
            }
        }
    }
}

/// Authentication middleware for Axum
pub async fn auth_middleware<S>(
    State(auth_service): State<Arc<AuthService>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Skip authentication for health check endpoint
    if request.uri().path() == "/api/v1/push/health" {
        return Ok(next.run(request).await);
    }

    // Get authorization header
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    // Validate authentication
    if auth_service.validate(auth_header) {
        debug!("Authentication successful");
        Ok(next.run(request).await)
    } else {
        error!("Authentication failed");
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Authorization service
pub struct AuthorizationService {
    // TODO: Implement topic-based authorization
}

impl AuthorizationService {
    /// Create a new authorization service
    pub fn new() -> Self {
        Self {}
    }

    /// Check if user has access to topic
    pub fn has_access_to_topic(&self, _user: &str, _topic: &str) -> bool {
        // TODO: Implement topic-based authorization
        true
    }
}
