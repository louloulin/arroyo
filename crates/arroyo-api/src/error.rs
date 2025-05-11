use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::error;

/// API 错误
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    /// 状态码
    #[serde(skip)]
    pub status_code: StatusCode,
    /// 错误消息
    pub message: String,
}

impl ApiError {
    /// 创建 API 错误
    pub fn new(status_code: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status_code,
            message: message.into(),
        }
    }

    /// 创建 400 Bad Request 错误
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    /// 创建 404 Not Found 错误
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    /// 创建 500 Internal Server Error 错误
    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }

    /// 创建 503 Service Unavailable 错误
    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status_code = self.status_code;
        let body = Json(serde_json::json!({
            "error": self.message
        }));
        (status_code, body).into_response()
    }
}

impl<E> From<E> for ApiError
where
    E: std::error::Error,
{
    fn from(err: E) -> Self {
        error!("Error: {}", err);
        Self::internal_error(format!("Internal server error: {}", err))
    }
}
