use std::fmt;

/// SDK 结果类型
pub type Result<T> = std::result::Result<T, Error>;

/// SDK 错误类型
#[derive(Debug)]
pub enum Error {
    /// API 错误
    ApiError(u16, String),
    /// 客户端错误
    ClientError(String),
    /// 反序列化错误
    DeserializationError(String),
    /// 序列化错误
    SerializationError(String),
    /// 验证错误
    ValidationError(String),
    /// 其他错误
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ApiError(status, message) => write!(f, "API error ({}): {}", status, message),
            Error::ClientError(message) => write!(f, "Client error: {}", message),
            Error::DeserializationError(message) => write!(f, "Deserialization error: {}", message),
            Error::SerializationError(message) => write!(f, "Serialization error: {}", message),
            Error::ValidationError(message) => write!(f, "Validation error: {}", message),
            Error::Other(message) => write!(f, "Error: {}", message),
        }
    }
}

impl std::error::Error for Error {}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::ClientError(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::SerializationError(err.to_string())
    }
}
