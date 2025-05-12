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
    /// 压缩错误
    CompressionError(String),
    /// 解压缩错误
    DecompressionError(String),
    /// 无效的压缩类型
    InvalidCompressionType(String),
    /// 会话错误
    SessionError(String),
    /// 连接错误
    ConnectionError(String),
    /// 请求错误
    RequestError(String),
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
            Error::CompressionError(message) => write!(f, "Compression error: {}", message),
            Error::DecompressionError(message) => write!(f, "Decompression error: {}", message),
            Error::InvalidCompressionType(message) => write!(f, "Invalid compression type: {}", message),
            Error::SessionError(message) => write!(f, "Session error: {}", message),
            Error::ConnectionError(message) => write!(f, "Connection error: {}", message),
            Error::RequestError(message) => write!(f, "Request error: {}", message),
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

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Other(format!("IO error: {}", err))
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(err: serde_yaml::Error) -> Self {
        Error::SerializationError(err.to_string())
    }
}

impl From<snap::Error> for Error {
    fn from(err: snap::Error) -> Self {
        Error::CompressionError(err.to_string())
    }
}

// zstd 库没有公开 Error 类型，所以我们使用字符串处理
impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::CompressionError(err)
    }
}
