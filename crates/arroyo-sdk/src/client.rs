use crate::error::{Error, Result};
use reqwest::{Client, Response, StatusCode};
use serde::de::DeserializeOwned;

/// Arroyo 客户端，用于与 Arroyo 服务器交互
#[derive(Debug, Clone)]
pub struct ArroyoClient {
    /// 基础 URL
    pub(crate) base_url: String,
    /// HTTP 客户端
    pub(crate) client: Client,
}

impl ArroyoClient {
    /// 创建新的 Arroyo 客户端
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| Error::ClientError(e.to_string()))?;

        Ok(Self {
            base_url: base_url.into(),
            client,
        })
    }

    /// 使用自定义 HTTP 客户端创建 Arroyo 客户端
    pub fn new_with_client(base_url: impl Into<String>, client: Client) -> Self {
        Self {
            base_url: base_url.into(),
            client,
        }
    }

    /// 处理 API 响应
    pub(crate) async fn handle_response<T: DeserializeOwned>(&self, response: Response) -> Result<T> {
        match response.status() {
            StatusCode::OK | StatusCode::CREATED => {
                response
                    .json::<T>()
                    .await
                    .map_err(|e| Error::DeserializationError(e.to_string()))
            }
            status => {
                let error_text = response.text().await.unwrap_or_default();
                Err(Error::ApiError(status.as_u16(), error_text))
            }
        }
    }

    /// 处理无返回值的 API 响应
    pub(crate) async fn handle_empty_response(&self, response: Response) -> Result<()> {
        match response.status() {
            StatusCode::OK | StatusCode::CREATED | StatusCode::NO_CONTENT => Ok(()),
            status => {
                let error_text = response.text().await.unwrap_or_default();
                Err(Error::ApiError(status.as_u16(), error_text))
            }
        }
    }
}
