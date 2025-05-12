use crate::client::ArroyoClient;
use crate::error::{Error, Result};
use crate::models::Topic;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;

/// Topic 配置选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicOptions {
    /// Topic 名称
    pub name: String,
    /// 分区数量
    pub partitions: u32,
    /// 复制因子
    pub replication_factor: u32,
    /// 保留策略（毫秒）
    pub retention_ms: Option<u64>,
    /// 保留策略（字节）
    pub retention_bytes: Option<u64>,
    /// 清理策略
    pub cleanup_policy: Option<String>,
    /// 其他配置选项
    pub config: Option<HashMap<String, String>>,
}

impl Default for TopicOptions {
    fn default() -> Self {
        Self {
            name: String::new(),
            partitions: 1,
            replication_factor: 1,
            retention_ms: None,
            retention_bytes: None,
            cleanup_policy: None,
            config: None,
        }
    }
}

/// Topic 构建器，用于流畅的 API 设计
pub struct TopicBuilder {
    options: TopicOptions,
}

impl TopicBuilder {
    /// 创建新的 Topic 构建器
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            options: TopicOptions {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    /// 设置分区数量
    pub fn partitions(mut self, partitions: u32) -> Self {
        self.options.partitions = partitions;
        self
    }

    /// 设置复制因子
    pub fn replication_factor(mut self, replication_factor: u32) -> Self {
        self.options.replication_factor = replication_factor;
        self
    }

    /// 设置保留时间（毫秒）
    pub fn retention_ms(mut self, retention_ms: u64) -> Self {
        self.options.retention_ms = Some(retention_ms);
        self
    }

    /// 设置保留大小（字节）
    pub fn retention_bytes(mut self, retention_bytes: u64) -> Self {
        self.options.retention_bytes = Some(retention_bytes);
        self
    }

    /// 设置清理策略
    pub fn cleanup_policy(mut self, cleanup_policy: impl Into<String>) -> Self {
        self.options.cleanup_policy = Some(cleanup_policy.into());
        self
    }

    /// 添加配置选项
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let config = self.options.config.get_or_insert_with(HashMap::new);
        config.insert(key.into(), value.into());
        self
    }

    /// 构建 TopicOptions
    pub fn build(self) -> TopicOptions {
        self.options
    }
}

/// Topic 相关的 API 扩展
impl ArroyoClient {
    /// 创建新的 Topic
    pub async fn create_topic(&self, options: TopicOptions) -> Result<Topic> {
        let response = self
            .client
            .post(&format!("{}/api/topics", self.base_url))
            .json(&options)
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// 获取所有 Topics
    pub async fn get_topics(&self) -> Result<Vec<Topic>> {
        let response = self
            .client
            .get(&format!("{}/api/topics", self.base_url))
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// 获取指定 Topic
    pub async fn get_topic(&self, name: &str) -> Result<Topic> {
        let response = self
            .client
            .get(&format!("{}/api/topics/{}", self.base_url, name))
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// 删除 Topic
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let response = self
            .client
            .delete(&format!("{}/api/topics/{}", self.base_url, name))
            .send()
            .await?;

        self.handle_empty_response(response).await
    }

    /// 更新 Topic 配置
    pub async fn update_topic_config(
        &self,
        name: &str,
        config: HashMap<String, String>,
    ) -> Result<Topic> {
        let response = self
            .client
            .patch(&format!("{}/api/topics/{}/config", self.base_url, name))
            .json(&config)
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// 导出 Topic 配置
    pub async fn export_topics(&self, topics: Option<Vec<String>>, format: Option<String>) -> Result<String> {
        let request = serde_json::json!({
            "topics": topics,
            "format": format.unwrap_or_else(|| "json".to_string()),
            "download": false
        });

        let response = self
            .client
            .post(&format!("{}/api/topics/export", self.base_url))
            .json(&request)
            .send()
            .await?;

        let result: serde_json::Value = self.handle_response(response).await?;
        Ok(result["content"].as_str().unwrap_or_default().to_string())
    }

    /// 导出 Topic 配置到文件
    pub async fn export_topics_to_file<P: AsRef<std::path::Path>>(&self, path: P, topics: Option<Vec<String>>, format: Option<String>) -> Result<()> {
        let content = self.export_topics(topics, format).await?;

        let mut file = std::fs::File::create(path)?;
        file.write_all(content.as_bytes())?;

        Ok(())
    }

    /// 导入 Topic 配置
    pub async fn import_topics(&self, content: &str, format: Option<String>, skip_existing: Option<bool>) -> Result<serde_json::Value> {
        let request = serde_json::json!({
            "content": content,
            "format": format.unwrap_or_else(|| "json".to_string()),
            "skip_existing": skip_existing.unwrap_or(true)
        });

        let response = self
            .client
            .post(&format!("{}/api/topics/import", self.base_url))
            .json(&request)
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// 从文件导入 Topic 配置
    pub async fn import_topics_from_file<P: AsRef<std::path::Path>>(&self, path: P, format: Option<String>, skip_existing: Option<bool>) -> Result<serde_json::Value> {
        let mut file = std::fs::File::open(path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;

        self.import_topics(&content, format, skip_existing).await
    }

    /// 创建 Topic 构建器，用于流畅的 API 设计
    pub fn topic_builder(&self, name: impl Into<String>) -> TopicBuilder {
        TopicBuilder::new(name)
    }
}
