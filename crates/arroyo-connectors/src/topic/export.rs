use anyhow::{anyhow, Result};
use arroyo_rpc::api_types::topics::{TopicConfig, TopicInfo};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use tracing::{error, info};

/// Topic 配置导出格式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicExport {
    /// 格式版本
    pub version: String,
    /// 导出时间戳
    pub timestamp: u64,
    /// Topic 配置列表
    pub topics: Vec<TopicConfig>,
    /// 元数据
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl TopicExport {
    /// 创建新的导出对象
    pub fn new(topics: Vec<TopicConfig>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            version: "1.0".to_string(),
            timestamp: now,
            topics,
            metadata: HashMap::new(),
        }
    }

    /// 添加元数据
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// 从 TopicInfo 列表创建导出对象
    pub fn from_topic_infos(infos: &[TopicInfo]) -> Self {
        let topics = infos
            .iter()
            .map(|info| TopicConfig {
                name: info.name.clone(),
                partitions: info.partitions,
                replication_factor: info.replication_factor,
                retention_ms: info.retention_ms,
                retention_bytes: info.retention_bytes,
                cleanup_policy: info.cleanup_policy.clone(),
                max_message_bytes: info.max_message_bytes,
                description: info.description.clone(),
            })
            .collect();

        Self::new(topics)
    }

    /// 导出到 JSON 文件
    pub fn export_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }

    /// 从 JSON 文件导入
    pub fn import_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let export: TopicExport = serde_json::from_reader(reader)?;
        Ok(export)
    }

    /// 导出到 YAML 文件
    pub fn export_to_yaml_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serde_yaml::to_writer(writer, self)?;
        Ok(())
    }

    /// 从 YAML 文件导入
    pub fn import_from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let export: TopicExport = serde_yaml::from_reader(reader)?;
        Ok(export)
    }

    /// 导出到字符串
    pub fn to_json_string(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// 从字符串导入
    pub fn from_json_string(json: &str) -> Result<Self> {
        Ok(serde_json::from_str(json)?)
    }

    /// 导出到 YAML 字符串
    pub fn to_yaml_string(&self) -> Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }

    /// 从 YAML 字符串导入
    pub fn from_yaml_string(yaml: &str) -> Result<Self> {
        Ok(serde_yaml::from_str(yaml)?)
    }
}
