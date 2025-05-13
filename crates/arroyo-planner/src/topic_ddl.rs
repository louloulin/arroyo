use anyhow::{anyhow, Result};
use arroyo_rpc::api_types::topics::{TopicConfig, TopicInfo};
use datafusion::common::DataFusionError;
use sqlparser::ast::{
    Expr, Ident, ObjectName, OneOrManyWithParens, SqlOption, Statement, Value as SqlValue, Values,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Topic DDL 语句类型
#[derive(Debug, Clone)]
pub enum TopicDdlStatement {
    /// 创建 Topic
    CreateTopic {
        /// Topic 名称
        name: String,
        /// 是否仅在不存在时创建
        if_not_exists: bool,
        /// Topic 配置
        config: TopicConfig,
    },
    /// 修改 Topic
    AlterTopic {
        /// Topic 名称
        name: String,
        /// Topic 配置
        config: TopicConfig,
    },
    /// 删除 Topic
    DropTopic {
        /// Topic 名称
        name: String,
        /// 是否仅在存在时删除
        if_exists: bool,
    },
    /// 显示所有 Topic
    ShowTopics,
}

/// 尝试将 SQL 语句解析为 Topic DDL 语句
pub fn try_parse_topic_ddl(statement: &Statement) -> Result<Option<TopicDdlStatement>> {
    match statement {
        Statement::CreateTable(create) => {
            // 检查是否是 CREATE TOPIC 语句
            if let Some(first_ident) = create.name.0.first() {
                if first_ident.value.to_uppercase() == "TOPIC" {
                    // 这是一个 CREATE TOPIC 语句
                    if create.name.0.len() < 2 {
                        return Err(anyhow!("Missing topic name in CREATE TOPIC statement"));
                    }

                    let topic_name = create.name.0[1].value.clone();
                    let if_not_exists = create.if_not_exists;

                    // 解析 WITH 选项
                    let mut config = TopicConfig {
                        name: topic_name.clone(),
                        partitions: 1,
                        replication_factor: 1,
                        retention_ms: None,
                        retention_bytes: None,
                        cleanup_policy: "delete".to_string(),
                        max_message_bytes: None,
                        description: None,
                    };

                    if let Some(with_options) = &create.with_options {
                        for option in with_options {
                            let name = option.name.value.to_lowercase();
                            match name.as_str() {
                                "partitions" => {
                                    if let SqlValue::Number(n, _) = &option.value {
                                        config.partitions = n.parse::<i32>().unwrap_or(1);
                                    }
                                }
                                "replication_factor" => {
                                    if let SqlValue::Number(n, _) = &option.value {
                                        config.replication_factor = n.parse::<i16>().unwrap_or(1);
                                    }
                                }
                                "retention_ms" => {
                                    if let SqlValue::Number(n, _) = &option.value {
                                        config.retention_ms = Some(n.parse::<i64>().unwrap_or(86400000));
                                    }
                                }
                                "retention_bytes" => {
                                    if let SqlValue::Number(n, _) = &option.value {
                                        config.retention_bytes = Some(n.parse::<i64>().unwrap_or(-1));
                                    }
                                }
                                "cleanup_policy" => {
                                    if let SqlValue::SingleQuotedString(s) = &option.value {
                                        config.cleanup_policy = s.clone();
                                    }
                                }
                                "max_message_bytes" => {
                                    if let SqlValue::Number(n, _) = &option.value {
                                        config.max_message_bytes = Some(n.parse::<i32>().unwrap_or(1048576));
                                    }
                                }
                                "description" => {
                                    if let SqlValue::SingleQuotedString(s) = &option.value {
                                        config.description = Some(s.clone());
                                    }
                                }
                                _ => {
                                    warn!("Unknown option in CREATE TOPIC: {}", name);
                                }
                            }
                        }
                    }

                    return Ok(Some(TopicDdlStatement::CreateTopic {
                        name: topic_name,
                        if_not_exists,
                        config,
                    }));
                }
            }
        }
        Statement::AlterTable(alter) => {
            // 检查是否是 ALTER TOPIC 语句
            if let Some(first_ident) = alter.name.0.first() {
                if first_ident.value.to_uppercase() == "TOPIC" {
                    // 这是一个 ALTER TOPIC 语句
                    if alter.name.0.len() < 2 {
                        return Err(anyhow!("Missing topic name in ALTER TOPIC statement"));
                    }

                    let topic_name = alter.name.0[1].value.clone();

                    // 创建基本配置
                    let mut config = TopicConfig {
                        name: topic_name.clone(),
                        partitions: 1,
                        replication_factor: 1,
                        retention_ms: None,
                        retention_bytes: None,
                        cleanup_policy: "delete".to_string(),
                        max_message_bytes: None,
                        description: None,
                    };

                    // 解析 SET 选项
                    for operation in &alter.operations {
                        if let sqlparser::ast::AlterTableOperation::SetOption { name, value } = operation {
                            let option_name = name.value.to_lowercase();
                            match option_name.as_str() {
                                "retention_ms" => {
                                    if let Expr::Value(SqlValue::Number(n, _)) = value {
                                        config.retention_ms = Some(n.parse::<i64>().unwrap_or(86400000));
                                    }
                                }
                                "retention_bytes" => {
                                    if let Expr::Value(SqlValue::Number(n, _)) = value {
                                        config.retention_bytes = Some(n.parse::<i64>().unwrap_or(-1));
                                    }
                                }
                                "cleanup_policy" => {
                                    if let Expr::Value(SqlValue::SingleQuotedString(s)) = value {
                                        config.cleanup_policy = s.clone();
                                    }
                                }
                                "max_message_bytes" => {
                                    if let Expr::Value(SqlValue::Number(n, _)) = value {
                                        config.max_message_bytes = Some(n.parse::<i32>().unwrap_or(1048576));
                                    }
                                }
                                "description" => {
                                    if let Expr::Value(SqlValue::SingleQuotedString(s)) = value {
                                        config.description = Some(s.clone());
                                    }
                                }
                                _ => {
                                    warn!("Unknown option in ALTER TOPIC: {}", option_name);
                                }
                            }
                        }
                    }

                    return Ok(Some(TopicDdlStatement::AlterTopic {
                        name: topic_name,
                        config,
                    }));
                }
            }
        }
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            // 检查是否是 DROP TOPIC 语句
            if object_type.to_string().to_uppercase() == "TOPIC" {
                if names.is_empty() {
                    return Err(anyhow!("Missing topic name in DROP TOPIC statement"));
                }

                let topic_name = names[0].to_string();

                return Ok(Some(TopicDdlStatement::DropTopic {
                    name: topic_name,
                    if_exists: *if_exists,
                }));
            }
        }
        Statement::ShowTables(show) => {
            // 检查是否是 SHOW TOPICS 语句
            if let Some(object_type) = &show.object_type {
                if object_type.to_uppercase() == "TOPICS" {
                    return Ok(Some(TopicDdlStatement::ShowTopics));
                }
            }
        }
        _ => {}
    }

    Ok(None)
}

/// 执行 Topic DDL 语句
pub async fn execute_topic_ddl(statement: TopicDdlStatement) -> Result<String> {
    // 获取 Topic 管理 API 客户端
    let topic_admin = get_topic_admin().await?;

    match statement {
        TopicDdlStatement::CreateTopic {
            name,
            if_not_exists,
            config,
        } => {
            info!("Creating topic: {}", name);

            // 检查 Topic 是否已存在
            let topic_exists = topic_admin.topic_exists(&name).await?;

            if topic_exists {
                if if_not_exists {
                    return Ok(format!("Topic '{}' already exists", name));
                } else {
                    return Err(anyhow!("Topic '{}' already exists", name));
                }
            }

            // 创建 Topic
            let topic_info = topic_admin.create_topic(&config, None).await?;

            Ok(format!("Topic '{}' created successfully with {} partitions and replication factor {}",
                topic_info.name, topic_info.partitions, topic_info.replication_factor))
        }
        TopicDdlStatement::AlterTopic { name, config } => {
            info!("Altering topic: {}", name);

            // 检查 Topic 是否存在
            let topic_exists = topic_admin.topic_exists(&name).await?;

            if !topic_exists {
                return Err(anyhow!("Topic '{}' does not exist", name));
            }

            // 修改 Topic
            let topic_info = topic_admin.update_topic(&config, None).await?;

            Ok(format!("Topic '{}' altered successfully", topic_info.name))
        }
        TopicDdlStatement::DropTopic { name, if_exists } => {
            info!("Dropping topic: {}", name);

            // 检查 Topic 是否存在
            let topic_exists = topic_admin.topic_exists(&name).await?;

            if !topic_exists {
                if if_exists {
                    return Ok(format!("Topic '{}' does not exist", name));
                } else {
                    return Err(anyhow!("Topic '{}' does not exist", name));
                }
            }

            // 删除 Topic
            topic_admin.delete_topic(&name, None).await?;

            Ok(format!("Topic '{}' dropped successfully", name))
        }
        TopicDdlStatement::ShowTopics => {
            info!("Showing topics");

            // 获取所有 Topic
            let topics = topic_admin.list_topics(None).await?;

            if topics.is_empty() {
                return Ok("No topics found".to_string());
            }

            // 格式化 Topic 列表
            let mut result = String::from("Topics:\n");
            for topic in topics {
                result.push_str(&format!("- {} (partitions: {}, replication: {})\n",
                    topic.name, topic.partitions, topic.replication_factor));
            }

            Ok(result)
        }
    }
}

/// 获取 Topic 管理 API 客户端
async fn get_topic_admin() -> Result<Arc<arroyo_connectors::topic::TopicAdmin>> {
    use arroyo_connectors::topic::TopicAdmin;
    use arroyo_rpc::config::config;

    // 从配置中获取 Kafka 连接信息
    let bootstrap_servers = config().kafka.bootstrap_servers.clone();

    // 创建 Topic 管理 API 客户端
    let topic_admin = TopicAdmin::new(bootstrap_servers, None).await?;

    Ok(Arc::new(topic_admin))
}
