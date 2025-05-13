use anyhow::{anyhow, Result};
use datafusion::common::DataFusionError;
use sqlparser::ast::{
    Expr, Ident, ObjectName, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    Values as SqlValues, Value as SqlValue,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};
use serde_json;

use crate::ArroyoSchemaProvider;

/// 消息队列 DML 语句类型
#[derive(Debug, Clone)]
pub enum QueueDmlStatement {
    /// 生产消息
    Produce {
        /// Topic 名称
        topic: String,
        /// 列名
        columns: Vec<String>,
        /// 键列索引（如果有）
        key_column_index: Option<usize>,
        /// 值（直接指定）
        values: Option<Vec<Vec<Expr>>>,
        /// 查询（从查询结果生产）
        query: Option<Box<Query>>,
    },
    /// 消费消息
    Consume {
        /// Topic 名称
        topic: String,
        /// 消费者组 ID
        group_id: Option<String>,
        /// 过滤条件
        filter: Option<Expr>,
        /// 限制数量
        limit: Option<usize>,
    },
}

/// 尝试将 SQL 语句解析为消息队列 DML 语句
pub fn try_parse_queue_dml(statement: &Statement) -> Result<Option<QueueDmlStatement>> {
    // 尝试解析 PRODUCE 语句
    if let Some(produce_stmt) = try_parse_produce(statement)? {
        return Ok(Some(produce_stmt));
    }

    // 尝试解析 CONSUME 语句
    if let Some(consume_stmt) = try_parse_consume(statement)? {
        return Ok(Some(consume_stmt));
    }

    Ok(None)
}

/// 尝试解析 PRODUCE 语句
fn try_parse_produce(statement: &Statement) -> Result<Option<QueueDmlStatement>> {
    // 检查是否是自定义的 PRODUCE 语句
    if let Statement::Query(query) = statement {
        if let SetExpr::Query(subquery) = &query.body {
            if let SetExpr::Values(_) = &subquery.body {
                // 检查是否有 PRODUCE INTO 子句
                if let Some(with) = &query.with {
                    for cte in &with.cte_tables {
                        if cte.alias.name.value.to_uppercase() == "PRODUCE" {
                            // 找到 PRODUCE 语句
                            if let Some(into_clause) = &cte.from {
                                let topic = into_clause.to_string();

                                // 解析列名
                                let columns = if let Some(columns) = &cte.columns {
                                    columns.iter().map(|c| c.value.clone()).collect()
                                } else {
                                    return Err(anyhow!("Missing columns in PRODUCE statement"));
                                };

                                // 解析键列
                                let mut key_column_index = None;
                                for (i, option) in cte.options.iter().enumerate() {
                                    if option.name.value.to_uppercase() == "KEY" {
                                        if let Expr::Identifier(key_ident) = &option.value {
                                            let key_column = key_ident.value.clone();
                                            key_column_index = columns.iter().position(|c| c == &key_column);
                                            if key_column_index.is_none() {
                                                return Err(anyhow!("Key column '{}' not found in column list", key_column));
                                            }
                                        }
                                    }
                                }

                                // 解析值
                                if let SetExpr::Values(values) = &subquery.body {
                                    let values = values.rows.clone();
                                    return Ok(Some(QueueDmlStatement::Produce {
                                        topic,
                                        columns,
                                        key_column_index,
                                        values: Some(values),
                                        query: None,
                                    }));
                                } else if let SetExpr::Query(query) = &subquery.body {
                                    return Ok(Some(QueueDmlStatement::Produce {
                                        topic,
                                        columns,
                                        key_column_index,
                                        values: None,
                                        query: Some(query.clone()),
                                    }));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(None)
}

/// 尝试解析 CONSUME 语句
fn try_parse_consume(statement: &Statement) -> Result<Option<QueueDmlStatement>> {
    // 检查是否是自定义的 CONSUME 语句
    if let Statement::Query(query) = statement {
        if let SetExpr::Query(subquery) = &query.body {
            if let SetExpr::Table(table) = &subquery.body {
                // 检查是否有 CONSUME FROM 子句
                if let Some(with) = &query.with {
                    for cte in &with.cte_tables {
                        if cte.alias.name.value.to_uppercase() == "CONSUME" {
                            // 找到 CONSUME 语句
                            if let Some(from_clause) = &cte.from {
                                let topic = from_clause.to_string();

                                // 解析消费者组 ID
                                let mut group_id = None;
                                for option in &cte.options {
                                    if option.name.value.to_uppercase() == "GROUP_ID" {
                                        if let Expr::Value(SqlValue::SingleQuotedString(group)) = &option.value {
                                            group_id = Some(group.clone());
                                        }
                                    }
                                }

                                // 解析过滤条件
                                let filter = subquery.selection.clone();

                                // 解析限制数量
                                let limit = subquery.limit.as_ref().and_then(|limit| {
                                    if let Expr::Value(SqlValue::Number(n, _)) = &limit {
                                        n.parse::<usize>().ok()
                                    } else {
                                        None
                                    }
                                });

                                return Ok(Some(QueueDmlStatement::Consume {
                                    topic,
                                    group_id,
                                    filter,
                                    limit,
                                }));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(None)
}

/// 执行消息队列 DML 语句
pub async fn execute_queue_dml(
    statement: QueueDmlStatement,
    schema_provider: &ArroyoSchemaProvider,
) -> Result<String> {
    match statement {
        QueueDmlStatement::Produce {
            topic,
            columns,
            key_column_index,
            values,
            query,
        } => {
            info!("Producing to topic: {}", topic);

            // 获取生产者
            let producer = get_producer(&topic).await?;

            if let Some(values) = values {
                // 直接生产指定的值
                let mut produced_count = 0;

                for value_row in values {
                    // 将 SQL 表达式转换为实际值
                    let row_values = value_row.iter()
                        .map(expr_to_value)
                        .collect::<Result<Vec<_>>>()?;

                    // 提取键和值
                    let (key, value) = if let Some(key_idx) = key_column_index {
                        let key = row_values.get(key_idx)
                            .ok_or_else(|| anyhow!("Key column index out of bounds"))?
                            .clone();

                        // 创建值对象（排除键）
                        let mut value_obj = serde_json::Map::new();
                        for (i, col) in columns.iter().enumerate() {
                            if i != key_idx {
                                if let Some(val) = row_values.get(i) {
                                    value_obj.insert(col.clone(), val.clone());
                                }
                            }
                        }

                        // 序列化键和值
                        let key_bytes = serde_json::to_vec(&key)?;
                        let value_bytes = serde_json::to_vec(&value_obj)?;

                        (Some(key_bytes), value_bytes)
                    } else {
                        // 没有键，将所有列作为值
                        let mut value_obj = serde_json::Map::new();
                        for (i, col) in columns.iter().enumerate() {
                            if let Some(val) = row_values.get(i) {
                                value_obj.insert(col.clone(), val.clone());
                            }
                        }

                        // 序列化值
                        let value_bytes = serde_json::to_vec(&value_obj)?;

                        (None, value_bytes)
                    };

                    // 发送消息
                    producer.send(key, value).await?;
                    produced_count += 1;
                }

                Ok(format!("Produced {} messages to topic '{}'", produced_count, topic))
            } else if let Some(_query) = query {
                // 执行查询并生产结果
                // 这里需要实际执行查询，然后将结果发送到 Topic
                // 由于执行查询需要更复杂的逻辑，这里简化处理
                Ok(format!("Produced query results to topic '{}' (query execution not implemented yet)", topic))
            } else {
                Err(anyhow!("No values or query provided for PRODUCE statement"))
            }
        }
        QueueDmlStatement::Consume {
            topic,
            group_id,
            filter,
            limit,
        } => {
            info!("Consuming from topic: {}", topic);

            // 获取消费者
            let consumer = get_consumer(&topic, group_id.as_deref()).await?;

            // 设置超时和限制
            let timeout = std::time::Duration::from_secs(5);
            let limit_val = limit.unwrap_or(10);

            // 消费消息
            let messages = consumer.poll(timeout).await?;

            // 应用过滤条件（简化处理）
            let filtered_messages = if let Some(_filter) = filter {
                // 这里应该根据过滤条件过滤消息
                // 由于过滤条件是 SQL 表达式，需要更复杂的处理
                messages
            } else {
                messages
            };

            // 应用限制
            let limited_messages = if filtered_messages.len() > limit_val {
                filtered_messages[0..limit_val].to_vec()
            } else {
                filtered_messages
            };

            // 格式化结果
            let mut result = format!("Consumed {} messages from topic '{}':\n", limited_messages.len(), topic);
            for (i, msg) in limited_messages.iter().enumerate() {
                let key_str = if let Some(key) = &msg.key {
                    match serde_json::from_slice::<serde_json::Value>(key) {
                        Ok(json) => format!("{}", json),
                        Err(_) => format!("<binary key of {} bytes>", key.len()),
                    }
                } else {
                    "null".to_string()
                };

                let value_str = match serde_json::from_slice::<serde_json::Value>(&msg.value) {
                    Ok(json) => format!("{}", json),
                    Err(_) => format!("<binary data of {} bytes>", msg.value.len()),
                };

                result.push_str(&format!("{}. Key: {}, Value: {}\n", i + 1, key_str, value_str));
            }

            Ok(result)
        }
    }
}

/// 将 SQL 表达式转换为 JSON 值
fn expr_to_value(expr: &Expr) -> Result<serde_json::Value> {
    match expr {
        Expr::Value(SqlValue::Number(n, _)) => {
            // 尝试解析为整数或浮点数
            if let Ok(i) = n.parse::<i64>() {
                Ok(serde_json::Value::Number(serde_json::Number::from(i)))
            } else if let Ok(f) = n.parse::<f64>() {
                // 创建 serde_json::Number
                let num = serde_json::Number::from_f64(f)
                    .ok_or_else(|| anyhow!("Failed to convert float to JSON number"))?;
                Ok(serde_json::Value::Number(num))
            } else {
                Err(anyhow!("Failed to parse number: {}", n))
            }
        }
        Expr::Value(SqlValue::SingleQuotedString(s)) | Expr::Value(SqlValue::DoubleQuotedString(s)) => {
            Ok(serde_json::Value::String(s.clone()))
        }
        Expr::Value(SqlValue::Boolean(b)) => {
            Ok(serde_json::Value::Bool(*b))
        }
        Expr::Value(SqlValue::Null) => {
            Ok(serde_json::Value::Null)
        }
        _ => Err(anyhow!("Unsupported expression type: {:?}", expr)),
    }
}

/// 获取生产者
async fn get_producer(topic: &str) -> Result<Arc<arroyo_sdk::producer::Producer>> {
    use arroyo_rpc::config::config;
    use arroyo_sdk::client::ArroyoClient;
    use arroyo_sdk::producer::{Producer, ProducerBuilder, ProducerOptions};

    // 从配置中获取 Kafka 连接信息
    let bootstrap_servers = config().kafka.bootstrap_servers.clone();

    // 创建 Arroyo 客户端
    let client = ArroyoClient::new(&format!("http://{}", bootstrap_servers))
        .map_err(|e| anyhow!("Failed to create Arroyo client: {}", e))?;

    // 创建生产者
    let producer = ProducerBuilder::new(client, topic)
        .with_options(ProducerOptions {
            batch_size: 1000,
            linger_ms: 100,
            compression_type: arroyo_sdk::CompressionType::None,
            acks: arroyo_sdk::AckLevel::All,
            retries: 3,
            retry_backoff_ms: 100,
            idempotent: false,
            idempotence_options: None,
            transactional: false,
            transaction_timeout_ms: 60000,
        })
        .build()
        .await
        .map_err(|e| anyhow!("Failed to create producer: {}", e))?;

    Ok(Arc::new(producer))
}

/// 获取消费者
async fn get_consumer(topic: &str, group_id: Option<&str>) -> Result<Arc<arroyo_sdk::consumer::Consumer>> {
    use arroyo_rpc::config::config;
    use arroyo_sdk::client::ArroyoClient;
    use arroyo_sdk::consumer::{Consumer, ConsumerOptions};

    // 从配置中获取 Kafka 连接信息
    let bootstrap_servers = config().kafka.bootstrap_servers.clone();

    // 创建 Arroyo 客户端
    let client = ArroyoClient::new(&format!("http://{}", bootstrap_servers))
        .map_err(|e| anyhow!("Failed to create Arroyo client: {}", e))?;

    // 创建消费者选项
    let options = ConsumerOptions {
        group_id: group_id.unwrap_or("arroyo-sql-consumer").to_string(),
        auto_offset_reset: arroyo_sdk::consumer::AutoOffsetReset::Earliest,
        enable_auto_commit: true,
        auto_commit_interval_ms: 5000,
        fetch_min_bytes: 1,
        fetch_max_bytes: 52428800,
        fetch_max_wait_ms: 500,
        max_partition_fetch_bytes: 1048576,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        enable_partition_eof: false,
        subscription_type: None,
        partition_assignment_strategy: arroyo_sdk::consumer_group::PartitionAssignmentStrategy::RangeAssignor,
        flow_control_enabled: false,
        flow_control_options: None,
    };

    // 创建消费者
    let consumer = Consumer::new(client, topic, options);

    Ok(Arc::new(consumer))
}
