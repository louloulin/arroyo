use crate::error::ApiError;
use arroyo_connectors::topic::{TopicAdmin, TopicExport, TopicHealthChecker};
use arroyo_rpc::api_types::topics::{
    CreateTopicRequest, TopicDetailsResponse, TopicExportRequest, TopicExportResponse,
    TopicHealthCheckRequest, TopicHealthCheckResponse, TopicImportRequest, TopicImportResponse,
    TopicListResponse, UpdateTopicRequest,
};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use std::sync::Arc;
use tracing::{error, info};

/// Topic 控制器
pub struct TopicController {
    /// Topic 管理器
    admin: Arc<TopicAdmin>,
    /// Topic 健康检查器
    health_checker: Arc<tokio::sync::Mutex<TopicHealthChecker>>,
}

impl TopicController {
    /// 创建 Topic 控制器
    pub fn new(server: &str) -> Result<Self, ApiError> {
        let admin = TopicAdmin::new(server).map_err(|e| {
            error!("Failed to create TopicAdmin: {}", e);
            ApiError::internal_error(format!("Failed to create TopicAdmin: {}", e))
        })?;

        let health_checker = TopicHealthChecker::new(server, Some(30)).map_err(|e| {
            error!("Failed to create TopicHealthChecker: {}", e);
            ApiError::internal_error(format!("Failed to create TopicHealthChecker: {}", e))
        })?;

        Ok(Self {
            admin: Arc::new(admin),
            health_checker: Arc::new(tokio::sync::Mutex::new(health_checker)),
        })
    }

    /// 创建 Topic
    pub async fn create_topic(
        &self,
        request: CreateTopicRequest,
        user_id: Option<&str>,
    ) -> Result<impl IntoResponse, ApiError> {
        info!("Creating topic: {}", request.config.name);

        let topic_info = self.admin.create_topic(&request.config, user_id).await.map_err(|e| {
            error!("Failed to create topic: {}", e);
            ApiError::bad_request(format!("Failed to create topic: {}", e))
        })?;

        Ok((StatusCode::CREATED, Json(topic_info)))
    }

    /// 删除 Topic
    pub async fn delete_topic(
        &self,
        name: String,
        user_id: Option<&str>,
    ) -> Result<impl IntoResponse, ApiError> {
        info!("Deleting topic: {}", name);

        self.admin.delete_topic(&name, user_id).await.map_err(|e| {
            error!("Failed to delete topic: {}", e);
            ApiError::bad_request(format!("Failed to delete topic: {}", e))
        })?;

        Ok(StatusCode::NO_CONTENT)
    }

    /// 更新 Topic
    pub async fn update_topic(
        &self,
        name: String,
        request: UpdateTopicRequest,
        user_id: Option<&str>,
    ) -> Result<impl IntoResponse, ApiError> {
        info!("Updating topic: {}", name);

        // 确保路径参数和请求体中的名称一致
        if name != request.config.name {
            return Err(ApiError::bad_request(format!(
                "Topic name in path ({}) does not match name in request body ({})",
                name, request.config.name
            )));
        }

        let topic_info = self.admin.update_topic(&request.config, user_id).await.map_err(|e| {
            error!("Failed to update topic: {}", e);
            ApiError::bad_request(format!("Failed to update topic: {}", e))
        })?;

        Ok((StatusCode::OK, Json(topic_info)))
    }

    /// 获取 Topic 列表
    pub async fn list_topics(
        &self,
        user_id: Option<&str>,
    ) -> Result<impl IntoResponse, ApiError> {
        info!("Listing topics");

        let topics = self.admin.list_topics(user_id).await.map_err(|e| {
            error!("Failed to list topics: {}", e);
            ApiError::internal_error(format!("Failed to list topics: {}", e))
        })?;

        Ok((
            StatusCode::OK,
            Json(TopicListResponse { topics }),
        ))
    }

    /// 获取 Topic 详情
    pub async fn get_topic_details(
        &self,
        name: String,
        user_id: Option<&str>,
    ) -> Result<impl IntoResponse, ApiError> {
        info!("Getting topic details: {}", name);

        let topic = self.admin.get_topic_details(&name, user_id).await.map_err(|e| {
            error!("Failed to get topic details: {}", e);
            ApiError::bad_request(format!("Failed to get topic details: {}", e))
        })?;

        Ok((
            StatusCode::OK,
            Json(TopicDetailsResponse { topic }),
        ))
    }

    /// 检查 Topic 健康状态
    pub async fn check_topic_health(
        &self,
        request: TopicHealthCheckRequest,
    ) -> Result<impl IntoResponse, ApiError> {
        let force_refresh = request.force_refresh;

        let mut health_checker = self.health_checker.lock().await;

        let topics = match request.topics {
            Some(topic_names) => {
                info!("Checking health for {} topics", topic_names.len());
                health_checker.check_topics_health(&topic_names, force_refresh).await
            }
            None => {
                info!("Checking health for all topics");
                health_checker.check_all_topics_health(force_refresh).await
            }
        }.map_err(|e| {
            error!("Failed to check topic health: {}", e);
            ApiError::internal_error(format!("Failed to check topic health: {}", e))
        })?;

        Ok((
            StatusCode::OK,
            Json(TopicHealthCheckResponse { topics }),
        ))
    }

    /// 导出 Topic 配置
    pub async fn export_topics(
        &self,
        request: TopicExportRequest,
        user_id: Option<&str>,
    ) -> Result<impl IntoResponse, ApiError> {
        info!("Exporting topics");

        // 获取所有 Topic
        let all_topics = self.admin.list_topics(user_id).await.map_err(|e| {
            error!("Failed to list topics: {}", e);
            ApiError::internal_error(format!("Failed to list topics: {}", e))
        })?;

        // 过滤 Topic
        let topics = match &request.topics {
            Some(topic_names) => {
                all_topics.into_iter()
                    .filter(|t| topic_names.contains(&t.name))
                    .collect::<Vec<_>>()
            }
            None => all_topics,
        };

        // 创建导出对象
        let export = TopicExport::from_topic_infos(&topics);

        // 根据格式导出
        let (content, format) = match request.format.as_str() {
            "yaml" => (export.to_yaml_string().map_err(|e| {
                error!("Failed to export topics to YAML: {}", e);
                ApiError::internal_error(format!("Failed to export topics to YAML: {}", e))
            })?, "yaml".to_string()),
            _ => (export.to_json_string().map_err(|e| {
                error!("Failed to export topics to JSON: {}", e);
                ApiError::internal_error(format!("Failed to export topics to JSON: {}", e))
            })?, "json".to_string()),
        };

        // 如果需要下载文件，返回文件响应
        if request.download {
            let filename = format!("topics_export_{}.{}",
                chrono::Utc::now().format("%Y%m%d%H%M%S"),
                format);

            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, match format.as_str() {
                    "yaml" => "application/yaml",
                    _ => "application/json",
                })
                .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
                .body(Body::from(content))
                .map_err(|e| {
                    error!("Failed to build response: {}", e);
                    ApiError::internal_error(format!("Failed to build response: {}", e))
                })?;

            Ok(response)
        } else {
            // 否则返回 JSON 响应
            let export_response = TopicExportResponse { content, format };
            let json_body = serde_json::to_string(&export_response).unwrap();

            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json_body))
                .map_err(|e| {
                    error!("Failed to build response: {}", e);
                    ApiError::internal_error(format!("Failed to build response: {}", e))
                })?;

            Ok(response)
        }
    }

    /// 导入 Topic 配置
    pub async fn import_topics(
        &self,
        request: TopicImportRequest,
        user_id: Option<&str>,
    ) -> Result<impl IntoResponse, ApiError> {
        info!("Importing topics");

        // 解析导入内容
        let export = match request.format.as_str() {
            "yaml" => TopicExport::from_yaml_string(&request.content).map_err(|e| {
                error!("Failed to parse YAML: {}", e);
                ApiError::bad_request(format!("Failed to parse YAML: {}", e))
            })?,
            _ => TopicExport::from_json_string(&request.content).map_err(|e| {
                error!("Failed to parse JSON: {}", e);
                ApiError::bad_request(format!("Failed to parse JSON: {}", e))
            })?,
        };

        let mut imported_topics = Vec::new();
        let mut skipped_topics = Vec::new();

        // 导入 Topic
        for config in &export.topics {
            // 检查 Topic 是否已存在
            let exists = self.admin.topic_exists(&config.name).await.map_err(|e| {
                error!("Failed to check if topic exists: {}", e);
                ApiError::internal_error(format!("Failed to check if topic exists: {}", e))
            })?;

            if exists {
                info!("Topic '{}' already exists, skipping", config.name);
                skipped_topics.push(config.name.clone());
                continue;
            }

            // 创建 Topic
            match self.admin.create_topic(config, user_id).await {
                Ok(_) => {
                    info!("Created topic: {}", config.name);
                    imported_topics.push(config.name.clone());
                }
                Err(e) => {
                    error!("Failed to create topic {}: {}", config.name, e);
                    return Err(ApiError::bad_request(format!("Failed to create topic {}: {}", config.name, e)));
                }
            }
        }

        Ok((
            StatusCode::OK,
            Json(TopicImportResponse {
                imported_count: imported_topics.len() as i32,
                skipped_count: skipped_topics.len() as i32,
                imported_topics,
                skipped_topics,
            }),
        ))
    }
}

/// Create a new topic
pub async fn create_topic(
    State(state): State<crate::rest::AppState>,
    Json(request): Json<CreateTopicRequest>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.create_topic(request, None).await
}

/// Delete a topic
pub async fn delete_topic(
    State(state): State<crate::rest::AppState>,
    Path(name): Path<String>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.delete_topic(name, None).await
}

/// Update a topic
pub async fn update_topic(
    State(state): State<crate::rest::AppState>,
    Path(name): Path<String>,
    Json(request): Json<UpdateTopicRequest>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.update_topic(name, request, None).await
}

/// List all topics
pub async fn list_topics(
    State(state): State<crate::rest::AppState>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.list_topics(None).await
}

/// Get topic details
pub async fn get_topic_details(
    State(state): State<crate::rest::AppState>,
    Path(name): Path<String>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.get_topic_details(name, None).await
}

/// Check topic health
pub async fn check_topic_health(
    State(state): State<crate::rest::AppState>,
    Json(request): Json<TopicHealthCheckRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.check_topic_health(request).await
}

/// Export topics
pub async fn export_topics(
    State(state): State<crate::rest::AppState>,
    Json(request): Json<TopicExportRequest>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.export_topics(request, None).await
}

/// Import topics
pub async fn import_topics(
    State(state): State<crate::rest::AppState>,
    Json(request): Json<TopicImportRequest>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.import_topics(request, None).await
}
