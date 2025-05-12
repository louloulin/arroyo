use crate::error::ApiError;
use arroyo_connectors::topic::{TopicAdmin, TopicHealthChecker};
use arroyo_rpc::api_types::topics::{
    CreateTopicRequest, DeleteTopicRequest, TopicDetailsResponse, TopicHealthCheckRequest,
    TopicHealthCheckResponse, TopicListResponse, UpdateTopicRequest,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
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
}

/// Topic 路由处理函数
pub async fn create_topic(
    State(state): State<crate::rest::AppState>,
    Json(request): Json<CreateTopicRequest>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.create_topic(request, None).await
}

pub async fn delete_topic(
    State(state): State<crate::rest::AppState>,
    Path(name): Path<String>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.delete_topic(name, None).await
}

pub async fn update_topic(
    State(state): State<crate::rest::AppState>,
    Path(name): Path<String>,
    Json(request): Json<UpdateTopicRequest>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.update_topic(name, request, None).await
}

pub async fn list_topics(
    State(state): State<crate::rest::AppState>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.list_topics(None).await
}

pub async fn get_topic_details(
    State(state): State<crate::rest::AppState>,
    Path(name): Path<String>,
    // 在实际应用中，这里应该从认证中间件获取用户 ID
    // 这里简化处理，使用 None 表示未认证用户
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.get_topic_details(name, None).await
}

pub async fn check_topic_health(
    State(state): State<crate::rest::AppState>,
    Json(request): Json<TopicHealthCheckRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state.topic_controller.check_topic_health(request).await
}
