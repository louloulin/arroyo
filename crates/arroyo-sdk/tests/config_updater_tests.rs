use arroyo_sdk::config_updater::{
    ConfigUpdater, ConfigUpdaterConfig, ConfigUpdateEvent, ConfigUpdateEventType, ConfigUpdateStrategy,
};
use arroyo_sdk::connection::{ConnectionConfig, PooledArroyoClient};
use arroyo_sdk::error::Result;
use mockito::Server;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[tokio::test]
async fn test_config_updater_creation() -> Result<()> {
    // 创建模拟服务器
    let mut server = Server::new_async().await;
    
    // 创建配置更新器配置
    let updater_config = ConfigUpdaterConfig {
        enabled: true,
        check_interval_secs: 1,
        update_strategy: ConfigUpdateStrategy::Immediate,
        config_endpoint: "/api/client/config".to_string(),
        retry_attempts: 3,
        retry_interval_ms: 500,
        enable_notifications: true,
        custom_params: HashMap::new(),
    };
    
    // 创建连接配置
    let mut config = ConnectionConfig::default();
    config.config_updater_config = Some(updater_config);
    
    // 创建客户端
    let client = PooledArroyoClient::new(&server.url(), Some(config))?;
    
    // 验证配置更新器已创建
    assert!(client.get_config_updater().is_some());
    
    Ok(())
}

#[tokio::test]
async fn test_config_update_check() -> Result<()> {
    // 创建模拟服务器
    let mut server = Server::new_async().await;
    
    // 设置模拟响应
    let _m = server
        .mock("GET", "/api/client/config")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"version":"1.0","connection_pool":{"max_connections":20}}"#)
        .expect(1)
        .create_async()
        .await;
    
    // 创建配置更新器配置
    let updater_config = ConfigUpdaterConfig {
        enabled: true,
        check_interval_secs: 1,
        update_strategy: ConfigUpdateStrategy::Immediate,
        config_endpoint: "/api/client/config".to_string(),
        retry_attempts: 3,
        retry_interval_ms: 500,
        enable_notifications: true,
        custom_params: HashMap::new(),
    };
    
    // 创建连接配置
    let mut config = ConnectionConfig::default();
    config.config_updater_config = Some(updater_config);
    
    // 创建客户端
    let client = PooledArroyoClient::new(&server.url(), Some(config))?;
    
    // 手动触发配置更新检查
    let updated = client.check_config_updates().await?;
    assert!(updated);
    
    // 验证配置版本
    let version = client.get_config_version().await?;
    assert_eq!(version, "1.0");
    
    Ok(())
}

#[tokio::test]
async fn test_config_update_listener() -> Result<()> {
    // 创建模拟服务器
    let mut server = Server::new_async().await;
    
    // 设置模拟响应
    let _m = server
        .mock("GET", "/api/client/config")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"version":"2.0","connection_pool":{"max_connections":30}}"#)
        .expect(1)
        .create_async()
        .await;
    
    // 创建配置更新器配置
    let updater_config = ConfigUpdaterConfig {
        enabled: true,
        check_interval_secs: 1,
        update_strategy: ConfigUpdateStrategy::NotifyOnly,
        config_endpoint: "/api/client/config".to_string(),
        retry_attempts: 3,
        retry_interval_ms: 500,
        enable_notifications: true,
        custom_params: HashMap::new(),
    };
    
    // 创建连接配置
    let mut config = ConnectionConfig::default();
    config.config_updater_config = Some(updater_config);
    
    // 创建客户端
    let client = PooledArroyoClient::new(&server.url(), Some(config))?;
    
    // 创建事件接收器
    let event_received = Arc::new(Mutex::new(false));
    let event_type = Arc::new(Mutex::new(None));
    
    // 添加配置更新监听器
    let event_received_clone = event_received.clone();
    let event_type_clone = event_type.clone();
    client.add_config_update_listener(Box::new(move |event| {
        let event_received_clone = event_received_clone.clone();
        let event_type_clone = event_type_clone.clone();
        tokio::spawn(async move {
            let mut received = event_received_clone.lock().await;
            *received = true;
            
            let mut type_value = event_type_clone.lock().await;
            *type_value = Some(event.event_type);
        });
    })).await?;
    
    // 手动触发配置更新检查
    let updated = client.check_config_updates().await?;
    assert!(updated);
    
    // 等待事件处理
    sleep(Duration::from_millis(500)).await;
    
    // 验证事件已接收
    let received = *event_received.lock().await;
    assert!(received);
    
    // 验证事件类型
    let type_value = event_type.lock().await.clone();
    assert!(type_value.is_some());
    assert_eq!(type_value.unwrap(), ConfigUpdateEventType::Global);
    
    Ok(())
}

#[tokio::test]
async fn test_config_update_delayed_strategy() -> Result<()> {
    // 创建模拟服务器
    let mut server = Server::new_async().await;
    
    // 设置模拟响应
    let _m = server
        .mock("GET", "/api/client/config")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"version":"3.0","connection_pool":{"max_connections":40}}"#)
        .expect(1)
        .create_async()
        .await;
    
    // 创建配置更新器配置
    let updater_config = ConfigUpdaterConfig {
        enabled: true,
        check_interval_secs: 1,
        update_strategy: ConfigUpdateStrategy::Delayed,
        config_endpoint: "/api/client/config".to_string(),
        retry_attempts: 3,
        retry_interval_ms: 500,
        enable_notifications: true,
        custom_params: HashMap::new(),
    };
    
    // 创建连接配置
    let mut config = ConnectionConfig::default();
    config.config_updater_config = Some(updater_config);
    
    // 创建客户端
    let client = PooledArroyoClient::new(&server.url(), Some(config))?;
    
    // 手动触发配置更新检查
    let updated = client.check_config_updates().await?;
    assert!(updated);
    
    // 验证配置版本
    let version = client.get_config_version().await?;
    assert_eq!(version, "3.0");
    
    Ok(())
}

#[tokio::test]
async fn test_config_update_error_handling() -> Result<()> {
    // 创建模拟服务器
    let mut server = Server::new_async().await;
    
    // 设置模拟响应 - 错误
    let _m = server
        .mock("GET", "/api/client/config")
        .with_status(500)
        .with_header("content-type", "application/json")
        .with_body(r#"{"error": "server error"}"#)
        .expect(1)
        .create_async()
        .await;
    
    // 创建配置更新器配置
    let updater_config = ConfigUpdaterConfig {
        enabled: true,
        check_interval_secs: 1,
        update_strategy: ConfigUpdateStrategy::Immediate,
        config_endpoint: "/api/client/config".to_string(),
        retry_attempts: 1,
        retry_interval_ms: 100,
        enable_notifications: true,
        custom_params: HashMap::new(),
    };
    
    // 创建连接配置
    let mut config = ConnectionConfig::default();
    config.config_updater_config = Some(updater_config);
    
    // 创建客户端
    let client = PooledArroyoClient::new(&server.url(), Some(config))?;
    
    // 手动触发配置更新检查 - 应该失败但不会崩溃
    let result = client.check_config_updates().await;
    assert!(result.is_err());
    
    Ok(())
}
