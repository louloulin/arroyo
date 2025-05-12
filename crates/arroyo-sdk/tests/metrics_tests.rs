use arroyo_sdk::metrics::{ClientMetricsConfig, ClientMetricsManager};
use arroyo_sdk::metrics_collectors::{RequestMetricsCollector, CacheMetricsCollector};
use arroyo_sdk::connection::{ConnectionConfig, PooledArroyoClient};
use arroyo_sdk::error::Result;
use mockito::Server;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_client_with_metrics() -> Result<()> {
    // 创建模拟服务器
    let mut server = Server::new_async().await;
    
    // 设置模拟响应
    let _m = server
        .mock("GET", "/api/metrics-test")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"message": "success"}"#)
        .expect(1)
        .create_async()
        .await;
    
    // 创建指标配置
    let metrics_config = ClientMetricsConfig {
        enabled: true,
        prefix: "test_client".to_string(),
        labels: HashMap::new(),
        collection_interval_secs: 1,
        enable_histograms: true,
        histogram_buckets: vec![0.001, 0.01, 0.1, 1.0],
        enable_export: false,
        export_address: None,
        export_interval_secs: 60,
        enable_debug_logs: true,
    };
    
    // 创建连接配置
    let mut config = ConnectionConfig::default();
    config.metrics_config = Some(metrics_config);
    
    // 创建客户端
    let client = PooledArroyoClient::new(&server.url(), Some(config))?;
    
    // 发送请求
    let response = client.get("/api/metrics-test").await?;
    assert_eq!(response.status(), 200);
    
    // 等待指标收集
    sleep(Duration::from_secs(2)).await;
    
    // 验证响应
    let body: serde_json::Value = client.handle_response(response).await?;
    assert_eq!(body["message"], "success");
    
    Ok(())
}

#[tokio::test]
async fn test_metrics_manager() -> Result<()> {
    // 创建指标配置
    let metrics_config = ClientMetricsConfig {
        enabled: true,
        prefix: "test_metrics".to_string(),
        labels: HashMap::new(),
        collection_interval_secs: 1,
        enable_histograms: true,
        histogram_buckets: vec![0.001, 0.01, 0.1, 1.0],
        enable_export: false,
        export_address: None,
        export_interval_secs: 60,
        enable_debug_logs: true,
    };
    
    // 创建指标管理器
    let metrics_manager = ClientMetricsManager::new(metrics_config);
    
    // 增加计数器
    metrics_manager.increment_int_counter(
        "test_counter",
        "Test counter",
        &["label1", "label2"],
        1,
    ).await?;
    
    // 设置仪表
    metrics_manager.set_int_gauge(
        "test_gauge",
        "Test gauge",
        &["label1", "label2"],
        100,
    ).await?;
    
    // 观察直方图
    metrics_manager.observe_histogram(
        "test_histogram",
        "Test histogram",
        &["label1", "label2"],
        0.5,
    ).await?;
    
    // 等待指标收集
    sleep(Duration::from_secs(2)).await;
    
    Ok(())
}

#[tokio::test]
async fn test_request_metrics_collector() -> Result<()> {
    // 创建请求指标收集器
    let collector = RequestMetricsCollector::new();
    
    // 记录请求
    collector.record_request("GET", "/api/test", Duration::from_millis(100), true).await;
    collector.record_request("POST", "/api/test", Duration::from_millis(200), true).await;
    collector.record_request("GET", "/api/test", Duration::from_millis(300), false).await;
    
    // 收集指标
    let metrics = collector.collect();
    
    // 验证指标
    assert!(metrics.contains_key("count_GET_/api/test"));
    assert!(metrics.contains_key("count_POST_/api/test"));
    assert!(metrics.contains_key("errors_GET_/api/test"));
    assert!(metrics.contains_key("latency_GET_/api/test"));
    assert!(metrics.contains_key("latency_POST_/api/test"));
    
    Ok(())
}

#[tokio::test]
async fn test_cache_metrics_collector() -> Result<()> {
    // 创建缓存指标收集器
    let collector = CacheMetricsCollector::new();
    
    // 记录缓存命中和未命中
    collector.record_cache_hit("/api/test").await;
    collector.record_cache_hit("/api/test").await;
    collector.record_cache_miss("/api/test").await;
    collector.update_cache_size("/api/test", 1024).await;
    
    // 收集指标
    let metrics = collector.collect();
    
    // 验证指标
    assert!(metrics.contains_key("hits_/api/test"));
    assert!(metrics.contains_key("misses_/api/test"));
    assert!(metrics.contains_key("size_/api/test"));
    assert!(metrics.contains_key("hit_rate_/api/test"));
    
    // 验证命中率
    let hit_rate = metrics.get("hit_rate_/api/test").unwrap();
    assert!(*hit_rate > 0.0 && *hit_rate < 1.0);
    
    Ok(())
}

#[tokio::test]
async fn test_client_with_metrics_error_handling() -> Result<()> {
    // 创建模拟服务器
    let mut server = Server::new_async().await;
    
    // 设置模拟响应 - 错误
    let _m = server
        .mock("GET", "/api/error")
        .with_status(500)
        .with_header("content-type", "application/json")
        .with_body(r#"{"error": "server error"}"#)
        .expect(1)
        .create_async()
        .await;
    
    // 创建指标配置
    let metrics_config = ClientMetricsConfig {
        enabled: true,
        prefix: "test_error".to_string(),
        labels: HashMap::new(),
        collection_interval_secs: 1,
        enable_histograms: true,
        histogram_buckets: vec![0.001, 0.01, 0.1, 1.0],
        enable_export: false,
        export_address: None,
        export_interval_secs: 60,
        enable_debug_logs: true,
    };
    
    // 创建连接配置
    let mut config = ConnectionConfig::default();
    config.metrics_config = Some(metrics_config);
    
    // 创建客户端
    let client = PooledArroyoClient::new(&server.url(), Some(config))?;
    
    // 发送请求 - 应该失败
    let result = client.get("/api/error").await;
    assert!(result.is_err());
    
    // 等待指标收集
    sleep(Duration::from_secs(2)).await;
    
    Ok(())
}
