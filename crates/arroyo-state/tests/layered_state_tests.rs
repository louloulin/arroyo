use anyhow::Result;
use arroyo_state::layered_state::{
    HotDataStrategy, LayeredStateBackend, LayeredStateConfig, StorageTier,
};
use arroyo_state::BackingStore;
use arroyo_storage::StorageProvider;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::sleep;

/// 创建测试用的分层状态后端
async fn create_test_backend() -> Result<LayeredStateBackend> {
    // 创建临时目录
    let temp_dir = tempdir()?;
    
    // 创建配置
    let config = LayeredStateConfig {
        enable_memory_tier: true,
        memory_tier_max_size: 1024 * 1024, // 1MB
        enable_local_disk_tier: true,
        local_disk_path: temp_dir.path().to_path_buf(),
        local_disk_max_size: 10 * 1024 * 1024, // 10MB
        remote_storage: Arc::new(StorageProvider::dummy()),
        cache_expiration: Duration::from_secs(60),
        hot_data_strategy: HotDataStrategy::Weighted,
        hot_data_ratio: 0.2,
        enable_prefetching: true,
        prefetch_threshold: 3,
        enable_compression: true,
        compression_level: 6,
    };
    
    // 创建分层状态后端
    Ok(LayeredStateBackend::new(config))
}

#[tokio::test]
async fn test_basic_operations() -> Result<()> {
    // 创建分层状态后端
    let backend = create_test_backend().await?;
    
    // 测试基本的 put 和 get 操作
    let key = "test_key";
    let value = vec![1, 2, 3, 4, 5];
    
    // 写入数据
    backend.put(key, value.clone()).await?;
    
    // 读取数据
    let result = backend.get(key).await?;
    assert!(result.is_some());
    assert_eq!(result.unwrap(), value);
    
    // 删除数据
    backend.delete(key).await?;
    
    // 验证数据已删除
    let result = backend.get(key).await?;
    assert!(result.is_none());
    
    Ok(())
}

#[tokio::test]
async fn test_cache_expiration() -> Result<()> {
    // 创建临时目录
    let temp_dir = tempdir()?;
    
    // 创建配置，设置非常短的过期时间
    let config = LayeredStateConfig {
        enable_memory_tier: true,
        memory_tier_max_size: 1024 * 1024, // 1MB
        enable_local_disk_tier: true,
        local_disk_path: temp_dir.path().to_path_buf(),
        local_disk_max_size: 10 * 1024 * 1024, // 10MB
        remote_storage: Arc::new(StorageProvider::dummy()),
        cache_expiration: Duration::from_millis(100), // 100ms
        hot_data_strategy: HotDataStrategy::Weighted,
        hot_data_ratio: 0.2,
        enable_prefetching: false,
        prefetch_threshold: 3,
        enable_compression: false,
        compression_level: 6,
    };
    
    // 创建分层状态后端
    let backend = LayeredStateBackend::new(config);
    
    // 写入数据
    let key = "test_key";
    let value = vec![1, 2, 3, 4, 5];
    backend.put(key, value.clone()).await?;
    
    // 立即读取数据，应该从内存缓存中获取
    let result = backend.get(key).await?;
    assert!(result.is_some());
    assert_eq!(result.unwrap(), value);
    
    // 等待缓存过期
    sleep(Duration::from_millis(200)).await;
    
    // 强制清理缓存
    backend.cleanup().await?;
    
    // 再次读取数据，应该从远程存储中获取
    let result = backend.get(key).await?;
    assert!(result.is_some());
    assert_eq!(result.unwrap(), value);
    
    // 验证统计信息
    let stats = backend.get_stats().await;
    assert!(stats.memory_hits > 0);
    
    Ok(())
}

#[tokio::test]
async fn test_compression() -> Result<()> {
    // 创建分层状态后端，启用压缩
    let backend = create_test_backend().await?;
    
    // 创建可压缩的数据（重复模式）
    let value = vec![0; 10000]; // 10KB 的零数据，应该可以很好地压缩
    
    // 写入数据
    let key = "test_key";
    backend.put(key, value.clone()).await?;
    
    // 读取数据
    let result = backend.get(key).await?;
    assert!(result.is_some());
    assert_eq!(result.unwrap(), value);
    
    // 验证统计信息
    let stats = backend.get_stats().await;
    assert!(stats.compression_savings > 0);
    
    Ok(())
}

#[tokio::test]
async fn test_hot_data() -> Result<()> {
    // 创建分层状态后端
    let backend = create_test_backend().await?;
    
    // 创建多个键值对
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = vec![i as u8; 100];
        backend.put(&key, value).await?;
    }
    
    // 频繁访问某些键，使其成为热点数据
    for _ in 0..10 {
        for i in 0..20 {
            let key = format!("key_{}", i);
            backend.get(&key).await?;
        }
    }
    
    // 验证统计信息
    let stats = backend.get_stats().await;
    assert!(stats.hot_data_hits > 0);
    
    Ok(())
}
