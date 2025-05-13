use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;

/// 模拟存储提供者，用于测试
pub struct MockStorageProvider {
    data: RwLock<HashMap<String, Vec<u8>>>,
}

impl MockStorageProvider {
    /// 创建新的模拟存储提供者
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// 存储数据
    pub async fn put(&self, path: &str, data: Vec<u8>) -> Result<()> {
        let mut store = self.data.write().await;
        store.insert(path.to_string(), data);
        Ok(())
    }

    /// 获取数据
    pub async fn get(&self, path: &str) -> Result<Vec<u8>> {
        let store = self.data.read().await;
        match store.get(path) {
            Some(data) => Ok(data.clone()),
            None => Err(anyhow::anyhow!("Path not found: {}", path)),
        }
    }

    /// 删除数据
    pub async fn delete(&self, path: &str) -> Result<()> {
        let mut store = self.data.write().await;
        store.remove(path);
        Ok(())
    }

    /// 如果存在则删除数据
    pub async fn delete_if_present(&self, path: &str) -> Result<()> {
        let mut store = self.data.write().await;
        store.remove(path);
        Ok(())
    }

    /// 检查路径是否存在
    pub async fn exists(&self, path: &str) -> Result<bool> {
        let store = self.data.read().await;
        Ok(store.contains_key(path))
    }

    /// 列出所有路径
    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let store = self.data.read().await;
        let paths = if let Some(prefix) = prefix {
            store.keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect()
        } else {
            store.keys().cloned().collect()
        };
        Ok(paths)
    }
}

/// 模拟存储提供者引用
pub type MockStorageProviderRef = Arc<MockStorageProvider>;
