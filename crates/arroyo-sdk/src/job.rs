use crate::client::ArroyoClient;
use crate::error::Result;
use serde::{Deserialize, Serialize};

/// 作业状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobStatus {
    /// 创建中
    Creating,
    /// 运行中
    Running,
    /// 已停止
    Stopped,
    /// 失败
    Failed,
    /// 已完成
    Completed,
}

/// 作业模型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// 作业 ID
    pub id: String,
    /// 作业名称
    pub name: String,
    /// 作业状态
    pub status: JobStatus,
    /// 创建时间
    pub created_at: String,
    /// 更新时间
    pub updated_at: String,
}

/// 作业相关的 API 扩展
impl ArroyoClient {
    /// 创建 SQL 作业
    pub async fn create_sql_job(
        &self,
        name: &str,
        sql: &str,
        udfs: Option<Vec<String>>,
    ) -> Result<String> {
        let mut body = serde_json::json!({
            "name": name,
            "query": sql,
            "parallelism": 1,
        });

        if let Some(udfs) = udfs {
            let udfs_json: Vec<serde_json::Value> = udfs
                .into_iter()
                .map(|udf| serde_json::json!({ "definition": udf }))
                .collect();
            body["udfs"] = serde_json::json!(udfs_json);
        }

        let response = self
            .client
            .post(&format!("{}/api/pipelines", self.base_url))
            .json(&body)
            .send()
            .await?;

        let job: serde_json::Value = self.handle_response(response).await?;
        Ok(job["id"].as_str().unwrap_or_default().to_string())
    }

    /// 获取所有作业
    pub async fn get_jobs(&self) -> Result<Vec<Job>> {
        let response = self
            .client
            .get(&format!("{}/api/jobs", self.base_url))
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// 获取指定作业
    pub async fn get_job(&self, id: &str) -> Result<Job> {
        let response = self
            .client
            .get(&format!("{}/api/jobs/{}", self.base_url, id))
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// 停止作业
    pub async fn stop_job(&self, id: &str) -> Result<()> {
        let response = self
            .client
            .post(&format!("{}/api/jobs/{}/stop", self.base_url, id))
            .send()
            .await?;

        self.handle_empty_response(response).await
    }

    /// 启动作业
    pub async fn start_job(&self, id: &str) -> Result<()> {
        let response = self
            .client
            .post(&format!("{}/api/jobs/{}/start", self.base_url, id))
            .send()
            .await?;

        self.handle_empty_response(response).await
    }

    /// 删除作业
    pub async fn delete_job(&self, id: &str) -> Result<()> {
        let response = self
            .client
            .delete(&format!("{}/api/jobs/{}", self.base_url, id))
            .send()
            .await?;

        self.handle_empty_response(response).await
    }
}
