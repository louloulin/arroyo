# Arroyo SDK

Arroyo SDK 是一个 Rust 库，用于简化与 Arroyo 流处理平台的交互。它提供了一组简单的 API，使开发者能够轻松地：

- 连接到 Arroyo 服务器
- 管理连接配置
- 创建和管理流处理作业
- 监控作业状态和指标
- 查询和处理数据

## 安装

将 `arroyo-sdk` 添加到你的 `Cargo.toml` 文件中：

```toml
[dependencies]
arroyo-sdk = "0.15.0-dev"
```

## 快速开始

```rust
use arroyo_sdk::client::ArroyoClient;
use arroyo_sdk::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 创建客户端
    let client = ArroyoClient::new("http://localhost:8000")?;
    
    // 获取所有作业
    let jobs = client.get_jobs().await?;
    println!("当前作业: {:?}", jobs);
    
    // 创建一个新的 SQL 作业
    let job_id = client.create_sql_job(
        "my_job",
        "SELECT * FROM kafka_source",
        None,
    ).await?;
    
    println!("创建了新作业，ID: {}", job_id);
    
    Ok(())
}
```

## 功能

- **连接管理**：创建、列出、更新和删除连接配置
- **作业管理**：创建、启动、停止和删除流处理作业
- **SQL 支持**：使用 SQL 查询创建和管理作业
- **监控**：获取作业状态、指标和日志
- **错误处理**：提供详细的错误信息和处理机制

## 示例

查看 `examples` 目录获取更多使用示例。

## 文档

完整的 API 文档可在 [doc.arroyo.dev](https://doc.arroyo.dev) 获取。

## 许可证

本项目采用 MIT 或 Apache-2.0 许可证。
