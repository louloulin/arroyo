# Arroyo 连接器示例

本目录包含了 Arroyo 的连接器示例，展示了各种连接器的配置和使用。

## 目录结构

- **kafka/** - Kafka 连接器示例
- **filesystem/** - 文件系统连接器示例
- **kinesis/** - Kinesis 连接器示例
- **redis/** - Redis 连接器示例
- **mqtt/** - MQTT 连接器示例
- **http/** - HTTP 连接器示例

## 连接器概述

Arroyo 提供了丰富的连接器生态系统，支持与各种外部系统集成。连接器分为两类：

1. **源连接器（Source Connectors）**：从外部系统读取数据
2. **目标连接器（Sink Connectors）**：将处理结果写入外部系统

## 连接器配置

每个连接器都需要两种配置：

1. **连接配置（Connection Profile）**：定义如何连接到外部系统，如服务器地址、认证信息等
2. **表配置（Table Configuration）**：定义如何映射数据，如主题名称、格式、模式等

## 运行示例

每个示例目录中都包含一个或多个 SQL 文件和相应的说明文档。要运行示例，可以使用以下方法：

### 方法一：使用 Web UI

1. 启动 Arroyo 集群：
   ```bash
   arroyo cluster
   ```

2. 打开浏览器访问 Web UI：
   ```
   http://localhost:5115
   ```

3. 在"连接"页面创建连接配置和连接表
4. 创建新管道，复制示例 SQL 到查询编辑器中，然后点击"创建"按钮

### 方法二：使用命令行

1. 使用 `arroyo run` 命令运行 SQL 文件：
   ```bash
   arroyo run examples/connectors/kafka/kafka_source.sql
   ```

## 注意事项

- 运行连接器示例前，请确保相应的外部系统已经启动并可访问
- 某些连接器可能需要额外的依赖或配置，请参考各连接器目录中的说明文档
