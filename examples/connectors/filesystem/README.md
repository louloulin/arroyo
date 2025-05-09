# 文件系统连接器示例

本目录包含了 Arroyo 的文件系统连接器示例，展示了如何使用文件系统作为数据源和数据目标。

## 文件系统连接器概述

文件系统连接器允许 Arroyo 从文件系统读取数据或将数据写入文件系统。支持本地文件系统和对象存储（如 S3、GCS）。

## 支持的文件格式

- JSON
- CSV
- Parquet
- Avro
- ORC
- 纯文本

## 示例列表

### 1. 文件源 (file_source.sql)

展示如何从文件系统读取数据。

### 2. 文件目标 (file_sink.sql)

展示如何将数据写入文件系统。

### 3. 文件到文件处理 (file_to_file.sql)

展示如何从文件读取数据，处理后再写入文件。

### 4. S3 集成 (s3_integration.sql)

展示如何与 Amazon S3 集成。

## 运行示例

### 步骤 1：准备目录

```bash
# 创建输入和输出目录
mkdir -p /tmp/arroyo/input
mkdir -p /tmp/arroyo/output
```

### 步骤 2：准备测试数据

创建测试数据文件 `/tmp/arroyo/input/data.json`：

```json
{"id": 1, "name": "John", "age": 30, "city": "New York"}
{"id": 2, "name": "Alice", "age": 25, "city": "Boston"}
{"id": 3, "name": "Bob", "age": 35, "city": "Chicago"}
{"id": 4, "name": "Carol", "age": 28, "city": "San Francisco"}
{"id": 5, "name": "Dave", "age": 40, "city": "Seattle"}
```

### 步骤 3：运行 Arroyo 查询

使用 Web UI 或命令行运行示例查询：

```bash
arroyo run examples/connectors/filesystem/file_source.sql
```

### 步骤 4：查看结果

如果使用了文件目标连接器，可以查看输出目录中的文件：

```bash
cat /tmp/arroyo/output/*
```
