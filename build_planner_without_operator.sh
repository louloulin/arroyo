#!/bin/bash
# 此脚本用于构建 arroyo-planner 模块，而不构建 arroyo-operator 模块
# 通过临时修改 Cargo.toml 文件，移除 arroyo-operator 依赖
# 构建完成后恢复原始的 Cargo.toml 文件

set -e  # 遇到错误立即退出

echo "开始构建 arroyo-planner 模块，不包含 arroyo-operator 依赖..."

# 创建临时目录
mkdir -p temp_build

# 复制 arroyo-planner 的 Cargo.toml 文件
cp crates/arroyo-planner/Cargo.toml temp_build/Cargo.toml.bak

# 修改 Cargo.toml 文件，移除 arroyo-operator 依赖
cat > crates/arroyo-planner/Cargo.toml << EOF
[package]
name = "arroyo-planner"
version = "0.15.0-dev"
edition = "2021"


[features]
default = []
planner = []
standalone = []
no-operator = []

[dependencies]
arroyo-types = { path = "../arroyo-types" }
arroyo-rpc = {path = "../arroyo-rpc"}
arroyo-datastream = { path = "../arroyo-datastream" }
arroyo-connectors = { path = "../arroyo-connectors" }
arroyo-udf-host = { path = "../arroyo-udf/arroyo-udf-host" }
arroyo-udf-python = { path = "../arroyo-udf/arroyo-udf-python" }

datafusion = { workspace = true }
datafusion-proto = { workspace = true }
datafusion-functions = { workspace = true }
datafusion-functions-json = { workspace = true }

prost = {workspace = true}
arrow-schema = {workspace = true, features = ["serde"] }
serde = {version = "1", features = ["derive"]}
serde_json = "1"

petgraph = { workspace = true }
tokio = "1.27"
tokio-stream = { version = "0.1", features = ["full"] }
futures = "0.3"
arrow = { workspace = true, features = ["ffi"] }
arrow-array = { workspace = true}
anyhow = {version = "1.0.70", features = ["backtrace"]}
async-trait = "0.1"

syn = {version = "2", features = ["full", "parsing", "extra-traits"]}
tracing = "0.1.37"

serde_json_path = "0.7"
unicase = "2.7.0"
xxhash-rust = { version = "0.8.12", features = ["xxh3", "std"] }
itertools = "0.14.0"

sqlparser = { workspace = true}

[dev-dependencies]
test-log = {version = "0.2.15", default-features = false, features = ["trace"]}
rstest = { version = "0.25" }

[build-dependencies]
glob = "0.3.1"
EOF

echo "修改后的 Cargo.toml 文件已创建，开始构建..."

# 尝试构建
cargo build -p arroyo-planner --lib --no-default-features --features="planner,no-operator"

# 构建完成后，恢复原始的 Cargo.toml 文件
mv temp_build/Cargo.toml.bak crates/arroyo-planner/Cargo.toml

# 清理临时目录
rm -rf temp_build

echo "构建完成！原始的 Cargo.toml 文件已恢复。"
echo "现在可以使用 arroyo-planner 模块，而不依赖于 arroyo-operator 模块。"
