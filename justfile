# Arroyo justfile
# 使用方法: just <recipe>

# 默认任务列表
default:
    @just --list

# 启动PostgreSQL容器
start-postgres:
    @echo "启动PostgreSQL容器..."
    docker run --name arroyo-postgres -e POSTGRES_USER=arroyo -e POSTGRES_PASSWORD=arroyo -e POSTGRES_DB=arroyo -p 5433:5432 -d postgres:14 || echo "PostgreSQL容器可能已经在运行"
    @echo "等待PostgreSQL启动..."
    sleep 5

# 运行数据库迁移
migrate: start-postgres
    @echo "运行数据库迁移..."
    refinery migrate -c refinery.toml -p crates/arroyo-api/migrations

# 编译项目
build: migrate
    @echo "编译项目..."
    cargo build

# 启动完整的Arroyo集群
start-cluster: build
    @echo "启动Arroyo集群..."
    cargo run -- cluster

# 启动API服务器
start-api: build
    @echo "启动API服务器..."
    cargo run -- api

# 启动控制器
start-controller: build
    @echo "启动控制器..."
    cargo run -- controller

# 启动编译器服务
start-compiler: build
    @echo "启动编译器服务..."
    cargo run -- compiler

# 启动工作节点
start-worker: build
    @echo "启动工作节点..."
    cargo run -- worker

# 启动节点服务器
start-node: build
    @echo "启动节点服务器..."
    cargo run -- node

# 编译Web UI
build-webui:
    @echo "编译Web UI..."
    cd webui && pnpm install && pnpm run build

# 停止PostgreSQL容器
stop-postgres:
    @echo "停止PostgreSQL容器..."
    docker stop arroyo-postgres || echo "PostgreSQL容器可能没有运行"

# 删除PostgreSQL容器
remove-postgres: stop-postgres
    @echo "删除PostgreSQL容器..."
    docker rm arroyo-postgres || echo "PostgreSQL容器可能不存在"

# 清理所有资源
clean: remove-postgres
    @echo "清理项目..."
    cargo clean

# 运行测试
test: build
    @echo "运行测试..."
    cargo test

# 运行SQL查询
run-query query="SELECT * FROM users":
    @echo "运行查询: {{query}}"
    echo '{{query}}' > /tmp/arroyo_query.sql
    cargo run -- run /tmp/arroyo_query.sql

# 可视化查询计划
visualize-query-open query="SELECT * FROM users":
    @echo "可视化查询(带--open选项): {{query}}"
    cargo run -- visualize --open "{{query}}"

visualize-query query="SELECT * FROM users":
    @echo "可视化查询: {{query}}"
    cargo run -- visualize "{{query}}"
