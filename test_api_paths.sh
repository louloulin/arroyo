#!/bin/bash

# 设置变量
API_URL="http://localhost:5115"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}===== API 路径测试 =====${NC}"

# 测试不同的API路径
test_path() {
    local path=$1
    echo -e "\n${YELLOW}测试路径: $path${NC}"

    response=$(curl -s "$path")
    echo -e "响应: $response"

    if [[ "$response" != *"error"* && "$response" != *"Route not found"* && "$response" != *"<!DOCTYPE html>"* ]]; then
        echo -e "${GREEN}路径可访问${NC}"
        return 0
    else
        if [[ "$response" == *"<!DOCTYPE html>"* ]]; then
            echo -e "${RED}路径返回HTML内容，不是API响应${NC}"
        else
            echo -e "${RED}路径不可访问${NC}"
        fi
        return 1
    fi
}

# 测试 ping 端点
echo -e "\n${YELLOW}测试 ping 端点${NC}"
test_path "$API_URL/api/v1/ping"

# 测试 Push 健康检查端点的不同路径
echo -e "\n${YELLOW}测试 Push 健康检查端点${NC}"
test_path "$API_URL/api/v1/push/health"

# 测试 Push topics 端点的不同路径
echo -e "\n${YELLOW}测试 Push topics 端点${NC}"
test_path "$API_URL/api/v1/push/topics"

echo -e "\n${YELLOW}===== 测试完成 =====${NC}"
