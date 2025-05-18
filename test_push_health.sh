#!/bin/bash

# 设置变量
API_URL="http://localhost:5115"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}===== Push API 健康检查测试 =====${NC}"

# 步骤1：检查API服务是否运行
echo -e "\n${YELLOW}步骤1: 检查API服务是否运行${NC}"
PING_RESPONSE=$(curl -s "$API_URL/api/v1/ping")
echo -e "响应: $PING_RESPONSE"

if echo "$PING_RESPONSE" | grep -q "Pong"; then
    echo -e "${GREEN}API服务正在运行${NC}"
else
    echo -e "${RED}API服务未运行，请先启动API服务${NC}"
    exit 1
fi

# 步骤2：检查Push健康端点
echo -e "\n${YELLOW}步骤2: 检查Push健康端点${NC}"
HEALTH_RESPONSE=$(curl -s "$API_URL/api/v1/push/health")
echo -e "响应: $HEALTH_RESPONSE"

if echo "$HEALTH_RESPONSE" | grep -q "status"; then
    echo -e "${GREEN}Push健康端点正常${NC}"
else
    echo -e "${RED}Push健康端点异常: $HEALTH_RESPONSE${NC}"
fi

echo -e "\n${YELLOW}===== 测试完成 =====${NC}"
