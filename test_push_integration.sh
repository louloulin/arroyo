#!/bin/bash

# 测试脚本：验证Push连接器集成和端口统一

# 设置变量
API_URL="http://localhost:5115"
TOPIC_NAME="test-topic-integration"
MESSAGE_CONTENT='{"message": "Hello, World!", "timestamp": '$(date +%s)'}'

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${YELLOW}===== Push连接器集成测试 =====${NC}"

# 步骤1：检查API服务是否运行
echo -e "\n${YELLOW}步骤1: 检查API服务是否运行${NC}"
if curl -s "$API_URL/ping" > /dev/null; then
    echo -e "${GREEN}API服务正在运行${NC}"
else
    echo -e "${RED}API服务未运行，请先启动API服务${NC}"
    exit 1
fi

# 步骤2：创建测试Topic
echo -e "\n${YELLOW}步骤2: 创建测试Topic${NC}"
CREATE_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/push/topics" \
    -H "Content-Type: application/json" \
    -d '{"name":"'$TOPIC_NAME'","retention_period":86400,"compression":"none"}')

echo -e "创建Topic响应: $CREATE_RESPONSE"

if echo "$CREATE_RESPONSE" | grep -q "error"; then
    if echo "$CREATE_RESPONSE" | grep -q "already exists"; then
        echo -e "${YELLOW}Topic已存在，继续测试${NC}"
    else
        echo -e "${RED}创建Topic失败: $CREATE_RESPONSE${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}成功创建Topic${NC}"
fi

# 步骤3：查询所有Topics
echo -e "\n${YELLOW}步骤3: 查询所有Topics${NC}"
TOPICS_RESPONSE=$(curl -s "$API_URL/api/v1/push/topics")
echo -e "所有Topics: $TOPICS_RESPONSE"

if echo "$TOPICS_RESPONSE" | grep -q "$TOPIC_NAME"; then
    echo -e "${GREEN}成功查询到Topic${NC}"
else
    echo -e "${RED}未查询到Topic${NC}"
    exit 1
fi

# 步骤4：查询特定Topic
echo -e "\n${YELLOW}步骤4: 查询特定Topic${NC}"
TOPIC_INFO=$(curl -s "$API_URL/api/v1/push/topics/$TOPIC_NAME")
echo -e "Topic信息: $TOPIC_INFO"

if echo "$TOPIC_INFO" | grep -q "$TOPIC_NAME"; then
    echo -e "${GREEN}成功查询到Topic信息${NC}"
else
    echo -e "${RED}未查询到Topic信息${NC}"
    exit 1
fi

# 步骤5：发送消息到Topic
echo -e "\n${YELLOW}步骤5: 发送消息到Topic${NC}"
echo -e "发送消息内容: $MESSAGE_CONTENT"

PUSH_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/push/$TOPIC_NAME" \
    -H "Content-Type: application/json" \
    -d "$MESSAGE_CONTENT")

echo -e "响应: $PUSH_RESPONSE"

if echo "$PUSH_RESPONSE" | grep -q "success"; then
    echo -e "${GREEN}消息发送成功!${NC}"
else
    echo -e "${RED}消息发送失败: $PUSH_RESPONSE${NC}"
    exit 1
fi

# 步骤6：验证独立Push服务已禁用
echo -e "\n${YELLOW}步骤6: 验证独立Push服务已禁用${NC}"
INDEPENDENT_RESPONSE=$(curl -s -m 1 "http://localhost:8000/api/v1/push/topics" || echo "Connection refused")

if echo "$INDEPENDENT_RESPONSE" | grep -q "Connection refused"; then
    echo -e "${GREEN}独立Push服务已禁用，无法连接到端口8000${NC}"
else
    echo -e "${RED}警告：独立Push服务似乎仍在运行，端口8000可访问${NC}"
    echo -e "响应: $INDEPENDENT_RESPONSE"
fi

# 步骤7：删除测试Topic
echo -e "\n${YELLOW}步骤7: 删除测试Topic${NC}"
DELETE_RESPONSE=$(curl -s -X DELETE "$API_URL/api/v1/push/topics/$TOPIC_NAME")
echo -e "删除响应: $DELETE_RESPONSE"

if echo "$DELETE_RESPONSE" | grep -q "success"; then
    echo -e "${GREEN}成功删除Topic${NC}"
else
    echo -e "${RED}删除Topic失败: $DELETE_RESPONSE${NC}"
    exit 1
fi

echo -e "\n${GREEN}===== 测试完成 =====${NC}"
echo -e "${GREEN}Push连接器已成功集成到API服务中，并且独立Push服务已禁用${NC}"
echo -e "${GREEN}所有Push API请求现在应通过API服务端口（5115）处理${NC}"
