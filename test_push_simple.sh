#!/bin/bash

# 设置变量
API_URL="http://localhost:5115"
TOPIC_NAME="test-topic-$(date +%s)"
MESSAGE_CONTENT='{"message": "Hello, World!", "timestamp": '$(date +%s)'}'

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}===== Push API 测试 =====${NC}"

# 步骤1：检查API服务是否运行
echo -e "\n${YELLOW}步骤1: 检查API服务是否运行${NC}"
if curl -s "$API_URL/api/v1/ping" | grep -q "Pong"; then
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

# 步骤3：创建Topic
echo -e "\n${YELLOW}步骤3: 创建Topic${NC}"
CREATE_RESPONSE=$(curl -s -X POST "$API_URL/api/v1/push/topics" \
    -H "Content-Type: application/json" \
    -d '{"name":"'$TOPIC_NAME'","retention_period":86400,"compression":false}')

echo -e "创建Topic响应: $CREATE_RESPONSE"

if echo "$CREATE_RESPONSE" | grep -q "name"; then
    echo -e "${GREEN}成功创建Topic${NC}"
else
    echo -e "${RED}创建Topic失败: $CREATE_RESPONSE${NC}"
    exit 1
fi

# 步骤4：查询所有Topics
echo -e "\n${YELLOW}步骤4: 查询所有Topics${NC}"
TOPICS_RESPONSE=$(curl -s "$API_URL/api/v1/push/topics")
echo -e "所有Topics: $TOPICS_RESPONSE"

if echo "$TOPICS_RESPONSE" | grep -q "$TOPIC_NAME"; then
    echo -e "${GREEN}成功查询到Topic${NC}"
else
    echo -e "${RED}未查询到Topic${NC}"
    exit 1
fi

# 步骤5：查询特定Topic
echo -e "\n${YELLOW}步骤5: 查询特定Topic${NC}"
TOPIC_INFO=$(curl -s "$API_URL/api/v1/push/topics/$TOPIC_NAME")
echo -e "Topic信息: $TOPIC_INFO"

if echo "$TOPIC_INFO" | grep -q "$TOPIC_NAME"; then
    echo -e "${GREEN}成功查询到Topic信息${NC}"
else
    echo -e "${RED}未查询到Topic信息${NC}"
    exit 1
fi

# 步骤6：发送消息到Topic
echo -e "\n${YELLOW}步骤6: 发送消息到Topic${NC}"
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

# 步骤7：删除Topic
echo -e "\n${YELLOW}步骤7: 删除Topic${NC}"
DELETE_RESPONSE=$(curl -s -X DELETE "$API_URL/api/v1/push/topics/$TOPIC_NAME")
echo -e "删除响应: $DELETE_RESPONSE"

if echo "$DELETE_RESPONSE" | grep -q "success"; then
    echo -e "${GREEN}成功删除Topic${NC}"
else
    echo -e "${RED}删除Topic失败: $DELETE_RESPONSE${NC}"
    exit 1
fi

echo -e "\n${GREEN}===== 测试完成 =====${NC}"
echo -e "${GREEN}Push API接口测试成功!${NC}"
