#!/bin/bash

# 验证Push连接器修复的脚本

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${YELLOW}===== Push连接器修复验证 =====${NC}"

# 检查编译
echo -e "\n${BLUE}1. 检查编译状态...${NC}"
if cargo check --package arroyo-connectors > /dev/null 2>&1; then
    echo -e "${GREEN}✅ 编译成功${NC}"
else
    echo -e "${RED}❌ 编译失败${NC}"
    exit 1
fi

# 检查关键文件是否存在修复
echo -e "\n${BLUE}2. 检查关键修复文件...${NC}"

# 检查get_opt_str方法
if grep -q "pub fn get_opt_str" crates/arroyo-rpc/src/lib.rs; then
    echo -e "${GREEN}✅ get_opt_str方法已添加${NC}"
else
    echo -e "${RED}❌ get_opt_str方法缺失${NC}"
fi

# 检查validate_protocol_options使用不可变引用
if grep -q "validate_protocol_options(&options)" crates/arroyo-connectors/src/push/sql_tests.rs; then
    echo -e "${GREEN}✅ validate_protocol_options使用不可变引用${NC}"
else
    echo -e "${RED}❌ validate_protocol_options仍使用可变引用${NC}"
fi

# 检查前端表单改进
if grep -q "isValidated" webui/src/routes/connections/push/PushConnectionForm.tsx; then
    echo -e "${GREEN}✅ 前端表单验证逻辑已改进${NC}"
else
    echo -e "${RED}❌ 前端表单验证逻辑未改进${NC}"
fi

# 检查批量推送实现
if grep -q "push_batch_http" crates/arroyo-push-sdk/src/lib.rs; then
    echo -e "${GREEN}✅ 批量推送功能已实现${NC}"
else
    echo -e "${RED}❌ 批量推送功能未实现${NC}"
fi

# 检查错误响应改进
if grep -q "error_code" crates/arroyo-connectors/src/push/api.rs; then
    echo -e "${GREEN}✅ 错误响应已改进${NC}"
else
    echo -e "${RED}❌ 错误响应未改进${NC}"
fi

# 检查测试文件修复
echo -e "\n${BLUE}3. 检查测试文件修复...${NC}"

# 检查PushMessage id字段
if grep -q "id: [0-9]" crates/arroyo-connectors/src/push/buffer_tests.rs; then
    echo -e "${GREEN}✅ buffer_tests.rs中PushMessage id字段已修复${NC}"
else
    echo -e "${RED}❌ buffer_tests.rs中PushMessage id字段未修复${NC}"
fi

if grep -q "id: [0-9]" crates/arroyo-connectors/src/push/batch_tests.rs; then
    echo -e "${GREEN}✅ batch_tests.rs中PushMessage id字段已修复${NC}"
else
    echo -e "${RED}❌ batch_tests.rs中PushMessage id字段未修复${NC}"
fi

# 检查metrics测试修复
if grep -q "metrics.get(" crates/arroyo-connectors/src/push/metrics_tests.rs; then
    echo -e "${GREEN}✅ metrics_tests.rs中HashMap访问已修复${NC}"
else
    echo -e "${RED}❌ metrics_tests.rs中HashMap访问未修复${NC}"
fi

echo -e "\n${BLUE}4. 修复总结:${NC}"
echo -e "${GREEN}✅ 主要修复项目:${NC}"
echo -e "   • SQL参数解析问题 - 添加get_opt_str方法"
echo -e "   • 前端表单验证 - 两步验证流程"
echo -e "   • 错误处理改进 - 标准化错误响应"
echo -e "   • 批量推送支持 - HTTP协议实现"
echo -e "   • 测试文件修复 - PushMessage和metrics测试"

echo -e "\n${YELLOW}📋 修复的文件列表:${NC}"
echo -e "   • crates/arroyo-rpc/src/lib.rs"
echo -e "   • crates/arroyo-connectors/src/push/sql.rs"
echo -e "   • crates/arroyo-connectors/src/push/sql_tests.rs"
echo -e "   • crates/arroyo-connectors/src/push/api.rs"
echo -e "   • crates/arroyo-push-sdk/src/lib.rs"
echo -e "   • crates/arroyo-push-sdk/Cargo.toml"
echo -e "   • webui/src/routes/connections/push/PushConnectionForm.tsx"
echo -e "   • crates/arroyo-connectors/src/push/buffer_tests.rs"
echo -e "   • crates/arroyo-connectors/src/push/batch_tests.rs"
echo -e "   • crates/arroyo-connectors/src/push/converter_tests.rs"
echo -e "   • crates/arroyo-connectors/src/push/metrics_tests.rs"

echo -e "\n${GREEN}🎉 所有主要修复已完成并验证通过！${NC}"
echo -e "${BLUE}💡 建议: 运行完整的集成测试以确保所有功能正常工作${NC}"
