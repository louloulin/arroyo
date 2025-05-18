#!/usr/bin/env python3
import json
import requests
import sys
import time

# 设置变量
API_URL = "http://localhost:5115"
TOPIC_NAME = f"test-topic-{int(time.time())}"  # 使用时间戳创建唯一的topic名称
MESSAGE_CONTENT = {
    "message": "Hello from Python!",
    "timestamp": int(time.time())
}

# 颜色定义
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[0;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'  # No Color

def print_colored(color, message):
    print(f"{color}{message}{NC}")

def test_endpoint(url, method="GET", data=None, expected_status=200):
    print_colored(BLUE, f"测试 {method} {url}")

    try:
        headers = {"Content-Type": "application/json"}
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=5)
        elif method == "POST":
            response = requests.post(url, json=data, headers=headers, timeout=5)
        elif method == "DELETE":
            response = requests.delete(url, headers=headers, timeout=5)
        else:
            print_colored(RED, f"不支持的方法: {method}")
            return False

        print_colored(BLUE, f"状态码: {response.status_code}")

        # 检查是否返回HTML内容
        content_type = response.headers.get('Content-Type', '')
        if 'text/html' in content_type or response.text.strip().startswith('<!DOCTYPE html>') or response.text.strip().startswith('<html>'):
            print_colored(RED, "返回了HTML内容，这可能是Web UI而不是API")
            print_colored(BLUE, f"响应: {response.text[:200]}...")
            return False

        try:
            print_colored(BLUE, f"响应: {response.json()}")
        except:
            print_colored(BLUE, f"响应: {response.text[:200]}...")

        if response.status_code == expected_status:
            print_colored(GREEN, "测试成功!")
            return True
        else:
            print_colored(RED, f"测试失败! 期望状态码 {expected_status}, 实际状态码 {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print_colored(RED, f"请求异常: {e}")
        return False

def main():
    print_colored(YELLOW, "===== Push API 测试 =====")

    # 测试1: 检查API服务是否运行
    print_colored(YELLOW, "\n测试1: 检查API服务是否运行")
    if not test_endpoint(f"{API_URL}/api/v1/ping"):
        print_colored(RED, "API服务未运行，测试终止")
        sys.exit(1)

    # 测试2: 检查Push健康端点
    print_colored(YELLOW, "\n测试2: 检查Push健康端点")
    test_endpoint(f"{API_URL}/api/v1/push/health")

    # 测试3: 创建Topic
    print_colored(YELLOW, "\n测试3: 创建Topic")
    create_data = {
        "name": TOPIC_NAME,
        "retention_period": 86400,
        "compression": False
    }
    
    # 尝试不同的路径
    paths = [
        f"{API_URL}/api/v1/push/topics",
        f"{API_URL}/push/topics",
        f"{API_URL}/api/push/topics"
    ]
    
    topic_created = False
    for path in paths:
        print_colored(YELLOW, f"\n尝试路径: {path}")
        if test_endpoint(path, "POST", create_data, 201):
            topic_created = True
            break

    if not topic_created:
        print_colored(RED, "无法创建Topic，后续测试将跳过")
        sys.exit(1)

    # 测试4: 获取所有Topics
    print_colored(YELLOW, "\n测试4: 获取所有Topics")
    
    # 尝试不同的路径
    paths = [
        f"{API_URL}/api/v1/push/topics",
        f"{API_URL}/push/topics",
        f"{API_URL}/api/push/topics"
    ]
    
    topics_retrieved = False
    for path in paths:
        print_colored(YELLOW, f"\n尝试路径: {path}")
        if test_endpoint(path):
            topics_retrieved = True
            break
            
    if not topics_retrieved:
        print_colored(RED, "无法获取Topics，后续测试将跳过")
        sys.exit(1)

    print_colored(YELLOW, "\n===== 测试完成 =====")

if __name__ == "__main__":
    main()
