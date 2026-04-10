"""
测试管理员仪表板的新API端点
"""
import requests
import json
import time

BASE_URL = "http://127.0.0.1:5000"

# 首先登录
print("="*60)
print("测试系统API端点")
print("="*60)

session = requests.Session()

# 1. 登录
print("\n1. 正在登录...")
try:
    resp = session.post(f"{BASE_URL}/login", data={
        'username': 'admin',
        'password': 'admin123',
        'role': 'admin'
    })
    print(f"   登录状态码: {resp.status_code}")
    if resp.status_code == 302 or 'admin' in resp.text.lower():
        print("   ✓ 登录成功")
    else:
        print(f"   ✗ 登录失败")
except Exception as e:
    print(f"   ✗ 错误: {e}")

time.sleep(1)

# 2. 测试新的metrics API
print("\n2. 测试 /api/metrics/summary 端点...")
try:
    resp = session.get(f"{BASE_URL}/api/metrics/summary")
    print(f"   状态码: {resp.status_code}")
    
    if resp.status_code == 200:
        data = resp.json()
        print(f"   ✓ 获取成功")
        print(f"   - 状态: {data.get('status', 'unknown')}")
        print(f"   - 报告数: {data.get('total_reports', 0)}")
        
        if data.get('data'):
            print(f"\n   报告详情:")
            for report in data['data'][:3]:  # 显示前3个报告
                print(f"     - [{report['report_id']}] {report['title'][:45]}")
                print(f"       类别: {report['category']}")
                print(f"       指标数: {report['metric_count']}")
                if report['metrics']:
                    for metric in report['metrics'][:3]:
                        print(f"         • {metric['metric_name']}: {metric['metric_value']} {metric['unit']}")
    else:
        print(f"   ✗ 请求失败: {resp.status_code}")
        print(f"   { 响应: {resp.text[:200]}}")
except Exception as e:
    print(f"   ✗ 错误: {e}")

# 3. 测试国家新闻API
print("\n3. 测试 /api/news/national 端点...")
try:
    resp = session.get(f"{BASE_URL}/api/news/national")
    print(f"   状态码: {resp.status_code}")
    
    if resp.status_code == 200:
        data = resp.json()
        print(f"   ✓ 获取成功")
        print(f"   - 新闻数: {len(data.get('items', []))}")
    else:
        print(f"   ✗ 请求失败: {resp.status_code}")
except Exception as e:
    print(f"   ✗ 错误: {e}")

# 4. 测试广西新闻API
print("\n4. 测试 /api/news/guangxi 端点...")
try:
    resp = session.get(f"{BASE_URL}/api/news/guangxi")
    print(f"   状态码: {resp.status_code}")
    
    if resp.status_code == 200:
        data = resp.json()
        print(f"   ✓ 获取成功")
        print(f"   - 新闻数: {len(data.get('items', []))}")
    else:
        print(f"   ✗ 请求失败: {resp.status_code}")
except Exception as e:
    print(f"   ✗ 错误: {e}")

print("\n" + "="*60)
print("测试完成")
print("="*60)
