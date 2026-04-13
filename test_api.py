"""管理员 API 连通性与基础返回结构测试脚本。"""

import argparse
import sys
from typing import Dict, Tuple

import requests


def login(session: requests.Session, base_url: str, username: str, password: str, role: str, timeout: int) -> bool:
    resp = session.post(
        f"{base_url}/login",
        data={"username": username, "password": password, "role": role},
        timeout=timeout,
        allow_redirects=False,
    )
    if resp.status_code in (301, 302):
        location = (resp.headers.get("Location") or "").lower()
        return "/admin/dashboard" in location or "/user/dashboard" in location
    return resp.status_code == 200 and "登录" not in (resp.text or "")


def check_endpoint(session: requests.Session, name: str, url: str, timeout: int) -> Tuple[bool, Dict]:
    resp = session.get(url, timeout=timeout)
    ok = resp.status_code == 200
    if not ok:
        return False, {"status_code": resp.status_code, "text": (resp.text or "")[:200]}
    try:
        data = resp.json()
    except ValueError:
        return False, {"status_code": resp.status_code, "text": "响应不是 JSON"}
    return True, data


def main() -> int:
    parser = argparse.ArgumentParser(description="测试管理员仪表盘 API")
    parser.add_argument("--base-url", default="http://127.0.0.1:5000", help="服务地址")
    parser.add_argument("--username", default="admin", help="登录用户名")
    parser.add_argument("--password", default="admin123", help="登录密码")
    parser.add_argument("--role", default="admin", help="登录角色")
    parser.add_argument("--timeout", type=int, default=12, help="请求超时（秒）")
    args = parser.parse_args()

    session = requests.Session()
    print("=" * 60)
    print("API 测试开始")
    print("=" * 60)

    try:
        login_ok = login(session, args.base_url, args.username, args.password, args.role, args.timeout)
    except requests.RequestException as exc:
        print(f"登录请求失败: {exc}")
        return 1

    if not login_ok:
        print("登录失败，请先确认服务已启动与账号配置。")
        return 1
    print("登录成功。")

    checks = [
        ("指标汇总", f"{args.base_url}/api/metrics/summary"),
        ("国家新闻", f"{args.base_url}/api/news/national"),
        ("广西新闻", f"{args.base_url}/api/news/guangxi"),
        ("区域新闻", f"{args.base_url}/api/news/region?scope=guangxi"),
        ("统计年报", f"{args.base_url}/api/news/tjnb?scope=guangxi&min_year=2015"),
    ]

    failed = 0
    for idx, (name, url) in enumerate(checks, start=1):
        print(f"\n[{idx}] {name}: {url}")
        try:
            ok, payload = check_endpoint(session, name, url, args.timeout)
        except requests.RequestException as exc:
            failed += 1
            print(f"  FAIL 请求异常: {exc}")
            continue

        if not ok:
            failed += 1
            print(f"  FAIL 状态: {payload.get('status_code')}, 内容: {payload.get('text')}")
            continue

        if isinstance(payload, dict):
            items = payload.get("items")
            if isinstance(items, list):
                print(f"  OK items={len(items)}")
            elif "total_reports" in payload:
                print(f"  OK total_reports={payload.get('total_reports')}")
            else:
                print("  OK")
        else:
            print("  OK")

    print("\n" + "=" * 60)
    if failed:
        print(f"测试完成：失败 {failed} 项")
        return 1
    print("测试完成：全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
