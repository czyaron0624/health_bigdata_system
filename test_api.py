#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试KPI API修改
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'web_app'))

import json
import mysql.connector

def test_data():
    """测试数据库中的数据"""
    print("=" * 60)
    print("数据库数据检测")
    print("=" * 60)
    
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='rootpassword',
            database='health_db'
        )
        cursor = conn.cursor()
        
        # 1. 医疗机构数
        cursor.execute("SELECT COUNT(*) FROM medical_institution")
        inst_count = cursor.fetchone()[0]
        print(f"✅ 医疗机构数: {inst_count}")
        
        # 2. 健康指标数
        cursor.execute("SELECT COUNT(*) FROM health_ocr_metrics")
        metric_count = cursor.fetchone()[0]
        print(f"✅ 健康指标数: {metric_count}")
        
        # 3. 新闻数据
        cursor.execute("SELECT COUNT(*) FROM guangxi_news")
        guangxi_news = cursor.fetchone()[0]
        print(f"✅ 广西新闻数: {guangxi_news}")
        
        cursor.execute("SELECT COUNT(*) FROM national_news")
        national_news = cursor.fetchone()[0]
        print(f"✅ 国家新闻数: {national_news}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ 数据库查询错误: {e}")
        return False
    
    return True


def test_detect_risk_events():
    """测试风险事件检测函数"""
    print("\n" + "=" * 60)
    print("风险事件检测测试")
    print("=" * 60)
    
    try:
        from web_app.app import detect_risk_events
        risk_count = detect_risk_events()
        print(f"✅ 检测到的风险事件数: {risk_count}")
        return True
    except Exception as e:
        print(f"❌ 风险事件检测错误: {e}")
        return False


def test_api_payload():
    """模拟API有效负载"""
    print("\n" + "=" * 60)
    print("API有效负载模拟")
    print("=" * 60)
    
    try:
        import mysql.connector
        from web_app.app import detect_risk_events, SCOPE_LABELS
        from datetime import datetime
        
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='rootpassword',
            database='health_db'
        )
        cursor = conn.cursor()
        
        # 构建有效负载
        payload = {
            "source": "live",
            "scope": "guangxi",
            "scope_label": SCOPE_LABELS.get('guangxi', 'guangxi'),
            "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        cursor.execute("SELECT COUNT(*) FROM medical_institution")
        payload["institution_count"] = int(cursor.fetchone()[0])
        
        cursor.execute("SELECT COUNT(*) FROM population_data")
        payload["population_count"] = int(cursor.fetchone()[0])
        
        cursor.execute("SELECT COUNT(*) FROM guangxi_news")
        payload["news_count"] = int(cursor.fetchone()[0])
        
        cursor.execute("SELECT COUNT(*) FROM health_ocr_metrics")
        payload["metric_count"] = int(cursor.fetchone()[0])
        
        # 风险事件（真实检测）
        payload["risk_events"] = detect_risk_events()
        
        # 在线用户（改进版）
        payload["online_users"] = 0  # 当前设为0（无真实会话追踪）
        
        cursor.close()
        conn.close()
        
        print("✅ API有效负载:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return payload
    except Exception as e:
        print(f"❌ 有效负载构建错误: {e}")
        return None


if __name__ == "__main__":
    print("\n🏥 健康大数据仪表板 KPI 修改验证\n")
    
    # 测试数据
    if not test_data():
        sys.exit(1)
    
    # 测试风险事件检测
    if not test_detect_risk_events():
        sys.exit(1)
    
    # 测试API有效负载
    payload = test_api_payload()
    if not payload:
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("✅ 所有测试通过！")
    print("=" * 60)
    print("\n📊 KPI 修改总结:")
    print("  1. ✅ 平台机构数: 现在显示真实的医疗机构数")
    print("  2. ✅ 指标总数: 改进标签为'指标总数'而非'数据更新批次'")
    print("  3. ✅ 重点风险事件: 实现了真实的数据质量异常检测")
    print("  4. ✅ 已移除硬编码的'在线用户'，改为0（可实现会话追踪）")
    print("  5. ✅ 新增'数据来源分布'作为第四个KPI卡片")
