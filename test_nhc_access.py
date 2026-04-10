"""
测试以极低速度 + 完整伪装访问国家卫健委官网
"""

import requests
from bs4 import BeautifulSoup
import time

# 多个真实User-Agent
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# 完整HTTP头模拟真实浏览器
HEADERS = {
    'User-Agent': USER_AGENTS[0],
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'max-age=0',
    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
}

def test_nhc_access():
    """测试是否能访问国家卫健委数据页"""
    
    url = "https://www.nhc.gov.cn/mohwsbwstjxxzx/s2906/new_list.shtml"
    
    print("="*70)
    print("测试国家卫健委官网数据页面访问")
    print("="*70)
    print(f"\n目标URL: {url}")
    print(f"访问方式:")
    print(f"  • User-Agent: 真实Chrome浏览器")
    print(f"  • HTTP头: 完整模拟浏览器请求")
    print(f"  • 延时: 3秒 (极低速)")
    print(f"  • SSL验证: 禁用")
    
    try:
        print(f"\n[开始请求...]")
        
        session = requests.Session()
        
        # 极低速访问
        time.sleep(3)
        
        response = session.get(
            url,
            headers=HEADERS,
            timeout=30,
            verify=False,
            allow_redirects=True
        )
        
        print(f"\n✓ 状态码: {response.status_code}")
        print(f"✓ 内容长度: {len(response.text)} 字符")
        print(f"✓ 编码: {response.encoding}")
        
        # 分析页面内容
        soup = BeautifulSoup(response.text, 'html.parser')
        
        print(f"\n【页面内容分析】")
        
        # 查找表格
        tables = soup.find_all('table')
        print(f"✓ 找到表格数: {len(tables)}")
        
        # 查找图片
        imgs = soup.find_all('img')
        print(f"✓ 找到图片数: {len(imgs)}")
        
        # 查找链接
        links = soup.find_all('a', href=True)
        print(f"✓ 找到链接数: {len(links)}")
        
        # 查找文本节点
        text = soup.get_text(strip=True)
        print(f"✓ 页面纯文本长度: {len(text)} 字符")
        
        # 输出前300字
        if len(text) > 0:
            print(f"\n【页面文本预览 (前300字)】")
            print(text[:300])
        
        # 检查是否包含特定关键词
        keywords = ['医疗', '卫生', '统计', '指标', '数据', '机构', '报告']
        found_keywords = [kw for kw in keywords if kw in text]
        print(f"\n【关键词检测】")
        print(f"✓ 找到的关键词: {found_keywords if found_keywords else '无'}")
        
        # 输出前3个图片的详情
        if len(imgs) > 0:
            print(f"\n【页面图片信息】")
            for i, img in enumerate(imgs[:3]):
                src = img.get('src', 'N/A')
                alt = img.get('alt', 'N/A')
                print(f"  [{i+1}] src: {src[:60]}")
                print(f"      alt: {alt}")
        
        # 输出前5个链接
        if len(links) > 0:
            print(f"\n【页面链接示例 (前5个)】")
            for i, link in enumerate(links[:5]):
                href = link.get('href', 'N/A')
                text_content = link.get_text(strip=True)[:40]
                print(f"  [{i+1}] {text_content}")
                print(f"      href: {href[:60]}")
        
        print(f"\n" + "="*70)
        print("✓ 页面访问成功!")
        print("="*70)
        
        return {
            'status': 'success',
            'status_code': response.status_code,
            'has_tables': len(tables) > 0,
            'has_images': len(imgs) > 0,
            'text_length': len(text)
        }
    
    except requests.exceptions.RequestException as e:
        print(f"\n✗ 请求失败: {type(e).__name__}")
        print(f"  {e}")
        
        return {
            'status': 'failed',
            'error': str(e)
        }

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings()
    
    result = test_nhc_access()
    
    print(f"\n【最终判断】")
    if result['status'] == 'success':
        if result['has_tables']:
            print("💡 页面包含HTML表格 → 可以直接解析")
        elif result['has_images']:
            print("⚠️  页面包含图片 → 需要OCR识别")
            print("   成本: 需要调用OCR API或本地模型")
        else:
            print("❌ 页面既无表格也无图片 → 可能是JavaScript动态加载")
    else:
        print(f"❌ 无法访问页面 → {result['error']}")
