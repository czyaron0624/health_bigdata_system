"""
第一阶段流水线：爬虫 -> OCR结构化回填 -> JSON/CSV导出
"""

import subprocess
import sys
from pathlib import Path


def run_step(step_name, command, cwd):
    print(f"\n{'=' * 70}")
    print(f"开始: {step_name}")
    print(f"命令: {' '.join(command)}")
    print(f"{'=' * 70}")

    result = subprocess.run(command, cwd=str(cwd))
    if result.returncode != 0:
        print(f"❌ 失败: {step_name}")
        sys.exit(result.returncode)

    print(f"✅ 完成: {step_name}")


def main():
    root = Path(__file__).resolve().parents[1]
    py = sys.executable

    run_step("广西爬虫采集与OCR", [py, "crawlers/guangxi_health_crawler.py", "--min-year", "2015"], root)
    run_step("OCR结构化回填", [py, "crawlers/backfill_ocr_metrics.py", "--min-year", "2015"], root)
    run_step("结构化数据导出", [py, "crawlers/export_structured_data.py"], root)

    print("\n🎉 第一阶段流水线执行完成")


if __name__ == "__main__":
    main()
