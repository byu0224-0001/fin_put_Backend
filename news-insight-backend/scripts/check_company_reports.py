"""종목분석 리포트 확인 스크립트"""
import json
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    import os
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# 최신 수집 파일 찾기
reports_dir = project_root / "reports"
json_files = list(reports_dir.glob("naver_reports_*.json"))
if not json_files:
    print("수집된 리포트 파일이 없습니다.")
    sys.exit(1)

latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
print(f"분석 파일: {latest_file}\n")

with open(latest_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# 카테고리 분포
categories = {}
for r in data:
    cat = r.get('category', 'N/A')
    categories[cat] = categories.get(cat, 0) + 1

print("카테고리 분포:")
for k, v in sorted(categories.items(), key=lambda x: x[1], reverse=True):
    print(f"  {k}: {v}개")

# 종목분석 리포트
company_reports = [r for r in data if r.get('category') == '종목분석']
print(f"\n종목분석 리포트: {len(company_reports)}개")
print("\n샘플 (최대 10개):")
for i, r in enumerate(company_reports[:10], 1):
    stock_name = r.get('stock_name', 'N/A')
    title = r.get('title', 'N/A')[:50]
    print(f"  [{i}] {stock_name} - {title}...")

