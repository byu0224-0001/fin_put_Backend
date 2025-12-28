"""Ticker 매칭 4분류 메트릭 확인 스크립트"""
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

# 최신 메트릭 파일 찾기
reports_dir = project_root / "reports"
json_files = list(reports_dir.glob("pipeline_metrics_*.json"))
if not json_files:
    print("메트릭 파일이 없습니다.")
    sys.exit(1)

latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
print(f"분석 파일: {latest_file}\n")

with open(latest_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

tm = data['by_stage']['ticker_matching']
print("=" * 80)
print("Ticker 매칭 4분류 메트릭")
print("=" * 80)
print(f"  총 시도: {tm.get('total', 0)}건")
print(f"  성공: {tm.get('success', 0)}건")
print(f"\n[4분류 상세]")
print(f"  MATCHED_IN_UNIVERSE: {tm.get('matched_in_universe', 0)}건 (DB에 기업 상세 정보 있음)")
print(f"  MATCHED_OUT_OF_UNIVERSE: {tm.get('matched_out_of_universe', 0)}건 (ticker는 찾았지만 DB에 상세 정보 없음)")
print(f"  AMBIGUOUS_NAME: {tm.get('ambiguous_name', 0)}건 (동명이인/복수 후보)")
print(f"  NOT_FOUND: {tm.get('not_found', 0)}건 (진짜 못 찾음)")

if tm.get('total', 0) > 0:
    success_rate = tm.get('success', 0) / tm.get('total', 1) * 100
    print(f"\n[성공률]")
    print(f"  {success_rate:.1f}% ({tm.get('success', 0)}/{tm.get('total', 0)})")
    
    if tm.get('matched_in_universe', 0) > 0:
        universe_rate = tm.get('matched_in_universe', 0) / tm.get('success', 1) * 100
        print(f"  MATCHED_IN_UNIVERSE 비율: {universe_rate:.1f}% ({tm.get('matched_in_universe', 0)}/{tm.get('success', 0)})")

