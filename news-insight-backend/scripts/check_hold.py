# -*- coding: utf-8 -*-
"""
HOLD 존재 확인 스크립트

reclassify_all_companies_report.json에서 HOLD 데이터 확인
"""
import sys
import os
import json

# Windows 환경에서 UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')

def check_hold():
    """HOLD 존재 확인"""
    report_file = 'reports/reclassify_all_companies_report.json'
    
    if not os.path.exists(report_file):
        print(f"❌ 리포트 파일이 없습니다: {report_file}", flush=True)
        print(f"  → python scripts/reclassify_all_companies.py를 먼저 실행하세요.", flush=True)
        return
    
    with open(report_file, 'r', encoding='utf-8') as f:
        d = json.load(f)
    
    conf_dist = d.get('stats', {}).get('confidence_distribution', {})
    
    print("=" * 80, flush=True)
    print("HOLD 존재 확인", flush=True)
    print("=" * 80, flush=True)
    print(f"\nHOLD 존재: {'HOLD' in str(conf_dist)}", flush=True)
    print(f"HOLD_UNMAPPED_REVENUE_HIGH: {conf_dist.get('HOLD_UNMAPPED_REVENUE_HIGH', 0)}개", flush=True)
    print(f"HOLD_LOW_CONF: {conf_dist.get('HOLD_LOW_CONF', 0)}개", flush=True)
    
    total_hold = conf_dist.get('HOLD_UNMAPPED_REVENUE_HIGH', 0) + conf_dist.get('HOLD_LOW_CONF', 0)
    print(f"총 HOLD: {total_hold}개", flush=True)
    
    if total_hold > 0:
        print(f"\n✅ HOLD 데이터가 존재합니다. (버그 아님)", flush=True)
        print(f"  → refetch_all_missing_revenue.py는 재수집만 하는 스크립트이므로", flush=True)
        print(f"  → 재분류를 하지 않아 HOLD 비율이 0%로 나오는 것은 정상입니다.", flush=True)
    else:
        print(f"\n⚠️  HOLD 데이터가 없습니다.", flush=True)
        print(f"  → reclassify_all_companies.py를 먼저 실행하세요.", flush=True)
    
    print("=" * 80, flush=True)

if __name__ == '__main__':
    check_hold()

