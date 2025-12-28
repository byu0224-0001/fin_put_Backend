# -*- coding: utf-8 -*-
"""
검증 리포트 요약 출력
"""
import json
import os

REPORT_FILE = 'reports/segment_mapping_validation.json'

def main():
    if not os.path.exists(REPORT_FILE):
        print("❌ 리포트 파일이 없습니다.")
        return
    
    with open(REPORT_FILE, 'r', encoding='utf-8') as f:
        report = json.load(f)
    
    cov = report.get('coverage', {})
    
    print("=" * 80)
    print("SEGMENT_TO_SECTOR_MAP 품질 검증 리포트 요약")
    print("=" * 80)
    
    print(f"\n[커버리지 지표]")
    print(f"  Coverage-A (매출 가중): {cov.get('coverage_a', 0):.1f}%")
    print(f"  Coverage-B (세그먼트 카운트): {cov.get('coverage_b', 0):.1f}%")
    print(f"  전체 기업: {cov.get('total_companies', 0)}")
    print(f"  매출 비중 있는 기업: {cov.get('companies_with_revenue', 0)}")
    print(f"  전체 세그먼트: {cov.get('total_segments', 0)}")
    print(f"  매핑된 세그먼트: {cov.get('mapped_segments', 0)}")
    
    print(f"\n[미매핑 세그먼트 Top 10]")
    unmapped = report.get('unmapped_top100', [])[:10]
    for i, (seg, info) in enumerate(unmapped, 1):
        print(f"  {i:2}. {seg:30} | 빈도: {info['count']:4} | 총 비중: {info['total_pct']:.1f}%")
    
    print(f"\n[섹터 힌트 키워드 Top 10]")
    hints = report.get('sector_hint_keywords', [])[:10]
    for i, hint_info in enumerate(hints, 1):
        print(f"  {i:2}. {hint_info['keyword']:20} (빈도: {hint_info['count']:4})")
    
    print(f"\n[오분류 후보 Top 5]")
    misclass = report.get('misclassification_candidates', [])[:5]
    for i, mc in enumerate(misclass, 1):
        print(f"  {i}. {mc['name']} ({mc['ticker']})")
        print(f"     현재: {mc['current_sector']} → 매출기반: {mc['revenue_best_sector']} (score: {mc['revenue_score']:.3f})")
    
    print("\n" + "=" * 80)

if __name__ == '__main__':
    main()

