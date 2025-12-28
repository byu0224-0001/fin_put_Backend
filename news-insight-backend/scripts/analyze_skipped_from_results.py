"""
enrichment_results.json에서 중복으로 스킵된 리포트 분석
"""
import json
import glob
from pathlib import Path

def analyze_skipped_from_results():
    """enrichment_results.json에서 중복 스킵 리포트 찾기"""
    # 가장 최신 파일 찾기
    result_files = glob.glob("reports/enrichment_results_*.json")
    if not result_files:
        print("enrichment_results 파일을 찾을 수 없습니다.")
        return
    
    latest_file = max(result_files)
    print(f"[분석 파일] {latest_file}\n")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    # 중복으로 스킵된 리포트 찾기
    skipped_macro = []
    skipped_industry = []
    saved_macro = []
    saved_industry = []
    
    for result in results:
        route_type = result.get('route_type', 'UNKNOWN')
        industry_edge_enriched = result.get('industry_edge_enriched', False)
        hold_reason = result.get('hold_reason')
        title = result.get('title', 'N/A')
        
        # industry_logic 추출 시도
        industry_logic = result.get('industry_logic', {})
        logic_summary = industry_logic.get('logic_summary', '') if industry_logic else ''
        
        if route_type == 'MACRO':
            if hold_reason and ('중복' in hold_reason or 'duplicate' in hold_reason.lower()):
                skipped_macro.append({
                    'title': title,
                    'hold_reason': hold_reason,
                    'logic_summary': logic_summary
                })
            elif industry_edge_enriched and not hold_reason:
                saved_macro.append({
                    'title': title,
                    'logic_summary': logic_summary
                })
        elif route_type == 'INDUSTRY':
            if hold_reason and ('중복' in hold_reason or 'duplicate' in hold_reason.lower()):
                skipped_industry.append({
                    'title': title,
                    'hold_reason': hold_reason,
                    'logic_summary': logic_summary
                })
            elif industry_edge_enriched and not hold_reason:
                saved_industry.append({
                    'title': title,
                    'logic_summary': logic_summary
                })
    
    # 결과 출력
    print("=" * 80)
    print("[MACRO 리포트 중복 스킵 내역]")
    print("=" * 80)
    print(f"총 {len(skipped_macro)}개 스킵\n")
    
    for i, report in enumerate(skipped_macro, 1):
        print(f"{i}. 제목: {report['title'][:70]}")
        print(f"   Hold Reason: {report['hold_reason']}")
        if report['logic_summary']:
            print(f"   Logic Summary: {report['logic_summary'][:150]}...")
        print()
    
    print("=" * 80)
    print("[INDUSTRY 리포트 중복 스킵 내역]")
    print("=" * 80)
    print(f"총 {len(skipped_industry)}개 스킵\n")
    
    for i, report in enumerate(skipped_industry, 1):
        print(f"{i}. 제목: {report['title'][:70]}")
        print(f"   Hold Reason: {report['hold_reason']}")
        if report['logic_summary']:
            print(f"   Logic Summary: {report['logic_summary'][:150]}...")
        print()
    
    print("=" * 80)
    print("[통계 요약]")
    print("=" * 80)
    print(f"MACRO 리포트: {len(saved_macro)}개 저장, {len(skipped_macro)}개 중복 스킵")
    print(f"INDUSTRY 리포트: {len(saved_industry)}개 저장, {len(skipped_industry)}개 중복 스킵")

if __name__ == "__main__":
    analyze_skipped_from_results()

