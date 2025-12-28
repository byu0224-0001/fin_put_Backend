"""
스킵된 리포트 내용 정리 스크립트
MACRO/INDUSTRY 리포트 중 중복으로 스킵된 리포트들의 logic_summary와 내용을 정리합니다.
"""
import json
import glob
import sys
from pathlib import Path
from collections import defaultdict

# 프로젝트 루트로 이동
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def analyze_skipped_reports():
    """스킵된 리포트 분석"""
    import hashlib
    
    # enrichment_results 파일 찾기
    result_files = glob.glob("reports/enrichment_results_*.json")
    if not result_files:
        print("[ERROR] enrichment_results 파일을 찾을 수 없습니다.")
        return
    
    latest_file = max(result_files)
    print(f"[분석 파일] {latest_file}\n")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    # logic_summary 기반으로 중복 그룹 찾기
    logic_groups = defaultdict(list)  # logic_fingerprint -> [reports]
    
    for result in results:
        report_type = result.get('route_type', 'UNKNOWN')
        if report_type not in ['MACRO', 'INDUSTRY']:
            continue
            
        # industry_logic에서 logic_summary 추출
        industry_logic = result.get('industry_logic', {})
        if isinstance(industry_logic, dict):
            logic_summary = industry_logic.get('logic_summary', '')
            source_driver = industry_logic.get('source_driver_code', '')
            target_sector = industry_logic.get('target_sector_code', '')
        else:
            logic_summary = ''
            source_driver = ''
            target_sector = ''
        
        if logic_summary:
            # logic_fingerprint 계산 (중복 체크 기준)
            logic_fp = hashlib.sha256(logic_summary.encode('utf-8')).hexdigest()[:16]
            key = f"{logic_fp}_{source_driver}_{target_sector}"
            logic_groups[key].append({
                'result': result,
                'logic_summary': logic_summary,
                'source_driver': source_driver,
                'target_sector': target_sector
            })
    
    # 중복 그룹 찾기 (2개 이상인 경우)
    duplicate_groups = {k: v for k, v in logic_groups.items() if len(v) > 1}
    
    # 스킵된 리포트 정리 (중복 그룹에서 첫 번째를 제외한 나머지)
    skipped_macro = []
    skipped_industry = []
    saved_macro = []
    saved_industry = []
    
    for key, group in logic_groups.items():
        is_duplicate_group = len(group) > 1
        for idx, item in enumerate(group):
            result = item['result']
            report_type = result.get('route_type', 'UNKNOWN')
            
            if is_duplicate_group and idx > 0:  # 중복 그룹에서 첫 번째 이후는 스킵
                if report_type == 'MACRO':
                    skipped_macro.append(item)
                elif report_type == 'INDUSTRY':
                    skipped_industry.append(item)
            else:  # 첫 번째 또는 중복이 아닌 경우 저장
                if report_type == 'MACRO':
                    saved_macro.append(item)
                elif report_type == 'INDUSTRY':
                    saved_industry.append(item)
    
    print("=" * 80)
    print("[스킵된 리포트 분석 결과]")
    print("=" * 80)
    print(f"\n[저장된 리포트]")
    print(f"   - MACRO: {len(saved_macro)}개")
    print(f"   - INDUSTRY: {len(saved_industry)}개")
    print(f"\n[스킵된 리포트]")
    print(f"   - MACRO: {len(skipped_macro)}개")
    print(f"   - INDUSTRY: {len(skipped_industry)}개")
    
    # MACRO 리포트 스킵 내역
    print("\n" + "=" * 80)
    print("[MACRO 리포트 스킵 내역]")
    print("=" * 80)
    if skipped_macro:
        for i, item in enumerate(skipped_macro, 1):
            report = item['result']
            print(f"\n[{i}] 제목: {report.get('title', 'N/A')}")
            print(f"    리포트 ID: {report.get('report_id', 'N/A')}")
            print(f"    Route Confidence: {report.get('route_confidence', 'N/A')}")
            print(f"    Source Driver: {item.get('source_driver', 'N/A')}")
            print(f"    Target Sector: {item.get('target_sector', 'N/A')}")
            
            logic_summary = item.get('logic_summary', 'N/A')
            if logic_summary and logic_summary != 'N/A':
                print(f"    Logic Summary: {logic_summary[:200]}...")
            
            # industry_logic에서 key_sentence 추출 시도
            industry_logic = report.get('industry_logic', {})
            if isinstance(industry_logic, dict):
                key_sentence = industry_logic.get('key_sentence', 'N/A')
                if key_sentence and key_sentence != 'N/A':
                    print(f"    Key Sentence: {key_sentence[:150]}...")
            
            print(f"    Route Evidence: {', '.join(report.get('route_evidence', []))}")
    else:
        print("\n   스킵된 MACRO 리포트가 없습니다.")
    
    # INDUSTRY 리포트 스킵 내역
    print("\n" + "=" * 80)
    print("[INDUSTRY 리포트 스킵 내역]")
    print("=" * 80)
    if skipped_industry:
        for i, item in enumerate(skipped_industry, 1):
            report = item['result']
            print(f"\n[{i}] 제목: {report.get('title', 'N/A')}")
            print(f"    리포트 ID: {report.get('report_id', 'N/A')}")
            print(f"    Route Confidence: {report.get('route_confidence', 'N/A')}")
            print(f"    Source Driver: {item.get('source_driver', 'N/A')}")
            print(f"    Target Sector: {item.get('target_sector', 'N/A')}")
            
            logic_summary = item.get('logic_summary', 'N/A')
            if logic_summary and logic_summary != 'N/A':
                print(f"    Logic Summary: {logic_summary[:200]}...")
            
            # industry_logic에서 key_sentence 추출 시도
            industry_logic = report.get('industry_logic', {})
            if isinstance(industry_logic, dict):
                key_sentence = industry_logic.get('key_sentence', 'N/A')
                if key_sentence and key_sentence != 'N/A':
                    print(f"    Key Sentence: {key_sentence[:150]}...")
            
            print(f"    Route Evidence: {', '.join(report.get('route_evidence', []))}")
    else:
        print("\n   스킵된 INDUSTRY 리포트가 없습니다.")
    
    # 저장된 리포트와 비교 (중복 기준 확인)
    print("\n" + "=" * 80)
    print("[저장된 리포트 (참고용)]")
    print("=" * 80)
    
    print(f"\n[저장된 MACRO 리포트] ({len(saved_macro)}개):")
    for i, item in enumerate(saved_macro, 1):
        report = item['result']
        print(f"   {i}. {report.get('title', 'N/A')[:60]}")
        logic_summary = item.get('logic_summary', 'N/A')
        if logic_summary and logic_summary != 'N/A':
            print(f"      Logic: {logic_summary[:80]}...")
    
    print(f"\n[저장된 INDUSTRY 리포트] ({len(saved_industry)}개):")
    for i, item in enumerate(saved_industry, 1):
        report = item['result']
        print(f"   {i}. {report.get('title', 'N/A')[:60]}")
        logic_summary = item.get('logic_summary', 'N/A')
        if logic_summary and logic_summary != 'N/A':
            print(f"      Logic: {logic_summary[:80]}...")
    
    # 중복 그룹 상세 정보
    if duplicate_groups:
        print("\n" + "=" * 80)
        print("[중복 그룹 상세 정보]")
        print("=" * 80)
        for key, group in list(duplicate_groups.items())[:5]:  # 상위 5개만 표시
            print(f"\n[중복 그룹] {len(group)}개 리포트가 동일한 논리:")
            first_item = group[0]
            print(f"   Logic Summary: {first_item['logic_summary'][:150]}...")
            print(f"   Source Driver: {first_item['source_driver']}, Target Sector: {first_item['target_sector']}")
            print(f"   리포트 목록:")
            for idx, item in enumerate(group, 1):
                report = item['result']
                status = "저장됨" if idx == 1 else "스킵됨"
                print(f"      {idx}. [{status}] {report.get('title', 'N/A')[:50]}")

if __name__ == "__main__":
    analyze_skipped_reports()

