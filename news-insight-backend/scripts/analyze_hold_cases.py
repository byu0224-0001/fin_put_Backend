"""
HOLD 케이스 분석 스크립트

P0+: Sanity Check 오탐 관리 - HOLD 케이스 Top N 분석
"""
import sys
import os
import json
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()


def analyze_hold_cases(top_n: int = 10):
    """
    HOLD 케이스 분석
    
    Args:
        top_n: 상위 N개 분석
    """
    print("=" * 80)
    print("[HOLD 케이스 분석]")
    print("=" * 80)
    
    # 1. 최신 enrichment_results 파일 찾기
    result_files = list(Path("reports").glob("enrichment_results_*.json"))
    if not result_files:
        print("[오류] enrichment_results 파일을 찾을 수 없습니다.")
        return
    
    latest_file = max(result_files, key=lambda p: p.stat().st_mtime)
    print(f"[분석 파일] {latest_file}\n")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    # 2. HOLD 케이스 수집
    hold_cases = []
    for result in results:
        if result.get('hold_reason') or result.get('error_type') in ['ROUTING_HOLD', 'HOLD_SECTOR_MAPPING']:
            hold_cases.append(result)
    
    print(f"[전체 HOLD 케이스] {len(hold_cases)}건\n")
    
    # 3. HOLD 사유별 분류
    hold_reasons = Counter()
    rule_ids = Counter()
    
    for case in hold_cases:
        hold_reason = case.get('hold_reason', case.get('error', 'UNKNOWN'))
        hold_reasons[hold_reason] += 1
        
        rule_id = case.get('sanity_check_rule_id') or case.get('routing_audit', {}).get('sanity_check_rule_id')
        if rule_id:
            rule_ids[rule_id] += 1
    
    print("[HOLD 사유 Top 10]")
    print("-" * 80)
    for reason, count in hold_reasons.most_common(top_n):
        print(f"  {count:>3}건: {reason[:70]}...")
    
    print(f"\n[Sanity Check Rule ID Top 10]")
    print("-" * 80)
    if rule_ids:
        for rule_id, count in rule_ids.most_common(top_n):
            print(f"  {count:>3}건: {rule_id}")
    else:
        print("  (rule_id 정보 없음)")
    
    # 4. 샘플 케이스 출력
    print(f"\n[샘플 케이스 Top {top_n}]")
    print("-" * 80)
    for i, case in enumerate(hold_cases[:top_n], 1):
        print(f"\n[{i}] {case.get('title', 'N/A')[:60]}...")
        print(f"    - Hold Reason: {case.get('hold_reason', 'N/A')[:60]}...")
        print(f"    - Rule ID: {case.get('sanity_check_rule_id', 'N/A')}")
        print(f"    - Route Evidence: {', '.join(case.get('route_evidence', [])[:3])}")
    
    # 5. 룰 개선 후보 저장
    print(f"\n" + "=" * 80)
    print("[룰 개선 후보 저장]")
    print("=" * 80)
    
    # HOLD 분석 결과를 룰 개선 후보로 저장
    backlog_data = {
        "analysis_date": datetime.now().isoformat(),
        "total_hold_cases": len(hold_cases),
        "hold_reasons": dict(hold_reasons.most_common()),
        "rule_ids": dict(rule_ids.most_common()),
        "sample_cases": [
            {
                "title": case.get('title', 'N/A'),
                "hold_reason": case.get('hold_reason', 'N/A'),
                "rule_id": case.get('sanity_check_rule_id') or case.get('routing_audit', {}).get('sanity_check_rule_id', 'N/A'),
                "route_evidence": case.get('route_evidence', [])
            }
            for case in hold_cases[:top_n]
        ],
        "recommendations": []
    }
    
    # 권장 사항 추가
    if hold_reasons:
        top_reason = hold_reasons.most_common(1)[0]
        backlog_data["recommendations"].append({
            "priority": "HIGH",
            "type": "RULE_ENHANCEMENT",
            "description": f"가장 빈번한 HOLD 사유: {top_reason[0][:100]} ({top_reason[1]}건)",
            "action": "룰 보강 또는 sector mapping 사전 확장 검토"
        })
    
    # 룰 개선 후보 파일 저장
    backlog_file = Path("reports") / f"hold_rule_backlog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    backlog_file.parent.mkdir(exist_ok=True)
    
    with open(backlog_file, 'w', encoding='utf-8') as f:
        json.dump(backlog_data, f, ensure_ascii=False, indent=2)
    
    print(f"  ✅ 룰 개선 후보 저장: {backlog_file}")
    
    # 6. 권장 사항
    print(f"\n" + "=" * 80)
    print("[권장 사항]")
    print("=" * 80)
    
    if hold_reasons:
        top_reason = hold_reasons.most_common(1)[0]
        print(f"1. 가장 빈번한 HOLD 사유: {top_reason[0][:50]}... ({top_reason[1]}건)")
        print("   → 룰 보강 또는 sector mapping 사전 확장 검토")
    
    print("2. HOLD 케이스는 '버림'이 아니라 '룰 학습 데이터'로 활용")
    print("   → 주기적으로 수동 검수하여 룰 개선")
    print("3. rule_id를 활용하여 룰별 성공률/실패률 추적")
    print(f"4. 룰 개선 후보는 {backlog_file}에 저장됨")


if __name__ == "__main__":
    analyze_hold_cases(top_n=10)

