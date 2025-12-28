"""
Enrichment 결과 검증 스크립트

1. 중복 스킵 카운트 확인
2. 섹터 매핑 품질 확인 (삼성전기 → SEC_TIRE 방지)
3. Sanity Check 발동 확인
"""
import sys
import os
import json
from pathlib import Path
from collections import defaultdict

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

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge

def verify_enrichment_results():
    """Enrichment 결과 검증"""
    print("=" * 80)
    print("[Enrichment 결과 검증]")
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
    
    # 2. 중복 스킵 카운트
    skipped_count = 0
    saved_count = 0
    hold_count = 0
    sanity_check_failed = 0
    
    for result in results:
        if result.get('hold_reason'):
            hold_count += 1
            if 'SANITY_CHECK' in result.get('hold_reason', ''):
                sanity_check_failed += 1
        elif result.get('industry_edge_enriched'):
            saved_count += 1
        else:
            skipped_count += 1
    
    print(f"[처리 결과]")
    print(f"  - 저장됨: {saved_count}건")
    print(f"  - 중복 스킵: {skipped_count}건")
    print(f"  - HOLD: {hold_count}건")
    print(f"    - Sanity Check 실패: {sanity_check_failed}건\n")
    
    # 3. DB에서 실제 저장된 데이터 확인
    db = SessionLocal()
    try:
        all_edges = db.query(IndustryEdge).all()
        print(f"[DB 상태]")
        print(f"  - 총 저장된 Industry Edges: {len(all_edges)}개\n")
        
        # 섹터별 분류
        sector_counts = defaultdict(int)
        for edge in all_edges:
            sector_counts[edge.target_sector_code] += 1
        
        print(f"[섹터별 분포]")
        for sector, count in sorted(sector_counts.items()):
            print(f"  - {sector}: {count}개")
        
        # 4. 삼성전기 관련 리포트 확인
        print(f"\n[삼성전기 관련 리포트 확인]")
        samsung_edges = []
        for edge in all_edges:
            if edge.logic_summary and ('삼성전기' in edge.logic_summary or '삼성전자' in edge.logic_summary):
                samsung_edges.append(edge)
        
        if samsung_edges:
            print(f"  - 발견: {len(samsung_edges)}건")
            for edge in samsung_edges:
                print(f"    - Sector: {edge.target_sector_code}, Driver: {edge.source_driver_code}")
                if edge.target_sector_code == "SEC_TIRE":
                    print(f"      [ERROR] 삼성전기가 SEC_TIRE로 매핑됨 (오류!)")
                else:
                    print(f"      [OK] 올바른 섹터 매핑")
        else:
            print(f"  - 발견: 0건 (DB에 없음)")
        
        # 5. 중복 논리 그룹 확인
        print(f"\n[중복 논리 그룹 확인]")
        import hashlib
        logic_groups = defaultdict(list)
        for edge in all_edges:
            if edge.logic_summary and edge.logic_fingerprint:
                logic_groups[edge.logic_fingerprint].append(edge)
        
        duplicate_groups = {k: v for k, v in logic_groups.items() if len(v) > 1}
        print(f"  - 총 논리 그룹: {len(logic_groups)}개")
        print(f"  - 중복 그룹 (2개 이상): {len(duplicate_groups)}개")
        
        if duplicate_groups:
            print(f"\n  [WARNING] 같은 논리가 여러 번 저장됨:")
            for logic_fp, edges in list(duplicate_groups.items())[:3]:
                print(f"    - Logic: {edges[0].logic_summary[:60]}...")
                print(f"      저장된 횟수: {len(edges)}회")
                for edge in edges:
                    print(f"        - Driver: {edge.source_driver_code}, Sector: {edge.target_sector_code}")
        else:
            print(f"\n  [OK] 중복 논리 그룹 없음 (정상)")
        
    finally:
        db.close()
    
    # 6. 최종 평가
    print(f"\n" + "=" * 80)
    print(f"[최종 평가]")
    print("=" * 80)
    
    if saved_count > 0:
        print(f"  [OK] 저장 성공: {saved_count}건")
    else:
        print(f"  [WARNING] 저장된 데이터 없음 (모두 중복 스킵 또는 HOLD)")
    
    if skipped_count > 0:
        print(f"  [OK] 중복 스킵 작동: {skipped_count}건")
    
    if sanity_check_failed > 0:
        print(f"  [OK] Sanity Check 작동: {sanity_check_failed}건 차단")
    
    # 삼성전기 → SEC_TIRE 오매핑 확인
    samsung_tire_count = sum(1 for edge in all_edges if edge.target_sector_code == "SEC_TIRE" and 
                            edge.logic_summary and '삼성전기' in edge.logic_summary)
    if samsung_tire_count == 0:
        print(f"  [OK] 삼성전기 → SEC_TIRE 오매핑 없음")
    else:
        print(f"  [ERROR] 삼성전기 → SEC_TIRE 오매핑 발견: {samsung_tire_count}건")

if __name__ == "__main__":
    verify_enrichment_results()

