"""
멱등성 검증 스크립트

Test A: 동일 입력(동일 report_id / 동일 section_fingerprint) 재실행
- 기대: skipped가 거의 100% 나와야 정상 (DB write 0)

Test B: 유사 입력(제목 동일/본문 약간 다름, driver 다를 수 있음)
- 기대: skipped가 0일 수도 있음(정책상 허용)
- 대신 여기서는 "유사논리 관측지표"로 빈도만 봄
"""
import sys
import os
import json
import glob
from pathlib import Path
from collections import defaultdict
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

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge

def test_idempotency_a():
    """
    Test A: 동일 입력 재실행
    - 현재 DB에 저장된 industry_edges의 report_id/section_fingerprint를 기준으로
    - 동일한 리포트를 재처리했을 때 중복 스킵이 발생하는지 확인
    """
    print("=" * 80)
    print("[Test A: 동일 입력 재실행 멱등성 검증]")
    print("=" * 80)
    
    db = SessionLocal()
    try:
        # 현재 DB에 저장된 모든 industry_edges 조회
        all_edges = db.query(IndustryEdge).all()
        print(f"현재 DB에 저장된 Industry Edges: {len(all_edges)}개\n")
        
        if not all_edges:
            print("⚠️  DB에 저장된 데이터가 없습니다. 먼저 enrichment를 실행하세요.")
            return
        
        # report_id별로 그룹화 (같은 리포트에서 여러 edge가 나올 수 있음)
        report_id_groups = defaultdict(list)
        for edge in all_edges:
            if edge.report_id:
                report_id_groups[edge.report_id].append(edge)
        
        print(f"고유 report_id 수: {len(report_id_groups)}개\n")
        
        # 각 report_id에 대해 중복 체크 시뮬레이션
        duplicate_count = 0
        unique_count = 0
        
        for report_id, edges in report_id_groups.items():
            # 같은 report_id에서 (target_sector_code, logic_fingerprint) 조합 확인
            logic_combinations = {}
            for edge in edges:
                import hashlib
                logic_summary = edge.logic_summary or ""
                if logic_summary:
                    logic_fp = hashlib.sha256(logic_summary.encode('utf-8')).hexdigest()[:16]
                    key = (edge.target_sector_code, logic_fp)
                    
                    if key in logic_combinations:
                        duplicate_count += 1
                        print(f"  ⚠️  중복 감지: report_id={report_id}, sector={edge.target_sector_code}")
                    else:
                        logic_combinations[key] = edge
                        unique_count += 1
        
        print(f"\n[결과]")
        print(f"  - 고유 조합: {unique_count}개")
        print(f"  - 중복 조합: {duplicate_count}개")
        
        if duplicate_count > 0:
            print(f"\n  [WARNING] 경고: 동일 리포트 내에서 중복 조합이 발견되었습니다.")
            print(f"      이는 중복 제거 로직이 제대로 작동하지 않았을 수 있습니다.")
        else:
            print(f"\n  [OK] 동일 리포트 내 중복 없음 (정상)")
        
        # Test A 기대값: 동일 입력 재실행 시 skipped가 거의 100%
        print(f"\n[Test A 기대값]")
        print(f"  - 동일 리포트를 재처리하면: skipped ≈ 100% (DB write ≈ 0)")
        print(f"  - 실제 검증은 enrichment 스크립트 재실행으로 확인 필요")
        
    finally:
        db.close()


def test_idempotency_b():
    """
    Test B: 유사 입력 관측
    - 유사한 논리(logic_summary)가 다른 리포트에서 나오는 빈도 확인
    - driver가 달라도 logic_summary가 같으면 중복으로 간주되는지 확인
    """
    print("\n" + "=" * 80)
    print("[Test B: 유사 입력 관측]")
    print("=" * 80)
    
    db = SessionLocal()
    try:
        all_edges = db.query(IndustryEdge).all()
        print(f"현재 DB에 저장된 Industry Edges: {len(all_edges)}개\n")
        
        if not all_edges:
            print("⚠️  DB에 저장된 데이터가 없습니다.")
            return
        
        # logic_fingerprint별로 그룹화
        import hashlib
        logic_groups = defaultdict(list)
        
        for edge in all_edges:
            logic_summary = edge.logic_summary or ""
            if logic_summary:
                logic_fp = hashlib.sha256(logic_summary.encode('utf-8')).hexdigest()[:16]
                logic_groups[logic_fp].append(edge)
        
        # 같은 logic_fingerprint를 가진 그룹 찾기
        duplicate_logic_groups = {k: v for k, v in logic_groups.items() if len(v) > 1}
        
        print(f"총 논리 그룹: {len(logic_groups)}개")
        print(f"중복 논리 그룹 (2개 이상): {len(duplicate_logic_groups)}개\n")
        
        if duplicate_logic_groups:
            print("[중복 논리 그룹 상세]")
            for i, (logic_fp, edges) in enumerate(list(duplicate_logic_groups.items())[:5], 1):
                print(f"\n[{i}] 같은 논리, 다른 driver/sector:")
                print(f"    Logic Summary: {edges[0].logic_summary[:100]}...")
                for edge in edges:
                    print(f"      - Driver: {edge.source_driver_code}, Sector: {edge.target_sector_code}")
            
            print(f"\n  ⚠️  같은 논리(logic_summary)가 다른 driver/sector로 저장됨")
            print(f"      → 중복 키 정책이 (target_sector_code, logic_fingerprint)만 체크하므로")
            print(f"      → 같은 논리라도 sector가 다르면 별도 저장됨 (정책상 허용)")
        else:
            print("  [OK] 중복 논리 그룹 없음 (모든 논리가 고유)")
        
        # 유사 논리 관측 지표
        print(f"\n[유사 논리 관측 지표]")
        sector_logic_counts = defaultdict(int)
        for edge in all_edges:
            if edge.logic_summary:
                import hashlib
                logic_fp = hashlib.sha256(edge.logic_summary.encode('utf-8')).hexdigest()[:16]
                key = (edge.target_sector_code, logic_fp)
                sector_logic_counts[key] += 1
        
        duplicate_keys = {k: v for k, v in sector_logic_counts.items() if v > 1}
        print(f"  - (sector, logic_fingerprint) 조합: {len(sector_logic_counts)}개")
        print(f"  - 중복 조합: {len(duplicate_keys)}개")
        
        if duplicate_keys:
            print(f"\n  [WARNING] 경고: 같은 (sector, logic) 조합이 여러 번 저장됨")
            print(f"      → 중복 제거 로직이 제대로 작동하지 않았을 수 있습니다.")
        else:
            print(f"\n  [OK] 중복 조합 없음 (정상)")
        
    finally:
        db.close()


if __name__ == "__main__":
    test_idempotency_a()
    test_idempotency_b()

