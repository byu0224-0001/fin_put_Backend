"""
driver_candidates 승인 루프 E2E 테스트

(후보 생성 → 승인/병합 → 재처리 → DRIVEN_BY 생성)이 한 번이라도 돌아가는지 확인
"""
import sys
from pathlib import Path

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    import os
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.db import SessionLocal
from app.models.driver_candidate import DriverCandidate
from app.models.edge import Edge
from sqlalchemy import func

def test_driver_candidates_e2e():
    """driver_candidates 승인 루프 E2E 테스트"""
    print("=" * 80)
    print("[driver_candidates 승인 루프 E2E 테스트]")
    print("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 1. 후보가 실제로 쌓였는지 확인
        print("\n[1단계] 후보 생성 확인")
        candidates = db.query(DriverCandidate).filter(
            DriverCandidate.status == 'PENDING'
        ).all()
        
        print(f"  PENDING 상태 후보: {len(candidates)}개")
        
        if len(candidates) == 0:
            print("  [WARNING] PENDING 상태 후보가 없습니다.")
            print("  enrichment를 실행하여 후보를 생성하세요.")
            return False
        
        # 샘플 후보 출력
        print("\n[샘플 후보 (최대 3개)]")
        for i, candidate in enumerate(candidates[:3], 1):
            print(f"  {i}. ID: {candidate.id}, text: {candidate.candidate_text}, confidence: {candidate.confidence}")
        
        # 2. 승인/병합 전 DRIVEN_BY 카운트 확인
        print("\n[2단계] 승인 전 DRIVEN_BY 카운트 확인")
        before_count = db.query(Edge).filter(
            Edge.relation_type == "DRIVEN_BY"
        ).count()
        print(f"  DRIVEN_BY edges: {before_count}개")
        
        # 3. 승인/병합 안내
        print("\n[3단계] 승인/병합 안내")
        print("  다음 명령으로 후보를 승인하세요:")
        print(f"  python scripts/approve_driver_candidates.py")
        print("\n  또는 직접 승인:")
        print(f"  python scripts/approve_driver_candidates.py --approve {candidates[0].id} --driver-code <기존_드라이버_코드>")
        
        # 4. 재처리 안내
        print("\n[4단계] 재처리 안내")
        print("  승인 후 다음 명령으로 재처리하세요:")
        print("  python scripts/enrich_edges_from_reports.py --input <파일> --limit 5")
        
        # 5. 검증 기준
        print("\n" + "=" * 80)
        print("[검증 기준]")
        print("=" * 80)
        print("1. 후보가 실제로 쌓임 ✅")
        print(f"   - PENDING 상태 후보: {len(candidates)}개")
        print("\n2. approve 스크립트로 승인/병합 1건 수행 (수동 필요)")
        print("\n3. 재처리 후 DRIVEN_BY가 0→1 이상 증가 확인 (수동 필요)")
        print(f"   - 현재 DRIVEN_BY: {before_count}개")
        print("   - 재처리 후 증가량 확인 필요")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    test_driver_candidates_e2e()

