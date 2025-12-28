"""
driver_candidates 후보 생성 조건 확인 스크립트

후보가 0개인 원인 파악:
(A) DriverNormalizer의 "unknown term 추출"이 실제 리포트 텍스트에서 트리거되지 않음
(B) 트리거는 되지만 "쿨다운/상한/필터"에 걸려 모두 스킵됨
"""
import sys
from pathlib import Path
import json

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
from extractors.driver_normalizer import _extract_unknown_economic_terms
from sqlalchemy import func

def test_driver_candidates_conditions():
    """driver_candidates 후보 생성 조건 확인"""
    print("=" * 80)
    print("[driver_candidates 후보 생성 조건 확인]")
    print("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 1. 현재 후보 상태 확인
        print("\n[1단계] 현재 후보 상태")
        total_candidates = db.query(DriverCandidate).count()
        pending_candidates = db.query(DriverCandidate).filter(
            DriverCandidate.status == 'PENDING'
        ).count()
        approved_candidates = db.query(DriverCandidate).filter(
            DriverCandidate.status == 'APPROVED'
        ).count()
        
        print(f"  전체 후보: {total_candidates}개")
        print(f"  PENDING: {pending_candidates}개")
        print(f"  APPROVED: {approved_candidates}개")
        
        # 2. 후보 생성 조건 확인 (테스트 텍스트)
        print("\n[2단계] 후보 생성 조건 테스트")
        test_texts = [
            "정제마진 회복이 핵심입니다.",
            "크랙스프레드 개선 전망이 나왔습니다.",
            "D램 가격 상승이 예상됩니다.",
            "최신 AI 정책이 시장에 영향을 미칠 것입니다.",  # 신조어 테스트
            "ESG 규제 강화로 인한 비용 증가 우려",  # 약어 테스트
        ]
        
        print("\n[테스트 텍스트별 unknown term 추출 결과]")
        for i, text in enumerate(test_texts, 1):
            unknown_terms = _extract_unknown_economic_terms(text)
            print(f"  {i}. 입력: {text}")
            print(f"     추출된 terms: {unknown_terms}")
            if len(unknown_terms) == 0:
                print(f"     [WARNING] 추출된 term이 없습니다!")
        
        # 3. 쿨다운/상한 확인
        print("\n[3단계] 쿨다운/상한 확인")
        if total_candidates > 0:
            # 최근 24시간 내 등록된 후보 확인
            from datetime import datetime, timedelta
            recent_candidates = db.query(DriverCandidate).filter(
                DriverCandidate.created_at >= datetime.utcnow() - timedelta(hours=24)
            ).count()
            print(f"  최근 24시간 내 등록: {recent_candidates}개")
            
            # 리포트당 상한 확인 (최대 10개)
            report_counts = db.query(
                DriverCandidate.source_report_id,
                func.count(DriverCandidate.id).label('count')
            ).filter(
                DriverCandidate.source_report_id.isnot(None)
            ).group_by(
                DriverCandidate.source_report_id
            ).having(
                func.count(DriverCandidate.id) >= 10
            ).all()
            
            if report_counts:
                print(f"  리포트당 상한 도달 리포트: {len(report_counts)}개")
                for report_id, count in report_counts[:3]:
                    print(f"    - {report_id}: {count}개")
            else:
                print(f"  리포트당 상한 도달 리포트: 0개")
        
        # 4. 원인 분석
        print("\n" + "=" * 80)
        print("[원인 분석]")
        print("=" * 80)
        
        if total_candidates == 0:
            print("[원인 A 가능성] unknown term 추출이 트리거되지 않음")
            print("  - economic_keywords 패턴이 실제 리포트 텍스트와 매칭되지 않을 수 있음")
            print("  - 해결: 신조어/최신 정책/약어 포함 리포트로 테스트 필요")
        elif pending_candidates == 0 and total_candidates > 0:
            print("[원인 B 가능성] 트리거는 되지만 모두 처리됨")
            print("  - 모든 후보가 이미 APPROVED/REJECTED 상태")
            print("  - 해결: 새로운 리포트로 테스트 필요")
        else:
            print(f"[정상] PENDING 후보 {pending_candidates}개 존재")
        
        # 5. 권장 사항
        print("\n[권장 사항]")
        print("1. 의도적으로 신조어/최신 정책/약어 포함 리포트로 테스트")
        print("2. economic_keywords 패턴 확장 고려")
        print("3. 후보 생성 로그 확인 (enrichment 실행 시)")
        
    except Exception as e:
        print(f"\n[ERROR] 확인 실패: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    test_driver_candidates_conditions()

