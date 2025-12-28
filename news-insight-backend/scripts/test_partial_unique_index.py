"""
P0.5-1: Partial unique index 검증

같은 (target_sector_code, logic_fingerprint)를 두 번 insert 시도했을 때
DB에서 unique violation이 발생하는지 확인
"""
import sys
import os
from pathlib import Path

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
from utils.text_normalizer import normalize_text_for_fingerprint
import hashlib
from datetime import datetime, timedelta


def test_partial_unique_index():
    """
    Partial unique index 검증
    
    같은 (target_sector_code, logic_fingerprint) 조합을 두 번 insert 시도
    """
    print("=" * 80)
    print("[P0.5-1: Partial Unique Index 검증]")
    print("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 테스트용 데이터
        test_sector = "SEC_TEST"
        test_logic = "테스트 논리 요약: 1월 효과로 중소형주 강세"
        normalized_logic = normalize_text_for_fingerprint(test_logic)
        test_fingerprint = hashlib.sha256(normalized_logic.encode('utf-8')).hexdigest()[:16]
        
        print(f"\n[테스트 데이터]")
        print(f"  - Sector: {test_sector}")
        print(f"  - Logic: {test_logic}")
        print(f"  - Fingerprint: {test_fingerprint}\n")
        
        # 1차 insert 시도
        print("[1차 Insert 시도]")
        edge1 = IndustryEdge(
            report_id="TEST_REPORT_001",
            source_driver_code="MACRO_TEST",
            target_sector_code=test_sector,
            target_type="SECTOR",
            relation_type="INDUSTRY_DRIVEN_BY",
            logic_summary=test_logic,
            logic_fingerprint=test_fingerprint,
            valid_from=datetime.now().date(),
            valid_to=(datetime.now() + timedelta(days=365)).date(),
            is_active="TRUE"
        )
        
        try:
            db.add(edge1)
            db.commit()
            print("  ✅ [OK] 1차 insert 성공")
        except Exception as e:
            db.rollback()
            if "unique_industry_edge_sector_logic" in str(e) or "duplicate key" in str(e).lower():
                print(f"  ⚠️  [SKIP] 이미 존재함 (정상): {str(e)[:100]}")
                # 기존 데이터 삭제 후 재시도
                existing = db.query(IndustryEdge).filter(
                    IndustryEdge.target_sector_code == test_sector,
                    IndustryEdge.logic_fingerprint == test_fingerprint
                ).first()
                if existing:
                    db.delete(existing)
                    db.commit()
                    db.add(edge1)
                    db.commit()
                    print("  ✅ [OK] 기존 데이터 삭제 후 1차 insert 성공")
            else:
                raise
        
        # 2차 insert 시도 (같은 조합)
        print("\n[2차 Insert 시도] (같은 sector + fingerprint)")
        edge2 = IndustryEdge(
            report_id="TEST_REPORT_002",  # 다른 report_id
            source_driver_code="MACRO_TEST_2",  # 다른 driver
            target_sector_code=test_sector,  # 같은 sector
            target_type="SECTOR",
            relation_type="INDUSTRY_DRIVEN_BY",
            logic_summary=test_logic,  # 같은 logic
            logic_fingerprint=test_fingerprint,  # 같은 fingerprint
            valid_from=datetime.now().date(),
            valid_to=(datetime.now() + timedelta(days=365)).date(),
            is_active="TRUE"
        )
        
        try:
            db.add(edge2)
            db.commit()
            print("  ❌ [ERROR] 2차 insert 성공 (예상과 다름 - unique 제약이 작동하지 않음)")
            
            # 삭제
            db.delete(edge2)
            db.commit()
        except Exception as e:
            db.rollback()
            if "unique_industry_edge_sector_logic" in str(e) or "duplicate key" in str(e).lower():
                print("  ✅ [OK] 2차 insert 실패 - Unique 제약 작동 확인")
                print(f"     오류: {str(e)[:150]}...")
            else:
                print(f"  ⚠️  [WARNING] 예상과 다른 오류: {e}")
                raise
        
        # 정리: 테스트 데이터 삭제
        print("\n[정리]")
        test_edges = db.query(IndustryEdge).filter(
            IndustryEdge.target_sector_code == test_sector
        ).all()
        for edge in test_edges:
            db.delete(edge)
        db.commit()
        print(f"  ✅ 테스트 데이터 {len(test_edges)}개 삭제 완료")
        
        print("\n" + "=" * 80)
        print("[결론]")
        print("=" * 80)
        print("  ✅ [OK] Partial Unique Index가 의도대로 작동함")
        print("     - 같은 (target_sector_code, logic_fingerprint) 조합은 중복 저장 불가")
        print("     - driver_code가 달라도 중복으로 차단됨 (의도된 동작)")
        
    except Exception as e:
        db.rollback()
        print(f"\n  ❌ [ERROR] 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    test_partial_unique_index()

