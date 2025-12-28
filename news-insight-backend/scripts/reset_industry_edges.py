"""
Industry Edges 테이블 초기화 스크립트

DB를 깨끗이 비우고 재실행을 위한 준비
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
from app.models.broker_report import BrokerReport

def reset_industry_edges():
    """Industry Edges 테이블 초기화 및 broker_reports 상태 리셋"""
    db = SessionLocal()
    
    try:
        # 1. Industry Edges 삭제
        deleted_count = db.query(IndustryEdge).delete()
        print(f"[1] Industry Edges 삭제: {deleted_count}건")
        
        # 2. Broker Reports 상태 리셋 (ENRICHED → WAITING) - 테이블 존재 시에만
        try:
            updated_count = db.query(BrokerReport).filter(
                BrokerReport.processing_status == 'ENRICHED'
            ).update({'processing_status': 'WAITING'})
            print(f"[2] Broker Reports 상태 리셋 (ENRICHED → WAITING): {updated_count}건")
        except Exception as e:
            print(f"[2] Broker Reports 상태 리셋 스킵 (테이블 없음 또는 오류): {e}")
            updated_count = 0
        
        db.commit()
        print(f"\n[완료] Industry Edges 초기화 완료")
        print(f"  - 삭제된 Industry Edges: {deleted_count}건")
        if updated_count > 0:
            print(f"  - 리셋된 Broker Reports: {updated_count}건")
        
    except Exception as e:
        db.rollback()
        print(f"[오류] 초기화 실패: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    print("=" * 80)
    print("[Industry Edges 초기화]")
    print("=" * 80)
    print("\n[WARNING] 주의: 이 작업은 industry_edges 테이블의 모든 데이터를 삭제합니다.")
    print("계속하시겠습니까? (y/n): ", end="")
    
    response = input().strip().lower()
    if response == 'y':
        reset_industry_edges()
    else:
        print("취소되었습니다.")

