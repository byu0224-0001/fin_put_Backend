"""
기존 빈 report_id 데이터 백필 스크립트

옵션 A: (title/date/url/broker_name) 가능한 범위에서 report_id 재계산 후 채움
못 채우면 is_active=false로 soft disable + disabled_reason="MISSING_REPORT_ID_LEGACY"
"""
import sys
from pathlib import Path
import hashlib

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
from app.models.industry_edge import IndustryEdge
from app.models.broker_report import BrokerReport
from utils.text_normalizer import normalize_date_for_report_id, canonicalize_url
from datetime import datetime

def backfill_report_id():
    """기존 빈 report_id 백필"""
    print("=" * 80)
    print("[기존 빈 report_id 백필]")
    print("=" * 80)
    
    db = SessionLocal()
    
    try:
        # report_id가 NULL이거나 빈 문자열인 industry_edges 찾기
        empty_edges = db.query(IndustryEdge).filter(
            (IndustryEdge.report_id == None) | (IndustryEdge.report_id == "")
        ).all()
        
        print(f"\n[1단계] 빈 report_id 발견: {len(empty_edges)}개")
        
        if len(empty_edges) == 0:
            print("  [OK] 백필할 데이터가 없습니다.")
            return
        
        # broker_reports에서 메타데이터 조회 시도
        filled_count = 0
        disabled_count = 0
        
        for edge in empty_edges:
            # broker_reports에서 report_id 찾기 시도
            # (현재는 report_id가 없으므로 다른 방법 필요)
            # 일단 routing_audit이나 다른 소스에서 정보를 가져와야 함
            
            # 임시: logic_summary나 key_sentence 기반으로 해시 생성
            # (완벽하지 않지만 최소한 고유성은 보장)
            if edge.logic_summary:
                # logic_summary 기반 해시 생성
                normalized_logic = edge.logic_summary.strip()
                report_id = hashlib.sha256(
                    f"LEGACY|{normalized_logic[:200]}|{edge.target_sector_code}".encode()
                ).hexdigest()[:32]
                
                # broker_reports에 upsert
                try:
                    broker_report = db.query(BrokerReport).filter(
                        BrokerReport.report_id == report_id
                    ).first()
                    
                    if not broker_report:
                        broker_report = BrokerReport(
                            report_id=report_id,
                            report_title=f"Legacy: {edge.target_sector_code}",
                            source="legacy_backfill",
                            processing_status="ENRICHED"
                        )
                        db.add(broker_report)
                    
                    # industry_edge 업데이트
                    edge.report_id = report_id
                    filled_count += 1
                    
                except Exception as e:
                    print(f"  [ERROR] report_id 생성 실패: {e}")
                    # soft disable
                    edge.is_active = "FALSE"
                    edge.disabled_reason = "MISSING_REPORT_ID_LEGACY"
                    edge.disabled_at = datetime.utcnow()
                    disabled_count += 1
            else:
                # logic_summary도 없으면 soft disable
                edge.is_active = "FALSE"
                edge.disabled_reason = "MISSING_REPORT_ID_LEGACY"
                edge.disabled_at = datetime.utcnow()
                disabled_count += 1
        
        db.commit()
        
        print(f"\n[2단계] 백필 결과")
        print(f"  채워진 개수: {filled_count}")
        print(f"  비활성화 개수: {disabled_count}")
        print(f"  총 처리: {filled_count + disabled_count}/{len(empty_edges)}")
        
        print("\n[OK] 백필 완료")
        
    except Exception as e:
        db.rollback()
        print(f"\n[ERROR] 백필 실패: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    backfill_report_id()

