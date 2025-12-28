"""
DB 상태 확인 스크립트 (삭제 전/후 비교용)
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.broker_report import BrokerReport
from app.models.industry_edge import IndustryEdge
from app.models.driver_candidate import DriverCandidate

def check_db_status():
    db = SessionLocal()
    
    try:
        cutoff = datetime.now() - timedelta(days=2)
        
        # broker_reports 확인
        br_count = db.query(BrokerReport).filter(
            BrokerReport.source.in_(['naver', 'kirs']),
            BrokerReport.created_at >= cutoff
        ).count()
        
        # industry_edges 확인
        ie_count = db.query(IndustryEdge).filter(
            IndustryEdge.created_at >= cutoff
        ).count()
        
        # driver_candidates 확인
        dc_count = db.query(DriverCandidate).filter(
            DriverCandidate.created_at >= cutoff
        ).count()
        
        print("=" * 80)
        print("[DB 상태 확인]")
        print("=" * 80)
        print(f"최근 2일 이내 데이터:")
        print(f"  - broker_reports (naver/kirs): {br_count}개")
        print(f"  - industry_edges: {ie_count}개")
        print(f"  - driver_candidates: {dc_count}개")
        
        # industry_edges의 report_id 확인
        if ie_count > 0:
            edges = db.query(IndustryEdge).filter(
                IndustryEdge.created_at >= cutoff
            ).limit(5).all()
            print(f"\nindustry_edges report_id 샘플:")
            for e in edges:
                report_id_str = e.report_id if e.report_id else "(NULL)"
                print(f"  - {report_id_str}")
            
            # broker_reports와 연결되지 않은 orphan edges 확인
            orphan_count = 0
            for e in edges:
                if e.report_id:
                    br = db.query(BrokerReport).filter(
                        BrokerReport.report_id == e.report_id
                    ).first()
                    if not br:
                        orphan_count += 1
            
            if orphan_count > 0:
                print(f"\n[참고] broker_reports와 연결되지 않은 industry_edges: {orphan_count}개 (다른 소스일 수 있음)")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_db_status()

