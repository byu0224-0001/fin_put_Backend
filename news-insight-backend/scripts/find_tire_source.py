"""
SEC_TIRE 엣지의 출처 확인
"""
import sys
import os
import json
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

def find_tire_source():
    """SEC_TIRE 엣지의 출처 확인"""
    db = SessionLocal()
    try:
        # SEC_TIRE 섹터의 모든 엣지 확인
        tire_edges = db.query(IndustryEdge).filter(
            IndustryEdge.target_sector_code == "SEC_TIRE"
        ).all()
        
        print("=" * 80)
        print("[SEC_TIRE 엣지 출처 확인]")
        print("=" * 80)
        
        for edge in tire_edges:
            print(f"\n[Edge ID: {edge.id}]")
            print(f"  - Driver: {edge.source_driver_code}")
            print(f"  - Sector: {edge.target_sector_code}")
            print(f"  - Logic Summary: {edge.logic_summary[:150] if edge.logic_summary else 'None'}...")
            print(f"  - Created At: {edge.created_at}")
            print(f"  - Report ID: {edge.report_id}")
            
            # 최신 enrichment_results에서 해당 리포트 찾기
            result_files = list(Path("reports").glob("enrichment_results_*.json"))
            if result_files:
                latest_file = max(result_files, key=lambda p: p.stat().st_mtime)
                with open(latest_file, 'r', encoding='utf-8') as f:
                    results = json.load(f)
                
                # logic_summary로 매칭
                matching_reports = []
                for r in results:
                    if r.get('industry_logic') and r['industry_logic'].get('logic_summary'):
                        if edge.logic_summary and edge.logic_summary[:100] in r['industry_logic']['logic_summary']:
                            matching_reports.append(r)
                
                if matching_reports:
                    print(f"\n  [매칭된 리포트]")
                    for r in matching_reports:
                        print(f"    - Title: {r.get('title', 'N/A')[:60]}...")
                        print(f"    - Enriched: {r.get('industry_edge_enriched', False)}")
                        print(f"    - Hold Reason: {r.get('hold_reason', 'None')}")
                else:
                    print(f"\n  [매칭된 리포트 없음] (이전 실행에서 저장된 데이터일 수 있음)")
        
    finally:
        db.close()

if __name__ == "__main__":
    find_tire_source()

