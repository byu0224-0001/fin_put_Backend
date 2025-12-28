"""
삼성전기 → SEC_TIRE 매핑 확인 스크립트
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

def check_samsung_tire_mapping():
    """삼성전기 → SEC_TIRE 매핑 확인"""
    db = SessionLocal()
    try:
        # SEC_TIRE 섹터의 모든 엣지 확인
        tire_edges = db.query(IndustryEdge).filter(
            IndustryEdge.target_sector_code == "SEC_TIRE"
        ).all()
        
        print("=" * 80)
        print("[SEC_TIRE 섹터 매핑 확인]")
        print("=" * 80)
        print(f"총 {len(tire_edges)}건\n")
        
        for edge in tire_edges:
            print(f"[{edge.id}]")
            print(f"  - Driver: {edge.source_driver_code}")
            print(f"  - Sector: {edge.target_sector_code}")
            print(f"  - Logic Summary: {edge.logic_summary[:100] if edge.logic_summary else 'None'}...")
            print(f"  - Key Sentence: {edge.key_sentence[:100] if edge.key_sentence else 'None'}...")
            print(f"  - Report ID: {edge.report_id}")
            print()
            
            # 삼성전기 관련 키워드 확인
            if edge.logic_summary and '삼성전기' in edge.logic_summary:
                print(f"  [ERROR] 삼성전기 키워드 발견!")
            if edge.key_sentence and '삼성전기' in edge.key_sentence:
                print(f"  [ERROR] 삼성전기 키워드 발견!")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_samsung_tire_mapping()

