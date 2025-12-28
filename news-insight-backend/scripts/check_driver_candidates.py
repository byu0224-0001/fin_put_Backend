"""
드라이버 후보 등록 확인 스크립트
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.db import SessionLocal
from app.models.driver_candidate import DriverCandidate

db = SessionLocal()
try:
    candidates = db.query(DriverCandidate).filter(
        DriverCandidate.status == 'PENDING'
    ).limit(10).all()
    
    print(f"Pending 후보 수: {len(candidates)}")
    print("\n상위 5개 후보:")
    for c in candidates[:5]:
        title = c.source_report_title[:50] if c.source_report_title else "N/A"
        print(f"  {c.candidate_text} | {title} | {c.occurrence_count}회")
finally:
    db.close()

