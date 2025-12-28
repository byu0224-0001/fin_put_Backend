"""경제 변수 온톨로지 확인 스크립트"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.economic_variable import EconomicVariable

db = SessionLocal()
try:
    count = db.query(EconomicVariable).count()
    print(f"DB에 저장된 경제 변수 수: {count}개")
    
    print("\n샘플 5개:")
    samples = db.query(EconomicVariable).limit(5).all()
    for s in samples:
        print(f"  - {s.code}: {s.name_ko} (카테고리: {s.category})")
    
    print("\n카테고리별 분포:")
    from sqlalchemy import func
    category_counts = db.query(
        EconomicVariable.category,
        func.count(EconomicVariable.code)
    ).group_by(EconomicVariable.category).all()
    
    for cat, cnt in category_counts:
        print(f"  - {cat}: {cnt}개")
finally:
    db.close()

