"""confidence 필드 길이 확인 스크립트"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    result = db.execute(text("""
        SELECT character_maximum_length 
        FROM information_schema.columns 
        WHERE table_name='investor_sector' AND column_name='confidence'
    """))
    row = result.fetchone()
    if row:
        print(f"Current confidence field length: {row[0]}")
        if row[0] and row[0] < 50:
            print("⚠️  필드 길이가 50 미만입니다. 마이그레이션이 필요합니다.")
            print("다음 명령어 실행:")
            print("ALTER TABLE investor_sector ALTER COLUMN confidence TYPE VARCHAR(50);")
        else:
            print("✅ 필드 길이가 충분합니다.")
    else:
        print("❌ confidence 필드를 찾을 수 없습니다.")
finally:
    db.close()

