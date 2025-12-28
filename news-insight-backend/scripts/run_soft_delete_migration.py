"""
Soft Delete 마이그레이션 실행 스크립트
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

from app.db import engine
from sqlalchemy import text

def run_migration():
    """soft delete 컬럼 추가 마이그레이션"""
    print("=" * 80)
    print("[Soft Delete 마이그레이션 실행]")
    print("=" * 80)
    
    statements = [
        "ALTER TABLE industry_edges ADD COLUMN IF NOT EXISTS is_active VARCHAR(10) DEFAULT 'TRUE'",
        "ALTER TABLE industry_edges ADD COLUMN IF NOT EXISTS disabled_reason TEXT",
        "ALTER TABLE industry_edges ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMP",
        "CREATE INDEX IF NOT EXISTS idx_industry_edges_is_active ON industry_edges(is_active) WHERE is_active = 'TRUE'"
    ]
    
    with engine.begin() as conn:
        for i, statement in enumerate(statements, 1):
            print(f"[{i}/{len(statements)}] 실행 중...")
            print(f"  SQL: {statement[:60]}...")
            try:
                conn.execute(text(statement))
                print(f"  [OK] 성공\n")
            except Exception as e:
                error_str = str(e).lower()
                if "already exists" in error_str or "duplicate" in error_str:
                    print(f"  [SKIP] 이미 존재함 (무시): {str(e)[:100]}\n")
                else:
                    print(f"  [ERROR] 실패: {e}\n")
                    raise
    
    print("[완료] Soft Delete 마이그레이션 실행 완료")

if __name__ == "__main__":
    run_migration()

