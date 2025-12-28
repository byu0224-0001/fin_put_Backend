#!/usr/bin/env python3
"""
밸류체인 컬럼 마이그레이션 실행 스크립트
"""

import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db import get_db

def run_migration():
    """마이그레이션 실행"""
    db = next(get_db())
    
    try:
        print("=" * 80)
        print("밸류체인 컬럼 마이그레이션 실행")
        print("=" * 80)
        
        # 마이그레이션 SQL 읽기
        migration_file = os.path.join(
            os.path.dirname(__file__), '..', 'sql', 'migrations', 'add_value_chain_columns.sql'
        )
        
        with open(migration_file, 'r', encoding='utf-8') as f:
            migration_sql = f.read()
        
        print("\n마이그레이션 SQL 실행 중...")
        print("-" * 80)
        
        # SQL 실행
        db.execute(text(migration_sql))
        db.commit()
        
        print("[OK] 마이그레이션 완료")
        print("=" * 80)
        
    except Exception as e:
        print(f"마이그레이션 실패: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == '__main__':
    run_migration()

