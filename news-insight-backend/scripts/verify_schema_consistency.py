#!/usr/bin/env python3
"""
데이터베이스 스키마 일관성 확인 스크립트
company_embeddings 테이블의 필수 컬럼 존재 여부 확인
"""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from app.db import SessionLocal

# UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

load_dotenv()

def verify_schema():
    """스키마 일관성 확인"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("데이터베이스 스키마 일관성 확인")
        print("=" * 80)
        
        # 필수 컬럼 목록
        required_columns = {
            'ticker': 'VARCHAR',
            'text_hash': 'VARCHAR',
            'embedding_vector': 'USER-DEFINED',  # pgvector 타입
            'created_at': 'TIMESTAMP',
            'updated_at': 'TIMESTAMP',
            'last_status': 'VARCHAR',  # 상태 추적용
            'last_error_type': 'VARCHAR',  # 에러 타입
            'last_attempted_at': 'TIMESTAMP'  # 마지막 시도 시간
        }
        
        # 테이블 존재 확인
        result = db.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'company_embeddings'
            );
        """))
        table_exists = result.scalar()
        
        if not table_exists:
            print("\n❌ [ERROR] company_embeddings 테이블이 존재하지 않습니다!")
            print("   마이그레이션을 실행하세요: python scripts/run_migrations.py")
            return False
        
        print("\n✅ 테이블 존재: 확인됨")
        
        # 컬럼 확인
        result = db.execute(text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'company_embeddings'
            ORDER BY ordinal_position;
        """))
        
        existing_columns = {}
        for row in result:
            col_name = row[0]
            col_type = row[1]
            existing_columns[col_name] = col_type
        
        print("\n📋 현재 컬럼 목록:")
        for col_name, col_type in existing_columns.items():
            print(f"  - {col_name}: {col_type}")
        
        # 필수 컬럼 확인
        print("\n🔍 필수 컬럼 확인:")
        missing_columns = []
        optional_columns = []
        
        for col_name, expected_type in required_columns.items():
            if col_name in existing_columns:
                actual_type = existing_columns[col_name]
                if expected_type == 'USER-DEFINED' or expected_type.lower() in actual_type.lower():
                    print(f"  ✅ {col_name}: 존재함 ({actual_type})")
                else:
                    print(f"  ⚠️  {col_name}: 존재하지만 타입 불일치 (예상: {expected_type}, 실제: {actual_type})")
            else:
                if col_name in ['last_status', 'last_error_type', 'last_attempted_at']:
                    optional_columns.append(col_name)
                    print(f"  ⚠️  {col_name}: 없음 (선택적 컬럼, 마이그레이션 필요 가능)")
                else:
                    missing_columns.append(col_name)
                    print(f"  ❌ {col_name}: 없음 (필수 컬럼)")
        
        # 결과 요약
        print("\n" + "=" * 80)
        if missing_columns:
            print("❌ [ERROR] 필수 컬럼이 누락되었습니다:")
            for col in missing_columns:
                print(f"   - {col}")
            print("\n해결 방법:")
            print("   python scripts/run_migrations.py 실행")
            return False
        elif optional_columns:
            print("⚠️  [WARN] 선택적 컬럼이 누락되었습니다:")
            for col in optional_columns:
                print(f"   - {col}")
            print("\n권장: 마이그레이션 실행")
            print("   sql/migrations/add_embedding_status_columns.sql")
            return True
        else:
            print("✅ [OK] 모든 필수 컬럼이 존재합니다!")
            return True
        
    except Exception as e:
        print(f"\n❌ [ERROR] 스키마 확인 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == '__main__':
    success = verify_schema()
    sys.exit(0 if success else 1)

