#!/usr/bin/env python3
"""
company_embeddings 테이블 상태 확인 스크립트
"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from app.db import SessionLocal

# UTF-8 인코딩 설정 (Windows 환경)
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

load_dotenv()
db = SessionLocal()

try:
    print("=" * 80)
    print("company_embeddings 테이블 상태 확인")
    print("=" * 80)
    
    # 1. 테이블 존재 확인
    result = db.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'company_embeddings'
        );
    """))
    table_exists = result.scalar()
    print(f"\n✅ 테이블 존재: {table_exists}")
    
    if not table_exists:
        print("\n❌ company_embeddings 테이블이 존재하지 않습니다!")
        print("   마이그레이션을 실행하세요: python scripts/run_migrations.py")
        sys.exit(1)
    
    # 2. 컬럼 확인
    result = db.execute(text("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'company_embeddings'
        ORDER BY ordinal_position;
    """))
    print("\n📋 컬럼 목록:")
    columns = []
    for row in result:
        print(f"  - {row[0]}: {row[1]}")
        columns.append(row[0])
    
    # 3. 레코드 수 확인
    result = db.execute(text("SELECT COUNT(*) FROM company_embeddings;"))
    count = result.scalar()
    print(f"\n📊 총 레코드 수: {count:,}")
    
    # 4. embedding_vector가 NULL이 아닌 레코드 수
    result = db.execute(text("""
        SELECT COUNT(*) 
        FROM company_embeddings 
        WHERE embedding_vector IS NOT NULL;
    """))
    non_null_count = result.scalar()
    print(f"📊 embedding_vector NOT NULL 레코드 수: {non_null_count:,}")
    
    # 5. 샘플 데이터 확인
    if count > 0:
        result = db.execute(text("""
            SELECT ticker, text_hash, 
                   created_at, updated_at,
                   CASE WHEN embedding_vector IS NULL THEN 'NULL' ELSE 'NOT NULL' END as embedding_status
            FROM company_embeddings 
            ORDER BY updated_at DESC
            LIMIT 5;
        """))
        print("\n🔍 최근 샘플 데이터 (최대 5개):")
        for row in result:
            hash_preview = row[1][:16] + '...' if row[1] and len(row[1]) > 16 else (row[1] or 'None')
            print(f"  - {row[0]}: text_hash={hash_preview}, created={row[2]}, embedding={row[4]}")
    
    # 6. 컬럼 존재 여부 확인
    has_status_columns = 'last_status' in columns and 'last_error_type' in columns and 'last_attempted_at' in columns
    print(f"\n⚠️  상태 컬럼 존재 여부:")
    print(f"  - last_status: {'✅' if 'last_status' in columns else '❌'}")
    print(f"  - last_error_type: {'✅' if 'last_error_type' in columns else '❌'}")
    print(f"  - last_attempted_at: {'✅' if 'last_attempted_at' in columns else '❌'}")
    
    if not has_status_columns:
        print("\n❌ 상태 컬럼이 없습니다! 마이그레이션을 실행해야 합니다:")
        print("   python scripts/run_migrations.py")
        print("\n또는 수동으로 실행:")
        print("   psql -d newsdb -f sql/migrations/add_embedding_status_columns.sql")
    
    # 7. pgvector 확장 확인
    result = db.execute(text("""
        SELECT EXISTS (
            SELECT 1 FROM pg_extension WHERE extname = 'vector'
        );
    """))
    has_vector_extension = result.scalar()
    print(f"\n🔧 pgvector 확장: {'✅' if has_vector_extension else '❌'}")
    
    print("\n" + "=" * 80)
    if non_null_count == 0:
        print("⚠️  경고: embedding_vector가 있는 레코드가 없습니다!")
        print("   이전 실행에서 저장이 실패했을 수 있습니다.")
    elif non_null_count < count:
        print(f"⚠️  경고: {count - non_null_count}개의 레코드가 embedding_vector가 NULL입니다.")
    else:
        print("✅ 모든 레코드가 embedding_vector를 가지고 있습니다.")
    print("=" * 80)
    
finally:
    db.close()
