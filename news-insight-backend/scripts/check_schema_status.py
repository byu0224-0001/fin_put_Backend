"""
스키마 상태 확인 스크립트

broker_reports 테이블, FK, unique 인덱스 확인
"""
import sys
from pathlib import Path

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    import os
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.db import engine
from sqlalchemy import inspect, text

def check_schema():
    """스키마 상태 확인"""
    print("=" * 80)
    print("[스키마 상태 확인]")
    print("=" * 80)
    
    inspector = inspect(engine)
    
    # 1. broker_reports 테이블 존재 여부
    print("\n[1단계] broker_reports 테이블 확인")
    tables = inspector.get_table_names()
    broker_exists = "broker_reports" in tables
    print(f"  broker_reports 테이블 존재: {broker_exists}")
    
    if broker_exists:
        # 컬럼 정보 확인
        columns = inspector.get_columns("broker_reports")
        print(f"  컬럼 수: {len(columns)}")
        for col in columns:
            if col['name'] in ['id', 'report_id', 'report_title', 'report_url']:
                print(f"    {col['name']}: {col['type']} (nullable={col['nullable']})")
        
        # 인덱스 확인
        indexes = inspector.get_indexes("broker_reports")
        print(f"  인덱스 수: {len(indexes)}")
        for idx in indexes:
            if 'report_id' in idx.get('column_names', []):
                print(f"    {idx['name']}: {idx['column_names']} (unique={idx.get('unique', False)})")
    
    # 2. industry_edges 테이블 확인
    print("\n[2단계] industry_edges 테이블 확인")
    industry_exists = "industry_edges" in tables
    print(f"  industry_edges 테이블 존재: {industry_exists}")
    
    if industry_exists:
        # report_id 컬럼 확인
        columns = inspector.get_columns("industry_edges")
        for col in columns:
            if col['name'] == 'report_id':
                print(f"  report_id 컬럼: {col['type']} (nullable={col['nullable']})")
        
        # FK 확인
        fks = inspector.get_foreign_keys("industry_edges")
        print(f"  Foreign Key 수: {len(fks)}")
        for fk in fks:
            if 'report_id' in fk.get('constrained_columns', []):
                print(f"    {fk['name']}: {fk['constrained_columns']} -> {fk['referred_table']}.{fk['referred_columns']}")
    
    # 3. 결과 요약
    print("\n" + "=" * 80)
    print("[결과 요약]")
    print("=" * 80)
    
    if not broker_exists:
        print("[ERROR] broker_reports 테이블이 없습니다!")
        print("  마이그레이션 실행 필요: sql/migrations/create_broker_reports_tables.sql")
        return False
    
    if industry_exists:
        # report_id FK 확인
        has_fk = any(
            fk.get('referred_table') == 'broker_reports' 
            for fk in inspector.get_foreign_keys("industry_edges")
        )
        if has_fk:
            print("[OK] industry_edges.report_id FK가 broker_reports.report_id를 참조합니다")
        else:
            print("[WARNING] industry_edges.report_id FK가 없거나 broker_reports를 참조하지 않습니다")
    
    return True

if __name__ == "__main__":
    check_schema()

