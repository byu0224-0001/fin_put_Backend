"""
DB 마이그레이션 실행 스크립트
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

def run_migration(migration_file_path=None):
    """DB 마이그레이션 실행"""
    if migration_file_path:
        migration_file = Path(migration_file_path)
        if not migration_file.is_absolute():
            migration_file = project_root / migration_file_path
    else:
        # 기본값 (하위 호환성)
        migration_file = project_root / "sql" / "migrations" / "update_industry_edges_dedup_key.sql"
    
    if not migration_file.exists():
        print(f"[오류] 마이그레이션 파일을 찾을 수 없습니다: {migration_file}")
        return False
    
    print("=" * 80)
    print("[DB 마이그레이션 실행]")
    print("=" * 80)
    print(f"파일: {migration_file}\n")
    
    try:
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # SQL 문장을 세미콜론으로 분리하고 주석 제거
        lines = sql_content.split('\n')
        statements = []
        current_statement = []
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            current_statement.append(line)
            if line.endswith(';'):
                statement = ' '.join(current_statement).rstrip(';').strip()
                if statement:
                    statements.append(statement)
                current_statement = []
        
        # 마지막 문장 처리
        if current_statement:
            statement = ' '.join(current_statement).strip()
            if statement:
                statements.append(statement)
        
        # 각 문장을 개별 트랜잭션으로 실행 (오류 시 롤백 방지)
        for i, statement in enumerate(statements, 1):
            if not statement:
                continue
            print(f"[{i}/{len(statements)}] 실행 중...")
            print(f"  SQL: {statement[:80]}...")
            
            # 주석 처리된 문장은 스킵
            if statement.strip().startswith('--'):
                print(f"  [SKIP] 주석 처리됨\n")
                continue
            
            try:
                with engine.begin() as conn:  # 각 문장마다 개별 트랜잭션
                    conn.execute(text(statement))
                print(f"  [OK] 성공\n")
            except Exception as e:
                error_str = str(e).lower()
                # 이미 존재하는 컬럼/제약/인덱스는 무시
                if any(keyword in error_str for keyword in ["already exists", "duplicate", "does not exist", "undefinedfunction"]):
                    print(f"  [SKIP] 이미 존재하거나 없음 (무시): {str(e)[:100]}\n")
                else:
                    print(f"  [ERROR] 실패: {e}\n")
                    # 치명적 오류가 아니면 계속 진행
                    if "constraint" not in error_str and "index" not in error_str:
                        raise
        
        print("[완료] 마이그레이션 실행 완료")
        return True
        
    except Exception as e:
        print(f"[오류] 마이그레이션 실행 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        migration_path = sys.argv[1]
        run_migration(migration_path)
    else:
        run_migration()
