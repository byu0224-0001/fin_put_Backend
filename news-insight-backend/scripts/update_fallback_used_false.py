"""fallback_used = 'FALSE' 기본값 업데이트"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

# UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()

user = quote_plus(os.getenv('POSTGRES_USER', 'postgres'))
pwd = quote_plus(os.getenv('POSTGRES_PASSWORD', 'postgres'))
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'newsdb')
url = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

engine = create_engine(url)

def main():
    print("=" * 80)
    print("fallback_used = 'FALSE' 기본값 업데이트")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    with engine.connect() as conn:
        # 업데이트 전 상태
        result = conn.execute(text("""
            SELECT 
                fallback_used,
                COUNT(*) as count
            FROM investor_sector
            WHERE is_primary = true
            GROUP BY fallback_used
            ORDER BY count DESC
        """))
        
        print("1. 업데이트 전 상태:")
        print("-" * 80)
        before_dist = {}
        for row in result.fetchall():
            fallback_used, count = row
            before_dist[fallback_used or 'NULL'] = count
            print(f"  {fallback_used or 'NULL'}: {count:,}개")
        print()
        
        # 업데이트 실행
        print("2. 업데이트 실행 중...")
        result = conn.execute(text("""
            UPDATE investor_sector
            SET fallback_used = 'FALSE'
            WHERE is_primary = true 
                AND fallback_used IS NULL
        """))
        updated_count = result.rowcount
        conn.commit()
        print(f"   업데이트된 레코드 수: {updated_count:,}개")
        print()
        
        # 업데이트 후 상태
        result = conn.execute(text("""
            SELECT 
                fallback_used,
                COUNT(*) as count
            FROM investor_sector
            WHERE is_primary = true
            GROUP BY fallback_used
            ORDER BY count DESC
        """))
        
        print("3. 업데이트 후 상태:")
        print("-" * 80)
        after_dist = {}
        for row in result.fetchall():
            fallback_used, count = row
            after_dist[fallback_used or 'NULL'] = count
            print(f"  {fallback_used or 'NULL'}: {count:,}개")
        print()
        
        # 최종 결론
        print("=" * 80)
        print("4. 최종 결론")
        print("=" * 80)
        false_count = after_dist.get('FALSE', 0)
        true_count = after_dist.get('TRUE', 0)
        null_count = after_dist.get('NULL', 0)
        
        if null_count == 0:
            print("✅ 모든 레코드가 'FALSE' 또는 'TRUE'로 설정됨")
            print(f"   - 'FALSE' (정상 분류): {false_count:,}개")
            print(f"   - 'TRUE' (Fallback 사용): {true_count:,}개")
        else:
            print(f"⚠️ NULL 레코드 {null_count:,}개 남아있음")
        print()

if __name__ == '__main__':
    main()

