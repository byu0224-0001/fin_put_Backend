"""DB 스키마 확인: fallback_used, fallback_type, sector_l1"""
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
    print("DB 스키마 확인: fallback_used, fallback_type, sector_l1")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    with engine.connect() as conn:
        # investor_sector 테이블 컬럼 정보
        result = conn.execute(text("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_name = 'investor_sector'
                AND column_name IN ('fallback_used', 'fallback_type', 'sector_l1', 'major_sector')
            ORDER BY column_name
        """))
        
        print("1. 컬럼 정보:")
        print("-" * 80)
        columns = {}
        for row in result.fetchall():
            col_name, data_type, is_nullable, col_default = row
            columns[col_name] = {
                'type': data_type,
                'nullable': is_nullable,
                'default': col_default
            }
            print(f"  {col_name}:")
            print(f"    타입: {data_type}")
            print(f"    NULL 허용: {is_nullable}")
            print(f"    기본값: {col_default}")
            print()
        
        # 2. fallback_used 타입 확인
        print("=" * 80)
        print("2. fallback_used 타입 확인")
        print("=" * 80)
        print()
        
        if 'fallback_used' in columns:
            fallback_type = columns['fallback_used']['type']
            print(f"현재 타입: {fallback_type}")
            
            if fallback_type in ['character varying', 'varchar', 'text']:
                print("⚠️ VARCHAR 타입 - 문자열 저장 가능")
                print("   → 코드에서 'UNKNOWN' 같은 문자열을 넣을 수 있음")
                print("   → 하지만 설계상 BOOLEAN이어야 함")
            elif fallback_type in ['boolean', 'bool']:
                print("✅ BOOLEAN 타입 - True/False만 가능")
                print("   → 코드에서 'UNKNOWN' 같은 문자열을 넣으면 에러 발생!")
                print("   → 이게 현재 버그의 원인일 가능성이 높음")
            else:
                print(f"⚠️ 알 수 없는 타입: {fallback_type}")
        else:
            print("❌ fallback_used 컬럼이 없습니다!")
        print()
        
        # 3. fallback_type 컬럼 확인
        print("=" * 80)
        print("3. fallback_type 컬럼 확인")
        print("=" * 80)
        print()
        
        if 'fallback_type' in columns:
            print("✅ fallback_type 컬럼 존재")
            print(f"   타입: {columns['fallback_type']['type']}")
        else:
            print("❌ fallback_type 컬럼이 없습니다!")
            print("   → 마이그레이션 필요")
        print()
        
        # 4. sector_l1 컬럼 확인
        print("=" * 80)
        print("4. sector_l1 컬럼 확인")
        print("=" * 80)
        print()
        
        if 'sector_l1' in columns:
            print("✅ sector_l1 컬럼 존재")
            print(f"   타입: {columns['sector_l1']['type']}")
            print(f"   NULL 허용: {columns['sector_l1']['nullable']}")
            if columns['sector_l1']['nullable'] == 'YES':
                print("   ⚠️ NULL 허용 - NOT NULL 제약조건 없음")
            else:
                print("   ✅ NOT NULL 제약조건 있음")
        else:
            print("❌ sector_l1 컬럼이 없습니다!")
        print()
        
        # 5. 현재 데이터 샘플 확인
        print("=" * 80)
        print("5. 현재 데이터 샘플 확인")
        print("=" * 80)
        print()
        
        result = conn.execute(text("""
            SELECT 
                ticker,
                sector_l1,
                major_sector,
                fallback_used,
                fallback_type
            FROM investor_sector
            WHERE is_primary = true
            ORDER BY updated_at DESC
            LIMIT 5
        """))
        
        print("최근 업데이트된 기업 샘플:")
        for row in result.fetchall():
            ticker, sector_l1, major_sector, fallback_used, fallback_type = row
            print(f"  {ticker}:")
            print(f"    sector_l1: {sector_l1}")
            print(f"    major_sector: {major_sector}")
            print(f"    fallback_used: {fallback_used} (타입: {type(fallback_used).__name__})")
            print(f"    fallback_type: {fallback_type}")
            print()
        
        # 6. 최종 권장사항
        print("=" * 80)
        print("6. 최종 권장사항")
        print("=" * 80)
        print()
        
        if 'fallback_used' in columns:
            if columns['fallback_used']['type'] in ['boolean', 'bool']:
                print("✅ fallback_used는 BOOLEAN 타입")
                print("   → 코드에서 True/False만 사용해야 함")
                print("   → 'UNKNOWN' 같은 문자열 사용 시 에러 발생")
            else:
                print("⚠️ fallback_used가 VARCHAR 타입")
                print("   → 설계상 BOOLEAN으로 변경 권장")
        
        if 'fallback_type' not in columns:
            print("❌ fallback_type 컬럼 없음")
            print("   → 마이그레이션 필요:")
            print("     ALTER TABLE investor_sector ADD COLUMN fallback_type VARCHAR(20);")
        
        if 'sector_l1' in columns:
            if columns['sector_l1']['nullable'] == 'YES':
                print("⚠️ sector_l1이 NULL 허용")
                print("   → NOT NULL 제약조건 추가 권장")
        
        print()
        print("=" * 80)

if __name__ == '__main__':
    main()

