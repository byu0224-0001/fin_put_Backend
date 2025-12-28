"""재분류 진행 상황 모니터링 스크립트"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime
import time

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

def check_progress():
    with engine.connect() as conn:
        # NULL L1 개수
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM investor_sector
            WHERE is_primary = true AND sector_l1 IS NULL
        """))
        null_l1 = result.fetchone()[0]
        
        # fallback_used = 'TRUE' 개수
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM investor_sector
            WHERE is_primary = true AND fallback_used = 'TRUE'
        """))
        fallback_count = result.fetchone()[0]
        
        # 최근 업데이트 (1분 이내)
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM investor_sector
            WHERE is_primary = true 
                AND updated_at > NOW() - INTERVAL '1 minute'
        """))
        recent_updates = result.fetchone()[0]
        
        return null_l1, fallback_count, recent_updates

def main():
    print("=" * 80)
    print("재분류 진행 상황 모니터링")
    print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    initial_null, initial_fallback, _ = check_progress()
    print(f"초기 상태:")
    print(f"  NULL L1: {initial_null:,}개")
    print(f"  fallback_used = 'TRUE': {initial_fallback:,}개")
    print()
    
    print("진행 상황 모니터링 중... (Ctrl+C로 종료)")
    print("-" * 80)
    
    try:
        while True:
            null_l1, fallback_count, recent_updates = check_progress()
            progress = ((initial_null - null_l1) / initial_null * 100) if initial_null > 0 else 0
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"NULL L1: {null_l1:,}개 ({progress:.1f}% 완료), "
                  f"fallback: {fallback_count:,}개, "
                  f"최근 1분 업데이트: {recent_updates:,}개")
            
            if null_l1 == 0:
                print()
                print("=" * 80)
                print("✅ 재분류 완료!")
                print("=" * 80)
                break
            
            time.sleep(30)  # 30초마다 확인
            
    except KeyboardInterrupt:
        print()
        print("모니터링 중단")
    
    # 최종 상태
    null_l1, fallback_count, _ = check_progress()
    print()
    print("=" * 80)
    print("최종 상태")
    print("=" * 80)
    print(f"NULL L1: {null_l1:,}개")
    print(f"fallback_used = 'TRUE': {fallback_count:,}개")
    print()

if __name__ == '__main__':
    main()

