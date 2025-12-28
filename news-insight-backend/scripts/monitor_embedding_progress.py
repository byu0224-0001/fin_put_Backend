"""임베딩 배치 생성 진행 상황 모니터링 스크립트"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime, timedelta

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

load_dotenv()

# UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

user = quote_plus(os.getenv('POSTGRES_USER', 'postgres'))
pwd = quote_plus(os.getenv('POSTGRES_PASSWORD', 'postgres'))
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'newsdb')
url = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

engine = create_engine(url)

print("=" * 80)
print("임베딩 배치 생성 진행 상황 모니터링")
print("=" * 80)

with engine.connect() as conn:
    # 전체 통계
    result = conn.execute(text("""
        SELECT COUNT(DISTINCT ticker) AS total_count
        FROM investor_sector
        WHERE is_primary = true
    """))
    total_count = result.fetchone()[0]
    
    result = conn.execute(text("""
        SELECT COUNT(DISTINCT ticker) AS with_embedding_count
        FROM company_embeddings
    """))
    with_embedding_count = result.fetchone()[0]
    
    result = conn.execute(text("""
        SELECT COUNT(DISTINCT i.ticker) AS missing_count
        FROM investor_sector i
        LEFT JOIN company_embeddings e ON i.ticker = e.ticker
        WHERE i.is_primary = true
          AND e.ticker IS NULL
    """))
    missing_count = result.fetchone()[0]
    
    completed_count = total_count - missing_count
    progress_pct = (completed_count / total_count * 100) if total_count > 0 else 0
    
    print(f"\n📊 전체 통계:")
    print(f"  전체 기업 수: {total_count:,}개")
    print(f"  임베딩 완료: {completed_count:,}개 ({progress_pct:.1f}%)")
    print(f"  임베딩 미완료: {missing_count:,}개 ({100-progress_pct:.1f}%)")
    
    # 최근 생성 통계 (최근 1시간, 2시간, 24시간)
    for hours in [1, 2, 24]:
        result = conn.execute(text(f"""
            SELECT 
                COUNT(DISTINCT ticker) AS new_count,
                MIN(created_at) AS first_created,
                MAX(created_at) AS last_created
            FROM company_embeddings
            WHERE created_at >= NOW() - INTERVAL '{hours} hours'
        """))
        new_data = result.fetchone()
        new_count = new_data[0] or 0
        first_created = new_data[1]
        last_created = new_data[2]
        
        if new_count > 0 and first_created and last_created:
            elapsed = (last_created - first_created).total_seconds()
            if elapsed > 0:
                rate = new_count / elapsed  # 개/초
                remaining_time_sec = missing_count / rate if rate > 0 else 0
                remaining_time_min = remaining_time_sec / 60
                remaining_time_hour = remaining_time_sec / 3600
                
                print(f"\n⏱️  최근 {hours}시간 통계:")
                print(f"  새로 생성: {new_count:,}개")
                print(f"  생성 속도: {rate:.2f}개/초 ({rate*60:.1f}개/분)")
                print(f"  첫 생성: {first_created}")
                print(f"  마지막 생성: {last_created}")
                print(f"  예상 남은 시간: {remaining_time_min:.1f}분 ({remaining_time_hour:.2f}시간)")
                break
    
    # 상태 분포
    result = conn.execute(text("""
        SELECT last_status, COUNT(*) AS cnt
        FROM company_embeddings
        WHERE last_status IS NOT NULL
        GROUP BY last_status
        ORDER BY cnt DESC
    """))
    
    print(f"\n📈 임베딩 상태 분포:")
    for row in result.fetchall():
        print(f"  {row[0]}: {row[1]:,}개")
    
    # 오류 유형 분포
    result = conn.execute(text("""
        SELECT last_error_type, COUNT(*) AS cnt
        FROM company_embeddings
        WHERE last_error_type IS NOT NULL
        GROUP BY last_error_type
        ORDER BY cnt DESC
    """))
    
    error_rows = result.fetchall()
    if error_rows:
        print(f"\n⚠️  오류 유형 분포:")
        for row in error_rows:
            print(f"  {row[0]}: {row[1]:,}개")
    
    # 최근 실패한 기업 (최근 1시간)
    result = conn.execute(text("""
        SELECT ticker, last_status, last_error_type, last_attempted_at
        FROM company_embeddings
        WHERE last_status = 'API_ERROR'
          AND last_attempted_at >= NOW() - INTERVAL '1 hour'
        ORDER BY last_attempted_at DESC
        LIMIT 10
    """))
    
    failed_rows = result.fetchall()
    if failed_rows:
        print(f"\n❌ 최근 실패한 기업 (최근 1시간, 상위 10개):")
        for row in failed_rows:
            print(f"  {row[0]}: {row[1]} ({row[2]}) - {row[3]}")
    
    print("\n" + "=" * 80)

