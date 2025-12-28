"""최종 상태 확인"""
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

load_dotenv()

user = quote_plus(os.getenv('POSTGRES_USER', 'postgres'))
pwd = quote_plus(os.getenv('POSTGRES_PASSWORD', 'postgres'))
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'newsdb')
url = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

engine = create_engine(url)

print("=" * 60)
print("섹터 재분류 최종 상태 확인")
print("=" * 60)

with engine.connect() as conn:
    # 총 기업 수
    result = conn.execute(text('SELECT COUNT(DISTINCT ticker) FROM company_details'))
    total_companies = result.fetchone()[0]
    
    # Primary 섹터 수
    result = conn.execute(text('SELECT COUNT(DISTINCT ticker) FROM investor_sector WHERE is_primary = true'))
    primary_sectors = result.fetchone()[0]
    
    # 벡터 DB 임베딩 수
    result = conn.execute(text('SELECT COUNT(*) FROM company_embeddings'))
    embeddings = result.fetchone()[0]
    
    # 최근 1시간 내 업데이트
    result = conn.execute(text("""
        SELECT COUNT(DISTINCT ticker) 
        FROM investor_sector 
        WHERE updated_at > NOW() - INTERVAL '1 hour'
    """))
    recent_updated = result.fetchone()[0]
    
    # KG Edge 수
    result = conn.execute(text('SELECT COUNT(*) FROM edges'))
    edge_count = result.fetchone()[0]
    
    print(f"총 기업 수: {total_companies}개")
    print(f"Primary 섹터 수: {primary_sectors}개")
    print(f"벡터 DB 임베딩: {embeddings}개")
    print(f"최근 1시간 업데이트: {recent_updated}개")
    print(f"KG Edge 수: {edge_count}개")
    
    if total_companies > 0:
        completion_rate = (primary_sectors / total_companies) * 100
        print(f"\n완료율: {completion_rate:.1f}% ({primary_sectors}/{total_companies})")
    
    # 최근 업데이트된 섹터 샘플
    result = conn.execute(text("""
        SELECT ticker, major_sector, sub_sector, confidence, updated_at
        FROM investor_sector 
        WHERE is_primary = true 
        ORDER BY updated_at DESC 
        LIMIT 10
    """))
    rows = result.fetchall()
    print("\n최근 업데이트된 Primary 섹터 (상위 10개):")
    for row in rows:
        print(f"  {row[0]}: {row[1]}/{row[2]} ({row[3]}) - {row[4]}")
    
    print("=" * 60)

