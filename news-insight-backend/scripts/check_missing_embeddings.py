"""임베딩 없는 기업 확인"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

load_dotenv()

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
print("임베딩 없는 기업 확인")
print("=" * 80)

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT i.ticker, s.stock_name
        FROM investor_sector i
        LEFT JOIN company_embeddings e ON i.ticker = e.ticker
        LEFT JOIN stocks s ON i.ticker = s.ticker
        WHERE i.is_primary = true
          AND e.ticker IS NULL
        ORDER BY i.ticker
    """))
    
    missing = result.fetchall()
    
    print(f"\n임베딩 없는 기업: {len(missing)}개\n")
    
    for row in missing:
        ticker = row[0]
        name = row[1] or 'N/A'
        print(f"  {ticker}: {name}")
    
    if len(missing) == 0:
        print("\n✅ 모든 기업의 임베딩이 완료되었습니다!")
    else:
        print(f"\n⚠️ {len(missing)}개 기업의 임베딩이 아직 없습니다.")
    
    print("=" * 80)

