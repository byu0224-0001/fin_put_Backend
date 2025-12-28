"""Check 2: 임베딩 커버리지 최종 확인"""
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
    print("Check 2: 임베딩 커버리지 최종 확인")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    with engine.connect() as conn:
        # 전체 기업 수 (Primary 섹터 기준)
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT ticker) 
            FROM investor_sector
            WHERE is_primary = true
        """))
        total_companies = result.fetchone()[0]
        print(f"1. 전체 기업 수 (Primary 섹터 기준): {total_companies:,}개")
        print()
        
        # 임베딩 있는 기업 수
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT ticker) 
            FROM company_embeddings
        """))
        with_embeddings = result.fetchone()[0]
        print(f"2. 임베딩 있는 기업 수: {with_embeddings:,}개")
        print()
        
        # 임베딩 커버리지
        coverage = (with_embeddings / total_companies * 100) if total_companies > 0 else 0
        print(f"3. 임베딩 커버리지: {coverage:.2f}%")
        print()
        
        # 임베딩 없는 기업
        missing_count = total_companies - with_embeddings
        print(f"4. 임베딩 없는 기업: {missing_count:,}개")
        print()
        
        # 해석 기준
        print("=" * 80)
        print("5. 해석 기준")
        print("=" * 80)
        if coverage >= 95:
            print("✅ ≥ 95% → 바로 밸류체인 진행 가능")
        elif coverage >= 90:
            print("⚠️ 90~95% → 일부 재생성 후 진행")
        else:
            print("❌ < 90% → 임베딩 파이프라인 보완 필요")
        print()
        
        # 임베딩 없는 기업 샘플
        if missing_count > 0:
            result = conn.execute(text("""
                SELECT DISTINCT i.ticker, s.stock_name
                FROM investor_sector i
                LEFT JOIN company_embeddings e ON i.ticker = e.ticker
                LEFT JOIN stocks s ON i.ticker = s.ticker
                WHERE i.is_primary = true
                    AND e.ticker IS NULL
                ORDER BY i.ticker
                LIMIT 10
            """))
            
            print("6. 임베딩 없는 기업 샘플 (상위 10개):")
            print("-" * 80)
            for row in result.fetchall():
                ticker, stock_name = row
                print(f"  {ticker} ({stock_name})")
            print()
        
        # 최종 결론
        print("=" * 80)
        print("7. 최종 결론")
        print("=" * 80)
        if coverage >= 95:
            print("✅ 밸류체인 분석 준비 완료")
            print("   → value chain 분류 진행 가능")
        else:
            print("⚠️ 임베딩 커버리지 부족")
            print(f"   → {missing_count:,}개 기업 임베딩 재생성 필요")
        print()

if __name__ == '__main__':
    main()

