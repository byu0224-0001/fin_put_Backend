"""Check C: Primary 섹터 개수 감소 원인 확인"""
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
    print("Check C: Primary 섹터 개수 감소 원인 확인")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    with engine.connect() as conn:
        # 전체 Primary 섹터 수
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM investor_sector
            WHERE is_primary = true
        """))
        total_primary = result.fetchone()[0]
        print(f"1. 현재 Primary 섹터 수: {total_primary:,}개")
        print()
        
        # 기업별 is_primary = True 개수 분포
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as primary_count,
                COUNT(*) as ticker_count
            FROM (
                SELECT ticker, COUNT(*) as primary_count
                FROM investor_sector
                WHERE is_primary = true
                GROUP BY ticker
            ) sub
        """))
        
        result = conn.execute(text("""
            SELECT 
                primary_count,
                COUNT(*) as ticker_count
            FROM (
                SELECT ticker, COUNT(*) as primary_count
                FROM investor_sector
                WHERE is_primary = true
                GROUP BY ticker
            ) sub
            GROUP BY primary_count
            ORDER BY primary_count
        """))
        
        print("2. 기업별 is_primary = True 개수 분포:")
        print("-" * 80)
        distribution = {}
        for row in result.fetchall():
            primary_count, ticker_count = row
            distribution[primary_count] = ticker_count
            print(f"  {primary_count}개: {ticker_count:,}개 기업")
        print()
        
        # 평균 계산
        total_tickers = sum(count * tickers for count, tickers in distribution.items())
        total_primary_sum = sum(count * tickers for count, tickers in distribution.items())
        avg_primary = total_primary_sum / total_tickers if total_tickers > 0 else 0
        
        print(f"3. 평균 is_primary = True 개수: {avg_primary:.2f}개")
        print()
        
        # 중복 확인
        if 1 in distribution:
            single_count = distribution[1]
            single_percentage = (single_count / total_tickers * 100) if total_tickers > 0 else 0
            print(f"4. 단일 Primary 섹터 기업: {single_count:,}개 ({single_percentage:.1f}%)")
        else:
            print("4. 단일 Primary 섹터 기업: 0개")
        print()
        
        if 2 in distribution or 3 in distribution:
            multi_count = sum(distribution.get(i, 0) for i in [2, 3, 4, 5] if i in distribution)
            print(f"5. 다중 Primary 섹터 기업: {multi_count:,}개")
            print()
            
            # 다중 Primary 섹터 기업 샘플
            result = conn.execute(text("""
                SELECT 
                    ticker,
                    COUNT(*) as primary_count
                FROM investor_sector
                WHERE is_primary = true
                GROUP BY ticker
                HAVING COUNT(*) > 1
                ORDER BY primary_count DESC
                LIMIT 10
            """))
            
            print("6. 다중 Primary 섹터 기업 샘플 (상위 10개):")
            print("-" * 80)
            for row in result.fetchall():
                ticker, primary_count = row
                print(f"  {ticker}: {primary_count}개 Primary 섹터")
            print()
        
        # 최종 결론
        print("=" * 80)
        print("7. 최종 결론")
        print("=" * 80)
        if avg_primary == 1.0:
            print("✅ 정상: 평균 1.0개 → 모든 기업이 단일 Primary 섹터")
            print("   → 데이터 품질 개선됨 (이전 중복 제거)")
        elif avg_primary < 1.5:
            print("✅ 양호: 평균 < 1.5개 → 대부분 단일 Primary 섹터")
            print("   → 데이터 품질 양호")
        else:
            print("⚠️ 주의: 평균 ≥ 1.5개 → 다중 Primary 섹터 기업 다수")
            print("   → Multi-sector 정책 확인 필요")
        print()

if __name__ == '__main__':
    main()

