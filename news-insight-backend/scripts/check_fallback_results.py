"""Fallback 결과 확인 스크립트"""
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
    print("Fallback 결과 확인")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    with engine.connect() as conn:
        # NULL L1 카운트
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM investor_sector
            WHERE is_primary = true AND sector_l1 IS NULL
        """))
        null_l1_count = result.fetchone()[0]
        print(f"1. NULL L1 개수: {null_l1_count:,}개")
        
        # NULL L2 카운트
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM investor_sector
            WHERE is_primary = true AND sector_l2 IS NULL
        """))
        null_l2_count = result.fetchone()[0]
        print(f"2. NULL L2 개수: {null_l2_count:,}개")
        print()
        
        # fallback_used 및 fallback_type 카운트
        result = conn.execute(text("""
            SELECT 
                COUNT(*) FILTER (WHERE fallback_used IS NOT NULL AND fallback_used = 'TRUE') AS has_fallback,
                COUNT(*) FILTER (WHERE fallback_type = 'RULE') AS rule_fallback,
                COUNT(*) FILTER (WHERE fallback_type = 'TOP1') AS top1_fallback,
                COUNT(*) FILTER (WHERE fallback_type = 'KRX') AS krx_fallback,
                COUNT(*) FILTER (WHERE fallback_type = 'UNKNOWN') AS unknown_fallback,
                COUNT(*) AS total
            FROM investor_sector
            WHERE is_primary = true
        """))
        row = result.fetchone()
        has_fallback, rule_fallback, top1_fallback, krx_fallback, unknown_fallback, total = row
        
        print("3. fallback_used 및 fallback_type 필드 상태:")
        print(f"   전체 Primary 섹터: {total:,}개")
        print(f"   fallback_used = 'TRUE': {has_fallback:,}개 ({has_fallback/total*100:.1f}%)")
        print(f"   fallback_type 분포:")
        print(f"     RULE: {rule_fallback:,}개")
        print(f"     TOP1: {top1_fallback:,}개")
        print(f"     KRX: {krx_fallback:,}개")
        print(f"     UNKNOWN: {unknown_fallback:,}개")
        print()
        
        # Fallback 사용 기업 샘플
        if has_fallback > 0:
            result = conn.execute(text("""
                SELECT 
                    i.ticker,
                    s.stock_name,
                    i.sector_l1,
                    i.sector_l2,
                    i.fallback_used,
                    i.fallback_type,
                    i.confidence,
                    i.updated_at
                FROM investor_sector i
                JOIN stocks s ON i.ticker = s.ticker
                WHERE i.is_primary = true 
                    AND i.fallback_used = 'TRUE'
                ORDER BY i.updated_at DESC
                LIMIT 10
            """))
            
            print("4. Fallback 사용 기업 샘플 (최근 10개):")
            for row in result.fetchall():
                ticker, stock_name, sector_l1, sector_l2, fallback_used, fallback_type, confidence, updated_at = row
                if updated_at.tzinfo:
                    updated_at = updated_at.replace(tzinfo=None)
                time_diff = datetime.now() - updated_at
                print(f"   {ticker} ({stock_name}): {sector_l1}/{sector_l2}, fallback_type={fallback_type}, conf={confidence}, {time_diff} 전")
            print()
        
        # 성공 조건 확인
        print("=" * 80)
        print("5. 성공 조건 확인")
        print("=" * 80)
        
        # UNKNOWN 비율 계산 (전체 대비)
        unknown_ratio = (unknown_fallback / total * 100) if total > 0 else 0
        
        success_conditions = {
            'NULL L1 = 0': null_l1_count == 0,
            'fallback_used = TRUE 레코드 존재': has_fallback > 0,
            'fallback_type 분포 정상': (rule_fallback > 0 or top1_fallback > 0 or krx_fallback > 0 or unknown_fallback > 0),
            'UNKNOWN 비율 < 95%': unknown_ratio < 95
        }
        
        for condition, result in success_conditions.items():
            status = "✅" if result else "❌"
            print(f"{status} {condition}")
        
        all_success = all(success_conditions.values())
        print()
        if all_success:
            print("✅ 모든 성공 조건 달성!")
        else:
            print("⚠️ 일부 성공 조건 미달성")
        print()
        
        # 최근 업데이트 통계
        result = conn.execute(text("""
            SELECT 
                COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '1 hour') AS last_1h,
                COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '6 hours') AS last_6h,
                MAX(updated_at) AS max_updated
            FROM investor_sector
            WHERE is_primary = true
        """))
        row = result.fetchone()
        last_1h, last_6h, max_updated = row
        
        print("6. 최근 업데이트 통계:")
        print(f"   최근 1시간: {last_1h:,}개")
        print(f"   최근 6시간: {last_6h:,}개")
        if max_updated:
            if max_updated.tzinfo:
                max_updated = max_updated.replace(tzinfo=None)
            time_diff = datetime.now() - max_updated
            print(f"   마지막 업데이트: {max_updated} ({time_diff} 전)")
        print()
        
        print("=" * 80)

if __name__ == '__main__':
    main()

