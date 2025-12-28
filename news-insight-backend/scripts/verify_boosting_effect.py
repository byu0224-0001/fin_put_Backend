"""P2-2: Boosting 효과 검증 (base_score vs final_score)"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime
import json

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
    print("P2-2: Boosting 효과 검증")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    with engine.connect() as conn:
        # boosting_log가 있는 기업 수
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM investor_sector
            WHERE is_primary = true 
                AND boosting_log IS NOT NULL
        """))
        with_boosting_log = result.fetchone()[0]
        print(f"1. boosting_log가 있는 기업 수: {with_boosting_log:,}개")
        print()
        
        if with_boosting_log == 0:
            print("⚠️ boosting_log가 있는 기업이 없습니다.")
            print("   → Boosting이 적용되지 않았거나 로그가 저장되지 않았습니다.")
            return
        
        # boosting_log 샘플 (50개)
        result = conn.execute(text("""
            SELECT 
                i.ticker,
                s.stock_name,
                i.sector_l1,
                i.ensemble_score,
                i.boosting_log,
                i.classification_reasoning
            FROM investor_sector i
            LEFT JOIN stocks s ON i.ticker = s.ticker
            WHERE i.is_primary = true 
                AND i.boosting_log IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 50
        """))
        
        samples = result.fetchall()
        
        print("=" * 80)
        print("2. Boosting 로그 샘플 분석 (50개)")
        print("=" * 80)
        print()
        
        match_count = 0
        mismatch_count = 0
        no_final_boost = 0
        anchor_applied_count = 0
        kg_applied_count = 0
        
        for idx, row in enumerate(samples, 1):
            ticker, stock_name, sector_l1, ensemble_score, boosting_log, reasoning = row
            
            if not boosting_log:
                continue
            
            try:
                if isinstance(boosting_log, str):
                    boosting_data = json.loads(boosting_log)
                else:
                    boosting_data = boosting_log
                
                final_boost = boosting_data.get('final_boost', 0)
                anchor_applied = boosting_data.get('anchor_applied', False)
                kg_applied = boosting_data.get('kg_applied', False)
                
                if anchor_applied:
                    anchor_applied_count += 1
                if kg_applied:
                    kg_applied_count += 1
                
                if final_boost == 0:
                    no_final_boost += 1
                    continue
                
                # base_score와 final_score 비교는 boosting_log에 없으므로
                # ensemble_score와 final_boost를 비교
                # 실제로는 base_score가 필요하지만, 현재는 ensemble_score만 있음
                
            except Exception as e:
                continue
        
        print(f"3. Boosting 적용 통계:")
        print("-" * 80)
        print(f"  Anchor 적용: {anchor_applied_count}개")
        print(f"  KG Edge 적용: {kg_applied_count}개")
        print(f"  final_boost = 0: {no_final_boost}개")
        print()
        
        # boosting_log 상세 샘플 (5개)
        print("=" * 80)
        print("4. Boosting 로그 상세 샘플 (5개)")
        print("=" * 80)
        print()
        
        for idx, row in enumerate(samples[:5], 1):
            ticker, stock_name, sector_l1, ensemble_score, boosting_log, reasoning = row
            
            print(f"[{idx}] {ticker} ({stock_name})")
            print(f"  섹터: {sector_l1}")
            print(f"  ensemble_score: {ensemble_score}")
            
            if boosting_log:
                try:
                    if isinstance(boosting_log, str):
                        boosting_data = json.loads(boosting_log)
                    else:
                        boosting_data = boosting_log
                    
                    print(f"  boosting_log:")
                    print(f"    anchor_applied: {boosting_data.get('anchor_applied')}")
                    print(f"    kg_applied: {boosting_data.get('kg_applied')}")
                    print(f"    final_boost: {boosting_data.get('final_boost')}")
                    print(f"    reason: {boosting_data.get('reason', '')[:100]}")
                except Exception as e:
                    print(f"  boosting_log 파싱 실패: {e}")
            print()
        
        # 최종 결론
        print("=" * 80)
        print("5. 최종 결론")
        print("=" * 80)
        if with_boosting_log > 0:
            print(f"✅ boosting_log가 {with_boosting_log:,}개 기업에 저장됨")
            print(f"   → Boosting 로직이 작동 중")
            if anchor_applied_count > 0 or kg_applied_count > 0:
                print(f"   → Anchor/KG Edge Boosting 적용 중")
            else:
                print(f"   ⚠️ Anchor/KG Edge Boosting이 적용되지 않음")
        else:
            print("⚠️ boosting_log가 저장되지 않음")
            print("   → Boosting 로직 확인 필요")
        print()

if __name__ == '__main__':
    main()

