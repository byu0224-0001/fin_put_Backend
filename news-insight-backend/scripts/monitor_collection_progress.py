"""데이터 수집 및 임베딩 생성 진행 상황 모니터링"""
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

# UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from app.db import SessionLocal

def get_missing_tickers():
    """누락된 기업 티커 목록 가져오기"""
    reports_dir = project_root / "reports"
    if not reports_dir.exists():
        return []
    
    missing_files = sorted(reports_dir.glob("missing_stocks_*.txt"), reverse=True)
    if not missing_files:
        return []
    
    latest_file = missing_files[0]
    tickers = []
    with open(latest_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('=') and not line.startswith('누락') and not line.startswith('총'):
                parts = line.split('\t')
                if len(parts) >= 1 and parts[0]:
                    ticker = parts[0].strip()
                    if ticker and len(ticker) == 6:
                        tickers.append(ticker)
    
    # 460470도 포함
    if '460470' not in tickers:
        tickers.append('460470')
    
    return tickers

def main():
    """메인 실행 함수"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("데이터 수집 및 임베딩 생성 진행 상황 모니터링")
        print("=" * 80)
        print()
        
        # 1. 누락된 기업 목록 가져오기
        missing_tickers = get_missing_tickers()
        total_count = len(missing_tickers)
        
        if total_count == 0:
            print("처리할 기업이 없습니다.")
            return
        
        print(f"📋 처리 대상 기업: {total_count}개")
        print()
        
        # 2. 데이터 수집 상태 확인
        print("=" * 80)
        print("1. 데이터 수집 상태")
        print("=" * 80)
        
        collected_count = 0
        not_collected = []
        
        for ticker in missing_tickers:
            result = db.execute(text("""
                SELECT COUNT(*) 
                FROM company_details 
                WHERE ticker = :ticker
            """), {'ticker': ticker})
            count = result.fetchone()[0]
            
            if count > 0:
                collected_count += 1
            else:
                not_collected.append(ticker)
        
        print(f"✅ 데이터 수집 완료: {collected_count}개 ({collected_count/total_count*100:.1f}%)")
        print(f"⏳ 데이터 수집 대기: {len(not_collected)}개 ({len(not_collected)/total_count*100:.1f}%)")
        
        if not_collected:
            print(f"\n수집 대기 중인 기업:")
            for ticker in not_collected[:10]:  # 최대 10개만 표시
                stock_result = db.execute(text("""
                    SELECT stock_name 
                    FROM stocks 
                    WHERE ticker = :ticker
                """), {'ticker': ticker})
                stock_row = stock_result.fetchone()
                stock_name = stock_row[0] if stock_row else ticker
                print(f"  - {ticker} ({stock_name})")
            if len(not_collected) > 10:
                print(f"  ... 외 {len(not_collected) - 10}개")
        
        print()
        
        # 3. 임베딩 생성 상태 확인
        print("=" * 80)
        print("2. 임베딩 생성 상태")
        print("=" * 80)
        
        embedding_count = 0
        no_embedding = []
        
        for ticker in missing_tickers:
            result = db.execute(text("""
                SELECT COUNT(*) 
                FROM company_embeddings 
                WHERE ticker = :ticker
            """), {'ticker': ticker})
            count = result.fetchone()[0]
            
            if count > 0:
                embedding_count += 1
            else:
                no_embedding.append(ticker)
        
        print(f"✅ 임베딩 생성 완료: {embedding_count}개 ({embedding_count/total_count*100:.1f}%)")
        print(f"⏳ 임베딩 생성 대기: {len(no_embedding)}개 ({len(no_embedding)/total_count*100:.1f}%)")
        
        if no_embedding:
            print(f"\n임베딩 대기 중인 기업:")
            for ticker in no_embedding[:10]:  # 최대 10개만 표시
                # 데이터 수집 여부 확인
                detail_result = db.execute(text("""
                    SELECT COUNT(*) 
                    FROM company_details 
                    WHERE ticker = :ticker
                """), {'ticker': ticker})
                detail_count = detail_result.fetchone()[0]
                
                stock_result = db.execute(text("""
                    SELECT stock_name 
                    FROM stocks 
                    WHERE ticker = :ticker
                """), {'ticker': ticker})
                stock_row = stock_result.fetchone()
                stock_name = stock_row[0] if stock_row else ticker
                
                status = "데이터 수집 완료" if detail_count > 0 else "데이터 수집 대기"
                print(f"  - {ticker} ({stock_name}) - {status}")
            if len(no_embedding) > 10:
                print(f"  ... 외 {len(no_embedding) - 10}개")
        
        print()
        
        # 4. 전체 진행률
        print("=" * 80)
        print("3. 전체 진행률")
        print("=" * 80)
        
        total_progress = (collected_count + embedding_count) / (total_count * 2) * 100
        print(f"전체 진행률: {total_progress:.1f}%")
        print(f"  - 데이터 수집: {collected_count}/{total_count} ({collected_count/total_count*100:.1f}%)")
        print(f"  - 임베딩 생성: {embedding_count}/{total_count} ({embedding_count/total_count*100:.1f}%)")
        
        print()
        
        # 5. 최근 활동 확인
        print("=" * 80)
        print("4. 최근 활동")
        print("=" * 80)
        
        # 최근 수집된 데이터
        recent_collected = db.execute(text("""
            SELECT c.ticker, s.stock_name, c.updated_at
            FROM company_details c
            JOIN stocks s ON c.ticker = s.ticker
            WHERE c.ticker = ANY(:tickers)
            ORDER BY c.updated_at DESC
            LIMIT 5
        """), {'tickers': missing_tickers})
        
        print("최근 데이터 수집 완료:")
        for row in recent_collected:
            ticker, stock_name, updated_at = row
            print(f"  - {ticker} ({stock_name}): {updated_at}")
        
        # 최근 생성된 임베딩
        recent_embeddings = db.execute(text("""
            SELECT e.ticker, s.stock_name, e.updated_at
            FROM company_embeddings e
            JOIN stocks s ON e.ticker = s.ticker
            WHERE e.ticker = ANY(:tickers)
            ORDER BY e.updated_at DESC
            LIMIT 5
        """), {'tickers': missing_tickers})
        
        print("\n최근 임베딩 생성 완료:")
        for row in recent_embeddings:
            ticker, stock_name, updated_at = row
            print(f"  - {ticker} ({stock_name}): {updated_at}")
        
        print()
        
        # 6. 최종 상태 요약
        print("=" * 80)
        print("5. 최종 상태 요약")
        print("=" * 80)
        
        if collected_count == total_count and embedding_count == total_count:
            print("✅ 모든 작업이 완료되었습니다!")
        elif collected_count == total_count:
            print(f"✅ 데이터 수집 완료! 임베딩 생성 진행 중... ({embedding_count}/{total_count})")
        else:
            print(f"⏳ 데이터 수집 진행 중... ({collected_count}/{total_count})")
            if embedding_count > 0:
                print(f"   임베딩 생성도 진행 중... ({embedding_count}/{total_count})")
        
        print()
        print("=" * 80)
        
    finally:
        db.close()

if __name__ == '__main__':
    main()

