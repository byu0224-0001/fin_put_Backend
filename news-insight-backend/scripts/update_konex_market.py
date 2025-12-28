# -*- coding: utf-8 -*-
"""
코넥스 기업 market 컬럼 업데이트 스크립트
CSV 파일을 기반으로 stocks 테이블의 market 컬럼을 동기화
"""
import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')
import csv
from pathlib import Path
from dotenv import load_dotenv

# .env 파일 로드
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path, override=True)

from app.db import SessionLocal
from sqlalchemy import text

def update_konex_market_from_csv():
    """CSV 파일에서 코넥스 기업 정보를 읽어서 stocks 테이블 업데이트"""
    
    csv_path = project_root / 'data' / 'krx_sector_industry.csv'
    
    if not csv_path.exists():
        print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}")
        return
    
    db = SessionLocal()
    
    try:
        print("=" * 70)
        print("코넥스 기업 market 컬럼 업데이트")
        print("=" * 70)
        
        # CSV 파일 읽기
        konex_companies = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # 헤더 스킵
            
            for row in reader:
                if len(row) >= 3:
                    ticker = row[0].strip()
                    stock_name = row[1].strip()
                    market = row[2].strip()
                    
                    if market == 'KONEX':
                        konex_companies.append({
                            'ticker': ticker,
                            'stock_name': stock_name,
                            'market': market
                        })
        
        print(f"\nCSV 파일에서 코넥스 기업 {len(konex_companies)}개 발견")
        
        # stocks 테이블 업데이트
        updated_count = 0
        not_found_count = 0
        already_correct_count = 0
        
        print("\n업데이트 시작...\n")
        
        for company in konex_companies:
            ticker = company['ticker']
            stock_name = company['stock_name']
            
            # 현재 market 값 확인
            current_result = db.execute(
                text("SELECT ticker, stock_name, market FROM stocks WHERE ticker = :ticker"),
                {'ticker': ticker}
            ).first()
            
            if not current_result:
                print(f"⚠️  {ticker}: {stock_name} - stocks 테이블에 존재하지 않음 (스킵)")
                not_found_count += 1
                continue
            
            current_ticker, current_name, current_market = current_result
            
            if current_market == 'KONEX':
                print(f"✅ {ticker}: {stock_name} - 이미 KONEX로 설정됨 (스킵)")
                already_correct_count += 1
                continue
            
            # market 컬럼 업데이트
            db.execute(
                text("UPDATE stocks SET market = :market WHERE ticker = :ticker"),
                {'market': 'KONEX', 'ticker': ticker}
            )
            db.commit()
            
            print(f"🔄 {ticker}: {stock_name} - {current_market} → KONEX (업데이트 완료)")
            updated_count += 1
        
        print("\n" + "=" * 70)
        print("업데이트 완료!")
        print("=" * 70)
        print(f"업데이트된 기업: {updated_count}개")
        print(f"이미 올바른 값: {already_correct_count}개")
        print(f"테이블에 없음: {not_found_count}개")
        print(f"총 처리: {len(konex_companies)}개")
        print("=" * 70)
        
        # 최종 확인
        final_konex_count = db.execute(
            text("SELECT COUNT(*) FROM stocks WHERE market = 'KONEX'")
        ).scalar()
        
        print(f"\n✅ stocks 테이블의 코넥스 기업 수: {final_konex_count}개")
        
    except Exception as e:
        db.rollback()
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    update_konex_market_from_csv()

