# -*- coding: utf-8 -*-
"""
전체 기업 DB 동기화 스크립트

기능:
1. 새로 상장된 기업 추가
2. 상장 폐지된 기업 비활성화 (is_active = False)
3. 기업명 변경 업데이트
4. KRX 업종 정보 업데이트

사용 라이브러리:
- pykrx: 상장 기업 목록, 기업명
- FinanceDataReader: 추가 정보 (업종 등)
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.stock import Stock
from sqlalchemy import text

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_krx_stocks():
    """pykrx로 현재 상장 기업 목록 조회"""
    try:
        from pykrx import stock
        
        today = datetime.now()
        days_back = 0
        
        # 최근 거래일 찾기
        while days_back < 7:
            check_date = (today - timedelta(days=days_back)).strftime("%Y%m%d")
            try:
                kospi = stock.get_market_ticker_list(check_date, market="KOSPI")
                kosdaq = stock.get_market_ticker_list(check_date, market="KOSDAQ")
                
                if kospi and kosdaq:
                    logger.info(f"기준일: {check_date}")
                    logger.info(f"KOSPI: {len(kospi)}개, KOSDAQ: {len(kosdaq)}개")
                    break
            except:
                pass
            days_back += 1
        
        # 종목명 조회
        krx_stocks = {}
        
        for ticker in kospi:
            try:
                name = stock.get_market_ticker_name(ticker)
                if name:
                    krx_stocks[ticker] = {'name': name, 'market': 'KOSPI'}
            except:
                pass
        
        for ticker in kosdaq:
            try:
                name = stock.get_market_ticker_name(ticker)
                if name:
                    krx_stocks[ticker] = {'name': name, 'market': 'KOSDAQ'}
            except:
                pass
        
        logger.info(f"KRX 전체: {len(krx_stocks)}개")
        return krx_stocks, check_date
        
    except ImportError:
        logger.error("pykrx가 설치되지 않았습니다. pip install pykrx")
        return None, None


def get_fdr_sector_info():
    """FinanceDataReader로 업종 정보 조회"""
    try:
        import FinanceDataReader as fdr
        
        # 한국 전체 상장기업 목록
        kospi_df = fdr.StockListing('KOSPI')
        kosdaq_df = fdr.StockListing('KOSDAQ')
        
        sector_info = {}
        
        for _, row in kospi_df.iterrows():
            ticker = str(row.get('Code', row.get('Symbol', ''))).zfill(6)
            sector_info[ticker] = {
                'industry': row.get('Industry', row.get('Sector', '')),
                'name': row.get('Name', ''),
                'market': 'KOSPI'
            }
        
        for _, row in kosdaq_df.iterrows():
            ticker = str(row.get('Code', row.get('Symbol', ''))).zfill(6)
            sector_info[ticker] = {
                'industry': row.get('Industry', row.get('Sector', '')),
                'name': row.get('Name', ''),
                'market': 'KOSDAQ'
            }
        
        logger.info(f"FDR 업종 정보: {len(sector_info)}개")
        return sector_info
        
    except ImportError:
        logger.warning("FinanceDataReader가 설치되지 않았습니다. pip install finance-datareader")
        return {}
    except Exception as e:
        logger.warning(f"FDR 조회 실패: {e}")
        return {}


def sync_stocks():
    """전체 기업 DB 동기화"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print(f"[전체 기업 DB 동기화] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # 1. KRX 상장 기업 목록 조회
        print("\n[1단계] KRX 상장 기업 목록 조회...")
        krx_stocks, check_date = get_krx_stocks()
        
        if not krx_stocks:
            print("❌ KRX 조회 실패")
            return
        
        # 2. FDR 업종 정보 조회
        print("\n[2단계] FDR 업종 정보 조회...")
        fdr_info = get_fdr_sector_info()
        
        # 3. 현재 DB 기업 목록 조회 (한국만)
        print("\n[3단계] DB 기업 목록 조회...")
        db_stocks = db.query(Stock).filter(Stock.country == 'KR').all()
        db_ticker_map = {s.ticker: s for s in db_stocks}
        
        print(f"현재 DB: {len(db_ticker_map)}개")
        
        # 결과 저장
        changes = {
            'added': [],      # 새로 상장
            'delisted': [],   # 상장 폐지
            'renamed': [],    # 이름 변경
            'industry_updated': [],  # 업종 업데이트
            'no_change': 0
        }
        
        # 4. 새로 상장된 기업 추가
        print("\n[4단계] 새로 상장된 기업 확인...")
        krx_tickers = set(krx_stocks.keys())
        db_tickers = set(db_ticker_map.keys())
        
        new_tickers = krx_tickers - db_tickers
        print(f"새로 상장: {len(new_tickers)}개")
        
        for ticker in new_tickers:
            info = krx_stocks.get(ticker, {})
            fdr = fdr_info.get(ticker, {})
            
            new_stock = Stock(
                ticker=ticker,
                stock_name=info.get('name', ''),
                market=info.get('market', 'KOSDAQ'),
                country='KR',
                industry_raw=fdr.get('industry', ''),
            )
            db.add(new_stock)
            
            changes['added'].append({
                'ticker': ticker,
                'name': info.get('name'),
                'market': info.get('market'),
                'industry': fdr.get('industry', '')
            })
        
        # 5. 상장 폐지된 기업 확인
        print("\n[5단계] 상장 폐지된 기업 확인...")
        delisted_tickers = db_tickers - krx_tickers
        
        # KONEX, 비상장 제외
        for ticker in delisted_tickers:
            stock_obj = db_ticker_map.get(ticker)
            if stock_obj and stock_obj.market in ['KOSPI', 'KOSDAQ']:
                changes['delisted'].append({
                    'ticker': ticker,
                    'name': stock_obj.stock_name,
                    'market': stock_obj.market
                })
                # is_active 필드가 있으면 False로 설정
                # 없으면 그냥 기록만
        
        print(f"상장 폐지 의심: {len(changes['delisted'])}개")
        
        # 6. 기업명 변경 확인
        print("\n[6단계] 기업명 변경 확인...")
        
        for ticker in krx_tickers & db_tickers:
            db_stock = db_ticker_map[ticker]
            krx_info = krx_stocks[ticker]
            fdr = fdr_info.get(ticker, {})
            
            krx_name = krx_info.get('name', '')
            db_name = db_stock.stock_name or ''
            
            # 이름 변경
            if krx_name and db_name and krx_name != db_name:
                changes['renamed'].append({
                    'ticker': ticker,
                    'old_name': db_name,
                    'new_name': krx_name
                })
                db_stock.stock_name = krx_name
            
            # 업종 업데이트 (비어있거나 다른 경우)
            fdr_industry = fdr.get('industry', '')
            if fdr_industry and (not db_stock.industry_raw or db_stock.industry_raw != fdr_industry):
                if db_stock.industry_raw != fdr_industry:
                    changes['industry_updated'].append({
                        'ticker': ticker,
                        'name': krx_name or db_name,
                        'old_industry': db_stock.industry_raw,
                        'new_industry': fdr_industry
                    })
                    db_stock.industry_raw = fdr_industry
            else:
                changes['no_change'] += 1
        
        # 커밋
        db.commit()
        
        # 결과 출력
        print("\n" + "=" * 80)
        print("[동기화 결과]")
        print("=" * 80)
        
        print(f"\n✅ 새로 상장: {len(changes['added'])}개")
        for item in changes['added'][:20]:
            print(f"   + {item['ticker']} {item['name']} ({item['market']}) - {item['industry']}")
        if len(changes['added']) > 20:
            print(f"   ... 외 {len(changes['added']) - 20}개")
        
        print(f"\n⚠️ 상장 폐지 의심: {len(changes['delisted'])}개")
        for item in changes['delisted'][:10]:
            print(f"   - {item['ticker']} {item['name']} ({item['market']})")
        if len(changes['delisted']) > 10:
            print(f"   ... 외 {len(changes['delisted']) - 10}개")
        
        print(f"\n🔄 기업명 변경: {len(changes['renamed'])}개")
        for item in changes['renamed']:
            print(f"   {item['ticker']}: {item['old_name']} → {item['new_name']}")
        
        print(f"\n📊 업종 업데이트: {len(changes['industry_updated'])}개")
        for item in changes['industry_updated'][:20]:
            print(f"   {item['ticker']} {item['name']}: {item['old_industry'] or '(없음)'} → {item['new_industry']}")
        if len(changes['industry_updated']) > 20:
            print(f"   ... 외 {len(changes['industry_updated']) - 20}개")
        
        print(f"\n변경 없음: {changes['no_change']}개")
        
        # 최종 통계
        final_count = db.query(Stock).filter(Stock.country == 'KR', Stock.market.in_(['KOSPI', 'KOSDAQ'])).count()
        print(f"\n[최종 DB 현황] KOSPI+KOSDAQ: {final_count}개")
        
        # 결과 저장
        output_path = project_root / 'reports' / f'stock_sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(changes, f, ensure_ascii=False, indent=2)
        print(f"\n결과 저장: {output_path}")
        
        return changes
        
    finally:
        db.close()


if __name__ == "__main__":
    sync_stocks()

