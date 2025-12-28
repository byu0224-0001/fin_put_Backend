"""한국 증시 상장 기업 목록 동기화 및 누락 기업 확인"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from urllib.parse import quote_plus
import logging
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
from app.models.stock import Stock

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_krx_listed_stocks():
    """KRX에서 상장 기업 목록 가져오기"""
    try:
        from pykrx import stock
        logger.info("pykrx를 사용하여 상장 기업 목록 조회 중...")
        
        # KOSPI + KOSDAQ 전체 종목 조회
        kospi_stocks = stock.get_market_ticker_list(datetime.now().strftime('%Y%m%d'), market="KOSPI")
        kosdaq_stocks = stock.get_market_ticker_list(datetime.now().strftime('%Y%m%d'), market="KOSDAQ")
        
        all_tickers = set(kospi_stocks + kosdaq_stocks)
        
        logger.info(f"KOSPI: {len(kospi_stocks)}개, KOSDAQ: {len(kosdaq_stocks)}개")
        logger.info(f"전체 상장 기업: {len(all_tickers)}개")
        
        return all_tickers, kospi_stocks, kosdaq_stocks
    except ImportError:
        logger.error("pykrx가 설치되지 않았습니다. pip install pykrx 실행 필요")
        return None, None, None
    except Exception as e:
        logger.error(f"KRX 상장 기업 목록 조회 실패: {e}")
        return None, None, None

def get_db_stocks(db: Session):
    """DB에 있는 기업 목록 가져오기"""
    stocks = db.query(Stock.ticker).all()
    return set([s.ticker for s in stocks])

def get_missing_stocks(krx_tickers, db_tickers):
    """누락된 기업 찾기"""
    missing = krx_tickers - db_tickers
    return missing

def add_missing_stocks(db: Session, missing_tickers, kospi_tickers, kosdaq_tickers):
    """누락된 기업을 DB에 추가"""
    added_count = 0
    
    try:
        from pykrx import stock
        today = datetime.now().strftime('%Y%m%d')
        
        for ticker in missing_tickers:
            try:
                # 종목 정보 조회
                market = "KOSPI" if ticker in kospi_tickers else "KOSDAQ"
                ticker_info = stock.get_market_ticker_name(ticker)
                
                if not ticker_info:
                    logger.warning(f"[{ticker}] 종목명 조회 실패")
                    continue
                
                # Stock 객체 생성
                new_stock = Stock(
                    ticker=ticker,
                    stock_name=ticker_info,
                    market=market,
                    country='KR'
                )
                
                db.add(new_stock)
                added_count += 1
                
                if added_count % 50 == 0:
                    logger.info(f"진행 중... ({added_count}개 추가)")
                    db.commit()
                    
            except Exception as e:
                logger.warning(f"[{ticker}] 추가 실패: {e}")
                continue
        
        db.commit()
        logger.info(f"총 {added_count}개 기업 추가 완료")
        return added_count
        
    except ImportError:
        logger.error("pykrx가 설치되지 않았습니다.")
        return 0
    except Exception as e:
        logger.error(f"기업 추가 중 오류: {e}", exc_info=True)
        db.rollback()
        return 0

def main():
    """메인 실행 함수"""
    db = SessionLocal()
    
    try:
        logger.info("=" * 80)
        logger.info("한국 증시 상장 기업 목록 동기화 시작")
        logger.info("=" * 80)
        
        # 1. KRX 상장 기업 목록 조회
        logger.info("1. KRX 상장 기업 목록 조회 중...")
        krx_tickers, kospi_tickers, kosdaq_tickers = get_krx_listed_stocks()
        
        if krx_tickers is None:
            logger.error("KRX 상장 기업 목록 조회 실패. pykrx 설치 확인 필요")
            return
        
        # 2. DB 기업 목록 조회
        logger.info("2. DB 기업 목록 조회 중...")
        db_tickers = get_db_stocks(db)
        logger.info(f"DB 기업 수: {len(db_tickers)}개")
        
        # 3. 누락된 기업 찾기
        logger.info("3. 누락된 기업 찾는 중...")
        missing_tickers = get_missing_stocks(krx_tickers, db_tickers)
        logger.info(f"누락된 기업: {len(missing_tickers)}개")
        
        if len(missing_tickers) == 0:
            logger.info("✅ 모든 상장 기업이 DB에 있습니다!")
            return
        
        # 4. 누락된 기업 리스트 출력
        logger.info("=" * 80)
        logger.info("누락된 기업 목록:")
        logger.info("=" * 80)
        for ticker in sorted(missing_tickers):
            market = "KOSPI" if ticker in kospi_tickers else "KOSDAQ"
            logger.info(f"  {ticker} ({market})")
        
        # 5. 누락된 기업 추가
        logger.info("=" * 80)
        logger.info("4. 누락된 기업 DB 추가 중...")
        added_count = add_missing_stocks(db, missing_tickers, kospi_tickers, kosdaq_tickers)
        
        logger.info("=" * 80)
        logger.info("동기화 완료")
        logger.info(f"  KRX 상장 기업: {len(krx_tickers)}개")
        logger.info(f"  DB 기업 (추가 전): {len(db_tickers)}개")
        logger.info(f"  누락된 기업: {len(missing_tickers)}개")
        logger.info(f"  추가된 기업: {added_count}개")
        logger.info(f"  DB 기업 (추가 후): {len(db_tickers) + added_count}개")
        logger.info("=" * 80)
        
        # 6. 누락된 기업 리스트 파일 저장
        if missing_tickers:
            output_file = project_root / "reports" / f"missing_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            output_file.parent.mkdir(exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"누락된 기업 목록 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n")
                f.write("=" * 80 + "\n")
                f.write(f"총 {len(missing_tickers)}개\n\n")
                for ticker in sorted(missing_tickers):
                    market = "KOSPI" if ticker in kospi_tickers else "KOSDAQ"
                    f.write(f"{ticker}\t{market}\n")
            
            logger.info(f"누락된 기업 리스트 저장: {output_file}")
        
    finally:
        db.close()

if __name__ == '__main__':
    main()

