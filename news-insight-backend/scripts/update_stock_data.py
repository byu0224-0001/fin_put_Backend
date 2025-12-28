"""
기업명 데이터 업데이트 스크립트

하루 1회 또는 주 1회 실행하여 기업명 데이터를 DB에 업데이트
- 한국: pykrx 사용
- 미국: Finnhub API 우선 사용 (대안: NASDAQ/NYSE 공식 API, Wikipedia)

실행 방법:
    python scripts/update_stock_data.py

실행 주기:
    - 주 1회 실행 권장 (상장/상장폐지 빈도 낮음)
    - 또는 수동으로 필요시 실행

필수 설정:
    - .env 파일에 FINNHUB_API_KEY 추가 (미국 기업명 수집용)
    - Finnhub API 키 발급: https://finnhub.io/
"""
import sys
import os
from pathlib import Path

# Windows 환경에서 인코딩 문제 방지
if sys.platform == 'win32':
    import codecs
    # Windows 콘솔 인코딩을 UTF-8로 설정
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    # 환경 변수 설정 (인코딩)
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datetime import datetime, timedelta
from app.db import SessionLocal, Base, engine
from app.models.stock import Stock
from app.utils.preferred_stock import is_preferred_stock_smart
import logging
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def update_korean_stocks():
    """한국 상장기업 데이터 업데이트 (pykrx 사용)"""
    logger.info("한국 기업명 데이터 수집 시작...")
    
    try:
        from pykrx import stock
        
        # 최근 거래일 찾기
        today = datetime.now()
        days_back = 0
        tickers = []
        
        while days_back < 7:
            check_date = (today - timedelta(days=days_back)).strftime("%Y%m%d")
            try:
                tickers = stock.get_market_ticker_list(check_date, market="ALL")
                if tickers:
                    logger.info(f"기준일: {check_date}, 상장기업 수: {len(tickers)}")
                    break
            except Exception as e:
                logger.warning(f"날짜 {check_date} 조회 실패: {e}")
                days_back += 1
        
        if not tickers:
            raise Exception("최근 거래일을 찾을 수 없습니다")
        
        db = SessionLocal()
        try:
            # 기존 한국 기업 데이터 삭제 (전체 업데이트)
            deleted_count = db.query(Stock).filter(Stock.country == "KR").delete()
            logger.info(f"기존 한국 기업 데이터 삭제: {deleted_count}개")
            
            # Phase 2: 2단계 처리 (본주 먼저, 우선주 나중)
            common_stocks = {}  # {이름: 티커} 매핑용
            preferred_stocks = []  # 우선주 정보 저장용
            
            # [1차 패스] 본주(Common Stock) 먼저 수집
            logger.info("[1단계] 본주(Common Stock) 수집 중...")
            for i, ticker in enumerate(tickers):
                try:
                    name = stock.get_market_ticker_name(ticker)
                    market = "KOSPI" if ticker.startswith("0") else "KOSDAQ"
                    
                    # 우선주 체크
                    is_preferred, _ = is_preferred_stock_smart(name, None)  # 패턴만 체크
                    
                    if not is_preferred:
                        # 본주로 판단 -> DB 저장 및 딕셔너리 등록
                        stock_obj = Stock(
                            stock_name=name,
                            ticker=ticker,
                            market=market,
                            country="KR",
                            synonyms=None,
                            is_preferred_stock=False,
                            parent_ticker=None
                        )
                        db.add(stock_obj)
                        common_stocks[name] = ticker
                        
                        if (i + 1) % 100 == 0:
                            logger.info(f"본주 수집 진행: {i + 1}/{len(tickers)}")
                            db.commit()
                    else:
                        # 우선주로 추정 -> 나중에 처리
                        preferred_stocks.append({
                            'ticker': ticker,
                            'name': name,
                            'market': market
                        })
                        
                except Exception as e:
                    logger.warning(f"티커 {ticker} 처리 실패: {e}")
                    continue
            
            # 중간 커밋
            db.commit()
            logger.info(f"본주 수집 완료: {len(common_stocks)}개")
            
            # [2차 패스] 우선주(Preferred Stock) 처리 및 연결
            logger.info(f"[2단계] 우선주(Preferred Stock) 처리 중... ({len(preferred_stocks)}개)")
            preferred_count = 0
            preferred_linked = 0
            
            for pref_info in preferred_stocks:
                try:
                    ticker = pref_info['ticker']
                    name = pref_info['name']
                    market = pref_info['market']
                    
                    # 본주 검증 (이제 common_stocks 딕셔너리가 채워짐)
                    is_preferred, parent_ticker = is_preferred_stock_smart(name, db)
                    
                    if is_preferred and parent_ticker:
                        # 우선주로 확인 + 본주 연결 성공
                        stock_obj = Stock(
                            stock_name=name,
                            ticker=ticker,
                            market=market,
                            country="KR",
                            synonyms=None,
                            is_preferred_stock=True,
                            parent_ticker=parent_ticker
                        )
                        db.add(stock_obj)
                        preferred_linked += 1
                        logger.debug(f"  🔗 {name} -> 본주: {parent_ticker}")
                    else:
                        # 우선주 패턴이지만 본주를 찾지 못함 -> 일반 주식으로 처리
                        stock_obj = Stock(
                            stock_name=name,
                            ticker=ticker,
                            market=market,
                            country="KR",
                            synonyms=None,
                            is_preferred_stock=False,
                            parent_ticker=None
                        )
                        db.add(stock_obj)
                        logger.debug(f"  ⚠️ {name} (우선주 패턴이지만 본주 미발견, 일반 주식으로 처리)")
                    
                    preferred_count += 1
                    
                    if preferred_count % 50 == 0:
                        logger.info(f"우선주 처리 진행: {preferred_count}/{len(preferred_stocks)}")
                        db.commit()
                        
                except Exception as e:
                    logger.warning(f"우선주 {ticker} 처리 실패: {e}")
                    continue
            
            db.commit()
            inserted_count = len(common_stocks) + preferred_count
            logger.info(f"한국 기업명 데이터 업데이트 완료:")
            logger.info(f"  - 본주: {len(common_stocks)}개")
            logger.info(f"  - 우선주: {preferred_linked}개 (본주 연결됨)")
            logger.info(f"  - 일반 주식: {preferred_count - preferred_linked}개 (우선주 패턴이지만 본주 미발견)")
            logger.info(f"  - 총계: {inserted_count}개")
            return inserted_count
            
        finally:
            db.close()
            
    except ImportError:
        logger.error("pykrx가 설치되지 않았습니다. 'pip install pykrx'를 실행하세요.")
        return 0
    except Exception as e:
        logger.error(f"한국 기업명 데이터 업데이트 실패: {e}")
        return 0


def update_us_stocks():
    """미국 상장기업 데이터 업데이트 (Finnhub API 우선 사용)"""
    logger.info("미국 기업명 데이터 수집 시작...")
    
    try:
        import pandas as pd
        import requests
        from io import StringIO
        import time
        
        companies = []
        
        # 1. Finnhub API 사용 (우선순위 1)
        finnhub_success = False
        try:
            from app.config import settings
            
            finnhub_api_key = settings.FINNHUB_API_KEY
            
            if finnhub_api_key:
                logger.info("Finnhub API를 사용하여 미국 기업명 데이터 수집 중...")
                
                # 방법 1: finnhub Python 라이브러리 사용
                try:
                    import finnhub
                    finnhub_client = finnhub.Client(api_key=finnhub_api_key)
                    us_symbols = finnhub_client.stock_symbols('US')
                    
                    for symbol_data in us_symbols:
                        symbol = symbol_data.get('symbol', '').strip()
                        name = symbol_data.get('description', '').strip()
                        exchange = symbol_data.get('mic', '').strip()  # 'XNAS' (NASDAQ), 'XNYS' (NYSE)
                        
                        if symbol and name:
                            # exchange를 market으로 변환
                            if 'XNAS' in exchange or 'NASDAQ' in exchange.upper():
                                market = "NASDAQ"
                            elif 'XNYS' in exchange or 'NYSE' in exchange.upper():
                                market = "NYSE"
                            else:
                                market = "US"  # 기타 미국 거래소
                            
                            companies.append({
                                "ticker": symbol,
                                "name": name,
                                "market": market
                            })
                    
                    logger.info(f"Finnhub API: {len(us_symbols)}개 수집 성공")
                    finnhub_success = True
                    
                except ImportError:
                    logger.warning("finnhub 라이브러리가 설치되지 않았습니다. requests로 직접 호출합니다.")
                    
                    # 방법 2: requests로 직접 호출
                    finnhub_url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={finnhub_api_key}"
                    response = requests.get(finnhub_url, timeout=60)
                    response.raise_for_status()
                    
                    us_symbols = response.json()
                    
                    for symbol_data in us_symbols:
                        symbol = symbol_data.get('symbol', '').strip()
                        name = symbol_data.get('description', '').strip()
                        exchange = symbol_data.get('mic', '').strip()
                        
                        if symbol and name:
                            if 'XNAS' in exchange or 'NASDAQ' in exchange.upper():
                                market = "NASDAQ"
                            elif 'XNYS' in exchange or 'NYSE' in exchange.upper():
                                market = "NYSE"
                            else:
                                market = "US"
                            
                            companies.append({
                                "ticker": symbol,
                                "name": name,
                                "market": market
                            })
                    
                    logger.info(f"Finnhub API (requests): {len(us_symbols)}개 수집 성공")
                    finnhub_success = True
                    
            else:
                logger.warning("FINNHUB_API_KEY가 설정되지 않았습니다. 대안 소스를 사용합니다.")
                
        except Exception as e:
            logger.warning(f"Finnhub API 수집 실패: {e}")
            logger.warning("대안 소스를 시도합니다...")
        
        # 2. 대안: NASDAQ/NYSE 공식 API (Finnhub 실패 시)
        if not finnhub_success:
            logger.info("대안 소스 사용: NASDAQ/NYSE 공식 API")
            
            # User-Agent 헤더 추가 (봇 차단 방지)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # NASDAQ 상장기업 목록 (재시도 로직 포함)
            nasdaq_success = False
            for attempt in range(3):  # 최대 3번 재시도
                try:
                    logger.info(f"NASDAQ 상장기업 목록 수집 중... (시도 {attempt + 1}/3)")
                    nasdaq_url = "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download"
                    response = requests.get(nasdaq_url, timeout=120, headers=headers)  # 타임아웃 120초로 증가
                    response.raise_for_status()
                    
                    nasdaq_df = pd.read_csv(StringIO(response.text))
                    for _, row in nasdaq_df.iterrows():
                        symbol = row.get("Symbol", "").strip()
                        name = row.get("Name", "").strip()
                        if symbol and name:
                            companies.append({
                                "ticker": symbol,
                                "name": name,
                                "market": "NASDAQ"
                            })
                    logger.info(f"NASDAQ: {len(nasdaq_df)}개 수집 성공")
                    nasdaq_success = True
                    break
                except Exception as e:
                    logger.warning(f"NASDAQ 데이터 수집 실패 (시도 {attempt + 1}/3): {e}")
                    if attempt < 2:  # 마지막 시도가 아니면 대기
                        time.sleep(5)  # 5초 대기 후 재시도
            
            if not nasdaq_success:
                logger.warning("NASDAQ 데이터 수집을 포기하고 대안 소스를 시도합니다...")
            
            # NYSE 상장기업 목록 (재시도 로직 포함)
            nyse_success = False
            for attempt in range(3):  # 최대 3번 재시도
                try:
                    logger.info(f"NYSE 상장기업 목록 수집 중... (시도 {attempt + 1}/3)")
                    nyse_url = "https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download"
                    response = requests.get(nyse_url, timeout=120, headers=headers)  # 타임아웃 120초로 증가
                    response.raise_for_status()
                    
                    nyse_df = pd.read_csv(StringIO(response.text))
                    for _, row in nyse_df.iterrows():
                        symbol = row.get("Symbol", "").strip()
                        name = row.get("Name", "").strip()
                        if symbol and name:
                            companies.append({
                                "ticker": symbol,
                                "name": name,
                                "market": "NYSE"
                            })
                    logger.info(f"NYSE: {len(nyse_df)}개 수집 성공")
                    nyse_success = True
                    break
                except Exception as e:
                    logger.warning(f"NYSE 데이터 수집 실패 (시도 {attempt + 1}/3): {e}")
                    if attempt < 2:  # 마지막 시도가 아니면 대기
                        time.sleep(5)  # 5초 대기 후 재시도
            
            if not nyse_success:
                logger.warning("NYSE 데이터 수집을 포기하고 대안 소스를 시도합니다...")
        
        # 3. 최종 대안: Wikipedia S&P 500 리스트 (모두 실패 시)
        if len(companies) == 0:
            logger.info("최종 대안 데이터 소스 사용: Wikipedia S&P 500 리스트")
            # User-Agent 헤더 추가 (봇 차단 방지)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            try:
                sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
                response = requests.get(sp500_url, timeout=60, headers=headers)
                response.raise_for_status()
                
                # HTML 테이블에서 데이터 추출
                tables = pd.read_html(response.text)
                if len(tables) > 0:
                    sp500_df = tables[0]
                    for _, row in sp500_df.iterrows():
                        symbol = str(row.get("Symbol", "")).strip()
                        name = str(row.get("Security", "")).strip()
                        if symbol and name and symbol != "nan" and name != "nan":
                            # GICS Sector 정보로 시장 추정 (정확하지 않지만 대략적인 분류)
                            # 대부분의 S&P 500 기업은 NYSE에 상장되어 있음
                            market = "NYSE"  # S&P 500은 주로 NYSE 상장
                            
                            companies.append({
                                "ticker": symbol,
                                "name": name,
                                "market": market
                            })
                    logger.info(f"S&P 500 리스트에서 {len(sp500_df)}개 수집 성공")
            except Exception as e:
                logger.warning(f"Wikipedia S&P 500 리스트 수집 실패: {e}")
                logger.warning("미국 기업명 데이터 수집에 실패했습니다. 나중에 다시 시도하거나 수동으로 데이터를 추가하세요.")
        
        # 중복 제거 (티커 기준)
        seen_tickers = set()
        unique_companies = []
        for company in companies:
            ticker = company.get("ticker", "")
            if ticker and ticker not in seen_tickers:
                seen_tickers.add(ticker)
                unique_companies.append(company)
        
        db = SessionLocal()
        try:
            # 기존 미국 기업 데이터 삭제 (전체 업데이트)
            deleted_count = db.query(Stock).filter(Stock.country == "US").delete()
            logger.info(f"기존 미국 기업 데이터 삭제: {deleted_count}개")
            
            # 새 데이터 삽입
            inserted_count = 0
            for company in unique_companies:
                try:
                    stock_obj = Stock(
                        stock_name=company["name"],
                        ticker=company["ticker"],
                        market=company["market"],
                        country="US",
                        synonyms=None
                    )
                    db.add(stock_obj)
                    inserted_count += 1
                    
                    if inserted_count % 500 == 0:
                        logger.info(f"진행: {inserted_count}/{len(unique_companies)}")
                        db.commit()  # 주기적으로 커밋
                        
                except Exception as e:
                    logger.warning(f"기업 {company['ticker']} 처리 실패: {e}")
                    continue
            
            db.commit()
            logger.info(f"미국 기업명 데이터 업데이트 완료: {inserted_count}개")
            return inserted_count
            
        finally:
            db.close()
            
    except ImportError:
        logger.error("pandas가 설치되지 않았습니다. 'pip install pandas'를 실행하세요.")
        return 0
    except Exception as e:
        logger.error(f"미국 기업명 데이터 업데이트 실패: {e}")
        return 0


def main():
    """메인 함수"""
    logger.info("=" * 50)
    logger.info("기업명 데이터 업데이트 시작")
    logger.info("=" * 50)
    
    # 환경 변수 로드 확인
    try:
        from app.config import settings
        logger.info(f"데이터베이스 호스트: {settings.POSTGRES_HOST}")
        logger.info(f"데이터베이스 포트: {settings.POSTGRES_PORT}")
        logger.info(f"데이터베이스 이름: {settings.POSTGRES_DB}")
        logger.info(f"데이터베이스 사용자: {settings.POSTGRES_USER}")
        # 비밀번호는 보안을 위해 일부만 표시
        if settings.POSTGRES_PASSWORD:
            logger.info(f"데이터베이스 비밀번호: {'*' * min(len(settings.POSTGRES_PASSWORD), 10)}")
    except Exception as e:
        logger.error(f"설정 로드 실패: {e}")
        logger.error(traceback.format_exc())
        logger.error("=" * 50)
        logger.error("문제 해결 방법:")
        logger.error("1. .env 파일이 프로젝트 루트에 있는지 확인하세요.")
        logger.error("2. .env 파일이 UTF-8 인코딩으로 저장되어 있는지 확인하세요.")
        logger.error("3. .env 파일의 내용을 확인하세요.")
        logger.error("=" * 50)
        return
    
    # 테이블 생성
    try:
        logger.info("데이터베이스 연결 확인 중...")
        # 연결 테스트
        with engine.connect() as conn:
            logger.info("데이터베이스 연결 성공")
        
        logger.info("데이터베이스 테이블 생성 중...")
        Base.metadata.create_all(bind=engine)
        logger.info("데이터베이스 테이블 생성 완료")
    except Exception as e:
        logger.error(f"데이터베이스 테이블 생성 실패: {e}")
        logger.error(traceback.format_exc())
        logger.error("=" * 50)
        logger.error("문제 해결 방법:")
        logger.error("1. PostgreSQL이 실행 중인지 확인하세요.")
        logger.error("2. .env 파일의 데이터베이스 연결 정보를 확인하세요.")
        logger.error("3. .env 파일이 UTF-8 인코딩으로 저장되어 있는지 확인하세요.")
        logger.error("4. 비밀번호에 특수 문자가 있는 경우 URL 인코딩이 필요할 수 있습니다.")
        logger.error("=" * 50)
        return
    
    # 한국 기업명 업데이트
    kr_count = update_korean_stocks()
    
    # 미국 기업명 업데이트
    us_count = update_us_stocks()
    
    logger.info("=" * 50)
    logger.info("업데이트 완료!")
    logger.info(f"한국 기업: {kr_count}개")
    logger.info(f"미국 기업: {us_count}개")
    logger.info(f"총 기업: {kr_count + us_count}개")
    logger.info("=" * 50)
    logger.info("서버를 재시작하면 새로운 기업명 데이터가 로드됩니다.")


if __name__ == "__main__":
    main()

