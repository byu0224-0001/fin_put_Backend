"""누락된 기업 데이터 수집 및 임베딩 생성"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
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
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.utils.stock_query import get_stock_by_ticker_safe
from app.services.solar_embedding_model import (
    get_or_create_embedding,
    prepare_company_text_for_solar
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_missing_tickers():
    """누락된 기업 티커 목록 가져오기"""
    # 최근 생성된 missing_stocks 파일 찾기
    reports_dir = project_root / "reports"
    if not reports_dir.exists():
        return []
    
    missing_files = sorted(reports_dir.glob("missing_stocks_*.txt"), reverse=True)
    if not missing_files:
        logger.warning("누락된 기업 리스트 파일을 찾을 수 없습니다.")
        return []
    
    latest_file = missing_files[0]
    logger.info(f"누락된 기업 리스트 파일: {latest_file}")
    
    tickers = []
    with open(latest_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('=') and not line.startswith('누락'):
                parts = line.split('\t')
                if len(parts) >= 1 and parts[0]:
                    ticker = parts[0].strip()
                    if ticker and len(ticker) == 6:  # 6자리 티커
                        tickers.append(ticker)
    
    # 460470도 포함
    if '460470' not in tickers:
        tickers.append('460470')
    
    return tickers

def collect_company_data(db: Session, ticker: str):
    """기업 데이터 수집 (04_fetch_dart.py의 process_company 함수 사용)"""
    try:
        from scripts.fetch_dart import process_company
        from app.services.dart_parser import DartParser
        from app.services.llm_handler import LLMHandler
        from app.utils.stock_query import get_stock_by_ticker_safe
        import os
        
        logger.info(f"[{ticker}] DART 데이터 수집 시작...")
        
        # DART API 키 확인
        dart_api_key = os.getenv('DART_API_KEY')
        if not dart_api_key:
            logger.error("DART_API_KEY가 설정되지 않았습니다.")
            return False
        
        # LLM API 키 확인
        openai_api_key = os.getenv('OPENAI_API_KEY')
        if not openai_api_key:
            logger.error("OPENAI_API_KEY가 설정되지 않았습니다.")
            return False
        
        # 최근 1년간 사업보고서 조회
        current_year = datetime.now().year
        
        # Stock 정보 조회 (회사명)
        stock = get_stock_by_ticker_safe(db, ticker)
        company_name = stock.stock_name if stock else ticker
        
        # DartParser 및 LLMHandler 초기화
        dart_parser = DartParser(dart_api_key)
        llm_handler = LLMHandler(
            analysis_model="gpt-5-mini",
            summary_model="gpt-5-nano",
            api_key=openai_api_key
        )
        
        # process_company 함수 호출
        result = process_company(
            ticker=ticker,
            company_name=company_name,
            year=current_year,
            dart_parser=dart_parser,
            llm_handler=llm_handler,
            skip_existing=False
        )
        
        if result.get('status') == 'SUCCESS':
            logger.info(f"[{ticker}] DART 데이터 수집 완료")
            return True
        else:
            error_msg = result.get('error', '알 수 없는 오류')
            logger.warning(f"[{ticker}] DART 데이터 수집 실패: {error_msg}")
            return False
            
    except ImportError:
        # 04_fetch_dart.py의 process_company 직접 import 시도
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "fetch_dart",
                project_root / "scripts" / "04_fetch_dart.py"
            )
            fetch_dart_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(fetch_dart_module)
            
            from app.services.dart_parser import DartParser
            from app.services.llm_handler import LLMHandler
            from app.utils.stock_query import get_stock_by_ticker_safe
            import os
            
            logger.info(f"[{ticker}] DART 데이터 수집 시작...")
            
            dart_api_key = os.getenv('DART_API_KEY')
            openai_api_key = os.getenv('OPENAI_API_KEY')
            
            if not dart_api_key or not openai_api_key:
                logger.error("API 키가 설정되지 않았습니다.")
                return False
            
            current_year = datetime.now().year
            stock = get_stock_by_ticker_safe(db, ticker)
            company_name = stock.stock_name if stock else ticker
            
            dart_parser = DartParser(dart_api_key)
            llm_handler = LLMHandler(
                analysis_model="gpt-5-mini",
                summary_model="gpt-5-nano",
                api_key=openai_api_key
            )
            
            result = fetch_dart_module.process_company(
                ticker=ticker,
                company_name=company_name,
                year=current_year,
                dart_parser=dart_parser,
                llm_handler=llm_handler,
                skip_existing=False
            )
            
            if result.get('status') == 'SUCCESS':
                logger.info(f"[{ticker}] DART 데이터 수집 완료")
                return True
            else:
                logger.warning(f"[{ticker}] DART 데이터 수집 실패: {result.get('error', '알 수 없는 오류')}")
                return False
                
        except Exception as e:
            logger.error(f"[{ticker}] DART 데이터 수집 중 오류: {e}", exc_info=True)
            return False
    except Exception as e:
        logger.error(f"[{ticker}] DART 데이터 수집 중 오류: {e}", exc_info=True)
        return False

def generate_embedding(db: Session, ticker: str):
    """임베딩 생성"""
    try:
        # CompanyDetail 조회
        company_detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == ticker
        ).first()
        
        if not company_detail:
            logger.warning(f"[{ticker}] CompanyDetail 없음 → 임베딩 생성 불가")
            return False
        
        # Stock 조회 (회사명)
        stock = get_stock_by_ticker_safe(db, ticker)
        company_name = stock.stock_name if stock else None
        
        # 텍스트 준비
        company_text = prepare_company_text_for_solar(company_detail, company_name)
        
        if not company_text or len(company_text.strip()) == 0:
            logger.warning(f"[{ticker}] 텍스트 없음 → 임베딩 생성 불가")
            return False
        
        # 임베딩 생성
        embedding = get_or_create_embedding(
            db=db,
            ticker=ticker,
            text=company_text,
            force_regenerate=False
        )
        
        if embedding is not None:
            logger.info(f"[{ticker}] 임베딩 생성 완료")
            return True
        else:
            logger.warning(f"[{ticker}] 임베딩 생성 실패")
            return False
            
    except Exception as e:
        logger.error(f"[{ticker}] 임베딩 생성 중 오류: {e}", exc_info=True)
        return False

def main():
    """메인 실행 함수"""
    db = SessionLocal()
    
    try:
        logger.info("=" * 80)
        logger.info("누락된 기업 데이터 수집 및 임베딩 생성 시작")
        logger.info("=" * 80)
        
        # 1. 누락된 기업 티커 목록 가져오기
        tickers = get_missing_tickers()
        logger.info(f"처리 대상 기업: {len(tickers)}개")
        
        if len(tickers) == 0:
            logger.info("처리할 기업이 없습니다.")
            return
        
        # 2. 각 기업에 대해 데이터 수집 및 임베딩 생성
        data_collected = 0
        embedding_generated = 0
        failed = 0
        
        for idx, ticker in enumerate(tickers, 1):
            logger.info(f"[{idx}/{len(tickers)}] {ticker} 처리 중...")
            
            try:
                # 2-1. 데이터 수집
                if collect_company_data(db, ticker):
                    data_collected += 1
                    
                    # 2-2. 임베딩 생성
                    if generate_embedding(db, ticker):
                        embedding_generated += 1
                else:
                    failed += 1
                    logger.warning(f"[{ticker}] 데이터 수집 실패")
                    
            except Exception as e:
                failed += 1
                logger.error(f"[{ticker}] 처리 중 오류: {e}", exc_info=True)
        
        logger.info("=" * 80)
        logger.info("작업 완료")
        logger.info(f"  총 기업 수: {len(tickers)}개")
        logger.info(f"  데이터 수집 성공: {data_collected}개")
        logger.info(f"  임베딩 생성 성공: {embedding_generated}개")
        logger.info(f"  실패: {failed}개")
        logger.info("=" * 80)
        
        # 3. 최종 상태 확인
        logger.info("=" * 80)
        logger.info("최종 상태 확인")
        logger.info("=" * 80)
        
        result = db.execute(text("""
            SELECT COUNT(DISTINCT i.ticker) AS missing_count
            FROM investor_sector i
            LEFT JOIN company_embeddings e ON i.ticker = e.ticker
            WHERE i.is_primary = true
              AND e.ticker IS NULL
        """))
        missing_count = result.fetchone()[0]
        
        result = db.execute(text("""
            SELECT COUNT(DISTINCT ticker) AS total_count
            FROM investor_sector
            WHERE is_primary = true
        """))
        total_count = result.fetchone()[0]
        
        result = db.execute(text("""
            SELECT COUNT(DISTINCT ticker) AS with_embedding_count
            FROM company_embeddings
        """))
        with_embedding_count = result.fetchone()[0]
        
        logger.info(f"전체 기업 수: {total_count:,}개")
        logger.info(f"임베딩 있는 기업: {with_embedding_count:,}개")
        logger.info(f"임베딩 없는 기업: {missing_count:,}개")
        
        if missing_count == 0:
            logger.info("✅ 모든 기업의 임베딩이 완료되었습니다!")
        else:
            logger.warning(f"⚠️ {missing_count:,}개 기업의 임베딩이 아직 없습니다.")
        
        logger.info("=" * 80)
        
    finally:
        db.close()

if __name__ == '__main__':
    main()

