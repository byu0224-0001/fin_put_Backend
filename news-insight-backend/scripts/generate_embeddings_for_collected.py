"""수집 완료된 기업의 임베딩 생성"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

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
from app.services.solar_embedding_model import (
    get_or_create_embedding,
    prepare_company_text_for_solar
)
from app.utils.stock_query import get_stock_by_ticker_safe
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """메인 실행 함수"""
    db = SessionLocal()
    
    try:
        # 누락된 기업 목록
        missing_tickers = [
            '460470', '0009K0', '0015N0', '0015S0', '0054V0', '0068Y0',
            '0088D0', '0091W0', '0093G0', '0096B0', '0096D0', '0098T0',
            '0105P0', '0120G0', '0126Z0', '217590', '261520', '388210',
            '464490', '466690', '488900', '950250'
        ]
        
        logger.info("=" * 80)
        logger.info("수집 완료된 기업의 임베딩 생성 시작")
        logger.info("=" * 80)
        
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for idx, ticker in enumerate(missing_tickers, 1):
            logger.info(f"[{idx}/{len(missing_tickers)}] {ticker} 처리 중...")
            
            try:
                # CompanyDetail 조회
                company_detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == ticker
                ).first()
                
                if not company_detail:
                    logger.warning(f"[{ticker}] CompanyDetail 없음 → 스킵")
                    skipped_count += 1
                    continue
                
                # Stock 조회 (회사명)
                stock = get_stock_by_ticker_safe(db, ticker)
                company_name = stock.stock_name if stock else None
                
                # 텍스트 준비
                company_text = prepare_company_text_for_solar(company_detail, company_name)
                
                if not company_text or len(company_text.strip()) == 0:
                    logger.warning(f"[{ticker}] 텍스트 없음 → 스킵")
                    skipped_count += 1
                    continue
                
                # 임베딩 생성
                embedding = get_or_create_embedding(
                    db=db,
                    ticker=ticker,
                    text=company_text,
                    force_regenerate=False
                )
                
                if embedding is not None:
                    logger.info(f"[{ticker}] 임베딩 생성 완료")
                    success_count += 1
                else:
                    logger.warning(f"[{ticker}] 임베딩 생성 실패")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"[{ticker}] 임베딩 생성 중 오류: {e}", exc_info=True)
                failed_count += 1
        
        logger.info("=" * 80)
        logger.info("작업 완료")
        logger.info(f"  성공: {success_count}개")
        logger.info(f"  실패: {failed_count}개")
        logger.info(f"  스킵: {skipped_count}개")
        logger.info(f"  총계: {len(missing_tickers)}개")
        logger.info("=" * 80)
        
    finally:
        db.close()

if __name__ == '__main__':
    main()

