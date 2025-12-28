"""
섹터 자동 분류 스크립트

company_details 데이터를 기반으로 InvestorSector 테이블에 섹터 분류 저장
"""
import sys
import os
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock
from app.services.sector_classifier import classify_sector, detect_secondary_sectors
from app.services.llm_handler import LLMHandler
from app.config import settings
import logging
import argparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='섹터 자동 분류')
    parser.add_argument('--ticker', type=str, help='특정 티커만 처리')
    parser.add_argument('--use-llm', action='store_true', help='LLM Fallback 사용')
    parser.add_argument('--overwrite', action='store_true', help='기존 분류 덮어쓰기')
    args = parser.parse_args()
    
    db = SessionLocal()
    llm_handler = None
    
    if args.use_llm:
        llm_handler = LLMHandler(api_key=settings.OPENAI_API_KEY)
        logger.info("LLM Fallback 활성화")
    
    try:
        # 처리할 티커 목록
        if args.ticker:
            tickers = [args.ticker]
        else:
            # company_details에 있는 모든 티커
            details = db.query(CompanyDetail.ticker).distinct().all()
            tickers = [t[0] for t in details]
        
        logger.info(f"총 {len(tickers)}개 기업 섹터 분류 시작")
        
        success_count = 0
        skip_count = 0
        fail_count = 0
        
        for ticker in tickers:
            try:
                # 기존 분류 확인
                existing = db.query(InvestorSector).filter(
                    InvestorSector.ticker == ticker
                ).first()
                
                if existing and not args.overwrite:
                    logger.info(f"[{ticker}] 이미 분류됨 (스킵): {existing.major_sector}")
                    skip_count += 1
                    continue
                
                # CompanyDetail 조회 (제품 리스트용)
                company_detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == ticker
                ).first()
                
                # Primary 섹터 분류
                result = classify_sector(
                    db, ticker, llm_handler=llm_handler, use_llm=args.use_llm
                )
                
                if not result:
                    logger.warning(f"[{ticker}] 섹터 분류 실패")
                    fail_count += 1
                    continue
                
                primary_sector = {
                    'major': result['major_sector'],
                    'sub': result['sub_sector']
                }
                
                # Secondary 섹터 탐지 (제품 + 매출 비중 기반)
                secondary_sectors = []
                if company_detail and company_detail.products:
                    # 매출 비중 데이터 가져오기 (있으면)
                    revenue_by_segment = getattr(company_detail, 'revenue_by_segment', None)
                    
                    secondary_sectors = detect_secondary_sectors(
                        products=company_detail.products,
                        primary_sector=primary_sector,
                        revenue_by_segment=revenue_by_segment,
                        max_secondary_sectors=3,
                        min_revenue_threshold=10.0
                    )
                
                # 기존 데이터 삭제 (Multi-Sector 지원을 위해)
                if args.overwrite or existing:
                    db.query(InvestorSector).filter(
                        InvestorSector.ticker == ticker
                    ).delete()
                    db.commit()
                
                # Primary 섹터 저장
                primary_id = f"{ticker}_{result['major_sector']}"
                if result['sub_sector']:
                    primary_id += f"_{result['sub_sector']}"
                
                primary_record = InvestorSector(
                    id=primary_id,
                    ticker=ticker,
                    major_sector=result['major_sector'],
                    sub_sector=result['sub_sector'],
                    value_chain=result['value_chain'],
                    classification_method=result['classification_method'],
                    confidence=result['confidence'],
                    is_primary=True,
                    psw=0.0  # Phase 3에서 계산
                )
                db.add(primary_record)
                
                # Secondary 섹터 저장
                for sec_sector in secondary_sectors:
                    sec_id = f"{ticker}_{sec_sector['major']}"
                    if sec_sector.get('sub'):
                        sec_id += f"_{sec_sector['sub']}"
                    
                    # 중복 체크
                    existing_sec = db.query(InvestorSector).filter(
                        InvestorSector.id == sec_id
                    ).first()
                    
                    if existing_sec:
                        continue
                    
                    sec_record = InvestorSector(
                        id=sec_id,
                        ticker=ticker,
                        major_sector=sec_sector['major'],
                        sub_sector=sec_sector.get('sub'),
                        classification_method="PRODUCT_BASED",
                        confidence="MEDIUM",
                        is_primary=False,
                        psw=0.0  # Phase 3에서 계산
                    )
                    db.add(sec_record)
                
                db.commit()
                
                # 로그 출력
                sector_list = [f"{result['major_sector']}/{result['sub_sector']} (Primary)"]
                for sec in secondary_sectors:
                    sector_list.append(f"{sec['major']}/{sec.get('sub', '')} (Secondary)")
                
                logger.info(
                    f"[{ticker}] 섹터 분류 완료: {', '.join(sector_list)} "
                    f"({result['confidence']}, {result['classification_method']})"
                )
                success_count += 1
                
            except Exception as e:
                logger.error(f"[{ticker}] 섹터 분류 오류: {e}")
                db.rollback()
                fail_count += 1
        
        logger.info("=" * 60)
        logger.info("섹터 분류 완료!")
        logger.info(f"  성공: {success_count}개")
        logger.info(f"  스킵: {skip_count}개")
        logger.info(f"  실패: {fail_count}개")
        logger.info(f"  총계: {len(tickers)}개")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"전체 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        db.close()


if __name__ == "__main__":
    main()

