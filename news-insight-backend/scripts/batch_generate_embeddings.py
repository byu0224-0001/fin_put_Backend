"""
배치 임베딩 생성 스크립트

전체 기업의 Solar Embedding을 사전 생성하여 벡터 DB에 저장
- 재임베딩 방지 (text_hash 기반)
- 배치 처리로 API 호출 최적화
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
from dotenv import load_dotenv
from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.utils.stock_query import get_stock_by_ticker_safe
from app.services.sector_classifier_ensemble import _prepare_company_text_for_embedding
from app.services.solar_embedding_model import get_or_create_embedding
import logging
from tqdm import tqdm
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 환경 변수 로드
load_dotenv()


def batch_generate_embeddings(
    batch_size: int = 50,
    limit: int = None,
    force_regenerate: bool = False
):
    """
    전체 기업의 임베딩 배치 생성
    
    Args:
        batch_size: 배치 크기 (API 호출 최적화)
        limit: 처리할 최대 기업 수 (None이면 전체)
        force_regenerate: 강제 재생성 여부
    """
    db = SessionLocal()
    
    try:
        # CompanyDetail이 있는 모든 ticker 조회
        query = db.query(CompanyDetail).filter(
            CompanyDetail.biz_summary.isnot(None),
            CompanyDetail.biz_summary != ''
        )
        
        if limit:
            query = query.limit(limit)
        
        all_details = query.all()
        total_count = len(all_details)
        
        logger.info(f"📊 배치 임베딩 생성 시작: {total_count}개 기업")
        logger.info(f"   배치 크기: {batch_size}")
        logger.info(f"   강제 재생성: {force_regenerate}")
        
        success_count = 0
        skip_count = 0
        error_count = 0
        
        # 진행 상황 표시
        with tqdm(total=total_count, desc="임베딩 생성") as pbar:
            for i, detail in enumerate(all_details):
                ticker = detail.ticker
                stock = get_stock_by_ticker_safe(db, ticker)
                company_name = stock.stock_name if stock else None
                
                try:
                    # Solar Embedding용 텍스트 준비
                    text = _prepare_company_text_for_embedding(detail, company_name)
                    
                    if not text or len(text.strip()) < 10:
                        logger.warning(f"[{ticker}] 텍스트가 너무 짧아서 스킵")
                        skip_count += 1
                        pbar.update(1)
                        continue
                    
                    # 임베딩 생성 또는 조회
                    embedding = get_or_create_embedding(
                        db=db,
                        ticker=ticker,
                        text=text,
                        force_regenerate=force_regenerate
                    )
                    
                    if embedding is not None:
                        success_count += 1
                        if i % 10 == 0:
                            logger.debug(f"[{ticker}] 임베딩 생성/조회 완료")
                    else:
                        error_count += 1
                        logger.warning(f"[{ticker}] 임베딩 생성 실패")
                    
                    # 배치 크기마다 커밋
                    if (i + 1) % batch_size == 0:
                        db.commit()
                        logger.info(f"진행 상황: {i + 1}/{total_count} (성공: {success_count}, 스킵: {skip_count}, 오류: {error_count})")
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"[{ticker}] 임베딩 생성 중 오류: {e}")
                    db.rollback()
                
                pbar.update(1)
        
        # 최종 커밋
        db.commit()
        
        logger.info("=" * 60)
        logger.info("배치 임베딩 생성 완료")
        logger.info(f"  전체: {total_count}개")
        logger.info(f"  성공: {success_count}개")
        logger.info(f"  스킵: {skip_count}개")
        logger.info(f"  오류: {error_count}개")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"배치 임베딩 생성 중 오류: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="배치 임베딩 생성 스크립트")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="배치 크기 (기본값: 50)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="처리할 최대 기업 수 (기본값: 전체)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="강제 재생성 (기존 임베딩 무시)"
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Solar Embedding 배치 생성 스크립트")
    logger.info("=" * 60)
    
    batch_generate_embeddings(
        batch_size=args.batch_size,
        limit=args.limit,
        force_regenerate=args.force
    )

