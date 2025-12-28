"""임베딩 없는 기업만 대상으로 임베딩 생성 배치 스크립트"""
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
from app.utils.stock_query import get_stock_by_ticker_safe
from app.services.solar_embedding_model import (
    get_or_create_embedding,
    prepare_company_text_for_solar
)
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_embeddings_completion(db: Session):
    """임베딩 완료 상태 DB 검증"""
    logger.info("=" * 80)
    logger.info("임베딩 완료 상태 검증 시작")
    logger.info("=" * 80)
    
    # 임베딩 없는 기업 수 확인
    result = db.execute(text("""
        SELECT COUNT(DISTINCT i.ticker) AS missing_count
        FROM investor_sector i
        LEFT JOIN company_embeddings e ON i.ticker = e.ticker
        WHERE i.is_primary = true
          AND e.ticker IS NULL
    """))
    missing_count = result.fetchone()[0]
    
    # 전체 기업 수 확인
    result = db.execute(text("""
        SELECT COUNT(DISTINCT ticker) AS total_count
        FROM investor_sector
        WHERE is_primary = true
    """))
    total_count = result.fetchone()[0]
    
    # 임베딩 있는 기업 수 확인
    result = db.execute(text("""
        SELECT COUNT(DISTINCT ticker) AS with_embedding_count
        FROM company_embeddings
    """))
    with_embedding_count = result.fetchone()[0]
    
    # 이번 실행에서 새로 생성된 수 (최근 1시간 내)
    result = db.execute(text("""
        SELECT COUNT(DISTINCT ticker) AS new_count
        FROM company_embeddings
        WHERE created_at >= NOW() - INTERVAL '1 hour'
    """))
    new_count = result.fetchone()[0]
    
    logger.info(f"전체 기업 수: {total_count:,}개")
    logger.info(f"임베딩 있는 기업 수: {with_embedding_count:,}개")
    logger.info(f"임베딩 없는 기업 수: {missing_count:,}개")
    logger.info(f"이번 실행에서 새로 생성: {new_count:,}개")
    
    if missing_count == 0:
        logger.info("✅ 모든 기업의 임베딩이 완료되었습니다!")
    else:
        logger.warning(f"⚠️ {missing_count:,}개 기업의 임베딩이 아직 없습니다.")
        logger.warning("원인 확인:")
        logger.warning("  1. text_hash 매칭 조건이 너무 엄격한지 확인")
        logger.warning("  2. DB 저장 실패 로그 확인")
        logger.warning("  3. API Rate Limit 확인")
    
    logger.info("=" * 80)
    return missing_count, total_count, with_embedding_count, new_count

def main():
    """임베딩 없는 기업만 대상으로 임베딩 생성"""
    db = SessionLocal()
    
    try:
        # 임베딩 없는 기업 조회
        result = db.execute(text("""
            SELECT DISTINCT i.ticker
            FROM investor_sector i
            LEFT JOIN company_embeddings e ON i.ticker = e.ticker
            WHERE i.is_primary = true
              AND e.ticker IS NULL
            ORDER BY i.ticker
        """))
        
        tickers = [row[0] for row in result.fetchall()]
        total_count = len(tickers)
        
        logger.info("=" * 80)
        logger.info(f"임베딩 없는 기업 수: {total_count}개")
        logger.info("=" * 80)
        
        if total_count == 0:
            logger.info("임베딩이 필요한 기업이 없습니다.")
            # 최종 검증
            verify_embeddings_completion(db)
            return
        
        # 실행 전 상태 기록
        before_count = total_count
        cache_hit_count = 0
        api_call_count = 0
        db_save_success_count = 0
        db_save_fail_count = 0
        
        success_count = 0
        fail_count = 0
        skip_count = 0
            
        for idx, ticker in enumerate(tickers, 1):
            try:
                # CompanyDetail 조회
                company_detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == ticker
                ).first()
                
                if not company_detail:
                    logger.warning(f"[{idx}/{total_count}] [{ticker}] CompanyDetail 없음 → 스킵")
                    skip_count += 1
                    continue
                
                # Stock 조회 (회사명)
                stock = get_stock_by_ticker_safe(db, ticker)
                company_name = stock.stock_name if stock else None
                
                # 텍스트 준비
                company_text = prepare_company_text_for_solar(company_detail, company_name)
                
                if not company_text or len(company_text.strip()) == 0:
                    logger.warning(f"[{idx}/{total_count}] [{ticker}] 텍스트 없음 → 스킵")
                    skip_count += 1
                    continue
                
                # 임베딩 생성 (이미 있으면 스킵)
                # ⭐ 로그에서 cache_hit, api_call, db_save_success 추적
                embedding = get_or_create_embedding(
                    db=db,
                    ticker=ticker,
                    text=company_text,
                    force_regenerate=False
                )
                
                if embedding is not None:
                    success_count += 1
                    # 로그에서 통계 추적 (간단한 추정)
                    # 실제로는 로그 파싱이 필요하지만, 여기서는 성공 = 저장 성공으로 간주
                    db_save_success_count += 1
                    if idx % 50 == 0:
                        logger.info(f"[{idx}/{total_count}] 진행 중... (성공: {success_count}, 실패: {fail_count}, 스킵: {skip_count})")
                else:
                    fail_count += 1
                    db_save_fail_count += 1
                    logger.warning(f"[{idx}/{total_count}] [{ticker}] 임베딩 생성 실패")
                
            except Exception as e:
                fail_count += 1
                logger.error(f"[{idx}/{total_count}] [{ticker}] 처리 중 오류: {e}", exc_info=True)
        
        logger.info("=" * 80)
        logger.info("임베딩 생성 완료")
        logger.info(f"총 기업 수: {total_count}개")
        logger.info(f"성공: {success_count}개")
        logger.info(f"실패: {fail_count}개")
        logger.info(f"스킵: {skip_count}개")
        if total_count > 0:
            logger.info(f"성공률: {success_count/total_count*100:.1f}%")
        logger.info(f"DB 저장 성공: {db_save_success_count}개")
        logger.info(f"DB 저장 실패: {db_save_fail_count}개")
        logger.info("=" * 80)
        
        # ⭐ 최종 검증: DB 기반 실제 상태 확인
        verify_embeddings_completion(db)
    except Exception as e:
        logger.error(f"전체 처리 중 오류: {e}", exc_info=True)
        raise
        
    finally:
        db.close()

if __name__ == '__main__':
    main()

