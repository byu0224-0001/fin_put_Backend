"""
테스트 전 최종 확인 쿼리 실행 스크립트

P0-α: industry_edges → company_edges 연결 경로 검증
"""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

load_dotenv(project_root / '.env')

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# DB 연결
user = quote_plus(os.getenv('POSTGRES_USER', 'postgres'))
pwd = quote_plus(os.getenv('POSTGRES_PASSWORD', 'postgres'))
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'newsdb')
url = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

engine = create_engine(url)

def test_industry_to_company_connection():
    """특정 기업의 산업 간접 노출 드라이버 조회"""
    logger.info("=" * 80)
    logger.info("테스트 1: industry_edges → company_edges 연결 경로 검증")
    logger.info("=" * 80)
    
    query = text("""
        SELECT 
            ie.source_driver_code,
            ie.target_sector_code,
            ie.logic_summary,
            ie.key_sentence,
            isec.major_sector,
            isec.sub_sector
        FROM industry_edges ie
        JOIN investor_sector isec 
            ON ie.target_sector_code = isec.major_sector
        WHERE isec.ticker = :ticker
            AND ie.valid_from <= CURRENT_DATE
            AND ie.valid_to >= CURRENT_DATE
        ORDER BY ie.created_at DESC
        LIMIT 5;
    """)
    
    test_tickers = ['000660', '005930', '035420']  # SK하이닉스, 삼성전자, NAVER
    
    with engine.connect() as conn:
        for ticker in test_tickers:
            logger.info(f"\n티커: {ticker}")
            result = conn.execute(query, {"ticker": ticker})
            rows = result.fetchall()
            
            if rows:
                logger.info(f"  ✅ {len(rows)}개 industry_edges 연결 발견")
                for row in rows:
                    logger.info(f"    - Driver: {row[0]}, Sector: {row[1]}")
                    logger.info(f"      Logic: {row[2][:100] if row[2] else 'N/A'}...")
            else:
                logger.info(f"  ⚠️ 연결된 industry_edges 없음 (정상: 아직 데이터가 없을 수 있음)")

def test_route_confidence_gate():
    """route_confidence 게이트 동작 확인"""
    logger.info("\n" + "=" * 80)
    logger.info("테스트 2: route_confidence 게이트 동작 확인")
    logger.info("=" * 80)
    
    query = text("""
        SELECT 
            report_id,
            processing_status,
            key_points->>'route_confidence' as route_confidence,
            key_points->>'hold_reason' as hold_reason
        FROM broker_reports
        WHERE processing_status = 'HOLD'
            AND key_points->>'hold_reason' = 'ROUTE_CONFIDENCE_LOW'
        ORDER BY created_at DESC
        LIMIT 10;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        
        if rows:
            logger.info(f"  ✅ {len(rows)}개 ROUTE_CONFIDENCE_LOW 리포트 발견")
            for row in rows:
                logger.info(f"    - Report ID: {row[0]}")
                logger.info(f"      Confidence: {row[2]}")
                logger.info(f"      Hold Reason: {row[3]}")
        else:
            logger.info("  ⚠️ ROUTE_CONFIDENCE_LOW 리포트 없음 (정상: 아직 데이터가 없을 수 있음)")

def test_sector_mapping_fallback():
    """섹터 코드 매핑 실패 처리 확인"""
    logger.info("\n" + "=" * 80)
    logger.info("테스트 3: 섹터 코드 매핑 실패 처리 확인")
    logger.info("=" * 80)
    
    query = text("""
        SELECT 
            report_id,
            processing_status,
            key_points->>'hold_reason' as hold_reason,
            key_points->>'retry_after' as retry_after
        FROM broker_reports
        WHERE processing_status = 'HOLD'
            AND key_points->>'hold_reason' = 'SECTOR_MAPPING_FAILED'
        ORDER BY created_at DESC
        LIMIT 10;
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        
        if rows:
            logger.info(f"  ✅ {len(rows)}개 SECTOR_MAPPING_FAILED 리포트 발견")
            for row in rows:
                logger.info(f"    - Report ID: {row[0]}")
                logger.info(f"      Hold Reason: {row[2]}")
                logger.info(f"      Retry After: {row[3]}")
        else:
            logger.info("  ⚠️ SECTOR_MAPPING_FAILED 리포트 없음 (정상: 아직 데이터가 없을 수 있음)")

def main():
    """메인 실행 함수"""
    logger.info("=" * 80)
    logger.info("테스트 전 최종 확인 쿼리 실행")
    logger.info("=" * 80)
    
    try:
        # 테스트 1: 연결 경로 검증
        test_industry_to_company_connection()
        
        # 테스트 2: route_confidence 게이트
        test_route_confidence_gate()
        
        # 테스트 3: 섹터 매핑 실패 처리
        test_sector_mapping_fallback()
        
        logger.info("\n" + "=" * 80)
        logger.info("모든 테스트 완료")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"테스트 실행 중 오류: {e}", exc_info=True)

if __name__ == "__main__":
    main()

