"""NULL 섹터만 재분류 실행 스크립트"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime
import logging

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

# UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()

from app.db import SessionLocal
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector
from app.services.sector_classifier_ensemble_won import classify_sector_ensemble_won
from app.services.gemini_handler import GeminiHandler
from app.config import settings
from sqlalchemy.orm import Session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_null_sector_tickers(db: Session) -> list:
    """NULL 섹터를 가진 기업 티커 리스트 조회"""
    result = db.execute(text("""
        SELECT DISTINCT ticker
        FROM investor_sector
        WHERE is_primary = true
            AND sector_l1 IS NULL
        ORDER BY ticker
    """))
    return [row[0] for row in result.fetchall()]

def verify_fallback_results(db: Session):
    """Fallback 결과 검증"""
    logger.info("=" * 80)
    logger.info("Fallback 결과 검증")
    logger.info("=" * 80)
    
    # NULL L1 카운트
    result = db.execute(text("""
        SELECT COUNT(*) 
        FROM investor_sector
        WHERE is_primary = true AND sector_l1 IS NULL
    """))
    null_l1_count = result.fetchone()[0]
    logger.info(f"NULL L1 개수: {null_l1_count:,}개")
    
    # fallback_used 카운트
    result = db.execute(text("""
        SELECT 
            COUNT(*) FILTER (WHERE fallback_used IS NOT NULL) AS has_fallback,
            COUNT(*) FILTER (WHERE fallback_used = 'RULE') AS rule_fallback,
            COUNT(*) FILTER (WHERE fallback_used = 'TOP1') AS top1_fallback,
            COUNT(*) FILTER (WHERE fallback_used = 'KRX') AS krx_fallback,
            COUNT(*) FILTER (WHERE fallback_used = 'UNKNOWN') AS unknown_fallback
        FROM investor_sector
        WHERE is_primary = true
    """))
    row = result.fetchone()
    has_fallback, rule_fallback, top1_fallback, krx_fallback, unknown_fallback = row
    
    logger.info(f"fallback_used 필드 상태:")
    logger.info(f"  NULL이 아닌 값: {has_fallback:,}개")
    logger.info(f"  RULE: {rule_fallback:,}개")
    logger.info(f"  TOP1: {top1_fallback:,}개")
    logger.info(f"  KRX: {krx_fallback:,}개")
    logger.info(f"  UNKNOWN: {unknown_fallback:,}개")
    logger.info("=" * 80)
    
    # 성공 조건 확인
    success = null_l1_count == 0 and has_fallback > 0
    if success:
        logger.info("✅ 성공 조건 달성:")
        logger.info(f"  - NULL L1 = 0")
        logger.info(f"  - fallback_used 레코드 존재")
    else:
        logger.warning("⚠️ 성공 조건 미달성:")
        if null_l1_count > 0:
            logger.warning(f"  - NULL L1 = {null_l1_count:,}개 (목표: 0)")
        if has_fallback == 0:
            logger.warning(f"  - fallback_used 레코드 없음")
    
    return success

def main():
    logger.info("=" * 80)
    logger.info("NULL 섹터만 재분류 실행")
    logger.info(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    logger.info("")
    
    db = SessionLocal()
    try:
        # NULL 섹터 티커 조회
        tickers = get_null_sector_tickers(db)
        total = len(tickers)
        
        if total == 0:
            logger.info("✅ NULL 섹터가 없습니다. 재분류할 기업이 없습니다.")
            verify_fallback_results(db)
            return
        
        logger.info(f"NULL 섹터 기업 수: {total:,}개")
        logger.info("")
        
        # Gemini Handler 초기화
        gemini_handler = None
        if settings.GEMINI_API_KEY:
            try:
                gemini_handler = GeminiHandler(api_key=settings.GEMINI_API_KEY)
                logger.info("✅ Gemini Handler 초기화 완료")
            except Exception as e:
                logger.warning(f"⚠️ Gemini Handler 초기화 실패: {e}")
        
        # 각 기업 재분류
        success_count = 0
        fail_count = 0
        
        for idx, ticker in enumerate(tickers, 1):
            try:
                logger.info(f"[{idx}/{total}] {ticker} 재분류 중...")
                
                # 기존 분류 삭제
                db.query(InvestorSector).filter(
                    InvestorSector.ticker == ticker
                ).delete()
                db.commit()
                
                # 재분류
                results = classify_sector_ensemble_won(
                    db=db,
                    ticker=ticker,
                    gemini_handler=gemini_handler,
                    use_embedding=True,
                    use_reranking=True,
                    max_sectors=3,
                    force_reclassify=True
                )
                
                if not results:
                    logger.warning(f"[{idx}/{total}] {ticker} 재분류 실패 (결과 없음)")
                    fail_count += 1
                    continue
                
                # 결과 저장
                for i, result in enumerate(results):
                    # ⭐ NEW: 저장 전 최종 검증
                    if not result.get('sector_l1') and not result.get('major_sector'):
                        logger.warning(f"[{idx}/{total}] {ticker} 저장 전 NULL 섹터 감지, 강제 Fallback 적용")
                        result['sector_l1'] = 'SEC_UNKNOWN'
                        result['major_sector'] = 'SEC_UNKNOWN'
                        result['fallback_used'] = 'TRUE'  # ⭐ VARCHAR에 문자열 저장
                        result['fallback_type'] = 'UNKNOWN'  # ⭐ 타입 분리
                        result['confidence'] = 'VERY_LOW'
                        result['method'] = 'FALLBACK_UNKNOWN'
                        result['ensemble_score'] = 0.0
                        result['reasoning'] = 'NULL 섹터 감지, UNKNOWN 할당'
                    
                    method = result.get('method', 'ENSEMBLE')
                    sub_sector_str = result.get('sub_sector', '') or ''
                    sector_id = f"{ticker}_{result['major_sector']}"
                    if sub_sector_str:
                        sector_id += f"_{sub_sector_str}"
                    if i > 0:
                        sector_id += f"_{i}"
                    
                    investor_sector = InvestorSector(
                        id=sector_id,
                        ticker=ticker,
                        major_sector=result.get('major_sector'),
                        sub_sector=result.get('sub_sector'),
                        value_chain=result.get('value_chain'),
                        sector_weight=result.get('weight', 0.5),
                        is_primary=result.get('is_primary', (i == 0)),
                        classification_method=method,
                        confidence=result.get('confidence', 'MEDIUM'),
                        fallback_used=result.get('fallback_used') or 'FALSE',  # ⭐ Fallback 사용 여부 (기본값: 'FALSE')
                        fallback_type=result.get('fallback_type'),  # ⭐ Fallback 타입
                        rule_score=result.get('rule_score'),
                        embedding_score=result.get('embedding_score'),
                        bge_score=result.get('bge_score'),
                        gpt_score=result.get('gpt_score'),
                        ensemble_score=result.get('ensemble_score'),
                        classification_reasoning=result.get('reasoning'),
                        causal_structure=result.get('causal_structure'),
                        investment_insights=result.get('investment_insights'),
                        rule_version=result.get('rule_version'),
                        rule_confidence=result.get('rule_confidence'),
                        training_label=result.get('training_label', False),
                        sector_l1=result.get('sector_l1') or result.get('major_sector'),
                        sector_l2=result.get('sector_l2') or result.get('sub_sector'),
                        sector_l3_tags=result.get('sector_l3_tags') or result.get('causal_structure', {}).get('sector_l3_tags', []),
                        boosting_log=result.get('boosting_log')
                    )
                    db.add(investor_sector)
                
                # ⭐ NEW: commit 전후 검증
                try:
                    db.commit()
                    logger.info(f"[{idx}/{total}] {ticker} commit 성공")
                    
                    # commit 후 검증
                    saved = db.query(InvestorSector).filter(
                        InvestorSector.ticker == ticker,
                        InvestorSector.is_primary == True
                    ).first()
                    
                    if not saved:
                        logger.error(f"[{idx}/{total}] {ticker} 저장 검증 실패: 레코드 없음")
                        db.rollback()
                        fail_count += 1
                        continue
                    
                    if not saved.sector_l1:
                        logger.error(f"[{idx}/{total}] {ticker} 저장 검증 실패: sector_l1이 NULL")
                        db.rollback()
                        fail_count += 1
                        continue
                    
                    # fallback_used 확인
                    if saved.fallback_used:
                        logger.info(f"[{idx}/{total}] {ticker} ✅ Fallback 저장 확인: fallback_used={saved.fallback_used}, fallback_type={saved.fallback_type}")
                    
                    success_count += 1
                except Exception as e:
                    logger.error(f"[{idx}/{total}] {ticker} commit 실패: {e}", exc_info=True)
                    db.rollback()
                    fail_count += 1
                    continue
                
                # Fallback 사용 여부 로깅
                primary_result = next((r for r in results if r.get('is_primary')), results[0] if results else None)
                if primary_result:
                    fallback_used = primary_result.get('fallback_used')
                    sector_l1 = primary_result.get('sector_l1') or primary_result.get('major_sector')
                    if fallback_used:
                        logger.info(f"[{idx}/{total}] {ticker} ✅ Fallback 사용: {fallback_used}, 섹터: {sector_l1}")
                    else:
                        logger.info(f"[{idx}/{total}] {ticker} ✅ 정상 분류, 섹터: {sector_l1}")
                
                if idx % 50 == 0:
                    logger.info(f"진행 상황: {idx}/{total} ({idx/total*100:.1f}%), 성공: {success_count}, 실패: {fail_count}")
                    
            except Exception as e:
                logger.error(f"[{idx}/{total}] {ticker} 처리 중 오류: {e}", exc_info=True)
                db.rollback()
                fail_count += 1
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("재분류 완료")
        logger.info(f"총 기업 수: {total:,}개")
        logger.info(f"성공: {success_count:,}개")
        logger.info(f"실패: {fail_count:,}개")
        logger.info("=" * 80)
        logger.info("")
        
        # 최종 검증
        verify_fallback_results(db)
        
    finally:
        db.close()

if __name__ == '__main__':
    main()

