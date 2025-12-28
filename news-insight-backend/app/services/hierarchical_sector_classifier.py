"""
계층적 섹터 분류 시스템

3단계 계층적 접근:
1. KRX 업종 → 투자 섹터 매핑 (1차 추정)
2. DART 내용 기반 재분류 (보정)
3. LLM 기반 최종 분류 (세부 조정)
"""
import logging
from typing import Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session

from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from app.models.investor_sector import InvestorSector
from app.models.industry_sector_mapping import IndustrySectorMapping
from app.services.sector_classifier import (
    classify_sector_rule_based,
    classify_sector_llm
)
from app.services.llm_handler import LLMHandler

logger = logging.getLogger(__name__)


def classify_sector_step1_krx(
    db: Session,
    ticker: str
) -> Tuple[Optional[str], Optional[str], str]:
    """
    Step 1: KRX 업종 → 투자 섹터 매핑 (1차 추정)
    
    Returns:
        (major_sector, sub_sector, confidence)
    """
    try:
        # stocks 테이블에서 industry_raw 조회 (Raw SQL 사용)
        from app.utils.stock_query import get_stock_by_ticker_safe
        stock = get_stock_by_ticker_safe(db, ticker)
        if not stock or not stock.industry_raw:
            return (None, None, "LOW")
        
        krx_sector = stock.industry_raw
        
        # industry_sector_mapping 테이블에서 매핑 조회
        mapping = db.query(IndustrySectorMapping).filter(
            IndustrySectorMapping.krx_sector == krx_sector
        ).first()
        
        if mapping and mapping.major_sector:
            # 매핑 테이블의 신뢰도 확인
            confidence = mapping.confidence or "MEDIUM"
            return (mapping.major_sector, mapping.sub_sector, confidence)
        
        return (None, None, "LOW")
        
    except Exception as e:
        logger.error(f"KRX 섹터 매핑 실패 ({ticker}): {e}")
        return (None, None, "LOW")


def classify_sector_step2_dart(
    db: Session,
    ticker: str,
    company_name: Optional[str] = None
) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    """
    Step 2: DART 내용 기반 재분류 (보정)
    
    Returns:
        (major_sector, sub_sector, value_chain, confidence)
    """
    try:
        # CompanyDetail 조회
        company_detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == ticker
        ).first()
        
        if not company_detail:
            return (None, None, None, "LOW")
        
        # 기존 rule-based 분류 사용
        major_sector, sub_sector, value_chain, confidence, _ = classify_sector_rule_based(
            company_detail=company_detail,
            company_name=company_name
        )
        
        return (major_sector, sub_sector, value_chain, confidence)
        
    except Exception as e:
        logger.error(f"DART 기반 분류 실패 ({ticker}): {e}")
        return (None, None, None, "LOW")


def classify_sector_hierarchical(
    db: Session,
    ticker: str,
    company_name: Optional[str] = None,
    llm_handler: Optional[LLMHandler] = None,
    use_llm_fallback: bool = True
) -> Optional[Dict[str, Any]]:
    """
    계층적 섹터 분류 (3단계)
    
    Args:
        db: DB 세션
        ticker: 종목코드
        company_name: 회사명 (선택)
        llm_handler: LLMHandler 객체 (Step 3용)
        use_llm_fallback: LLM Fallback 사용 여부
    
    Returns:
        {
            'major_sector': str,
            'sub_sector': str,
            'value_chain': str,
            'confidence': str,
            'classification_method': str,
            'krx_sector': str,
            'conflict_resolved': bool
        } 또는 None
    """
    logger.info(f"[계층적 분류] {ticker} ({company_name or 'N/A'}) 분류 시작")
    
    # Step 1: KRX 업종 기반 1차 분류
    krx_major, krx_sub, krx_confidence = classify_sector_step1_krx(db, ticker)
    
    # stocks 테이블에서 KRX 업종 조회 (로깅용, Raw SQL 사용)
    from app.utils.stock_query import get_stock_by_ticker_safe
    stock = get_stock_by_ticker_safe(db, ticker)
    krx_sector_raw = stock.industry_raw if stock else None
    
    logger.info(f"[Step 1] KRX: {krx_sector_raw} → {krx_major}/{krx_sub} (confidence: {krx_confidence})")
    
    # Step 2: DART 내용 기반 재분류
    dart_major, dart_sub, dart_value_chain, dart_confidence = classify_sector_step2_dart(
        db, ticker, company_name
    )
    
    logger.info(f"[Step 2] DART: → {dart_major}/{dart_sub} (confidence: {dart_confidence})")
    
    # 충돌 해결 로직
    conflict_resolved = False
    final_major = None
    final_sub = None
    final_value_chain = None
    final_confidence = "LOW"
    classification_method = "UNKNOWN"
    
    if krx_major and dart_major:
        # 두 방법 모두 결과가 있는 경우
        if krx_major == dart_major:
            # 일치: DART 내용이 더 상세할 수 있으므로 DART 우선
            final_major = dart_major
            final_sub = dart_sub or krx_sub
            final_value_chain = dart_value_chain
            final_confidence = "HIGH"  # 두 방법 일치 → 높은 신뢰도
            classification_method = "KRX_DART_MATCHED"
            logger.info(f"[결정] KRX와 DART 일치 → DART 결과 사용 (confidence: HIGH)")
        else:
            # 충돌: DART 내용이 더 구체적이므로 DART 우선
            final_major = dart_major
            final_sub = dart_sub
            final_value_chain = dart_value_chain
            # DART confidence가 HIGH면 그대로, 아니면 MEDIUM
            final_confidence = dart_confidence if dart_confidence == "HIGH" else "MEDIUM"
            classification_method = "DART_OVERRIDE"
            conflict_resolved = True
            logger.info(f"[결정] KRX와 DART 충돌 → DART 결과 우선 (KRX: {krx_major}, DART: {dart_major})")
    elif dart_major:
        # DART만 결과가 있는 경우
        final_major = dart_major
        final_sub = dart_sub
        final_value_chain = dart_value_chain
        final_confidence = dart_confidence
        classification_method = "DART_ONLY"
        logger.info(f"[결정] DART 결과만 존재 → DART 결과 사용")
    elif krx_major:
        # KRX만 결과가 있는 경우
        final_major = krx_major
        final_sub = krx_sub
        final_confidence = krx_confidence if krx_confidence != "LOW" else "MEDIUM"
        classification_method = "KRX_ONLY"
        logger.info(f"[결정] KRX 결과만 존재 → KRX 결과 사용 (DART 내용 없음)")
    else:
        # 둘 다 결과가 없는 경우
        logger.warning(f"[결정] KRX와 DART 모두 결과 없음")
        
        # Step 3: LLM Fallback
        if use_llm_fallback and llm_handler:
            logger.info(f"[Step 3] LLM Fallback 시도...")
            company_detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).first()
            
            if company_detail:
                llm_major, llm_sub, llm_value_chain, llm_confidence, llm_boosting_log = classify_sector_llm(
                    llm_handler=llm_handler,
                    company_detail=company_detail,
                    company_name=company_name
                )
                
                if llm_major:
                    final_major = llm_major
                    final_sub = llm_sub
                    final_value_chain = llm_value_chain
                    final_confidence = llm_confidence
                    classification_method = "LLM_FALLBACK"
                    logger.info(f"[Step 3] LLM 결과: {llm_major}/{llm_sub}")
                else:
                    logger.warning(f"[Step 3] LLM도 결과 없음")
            else:
                logger.warning(f"[Step 3] CompanyDetail 없어서 LLM 실행 불가")
    
    # 최종 결과 반환
    if final_major:
        return {
            'major_sector': final_major,
            'sub_sector': final_sub,
            'value_chain': final_value_chain,
            'confidence': final_confidence,
            'classification_method': classification_method,
            'krx_sector': krx_sector_raw,
            'conflict_resolved': conflict_resolved
        }
    else:
        logger.warning(f"[최종] 모든 방법 실패 → None 반환")
        return None

