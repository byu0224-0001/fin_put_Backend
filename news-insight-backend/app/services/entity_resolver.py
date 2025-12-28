"""
Entity Resolution Service

공급업체 이름을 정규화하고 stocks 테이블 또는 company_aliases 테이블과 매칭
"""
import re
import logging
from typing import Optional, Tuple
from difflib import SequenceMatcher
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.stock import Stock
from app.models.company_alias import CompanyAlias

logger = logging.getLogger(__name__)


def normalize_company_name(raw_name: str) -> str:
    """
    기업명 정규화
    
    예:
        "㈜삼성전기" -> "삼성전기"
        "삼성전기(주)" -> "삼성전기"
        "SAMSUNG ELECTRO-MECHANICS" -> "SAMSUNGELECTRO-MECHANICS"
    """
    if not raw_name:
        return ""
    
    # 1. 괄호 및 주식회사 제거
    clean = re.sub(r'\(주\)|주식회사|\(cnt\)|㈜|\(\)|\(', '', raw_name)
    clean = re.sub(r'\)', '', clean)
    
    # 2. 공백 제거
    clean = re.sub(r'\s+', '', clean)
    
    # 3. 대소문자 통일 (영문인 경우)
    # 한글은 그대로, 영문은 대문자로
    if clean.isascii():
        clean = clean.upper()
    
    return clean.strip()


def resolve_entity_comprehensive(
    db: Session,
    raw_name: str,
    use_llm: bool = False
) -> Tuple[str, str, float]:
    """
    공급업체 이름을 정규화하고 매칭 (5단계 전략)
    
    Args:
        db: DB 세션
        raw_name: 원본 공급업체 이름
        use_llm: LLM Fallback 사용 여부 (기본 False)
    
    Returns:
        (resolved_id, company_type, confidence)
        - resolved_id: ticker 또는 정규화된 기업명
        - company_type: "LISTED", "UNLISTED"
        - confidence: 0.0 ~ 1.0
    """
    if not raw_name or raw_name.strip() == "":
        return "", "UNLISTED", 0.0
    
    normalized = normalize_company_name(raw_name)
    
    # 1순위: 정확 매칭 (stocks 테이블 - stock_name)
    stock = db.query(Stock).filter(
        func.replace(func.replace(Stock.stock_name, ' ', ''), '㈜', '') == normalized
    ).first()
    if stock:
        logger.debug(f"[Entity Resolution] 정확 매칭: {raw_name} -> {stock.ticker} ({stock.stock_name})")
        return stock.ticker, "LISTED", 1.0
    
    # 1-2순위: synonyms 필드 확인
    stock = db.query(Stock).filter(
        Stock.synonyms.contains([normalized])
    ).first()
    if stock:
        logger.debug(f"[Entity Resolution] Synonyms 매칭: {raw_name} -> {stock.ticker} ({stock.stock_name})")
        return stock.ticker, "LISTED", 0.95
    
    # 2순위: Alias 매칭 (company_aliases 테이블)
    alias = db.query(CompanyAlias).filter(
        CompanyAlias.alias_name == normalized
    ).first()
    if alias:
        if alias.ticker:
            logger.debug(f"[Entity Resolution] Alias 매칭: {raw_name} -> {alias.ticker} ({alias.official_name})")
            return alias.ticker, "LISTED", 0.9
        else:
            logger.debug(f"[Entity Resolution] Alias 매칭 (비상장): {raw_name} -> {alias.official_name}")
            return alias.official_name, "UNLISTED", 0.85
    
    # 3순위: Fuzzy 매칭 (threshold 0.75)
    best_match = None
    best_score = 0.0
    all_stocks = db.query(Stock).all()
    
    for stock in all_stocks:
        stock_name_clean = normalize_company_name(stock.stock_name)
        score = SequenceMatcher(None, normalized, stock_name_clean).ratio()
        if score > best_score:
            best_score = score
            best_match = stock
    
    if best_match and best_score >= 0.75:
        logger.debug(f"[Entity Resolution] Fuzzy 매칭: {raw_name} -> {best_match.ticker} ({best_match.stock_name}, score={best_score:.2f})")
        return best_match.ticker, "LISTED", best_score
    
    # 4순위: LLM Fallback (선택적)
    if use_llm:
        llm_ticker = llm_resolve_company_name(db, raw_name)
        if llm_ticker:
            logger.debug(f"[Entity Resolution] LLM 매칭: {raw_name} -> {llm_ticker}")
            return llm_ticker, "LISTED", 0.7
    
    # 5순위: 비상장사 처리
    logger.debug(f"[Entity Resolution] 비상장사 처리: {raw_name} -> {normalized}")
    return normalized, "UNLISTED", 0.5


def llm_resolve_company_name(db: Session, raw_name: str) -> Optional[str]:
    """
    LLM을 사용한 기업명 해결 (최후의 수단)
    
    주의: 비용이 높으므로 선택적으로만 사용
    """
    # TODO: LLM 구현 (필요 시)
    # 현재는 구현하지 않음 (비용 절감)
    return None

