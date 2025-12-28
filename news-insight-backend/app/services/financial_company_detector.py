"""
금융사 감지 서비스 (일원화)

모든 금융사 감지 로직을 단일 함수로 통합하여 일관성 보장
"""
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def detect_financial_company(
    company_name: Optional[str] = None,
    ticker: Optional[str] = None,
    business_summary: Optional[str] = None,
    keywords: Optional[list] = None
) -> Tuple[bool, bool, float]:
    """
    금융사 감지 (단일 정의)
    
    Args:
        company_name: 회사명
        ticker: 종목코드
        business_summary: 사업개요
        keywords: 키워드 리스트
    
    Returns:
        (is_financial, is_financial_holding, confidence)
        - is_financial: 일반 금융사 여부
        - is_financial_holding: 금융지주 여부
        - confidence: 신뢰도 (0.0 ~ 1.0)
    """
    if not company_name:
        return False, False, 0.0
    
    company_name_lower = company_name.lower()
    
    # 1차: 회사명 패턴 기반 감지 (가장 확실)
    financial_name_patterns = [
        "금융", "은행", "증권", "보험", "카드", "캐피탈", "리츠",
        "bank", "securities", "insurance", "card", "capital", "reit"
    ]
    
    financial_holding_patterns = [
        "금융지주", "kb금융", "신한지주", "하나금융지주", 
        "우리금융지주", "메리츠금융지주", "은행지주", "보험지주"
    ]
    
    # 금융지주 감지
    is_financial_holding = False
    if any(pattern in company_name_lower for pattern in financial_holding_patterns):
        is_financial_holding = True
        logger.debug(f"[{ticker or 'N/A'}] 회사명 기반 금융지주 감지: {company_name}")
        return True, True, 0.95
    
    # KB금융 특별 처리
    if ('kb' in company_name_lower or 'KB' in company_name) and '금융' in company_name_lower:
        is_financial_holding = True
        logger.debug(f"[{ticker or 'N/A'}] KB금융 감지: {company_name}")
        return True, True, 0.95
    
    # 일반 금융사 감지
    if any(pattern in company_name_lower for pattern in financial_name_patterns):
        logger.debug(f"[{ticker or 'N/A'}] 회사명 기반 금융사 감지: {company_name}")
        return True, False, 0.90
    
    # 2차: 키워드 기반 감지
    financial_keywords = [
        "은행", "증권", "보험", "카드", "금융", "자산운용",
        "저축은행", "신용카드", "리스", "리츠", "캐피탈",
        "금융지주", "금융서비스", "투자은행", "증권사", "보험사",
        "bank", "securities", "insurance", "financial"
    ]
    
    financial_industry_keywords = [
        "은행업", "보험업", "증권업", "여신전문", "자본시장법", "보험업법",
        "금융업", "투자매매업", "투자중개업", "ib", "브로커리지"
    ]
    
    text = ""
    if business_summary:
        text += " " + business_summary.lower()
    if company_name:
        text += " " + company_name_lower
    if keywords:
        keywords_text = ' '.join([str(k) for k in keywords]).lower()
        text += " " + keywords_text
    
    has_financial_keyword = any(keyword in text for keyword in financial_keywords)
    has_industry_keyword = any(keyword in text for keyword in financial_industry_keywords)
    
    if has_financial_keyword or has_industry_keyword:
        # "투자"는 단독으로는 금융사 판단하지 않음
        if "투자" in text:
            has_other_financial_keyword = any(
                kw in text for kw in financial_keywords 
                if kw != "투자" and kw not in ["bank", "securities", "insurance", "financial"]
            )
            has_financial_context = any(
                kw in text for kw in ["투자매매업", "투자중개업", "ib", "브로커리지", "보험"]
            )
            
            if not has_other_financial_keyword and not has_financial_context:
                logger.debug(f"[{ticker or 'N/A'}] '투자' 키워드만 발견, 금융사로 판단하지 않음")
                return False, False, 0.0
        
        # 금융업 본질 키워드가 있으면 확정
        if has_industry_keyword:
            logger.debug(f"[{ticker or 'N/A'}] 금융업 본질 키워드 기반 금융사 감지")
            return True, False, 0.85
        
        # 일반 금융 키워드
        logger.debug(f"[{ticker or 'N/A'}] 키워드 기반 금융사 감지")
        return True, False, 0.70
    
    return False, False, 0.0

