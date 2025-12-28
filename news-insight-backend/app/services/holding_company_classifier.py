"""
지주사 자동 분류 서비스

분석 결과를 바탕으로 지주사를 자동으로 분류합니다.
- 회사명 패턴 매칭
- biz_summary 키워드 분석
- Multi-sector 지원 (지주사이면서 다른 섹터도 포함 가능)
"""
import logging
import re
from typing import Optional, Dict, Any, List
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

logger = logging.getLogger(__name__)

# 지주사 관련 키워드 (분석 결과 기반)
HOLDING_KEYWORDS = [
    '지주', '홀딩', 'holding', '지주사', '홀딩스', '지주회사',
    '계열사', '자회사', '관리', '경영', '투자', '자산관리',
    '경영지원', '사업관리', '기업지배구조', '그룹', '그룹사'
]

# 회사명 패턴 (정규식)
HOLDING_NAME_PATTERNS = [
    r'홀딩스?$',
    r'지주$',
    r'홀딩$',
    r'지주사$',
    r'지주회사$',
]

# biz_summary에서 지주사 관련 문구 패턴
HOLDING_PHRASE_PATTERNS = [
    r'지주회사로서',
    r'지주회사로',
    r'지주사로서',
    r'지주사로',
    r'계열사.*관리',
    r'자회사.*관리',
    r'계열사.*경영',
    r'자회사.*경영',
    r'계열사.*운영',
    r'자회사.*운영',
]


def is_holding_company_by_name(stock_name: str) -> bool:
    """
    회사명으로 지주사 여부 판단
    
    Args:
        stock_name: 회사명
    
    Returns:
        지주사 여부
    """
    if not stock_name:
        return False
    
    normalized = stock_name.replace(" ", "").replace("　", "")
    
    # 패턴 매칭
    for pattern in HOLDING_NAME_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            return True
    
    return False


def is_holding_company_by_keywords(text: str) -> bool:
    """
    텍스트(키워드, biz_summary 등)로 지주사 여부 판단
    
    Args:
        text: 분석할 텍스트
    
    Returns:
        지주사 여부
    """
    if not text:
        return False
    
    text_lower = str(text).lower()
    
    # 키워드 매칭
    for keyword in HOLDING_KEYWORDS:
        if keyword in text_lower:
            return True
    
    # 문구 패턴 매칭
    for pattern in HOLDING_PHRASE_PATTERNS:
        if re.search(pattern, text_lower):
            return True
    
    return False


def classify_holding_company(
    stock: Stock,
    company_detail: Optional[CompanyDetail] = None
) -> Dict[str, Any]:
    """
    지주사 분류 수행
    
    Args:
        stock: Stock 객체
        company_detail: CompanyDetail 객체 (선택적)
    
    Returns:
        {
            'is_holding': bool,
            'confidence': float,  # 0.0 ~ 1.0
            'evidence': List[str],  # 분류 근거
            'l2_sector': Optional[str],  # GENERAL_HOLDING, FINANCIAL_HOLDING, INDUSTRIAL_HOLDING
        }
    """
    evidence = []
    confidence_score = 0.0
    
    # 1. 회사명 패턴 매칭 (강한 신호)
    if is_holding_company_by_name(stock.stock_name):
        evidence.append(f"회사명 패턴: {stock.stock_name}")
        confidence_score += 0.6
    
    # 2. biz_summary 분석
    if company_detail and company_detail.biz_summary:
        if is_holding_company_by_keywords(company_detail.biz_summary):
            evidence.append("biz_summary에 지주사 관련 키워드 포함")
            confidence_score += 0.3
    
    # 3. keywords 분석
    if company_detail and company_detail.keywords:
        keywords_text = " ".join([str(k) for k in company_detail.keywords])
        if is_holding_company_by_keywords(keywords_text):
            evidence.append("keywords에 지주사 관련 키워드 포함")
            confidence_score += 0.1
    
    # L2 분류 (금융 지주사 vs 일반 지주사)
    l2_sector = None
    if confidence_score >= 0.5:  # 지주사로 판단되는 경우
        # 금융 지주사 판단
        if company_detail and company_detail.biz_summary:
            biz_lower = company_detail.biz_summary.lower()
            if any(kw in biz_lower for kw in ['금융지주', '금융그룹', '금융지주사', '은행', '증권', '보험']):
                l2_sector = 'FINANCIAL_HOLDING'
                evidence.append("금융 지주사로 분류")
            elif any(kw in biz_lower for kw in ['제조', '산업', '산업지주', '제조지주']):
                l2_sector = 'INDUSTRIAL_HOLDING'
                evidence.append("산업 지주사로 분류")
            else:
                l2_sector = 'GENERAL_HOLDING'
                evidence.append("일반 지주사로 분류")
        else:
            l2_sector = 'GENERAL_HOLDING'
    
    return {
        'is_holding': confidence_score >= 0.5,
        'confidence': min(confidence_score, 1.0),
        'evidence': evidence,
        'l2_sector': l2_sector,
    }


def classify_holding_with_multi_sector(
    stock: Stock,
    company_detail: Optional[CompanyDetail] = None,
    existing_sectors: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    지주사 분류 (Multi-sector 지원)
    
    지주사이면서 동시에 다른 섹터도 포함할 수 있음
    
    Args:
        stock: Stock 객체
        company_detail: CompanyDetail 객체
        existing_sectors: 기존에 분류된 섹터 리스트
    
    Returns:
        {
            'is_holding': bool,
            'confidence': float,
            'evidence': List[str],
            'l2_sector': Optional[str],
            'multi_sector': bool,  # 다른 섹터와 함께 존재하는지
        }
    """
    holding_result = classify_holding_company(stock, company_detail)
    
    # Multi-sector 여부 확인
    multi_sector = False
    if existing_sectors:
        other_sectors = [s for s in existing_sectors if s != 'SEC_HOLDING']
        if other_sectors:
            multi_sector = True
            holding_result['evidence'].append(f"다른 섹터와 함께 존재: {', '.join(other_sectors)}")
    
    holding_result['multi_sector'] = multi_sector
    
    return holding_result

