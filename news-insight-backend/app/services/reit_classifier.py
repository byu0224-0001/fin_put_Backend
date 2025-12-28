"""
리츠(REITs) 자동 분류 서비스

Rule-based 분류:
- 회사명 패턴: "리츠", "투자회사", "PFV" 포함
- biz_summary 키워드 분석
- L2 분류: 상업용, 오피스, 인프라, 주거용
"""
import logging
import re
from typing import Optional, Dict, Any, List
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

logger = logging.getLogger(__name__)

# 리츠 관련 키워드
REIT_KEYWORDS = [
    '리츠', 'REIT', 'REITs', '부동산투자회사', '부동산투자', '부동산신탁',
    '투자회사', 'PFV', '부동산', '임대', '부동산임대', '부동산투자'
]

# 회사명 패턴 (정규식)
REIT_NAME_PATTERNS = [
    r'리츠$',
    r'REIT$',
    r'REITs$',
    r'투자회사$',
    r'PFV$',
    r'부동산투자회사$',
]

# biz_summary에서 리츠 관련 문구 패턴
REIT_PHRASE_PATTERNS = [
    r'부동산투자회사',
    r'부동산.*투자',
    r'부동산.*임대',
    r'리츠.*운영',
    r'REIT.*운영',
]

# L2 분류 키워드
L2_KEYWORDS = {
    'COMMERCIAL_REIT': ['상업용', '상업시설', '쇼핑몰', '리테일', '상업', '상업용 부동산'],
    'OFFICE_REIT': ['오피스', '사무용', '사무실', '오피스빌딩', '사무', '사무용 부동산'],
    'INFRA_REIT': ['인프라', '인프라스트럭처', 'SOC', '사회간접자본', '인프라 부동산'],
    'RESIDENTIAL_REIT': ['주거용', '주택', '아파트', '주거시설', '주거', '주거용 부동산'],
}


def is_reit_by_name(stock_name: str) -> bool:
    """
    회사명으로 리츠 여부 판단
    
    Args:
        stock_name: 회사명
    
    Returns:
        리츠 여부
    """
    if not stock_name:
        return False
    
    normalized = stock_name.replace(" ", "").replace("　", "")
    
    # 패턴 매칭
    for pattern in REIT_NAME_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            return True
    
    return False


def is_reit_by_keywords(text: str) -> bool:
    """
    텍스트(키워드, biz_summary 등)로 리츠 여부 판단
    
    Args:
        text: 분석할 텍스트
    
    Returns:
        리츠 여부
    """
    if not text:
        return False
    
    text_lower = str(text).lower()
    
    # 키워드 매칭
    for keyword in REIT_KEYWORDS:
        if keyword.lower() in text_lower:
            return True
    
    # 문구 패턴 매칭
    for pattern in REIT_PHRASE_PATTERNS:
        if re.search(pattern, text_lower):
            return True
    
    return False


def classify_reit_l2(biz_summary: Optional[str], keywords: Optional[List[str]]) -> Optional[str]:
    """
    L2 분류 (상업용, 오피스, 인프라, 주거용)
    
    Args:
        biz_summary: 사업 요약
        keywords: 키워드 리스트
    
    Returns:
        L2 섹터 코드 (COMMERCIAL_REIT, OFFICE_REIT, INFRA_REIT, RESIDENTIAL_REIT)
    """
    text_for_analysis = ""
    
    if biz_summary:
        text_for_analysis += biz_summary.lower() + " "
    
    if keywords:
        text_for_analysis += " ".join([str(k).lower() for k in keywords])
    
    if not text_for_analysis:
        return 'COMMERCIAL_REIT'  # 기본값
    
    # 각 L2별 점수 계산
    l2_scores = {}
    for l2_code, l2_keywords in L2_KEYWORDS.items():
        score = 0
        for keyword in l2_keywords:
            if keyword.lower() in text_for_analysis:
                score += 1
        l2_scores[l2_code] = score
    
    # 가장 높은 점수의 L2 반환
    if max(l2_scores.values()) > 0:
        return max(l2_scores.items(), key=lambda x: x[1])[0]
    
    return 'COMMERCIAL_REIT'  # 기본값


def classify_reit(
    stock: Stock,
    company_detail: Optional[CompanyDetail] = None
) -> Dict[str, Any]:
    """
    리츠 분류 수행
    
    Args:
        stock: Stock 객체
        company_detail: CompanyDetail 객체 (선택적)
    
    Returns:
        {
            'is_reit': bool,
            'confidence': float,  # 0.0 ~ 1.0
            'evidence': List[str],  # 분류 근거
            'l2_sector': Optional[str],  # COMMERCIAL_REIT, OFFICE_REIT, INFRA_REIT, RESIDENTIAL_REIT
        }
    """
    evidence = []
    confidence_score = 0.0
    
    # 1. 회사명 패턴 매칭 (강한 신호)
    if is_reit_by_name(stock.stock_name):
        evidence.append(f"회사명 패턴: {stock.stock_name}")
        confidence_score += 0.7  # 리츠는 회사명 패턴이 매우 강한 신호
    
    # 2. biz_summary 분석
    if company_detail and company_detail.biz_summary:
        if is_reit_by_keywords(company_detail.biz_summary):
            evidence.append("biz_summary에 리츠 관련 키워드 포함")
            confidence_score += 0.2
    
    # 3. keywords 분석
    if company_detail and company_detail.keywords:
        keywords_text = " ".join([str(k) for k in company_detail.keywords])
        if is_reit_by_keywords(keywords_text):
            evidence.append("keywords에 리츠 관련 키워드 포함")
            confidence_score += 0.1
    
    # L2 분류
    l2_sector = None
    if confidence_score >= 0.5:  # 리츠로 판단되는 경우
        l2_sector = classify_reit_l2(
            company_detail.biz_summary if company_detail else None,
            company_detail.keywords if company_detail else None
        )
        evidence.append(f"L2 분류: {l2_sector}")
    
    return {
        'is_reit': confidence_score >= 0.5,
        'confidence': min(confidence_score, 1.0),
        'evidence': evidence,
        'l2_sector': l2_sector,
    }


def classify_reit_with_multi_sector(
    stock: Stock,
    company_detail: Optional[CompanyDetail] = None,
    existing_sectors: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    리츠 분류 (Multi-sector 지원)
    
    리츠이면서 동시에 다른 섹터도 포함할 수 있음 (예: 건설 + 리츠)
    
    Args:
        stock: Stock 객체
        company_detail: CompanyDetail 객체
        existing_sectors: 기존에 분류된 섹터 리스트
    
    Returns:
        {
            'is_reit': bool,
            'confidence': float,
            'evidence': List[str],
            'l2_sector': Optional[str],
            'multi_sector': bool,  # 다른 섹터와 함께 존재하는지
        }
    """
    reit_result = classify_reit(stock, company_detail)
    
    # Multi-sector 여부 확인
    multi_sector = False
    if existing_sectors:
        other_sectors = [s for s in existing_sectors if s != 'SEC_REIT']
        if other_sectors:
            multi_sector = True
            reit_result['evidence'].append(f"다른 섹터와 함께 존재: {', '.join(other_sectors)}")
    
    reit_result['multi_sector'] = multi_sector
    
    return reit_result

