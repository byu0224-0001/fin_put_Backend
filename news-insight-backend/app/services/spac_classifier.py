"""
SPAC 자동 분류 서비스

Rule-based 분류:
- 회사명 패턴: "스팩", "SPAC" 포함
- company_type으로 분리 (섹터가 아님)
- 상태 관리: PRE_MERGER, TARGET_ANNOUNCED, POST_MERGER
"""
import logging
import re
from typing import Optional, Dict, Any
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

logger = logging.getLogger(__name__)

# SPAC 관련 키워드
SPAC_KEYWORDS = [
    '스팩', 'SPAC', 'Special Purpose Acquisition Company',
    '특수목적인수회사', '특수목적기업'
]

# 회사명 패턴 (정규식)
SPAC_NAME_PATTERNS = [
    r'스팩$',
    r'SPAC$',
    r'스팩\d+',  # 스팩1호, 스팩2호 등
    r'SPAC\d+',  # SPAC1, SPAC2 등
]

# biz_summary에서 SPAC 관련 문구 패턴
SPAC_PHRASE_PATTERNS = [
    r'특수목적인수회사',
    r'특수목적기업',
    r'SPAC',
    r'스팩',
    r'합병.*대상',
    r'합병.*예정',
]


def is_spac_by_name(stock_name: str) -> bool:
    """
    회사명으로 SPAC 여부 판단
    
    Args:
        stock_name: 회사명
    
    Returns:
        SPAC 여부
    """
    if not stock_name:
        return False
    
    normalized = stock_name.replace(" ", "").replace("　", "")
    
    # 패턴 매칭
    for pattern in SPAC_NAME_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            return True
    
    return False


def is_spac_by_keywords(text: str) -> bool:
    """
    텍스트(키워드, biz_summary 등)로 SPAC 여부 판단
    
    Args:
        text: 분석할 텍스트
    
    Returns:
        SPAC 여부
    """
    if not text:
        return False
    
    text_lower = str(text).lower()
    
    # 키워드 매칭
    for keyword in SPAC_KEYWORDS:
        if keyword.lower() in text_lower:
            return True
    
    # 문구 패턴 매칭
    for pattern in SPAC_PHRASE_PATTERNS:
        if re.search(pattern, text_lower):
            return True
    
    return False


def classify_spac_status(
    biz_summary: Optional[str],
    keywords: Optional[list]
) -> str:
    """
    SPAC 상태 분류
    
    Args:
        biz_summary: 사업 요약
        keywords: 키워드 리스트
    
    Returns:
        상태 코드: 'PRE_MERGER', 'TARGET_ANNOUNCED', 'POST_MERGER'
    """
    text_for_analysis = ""
    
    if biz_summary:
        text_for_analysis += biz_summary.lower() + " "
    
    if keywords:
        text_for_analysis += " ".join([str(k).lower() for k in keywords])
    
    if not text_for_analysis:
        return 'PRE_MERGER'  # 기본값 (합병 전)
    
    # 합병 완료 여부 확인
    if any(term in text_for_analysis for term in ['합병 완료', '합병 완료', '인수 완료', 'merger completed']):
        return 'POST_MERGER'
    
    # 합병 대상 발표 여부 확인
    if any(term in text_for_analysis for term in ['합병 대상', '인수 대상', 'target company', '합병 예정', '인수 예정']):
        return 'TARGET_ANNOUNCED'
    
    return 'PRE_MERGER'  # 기본값


def extract_expected_sector(
    biz_summary: Optional[str],
    keywords: Optional[list]
) -> Optional[str]:
    """
    합병 대상 기업의 예상 섹터 추출 (TARGET_ANNOUNCED 상태일 때)
    
    Args:
        biz_summary: 사업 요약
        keywords: 키워드 리스트
    
    Returns:
        예상 섹터 코드 (예: 'SEC_BATTERY', 'SEC_BIO')
    """
    # 간단한 키워드 매칭 (향후 LLM으로 확장 가능)
    text_for_analysis = ""
    
    if biz_summary:
        text_for_analysis += biz_summary.lower() + " "
    
    if keywords:
        text_for_analysis += " ".join([str(k).lower() for k in keywords])
    
    # 섹터 키워드 매핑 (간단한 버전)
    sector_keywords = {
        'SEC_BATTERY': ['배터리', '2차전지', '이차전지', '리튬', '전지'],
        'SEC_BIO': ['바이오', '제약', '의약품', '신약', '바이오의약'],
        'SEC_SEMI': ['반도체', '반도체', '칩', '웨이퍼'],
        'SEC_IT': ['IT', '소프트웨어', '플랫폼', 'AI', '인공지능'],
    }
    
    for sector, keywords_list in sector_keywords.items():
        if any(kw in text_for_analysis for kw in keywords_list):
            return sector
    
    return None


def classify_spac(
    stock: Stock,
    company_detail: Optional[CompanyDetail] = None
) -> Dict[str, Any]:
    """
    SPAC 분류 수행
    
    Args:
        stock: Stock 객체
        company_detail: CompanyDetail 객체 (선택적)
    
    Returns:
        {
            'is_spac': bool,
            'confidence': float,  # 0.0 ~ 1.0
            'evidence': List[str],  # 분류 근거
            'status': Optional[str],  # PRE_MERGER, TARGET_ANNOUNCED, POST_MERGER
            'expected_sector': Optional[str],  # 합병 대상 기업의 예상 섹터
        }
    """
    evidence = []
    confidence_score = 0.0
    
    # 1. 회사명 패턴 매칭 (강한 신호)
    if is_spac_by_name(stock.stock_name):
        evidence.append(f"회사명 패턴: {stock.stock_name}")
        confidence_score += 0.8  # SPAC은 회사명 패턴이 매우 강한 신호
    
    # 2. biz_summary 분석
    if company_detail and company_detail.biz_summary:
        if is_spac_by_keywords(company_detail.biz_summary):
            evidence.append("biz_summary에 SPAC 관련 키워드 포함")
            confidence_score += 0.15
    
    # 3. keywords 분석
    if company_detail and company_detail.keywords:
        keywords_text = " ".join([str(k) for k in company_detail.keywords])
        if is_spac_by_keywords(keywords_text):
            evidence.append("keywords에 SPAC 관련 키워드 포함")
            confidence_score += 0.05
    
    # 상태 분류
    status = None
    expected_sector = None
    if confidence_score >= 0.5:  # SPAC으로 판단되는 경우
        status = classify_spac_status(
            company_detail.biz_summary if company_detail else None,
            company_detail.keywords if company_detail else None
        )
        evidence.append(f"상태: {status}")
        
        # TARGET_ANNOUNCED 상태일 때 예상 섹터 추출
        if status == 'TARGET_ANNOUNCED':
            expected_sector = extract_expected_sector(
                company_detail.biz_summary if company_detail else None,
                company_detail.keywords if company_detail else None
            )
            if expected_sector:
                evidence.append(f"예상 섹터: {expected_sector}")
    
    return {
        'is_spac': confidence_score >= 0.5,
        'confidence': min(confidence_score, 1.0),
        'evidence': evidence,
        'status': status,
        'expected_sector': expected_sector,
    }

