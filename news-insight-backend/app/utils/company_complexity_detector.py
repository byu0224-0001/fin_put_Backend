"""
복합기업 감지 모듈

BGE-M3 조건부 사용을 위한 복합기업 감지 로직
"""
import logging
import re
from typing import Optional
from app.models.company_detail import CompanyDetail

logger = logging.getLogger(__name__)


def is_complex_company(
    company_detail: CompanyDetail,
    company_name: Optional[str] = None
) -> bool:
    """
    복합기업 여부 판단
    
    복합기업 특징:
    1. 지주사 (홀딩스, 지주)
    2. 종합상사 (다양한 사업 영역)
    3. 다양한 키워드/제품 (10개 이상)
    4. 여러 섹터 키워드 포함
    5. 문장 수 (복잡한 사업 설명)
    
    주의: biz_summary 길이는 체크하지 않음 (500자로 잘려서 사용되므로 의미 없음)
    
    Args:
        company_detail: CompanyDetail 객체
        company_name: 회사명 (선택)
    
    Returns:
        True: 복합기업, False: 단순 기업
    """
    if not company_detail:
        return False
    
    complexity_score = 0
    
    # 1. 회사명 기반 판단
    if company_name:
        name_lower = company_name.lower()
        # 지주사 키워드
        if any(kw in name_lower for kw in ['지주', '홀딩스', '홀딩', 'holdings']):
            complexity_score += 3
            logger.debug(f"지주사 키워드 감지: {company_name}")
        
        # 종합상사 키워드
        if any(kw in name_lower for kw in ['종합', '물산', '상사', '그룹']):
            complexity_score += 2
            logger.debug(f"종합상사 키워드 감지: {company_name}")
    
    # 2. 키워드 다양성
    if company_detail.keywords:
        unique_keywords = len(set([str(k).lower() for k in company_detail.keywords if k]))
        if unique_keywords > 15:
            complexity_score += 2
        elif unique_keywords > 10:
            complexity_score += 1
    
    # 3. 제품 다양성
    if company_detail.products:
        unique_products = len(set([str(p).lower() for p in company_detail.products if p]))
        if unique_products > 10:
            complexity_score += 2
        elif unique_products > 5:
            complexity_score += 1
    
    # 4. biz_summary에 여러 섹터 키워드 포함 여부
    if company_detail.biz_summary:
        summary_lower = company_detail.biz_summary.lower()
        # 다양한 섹터 키워드 카운트
        sector_keywords_found = 0
        sector_keywords = [
            '반도체', '자동차', '화학', '건설', '은행', '증권', '보험',
            '게임', '엔터테인먼트', '화장품', '바이오', '의료기기',
            '조선', '방산', '기계', '철강', '유통', '통신'
        ]
        for keyword in sector_keywords:
            if keyword in summary_lower:
                sector_keywords_found += 1
        
        if sector_keywords_found > 3:
            complexity_score += 2
        elif sector_keywords_found > 2:
            complexity_score += 1
    
    # 5. 문장 수 (복잡한 사업 설명 지표)
    if company_detail.biz_summary:
        # 문장 수 계산 (간단한 방법)
        # 한국어/영문 문장 끝 패턴
        sentences = re.split(r'[.!?。]\s+', company_detail.biz_summary)
        # 의미 있는 문장만 카운트 (너무 짧은 문장 제외)
        meaningful_sentences = [s for s in sentences if s and len(s.strip()) > 10]
        num_sentences = len(meaningful_sentences)
        
        if num_sentences > 10:
            complexity_score += 1
            logger.debug(f"문장 수 많음: {num_sentences}개")
    
    # 복합기업 판단 기준: 점수 3 이상
    is_complex = complexity_score >= 3
    
    logger.debug(
        f"복합기업 감지: {company_name} "
        f"(complexity_score={complexity_score}, is_complex={is_complex})"
    )
    
    return is_complex

