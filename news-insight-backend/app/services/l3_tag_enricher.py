"""
L3 태그 Enrichment 서비스

LLM 출력에 의존하지 않고, Backend에서 텍스트 매칭을 통해 L3 태그를 자동으로 부여합니다.
이미지 기반 SECTOR_L3_TAG_CANDIDATES를 활용하여 정확한 태그 매칭을 수행합니다.
"""
import logging
from typing import List, Dict, Optional, Any
from app.models.sector_reference import (
    SECTOR_L3_TAG_CANDIDATES,
    get_l3_tag_candidates,
    get_l3_keywords_map
)

logger = logging.getLogger(__name__)


def enrich_sector_l3_tags(
    sector_l1: str,
    text_blobs: List[str],
    granular_tags_from_llm: Optional[List[str]] = None,
    min_match_count: int = 1
) -> List[str]:
    """
    L3 태그를 Backend에서 자동으로 Enrichment
    
    LLM 출력에 의존하지 않고, 텍스트 매칭을 통해 정확한 L3 태그를 부여합니다.
    
    Args:
        sector_l1: L1 섹터 코드 (예: 'SEC_SEMI')
        text_blobs: 검색할 텍스트 리스트 (company_detail.biz_summary, key_drivers.description 등)
        granular_tags_from_llm: LLM이 생성한 granular_tags (참고용, 필수 아님)
        min_match_count: 최소 매칭 횟수 (기본: 1)
    
    Returns:
        매칭된 L3 태그 리스트 (예: ['반도체부품소재', '반도체장비'])
    """
    if not sector_l1 or not text_blobs:
        return []
    
    # L3 태그 후보 가져오기
    tag_candidates = get_l3_tag_candidates(sector_l1)
    if not tag_candidates:
        logger.debug(f"[L3 Enrichment] {sector_l1}에 대한 L3 태그 후보가 없습니다.")
        return []
    
    # 키워드 매핑 가져오기
    keywords_map = get_l3_keywords_map(sector_l1)
    if not keywords_map:
        logger.debug(f"[L3 Enrichment] {sector_l1}에 대한 L3 키워드 매핑이 없습니다.")
        return []
    
    # 모든 텍스트를 하나로 합치기 (대소문자 무시)
    combined_text = " ".join([str(blob).lower() for blob in text_blobs if blob])
    
    # LLM granular_tags도 텍스트에 추가 (참고용)
    if granular_tags_from_llm:
        combined_text += " " + " ".join([str(tag).lower() for tag in granular_tags_from_llm])
    
    # 각 태그별로 키워드 매칭 확인
    matched_tags = []
    tag_match_counts = {}
    
    for tag in tag_candidates:
        if tag not in keywords_map:
            continue
        
        keywords = keywords_map[tag]
        match_count = 0
        
        # 각 키워드가 텍스트에 포함되어 있는지 확인
        for keyword in keywords:
            keyword_lower = str(keyword).lower()
            if keyword_lower in combined_text:
                match_count += 1
        
        # 최소 매칭 횟수 이상이면 태그 추가
        if match_count >= min_match_count:
            matched_tags.append(tag)
            tag_match_counts[tag] = match_count
    
    # 매칭 횟수 순으로 정렬 (높은 순)
    matched_tags.sort(key=lambda t: tag_match_counts.get(t, 0), reverse=True)
    
    # 최대 5개로 제한
    matched_tags = matched_tags[:5]
    
    if matched_tags:
        logger.info(f"[L3 Enrichment] {sector_l1}: {len(matched_tags)}개 태그 매칭됨: {matched_tags}")
    else:
        logger.debug(f"[L3 Enrichment] {sector_l1}: 매칭된 태그 없음")
    
    return matched_tags


def enrich_l3_tags_from_company_detail(
    sector_l1: str,
    company_detail: Any,
    causal_structure: Optional[Dict] = None
) -> List[str]:
    """
    CompanyDetail과 CausalStructure에서 L3 태그를 자동으로 Enrichment
    
    Args:
        sector_l1: L1 섹터 코드
        company_detail: CompanyDetail 객체
        causal_structure: Gemini가 생성한 인과 구조 (granular_tags 포함 가능)
    
    Returns:
        매칭된 L3 태그 리스트
    """
    text_blobs = []
    
    # CompanyDetail에서 텍스트 수집
    if company_detail:
        if hasattr(company_detail, 'biz_summary') and company_detail.biz_summary:
            text_blobs.append(company_detail.biz_summary)
        
        if hasattr(company_detail, 'products') and company_detail.products:
            products_text = " ".join([str(p) for p in company_detail.products[:10]])
            if products_text:
                text_blobs.append(products_text)
        
        if hasattr(company_detail, 'keywords') and company_detail.keywords:
            keywords_text = " ".join([str(k) for k in company_detail.keywords[:10]])
            if keywords_text:
                text_blobs.append(keywords_text)
        
        if hasattr(company_detail, 'raw_materials') and company_detail.raw_materials:
            materials_text = " ".join([str(rm) for rm in company_detail.raw_materials[:10]])
            if materials_text:
                text_blobs.append(materials_text)
    
    # CausalStructure에서 텍스트 수집
    granular_tags_from_llm = None
    if causal_structure:
        # LLM이 생성한 granular_tags (참고용)
        granular_tags_from_llm = causal_structure.get('granular_tags', [])
        
        # key_drivers의 description도 텍스트에 추가
        key_drivers = causal_structure.get('key_drivers', [])
        for driver in key_drivers:
            if isinstance(driver, dict):
                description = driver.get('description', '')
                var = driver.get('var', '')
                if description:
                    text_blobs.append(description)
                if var:
                    text_blobs.append(var)
    
    # L3 태그 Enrichment 수행
    l3_tags = enrich_sector_l3_tags(
        sector_l1=sector_l1,
        text_blobs=text_blobs,
        granular_tags_from_llm=granular_tags_from_llm,
        min_match_count=1
    )
    
    return l3_tags

