"""
Driver Tags 자동 부여 서비스

L2 정보와 드라이버 코드를 기반으로 driver_tags를 자동으로 부여합니다.
LLM이 생성하지 않고, 100% Backend Rule로 처리합니다.

우선순위:
1. L2 기반 룰 (최우선) - sector_l2 정보로 태그 부여
2. 텍스트 매칭 (후순위) - company_detail 텍스트로 태그 매칭
3. 최대 3개 제한 - 우선순위가 높은 태그만 선택
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from app.models.sector_reference import (
    SECTOR_L2_DEFINITIONS,
    DRIVER_TAG_ALLOWLIST,
    DRIVER_TAG_PRIORITY,
    DRIVER_TAG_KEYWORDS,
    CONFLICTING_TAG_PAIRS  # ⭐ 상반 태그 충돌 해결용
)

logger = logging.getLogger(__name__)


def enrich_driver_tags(
    driver_code: str,
    sector_l1: str,
    sector_l2: Optional[str],
    sector_l2_confidence: Optional[float] = None,
    company_detail: Optional[Any] = None
) -> List[Dict[str, Any]]:
    """
    드라이버에 태그를 자동으로 부여 (우선순위 처리 포함)
    
    Args:
        driver_code: 드라이버 코드 (예: 'EXCHANGE_RATE_USD_KRW')
        sector_l1: L1 섹터 코드 (예: 'SEC_SEMI')
        sector_l2: L2 섹터 코드 (예: 'DISTRIBUTION')
        sector_l2_confidence: L2 분류 신뢰도 (0.0~1.0, 선택적)
        company_detail: CompanyDetail 객체 (선택적)
    
    Returns:
        드라이버 태그 리스트 (최대 3개)
        형식: [
            {
                "tag": "IMPORT_DEPENDENT",
                "confidence": 0.85,
                "derived_from": "sector_l2",
                "as_of": "2025-12-12"
            }
        ]
    """
    # 1. Allowlist에서 후보 가져오기
    allowed_tags = DRIVER_TAG_ALLOWLIST.get(driver_code, [])
    if not allowed_tags:
        logger.debug(f"[Driver Tags] {driver_code}에 대한 Allowlist 없음")
        return []
    
    matched_tags = []
    tag_metadata = {}  # 태그별 메타데이터 저장
    
    # 2. L2 기반 태그 부여 (최우선 - Priority 100)
    if sector_l2:
        l2_tags = _get_driver_tags_by_l2(
            driver_code=driver_code,
            sector_l1=sector_l1,
            sector_l2=sector_l2,
            sector_l2_confidence=sector_l2_confidence
        )
        for tag_info in l2_tags:
            tag = tag_info['tag']
            if tag in allowed_tags and tag not in [t['tag'] for t in matched_tags]:
                matched_tags.append(tag_info)
                tag_metadata[tag] = tag_info
    
    # 3. 비즈니스 모델 태그 부여 (L2와 무관하게 텍스트에서 추출)
    if len(matched_tags) < 3 and company_detail:
        business_model_tags = _get_business_model_tags_by_text(
            company_detail=company_detail,
            existing_tags=[t['tag'] for t in matched_tags]
        )
        for tag_info in business_model_tags:
            if len(matched_tags) < 3:
                matched_tags.append(tag_info)
                tag_metadata[tag_info['tag']] = tag_info
    
    # 4. 텍스트 매칭으로 추가 태그 부여 (후순위)
    if len(matched_tags) < 3 and company_detail:
        text_tags = _get_driver_tags_by_text(
            driver_code=driver_code,
            company_detail=company_detail,
            allowed_tags=allowed_tags,
            existing_tags=[t['tag'] for t in matched_tags]
        )
        for tag_info in text_tags:
            if len(matched_tags) < 3:
                matched_tags.append(tag_info)
                tag_metadata[tag_info['tag']] = tag_info
    
    # 5. 상반 태그 충돌 해결 (IMPORT_DEPENDENT vs EXPORT_DRIVEN 등)
    matched_tags = _resolve_tag_conflicts(matched_tags)
    
    # 6. 우선순위 정렬 (높은 우선순위 먼저)
    matched_tags.sort(
        key=lambda t: DRIVER_TAG_PRIORITY.get(t['tag'], 0),
        reverse=True
    )
    
    # 7. 최대 3개 제한
    result = matched_tags[:3]
    
    if result:
        logger.debug(f"[Driver Tags] {driver_code}: {len(result)}개 태그 부여 - {[t['tag'] for t in result]}")
    
    return result


def _get_driver_tags_by_l2(
    driver_code: str,
    sector_l1: str,
    sector_l2: str,
    sector_l2_confidence: Optional[float] = None
) -> List[Dict[str, Any]]:
    """
    L2 정보로 드라이버 태그 부여 (최우선)
    
    Returns:
        태그 정보 리스트 (confidence, derived_from, as_of 포함)
    """
    tags = []
    base_confidence = sector_l2_confidence if sector_l2_confidence is not None else 0.8
    
    # Rule 1: 환율 드라이버 + DISTRIBUTION L2
    if driver_code == 'EXCHANGE_RATE_USD_KRW':
        if sector_l2 == 'DISTRIBUTION':
            tags.append({
                'tag': 'IMPORT_DEPENDENT',
                'confidence': base_confidence,
                'derived_from': 'sector_l2',
                'as_of': datetime.now().strftime('%Y-%m-%d')
            })
            tags.append({
                'tag': 'COST_FACTOR',
                'confidence': base_confidence * 0.9,  # L2 confidence 기반
                'derived_from': 'sector_l2',
                'as_of': datetime.now().strftime('%Y-%m-%d')
            })
        elif sector_l2 == 'MANUFACTURING':
            tags.append({
                'tag': 'EXPORT_DRIVEN',
                'confidence': base_confidence,
                'derived_from': 'sector_l2',
                'as_of': datetime.now().strftime('%Y-%m-%d')
            })
            tags.append({
                'tag': 'REVENUE_FACTOR',
                'confidence': base_confidence * 0.9,
                'derived_from': 'sector_l2',
                'as_of': datetime.now().strftime('%Y-%m-%d')
            })
    
    # Rule 2: HBM_DEMAND + AI 관련
    if driver_code == 'HBM_DEMAND':
        tags.append({
            'tag': 'AI_SERVER',
            'confidence': 0.85,
            'derived_from': 'driver_characteristic',
            'as_of': datetime.now().strftime('%Y-%m-%d')
        })
        tags.append({
            'tag': 'DATA_CENTER',
            'confidence': 0.80,
            'derived_from': 'driver_characteristic',
            'as_of': datetime.now().strftime('%Y-%m-%d')
        })
    
    # Rule 3: DRAM_ASP / NAND_ASP + MEMORY L2
    if driver_code in ['DRAM_ASP', 'NAND_ASP']:
        if sector_l2 == 'MEMORY':
            tags.append({
                'tag': 'MEMORY_MARKET',
                'confidence': base_confidence,
                'derived_from': 'sector_l2',
                'as_of': datetime.now().strftime('%Y-%m-%d')
            })
    
    # Rule 4: E_COMMERCE_TRANS_VOL + PG L2
    if driver_code == 'E_COMMERCE_TRANS_VOL':
        if sector_l2 == 'PG':
            tags.append({
                'tag': 'ONLINE_RETAIL',
                'confidence': base_confidence,
                'derived_from': 'sector_l2',
                'as_of': datetime.now().strftime('%Y-%m-%d')
            })
            tags.append({
                'tag': 'DIGITAL_PAYMENT',
                'confidence': base_confidence * 0.9,
                'derived_from': 'sector_l2',
                'as_of': datetime.now().strftime('%Y-%m-%d')
            })
    
    # Rule 5: IT_HARDWARE_DEMAND + DISTRIBUTION L2
    if driver_code == 'IT_HARDWARE_DEMAND':
        if sector_l2 == 'DISTRIBUTION' or sector_l2 == 'HARDWARE_DISTRIBUTION':
            tags.append({
                'tag': 'DISTRIBUTION',
                'confidence': base_confidence,
                'derived_from': 'sector_l2',
                'as_of': datetime.now().strftime('%Y-%m-%d')
            })
    
    return tags


def _get_business_model_tags_by_text(
    company_detail: Any,
    existing_tags: List[str]
) -> List[Dict[str, Any]]:
    """
    비즈니스 모델 특성 태그 추출 (L2와 무관하게 텍스트에서)
    
    Returns:
        비즈니스 모델 태그 리스트
    """
    tags = []
    
    if not company_detail:
        return tags
    
    # 텍스트 수집
    text_blobs = []
    if hasattr(company_detail, 'biz_summary') and company_detail.biz_summary:
        text_blobs.append(company_detail.biz_summary.lower())
    if hasattr(company_detail, 'keywords') and company_detail.keywords:
        keywords_text = " ".join([str(k).lower() for k in company_detail.keywords[:10]])
        text_blobs.append(keywords_text)
    
    combined_text = " ".join(text_blobs)
    
    # 비즈니스 모델 특성 키워드 매핑
    business_model_keywords = {
        'RECURRING_REVENUE': ['구독', 'subscription', '정기결제', '멤버십', '월정액', '구독료', '정기수익'],
        'PLATFORM_BIZ': ['플랫폼', 'platform', '마켓플레이스', '중개', '거래소', '매칭'],
        'LOW_MARGIN': ['OEM', 'ODM', '위탁생산', '하청', '대행', '원가절감'],
        'HIGH_MARGIN': ['브랜드', '자체브랜드', '프리미엄', '고부가가치', '차별화'],
    }
    
    for tag, keywords in business_model_keywords.items():
        if tag in existing_tags:
            continue
        
        if len(tags) >= 2:  # 비즈니스 모델 태그는 최대 2개
            break
        
        for keyword in keywords:
            if keyword.lower() in combined_text:
                tags.append({
                    'tag': tag,
                    'confidence': 0.7,  # 비즈니스 모델 특성은 중간 신뢰도
                    'derived_from': 'business_model_text',
                    'as_of': datetime.now().strftime('%Y-%m-%d')
                })
                break
    
    return tags


def _get_driver_tags_by_text(
    driver_code: str,
    company_detail: Any,
    allowed_tags: List[str],
    existing_tags: List[str]
) -> List[Dict[str, Any]]:
    """
    텍스트 매칭으로 드라이버 태그 부여 (후순위)
    
    Returns:
        태그 정보 리스트
    """
    tags = []
    
    if not company_detail:
        return tags
    
    # 텍스트 수집
    text_blobs = []
    if hasattr(company_detail, 'biz_summary') and company_detail.biz_summary:
        text_blobs.append(company_detail.biz_summary.lower())
    if hasattr(company_detail, 'keywords') and company_detail.keywords:
        keywords_text = " ".join([str(k).lower() for k in company_detail.keywords[:10]])
        text_blobs.append(keywords_text)
    
    combined_text = " ".join(text_blobs)
    
    # 각 태그별 키워드 매칭
    for tag in allowed_tags:
        if tag in existing_tags:
            continue
        
        if len(tags) >= 3:  # 최대 3개
            break
        
        tag_keywords = DRIVER_TAG_KEYWORDS.get(tag, [])
        for keyword in tag_keywords:
            if keyword.lower() in combined_text:
                tags.append({
                    'tag': tag,
                    'confidence': 0.6,  # 텍스트 매칭은 낮은 신뢰도
                    'derived_from': 'text_matching',
                    'as_of': datetime.now().strftime('%Y-%m-%d')
                })
                break
    
    return tags


def get_driver_tag_confidence(
    driver_tag: Dict[str, Any],
    sector_l2_confidence: Optional[float] = None
) -> float:
    """
    Driver Tag의 최종 신뢰도 계산
    
    Args:
        driver_tag: 태그 정보 (confidence, derived_from 포함)
        sector_l2_confidence: L2 분류 신뢰도 (0.0~1.0)
    
    Returns:
        최종 신뢰도 (0.0~1.0)
    """
    base_confidence = driver_tag.get('confidence', 0.5)
    derived_from = driver_tag.get('derived_from', '')
    
    # L2 기반 태그는 L2 confidence를 반영
    if derived_from == 'sector_l2' and sector_l2_confidence is not None:
        # L2 confidence와 태그 confidence의 곱으로 계산
        return min(1.0, base_confidence * sector_l2_confidence)
    
    return base_confidence


def _resolve_tag_conflicts(tags: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    상반 태그 충돌 해결
    
    - IMPORT_DEPENDENT vs EXPORT_DRIVEN: 둘 다 있으면 confidence 높은 쪽만 유지
    - LOW_MARGIN vs HIGH_MARGIN: 둘 다 있으면 confidence 높은 쪽만 유지
    
    Args:
        tags: 태그 리스트 (confidence 포함)
    
    Returns:
        충돌 해결된 태그 리스트
    """
    if not tags:
        return tags
    
    tag_names = {t.get('tag') for t in tags}
    resolved = []
    excluded = set()  # 제외된 태그 추적
    
    for tag_info in tags:
        tag = tag_info.get('tag')
        
        # 이미 제외된 태그면 스킵
        if tag in excluded:
            continue
        
        # 충돌하는 태그 확인
        conflicting_tags = CONFLICTING_TAG_PAIRS.get(tag, [])
        has_conflict = any(c in tag_names for c in conflicting_tags)
        
        if has_conflict:
            # 충돌하는 태그 찾기
            for conflict_tag in conflicting_tags:
                if conflict_tag in tag_names:
                    # 충돌 태그 정보 찾기
                    conflict_info = next(
                        (t for t in tags if t.get('tag') == conflict_tag),
                        None
                    )
                    
                    if conflict_info:
                        my_conf = tag_info.get('confidence', 0.5)
                        other_conf = conflict_info.get('confidence', 0.5)
                        
                        if my_conf > other_conf:
                            # 내 태그가 더 높은 confidence → 내 태그 유지, 충돌 태그 제외
                            resolved.append(tag_info)
                            excluded.add(conflict_tag)
                            logger.debug(
                                f"[Driver Tags] 충돌 해결: {tag}({my_conf:.2f}) > {conflict_tag}({other_conf:.2f}) → {tag} 유지"
                            )
                        elif my_conf < other_conf:
                            # 충돌 태그가 더 높은 confidence → 내 태그 제외
                            excluded.add(tag)
                            logger.debug(
                                f"[Driver Tags] 충돌 해결: {tag}({my_conf:.2f}) < {conflict_tag}({other_conf:.2f}) → {conflict_tag} 유지"
                            )
                        else:
                            # 동일 confidence → 우선순위로 결정
                            my_priority = DRIVER_TAG_PRIORITY.get(tag, 0)
                            other_priority = DRIVER_TAG_PRIORITY.get(conflict_tag, 0)
                            
                            if my_priority >= other_priority:
                                resolved.append(tag_info)
                                excluded.add(conflict_tag)
                                logger.debug(
                                    f"[Driver Tags] 충돌 해결 (동일 confidence, 우선순위): {tag} 유지"
                                )
                            else:
                                excluded.add(tag)
                                logger.debug(
                                    f"[Driver Tags] 충돌 해결 (동일 confidence, 우선순위): {conflict_tag} 유지"
                                )
                    break
        else:
            # 충돌 없음 → 그대로 추가
            resolved.append(tag_info)
    
    return resolved


def enrich_driver_tags_with_supersession(
    driver_code: str,
    sector_l1: str,
    sector_l2: Optional[str],
    sector_l2_confidence: Optional[float] = None,
    company_detail: Optional[Any] = None,
    existing_tags: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    기존 태그를 고려한 Driver Tags 부여 (Supersession 규칙 적용)
    
    재분류 시 기존 태그와 비교하여:
    - 새 태그가 confidence 높으면 → 기존 태그 대체
    - 새 태그가 confidence 낮으면 → 기존 태그 유지
    - 상반 태그 충돌 → confidence 높은 쪽만 유지
    
    Args:
        driver_code: 드라이버 코드
        sector_l1: L1 섹터 코드
        sector_l2: L2 섹터 코드
        sector_l2_confidence: L2 분류 신뢰도
        company_detail: CompanyDetail 객체
        existing_tags: 기존에 부여된 태그 (재분류 시)
    
    Returns:
        최종 태그 리스트 (기존 태그와 비교하여 업데이트)
    """
    # 새로운 태그 생성
    new_tags = enrich_driver_tags(
        driver_code=driver_code,
        sector_l1=sector_l1,
        sector_l2=sector_l2,
        sector_l2_confidence=sector_l2_confidence,
        company_detail=company_detail
    )
    
    if not existing_tags:
        return new_tags
    
    # 기존 태그와 새 태그 비교
    existing_tag_map = {t.get('tag'): t for t in existing_tags if isinstance(t, dict)}
    new_tag_map = {t.get('tag'): t for t in new_tags}
    
    # 변경 감지
    existing_tag_names = set(existing_tag_map.keys())
    new_tag_names = set(new_tag_map.keys())
    
    added_tags = new_tag_names - existing_tag_names
    removed_tags = existing_tag_names - new_tag_names
    common_tags = existing_tag_names & new_tag_names
    
    if added_tags or removed_tags:
        logger.info(
            f"[Driver Tags Supersession] {driver_code}: "
            f"추가={added_tags}, 제거={removed_tags}, 유지={common_tags}"
        )
    
    # 최종 태그 결정
    final_tags = []
    processed_tags = set()
    
    # 1. 새 태그 중 기존에 없던 것 추가
    for tag in added_tags:
        if tag not in processed_tags:
            final_tags.append(new_tag_map[tag])
            processed_tags.add(tag)
    
    # 2. 기존 태그 중 새 태그에 없는 것: confidence 비교 후 결정
    for tag in removed_tags:
        existing_info = existing_tag_map.get(tag)
        if existing_info:
            existing_conf = existing_info.get('confidence', 0.5)
            
            # 기존 태그의 confidence가 높으면 유지 (deprecated 마킹)
            if existing_conf >= 0.7:
                # deprecated 마킹하고 유지
                existing_info_copy = existing_info.copy()
                existing_info_copy['status'] = 'deprecated_candidate'
                final_tags.append(existing_info_copy)
                processed_tags.add(tag)
                logger.debug(
                    f"[Driver Tags Supersession] {driver_code}: "
                    f"{tag} 유지 (높은 confidence={existing_conf:.2f})"
                )
    
    # 3. 공통 태그: 새 태그로 업데이트 (confidence가 변경될 수 있음)
    for tag in common_tags:
        if tag not in processed_tags:
            new_info = new_tag_map.get(tag)
            existing_info = existing_tag_map.get(tag)
            
            # 새 태그의 confidence가 더 높거나 같으면 새 태그 사용
            new_conf = new_info.get('confidence', 0.5) if new_info else 0
            old_conf = existing_info.get('confidence', 0.5) if existing_info else 0
            
            if new_conf >= old_conf:
                final_tags.append(new_info)
            else:
                # 기존 태그 유지하되 as_of 업데이트
                existing_info_copy = existing_info.copy()
                existing_info_copy['as_of'] = datetime.now().strftime('%Y-%m-%d')
                final_tags.append(existing_info_copy)
            
            processed_tags.add(tag)
    
    # 4. 상반 태그 충돌 해결
    final_tags = _resolve_tag_conflicts(final_tags)
    
    # 5. 우선순위 정렬 및 최대 3개 제한
    final_tags.sort(
        key=lambda t: DRIVER_TAG_PRIORITY.get(t.get('tag', ''), 0),
        reverse=True
    )
    
    return final_tags[:3]

