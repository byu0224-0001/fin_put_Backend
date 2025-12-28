"""
Multi-sector 케이스에서 Primary 섹터 결정 로직

PSW (Price Sensitivity Weight) 기반으로 Primary 섹터를 결정합니다.
"""
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

# 기본적으로 Non-primary로 처리할 섹터 타입
NON_PRIMARY_SECTOR_TYPES = ['SEC_HOLDING', 'SEC_REIT']


def determine_primary_sector(
    sector_results: List[Dict[str, Any]]
) -> Optional[str]:
    """
    Multi-sector 케이스에서 Primary 섹터 결정
    
    기준:
    1. PSW (Price Sensitivity Weight) 높은 순
    2. sector_weight 높은 순
    3. 지주사/리츠는 기본적으로 Non-primary (다른 섹터가 있으면)
    4. 모든 섹터가 지주사/리츠인 경우 첫 번째 섹터를 Primary로
    
    Args:
        sector_results: 섹터 분류 결과 리스트
    
    Returns:
        Primary 섹터 코드 (major_sector 또는 sector_l1)
    """
    if not sector_results:
        return None
    
    if len(sector_results) == 1:
        # 단일 섹터인 경우
        return sector_results[0].get('major_sector') or sector_results[0].get('sector_l1')
    
    # Multi-sector 케이스
    
    # 1. 지주사/리츠가 아닌 섹터 우선
    non_special_sectors = [
        s for s in sector_results
        if (s.get('major_sector') or s.get('sector_l1')) not in NON_PRIMARY_SECTOR_TYPES
    ]
    
    if non_special_sectors:
        # PSW + sector_weight 기반 정렬
        sorted_sectors = sorted(
            non_special_sectors,
            key=lambda x: (
                x.get('psw', 0),  # PSW 높은 순
                x.get('sector_weight', 0)  # sector_weight 높은 순
            ),
            reverse=True
        )
        
        primary_sector = sorted_sectors[0].get('major_sector') or sorted_sectors[0].get('sector_l1')
        logger.info(f"Primary 섹터 결정: {primary_sector} (PSW={sorted_sectors[0].get('psw', 0):.2f}, weight={sorted_sectors[0].get('sector_weight', 0):.2f})")
        return primary_sector
    
    # 2. 지주사/리츠만 있는 경우
    # 첫 번째 섹터를 Primary로 (또는 sector_weight 높은 순)
    sorted_special = sorted(
        sector_results,
        key=lambda x: x.get('sector_weight', 0),
        reverse=True
    )
    
    primary_sector = sorted_special[0].get('major_sector') or sorted_special[0].get('sector_l1')
    logger.info(f"Primary 섹터 결정 (특수 섹터만): {primary_sector}")
    return primary_sector


def apply_primary_sector_flags(
    sector_results: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    섹터 결과에 is_primary 플래그 적용
    
    Args:
        sector_results: 섹터 분류 결과 리스트
    
    Returns:
        is_primary 플래그가 적용된 섹터 결과 리스트
    """
    if not sector_results:
        return sector_results
    
    # Primary 섹터 결정
    primary_sector = determine_primary_sector(sector_results)
    
    # is_primary 플래그 적용
    for result in sector_results:
        result_sector = result.get('major_sector') or result.get('sector_l1')
        result['is_primary'] = (result_sector == primary_sector)
    
    return sector_results

