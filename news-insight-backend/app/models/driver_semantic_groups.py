# -*- coding: utf-8 -*-
"""
Driver Semantic Groups
UI에서 유사한 의미의 드라이버를 묶어서 표시하기 위한 그룹 정의

사용 예:
- Top-5 드라이버 중 같은 그룹에 속한 드라이버는 하나로 압축
- "DRAM_ASP, NAND_ASP" → "메모리 가격"
"""

from typing import Dict, List, Optional


# =============================================================================
# Driver Semantic Groups Definition
# =============================================================================

DRIVER_SEMANTIC_GROUPS = {
    # 메모리 반도체 가격
    'MEMORY_PRICE': {
        'display_name': '메모리 가격',
        'display_name_en': 'Memory Price',
        'members': ['DRAM_ASP', 'NAND_ASP'],
        'description': 'DRAM/NAND 반도체 평균판매가격',
        'icon': '💾',
    },
    # AI 인프라 수요
    'AI_INFRA_DEMAND': {
        'display_name': 'AI 인프라 수요',
        'display_name_en': 'AI Infrastructure Demand',
        'members': ['HBM_DEMAND', 'AI_SERVER_CAPEX', 'GPU_DEMAND'],
        'description': 'AI 서버, HBM, GPU 등 AI 인프라 투자',
        'icon': '🤖',
    },
    # 반도체 산업 전반
    'SEMICONDUCTOR_CYCLE': {
        'display_name': '반도체 사이클',
        'display_name_en': 'Semiconductor Cycle',
        'members': ['SEMICONDUCTOR_CAPEX', 'SEMICONDUCTOR_DEMAND', 'WAFER_DEMAND'],
        'description': '반도체 산업 투자 및 수요 사이클',
        'icon': '🔌',
    },
    # 환율
    'FX_RATE': {
        'display_name': '환율',
        'display_name_en': 'FX Rate',
        'members': ['EXCHANGE_RATE_USD_KRW', 'EXCHANGE_RATE_CNY_KRW', 'EXCHANGE_RATE_JPY_KRW'],
        'description': '원/달러, 원/위안 등 환율',
        'icon': '💱',
    },
    # 원자재/에너지
    'COMMODITY_ENERGY': {
        'display_name': '원자재/에너지',
        'display_name_en': 'Commodity & Energy',
        'members': ['OIL_PRICE', 'NAPHTHA_PRICE', 'COMMODITY_PRICE', 'GAS_PRICE', 'COAL_PRICE'],
        'description': '유가, 나프타, 원자재 가격',
        'icon': '🛢️',
    },
    # 소비/유통
    'CONSUMER_RETAIL': {
        'display_name': '소비/유통',
        'display_name_en': 'Consumer & Retail',
        'members': ['CONSUMER_SPENDING', 'RETAIL_SALES', 'E_COMMERCE_TRANS_VOL', 'CONSUMER_CONFIDENCE'],
        'description': '소비자 지출, 소매 판매',
        'icon': '🛒',
    },
    # 금리/금융
    'INTEREST_CREDIT': {
        'display_name': '금리/금융',
        'display_name_en': 'Interest & Credit',
        'members': ['INTEREST_RATE', 'LOAN_DEMAND', 'CONSUMER_CREDIT', 'DEPOSIT_GROWTH'],
        'description': '기준금리, 대출 수요',
        'icon': '🏦',
    },
    # 전기차/배터리
    'EV_BATTERY': {
        'display_name': '전기차/배터리',
        'display_name_en': 'EV & Battery',
        'members': ['EV_SALES', 'BATTERY_DEMAND', 'LITHIUM_PRICE', 'COBALT_PRICE'],
        'description': '전기차 판매, 배터리 수요',
        'icon': '🔋',
    },
    # IT 투자
    'IT_SPENDING': {
        'display_name': 'IT 투자',
        'display_name_en': 'IT Spending',
        'members': ['ENTERPRISE_IT_SPENDING', 'CLOUD_ADOPTION', 'RD_SPENDING', 'SOFTWARE_DEMAND'],
        'description': '기업 IT 투자, 클라우드, R&D',
        'icon': '💻',
    },
    # 철강/금속
    'STEEL_METAL': {
        'display_name': '철강/금속',
        'display_name_en': 'Steel & Metal',
        'members': ['STEEL_PRICE', 'IRON_ORE_PRICE', 'ALUMINUM_PRICE', 'COPPER_PRICE'],
        'description': '철강, 비철금속 가격',
        'icon': '🔩',
    },
    # 건설/부동산
    'CONSTRUCTION_RE': {
        'display_name': '건설/부동산',
        'display_name_en': 'Construction & Real Estate',
        'members': ['CONSTRUCTION_ORDERS', 'HOUSING_STARTS', 'REAL_ESTATE_PRICE', 'CEMENT_PRICE'],
        'description': '건설 수주, 주택 착공',
        'icon': '🏗️',
    },
    # 헬스케어
    'HEALTHCARE': {
        'display_name': '헬스케어',
        'display_name_en': 'Healthcare',
        'members': ['HEALTHCARE_SPENDING', 'PHARMA_RD_SPENDING', 'FDA_APPROVAL', 'AGING_POPULATION'],
        'description': '의료비, 제약 R&D',
        'icon': '🏥',
    },
    # 여행/항공
    'TRAVEL_AIRLINE': {
        'display_name': '여행/항공',
        'display_name_en': 'Travel & Airline',
        'members': ['TOURIST_ARRIVALS', 'AIRLINE_PASSENGERS', 'HOTEL_OCCUPANCY'],
        'description': '관광객, 항공 여객',
        'icon': '✈️',
    },
    # 자동차
    'AUTO_INDUSTRY': {
        'display_name': '자동차',
        'display_name_en': 'Auto Industry',
        'members': ['AUTO_SALES', 'AUTO_PRODUCTION', 'VEHICLE_INVENTORY'],
        'description': '자동차 판매, 생산',
        'icon': '🚗',
    },
}


# =============================================================================
# Reverse Mapping (driver -> group)
# =============================================================================

DRIVER_TO_GROUP: Dict[str, str] = {}
for group_id, group_info in DRIVER_SEMANTIC_GROUPS.items():
    for member in group_info['members']:
        DRIVER_TO_GROUP[member] = group_id


# =============================================================================
# Helper Functions
# =============================================================================

def get_group_for_driver(driver_code: str) -> Optional[str]:
    """
    드라이버 코드에 해당하는 그룹 ID 반환
    
    Args:
        driver_code: 드라이버 코드 (예: 'DRAM_ASP')
    
    Returns:
        그룹 ID (예: 'MEMORY_PRICE') 또는 None
    """
    return DRIVER_TO_GROUP.get(driver_code)


def get_group_display_name(group_id: str, lang: str = 'ko') -> str:
    """
    그룹 ID에 해당하는 표시명 반환
    
    Args:
        group_id: 그룹 ID (예: 'MEMORY_PRICE')
        lang: 언어 ('ko' 또는 'en')
    
    Returns:
        표시명 (예: '메모리 가격')
    """
    group = DRIVER_SEMANTIC_GROUPS.get(group_id)
    if not group:
        return group_id
    
    if lang == 'en':
        return group.get('display_name_en', group.get('display_name', group_id))
    return group.get('display_name', group_id)


def compress_drivers_for_ui(
    drivers: List[Dict],
    max_display: int = 5,
    keep_top_per_group: int = 1
) -> List[Dict]:
    """
    UI 표시용으로 드라이버 리스트 압축
    같은 그룹의 드라이버는 가장 weight가 높은 것만 유지
    
    Args:
        drivers: [{'code': 'DRAM_ASP', 'weight': 0.8}, ...]
        max_display: 최대 표시 개수
        keep_top_per_group: 그룹당 유지할 드라이버 수
    
    Returns:
        압축된 드라이버 리스트 (그룹 정보 포함)
    """
    # 그룹별로 정리
    grouped = {}
    ungrouped = []
    
    for driver in drivers:
        code = driver.get('code')
        group_id = get_group_for_driver(code)
        
        if group_id:
            if group_id not in grouped:
                grouped[group_id] = []
            grouped[group_id].append(driver)
        else:
            ungrouped.append(driver)
    
    # 각 그룹에서 top-N 선택
    result = []
    for group_id, group_drivers in grouped.items():
        # weight 기준 정렬
        sorted_drivers = sorted(group_drivers, key=lambda x: x.get('weight', 0), reverse=True)
        top_drivers = sorted_drivers[:keep_top_per_group]
        
        for d in top_drivers:
            d['group_id'] = group_id
            d['group_name'] = get_group_display_name(group_id)
            d['group_icon'] = DRIVER_SEMANTIC_GROUPS[group_id].get('icon', '')
            d['grouped_count'] = len(group_drivers)  # 몇 개가 그룹에 속했는지
            result.append(d)
    
    # 그룹 없는 드라이버 추가
    for d in ungrouped:
        d['group_id'] = None
        d['group_name'] = None
        d['grouped_count'] = 1
        result.append(d)
    
    # weight 기준 재정렬 후 top-N
    result = sorted(result, key=lambda x: x.get('weight', 0), reverse=True)
    return result[:max_display]


def get_all_groups() -> Dict:
    """모든 그룹 정보 반환"""
    return DRIVER_SEMANTIC_GROUPS.copy()


def get_group_members(group_id: str) -> List[str]:
    """그룹에 속한 드라이버 코드 리스트 반환"""
    group = DRIVER_SEMANTIC_GROUPS.get(group_id)
    if group:
        return group.get('members', [])
    return []

