"""
밸류체인 위치별 Reference 텍스트 정의

KorFinMTEB Zero-shot 분류를 위한 섹터별 밸류체인 위치 대표 텍스트
"""
# 순환 참조 방지를 위해 lazy import 사용
_SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS = None

def _get_sector_keywords():
    """Lazy import로 섹터별 키워드 가져오기"""
    global _SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS
    if _SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS is None:
        from app.services.value_chain_classifier import SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS
        _SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS = SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS
    return _SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS

# 28개 섹터별 밸류체인 위치 Reference 텍스트
VALUE_CHAIN_REFERENCES = {
    'UPSTREAM': {},
    'MIDSTREAM': {},
    'DOWNSTREAM': {}
}

def _initialize_references():
    """Reference 텍스트 초기화 (lazy initialization)"""
    if not VALUE_CHAIN_REFERENCES['UPSTREAM']:
        sector_keywords = _get_sector_keywords()
        for sector_code, vc_keywords in sector_keywords.items():
            # UPSTREAM
            if 'UPSTREAM' in vc_keywords:
                VALUE_CHAIN_REFERENCES['UPSTREAM'][sector_code] = ' '.join(vc_keywords['UPSTREAM'])
            
            # MIDSTREAM
            if 'MIDSTREAM' in vc_keywords:
                VALUE_CHAIN_REFERENCES['MIDSTREAM'][sector_code] = ' '.join(vc_keywords['MIDSTREAM'])
            
            # DOWNSTREAM
            if 'DOWNSTREAM' in vc_keywords:
                VALUE_CHAIN_REFERENCES['DOWNSTREAM'][sector_code] = ' '.join(vc_keywords['DOWNSTREAM'])

# 일반 밸류체인 Reference (섹터별 특화 키워드가 없는 경우)
GENERAL_VALUE_CHAIN_REFERENCES = {
    'UPSTREAM': "원재료 부품 소재 조달 매입 공급 업스트림",
    'MIDSTREAM': "제조 생산 가공 조립 패키징 중간",
    'DOWNSTREAM': "판매 유통 고객 매출 서비스 다운스트림 최종"
}


def get_value_chain_reference(sector_code: str, value_chain: str) -> str:
    """
    섹터 코드와 밸류체인 위치에 해당하는 Reference 텍스트 반환
    
    Args:
        sector_code: 섹터 코드 (예: 'SEC_SEMI')
        value_chain: 밸류체인 위치 ('UPSTREAM', 'MIDSTREAM', 'DOWNSTREAM')
    
    Returns:
        Reference 텍스트 또는 일반 Reference 텍스트
    """
    # 필요 시에만 Lazy 초기화
    _initialize_references()

    if value_chain not in VALUE_CHAIN_REFERENCES:
        return GENERAL_VALUE_CHAIN_REFERENCES.get(value_chain, "")
    
    # 섹터별 특화 Reference가 있으면 사용
    if sector_code in VALUE_CHAIN_REFERENCES[value_chain]:
        return VALUE_CHAIN_REFERENCES[value_chain][sector_code]
    
    # 없으면 일반 Reference 사용
    return GENERAL_VALUE_CHAIN_REFERENCES.get(value_chain, "")


def get_all_value_chain_references(sector_code: str) -> dict:
    """
    특정 섹터의 모든 밸류체인 위치 Reference 텍스트 반환
    
    Args:
        sector_code: 섹터 코드
    
    Returns:
        {
            'UPSTREAM': '...',
            'MIDSTREAM': '...',
            'DOWNSTREAM': '...'
        }
    """
    _initialize_references()
    return {
        vc: get_value_chain_reference(sector_code, vc)
        for vc in ['UPSTREAM', 'MIDSTREAM', 'DOWNSTREAM']
    }

