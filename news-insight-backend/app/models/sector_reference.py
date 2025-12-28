"""
28개 섹터별 Reference 텍스트 정의

KorFinMTEB Zero-shot 분류를 위한 섹터별 대표 텍스트
"""
from typing import Optional, Dict, Any, Tuple

# 28개 섹터별 Reference 텍스트 (Few-shot 예시)
SECTOR_REFERENCES = {
    # [Tech & Growth] - 5개
    'SEC_SEMI': "반도체 메모리 DRAM NAND HBM 제조 및 반도체 장비 공급 웨이퍼 파운드리 칩",
    'SEC_BATTERY': "2차전지 리튬이온 배터리 셀 양극재 음극재 전해액 소재 제조",
    'SEC_IT': "소프트웨어 개발 클라우드 SaaS 플랫폼 인터넷 서비스 보안 솔루션",
    'SEC_GAME': "모바일 게임 PC 게임 콘솔 게임 개발 및 게임 퍼블리싱 e스포츠",
    'SEC_ELECTRONICS': "가전 제품 TV 냉장고 세탁기 디스플레이 패널 OLED LCD",
    
    # [Mobility] - 2개
    'SEC_AUTO': "자동차 완성차 전기차 EV 자동차부품 전장 모터 인버터",
    'SEC_TIRE': "타이어 제조 고무 제품 합성고무",
    
    # [Industry & Cyclical] - 6개
    'SEC_SHIP': "조선 선박 조선소 해운 컨테이너선 LNG선 유조선 해상운송",
    'SEC_DEFENSE': "방산 방위 무기 국방 항공기 미사일 레이더 위성 우주 KFX",
    'SEC_MACH': "기계 공작기계 산업기계 전력기기 건설기계 중장비 펌프 발전기",
    'SEC_CONST': "건설 건축 토목 인프라 플랜트 공사",
    'SEC_STEEL': "철강 강판 강재 제철 비철금속 구리 알루미늄",
    'SEC_CHEM': "화학 석유화학 정유 LNG 폴리머 수지 플라스틱 합성섬유",
    
    # [Consumer & K-Culture] - 6개
    'SEC_ENT': "엔터테인먼트 미디어 콘텐츠 K-POP K팝 아이돌 앨범 음악 영화 드라마 OTT",
    'SEC_COSMETIC': "화장품 뷰티 코스메틱 스킨케어 메이크업 패션 의류 섬유 OEM",
    'SEC_TRAVEL': "여행 항공 카지노 면세 호텔 관광 레저 리조트 여행사",
    'SEC_FOOD': "음식료 식품 음료 가공식품 냉동식품 유제품 담배",
    'SEC_RETAIL': "유통 소매 마트 편의점 온라인쇼핑 이커머스 백화점",
    'SEC_CONSUMER': "가구 인테리어 렌탈 가전렌탈 소비재 생활용품",
    
    # [Healthcare] - 2개
    'SEC_BIO': "바이오 제약 신약 바이오의약 백신 바이오시밀러 CMO CDMO 임상",
    'SEC_MEDDEV': "의료기기 임플란트 미용기기 진단기기 수술기기 의료장비",
    
    # [Finance] - SEC_FINANCE로 통합 (하위 호환성을 위해 기존 섹터도 유지)
    'SEC_FINANCE': "금융 서비스 은행 증권 보험 카드 결제대행 PG 벤처캐피탈 신용평가",
    'SEC_BANK': "은행 대출 예금 저축 금융",  # 하위 호환성: SEC_FINANCE/BANK로 매핑
    'SEC_SEC': "증권 투자 자산운용 증권사 브로커",  # 하위 호환성: SEC_FINANCE/SECURITIES로 매핑
    'SEC_INS': "보험 생명보험 손해보험 재보험",  # 하위 호환성: SEC_FINANCE/INSURANCE로 매핑
    'SEC_CARD': "카드 신용카드 체크카드 결제 캐피탈 리츠",  # 하위 호환성: SEC_FINANCE/CARD로 매핑
    # [Holding] - 지주사는 독립 섹터로 분리
    'SEC_HOLDING': "지주 지주회사 홀딩스 계열사 자회사 경영 관리 투자 자산관리",
    
    # [Utility] - 2개
    'SEC_UTIL': "전기 가스 수도 열 발전 송전 배전 전력 원자력 신재생에너지 태양광 풍력 환경",
    'SEC_TELECOM': "통신 이동통신 인터넷 5G 6G 통신사 통신망",
    
    # [Education] - 1개
    'SEC_EDU': "교육 교육서비스 교육장비 교육출판 온라인교육 오프라인교육 기업교육 금융교육 투자교육 교과서 참고서 학습지"
}

# 하위 호환성을 위한 기존 섹터 매핑
LEGACY_SECTOR_MAPPING = {
    'SEC_CHEMICAL': 'SEC_CHEM',
    'SEC_UTILITIES': 'SEC_UTIL',
    'SEC_STAPLE': 'SEC_FOOD',
    'SEC_CONSTRUCTION': 'SEC_CONST',
    # 기존 섹터 → 새 28개 섹터 매핑
    'SEC_MEDICAL': 'SEC_BIO',  # 의료 → 바이오/제약 (더 일반적)
    'SEC_INDUSTRIAL': 'SEC_MACH',  # 산업재 → 기계/전력기기
    'SEC_DISCRETIONARY': 'SEC_CONSUMER',  # 경기소비재 → 기타 소비재
    # ⚠️ 금융 섹터 통합: SEC_FINANCE로 통합됨 (L2로 분류)
    # 기존 코드에서 SEC_BANK 등을 사용하는 경우 SEC_FINANCE로 매핑
    # 실제 L2 분류는 별도 로직에서 처리 (SEC_FINANCE/BANK)
}

# 금융 섹터 L1 → L2 매핑 (하위 호환성)
FINANCE_L1_TO_L2_MAPPING = {
    'SEC_BANK': ('SEC_FINANCE', 'BANK'),
    'SEC_SEC': ('SEC_FINANCE', 'SECURITIES'),
    'SEC_INS': ('SEC_FINANCE', 'INSURANCE'),
    'SEC_CARD': ('SEC_FINANCE', 'CARD'),
    # ⚠️ SEC_HOLDING은 이제 독립 섹터이므로 매핑에서 제외
}


def get_finance_l1_to_l2(sector_code: str) -> tuple:
    """
    기존 금융 섹터 코드를 (L1, L2) 튜플로 변환
    
    Args:
        sector_code: 기존 섹터 코드 (예: 'SEC_BANK')
    
    Returns:
        (L1, L2) 튜플 또는 (None, None)
        예: ('SEC_FINANCE', 'BANK')
    """
    return FINANCE_L1_TO_L2_MAPPING.get(sector_code, (None, None))


def get_allowed_sectors_for_validation() -> set:
    """
    Allowlist 검증용 섹터 코드 집합 반환
    
    SEC_FINANCE를 포함하고, 하위 호환성을 위해 기존 금융 섹터도 포함
    
    Returns:
        섹터 코드 집합
    """
    allowed = set(SECTOR_REFERENCES.keys())
    # SEC_FINANCE가 없으면 추가
    if 'SEC_FINANCE' not in allowed:
        allowed.add('SEC_FINANCE')
    return allowed

def get_sector_reference(sector_code: str) -> str:
    """
    섹터 코드에 해당하는 Reference 텍스트 반환
    
    Args:
        sector_code: 섹터 코드 (예: 'SEC_SEMI')
    
    Returns:
        Reference 텍스트 또는 빈 문자열
    """
    # 직접 매핑
    if sector_code in SECTOR_REFERENCES:
        return SECTOR_REFERENCES[sector_code]
    
    # 하위 호환성 매핑
    if sector_code in LEGACY_SECTOR_MAPPING:
        mapped_code = LEGACY_SECTOR_MAPPING[sector_code]
        return SECTOR_REFERENCES.get(mapped_code, "")
    
    return ""


def get_all_sector_references() -> dict:
    """
    모든 섹터 Reference 텍스트 반환
    
    Returns:
        {sector_code: reference_text} 딕셔너리
    """
    return SECTOR_REFERENCES.copy()


# =============================================================================
# L1 (대분류) 정의
# =============================================================================

# SEC_FINANCE 대분류 추가 (기존 SEC_BANK, SEC_SEC, SEC_INS, SEC_CARD 통합)
SECTOR_L1_DEFINITIONS = {
    'SEC_FINANCE': {
        'name_ko': '금융',
        'name_en': 'Finance',
        'description': '금융 서비스 산업',
        'l2_sectors': ['PG', 'CARD', 'BANK', 'SECURITIES', 'INSURANCE', 'VC', 'CREDIT_RATING', 'PAYMENT_SERVICE']
    },
    'SEC_SEMI': {
        'name_ko': '반도체',
        'name_en': 'Semiconductor',
        'description': '반도체 제조 및 장비',
        'l2_sectors': ['MEMORY', 'FOUNDRY', 'FABLESS', 'EQUIPMENT', 'MATERIAL', 'PACKAGING', 'DISTRIBUTION']
    },
    'SEC_BATTERY': {
        'name_ko': '배터리',
        'name_en': 'Battery',
        'description': '2차전지 및 배터리 관련',
        'l2_sectors': ['CELL', 'CATHODE', 'ANODE', 'ELECTROLYTE', 'EQUIPMENT', 'RECYCLE']
    },
    'SEC_AUTO': {
        'name_ko': '자동차',
        'name_en': 'Automobile',
        'description': '자동차 제조 및 부품',
        'l2_sectors': ['OEM', 'EV_PARTS', 'TRAD_PARTS', 'AUTONOMOUS']
    },
    'SEC_IT': {
        'name_ko': 'IT',
        'name_en': 'Information Technology',
        'description': '정보기술 및 소프트웨어',
        'l2_sectors': ['SOFTWARE', 'SECURITY', 'AI', 'CLOUD', 'INTERNET', 'HARDWARE_DISTRIBUTION']
    },
    'SEC_ENT': {
        'name_ko': '엔터테인먼트',
        'name_en': 'Entertainment',
        'description': '엔터테인먼트 및 미디어',
        'l2_sectors': ['AGENCY', 'CONTENT', 'PLATFORM', 'MUSIC', 'PUBLISHING']
    },
    # ⚠️ SEC_BANK, SEC_SEC, SEC_INS, SEC_CARD, SEC_HOLDING은 SEC_FINANCE의 L2로 통합됨
    # 하위 호환성을 위해 LEGACY_SECTOR_MAPPING 참고
    'SEC_GAME': {'name_ko': '게임', 'name_en': 'Game', 'description': '게임 개발 및 플랫폼'},
    'SEC_BIO': {'name_ko': '바이오', 'name_en': 'Bio', 'description': '바이오 제약'},
    'SEC_MEDDEV': {'name_ko': '의료기기', 'name_en': 'Medical Device', 'description': '의료기기'},
    'SEC_CONST': {'name_ko': '건설', 'name_en': 'Construction', 'description': '건설업'},
    'SEC_STEEL': {'name_ko': '철강', 'name_en': 'Steel', 'description': '철강업'},
    'SEC_CHEM': {'name_ko': '화학', 'name_en': 'Chemistry', 'description': '화학 산업'},
    'SEC_SHIP': {'name_ko': '조선', 'name_en': 'Shipbuilding', 'description': '조선업'},
    'SEC_DEFENSE': {'name_ko': '방위산업물자', 'name_en': 'Defense', 'description': '방위산업'},
    'SEC_MACH': {'name_ko': '기계', 'name_en': 'Machinery', 'description': '기계 산업'},
    'SEC_ELECTRONICS': {'name_ko': '가전제품', 'name_en': 'Electronics', 'description': '가전제품'},
    'SEC_COSMETIC': {'name_ko': '화장품', 'name_en': 'Cosmetics', 'description': '화장품'},
    'SEC_TRAVEL': {'name_ko': '여행', 'name_en': 'Travel', 'description': '여행 및 관광'},
    'SEC_FOOD': {'name_ko': '음식료', 'name_en': 'Food & Beverage', 'description': '음식료'},
    'SEC_RETAIL': {'name_ko': '유통', 'name_en': 'Retail', 'description': '유통업'},
    'SEC_CONSUMER': {'name_ko': '생활용품', 'name_en': 'Consumer Goods', 'description': '생활용품'},
    'SEC_UTIL': {'name_ko': '전력에너지', 'name_en': 'Utilities', 'description': '전력 및 에너지'},
    'SEC_TELECOM': {'name_ko': '통신', 'name_en': 'Telecom', 'description': '통신업'},
    'SEC_TIRE': {'name_ko': '타이어', 'name_en': 'Tire', 'description': '타이어 제조'},
    'SEC_HOLDING': {
        'name_ko': '지주사',
        'name_en': 'Holding Company',
        'description': '지주회사 (계열사 관리 및 경영)',
        'l2_sectors': ['GENERAL_HOLDING', 'FINANCIAL_HOLDING', 'INDUSTRIAL_HOLDING']
    },
    'SEC_REIT': {
        'name_ko': '리츠',
        'name_en': 'REITs',
        'description': '부동산투자회사 (Real Estate Investment Trusts)',
        'l2_sectors': ['COMMERCIAL_REIT', 'OFFICE_REIT', 'INFRA_REIT', 'RESIDENTIAL_REIT']
    },
    'SEC_EDU': {
        'name_ko': '교육',
        'name_en': 'Education',
        'description': '교육 서비스 산업',
        'l2_sectors': ['EDU_SERVICE', 'EDU_EQUIPMENT', 'EDU_PUBLISHING']
    },
}


# =============================================================================
# L2 (중분류) 정의 - 비즈니스 모델/산업 유형
# =============================================================================

SECTOR_L2_DEFINITIONS = {
    'SEC_FINANCE': {
        'PG': {
            'display_name': '결제대행사',
            'name_ko': '결제대행사',
            'business_model_role': 'SERVICE',
            'keywords': ['PG', '결제대행', '전자지급결제대행', 'VAN', '이니시스', 'KG이니시스'],
            'recommended_drivers': ['E_COMMERCE_TRANS_VOL', 'MOBILE_PAYMENT_ADOPTION', 'CARD_SPENDING'],
            'common_drivers': ['CARD_TXN', 'DIGITAL_PAYMENT_GROWTH'],
            'do_not_use_drivers': [],
        },
        'CARD': {
            'display_name': '카드',
            'name_ko': '카드사',
            'business_model_role': 'SERVICE',
            'keywords': ['카드', '신용카드', '체크카드', '카드사'],
            'recommended_drivers': ['CARD_SPENDING', 'CARD_TXN', 'CONSUMER_CREDIT'],
            'common_drivers': ['RETAIL_SALES'],
            'do_not_use_drivers': [],
        },
        'BANK': {
            'display_name': '은행',
            'name_ko': '은행',
            'business_model_role': 'SERVICE',
            'keywords': ['은행', '저축은행', '대출', '예금'],
            'recommended_drivers': ['INTEREST_RATE', 'LOAN_DEMAND', 'DEPOSIT_GROWTH'],
            'common_drivers': ['ECONOMIC_GROWTH'],
            'do_not_use_drivers': [],
        },
        'SECURITIES': {
            'display_name': '증권',
            'name_ko': '증권사',
            'business_model_role': 'SERVICE',
            'keywords': ['증권', '증권사', '투자', '자산운용'],
            'recommended_drivers': ['STOCK_MARKET_VOLUME', 'TRADING_VOLUME'],
            'common_drivers': ['MARKET_SENTIMENT'],
            'do_not_use_drivers': [],
        },
        'INSURANCE': {
            'display_name': '보험',
            'name_ko': '보험사',
            'business_model_role': 'SERVICE',
            'keywords': ['보험', '생명보험', '손해보험', '재보험'],
            'recommended_drivers': ['INSURANCE_PREMIUM', 'CLAIM_RATIO'],
            'common_drivers': ['ECONOMIC_GROWTH'],
            'do_not_use_drivers': [],
        },
        'VC': {
            'display_name': '벤처캐피탈',
            'name_ko': '벤처캐피탈',
            'business_model_role': 'INVESTMENT',
            'keywords': ['벤처캐피탈', 'VC', '투자', '스타트업'],
            'recommended_drivers': ['STARTUP_FUNDING', 'VC_INVESTMENT'],
            'common_drivers': [],
            'do_not_use_drivers': [],
        },
        'CREDIT_RATING': {
            'display_name': '신용평가',
            'name_ko': '신용평가사',
            'business_model_role': 'SERVICE',
            'keywords': ['신용평가', '신용평가사', '평가'],
            'recommended_drivers': ['BOND_ISSUANCE', 'CREDIT_DEMAND'],
            'common_drivers': [],
            'do_not_use_drivers': [],
        },
        'PAYMENT_SERVICE': {
            'display_name': '결제서비스',
            'name_ko': '결제서비스',
            'business_model_role': 'SERVICE',
            'keywords': ['결제서비스', '모바일결제', '간편결제'],
            'recommended_drivers': ['MOBILE_PAYMENT_ADOPTION', 'E_COMMERCE_TRANS_VOL'],
            'common_drivers': ['CARD_SPENDING'],
            'do_not_use_drivers': [],
        },
    },
    'SEC_HOLDING': {
        'GENERAL_HOLDING': {
            'display_name': '일반 지주사',
            'name_ko': '일반 지주사',
            'business_model_role': 'HOLDING',
            'keywords': ['지주', '홀딩', '계열사', '자회사', '경영', '관리', '투자'],
            'recommended_drivers': ['ECONOMIC_GROWTH', 'CORPORATE_INVESTMENT'],
            'common_drivers': ['M&A_ACTIVITY'],
            'do_not_use_drivers': [],
        },
        'FINANCIAL_HOLDING': {
            'display_name': '금융 지주사',
            'name_ko': '금융 지주사',
            'business_model_role': 'HOLDING',
            'keywords': ['금융지주', '금융그룹', '금융지주사'],
            'recommended_drivers': ['INTEREST_RATE', 'ECONOMIC_GROWTH'],
            'common_drivers': ['FINANCIAL_MARKET_VOLUME'],
            'do_not_use_drivers': [],
        },
        'INDUSTRIAL_HOLDING': {
            'display_name': '산업 지주사',
            'name_ko': '산업 지주사',
            'business_model_role': 'HOLDING',
            'keywords': ['산업지주', '산업그룹'],
            'recommended_drivers': ['ECONOMIC_GROWTH', 'INDUSTRIAL_PRODUCTION'],
            'common_drivers': ['CORPORATE_INVESTMENT'],
            'do_not_use_drivers': [],
        },
    },
    'SEC_REIT': {
        'COMMERCIAL_REIT': {
            'display_name': '상업용 리츠',
            'name_ko': '상업용 리츠',
            'business_model_role': 'REAL_ESTATE',
            'keywords': ['상업용', '상업시설', '쇼핑몰', '리테일', '상업용 부동산', '상업용 리츠'],
            'recommended_drivers': ['RETAIL_SALES', 'COMMERCIAL_REAL_ESTATE_DEMAND'],
            'common_drivers': ['ECONOMIC_GROWTH'],
            'do_not_use_drivers': [],
        },
        'OFFICE_REIT': {
            'display_name': '오피스 리츠',
            'name_ko': '오피스 리츠',
            'business_model_role': 'REAL_ESTATE',
            'keywords': ['오피스', '사무용', '사무실', '오피스빌딩', '사무용 부동산', '오피스 리츠'],
            'recommended_drivers': ['OFFICE_REAL_ESTATE_DEMAND', 'BUSINESS_CONDITIONS'],
            'common_drivers': ['ECONOMIC_GROWTH'],
            'do_not_use_drivers': [],
        },
        'INFRA_REIT': {
            'display_name': '인프라 리츠',
            'name_ko': '인프라 리츠',
            'business_model_role': 'REAL_ESTATE',
            'keywords': ['인프라', '인프라스트럭처', 'SOC', '사회간접자본', '인프라 부동산', '인프라 리츠'],
            'recommended_drivers': ['INFRASTRUCTURE_INVESTMENT', 'GOVERNMENT_SPENDING'],
            'common_drivers': ['ECONOMIC_GROWTH'],
            'do_not_use_drivers': [],
        },
        'RESIDENTIAL_REIT': {
            'display_name': '주거용 리츠',
            'name_ko': '주거용 리츠',
            'business_model_role': 'REAL_ESTATE',
            'keywords': ['주거용', '주택', '아파트', '주거시설', '주거용 부동산', '주거용 리츠'],
            'recommended_drivers': ['HOUSING_STARTS', 'HOUSING_PRICES'],
            'common_drivers': ['INTEREST_RATE'],
            'do_not_use_drivers': [],
        },
    },
    'SEC_IT': {
        'SOFTWARE': {
            'display_name': '소프트웨어',
            'name_ko': '소프트웨어',
            'business_model_role': 'SERVICE',
            'keywords': ['소프트웨어', 'SW', '애플리케이션', '앱'],
            'recommended_drivers': ['ENTERPRISE_IT_SPENDING', 'SOFTWARE_DEMAND'],
            'common_drivers': ['ECONOMIC_GROWTH'],
            'do_not_use_drivers': [],
        },
        'SECURITY': {
            'display_name': '보안',
            'name_ko': '보안',
            'business_model_role': 'SERVICE',
            'keywords': ['보안', '사이버보안', '정보보안', '보안솔루션'],
            'recommended_drivers': ['CYBERSECURITY_SPENDING', 'DATA_BREACH_INCIDENTS'],
            'common_drivers': ['ENTERPRISE_IT_SPENDING'],
            'do_not_use_drivers': [],
        },
        'AI': {
            'display_name': '인공지능',
            'name_ko': '인공지능',
            'business_model_role': 'SERVICE',
            'keywords': ['AI', '인공지능', '머신러닝', '딥러닝'],
            'recommended_drivers': ['AI_INVESTMENT', 'AI_ADOPTION'],
            'common_drivers': ['ENTERPRISE_IT_SPENDING'],
            'do_not_use_drivers': [],
        },
        'CLOUD': {
            'display_name': '클라우드',
            'name_ko': '클라우드',
            'business_model_role': 'SERVICE',
            'keywords': ['클라우드', '클라우드컴퓨팅', 'SaaS', 'PaaS'],
            'recommended_drivers': ['CLOUD_ADOPTION', 'ENTERPRISE_IT_SPENDING'],
            'common_drivers': ['DIGITAL_TRANSFORMATION'],
            'do_not_use_drivers': [],
        },
        'INTERNET': {
            'display_name': '인터넷',
            'name_ko': '인터넷',
            'business_model_role': 'PLATFORM',
            'keywords': ['인터넷', '온라인', '플랫폼', '포털'],
            'recommended_drivers': ['INTERNET_PENETRATION', 'DIGITAL_ADOPTION'],
            'common_drivers': ['ECONOMIC_GROWTH'],
            'do_not_use_drivers': [],
        },
        'HARDWARE_DISTRIBUTION': {
            'display_name': '하드웨어 유통',
            'name_ko': '하드웨어 유통',
            'business_model_role': 'DISTRIBUTION',
            'keywords': ['하드웨어', '유통', 'IT유통', '컴퓨터유통'],
            'recommended_drivers': ['IT_HARDWARE_DEMAND', 'EXCHANGE_RATE_USD_KRW'],
            'common_drivers': ['PC_DEMAND'],
            'do_not_use_drivers': [],
        },
    },
    'SEC_EDU': {
        'EDU_SERVICE': {
            'display_name': '교육서비스',
            'name_ko': '교육서비스',
            'business_model_role': 'SERVICE',
            'keywords': ['교육', '교육서비스', '학원', '온라인교육'],
            'recommended_drivers': ['EDUCATION_SPENDING', 'STUDENT_ENROLLMENT'],
            'common_drivers': ['DISPOSABLE_INCOME'],
            'do_not_use_drivers': [],
        },
        'EDU_EQUIPMENT': {
            'display_name': '교육장비',
            'name_ko': '교육장비',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['교육장비', '교육기기', '교육용기기'],
            'recommended_drivers': ['EDUCATION_SPENDING', 'SCHOOL_BUDGET'],
            'common_drivers': ['GOVERNMENT_SPENDING'],
            'do_not_use_drivers': [],
        },
        'EDU_PUBLISHING': {
            'display_name': '교육출판',
            'name_ko': '교육출판',
            'business_model_role': 'CONTENT',
            'keywords': ['교육출판', '교과서', '참고서', '학습지'],
            'recommended_drivers': ['EDUCATION_SPENDING', 'STUDENT_ENROLLMENT'],
            'common_drivers': ['GOVERNMENT_SPENDING'],
            'do_not_use_drivers': [],
        },
    },
    'SEC_SEMI': {
        'MEMORY': {
            'display_name': '메모리 반도체',
            'name_ko': '메모리 반도체',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['메모리', 'DRAM', 'NAND', 'HBM'],
            'recommended_drivers': ['DRAM_ASP', 'NAND_ASP', 'HBM_DEMAND'],
            'common_drivers': ['SMARTPHONE_DEMAND', 'PC_DEMAND'],
            'do_not_use_drivers': [],
        },
        'FOUNDRY': {
            'display_name': '파운드리',
            'name_ko': '파운드리',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['파운드리', '웨이퍼', '제조'],
            'recommended_drivers': ['WAFER_DEMAND', 'CAPACITY_UTILIZATION'],
            'common_drivers': ['SEMICONDUCTOR_DEMAND'],
            'do_not_use_drivers': [],
        },
        'FABLESS': {
            'display_name': '팹리스/설계',
            'name_ko': '팹리스/설계',
            'business_model_role': 'DESIGN',
            'keywords': ['팹리스', '설계', '칩설계'],
            'recommended_drivers': ['SEMICONDUCTOR_DEMAND', 'DESIGN_WINS'],
            'common_drivers': ['TECHNOLOGY_ADOPTION'],
            'do_not_use_drivers': [],
        },
        'EQUIPMENT': {
            'display_name': '반도체 장비',
            'name_ko': '반도체 장비',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['반도체장비', '장비', '제조장비'],
            'recommended_drivers': ['SEMICONDUCTOR_CAPEX', 'EQUIPMENT_ORDERS'],
            'common_drivers': ['SEMICONDUCTOR_DEMAND'],
            'do_not_use_drivers': [],
        },
        'MATERIAL': {
            'display_name': '반도체 소재',
            'name_ko': '반도체 소재',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['반도체소재', '소재', '화학소재'],
            'recommended_drivers': ['SEMICONDUCTOR_DEMAND', 'MATERIAL_COSTS'],
            'common_drivers': ['SEMICONDUCTOR_CAPEX'],
            'do_not_use_drivers': [],
        },
        'PACKAGING': {
            'display_name': '패키징/테스트',
            'name_ko': '패키징/테스트',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['패키징', '테스트', '포장'],
            'recommended_drivers': ['SEMICONDUCTOR_DEMAND', 'PACKAGING_DEMAND'],
            'common_drivers': ['SEMICONDUCTOR_CAPEX'],
            'do_not_use_drivers': [],
        },
        'DISTRIBUTION': {
            'display_name': '반도체 유통',
            'name_ko': '반도체 유통',
            'business_model_role': 'DISTRIBUTION',
            'keywords': ['유통', '반도체유통', '리셀러'],
            'recommended_drivers': ['EXCHANGE_RATE_USD_KRW', 'IT_HARDWARE_DEMAND'],
            'common_drivers': ['SEMICONDUCTOR_DEMAND'],
            'do_not_use_drivers': [],
        },
    },
    'SEC_BATTERY': {
        'CELL': {
            'display_name': '셀/팩 제조',
            'name_ko': '셀/팩 제조',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['셀', '팩', '배터리셀', '배터리팩'],
            'recommended_drivers': ['EV_SALES', 'BATTERY_DEMAND'],
            'common_drivers': ['ENERGY_STORAGE_DEMAND'],
            'do_not_use_drivers': [],
        },
        'CATHODE': {
            'display_name': '양극재',
            'name_ko': '양극재',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['양극재', '캐소드', 'NCM', 'NCA'],
            'recommended_drivers': ['BATTERY_DEMAND', 'CATHODE_MATERIAL_PRICE'],
            'common_drivers': ['EV_SALES'],
            'do_not_use_drivers': [],
        },
        'ANODE': {
            'display_name': '음극재',
            'name_ko': '음극재',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['음극재', '애노드', '그래핀'],
            'recommended_drivers': ['BATTERY_DEMAND', 'ANODE_MATERIAL_PRICE'],
            'common_drivers': ['EV_SALES'],
            'do_not_use_drivers': [],
        },
        'ELECTROLYTE': {
            'display_name': '전해액/분리막',
            'name_ko': '전해액/분리막',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['전해액', '분리막', '전해질'],
            'recommended_drivers': ['BATTERY_DEMAND', 'ELECTROLYTE_PRICE'],
            'common_drivers': ['EV_SALES'],
            'do_not_use_drivers': [],
        },
        'EQUIPMENT': {
            'display_name': '배터리 장비',
            'name_ko': '배터리 장비',
            'business_model_role': 'MANUFACTURING',
            'keywords': ['배터리장비', '장비', '제조장비'],
            'recommended_drivers': ['BATTERY_CAPEX', 'EQUIPMENT_ORDERS'],
            'common_drivers': ['EV_SALES'],
            'do_not_use_drivers': [],
        },
        'RECYCLE': {
            'display_name': '폐배터리/리사이클',
            'name_ko': '폐배터리/리사이클',
            'business_model_role': 'SERVICE',
            'keywords': ['폐배터리', '리사이클', '재활용'],
            'recommended_drivers': ['BATTERY_RECYCLING_DEMAND', 'EV_FLEET_SIZE'],
            'common_drivers': ['ENVIRONMENTAL_REGULATION'],
            'do_not_use_drivers': [],
        },
    },
}

# Backward compatibility alias
SUB_SECTOR_DEFINITIONS = SECTOR_L2_DEFINITIONS


# =============================================================================
# Driver Tags Allowlist 정의
# =============================================================================

DRIVER_TAG_ALLOWLIST = {
    'EXCHANGE_RATE_USD_KRW': [
        'IMPORT_DEPENDENT',  # 수입 의존
        'EXPORT_DRIVEN',     # 수출 주도
        'COST_FACTOR',       # 비용 요인
        'REVENUE_FACTOR',    # 수익 요인
        'DISTRIBUTION',      # 유통
        'MANUFACTURING',     # 제조
    ],
    'HBM_DEMAND': [
        'AI_SERVER',
        'DATA_CENTER',
        'GPU',
        'HIGH_PERFORMANCE_COMPUTING',
    ],
    'DRAM_ASP': [
        'MEMORY_MARKET',
        'PC',
        'MOBILE',
        'SERVER',
    ],
    'NAND_ASP': [
        'MEMORY_MARKET',
        'STORAGE',
        'SSD',
    ],
    'E_COMMERCE_TRANS_VOL': [
        'ONLINE_RETAIL',
        'DIGITAL_PAYMENT',
        'MOBILE_COMMERCE',
    ],
    'MOBILE_PAYMENT_ADOPTION': [
        'DIGITAL_PAYMENT',
        'FINANCIAL_TECH',
    ],
    'CARD_SPENDING': [
        'CONSUMER_SPENDING',
        'RETAIL',
    ],
    'INTEREST_RATE': [
        'MONETARY_POLICY',
        'FINANCIAL_SECTOR',
    ],
    'LOAN_DEMAND': [
        'CREDIT_DEMAND',
        'ECONOMIC_GROWTH',
    ],
    'STOCK_MARKET_VOLUME': [
        'MARKET_ACTIVITY',
        'INVESTOR_SENTIMENT',
    ],
    'AI_SERVER_CAPEX': [
        'AI_INVESTMENT',
        'DATA_CENTER',
        'CLOUD_INFRASTRUCTURE',
    ],
    'SEMICONDUCTOR_CAPEX': [
        'CAPEX_CYCLE',
        'TECHNOLOGY_INVESTMENT',
    ],
    'EV_SALES': [
        'ELECTRIC_VEHICLE',
        'AUTOMOTIVE',
    ],
    'BATTERY_DEMAND': [
        'EV_MARKET',
        'ENERGY_STORAGE',
    ],
    # 비즈니스 모델 관련 태그 (범용 - 모든 드라이버에 적용 가능)
    'REVENUE': [
        'RECURRING_REVENUE',  # 반복 매출 (구독)
        'PLATFORM_BIZ',       # 플랫폼 비즈니스
        'LOW_MARGIN',         # 저마진 (OEM)
        'HIGH_MARGIN',        # 고마진
    ],
    'COST': [
        'LOW_MARGIN',         # 저마진 (OEM)
        'HIGH_MARGIN',        # 고마진
    ],
}

# Driver Tag 우선순위 정의 (높을수록 우선)
DRIVER_TAG_PRIORITY = {
    # L2 기반 태그 (최우선)
    'IMPORT_DEPENDENT': 100,
    'EXPORT_DRIVEN': 100,
    'DISTRIBUTION': 90,
    'MANUFACTURING': 90,
    'COST_FACTOR': 85,
    'REVENUE_FACTOR': 85,
    
    # 드라이버 특성 태그 (중간 우선순위)
    'AI_SERVER': 70,
    'DATA_CENTER': 70,
    'MEMORY_MARKET': 70,
    
    # 비즈니스 모델 특성 태그 (중간 우선순위)
    'RECURRING_REVENUE': 75,  # 반복 매출 (구독)
    'PLATFORM_BIZ': 75,       # 플랫폼 비즈니스
    'LOW_MARGIN': 70,         # 저마진 (OEM)
    'HIGH_MARGIN': 70,        # 고마진
    
    # 일반 태그 (낮은 우선순위)
    'PC': 50,
    'MOBILE': 50,
    'STORAGE': 50,
    'DIGITAL_PAYMENT': 50,
    'CONSUMER_SPENDING': 40,
    'ECONOMIC_GROWTH': 30,
}

# Driver Tag 키워드 매핑 (텍스트 매칭용)
DRIVER_TAG_KEYWORDS = {
    'IMPORT_DEPENDENT': ['수입', 'import', '해외구매', '원자재수입'],
    'EXPORT_DRIVEN': ['수출', 'export', '해외판매', '수출주도'],
    'COST_FACTOR': ['원가', '비용', 'cost', '원자재비'],
    'REVENUE_FACTOR': ['수익', '매출', 'revenue', '판매'],
    'AI_SERVER': ['AI서버', 'AI 서버', '인공지능서버', '데이터센터'],
    'DATA_CENTER': ['데이터센터', '데이터센터', 'IDC', '클라우드인프라'],
    'MEMORY_MARKET': ['메모리', 'DRAM', 'NAND', 'HBM'],
    'PC': ['PC', '컴퓨터', '노트북', '데스크톱'],
    'MOBILE': ['모바일', '스마트폰', '휴대폰', 'mobile'],
    'STORAGE': ['저장장치', 'SSD', '하드디스크', 'storage'],
    'DIGITAL_PAYMENT': ['디지털결제', '모바일결제', '간편결제', '전자결제'],
    'CONSUMER_SPENDING': ['소비', '소비지출', '소비자지출', 'consumer spending'],
    'ECONOMIC_GROWTH': ['경제성장', 'GDP', '경제', 'economic growth'],
    # 비즈니스 모델 특성 키워드
    'RECURRING_REVENUE': ['구독', 'subscription', '정기결제', '멤버십', '월정액', '구독료', '정기수익'],
    'PLATFORM_BIZ': ['플랫폼', 'platform', '마켓플레이스', '중개', '거래소', '매칭'],
    'LOW_MARGIN': ['OEM', 'ODM', '위탁생산', '하청', '대행', '원가절감'],
    'HIGH_MARGIN': ['브랜드', '자체브랜드', '프리미엄', '고부가가치', '차별화'],
}

# =============================================================================
# Driver Tag 충돌 쌍 정의 (Mutual Exclusion)
# - 같은 Driver에 상반되는 태그가 동시에 부여되면 안 됨
# - Supersession 시 높은 confidence 태그만 유지
# =============================================================================

CONFLICTING_TAG_PAIRS = {
    # 수출/수입 상반
    'IMPORT_DEPENDENT': ['EXPORT_DRIVEN'],
    'EXPORT_DRIVEN': ['IMPORT_DEPENDENT'],
    
    # 비용/수익 요인 상반
    'COST_FACTOR': ['REVENUE_FACTOR'],
    'REVENUE_FACTOR': ['COST_FACTOR'],
    
    # 마진 상반
    'LOW_MARGIN': ['HIGH_MARGIN'],
    'HIGH_MARGIN': ['LOW_MARGIN'],
    
    # 일회성/반복 수익 상반
    'ONE_OFF_REVENUE': ['RECURRING_REVENUE'],
    'RECURRING_REVENUE': ['ONE_OFF_REVENUE'],
}


# =============================================================================
# 분석 상태 상수 정의 (Enum-like)
# =============================================================================

ANALYSIS_STATE = {
    'OK': 'OK',
    'LIMITED': 'LIMITED',
    'INSUFFICIENT_DRIVER_SIGNAL': 'INSUFFICIENT_DRIVER_SIGNAL',
    'MODEL_ERROR': 'MODEL_ERROR',
    'SUCCESS': 'SUCCESS',  # Gemini 호출 성공
    'FAILED': 'FAILED',    # Gemini 호출 실패
    'ERROR': 'ERROR',      # 예외 발생
}


# =============================================================================
# L2 분리 규칙 정의 (하드코딩 제거)
# =============================================================================

L2_SPLIT_RULES = {
    # 제조 vs 유통 분리가 필요한 섹터
    'MANUFACTURING_DISTRIBUTION': {
        'sectors': ['SEC_SEMI', 'SEC_IT', 'SEC_AUTO', 'SEC_CHEM', 'SEC_ELECTRONICS'],
        'classification_keywords': {
            'DISTRIBUTION': ['유통', '수입', '수출', '판매', '도매', '소매', 'import', 'export', 'distribution'],
            'MANUFACTURING': ['제조', '생산', '공장', '제작', 'manufacturing', 'production', 'fabrication'],
        },
        'default': 'MANUFACTURING',  # 키워드 없으면 제조로 가정
    },
    
    # 콘텐츠 vs 플랫폼 분리가 필요한 섹터
    'CONTENT_PLATFORM': {
        'sectors': ['SEC_GAME', 'SEC_MEDIA', 'SEC_ENTERTAINMENT'],
        'classification_keywords': {
            'PLATFORM': ['플랫폼', '중개', '마켓플레이스', 'platform', 'marketplace'],
            'CONTENT': ['개발', '제작', '콘텐츠', '스튜디오', 'development', 'studio', 'content'],
        },
        'default': 'CONTENT',
    },
    
    # 자산 vs 플랫폼 분리가 필요한 섹터
    'ASSET_PLATFORM': {
        'sectors': ['SEC_TRAVEL', 'SEC_REIT'],
        'classification_keywords': {
            'PLATFORM': ['플랫폼', '중개', '예약', 'OTA', 'platform'],
            'ASSET_OWNER': ['호텔', '리조트', '부동산', '자산', 'hotel', 'resort', 'real estate'],
        },
        'default': 'ASSET_OWNER',
    },
}


def get_l2_split_type(major_sector: str) -> Optional[str]:
    """
    섹터에 적용할 L2 분리 유형 반환
    
    Returns:
        'MANUFACTURING_DISTRIBUTION', 'CONTENT_PLATFORM', 'ASSET_PLATFORM', 또는 None
    """
    for split_type, rule in L2_SPLIT_RULES.items():
        if major_sector in rule['sectors']:
            return split_type
    return None


def classify_l2_by_rule(
    major_sector: str,
    biz_summary: str
) -> Tuple[Optional[str], float]:
    """
    규칙 기반 L2 분류 (Confidence 포함)
    
    Args:
        major_sector: L1 섹터 코드
        biz_summary: 사업 설명 텍스트
    
    Returns:
        (L2 코드, Confidence 점수)
        - L2 코드: 예: 'DISTRIBUTION', 'MANUFACTURING'
        - Confidence: 0.0 ~ 1.0
    """
    split_type = get_l2_split_type(major_sector)
    if not split_type:
        return None, 0.0
    
    rule = L2_SPLIT_RULES[split_type]
    text_lower = (biz_summary or '').lower()
    
    scores = {}
    total_score = 0
    for l2_code, keywords in rule['classification_keywords'].items():
        score = sum(1 for kw in keywords if kw.lower() in text_lower)
        scores[l2_code] = score
        total_score += score
    
    # 점수가 가장 높은 L2 선택
    max_score = max(scores.values()) if scores else 0
    
    if max_score > 0:
        best_l2 = max(scores, key=scores.get)
        # Confidence 계산: 가장 높은 점수 / (전체 점수 + 1) -> 1은 smoothing
        confidence = max_score / (total_score + 1)
        return best_l2, confidence
    
    # 키워드 없으면 기본값 (Low confidence)
    default_l2 = rule.get('default')
    return default_l2, 0.3


# =============================================================================
# L3 (소분류) 태그 후보 정의 - 이미지 카테고리 전량 흡수
# =============================================================================

SECTOR_L3_TAG_CANDIDATES = {
    'SEC_SEMI': {
        'tags': [
            '반도체부품소재',
            '반도체장비',
            '반도체파운드리',
            '반도체패키징',
            '반도체팹리스',
            '종합반도체',
        ],
        'keywords_map': {
            '반도체부품소재': ['반도체소재', '웨이퍼', '화학소재', '부품소재'],
            '반도체장비': ['반도체장비', '제조장비', '장비', '설비'],
            '반도체파운드리': ['파운드리', '웨이퍼', '제조', '파운드리'],
            '반도체패키징': ['패키징', '포장', '테스트', '패키징'],
            '반도체팹리스': ['팹리스', '설계', '칩설계', 'IC설계'],
            '종합반도체': ['종합반도체', 'IDM', '통합반도체'],
        }
    },
    'SEC_FINANCE': {
        'tags': [
            '결제서비스',
            '금융그룹',
            '금융기기',
            '금융상품거래소',
            '벤처캐피탈',
            '보험',
            '신용평가',
            '암호화폐',
            '은행',
            '저축은행',
            '증권',
            '카드',
        ],
        'keywords_map': {
            '결제서비스': ['PG', '결제대행', '전자지급결제대행', 'VAN', '결제서비스', '모바일결제'],
            '금융그룹': ['금융그룹', '금융지주', '지주사'],
            '금융기기': ['금융기기', 'ATM', 'POS'],
            '금융상품거래소': ['거래소', '금융상품', '파생상품'],
            '벤처캐피탈': ['벤처캐피탈', 'VC', '투자', '스타트업'],
            '보험': ['보험', '생명보험', '손해보험', '재보험'],
            '신용평가': ['신용평가', '신용평가사', '평가'],
            '암호화폐': ['암호화폐', '비트코인', '블록체인', '가상화폐'],
            '은행': ['은행', '대출', '예금', '저축'],
            '저축은행': ['저축은행', '상호저축은행'],
            '증권': ['증권', '증권사', '투자', '자산운용'],
            '카드': ['카드', '신용카드', '체크카드', '카드사'],
        }
    },
    'SEC_BIO': {
        'tags': [
            '바이오서비스',
            '바이오시밀러',
            '바이오신약',
        ],
        'keywords_map': {
            '바이오서비스': ['CMO', 'CDMO', '바이오서비스', '위탁생산'],
            '바이오시밀러': ['바이오시밀러', '제네릭'],
            '바이오신약': ['신약', '바이오의약', '백신', '임상'],
        }
    },
    'SEC_CONST': {
        'tags': [
            '건설사',
            '건설자재',
            '건설장비',
            '부동산신탁',
            '부동산임대개발',
            '폐기물처리',
        ],
        'keywords_map': {
            '건설사': ['건설', '건축', '토목', '인프라', '플랜트'],
            '건설자재': ['시멘트', '철근', '형강', '건설자재'],
            '건설장비': ['건설기계', '중장비', '장비'],
            '부동산신탁': ['부동산신탁', 'REITs', '리츠'],
            '부동산임대개발': ['부동산', '임대', '개발'],
            '폐기물처리': ['폐기물', '재활용', '처리'],
        }
    },
    'SEC_REIT': {
        'tags': [
            '상업용 리츠',
            '오피스 리츠',
            '인프라 리츠',
            '주거용 리츠',
        ],
        'keywords_map': {
            '상업용 리츠': ['상업용', '상업시설', '쇼핑몰', '리테일', '상업용 부동산', '상업용 리츠'],
            '오피스 리츠': ['오피스', '사무용', '사무실', '오피스빌딩', '사무용 부동산', '오피스 리츠'],
            '인프라 리츠': ['인프라', '인프라스트럭처', 'SOC', '사회간접자본', '인프라 부동산', '인프라 리츠'],
            '주거용 리츠': ['주거용', '주택', '아파트', '주거시설', '주거용 부동산', '주거용 리츠'],
        }
    },
    'SEC_GAME': {
        'tags': [
            '게임개발',
            '게임플랫폼',
        ],
        'keywords_map': {
            '게임개발': ['게임', '모바일게임', 'PC게임', '콘솔게임', '개발'],
            '게임플랫폼': ['게임플랫폼', '스팀', '게임스토어'],
        }
    },
    'SEC_ELECTRONICS': {
        'tags': [
            '생활가전',
            '컴퓨터와 주변기기',
        ],
        'keywords_map': {
            '생활가전': ['TV', '냉장고', '세탁기', '가전'],
            '컴퓨터와 주변기기': ['컴퓨터', 'PC', '노트북', '주변기기'],
        }
    },
    'SEC_COSMETIC': {
        'tags': [
            '화장품브랜드',
            '화장품제조',
        ],
        'keywords_map': {
            '화장품브랜드': ['화장품', '뷰티', '코스메틱', '브랜드'],
            '화장품제조': ['화장품', '제조', 'OEM'],
        }
    },
    'SEC_TRAVEL': {
        'tags': [
            '렌터카',
            '여행플랫폼',
            '카지노',
            '호텔과 리조트',
        ],
        'keywords_map': {
            '렌터카': ['렌터카', '렌트카', '자동차렌탈'],
            '여행플랫폼': ['여행', '여행사', '플랫폼'],
            '카지노': ['카지노', '게임', '도박'],
            '호텔과 리조트': ['호텔', '리조트', '관광', '레저'],
        }
    },
    'SEC_FOOD': {
        'tags': [
            '간편식',
            '과자',
            '닭고기',
            '담배',
            '대마초',
            '돼지고기',
            '밀가루',
            '빵',
            '사료',
            '설탕',
            '수산물',
            '술',
            '식자재유통',
            '식품첨가물',
            '식품포장재',
            '아이스크림',
            '음료',
            '음식배달플랫폼',
            '음식점브랜드',
        ],
        'keywords_map': {
            '간편식': ['간편식', '냉동식품', '즉석식품'],
            '과자': ['과자', '스낵', '쿠키'],
            '닭고기': ['닭고기', '치킨', '계란'],
            '담배': ['담배', '니코틴'],
            '대마초': ['대마초', '마리화나'],
            '돼지고기': ['돼지고기', '돈육'],
            '밀가루': ['밀가루', '밀', '곡물'],
            '빵': ['빵', '베이커리'],
            '사료': ['사료', '동물사료'],
            '설탕': ['설탕', '당'],
            '수산물': ['수산물', '어류', '해산물'],
            '술': ['술', '주류', '맥주', '소주'],
            '식자재유통': ['식자재', '유통'],
            '식품첨가물': ['식품첨가물', '첨가물'],
            '식품포장재': ['포장재', '포장'],
            '아이스크림': ['아이스크림'],
            '음료': ['음료', '탄산음료', '주스'],
            '음식배달플랫폼': ['배달', '배달플랫폼', '배달앱'],
            '음식점브랜드': ['음식점', '레스토랑', '프랜차이즈'],
        }
    },
    'SEC_RETAIL': {
        'tags': [
            '대형마트',
            '면세점',
            '무역',
            '백화점',
            '온라인쇼핑',
            '전문점',
            '편의점',
            '홈쇼핑',
        ],
        'keywords_map': {
            '대형마트': ['대형마트', '마트', '할인점'],
            '면세점': ['면세점', '면세'],
            '무역': ['무역', '수출', '수입'],
            '백화점': ['백화점', '백화점'],
            '온라인쇼핑': ['온라인쇼핑', '이커머스', '온라인몰'],
            '전문점': ['전문점', '특화매장'],
            '편의점': ['편의점', 'CVS'],
            '홈쇼핑': ['홈쇼핑', 'TV쇼핑'],
        }
    },
    'SEC_ENT': {
        'tags': [
            '광고',
            '동영상플랫폼',
            '방송',
            '연예기획사',
            '영화',
            '웹툰',
            '음원',
            '출판',
            '캐릭터',
        ],
        'keywords_map': {
            '광고': ['광고', '마케팅', '홍보'],
            '동영상플랫폼': ['OTT', '플랫폼', '스트리밍', '동영상'],
            '방송': ['방송', 'TV', '텔레비전', '라디오'],
            '연예기획사': ['기획사', '연예', '아이돌', '앨범', 'K-POP'],
            '영화': ['영화', '시네마', '영화제작'],
            '웹툰': ['웹툰', '만화', '웹툰플랫폼'],
            '음원': ['음원', '음악', '앨범', '음악서비스'],
            '출판': ['출판', '도서', '책'],
            '캐릭터': ['캐릭터', '라이선싱', 'IP'],
        }
    },
    'SEC_IT': {
        'tags': [
            '보안',
            '소프트웨어',
            '양자컴퓨터',
            '인공지능',
            '인터넷',
            '클라우드',
            'IT솔루션구축',
            'HR플랫폼',
            '구인구직',
        ],
        'keywords_map': {
            '보안': ['보안', '사이버보안', '정보보안'],
            '소프트웨어': ['소프트웨어', 'SW', '애플리케이션'],
            '양자컴퓨터': ['양자컴퓨터', '양자컴퓨팅'],
            '인공지능': ['AI', '인공지능', '머신러닝'],
            '인터넷': ['인터넷', '온라인', '플랫폼'],
            '클라우드': ['클라우드', '클라우드컴퓨팅', 'SaaS'],
            'IT솔루션구축': ['IT솔루션', '시스템구축', 'SI'],
            'HR플랫폼': ['HR', '인사', '인력관리', 'HR플랫폼', '인사관리'],
            '구인구직': ['구인구직', '채용', '인재채용', '구직', '구인', '채용플랫폼'],
        }
    },
    'SEC_AUTO': {
        'tags': [
            '수소차',
            '오토바이',
            '자동차부품',
            '자동차브랜드',
            '자동차유통',
            '전기차',
            '전기차부품',
        ],
        'keywords_map': {
            '수소차': ['수소차', '수소연료전지', 'FCEV'],
            '오토바이': ['오토바이', '모터사이클'],
            '자동차부품': ['자동차부품', '부품', '자동차부품'],
            '자동차브랜드': ['자동차', '완성차', '브랜드'],
            '자동차유통': ['자동차유통', '자동차판매', '딜러'],
            '전기차': ['전기차', 'EV', '전기자동차'],
            '전기차부품': ['전기차부품', 'EV부품', '전장부품'],
        }
    },
    'SEC_MACH': {
        'tags': [
            '공작기계',
            '산업기계',
            '전력기기',
            '건설기계',
            '중장비',
            '펌프',
            '발전기',
            '농업용기계',
        ],
        'keywords_map': {
            '공작기계': ['공작기계', 'CNC', '절삭기계'],
            '산업기계': ['산업기계', '산업용기계'],
            '전력기기': ['전력기기', '변압기', '전기기기'],
            '건설기계': ['건설기계', '중장비', '건설장비'],
            '중장비': ['중장비', '건설중장비'],
            '펌프': ['펌프', '수 Pump'],
            '발전기': ['발전기', '발전설비'],
            '농업용기계': ['농기계', '트랙터'],
        }
    },
    'SEC_STEEL': {
        'tags': [
            '강판',
            '강재',
            '제철',
            '비철금속',
            '구리',
            '알루미늄',
        ],
        'keywords_map': {
            '강판': ['강판', '철강판'],
            '강재': ['강재', '형강'],
            '제철': ['제철', '제철소'],
            '비철금속': ['비철금속', '구리', '알루미늄'],
            '구리': ['구리', '동', 'copper'],
            '알루미늄': ['알루미늄', '알루미늄', 'aluminum'],
        }
    },
    'SEC_CHEM': {
        'tags': [
            '석유화학',
            '정유',
            'LNG',
            '폴리머',
            '수지',
            '플라스틱',
            '합성섬유',
        ],
        'keywords_map': {
            '석유화학': ['석유화학', '페트로케미컬'],
            '정유': ['정유', '정유소'],
            'LNG': ['LNG', '액화천연가스'],
            '폴리머': ['폴리머', '고분자'],
            '수지': ['수지', '레진'],
            '플라스틱': ['플라스틱', '합성수지'],
            '합성섬유': ['합성섬유', '화학섬유'],
        }
    },
    'SEC_SHIP': {
        'tags': [
            '조선소',
            '해운',
            '컨테이너선',
            'LNG선',
            '유조선',
            '해상운송',
        ],
        'keywords_map': {
            '조선소': ['조선소', '조선', '선박제조'],
            '해운': ['해운', '해운사', '선박운항'],
            '컨테이너선': ['컨테이너선', '컨테이너'],
            'LNG선': ['LNG선', 'LNG운반선'],
            '유조선': ['유조선', '탱커'],
            '해상운송': ['해상운송', '해운운송'],
        }
    },
    'SEC_DEFENSE': {
        'tags': [
            '무기',
            '항공기',
            '미사일',
            '레이더',
            '위성',
            '우주',
            'KFX',
        ],
        'keywords_map': {
            '무기': ['무기', '방산무기'],
            '항공기': ['항공기', '전투기', '헬리콥터'],
            '미사일': ['미사일', '로켓'],
            '레이더': ['레이더', '레이더시스템'],
            '위성': ['위성', '인공위성'],
            '우주': ['우주', '우주개발'],
            'KFX': ['KFX', '한국형전투기'],
        }
    },
    'SEC_UTIL': {
        'tags': [
            '전기',
            '가스',
            '수도',
            '열',
            '발전',
            '송전',
            '배전',
            '전력',
            '원자력',
            '신재생에너지',
            '태양광',
            '풍력',
            '환경',
        ],
        'keywords_map': {
            '전기': ['전기', '전력', '전력공급'],
            '가스': ['가스', '도시가스', 'LPG'],
            '수도': ['수도', '상수도', '하수도'],
            '열': ['열', '지역난방', '열공급'],
            '발전': ['발전', '발전소'],
            '송전': ['송전', '송전선'],
            '배전': ['배전', '배전선'],
            '전력': ['전력', '전기'],
            '원자력': ['원자력', '원전', '핵발전'],
            '신재생에너지': ['신재생에너지', '재생에너지'],
            '태양광': ['태양광', '태양광발전'],
            '풍력': ['풍력', '풍력발전'],
            '환경': ['환경', '환경기술'],
        }
    },
    'SEC_TELECOM': {
        'tags': [
            '이동통신',
            '인터넷',
            '5G',
            '6G',
            '통신사',
            '통신망',
        ],
        'keywords_map': {
            '이동통신': ['이동통신', '무선통신'],
            '인터넷': ['인터넷', '인터넷서비스'],
            '5G': ['5G', '5세대통신'],
            '6G': ['6G', '6세대통신'],
            '통신사': ['통신사', '통신업체'],
            '통신망': ['통신망', '네트워크'],
        }
    },
    'SEC_TIRE': {
        'tags': [
            '타이어',
            '고무',
            '합성고무',
        ],
        'keywords_map': {
            '타이어': ['타이어', '타이어제조'],
            '고무': ['고무', '천연고무'],
            '합성고무': ['합성고무', '합성고무'],
        }
    },
    'SEC_COSMETIC': {
        'tags': [
            '화장품브랜드',
            '화장품제조',
        ],
        'keywords_map': {
            '화장품브랜드': ['화장품', '뷰티', '코스메틱', '브랜드'],
            '화장품제조': ['화장품', '제조', 'OEM'],
        }
    },
    'SEC_CONSUMER': {
        'tags': [
            '가구',
            '인테리어',
            '렌탈',
            '가전렌탈',
            '소비재',
            '생활용품',
        ],
        'keywords_map': {
            '가구': ['가구', '가구제조'],
            '인테리어': ['인테리어', '인테리어서비스'],
            '렌탈': ['렌탈', '렌탈서비스'],
            '가전렌탈': ['가전렌탈', '가전렌탈'],
            '소비재': ['소비재', '소비재'],
            '생활용품': ['생활용품', '생활용품'],
        }
    },
    'SEC_MEDDEV': {
        'tags': [
            '임플란트',
            '미용기기',
            '진단기기',
            '수술기기',
            '의료장비',
        ],
        'keywords_map': {
            '임플란트': ['임플란트', '치과임플란트'],
            '미용기기': ['미용기기', '미용장비'],
            '진단기기': ['진단기기', '의료진단기기'],
            '수술기기': ['수술기기', '의료수술기기'],
            '의료장비': ['의료장비', '의료기기'],
        }
    },
    'SEC_EDU': {
        'tags': [
            '교육서비스',
            '에듀테크',
            '입시학원',
            '교육출판',
        ],
        'keywords_map': {
            '교육서비스': ['교육', '교육서비스', '학원'],
            '에듀테크': ['에듀테크', '교육기술'],
            '입시학원': ['입시학원', '학원'],
            '교육출판': ['교육출판', '교과서', '참고서'],
        }
    },
    'SEC_FOOD': {
        'tags': [
            '간편식',
            '과자',
            '닭고기',
            '담배',
            '대마초',
            '돼지고기',
            '밀가루',
            '빵',
            '사료',
            '설탕',
            '수산물',
            '술',
            '식자재유통',
            '식품첨가물',
            '식품포장재',
            '아이스크림',
            '음료',
            '음식배달플랫폼',
            '음식점브랜드',
            '농산물종자',
            '비료와농약',
        ],
        'keywords_map': {
            '간편식': ['간편식', '냉동식품', '즉석식품'],
            '과자': ['과자', '스낵', '쿠키'],
            '닭고기': ['닭고기', '치킨', '계란'],
            '담배': ['담배', '니코틴'],
            '대마초': ['대마초', '마리화나'],
            '돼지고기': ['돼지고기', '돈육'],
            '밀가루': ['밀가루', '밀', '곡물'],
            '빵': ['빵', '베이커리'],
            '사료': ['사료', '동물사료'],
            '설탕': ['설탕', '당'],
            '수산물': ['수산물', '어류', '해산물'],
            '술': ['술', '주류', '맥주', '소주'],
            '식자재유통': ['식자재', '유통'],
            '식품첨가물': ['식품첨가물', '첨가물'],
            '식품포장재': ['포장재', '포장'],
            '아이스크림': ['아이스크림'],
            '음료': ['음료', '탄산음료', '주스'],
            '음식배달플랫폼': ['배달', '배달플랫폼', '배달앱'],
            '음식점브랜드': ['음식점', '레스토랑', '프랜차이즈'],
            '농산물종자': ['종자', '씨앗', '육종', '농산물종자'],
            '비료와농약': ['비료', '농약', '살충제'],
        }
    },
}


# =============================================================================
# Helper Functions
# =============================================================================

def get_l3_tag_candidates(sector_l1: str) -> list:
    """
    L1 섹터에 대한 L3 태그 후보 리스트 반환
    
    Args:
        sector_l1: L1 섹터 코드 (예: 'SEC_SEMI')
    
    Returns:
        태그 리스트 (예: ['반도체부품소재', '반도체장비'])
    """
    if sector_l1 in SECTOR_L3_TAG_CANDIDATES:
        return SECTOR_L3_TAG_CANDIDATES[sector_l1].get('tags', [])
    return []


def get_l3_keywords_map(sector_l1: str) -> dict:
    """
    L1 섹터에 대한 L3 키워드 매핑 반환
    
    Args:
        sector_l1: L1 섹터 코드 (예: 'SEC_SEMI')
    
    Returns:
        키워드 매핑 딕셔너리 (예: {'반도체부품소재': ['반도체소재', '웨이퍼']})
    """
    if sector_l1 in SECTOR_L3_TAG_CANDIDATES:
        return SECTOR_L3_TAG_CANDIDATES[sector_l1].get('keywords_map', {})
    return {}


# =============================================================================
# ECONVAR_MASTER (기존 유지)
# =============================================================================

# ECONVAR_MASTER는 별도 파일로 분리되었거나, 필요시 여기에 정의
# 임시로 빈 딕셔너리로 정의 (실제 사용 시 채워야 함)
ECONVAR_MASTER = {}
