# -*- coding: utf-8 -*-
"""
DRIVEN_BY Edge Builder Script - KG V1.5

기업과 경제변수 간의 인과관계 엣지를 생성합니다.

V1.5 핵심 변경:
1. 메모리 키워드 가드레일: DRAM_ASP/NAND_ASP는 메모리 키워드가 있는 기업에만 연결
2. Mechanism 태깅: INPUT_COST, PRODUCT_PRICE, SPREAD, DEMAND, MACRO_SENSITIVITY
3. Polarity 태깅: POSITIVE, NEGATIVE, MIXED
4. 섹터 예외 규칙 적용
"""

import sys
import os
import codecs
import json
import re
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from datetime import datetime

# 인코딩 설정
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

# 프로젝트 루트 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from app.db import get_db
from sqlalchemy import text
from app.models.sector_reference import (
    SECTOR_L2_DEFINITIONS,
    DRIVER_TAG_KEYWORDS,
    SECTOR_L1_DEFINITIONS,
)

# =============================================================================
# KG V1.5.1 Configuration
# =============================================================================

DRIVEN_BY_RULE_VERSION = "v1.5.1"  # 2024-12-18: Mechanism 룰 레이어 분리, 성장주 금리 보강


# =============================================================================
# ⭐ 개선 1: Mechanism 예외 → 데이터 기반 룰로 분리
# =============================================================================

# (sector_l1, driver_code) 튜플 기반 Mechanism Override
# 하드코딩 대신 데이터로 관리 → 유지보수 용이
MECHANISM_OVERRIDE_RULES = {
    # ==========================================
    # 🔴 필수 수정: 반도체 메모리 = PRODUCT_PRICE
    # 삼성전자/SK하이닉스가 D램을 "팔아서 이익"
    # ==========================================
    ('SEC_SEMI', 'DRAM_ASP'): 'PRODUCT_PRICE',
    ('SEC_SEMI', 'NAND_ASP'): 'PRODUCT_PRICE',
    ('SEC_SEMI', 'HBM_DEMAND'): 'PRODUCT_PRICE',
    ('SEC_SEMI', 'INTEREST_RATE'): 'MACRO_SENSITIVITY',
    ('SEC_SEMI', 'EXCHANGE_RATE_USD_KRW'): 'MACRO_SENSITIVITY',
    
    # ==========================================
    # 🔴 필수 수정: 금융 섹터 = PRODUCT_PRICE
    # 은행/보험/금융지주는 금리 상승 시 NIM(예대마진) 확대
    # ==========================================
    ('SEC_BANK', 'INTEREST_RATE'): 'PRODUCT_PRICE',
    ('SEC_INSURANCE', 'INTEREST_RATE'): 'PRODUCT_PRICE',
    ('SEC_INS', 'INTEREST_RATE'): 'PRODUCT_PRICE',  # 보험 약어
    ('SEC_FINANCE', 'INTEREST_RATE'): 'PRODUCT_PRICE',
    ('SEC_HOLDING', 'INTEREST_RATE'): 'PRODUCT_PRICE',  # 금융지주 (KB금융, 신한지주)
    ('SEC_SEC', 'INTEREST_RATE'): 'MACRO_SENSITIVITY',  # 증권은 혼합
    ('SEC_CARD', 'INTEREST_RATE'): 'INPUT_COST',  # 카드는 자금조달 비용
    
    # ==========================================
    # 정유/화학 = SPREAD (제품가격 - 원유가격)
    # ==========================================
    ('SEC_CHEM', 'OIL_PRICE'): 'SPREAD',
    ('SEC_CHEM', 'NAPHTHA_PRICE'): 'SPREAD',
    ('SEC_OIL', 'OIL_PRICE'): 'PRODUCT_PRICE',
    
    # ==========================================
    # 항공/해운 = INPUT_COST (연료비 부담)
    # ==========================================
    ('SEC_TRAVEL', 'OIL_PRICE'): 'INPUT_COST',
    ('SEC_SHIP', 'OIL_PRICE'): 'INPUT_COST',
    
    # ==========================================
    # 🔴 개선 3: 성장주 금리 민감도 (SEC_GAME/SEC_ENT/SEC_IT)
    # 미래 현금흐름 할인 → 할인율(금리) 민감
    # ==========================================
    ('SEC_IT', 'INTEREST_RATE'): 'MACRO_SENSITIVITY',
    ('SEC_BIO', 'INTEREST_RATE'): 'MACRO_SENSITIVITY',
    ('SEC_GAME', 'INTEREST_RATE'): 'MACRO_SENSITIVITY',
    ('SEC_ENT', 'INTEREST_RATE'): 'MACRO_SENSITIVITY',
}

# (sector_l1, driver_code) 튜플 기반 Polarity Override
POLARITY_OVERRIDE_RULES = {
    # ==========================================
    # 반도체: D램 가격/환율 상승 = POSITIVE
    # ==========================================
    ('SEC_SEMI', 'DRAM_ASP'): 'POSITIVE',
    ('SEC_SEMI', 'NAND_ASP'): 'POSITIVE',
    ('SEC_SEMI', 'HBM_DEMAND'): 'POSITIVE',
    ('SEC_SEMI', 'EXCHANGE_RATE_USD_KRW'): 'POSITIVE',
    
    # ==========================================
    # 금융: 금리 상승 = POSITIVE (NIM 확대)
    # ==========================================
    ('SEC_BANK', 'INTEREST_RATE'): 'POSITIVE',
    ('SEC_INSURANCE', 'INTEREST_RATE'): 'POSITIVE',
    ('SEC_INS', 'INTEREST_RATE'): 'POSITIVE',  # 보험 약어
    ('SEC_FINANCE', 'INTEREST_RATE'): 'POSITIVE',
    ('SEC_HOLDING', 'INTEREST_RATE'): 'POSITIVE',  # 금융지주 (KB금융, 신한지주)
    ('SEC_CARD', 'INTEREST_RATE'): 'NEGATIVE',  # 카드는 자금조달 비용 증가
    
    # ==========================================
    # 정유: 유가 상승 = POSITIVE
    # ==========================================
    ('SEC_OIL', 'OIL_PRICE'): 'POSITIVE',
    
    # ==========================================
    # 수출 기업: 환율 상승 = POSITIVE
    # ==========================================
    ('SEC_AUTO', 'EXCHANGE_RATE_USD_KRW'): 'POSITIVE',
    ('SEC_SHIP', 'EXCHANGE_RATE_USD_KRW'): 'POSITIVE',
    
    # ==========================================
    # 🔴 개선 3: 성장주 금리 상승 = NEGATIVE
    # ==========================================
    ('SEC_IT', 'INTEREST_RATE'): 'NEGATIVE',
    ('SEC_BIO', 'INTEREST_RATE'): 'NEGATIVE',
    ('SEC_GAME', 'INTEREST_RATE'): 'NEGATIVE',
    ('SEC_ENT', 'INTEREST_RATE'): 'NEGATIVE',
}


# =============================================================================
# ⭐ 개선 2: 메모리 가드레일 (아키텍트 판단 반영)
# =============================================================================
# 한미반도체(장비) 등도 D램 가격에 간접 민감 → 연결 유지 (Acceptable)
# 단, FOUNDRY(비메모리 파운드리)만 제외

MEMORY_KEYWORDS = [
    '메모리', 'DRAM', 'NAND', 'D램', '낸드', 'Memory', 'HBM', 
    '고대역폭', '플래시', 'Flash', 'SSD', '메모리반도체', 
    'DDR', 'LPDDR', 'eMCP', 'UFS'
]

MEMORY_DRIVERS = ['DRAM_ASP', 'NAND_ASP', 'HBM_DEMAND']

# 비메모리 파운드리만 제외 (장비 업체는 유지)
FOUNDRY_SECTOR_L2 = ['FOUNDRY', 'SYSTEM_IC', 'LOGIC']

def should_apply_memory_driver(
    driver_code: str, 
    biz_summary: str, 
    keywords: list,
    sector_l2: str = None
) -> bool:
    """
    메모리 관련 드라이버 적용 여부 결정
    
    아키텍트 판단:
    - 파운드리(비메모리): 제외 (실제로 D램 가격과 무관)
    - 장비/테스트: 유지 (D램 가격 상승 → CAPEX 증가 → 장비 수주)
    """
    if driver_code not in MEMORY_DRIVERS:
        return True  # 메모리 드라이버가 아니면 통과
    
    # sector_l2가 MEMORY면 무조건 통과 (삼성전자, SK하이닉스)
    if sector_l2 and 'MEMORY' in sector_l2.upper():
        return True
    
    # sector_l2가 FOUNDRY/SYSTEM_IC면 제외 (DB하이텍 등 비메모리)
    if sector_l2 and sector_l2.upper() in FOUNDRY_SECTOR_L2:
        return False
    
    # 텍스트에서 메모리 키워드 검색 (장비/테스트 업체 포함)
    text = (biz_summary or '').upper()
    if keywords:
        text += ' ' + ' '.join(str(k).upper() for k in keywords if k)
    
    return any(kw.upper() in text for kw in MEMORY_KEYWORDS)


# =============================================================================
# ⭐ Phase 2-1: Mechanism & Polarity 정의
# =============================================================================

# Mechanism 유형 정의
MECHANISM_TYPES = {
    'INPUT_COST': 'Cost pressure from input price changes',
    'PRODUCT_PRICE': 'Revenue impact from product price changes',
    'SPREAD': 'Margin impact from price spreads',
    'DEMAND': 'Volume impact from demand changes',
    'MACRO_SENSITIVITY': 'Valuation/financial impact from macro changes',
}

# Polarity 기본값 by mechanism
MECHANISM_DEFAULT_POLARITY = {
    'INPUT_COST': 'NEGATIVE',      # 원가 상승 = 부정적
    'PRODUCT_PRICE': 'POSITIVE',   # 제품가격 상승 = 긍정적
    'SPREAD': 'MIXED',             # 스프레드는 복합적
    'DEMAND': 'POSITIVE',          # 수요 증가 = 긍정적
    'MACRO_SENSITIVITY': 'MIXED',  # 거시변수는 섹터별로 다름
}

# 밸류체인별 기본 mechanism (폴백용)
VALUE_CHAIN_DEFAULT_MECHANISM = {
    'UPSTREAM': 'PRODUCT_PRICE',    # 원자재 기업은 가격 수혜
    'MID_HARD': 'INPUT_COST',       # 제조업은 원가 부담
    'MID_SOFT': 'DEMAND',           # 설계/SW는 수요
    'DOWN_BIZ': 'INPUT_COST',       # 유통은 원가 부담
    'DOWN_SERVICE': 'DEMAND',       # 서비스는 수요
}


def get_mechanism_and_polarity(
    driver_code: str,
    sector_l1: str,
    value_chain: str
) -> Tuple[str, str]:
    """
    드라이버-기업 연결의 mechanism과 polarity 결정
    
    V1.5.1 개선: 데이터 기반 룰 레이어 사용
    
    우선순위:
    1. (sector_l1, driver_code) 튜플 기반 Override Rules (최우선)
    2. 밸류체인 기본값 (폴백)
    """
    # ⭐ 개선 1: 데이터 기반 룰 레이어 (최우선)
    rule_key = (sector_l1, driver_code)
    
    # Mechanism 결정
    if rule_key in MECHANISM_OVERRIDE_RULES:
        mechanism = MECHANISM_OVERRIDE_RULES[rule_key]
    else:
        # 폴백: 밸류체인 기본값
        mechanism = VALUE_CHAIN_DEFAULT_MECHANISM.get(value_chain, 'DEMAND')
    
    # Polarity 결정
    if rule_key in POLARITY_OVERRIDE_RULES:
        polarity = POLARITY_OVERRIDE_RULES[rule_key]
    else:
        # 폴백: mechanism 기본 polarity
        polarity = MECHANISM_DEFAULT_POLARITY.get(mechanism, 'MIXED')
    
    return mechanism, polarity


# =============================================================================
# 섹터-드라이버 매핑 (기존 + 개선)
# =============================================================================

SECTOR_L1_DRIVER_MAPPING = {
    'SEC_SEMI': {
        'recommended': ['DRAM_ASP', 'NAND_ASP', 'HBM_DEMAND', 'SEMICONDUCTOR_CAPEX', 
                        'SEMICONDUCTOR_DEMAND', 'WAFER_DEMAND'],
        'common': ['EXCHANGE_RATE_USD_KRW', 'AI_SERVER_CAPEX', 'IT_HARDWARE_DEMAND'],
    },
    'SEC_BATTERY': {
        'recommended': ['EV_SALES', 'BATTERY_DEMAND', 'LITHIUM_PRICE'],
        'common': ['EXCHANGE_RATE_USD_KRW'],
    },
    'SEC_AUTO': {
        'recommended': ['EV_SALES', 'AUTO_SALES', 'AUTO_PRODUCTION'],
        'common': ['EXCHANGE_RATE_USD_KRW', 'STEEL_PRICE'],
    },
    'SEC_IT': {
        'recommended': ['ENTERPRISE_IT_SPENDING', 'CLOUD_ADOPTION', 'INTEREST_RATE'],
        'common': ['ECONOMIC_GROWTH'],
    },
    'SEC_GAME': {
        # ⭐ 개선 3: 성장주 금리 민감도 추가
        'recommended': ['GAME_MARKET_SIZE', 'MOBILE_GAME_REVENUE', 'INTEREST_RATE'],
        'common': ['DIGITAL_ADOPTION', 'CONSUMER_SPENDING'],
    },
    'SEC_ENT': {
        # ⭐ 개선 3: 성장주 금리 민감도 추가
        'recommended': ['CONTENT_MARKET_SIZE', 'AD_REVENUE', 'INTEREST_RATE'],
        'common': ['CONSUMER_SPENDING', 'DIGITAL_ADOPTION'],
    },
    'SEC_BIO': {
        'recommended': ['PHARMA_RD_SPENDING', 'FDA_APPROVAL', 'INTEREST_RATE'],
        'common': ['HEALTHCARE_SPENDING', 'AGING_POPULATION'],
    },
    'SEC_MEDDEV': {
        'recommended': ['MEDDEV_MARKET_SIZE', 'HEALTHCARE_SPENDING'],
        'common': ['AGING_POPULATION'],
    },
    'SEC_CONST': {
        'recommended': ['CONSTRUCTION_ORDERS', 'HOUSING_STARTS', 'INTEREST_RATE'],
        'common': ['STEEL_PRICE', 'CEMENT_PRICE'],
    },
    'SEC_STEEL': {
        'recommended': ['STEEL_PRICE', 'IRON_ORE_PRICE'],
        'common': ['CONSTRUCTION_ORDERS', 'AUTO_PRODUCTION'],
    },
    'SEC_CHEM': {
        'recommended': ['OIL_PRICE', 'NAPHTHA_PRICE', 'POLYMER_PRICE'],
        'common': ['EXCHANGE_RATE_USD_KRW'],
    },
    'SEC_SHIP': {
        'recommended': ['SHIPBUILDING_ORDERS', 'BDI', 'LNG_PRICE'],
        'common': ['OIL_PRICE', 'GLOBAL_TRADE_VOLUME'],
    },
    'SEC_DEFENSE': {
        'recommended': ['DEFENSE_BUDGET', 'ARMS_EXPORTS'],
        'common': ['GOVERNMENT_SPENDING'],
    },
    'SEC_MACH': {
        'recommended': ['CAPEX_CYCLE', 'INDUSTRIAL_PRODUCTION'],
        'common': ['STEEL_PRICE', 'ECONOMIC_GROWTH'],
    },
    'SEC_ELECTRONICS': {
        'recommended': ['CONSUMER_ELECTRONICS_DEMAND', 'TV_SHIPMENTS'],
        'common': ['CONSUMER_SPENDING', 'EXCHANGE_RATE_USD_KRW'],
    },
    'SEC_COSMETIC': {
        'recommended': ['COSMETIC_MARKET_SIZE', 'CHINA_COSMETIC_IMPORT'],
        'common': ['CONSUMER_SPENDING', 'EXCHANGE_RATE_CNY_KRW'],
    },
    'SEC_TRAVEL': {
        'recommended': ['TOURIST_ARRIVALS', 'AIRLINE_PASSENGERS', 'OIL_PRICE'],
        'common': ['CONSUMER_SPENDING'],
    },
    'SEC_FOOD': {
        'recommended': ['FOOD_CPI', 'GRAIN_PRICE'],
        'common': ['CONSUMER_SPENDING', 'EXCHANGE_RATE_USD_KRW'],
    },
    'SEC_RETAIL': {
        'recommended': ['RETAIL_SALES', 'E_COMMERCE_TRANS_VOL'],
        'common': ['CONSUMER_SPENDING', 'CONSUMER_CONFIDENCE'],
    },
    'SEC_CONSUMER': {
        'recommended': ['CONSUMER_SPENDING', 'HOUSING_MARKET'],
        'common': ['INTEREST_RATE', 'DISPOSABLE_INCOME'],
    },
    'SEC_UTIL': {
        'recommended': ['ELECTRICITY_DEMAND', 'GAS_PRICE'],
        'common': ['ECONOMIC_GROWTH', 'WEATHER'],
    },
    'SEC_TELECOM': {
        'recommended': ['MOBILE_SUBSCRIBERS', 'DATA_TRAFFIC'],
        'common': ['5G_ADOPTION', 'CONSUMER_SPENDING'],
    },
    'SEC_TIRE': {
        'recommended': ['AUTO_SALES', 'RUBBER_PRICE'],
        'common': ['EXCHANGE_RATE_USD_KRW', 'OIL_PRICE'],
    },
    'SEC_FINANCE': {
        'recommended': ['INTEREST_RATE', 'STOCK_MARKET_VOLUME'],
        'common': ['ECONOMIC_GROWTH', 'CONSUMER_CREDIT'],
    },
    'SEC_BANK': {
        'recommended': ['INTEREST_RATE', 'LOAN_DEMAND'],
        'common': ['ECONOMIC_GROWTH', 'CONSUMER_CREDIT'],
    },
    'SEC_INSURANCE': {
        'recommended': ['INTEREST_RATE', 'INSURANCE_PREMIUM'],
        'common': ['ECONOMIC_GROWTH', 'AGING_POPULATION'],
    },
    'SEC_HOLDING': {
        # 금융지주는 금리가 핵심 드라이버
        'recommended': ['INTEREST_RATE', 'ECONOMIC_GROWTH', 'CORPORATE_INVESTMENT'],
        'common': [],
    },
    'SEC_REIT': {
        'recommended': ['INTEREST_RATE', 'REAL_ESTATE_PRICE'],
        'common': ['ECONOMIC_GROWTH'],
    },
    'SEC_EDU': {
        'recommended': ['EDUCATION_SPENDING', 'STUDENT_ENROLLMENT'],
        'common': ['DISPOSABLE_INCOME'],
    },
    'SEC_OIL': {
        'recommended': ['OIL_PRICE', 'REFINING_MARGIN'],
        'common': ['GLOBAL_OIL_DEMAND'],
    },
}

VALUE_CHAIN_DRIVER_BOOST = {
    'UPSTREAM': {
        'boost_drivers': ['OIL_PRICE', 'COMMODITY_PRICE', 'EXCHANGE_RATE_USD_KRW'],
        'weight': 0.3,
    },
    'MID_HARD': {
        'boost_drivers': ['CAPEX_CYCLE', 'CAPACITY_UTILIZATION', 'EXCHANGE_RATE_USD_KRW'],
        'weight': 0.2,
    },
    'MID_SOFT': {
        'boost_drivers': ['ENTERPRISE_IT_SPENDING', 'RD_SPENDING'],
        'weight': 0.2,
    },
    'DOWN_BIZ': {
        'boost_drivers': ['CONSUMER_SPENDING', 'RETAIL_SALES', 'INVENTORY_CYCLE'],
        'weight': 0.2,
    },
    'DOWN_SERVICE': {
        'boost_drivers': ['DIGITAL_ADOPTION', 'MOBILE_PAYMENT_ADOPTION', 'AD_REVENUE'],
        'weight': 0.2,
    },
}

DRIVER_TEXT_KEYWORDS = {
    'EXCHANGE_RATE_USD_KRW': ['환율', '달러', 'USD', '원화', '외화', '수출', '수입'],
    'INTEREST_RATE': ['금리', '기준금리', '이자율', '대출금리'],
    'OIL_PRICE': ['유가', '원유', '석유', '정유', '휘발유', '경유'],
    'DRAM_ASP': ['DRAM', '메모리', '반도체가격', 'ASP'],
    'NAND_ASP': ['NAND', '낸드', '플래시메모리', 'SSD'],
    'HBM_DEMAND': ['HBM', '고대역폭메모리', 'AI반도체', 'GPU메모리'],
    'EV_SALES': ['전기차', 'EV', '전기자동차', '친환경차'],
    'BATTERY_DEMAND': ['배터리', '2차전지', '리튬이온', '셀'],
    'STEEL_PRICE': ['철강', '강철', '철근', '형강'],
    'CONSUMER_SPENDING': ['소비', '소비지출', '가계소비', '소비자'],
    'RETAIL_SALES': ['소매', '유통', '판매', '매출'],
    'SEMICONDUCTOR_CAPEX': ['반도체투자', '설비투자', 'CAPEX', '시설투자'],
    'AI_SERVER_CAPEX': ['AI서버', 'GPU', '데이터센터', 'AI인프라'],
    'CONSTRUCTION_ORDERS': ['건설수주', '수주', '착공', '분양'],
    'HOUSING_STARTS': ['주택착공', '아파트', '주택', '부동산'],
    'HEALTHCARE_SPENDING': ['의료비', '건강보험', '의료', '헬스케어'],
    'CLOUD_ADOPTION': ['클라우드', 'SaaS', 'AWS', '클라우드컴퓨팅'],
    'E_COMMERCE_TRANS_VOL': ['이커머스', '온라인쇼핑', '전자상거래', '온라인판매'],
    'MOBILE_PAYMENT_ADOPTION': ['모바일결제', '간편결제', '페이', '결제'],
    'ECONOMIC_GROWTH': ['GDP', '경제성장', '경기', '성장률'],
}


def get_drivers_for_sector(sector_l1: str, sector_l2: str = None) -> Tuple[List[str], List[str]]:
    """섹터에 해당하는 드라이버 목록 반환"""
    recommended = []
    common = []
    
    if sector_l1 in SECTOR_L1_DRIVER_MAPPING:
        l1_mapping = SECTOR_L1_DRIVER_MAPPING[sector_l1]
        recommended = list(l1_mapping.get('recommended', []))
        common = list(l1_mapping.get('common', []))
    
    if sector_l1 in SECTOR_L2_DEFINITIONS and sector_l2:
        l2_def = SECTOR_L2_DEFINITIONS[sector_l1].get(sector_l2, {})
        l2_recommended = l2_def.get('recommended_drivers', [])
        l2_common = l2_def.get('common_drivers', [])
        
        for d in l2_recommended:
            if d not in recommended:
                recommended.append(d)
        for d in l2_common:
            if d not in common and d not in recommended:
                common.append(d)
    
    return recommended, common


def calculate_text_weight(biz_summary: str, keywords: list, driver_code: str) -> float:
    """텍스트에서 드라이버 관련 키워드 매칭으로 가중치 계산"""
    if driver_code not in DRIVER_TEXT_KEYWORDS:
        return 0.0
    
    text = (biz_summary or '').lower()
    if keywords:
        text += ' ' + ' '.join(str(k).lower() for k in keywords if k)
    
    driver_keywords = DRIVER_TEXT_KEYWORDS[driver_code]
    match_count = sum(1 for kw in driver_keywords if kw.lower() in text)
    
    if match_count == 0:
        return 0.0
    elif match_count == 1:
        return 0.3
    elif match_count == 2:
        return 0.5
    else:
        return 0.7


def build_driven_by_edges(db, dry_run: bool = False) -> Dict:
    """
    DRIVEN_BY 엣지 생성 (V1.5)
    
    V1.5 핵심 변경:
    1. 메모리 키워드 가드레일
    2. Mechanism/Polarity 태깅
    """
    print('=' * 70)
    print('Building DRIVEN_BY Edges (V1.5)')
    print('=' * 70)
    
    # 1. 기존 DRIVEN_BY 엣지 삭제
    if not dry_run:
        db.execute(text("DELETE FROM edges WHERE relation_type = 'DRIVEN_BY'"))
        db.commit()
        print('Deleted existing DRIVEN_BY edges.')
    
    # 2. 모든 기업 조회
    result = db.execute(text('''
        SELECT 
            i.ticker,
            i.sector_l1,
            i.sector_l2,
            i.value_chain,
            cd.biz_summary,
            cd.keywords
        FROM investor_sector i
        LEFT JOIN company_details cd ON i.ticker = cd.ticker
        WHERE i.is_primary = true
    '''))
    
    companies = list(result)
    print(f'Total companies to process: {len(companies)}')
    
    # 3. 엣지 생성
    edges_to_insert = []
    driver_stats = defaultdict(int)
    sector_stats = defaultdict(int)
    memory_filtered_count = 0  # 메모리 가드레일로 필터된 수
    
    for ticker, sector_l1, sector_l2, value_chain, biz_summary, keywords_json in companies:
        keywords = []
        if keywords_json:
            try:
                keywords = keywords_json if isinstance(keywords_json, list) else json.loads(keywords_json)
            except:
                pass
        
        recommended, common = get_drivers_for_sector(sector_l1, sector_l2)
        
        vc_boost = VALUE_CHAIN_DRIVER_BOOST.get(value_chain, {})
        boost_drivers = vc_boost.get('boost_drivers', [])
        boost_weight = vc_boost.get('weight', 0)
        
        all_drivers = set(recommended + common + boost_drivers)
        
        for driver_code in all_drivers:
            # ⭐ Phase 1-1: 메모리 키워드 가드레일
            if not should_apply_memory_driver(driver_code, biz_summary, keywords, sector_l2):
                memory_filtered_count += 1
                continue
            
            # 기본 weight 결정
            if driver_code in recommended:
                base_weight = 0.8
            elif driver_code in common:
                base_weight = 0.5
            elif driver_code in boost_drivers:
                base_weight = boost_weight
            else:
                base_weight = 0.3
            
            text_weight = calculate_text_weight(biz_summary, keywords, driver_code)
            final_weight = min(1.0, base_weight + text_weight * 0.3)
            
            # ⭐ Phase 2-1: Mechanism/Polarity 태깅
            mechanism, polarity = get_mechanism_and_polarity(driver_code, sector_l1, value_chain)
            
            edge_id = f"{ticker}_{driver_code}_DRIVEN_BY"
            
            edge_data = {
                'id': edge_id,
                'source_type': 'COMPANY',
                'source_id': ticker,
                'target_type': 'ECONVAR',
                'target_id': driver_code,
                'relation_type': 'DRIVEN_BY',
                'weight': round(final_weight, 3),
                'properties': json.dumps({
                    'source': 'sector_mapping' if driver_code in (recommended + common) else 'value_chain_boost',
                    'sector_l1': sector_l1,
                    'sector_l2': sector_l2,
                    'value_chain': value_chain,
                    'text_match_weight': round(text_weight, 3),
                    # ⭐ V1.5: Mechanism/Polarity
                    'mechanism': mechanism,
                    'polarity': polarity,
                    # 메타데이터
                    'rule_version': DRIVEN_BY_RULE_VERSION,
                    'evidence_type': 'RULE',
                    'created_at': datetime.utcnow().isoformat(),
                }),
            }
            edges_to_insert.append(edge_data)
            
            driver_stats[driver_code] += 1
            sector_stats[sector_l1] += 1
    
    print(f'Total edges to create: {len(edges_to_insert)}')
    print(f'Memory driver connections filtered: {memory_filtered_count}')
    
    # 4. 배치 삽입
    if not dry_run and edges_to_insert:
        batch_size = 1000
        for i in range(0, len(edges_to_insert), batch_size):
            batch = edges_to_insert[i:i+batch_size]
            
            for edge in batch:
                db.execute(text('''
                    INSERT INTO edges (id, source_type, source_id, target_type, target_id, relation_type, weight, properties)
                    VALUES (:id, :source_type, :source_id, :target_type, :target_id, :relation_type, :weight, :properties)
                    ON CONFLICT (id) DO UPDATE SET weight = EXCLUDED.weight, properties = EXCLUDED.properties
                '''), edge)
            
            db.commit()
            print(f'  Inserted batch {i//batch_size + 1}: {len(batch)} edges')
    
    # 5. 통계 출력
    print()
    print('=' * 70)
    print('DRIVEN_BY Edge Statistics (V1.5.1)')
    print('=' * 70)
    
    print('\nTop 15 Drivers by Edge Count:')
    for driver, count in sorted(driver_stats.items(), key=lambda x: -x[1])[:15]:
        print(f'  {driver}: {count}')
    
    print('\nEdges by Sector:')
    for sector, count in sorted(sector_stats.items(), key=lambda x: -x[1]):
        print(f'  {sector}: {count}')
    
    return {
        'total_edges': len(edges_to_insert),
        'unique_drivers': len(driver_stats),
        'sectors_covered': len(sector_stats),
        'memory_filtered': memory_filtered_count,
        'driver_stats': dict(driver_stats),
        'sector_stats': dict(sector_stats),
    }


def verify_driven_by_edges(db):
    """DRIVEN_BY 엣지 검증 (V1.5)"""
    print()
    print('=' * 70)
    print('DRIVEN_BY Edge Verification (V1.5)')
    print('=' * 70)
    
    # 1. 총 개수
    result = db.execute(text('''
        SELECT COUNT(*) FROM edges WHERE relation_type = 'DRIVEN_BY'
    '''))
    total = result.fetchone()[0]
    print(f'Total DRIVEN_BY edges: {total}')
    
    # 2. Mechanism 분포
    result = db.execute(text('''
        SELECT 
            properties->>'mechanism' as mechanism,
            COUNT(*) as cnt
        FROM edges
        WHERE relation_type = 'DRIVEN_BY'
        GROUP BY mechanism
        ORDER BY cnt DESC
    '''))
    print('\nMechanism Distribution:')
    for row in result:
        print(f'  {row[0]}: {row[1]}')
    
    # 3. Polarity 분포
    result = db.execute(text('''
        SELECT 
            properties->>'polarity' as polarity,
            COUNT(*) as cnt
        FROM edges
        WHERE relation_type = 'DRIVEN_BY'
        GROUP BY polarity
        ORDER BY cnt DESC
    '''))
    print('\nPolarity Distribution:')
    for row in result:
        print(f'  {row[0]}: {row[1]}')
    
    # 4. 메모리 드라이버 연결 기업 확인
    print('\n--- Memory Driver Verification ---')
    for mem_driver in MEMORY_DRIVERS:
        result = db.execute(text('''
            SELECT e.source_id, s.stock_name
            FROM edges e
            JOIN stocks s ON e.source_id = s.ticker
            WHERE e.target_id = :driver AND e.relation_type = 'DRIVEN_BY'
            LIMIT 5
        '''), {'driver': mem_driver})
        companies = list(result)
        print(f'\n{mem_driver} connected to {len(companies)} sample companies:')
        for ticker, name in companies:
            print(f'  {ticker} ({name})')
    
    # 5. 비메모리 기업 검증 (DB하이텍, 한미반도체)
    print('\n--- Non-Memory Company Check ---')
    non_memory_tickers = ['000990', '042700', '039030']  # DB하이텍, 한미반도체, 이오테크닉스
    
    for ticker in non_memory_tickers:
        result = db.execute(text('''
            SELECT e.target_id, s.stock_name
            FROM edges e
            JOIN stocks s ON e.source_id = s.ticker
            WHERE e.source_id = :ticker 
            AND e.relation_type = 'DRIVEN_BY'
            AND e.target_id IN ('DRAM_ASP', 'NAND_ASP', 'HBM_DEMAND')
        '''), {'ticker': ticker})
        
        memory_edges = list(result)
        result2 = db.execute(text('SELECT stock_name FROM stocks WHERE ticker = :ticker'), {'ticker': ticker})
        name_row = result2.fetchone()
        name = name_row[0] if name_row else ticker
        
        if memory_edges:
            print(f'  [WARNING] {ticker} ({name}): {len(memory_edges)} memory driver(s) connected')
        else:
            print(f'  [OK] {ticker} ({name}): No memory drivers')


def verify_mechanism_polarity(db):
    """Mechanism/Polarity 검증 (V1.5.1)"""
    print()
    print('=' * 70)
    print('Mechanism/Polarity Verification (V1.5.1)')
    print('=' * 70)
    
    # 주요 기업의 mechanism/polarity 확인
    test_cases = [
        # 🔴 필수 수정: 반도체 메모리 = PRODUCT_PRICE
        ('005930', 'Samsung Electronics', 'DRAM_ASP', 'PRODUCT_PRICE', 'POSITIVE'),
        ('000660', 'SK Hynix', 'DRAM_ASP', 'PRODUCT_PRICE', 'POSITIVE'),
        ('000660', 'SK Hynix', 'HBM_DEMAND', 'PRODUCT_PRICE', 'POSITIVE'),
        
        # 🔴 필수 수정: 금융 섹터 = PRODUCT_PRICE/POSITIVE
        ('105560', 'KB Financial', 'INTEREST_RATE', 'PRODUCT_PRICE', 'POSITIVE'),
        ('055550', 'Shinhan Financial', 'INTEREST_RATE', 'PRODUCT_PRICE', 'POSITIVE'),
        
        # 정유/화학 = SPREAD
        ('010950', 'S-Oil', 'OIL_PRICE', 'SPREAD', 'MIXED'),
        
        # 항공 = INPUT_COST/NEGATIVE
        ('003490', 'Korean Air', 'OIL_PRICE', 'INPUT_COST', 'NEGATIVE'),
        
        # 🔴 개선 3: 성장주 금리 = MACRO_SENSITIVITY/NEGATIVE
        ('035420', 'NAVER', 'INTEREST_RATE', 'MACRO_SENSITIVITY', 'NEGATIVE'),
        ('035720', 'Kakao', 'INTEREST_RATE', 'MACRO_SENSITIVITY', 'NEGATIVE'),
        ('259960', 'Krafton', 'INTEREST_RATE', 'MACRO_SENSITIVITY', 'NEGATIVE'),
    ]
    
    print('\n[Critical Fixes Verification]')
    all_passed = True
    
    for ticker, name, driver, expected_mech, expected_pol in test_cases:
        result = db.execute(text('''
            SELECT properties->>'mechanism' as mech, properties->>'polarity' as pol
            FROM edges
            WHERE source_id = :ticker AND target_id = :driver AND relation_type = 'DRIVEN_BY'
        '''), {'ticker': ticker, 'driver': driver})
        
        row = result.fetchone()
        if row:
            actual_mech, actual_pol = row
            mech_ok = actual_mech == expected_mech
            pol_ok = actual_pol == expected_pol
            mech_mark = '✓' if mech_ok else '✗'
            pol_mark = '✓' if pol_ok else '✗'
            status = '✅ PASS' if (mech_ok and pol_ok) else '❌ FAIL'
            
            if not (mech_ok and pol_ok):
                all_passed = False
            
            print(f'  {status} {ticker} ({name}) + {driver}:')
            print(f'         Mechanism: {expected_mech} vs {actual_mech} [{mech_mark}]')
            print(f'         Polarity: {expected_pol} vs {actual_pol} [{pol_mark}]')
        else:
            all_passed = False
            print(f'  ❌ FAIL {ticker} ({name}) + {driver}: NOT FOUND')
    
    print()
    if all_passed:
        print('🎉 All critical fixes verified successfully!')
    else:
        print('⚠️  Some tests failed. Please review the rules.')


def main():
    print('=' * 70)
    print('DRIVEN_BY Edge Builder (V1.5.1)')
    print('=' * 70)
    print()
    print('개선 사항:')
    print('  1. Mechanism 예외 → 데이터 기반 룰 레이어로 분리')
    print('  2. 반도체 메모리 PRODUCT_PRICE 수정')
    print('  3. 금융 섹터 PRODUCT_PRICE/POSITIVE 수정')
    print('  4. 성장주(게임/엔터/IT) 금리 민감도 보강')
    print()
    
    db = next(get_db())
    
    # 1. 엣지 생성
    stats = build_driven_by_edges(db, dry_run=False)
    
    # 2. 검증
    verify_driven_by_edges(db)
    verify_mechanism_polarity(db)
    
    print()
    print('=' * 70)
    print('DRIVEN_BY Edge Building Complete (V1.5.1)!')
    print('=' * 70)
    print(f'Total edges created: {stats["total_edges"]}')
    print(f'Unique drivers: {stats["unique_drivers"]}')
    print(f'Sectors covered: {stats["sectors_covered"]}')
    print(f'Memory connections filtered: {stats["memory_filtered"]}')


if __name__ == '__main__':
    main()
