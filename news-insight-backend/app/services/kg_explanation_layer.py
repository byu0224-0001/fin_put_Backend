# -*- coding: utf-8 -*-
"""
KG Explanation Layer - V1.5.1

KG 데이터를 사용자가 이해할 수 있는 자연어 문장으로 변환합니다.

핵심 기능:
1. Mechanism → 문장 템플릿 (고정된 투자 언어)
2. 2-hop 경로 → 스토리텔링 (논리적 흐름)
3. MIXED → 양면성 분해 (호재/악재 분리)
"""

import sys
import codecs

# 인코딩 설정 (Windows 콘솔용)
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

from typing import Dict, List, Tuple, Optional


# =============================================================================
# ⭐ Mechanism → 문장 템플릿 (고정)
# =============================================================================

MECHANISM_TEMPLATES = {
    'PRODUCT_PRICE': {
        'POSITIVE': {
            'short': '판가 상승 수혜',
            'medium': '해당 변수 상승 시 제품 판매 가격(판가) 상승으로 이어져 수익성에 긍정적입니다.',
            'full': '{variable} 상승은 {company}의 **제품 판매 가격(판가) 상승**으로 이어져, 매출 증가와 수익성 개선에 **긍정적**입니다.',
        },
        'NEGATIVE': {
            'short': '판가 하락 우려',
            'medium': '해당 변수 하락 시 제품 판매 가격(판가) 하락으로 수익성에 부정적입니다.',
            'full': '{variable} 하락은 {company}의 **제품 판매 가격(판가) 하락**으로 이어져, 매출 감소와 수익성 악화에 **부정적**입니다.',
        },
    },
    'INPUT_COST': {
        'POSITIVE': {  # 드문 케이스: 원가 하락 시 수혜
            'short': '원가 절감 수혜',
            'medium': '해당 변수 하락 시 원가 부담이 감소하여 수익성에 긍정적입니다.',
            'full': '{variable} 하락은 {company}의 **원가 부담 감소**로 이어져, 이익률 개선에 **긍정적**입니다.',
        },
        'NEGATIVE': {
            'short': '원가 부담 증가',
            'medium': '해당 변수 상승 시 원가 부담이 증가하여 수익성에 부정적입니다.',
            'full': '{variable} 상승은 {company}의 **원가 부담 증가**로 이어져, 이익률 악화에 **부정적**입니다.',
        },
    },
    'SPREAD': {
        'MIXED': {
            'short': '스프레드 영향 (양면성)',
            'medium': '원가와 판가가 동시에 변해 정제/마진 스프레드에 따라 영향이 달라질 수 있습니다.',
            'full': '{variable} 변동은 {company}에 **양면적 영향**을 줍니다. 단기적으로 **재고평가 이익(호재)**이 발생할 수 있으나, 장기적으로는 **원가 부담(악재)**으로 작용할 수 있습니다.',
            # MIXED 분해용 상세 템플릿
            'positive_aspect': '단기적으로 **재고평가 이익** 및 **제품 가격 전가력**으로 수혜 가능',
            'negative_aspect': '장기적으로 **원가 부담 증가** 및 **수요 위축** 우려',
        },
    },
    'DEMAND': {
        'POSITIVE': {
            'short': '수요 증가 수혜',
            'medium': '해당 변수 증가 시 제품/서비스 수요가 늘어나 매출 성장에 긍정적입니다.',
            'full': '{variable} 증가는 {company}의 **제품/서비스 수요 확대**로 이어져, 매출 성장에 **긍정적**입니다.',
        },
        'NEGATIVE': {
            'short': '수요 감소 우려',
            'medium': '해당 변수 감소 시 제품/서비스 수요가 줄어 매출 성장에 부정적입니다.',
            'full': '{variable} 감소는 {company}의 **제품/서비스 수요 위축**으로 이어져, 매출 성장에 **부정적**입니다.',
        },
    },
    'MACRO_SENSITIVITY': {
        'POSITIVE': {
            'short': '거시 환경 수혜',
            'medium': '해당 거시 변수 변화가 기업 가치평가에 긍정적으로 작용합니다.',
            'full': '{variable} 변화는 {company}의 **기업 가치평가(밸류에이션)**에 **긍정적**으로 작용합니다.',
        },
        'NEGATIVE': {
            'short': '거시 환경 부담',
            'medium': '해당 거시 변수 변화가 기업 가치평가에 부정적으로 작용합니다.',
            'full': '{variable} 상승은 {company}와 같은 **성장주의 밸류에이션에 부담**을 줄 수 있습니다. 미래 현금흐름의 할인율이 높아져 현재 가치가 낮아지기 때문입니다.',
        },
        'MIXED': {
            'short': '거시 환경 복합 영향',
            'medium': '해당 거시 변수 변화가 기업에 복합적인 영향을 줍니다.',
            'full': '{variable} 변화는 {company}에 **복합적인 영향**을 줍니다. 구체적인 영향은 기업의 재무구조와 사업모델에 따라 달라집니다.',
        },
    },
}


# =============================================================================
# ⭐ 경제 변수 한국어 이름 매핑
# =============================================================================

VARIABLE_KOREAN_NAMES = {
    'OIL_PRICE': '유가',
    'DRAM_ASP': 'D램 가격',
    'NAND_ASP': '낸드 가격',
    'HBM_DEMAND': 'HBM(고대역폭 메모리) 수요',
    'INTEREST_RATE': '금리',
    'EXCHANGE_RATE_USD_KRW': '원/달러 환율',
    'INFLATION_CPI': '물가(인플레이션)',
    'SEMICONDUCTOR_DEMAND': '반도체 수요',
    'EV_SALES': '전기차 판매',
    'BATTERY_DEMAND': '배터리 수요',
    'CONSUMER_SPENDING': '소비 지출',
    'STEEL_PRICE': '철강 가격',
    'NAPHTHA_PRICE': '나프타 가격',
    'CONSTRUCTION_ORDERS': '건설 수주',
    'GROWTH_STOCK_VALUATION': '성장주 밸류에이션',
    'LOAN_DEMAND': '대출 수요',
    'RETAIL_SALES': '소매 판매',
    'AI_SERVER_CAPEX': 'AI 서버 투자',
    'LITHIUM_PRICE': '리튬 가격',
}


# =============================================================================
# ⭐ 2-hop 경로 스토리텔링 템플릿
# =============================================================================

# Macro Link 관계 → 연결 문장
MACRO_LINK_CONNECTORS = {
    'PRESSURES': '{source} 상승은 {target}에 상승 압력을 가합니다.',
    'NEGATIVE_PRESSURE': '{source} 상승은 {target}에 하락 압력을 가합니다.',
    'CONTEXT_LINK': '{source}와 {target}은 함께 움직이는 경향이 있습니다.',
}

# 2-hop 스토리 템플릿
TWO_HOP_STORY_TEMPLATE = """
**{scenario_name}**

**1단계 (거시 환경):** {macro_explanation}

**2단계 (기업 영향):** {company_explanation}

**결론:** {conclusion}
"""


def get_variable_korean_name(var_code: str) -> str:
    """경제 변수 코드 → 한국어 이름"""
    return VARIABLE_KOREAN_NAMES.get(var_code, var_code)


# =============================================================================
# ⭐ V1.5.4 - Direct/Indirect Impact 판정 (핵심)
#
# 운영 정책: 동일 섹터 내 DIRECT/INDIRECT 혼재는 복합 사업 구조에 따른 정상 현상이며,
# 예외 리스트로 관리한다.
# (예: SEC_CHEM, SEC_TRAVEL, SEC_CONST, SEC_BIO 일부 기업)
#
# =============================================================================

# Direct 판정 키워드 (원가/판가 직접)
DIRECT_KEYWORDS = {
    'cost': ['원재료', '원유', '나프타', '항공유', '전력비', '연료비', '유류비',
             '매입', '원가율', '매출원가', '구매', '조달'],
    'price': ['판매가격', '판가', '스프레드', '정제마진', '크랙스프레드',
              '제품가격', 'ASP', '평균판매가격', '가격 변동', '가격 연동'],
}

# Indirect 판정 키워드 (거시/경로)
INDIRECT_KEYWORDS = {
    'macro': ['경기', '물가', '금리', '환율', '인플레이션', '통화정책',
              '시장금리', '기준금리', '할인율', '밸류에이션'],
    'demand': ['수요', 'IT지출', 'IT투자', '설비투자', 'CAPEX', '투자심리',
               '기업투자', '경기변동', '업황', '산업수요'],
}

# 섹터별 Direct 변수 매핑 (Fallback 규칙)
SECTOR_DIRECT_VARIABLES = {
    # 유가 직접 섹터
    'SEC_CHEM': ['OIL_PRICE', 'NAPHTHA_PRICE'],
    'SEC_OIL': ['OIL_PRICE', 'NAPHTHA_PRICE'],
    'SEC_ENERGY': ['OIL_PRICE', 'GAS_PRICE'],
    'SEC_TRAVEL': ['OIL_PRICE'],  # 항공: 연료비
    'SEC_SHIP': ['OIL_PRICE'],    # 해운: 연료비
    # 금리 직접 섹터
    'SEC_BANK': ['INTEREST_RATE'],
    'SEC_INSURANCE': ['INTEREST_RATE'],
    'SEC_HOLDING': ['INTEREST_RATE'],
    # 메모리 직접 섹터
    'SEC_SEMI': ['DRAM_ASP', 'NAND_ASP', 'HBM_DEMAND'],
}

# Indirect 채널 템플릿
INDIRECT_CHANNEL_TEMPLATES = {
    'MACRO_POLICY': {
        'path': ['INFLATION', 'INTEREST_RATE'],
        'template': (
            "{variable} 변동은 **물가/금리 환경**에 영향을 주어, "
            "{company}와 같은 **성장주의 밸류에이션**에 간접적으로 작용할 수 있습니다."
        ),
    },
    'DEMAND': {
        'path': ['DEMAND', 'SPENDING', 'CONSUMPTION'],
        'template': (
            "{variable} 변동은 **산업 수요/투자 환경**을 바꿔, "
            "{company}의 **제품 수요 및 업황**에 간접적인 영향을 줄 수 있습니다."
        ),
    },
    'CAPEX': {
        'path': ['CAPEX', 'INVESTMENT', 'IT_SPENDING'],
        'template': (
            "{variable} 변동은 **기업 투자(CAPEX) 환경**에 영향을 주어, "
            "{company}의 **장비/부품 수주**에 간접적으로 작용할 수 있습니다."
        ),
    },
    'FX': {
        'path': ['USD', 'KRW', 'EXCHANGE'],
        'template': (
            "{variable} 변동은 **환율 변화**를 통해 "
            "{company}의 **수출 채산성/원가**에 간접적인 영향을 줄 수 있습니다."
        ),
    },
}

# Indirect Disclaimer
INDIRECT_DISCLAIMER = "※ 직접적인 원가/판가 관계가 아닌 **거시 경로 기반 간접 영향**입니다."


def classify_impact_nature(
    variable: str,
    sector_l1: str,
    mechanism: str,
    evidence_snippets: List[str] = None,
    biz_summary: str = None
) -> Dict:
    """
    Direct/Indirect Impact 판정
    
    Args:
        variable: 경제 변수
        sector_l1: 섹터
        mechanism: 메커니즘
        evidence_snippets: 증거 문장 리스트
        biz_summary: 사업개요 텍스트
    
    Returns:
        {
            'nature': 'DIRECT' | 'INDIRECT' | 'MIXED',
            'reason': '판정 근거',
            'channel': 'MACRO_POLICY' | 'DEMAND' | 'CAPEX' | 'FX' | None,
            'evidence_keywords': ['원재료', '원유', ...]
        }
    """
    # P0 수정: SPREAD는 Impact Nature가 아니라 Polarity 레이어에서 처리
    # SPREAD 메커니즘은 DIRECT/INDIRECT 판정과 무관하며, Polarity(MIXED)로 처리됨
    # 따라서 여기서는 SPREAD 분기를 제거하고 정상적인 DIRECT/INDIRECT 판정 진행
    
    # 1) Evidence/biz_summary에서 키워드 점수 계산
    text_to_analyze = ' '.join(evidence_snippets or []) + ' ' + (biz_summary or '')
    text_to_analyze = text_to_analyze.lower()
    
    direct_score = 0
    indirect_score = 0
    direct_keywords_found = []
    indirect_keywords_found = []
    
    for category, keywords in DIRECT_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in text_to_analyze:
                direct_score += 1
                direct_keywords_found.append(kw)
    
    for category, keywords in INDIRECT_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in text_to_analyze:
                indirect_score += 1
                indirect_keywords_found.append(kw)
    
    # 2) Snippet 기반 판정 (우선)
    if direct_score > 0 or indirect_score > 0:
        if direct_score > indirect_score:
            return {
                'nature': 'DIRECT',
                'reason': f'증거 문장에서 직접 키워드 발견: {direct_keywords_found[:3]}',
                'channel': None,
                'evidence_keywords': direct_keywords_found,
            }
        else:
            # 간접: 채널 결정
            channel = _determine_indirect_channel(indirect_keywords_found)
            return {
                'nature': 'INDIRECT',
                'reason': f'증거 문장에서 거시 키워드 발견: {indirect_keywords_found[:3]}',
                'channel': channel,
                'evidence_keywords': indirect_keywords_found,
            }
    
    # 3) Sector Fallback
    direct_vars = SECTOR_DIRECT_VARIABLES.get(sector_l1, [])
    if variable in direct_vars:
        return {
            'nature': 'DIRECT',
            'reason': f'섹터({sector_l1})가 {variable}에 직접 노출됨',
            'channel': None,
            'evidence_keywords': [],
        }
    
    # 4) 기본값: Indirect
    return {
        'nature': 'INDIRECT',
        'reason': f'섹터({sector_l1})는 {variable}에 간접 경로로 연결됨',
        'channel': 'MACRO_POLICY',  # 기본 채널
        'evidence_keywords': [],
    }


def _determine_indirect_channel(keywords: List[str]) -> str:
    """간접 경로 채널 결정"""
    keywords_lower = [k.lower() for k in keywords]
    
    for channel, config in INDIRECT_CHANNEL_TEMPLATES.items():
        for path_kw in config['path']:
            if any(path_kw.lower() in kw for kw in keywords_lower):
                return channel
    
    return 'MACRO_POLICY'  # 기본값


def generate_indirect_explanation(
    variable: str,
    company: str,
    channel: str,
    polarity: str
) -> str:
    """
    Indirect 영향에 대한 설명 생성
    
    Direct와 완전히 다른 문장 구조 사용
    """
    var_name = get_variable_korean_name(variable)
    
    # 채널별 템플릿 선택
    channel_config = INDIRECT_CHANNEL_TEMPLATES.get(channel, INDIRECT_CHANNEL_TEMPLATES['MACRO_POLICY'])
    base_explanation = channel_config['template'].format(variable=var_name, company=company)
    
    # 방향성 추가
    if polarity == 'POSITIVE':
        direction = "이는 **긍정적**인 요인으로 작용할 수 있습니다."
    elif polarity == 'NEGATIVE':
        direction = "이는 **부정적**인 요인으로 작용할 수 있습니다."
    else:
        direction = "구체적인 영향은 시장 환경에 따라 달라질 수 있습니다."
    
    return f"{base_explanation}\n{direction}\n\n{INDIRECT_DISCLAIMER}"


def generate_mechanism_explanation(
    mechanism: str,
    polarity: str,
    variable: str,
    company: str,
    template_type: str = 'full',
    # V1.5.4 추가 파라미터
    sector_l1: str = None,
    evidence_snippets: List[str] = None,
    biz_summary: str = None,
    force_direct: bool = None  # 강제 Direct/Indirect 지정
) -> str:
    """
    Mechanism + Polarity → 자연어 설명 생성
    
    V1.5.4: Direct/Indirect 자동 판정으로 문장 분기
    
    Args:
        mechanism: PRODUCT_PRICE, INPUT_COST, SPREAD, DEMAND, MACRO_SENSITIVITY
        polarity: POSITIVE, NEGATIVE, MIXED
        variable: 경제 변수 코드 (예: OIL_PRICE)
        company: 기업명
        template_type: 'short', 'medium', 'full'
        sector_l1: 섹터 (Direct/Indirect 판정용)
        evidence_snippets: 증거 문장 (Direct/Indirect 판정용)
        biz_summary: 사업개요 (Direct/Indirect 판정용)
        force_direct: 강제 Direct(True) 또는 Indirect(False) 지정
    
    Returns:
        자연어 설명 문장
    """
    var_name = get_variable_korean_name(variable)
    
    # V1.5.4: Direct/Indirect 판정
    if force_direct is not None:
        is_direct = force_direct
        impact_channel = None
    elif sector_l1:
        impact_info = classify_impact_nature(
            variable=variable,
            sector_l1=sector_l1,
            mechanism=mechanism,
            evidence_snippets=evidence_snippets,
            biz_summary=biz_summary
        )
        is_direct = (impact_info['nature'] == 'DIRECT')
        impact_channel = impact_info.get('channel')
    else:
        # 호환성: 파라미터 없으면 기존 Direct 방식
        is_direct = True
        impact_channel = None
    
    # Indirect인 경우 별도 템플릿 사용
    if not is_direct and mechanism in ['PRODUCT_PRICE', 'INPUT_COST']:
        return generate_indirect_explanation(
            variable=variable,
            company=company,
            channel=impact_channel or 'MACRO_POLICY',
            polarity=polarity
        )
    
    # Direct: 기존 템플릿 사용
    mech_templates = MECHANISM_TEMPLATES.get(mechanism, {})
    pol_templates = mech_templates.get(polarity, mech_templates.get('MIXED', {}))
    
    if not pol_templates:
        return f"{var_name} 변화가 {company}에 영향을 줍니다."
    
    template = pol_templates.get(template_type, pol_templates.get('medium', ''))
    
    return template.format(variable=var_name, company=company)


# MIXED 조건부 트리거 템플릿
MIXED_CONDITIONAL_TRIGGERS = {
    'OIL_PRICE': {
        'SPREAD': {
            'positive_condition': '정제마진이 동반 확대되면',
            'negative_condition': '수요 둔화로 마진 축소 시',
        },
    },
    'INTEREST_RATE': {
        'MACRO_SENSITIVITY': {
            'positive_condition': '경기 회복으로 수익성 개선 시',
            'negative_condition': '금리 상승이 지속되면',
        },
    },
}


def generate_mixed_disambiguation(
    variable: str,
    company: str,
    mechanism: str = 'SPREAD'
) -> Dict[str, str]:
    """
    MIXED(양면성) → 호재/악재 분리 설명
    
    V1.5.4: 조건부 트리거 추가
    
    Returns:
        {
            'summary': '전체 요약',
            'positive': '호재 측면',
            'negative': '악재 측면',
            'conditional_triggers': {...}  # V1.5.4: 조건부 트리거
        }
    """
    var_name = get_variable_korean_name(variable)
    
    spread_templates = MECHANISM_TEMPLATES.get('SPREAD', {}).get('MIXED', {})
    
    result = {
        'summary': spread_templates.get('full', '').format(variable=var_name, company=company),
        'positive': spread_templates.get('positive_aspect', ''),
        'negative': spread_templates.get('negative_aspect', ''),
    }
    
    # V1.5.4: 조건부 트리거 추가
    triggers = MIXED_CONDITIONAL_TRIGGERS.get(variable, {}).get(mechanism, {})
    if triggers:
        result['conditional_triggers'] = {
            'positive_condition': triggers.get('positive_condition', ''),
            'negative_condition': triggers.get('negative_condition', ''),
            'note': '※ 위 조건이 충족될 때 각각 호재/악재로 작용할 수 있습니다.',
        }
    
    return result


def generate_2hop_story(
    var1: str,
    var1_to_var2_relation: str,
    var2: str,
    var2_to_company_mechanism: str,
    var2_to_company_polarity: str,
    company: str,
    scenario_name: str = None
) -> str:
    """
    2-hop 경로 → 스토리텔링 문장 생성
    
    예: OIL_PRICE → INFLATION_CPI → INTEREST_RATE → NAVER
    
    Args:
        var1: 시작 변수 (예: OIL_PRICE)
        var1_to_var2_relation: Macro Link 관계 (예: PRESSURES)
        var2: 중간 변수 (예: INTEREST_RATE)
        var2_to_company_mechanism: 기업 영향 메커니즘
        var2_to_company_polarity: 기업 영향 방향
        company: 기업명
        scenario_name: 시나리오 이름 (예: "유가 급등 시나리오")
    
    Returns:
        스토리텔링 문장
    """
    var1_name = get_variable_korean_name(var1)
    var2_name = get_variable_korean_name(var2)
    
    # 1단계: 거시 환경 설명
    macro_connector = MACRO_LINK_CONNECTORS.get(
        var1_to_var2_relation, 
        f'{var1_name}은(는) {var2_name}에 영향을 줍니다.'
    )
    macro_explanation = macro_connector.format(source=var1_name, target=var2_name)
    
    # 2단계: 기업 영향 설명
    company_explanation = generate_mechanism_explanation(
        mechanism=var2_to_company_mechanism,
        polarity=var2_to_company_polarity,
        variable=var2,
        company=company,
        template_type='full'
    )
    
    # 결론
    if var2_to_company_polarity == 'POSITIVE':
        conclusion_direction = '긍정적'
    elif var2_to_company_polarity == 'NEGATIVE':
        conclusion_direction = '부정적'
    else:
        conclusion_direction = '복합적'
    
    conclusion = f"따라서 {var1_name} 상승은 {company}에 **{conclusion_direction}**인 영향을 줄 수 있습니다."
    
    # 시나리오 이름 기본값
    if not scenario_name:
        scenario_name = f"{var1_name} 상승 시나리오"
    
    return TWO_HOP_STORY_TEMPLATE.format(
        scenario_name=scenario_name,
        macro_explanation=macro_explanation,
        company_explanation=company_explanation,
        conclusion=conclusion
    ).strip()


def generate_oil_price_scenario_explanation(company: str, mechanism: str, polarity: str) -> str:
    """
    유가 시나리오 특화 설명 생성 (가장 많이 쓰이는 시나리오)
    """
    if mechanism == 'SPREAD':
        return f"""
**유가 급등 시나리오 - {company}**

{company}은(는) 정유/화학 기업으로, 유가 변동에 **양면적 영향**을 받습니다.

**호재 측면:**
- 단기적으로 **재고평가 이익** 발생 (보유 재고 가치 상승)
- **제품 가격 전가** 시 정제마진(크랙스프레드) 유지 가능

**악재 측면:**
- 장기적으로 **원료비 부담 증가**
- 제품 수요 위축 시 **판가 전가력 약화** 우려

**결론:** 유가 상승의 **원인(수요 주도 vs 공급 충격)**과 **속도**에 따라 영향이 달라집니다.
""".strip()
    
    elif mechanism == 'INPUT_COST' and polarity == 'NEGATIVE':
        return f"""
**유가 급등 시나리오 - {company}**

{company}은(는) 유류비가 주요 원가 항목인 기업으로, 유가 상승 시 **부정적 영향**을 받습니다.

**영향 경로:**
- 유가 상승 → **연료비/운송비 증가** → 원가율 상승 → 이익률 악화

**결론:** 유가 급등은 {company}의 **수익성에 부담**을 줍니다.
""".strip()
    
    else:
        var_name = get_variable_korean_name('OIL_PRICE')
        return generate_mechanism_explanation(mechanism, polarity, 'OIL_PRICE', company, 'full')


def generate_interest_rate_scenario_explanation(company: str, mechanism: str, polarity: str, sector: str = None) -> str:
    """
    금리 시나리오 특화 설명 생성
    """
    if mechanism == 'PRODUCT_PRICE' and polarity == 'POSITIVE':
        # 금융 섹터
        return f"""
**금리 인상 시나리오 - {company}**

{company}은(는) 금융 기업으로, 금리 상승 시 **긍정적 영향**을 받습니다.

**영향 경로:**
- 금리 인상 → **예대마진(NIM) 확대** → 이자이익 증가 → 수익성 개선

**결론:** 금리 인상은 {company}의 **수익성에 긍정적**입니다.
""".strip()
    
    elif mechanism == 'MACRO_SENSITIVITY' and polarity == 'NEGATIVE':
        # 성장주
        return f"""
**금리 인상 시나리오 - {company}**

{company}은(는) 성장주로, 금리 상승 시 **부정적 영향**을 받을 수 있습니다.

**영향 경로:**
- 금리 인상 → **할인율 상승** → 미래 현금흐름의 현재 가치 감소 → 밸류에이션 부담

**성장주가 금리에 민감한 이유:**
- 성장주의 가치는 **먼 미래의 이익**에 크게 의존
- 금리가 오르면 먼 미래의 이익을 현재 가치로 환산할 때 **더 크게 할인**됨

**결론:** 금리 인상은 {company}의 **밸류에이션에 부담**을 줄 수 있습니다.
""".strip()
    
    else:
        return generate_mechanism_explanation(mechanism, polarity, 'INTEREST_RATE', company, 'full')


# =============================================================================
# ⭐ 검증용 함수
# =============================================================================

def validate_explanation_consistency():
    """
    설명 일관성 검증
    
    같은 Mechanism + Polarity → 항상 같은 뉘앙스의 문장
    """
    test_cases = [
        # (mechanism, polarity, variable, company)
        ('PRODUCT_PRICE', 'POSITIVE', 'DRAM_ASP', '삼성전자'),
        ('PRODUCT_PRICE', 'POSITIVE', 'DRAM_ASP', 'SK하이닉스'),
        ('INPUT_COST', 'NEGATIVE', 'OIL_PRICE', '대한항공'),
        ('INPUT_COST', 'NEGATIVE', 'OIL_PRICE', '아시아나항공'),
        ('SPREAD', 'MIXED', 'OIL_PRICE', 'S-Oil'),
        ('SPREAD', 'MIXED', 'OIL_PRICE', 'SK이노베이션'),
        ('MACRO_SENSITIVITY', 'NEGATIVE', 'INTEREST_RATE', '네이버'),
        ('MACRO_SENSITIVITY', 'NEGATIVE', 'INTEREST_RATE', '카카오'),
    ]
    
    results = []
    for mech, pol, var, company in test_cases:
        explanation = generate_mechanism_explanation(mech, pol, var, company, 'medium')
        results.append({
            'mechanism': mech,
            'polarity': pol,
            'variable': var,
            'company': company,
            'explanation': explanation,
        })
    
    return results


def print_validation_report():
    """검증 리포트 출력"""
    print('=' * 70)
    print('KG Explanation Layer Validation Report')
    print('=' * 70)
    
    results = validate_explanation_consistency()
    
    # Mechanism별 그룹핑
    from collections import defaultdict
    grouped = defaultdict(list)
    for r in results:
        key = (r['mechanism'], r['polarity'])
        grouped[key].append(r)
    
    print('\n[일관성 검증: 같은 Mechanism+Polarity → 같은 뉘앙스]')
    for (mech, pol), items in grouped.items():
        print(f'\n--- {mech} / {pol} ---')
        explanations = set(item['explanation'] for item in items)
        
        if len(explanations) == 1:
            print('  ✅ 일관성 OK (모든 기업에 동일한 설명)')
        else:
            print('  ⚠️  일관성 주의 (기업마다 다른 설명)')
        
        for item in items:
            print(f'  • {item["company"]}: {item["explanation"][:50]}...')
    
    # 2-hop 스토리텔링 예시
    print('\n' + '=' * 70)
    print('[2-hop 스토리텔링 예시]')
    print('=' * 70)
    
    # 유가 → 금리 → 네이버
    story = generate_2hop_story(
        var1='OIL_PRICE',
        var1_to_var2_relation='PRESSURES',
        var2='INTEREST_RATE',
        var2_to_company_mechanism='MACRO_SENSITIVITY',
        var2_to_company_polarity='NEGATIVE',
        company='네이버',
        scenario_name='유가 급등 → 금리 인상 시나리오'
    )
    print('\n[예시 1: 유가 → 금리 → 네이버]')
    print(story)
    
    # MIXED 분해 예시
    print('\n' + '=' * 70)
    print('[MIXED 해상도 높이기 예시]')
    print('=' * 70)
    
    mixed_result = generate_mixed_disambiguation('OIL_PRICE', 'S-Oil')
    print('\n[예시: S-Oil + 유가 (MIXED)]')
    print(f'요약: {mixed_result["summary"]}')
    print(f'호재: {mixed_result["positive"]}')
    print(f'악재: {mixed_result["negative"]}')
    
    # 특화 시나리오 예시
    print('\n' + '=' * 70)
    print('[특화 시나리오 설명 예시]')
    print('=' * 70)
    
    print('\n[유가 시나리오 - S-Oil (SPREAD)]')
    print(generate_oil_price_scenario_explanation('S-Oil', 'SPREAD', 'MIXED'))
    
    print('\n[금리 시나리오 - KB금융 (금융)]')
    print(generate_interest_rate_scenario_explanation('KB금융', 'PRODUCT_PRICE', 'POSITIVE'))
    
    print('\n[금리 시나리오 - 네이버 (성장주)]')
    print(generate_interest_rate_scenario_explanation('네이버', 'MACRO_SENSITIVITY', 'NEGATIVE'))


# =============================================================================
# ⭐ Source Tagging (Evidence Source) - V1.5.2
# =============================================================================

# Evidence Source 유형
EVIDENCE_SOURCES = {
    'DART_BIZ_SUMMARY': 'DART 사업보고서 (사업개요)',
    'DART_KEYWORDS': 'DART 사업보고서 (키워드 추출)',
    'INDUSTRY_RULE': '산업 표준 규칙 (KG Rule v1.5)',
    'SECTOR_MAPPING': '섹터-드라이버 매핑 (KG Rule v1.5)',
    'VALUE_CHAIN_RULE': '밸류체인 분류 규칙 (KG Rule v1.5)',
    'MACRO_THEORY': '거시경제 이론 (Macro Graph)',
}

def get_evidence_source(
    source_type: str,
    detail: str = None
) -> Dict[str, str]:
    """
    Evidence Source 메타데이터 생성
    
    Args:
        source_type: EVIDENCE_SOURCES 키
        detail: 추가 상세 정보 (예: 키워드)
    
    Returns:
        {
            'type': 'DART_BIZ_SUMMARY',
            'description': 'DART 사업보고서 (사업개요)',
            'detail': '키워드: 항공유, 유류할증료',
        }
    """
    return {
        'type': source_type,
        'description': EVIDENCE_SOURCES.get(source_type, source_type),
        'detail': detail,
    }


# =============================================================================
# ⭐ 확신도 표현 (어조 조절) - V1.5.2
# =============================================================================

# 확신도 수준
CONFIDENCE_MODIFIERS = {
    'HIGH': '',  # 단정적 (룰 기반 명확한 경우)
    'MEDIUM': '~할 수 있습니다',  # 가능성 표현
    'LOW': '~할 가능성이 있습니다',  # 약한 가능성
}

def apply_confidence_modifier(
    text: str,
    confidence: str = 'MEDIUM'
) -> str:
    """
    문장에 확신도 표현 적용
    
    "긍정적입니다" → "긍정적일 수 있습니다" (MEDIUM)
    "부정적입니다" → "부정적일 가능성이 있습니다" (LOW)
    """
    if confidence == 'HIGH':
        return text  # 단정적 표현 유지
    
    # 확률적 어조 변환
    replacements = [
        ('긍정적입니다', '긍정적일 수 있습니다' if confidence == 'MEDIUM' else '긍정적일 가능성이 있습니다'),
        ('부정적입니다', '부정적일 수 있습니다' if confidence == 'MEDIUM' else '부정적일 가능성이 있습니다'),
        ('이어집니다', '이어질 수 있습니다' if confidence == 'MEDIUM' else '이어질 가능성이 있습니다'),
        ('작용합니다', '작용할 수 있습니다' if confidence == 'MEDIUM' else '작용할 가능성이 있습니다'),
        ('줍니다', '줄 수 있습니다' if confidence == 'MEDIUM' else '줄 가능성이 있습니다'),
    ]
    
    result = text
    for old, new in replacements:
        result = result.replace(old, new)
    
    return result


# =============================================================================
# ⭐ 구조화된 JSON 출력 - V1.5.2
# =============================================================================

def generate_scenario_json(
    scenario_type: str,
    direction: str,  # 'UP' or 'DOWN'
    affected_companies: List[Dict],
) -> Dict:
    """
    시나리오 분석 결과를 구조화된 JSON으로 출력
    
    Args:
        scenario_type: 'OIL_PRICE', 'INTEREST_RATE', 'EXCHANGE_RATE' 등
        direction: 'UP' or 'DOWN'
        affected_companies: [
            {
                'ticker': '005930',
                'name': '삼성전자',
                'sector_l1': 'SEC_SEMI',
                'value_chain': 'MID_HARD',
                'mechanism': 'PRODUCT_PRICE',
                'polarity': 'POSITIVE',
                'weight': 0.85,
            },
            ...
        ]
    
    Returns:
        구조화된 시나리오 JSON
    """
    var_name = get_variable_korean_name(scenario_type)
    direction_kr = '상승' if direction == 'UP' else '하락'
    
    results = []
    for company in affected_companies:
        mechanism = company.get('mechanism', 'DEMAND')
        polarity = company.get('polarity', 'MIXED')
        weight = company.get('weight', 0.5)
        
        # 설명 생성
        reasoning = generate_mechanism_explanation(
            mechanism=mechanism,
            polarity=polarity,
            variable=scenario_type,
            company=company['name'],
            template_type='full'
        )
        
        # 확신도 적용 (weight 기반)
        confidence = 'HIGH' if weight >= 0.8 else ('MEDIUM' if weight >= 0.5 else 'LOW')
        reasoning_with_confidence = apply_confidence_modifier(reasoning, confidence)
        
        # Evidence Source 결정
        if company.get('text_match_weight', 0) > 0:
            evidence = get_evidence_source(
                'DART_BIZ_SUMMARY',
                f"텍스트 매칭 점수: {company.get('text_match_weight', 0):.2f}"
            )
        else:
            evidence = get_evidence_source('SECTOR_MAPPING')
        
        results.append({
            'ticker': company['ticker'],
            'name': company['name'],
            'market_cap': company.get('market_cap'),  # V1.5.5: 시가총액 (설명용 메타)
            'sector': company.get('sector_l1', ''),
            'value_chain': company.get('value_chain', ''),
            'impact': {
                'direction': polarity,
                'mechanism': mechanism,
                'mechanism_kr': {
                    'PRODUCT_PRICE': '판가 채널',
                    'INPUT_COST': '원가 채널',
                    'SPREAD': '스프레드 채널',
                    'DEMAND': '수요 채널',
                    'MACRO_SENSITIVITY': '거시 민감도',
                }.get(mechanism, mechanism),
                'weight': weight,
                'confidence': confidence,
            },
            'reasoning': reasoning_with_confidence,
            'evidence_source': evidence,
        })
    
    return {
        'scenario': {
            'variable': scenario_type,
            'variable_kr': var_name,
            'direction': direction,
            'direction_kr': direction_kr,
            'title': f"{var_name} {direction_kr} 시나리오",
        },
        'affected_companies': results,
        'generated_at': __import__('datetime').datetime.utcnow().isoformat(),
        'kg_version': 'v1.5.2',
    }


def generate_company_insight_json(
    ticker: str,
    name: str,
    sector_l1: str,
    value_chain: str,
    drivers: List[Dict],
) -> Dict:
    """
    단일 기업의 드라이버 분석 결과를 JSON으로 출력
    
    Args:
        ticker: 종목코드
        name: 기업명
        sector_l1: 섹터
        value_chain: 밸류체인
        drivers: [
            {
                'driver_code': 'DRAM_ASP',
                'mechanism': 'PRODUCT_PRICE',
                'polarity': 'POSITIVE',
                'weight': 0.85,
                'text_match_weight': 0.3,
            },
            ...
        ]
    
    Returns:
        기업 인사이트 JSON
    """
    driver_insights = []
    
    for driver in drivers:
        driver_code = driver['driver_code']
        mechanism = driver.get('mechanism', 'DEMAND')
        polarity = driver.get('polarity', 'MIXED')
        weight = driver.get('weight', 0.5)
        
        # 설명 생성
        reasoning = generate_mechanism_explanation(
            mechanism=mechanism,
            polarity=polarity,
            variable=driver_code,
            company=name,
            template_type='full'
        )
        
        # 확신도 적용
        confidence = 'HIGH' if weight >= 0.8 else ('MEDIUM' if weight >= 0.5 else 'LOW')
        reasoning_with_confidence = apply_confidence_modifier(reasoning, confidence)
        
        # Evidence Source
        if driver.get('text_match_weight', 0) > 0:
            evidence = get_evidence_source(
                'DART_BIZ_SUMMARY',
                f"키워드 매칭: {get_variable_korean_name(driver_code)}"
            )
        else:
            evidence = get_evidence_source('SECTOR_MAPPING')
        
        driver_insights.append({
            'driver': {
                'code': driver_code,
                'name_kr': get_variable_korean_name(driver_code),
            },
            'impact': {
                'direction': polarity,
                'mechanism': mechanism,
                'weight': weight,
                'confidence': confidence,
            },
            'reasoning': reasoning_with_confidence,
            'evidence_source': evidence,
        })
    
    return {
        'company': {
            'ticker': ticker,
            'name': name,
            'sector': sector_l1,
            'value_chain': value_chain,
        },
        'drivers': driver_insights,
        'generated_at': __import__('datetime').datetime.utcnow().isoformat(),
        'kg_version': 'v1.5.2',
    }


# =============================================================================
# ⭐ 특화 시나리오 JSON 생성기
# =============================================================================

def generate_oil_scenario_json(
    direction: str,
    companies: List[Dict]
) -> Dict:
    """유가 시나리오 JSON 생성"""
    result = generate_scenario_json('OIL_PRICE', direction, companies)
    
    # SPREAD 기업에 양면성 추가 설명
    for company in result['affected_companies']:
        if company['impact']['mechanism'] == 'SPREAD':
            mixed_detail = generate_mixed_disambiguation('OIL_PRICE', company['name'])
            company['mixed_analysis'] = {
                'positive_aspect': mixed_detail['positive'],
                'negative_aspect': mixed_detail['negative'],
            }
    
    return result


def generate_interest_rate_scenario_json(
    direction: str,
    companies: List[Dict]
) -> Dict:
    """금리 시나리오 JSON 생성"""
    result = generate_scenario_json('INTEREST_RATE', direction, companies)
    
    # 성장주에 추가 설명
    for company in result['affected_companies']:
        if company['impact']['mechanism'] == 'MACRO_SENSITIVITY':
            company['growth_stock_note'] = (
                "성장주의 가치는 먼 미래의 이익에 크게 의존하며, "
                "금리 상승 시 할인율이 높아져 현재 가치가 낮아질 수 있습니다."
            )
    
    return result


# =============================================================================
# ⭐ V1.5.3 - Evidence Snippet (DART 문장 추출)
# =============================================================================

# 변수별 증거 문장 템플릿 (DART에서 실제로 자주 등장하는 표현)
EVIDENCE_SNIPPET_TEMPLATES = {
    'OIL_PRICE': {
        'INPUT_COST': [
            "유류비는 당사의 주요 원가 항목으로, 유가 변동에 따라 원가율이 영향을 받습니다.",
            "항공유/선박유 가격은 영업비용의 상당 부분을 차지합니다.",
        ],
        'SPREAD': [
            "원유 가격과 정제마진은 당사 수익성의 핵심 변수입니다.",
            "나프타 등 원료 가격 변동에 따라 제품 판매 가격을 조정하고 있습니다.",
        ],
        'PRODUCT_PRICE': [
            "원유 가격 상승 시 제품 판매 가격 상승으로 이어질 수 있습니다.",
        ],
    },
    'INTEREST_RATE': {
        'PRODUCT_PRICE': [
            "금리 상승은 예대마진(NIM) 확대로 이어져 이자이익 증가에 기여합니다.",
            "시장금리 변동은 당사의 순이자마진에 직접적인 영향을 미칩니다.",
        ],
        'MACRO_SENSITIVITY': [
            "금리 변동은 당사 자산의 현재 가치 평가에 영향을 줄 수 있습니다.",
            "할인율 변동에 따라 사업 가치가 변동될 수 있습니다.",
        ],
    },
    'DRAM_ASP': {
        'PRODUCT_PRICE': [
            "메모리 반도체 가격은 당사 매출의 핵심 드라이버입니다.",
            "D램/낸드 평균판매가격(ASP)은 수익성에 직접적인 영향을 미칩니다.",
        ],
    },
    'EXCHANGE_RATE_USD_KRW': {
        'POSITIVE': [
            "수출 비중이 높아 원화 약세는 매출에 긍정적으로 작용합니다.",
            "해외 매출의 원화 환산 시 환율 변동 효과가 발생합니다.",
        ],
        'NEGATIVE': [
            "수입 원재료 비중이 높아 환율 상승은 원가 부담으로 작용합니다.",
        ],
    },
}


def _classify_snippet_strength(text: str) -> str:
    """
    Evidence snippet의 강도 분류
    
    Returns:
        'STRONG' | 'MODERATE' | 'CONSERVATIVE'
    """
    text_lower = text.lower()
    
    # 강한 표현 키워드
    strong_keywords = ['주요', '핵심', '대부분', '상당', '직접적', '결정적', '필수', '대부분']
    # 보수적 표현 키워드
    conservative_keywords = ['일부', '부분적', '제한적', '미미', '가능성', '영향을 줄 수', '~할 수 있음']
    
    strong_count = sum(1 for kw in strong_keywords if kw in text_lower)
    conservative_count = sum(1 for kw in conservative_keywords if kw in text_lower)
    
    if strong_count > conservative_count:
        return 'STRONG'
    elif conservative_count > 0:
        return 'CONSERVATIVE'
    else:
        return 'MODERATE'


def get_evidence_snippets(
    variable: str,
    mechanism: str,
    polarity: str,
    biz_summary: str = None,
    keywords: List[str] = None,
    return_with_metadata: bool = False,
    max_candidates: int = 3  # V1.5.4: 최대 후보 수
) -> List:
    """
    Evidence Snippet (증거 문장) 생성
    
    V1.5.4: 2~3개 후보 제공 + 강도 분류
    
    Args:
        variable: 경제 변수
        mechanism: 메커니즘
        polarity: 방향성
        biz_summary: 사업개요 (있으면 실제 문장 추출 시도)
        keywords: 키워드 리스트
        return_with_metadata: True면 Dict 리스트 반환
        max_candidates: 최대 후보 수 (기본 3개)
    
    Returns:
        증거 문장 리스트 (또는 메타데이터 포함 Dict 리스트)
    """
    snippets = []
    snippet_metadata = []
    
    # 1) biz_summary에서 실제 문장 추출 시도 (있는 경우)
    if biz_summary:
        var_keywords = {
            'OIL_PRICE': ['유가', '원유', '유류', '항공유', '연료', '나프타'],
            'INTEREST_RATE': ['금리', '이자', 'NIM', '예대마진', '할인율'],
            'DRAM_ASP': ['D램', 'DRAM', '메모리', '반도체', 'ASP', '평균판매가'],
            'EXCHANGE_RATE_USD_KRW': ['환율', '달러', '원화', '수출', '환산'],
        }
        
        search_keywords = var_keywords.get(variable, [])
        for keyword in search_keywords:
            if keyword in biz_summary:
                # 키워드가 포함된 문장 추출 (간단한 로직)
                sentences = biz_summary.replace('다.', '다.|').split('|')
                for sent in sentences:
                    sent = sent.strip()
                    if keyword in sent and len(sent) > 20:
                        # 중복 방지 및 문장 끝 정리
                        if not sent.endswith('.') and not sent.endswith('다'):
                            sent += '다.'
                        if sent not in snippets:
                            snippets.append(sent)
                            strength = _classify_snippet_strength(sent)
                            snippet_metadata.append({
                                'text': sent,
                                'source': 'biz_summary',
                                'selection_reason': f'키워드 "{keyword}" 매칭 (사업개요 직접 추출)',
                                'matched_keyword': keyword,
                                'strength': strength,  # V1.5.4: 강도 분류
                            })
                        if len(snippets) >= max_candidates:
                            break
                if len(snippets) >= max_candidates:
                    break
    
    # 2) 템플릿에서 가져오기 (fallback)
    if len(snippets) < max_candidates:
        templates = EVIDENCE_SNIPPET_TEMPLATES.get(variable, {})
        mech_templates = templates.get(mechanism, templates.get(polarity, []))
        template_snippets = mech_templates[:max_candidates] if mech_templates else []
        
        for tmpl in template_snippets:
            if tmpl not in snippets:
                snippets.append(tmpl)
                strength = _classify_snippet_strength(tmpl)
                snippet_metadata.append({
                    'text': tmpl,
                    'source': 'template',
                    'selection_reason': f'{mechanism} 메커니즘 표준 템플릿',
                    'matched_keyword': None,
                    'strength': strength,  # V1.5.4: 강도 분류
                })
            if len(snippets) >= max_candidates:
                break
    
    # 3) 기본 문장
    if not snippets:
        var_name = get_variable_korean_name(variable)
        default_text = f"{var_name} 변동은 당사의 실적에 영향을 줄 수 있습니다."
        snippets = [default_text]
        snippet_metadata.append({
            'text': default_text,
            'source': 'default',
            'selection_reason': '기본 템플릿 (구체적 증거 없음)',
            'matched_keyword': None,
            'strength': 'CONSERVATIVE',  # 기본은 보수적
        })
    
    # V1.5.4: 강한 것과 보수적인 것 구분
    if return_with_metadata:
        # 강도별 정렬 (STRONG → MODERATE → CONSERVATIVE)
        strength_order = {'STRONG': 0, 'MODERATE': 1, 'CONSERVATIVE': 2}
        snippet_metadata.sort(key=lambda x: strength_order.get(x.get('strength', 'MODERATE'), 1))
        
        # primary(가장 강한 것)와 secondary(가장 보수적인 것) 표시
        if len(snippet_metadata) > 0:
            snippet_metadata[0]['is_primary'] = True
        if len(snippet_metadata) > 1:
            snippet_metadata[-1]['is_secondary'] = True
        
        return snippet_metadata
    
    return snippets


# =============================================================================
# ⭐ V1.5.3 - Exposure Level (노출도) 분리
# =============================================================================

# Exposure 판단 키워드 (원가/수익 구조 관련)
EXPOSURE_KEYWORDS = {
    'HIGH': ['주요', '핵심', '대부분', '상당', '직접적', '결정적', '필수'],
    'MEDIUM': ['일부', '부분적', '영향', '관련', '연관'],
    'LOW': ['간접적', '미미', '제한적', '낮은'],
}

# 메커니즘별 기본 Exposure
MECHANISM_DEFAULT_EXPOSURE = {
    'PRODUCT_PRICE': 'HIGH',  # 제품 가격 = 직접 노출
    'INPUT_COST': 'HIGH',     # 원가 = 직접 노출
    'SPREAD': 'HIGH',         # 스프레드 = 직접 노출
    'DEMAND': 'MEDIUM',       # 수요 = 간접적
    'MACRO_SENSITIVITY': 'MEDIUM',  # 거시 민감도 = 간접적
}


def _calculate_exposure_score(
    mechanism: str,
    weight: float,
    text_match_weight: float = 0,
    return_components: bool = False
) -> int:
    """
    Exposure 점수 계산 (Percentile 산출용)
    
    V1.5.4: components 반환 옵션 추가
    """
    score = 0
    components = {
        'mechanism_bonus': 0,
        'weight_bonus': 0,
        'text_match_bonus': 0,
    }
    
    # 메커니즘 기반 기본 점수
    base_exposure = MECHANISM_DEFAULT_EXPOSURE.get(mechanism, 'MEDIUM')
    if base_exposure == 'HIGH':
        mechanism_bonus = 2
        score += mechanism_bonus
        components['mechanism_bonus'] = mechanism_bonus
    elif base_exposure == 'MEDIUM':
        mechanism_bonus = 1
        score += mechanism_bonus
        components['mechanism_bonus'] = mechanism_bonus
    
    # 가중치 기반 가산점
    if weight >= 0.8:
        weight_bonus = 2
        score += weight_bonus
        components['weight_bonus'] = weight_bonus
    elif weight >= 0.6:
        weight_bonus = 1
        score += weight_bonus
        components['weight_bonus'] = weight_bonus
    
    # 텍스트 매칭 가산점
    if text_match_weight >= 0.5:
        text_match_bonus = 2
        score += text_match_bonus
        components['text_match_bonus'] = text_match_bonus
    elif text_match_weight >= 0.2:
        text_match_bonus = 1
        score += text_match_bonus
        components['text_match_bonus'] = text_match_bonus
    
    if return_components:
        return score, components
    return score


def calculate_exposure_percentile(
    exposure_score: int,
    all_scores: List[int],
    components: Dict = None
) -> Dict:
    """
    V1.5.4: Exposure Percentile 계산 + 설명 가능성
    
    "이 기업이 같은 변수 내에서 상위 몇 %인가?"
    
    ⚠️ 중요: percentile의 의미 범위
    - ❌ "절대적인 리스크/수익 크기"가 아님
    - ⭕ "동일 변수 내 상대적 노출도"
    
    예시:
    - percentile 90 = "이 변수에 노출된 모든 기업 중 상위 10%"
    - percentile 50 = "이 변수에 노출된 모든 기업 중 중간 수준"
    - percentile 10 = "이 변수에 노출된 모든 기업 중 하위 10%"
    
    핵심 원칙: Exposure는 '구조적 민감도'이며, percentile은 동일 변수 내에서의 상대적 위치를 나타낸다.
    
    Args:
        exposure_score: 노출도 점수
        all_scores: 전체 점수 리스트
        components: 점수 구성 요소 (설명용)
    
    Returns:
        {
            'percentile': int,
            'explanation': str,
            'components': {...}  # 숨은 필드 (UI에 안 보여도 됨)
        }
    """
    if not all_scores:
        return {
            'percentile': 50,
            'explanation': '기준 데이터 부족',
            'components': components or {},
        }
    
    below_count = sum(1 for s in all_scores if s < exposure_score)
    percentile = int((below_count / len(all_scores)) * 100)
    
    # 설명 생성
    explanation_parts = []
    if components:
        if components.get('weight_bonus', 0) > 0:
            explanation_parts.append(f"가중치({components['weight_bonus']}점)")
        if components.get('text_match_bonus', 0) > 0:
            explanation_parts.append(f"증거 매칭({components['text_match_bonus']}점)")
        if components.get('mechanism_bonus', 0) > 0:
            explanation_parts.append(f"메커니즘({components['mechanism_bonus']}점)")
    
    explanation = "동일 변수 내에서, " + " + ".join(explanation_parts) if explanation_parts else "기본 계산"
    explanation += f"로 계산한 상대 노출도"
    
    return {
        'percentile': percentile,
        'explanation': explanation,
        'components': components or {},  # 숨은 필드
    }


def calculate_exposure_level(
    mechanism: str,
    weight: float,
    text_match_weight: float = 0,
    biz_summary: str = None,
    keywords: List[str] = None
) -> str:
    """
    Exposure Level (노출도) 계산
    
    Confidence(모델 확신도)와 분리된 개념
    - Exposure: 기업이 해당 변수에 얼마나 '구조적으로' 노출되어 있는가
    
    Args:
        mechanism: 메커니즘
        weight: DRIVEN_BY 가중치
        text_match_weight: 텍스트 매칭 가중치
        biz_summary: 사업개요
        keywords: 키워드 리스트
    
    Returns:
        'HIGH', 'MEDIUM', 'LOW'
    """
    score = 0
    
    # 1) 메커니즘 기반 기본 점수
    base_exposure = MECHANISM_DEFAULT_EXPOSURE.get(mechanism, 'MEDIUM')
    if base_exposure == 'HIGH':
        score += 2
    elif base_exposure == 'MEDIUM':
        score += 1
    
    # 2) 가중치 기반 가산점
    if weight >= 0.8:
        score += 2
    elif weight >= 0.6:
        score += 1
    
    # 3) 텍스트 매칭 가산점 (DART에서 직접 언급됨)
    if text_match_weight >= 0.5:
        score += 2
    elif text_match_weight >= 0.2:
        score += 1
    
    # 4) biz_summary에서 강조 키워드 확인
    if biz_summary:
        for kw in EXPOSURE_KEYWORDS['HIGH']:
            if kw in biz_summary:
                score += 1
                break
    
    # 5) 최종 레벨 결정
    if score >= 5:
        return 'HIGH'
    elif score >= 3:
        return 'MEDIUM'
    else:
        return 'LOW'


# =============================================================================
# ⭐ V1.5.3 - 비교 출력 (Comparison)
# =============================================================================

def is_preferred_stock(name: str) -> bool:
    """우선주 판별"""
    preferred_suffixes = ['우', '우B', '1우', '2우', '3우', '우선', 'PFD']
    return any(name.endswith(suffix) for suffix in preferred_suffixes)


def get_base_company_name(name: str) -> str:
    """기업명에서 우선주 접미사 제거"""
    preferred_suffixes = ['우B', '2우', '3우', '1우', '우선', '우']
    for suffix in preferred_suffixes:
        if name.endswith(suffix):
            return name[:-len(suffix)]
    return name


def deduplicate_companies(companies: List[Dict], key: str = 'name') -> List[Dict]:
    """
    우선주 및 중복 기업 제거
    
    V1.5.4: 우선주 제외, 동일 그룹 첫 번째만 유지
    """
    seen_base_names = set()
    result = []
    
    for c in companies:
        name = c.get(key, '')
        
        # 우선주 제외
        if is_preferred_stock(name):
            continue
        
        # 동일 그룹 중복 제거
        base_name = get_base_company_name(name)
        if base_name in seen_base_names:
            continue
        
        seen_base_names.add(base_name)
        result.append(c)
    
    return result


def generate_comparison_output(
    variable: str,
    companies: List[Dict],
    top_n: int = 3,
    dedupe: bool = True  # V1.5.4: 중복 제거 옵션
) -> Dict:
    """
    같은 변수에 대한 기업 비교 출력
    
    V1.5.4: 우선주/중복 제거 + Exposure Percentile
    
    Args:
        variable: 경제 변수
        companies: 기업 리스트 (mechanism, polarity, weight 포함)
        top_n: 상위/하위 각각 N개
        dedupe: 우선주/중복 제거 여부
    
    Returns:
        {
            'variable': ...,
            'positive_impact': [...],  # Top N 긍정 영향
            'negative_impact': [...],  # Top N 부정 영향
            'mixed_impact': [...],     # MIXED 기업
            'comparison_summary': "..."  # 한 줄 요약
        }
    """
    var_name = get_variable_korean_name(variable)
    
    # V1.5.4: 우선주/중복 제거
    if dedupe:
        companies = deduplicate_companies(companies)
    
    positive = []
    negative = []
    mixed = []
    
    # Exposure 점수 계산 (percentile용)
    all_exposure_scores = []
    all_components = []  # V1.5.4: components 저장
    
    for c in companies:
        exposure_score, components = _calculate_exposure_score(
            c.get('mechanism', 'DEMAND'),
            c.get('weight', 0.5),
            c.get('text_match_weight', 0),
            return_components=True  # V1.5.4: components 반환
        )
        all_exposure_scores.append(exposure_score)
        all_components.append(components)
        
        company_data = {
            'ticker': c['ticker'],
            'name': c['name'],
            'market_cap': c.get('market_cap'),  # V1.5.5: 시가총액 (설명용 메타)
            'mechanism': c.get('mechanism', 'DEMAND'),
            'mechanism_kr': {
                'PRODUCT_PRICE': '판가 채널',
                'INPUT_COST': '원가 채널',
                'SPREAD': '스프레드',
                'DEMAND': '수요 채널',
                'MACRO_SENSITIVITY': '거시 민감도',
            }.get(c.get('mechanism', 'DEMAND'), c.get('mechanism', '')),
            'weight': c.get('weight', 0.5),
            'exposure': calculate_exposure_level(
                c.get('mechanism', 'DEMAND'),
                c.get('weight', 0.5),
                c.get('text_match_weight', 0)
            ),
            'exposure_score': exposure_score,
            'exposure_components': components,  # V1.5.4: 숨은 필드
        }
        
        polarity = c.get('polarity', 'MIXED')
        if polarity == 'POSITIVE':
            positive.append(company_data)
        elif polarity == 'NEGATIVE':
            negative.append(company_data)
        else:
            mixed.append(company_data)
    
    # V1.5.4: Exposure Percentile 계산 (components 포함)
    idx = 0
    for group in [positive, negative, mixed]:
        for company in group:
            percentile_info = calculate_exposure_percentile(
                company['exposure_score'],
                all_exposure_scores,
                components=all_components[idx]  # V1.5.4: components 전달
            )
            company['exposure_percentile'] = percentile_info['percentile']
            company['exposure_percentile_explanation'] = percentile_info['explanation']  # 설명
            idx += 1
    
    # V1.5.4: Tie-Breaking 정렬
    # weight가 같으면 exposure_score로, 그것도 같으면 ticker 순서로
    # 
    # ⚠️ market_cap 사용 원칙:
    # - ✅ 허용: Tie-breaking 최후의 안정 장치 (정렬만, 순위 변경 없음)
    # - ❌ 금지: 노출도 계산에 곱하기, Top-N 선정에 직접 반영, "중요도 점수"에 포함
    # 
    # 원칙: Exposure는 '구조적 민감도'이며, market_cap은 '해석을 돕는 맥락 정보'다.
    # 두 값은 계산적으로 결합하지 않는다.
    def sort_key(x):
        return (
            x.get('weight', 0) or 0,  # 1차: 가중치
            x.get('exposure_score', 0) or 0,  # 2차: 노출도 점수
            x.get('market_cap', 0) or 0 if x.get('market_cap') is not None else 0,  # 3차: 시가총액 (Tie-breaking만, 순위 변경 없음)
            x.get('ticker', '')  # 4차: 종목코드 (안정적 순서)
        )
    
    positive.sort(key=sort_key, reverse=True)
    negative.sort(key=sort_key, reverse=True)
    mixed.sort(key=sort_key, reverse=True)
    
    # Top N 선택
    top_positive = positive[:top_n]
    top_negative = negative[:top_n]
    top_mixed = mixed[:top_n]
    
    # 비교 요약 생성
    summary_parts = []
    if top_positive:
        names = ', '.join([c['name'] for c in top_positive])
        summary_parts.append(f"**수혜주**: {names} ({top_positive[0]['mechanism_kr']})")
    if top_negative:
        names = ', '.join([c['name'] for c in top_negative])
        summary_parts.append(f"**피해주**: {names} ({top_negative[0]['mechanism_kr']})")
    if top_mixed:
        names = ', '.join([c['name'] for c in top_mixed])
        summary_parts.append(f"**양면성**: {names}")
    
    comparison_summary = f"{var_name} 상승 시: " + " / ".join(summary_parts) if summary_parts else ""
    
    # V1.5.5: market_cap 기반 해석 문장 추가 (설명용 메타 정보)
    # ⚠️ market_cap은 해석 문장에만 사용, 순위 변경 없음
    market_cap_insights = []
    
    # 대형주/중소형주 구분 (10조원 = 10,000,000,000,000원 기준)
    LARGE_CAP_THRESHOLD = 10_000_000_000_000  # 10조원
    
    # 긍정 영향 기업 중 대형주/중소형주 구분
    if top_positive:
        large_cap_positive = [c for c in top_positive if c.get('market_cap') and c['market_cap'] >= LARGE_CAP_THRESHOLD]
        small_cap_positive = [c for c in top_positive if c.get('market_cap') and c['market_cap'] < LARGE_CAP_THRESHOLD]
        
        if large_cap_positive:
            names = ', '.join([c['name'] for c in large_cap_positive[:3]])
            market_cap_insights.append(f"**대형주 중에서는** {names}가 상대적으로 민감합니다.")
        if small_cap_positive:
            names = ', '.join([c['name'] for c in small_cap_positive[:3]])
            market_cap_insights.append(f"**중소형주 중에서는** {names}가 노출도가 높습니다.")
    
    # 부정 영향 기업 중 대형주/중소형주 구분
    if top_negative:
        large_cap_negative = [c for c in top_negative if c.get('market_cap') and c['market_cap'] >= LARGE_CAP_THRESHOLD]
        small_cap_negative = [c for c in top_negative if c.get('market_cap') and c['market_cap'] < LARGE_CAP_THRESHOLD]
        
        if large_cap_negative:
            names = ', '.join([c['name'] for c in large_cap_negative[:3]])
            market_cap_insights.append(f"**대형주 중에서는** {names}가 상대적으로 민감합니다.")
        if small_cap_negative:
            names = ', '.join([c['name'] for c in small_cap_negative[:3]])
            market_cap_insights.append(f"**중소형주 중에서는** {names}가 노출도가 높습니다.")
    
    # P0-2: 차이점 설명 강제 생성 (비교 API의 핵심 가치)
    difference_explanation = []
    
    # 1) Positive vs Negative 비교
    if top_positive and top_negative:
        pos_mech = top_positive[0]['mechanism']
        neg_mech = top_negative[0]['mechanism']
        pos_name = top_positive[0]['name']
        neg_name = top_negative[0]['name']
        
        if pos_mech == 'PRODUCT_PRICE' and neg_mech == 'INPUT_COST':
            difference_explanation.append(
                f"**{pos_name}**은 {var_name} 관련 제품을 **판매**하여 수익을 얻는 반면, "
                f"**{neg_name}**은 {var_name}를 **구매**하여 원가로 지출합니다."
            )
        elif pos_mech == 'INPUT_COST' and neg_mech == 'PRODUCT_PRICE':
            difference_explanation.append(
                f"**{neg_name}**은 {var_name} 관련 제품을 **판매**하여 수익을 얻는 반면, "
                f"**{pos_name}**은 {var_name}를 **구매**하여 원가로 지출합니다."
            )
        elif pos_mech == 'SPREAD' or neg_mech == 'SPREAD':
            spread_company = pos_name if pos_mech == 'SPREAD' else neg_name
            difference_explanation.append(
                f"**{spread_company}**은(는) 정제마진(스프레드)에 따라 영향이 달라집니다. "
                f"원가와 판가가 동시에 변해 단기적으로는 재고평가 이익이 발생할 수 있으나, "
                f"장기적으로는 원가 부담으로 작용할 수 있습니다."
            )
        elif pos_mech != neg_mech:
            # 다른 메커니즘이면 기본 비교 문장 생성
            pos_mech_kr = {
                'PRODUCT_PRICE': '판가 수혜',
                'INPUT_COST': '원가 부담',
                'SPREAD': '스프레드 영향',
                'DEMAND': '수요 영향',
                'MACRO_SENSITIVITY': '거시 민감도',
            }.get(pos_mech, pos_mech)
            
            neg_mech_kr = {
                'PRODUCT_PRICE': '판가 수혜',
                'INPUT_COST': '원가 부담',
                'SPREAD': '스프레드 영향',
                'DEMAND': '수요 영향',
                'MACRO_SENSITIVITY': '거시 민감도',
            }.get(neg_mech, neg_mech)
            
            difference_explanation.append(
                f"**{pos_name}**은 {var_name}에 대해 **{pos_mech_kr}** 경로로 영향을 받는 반면, "
                f"**{neg_name}**은 **{neg_mech_kr}** 경로로 영향을 받습니다."
            )
    
    # 2) Mixed vs 다른 그룹 비교
    if top_mixed and (top_positive or top_negative):
        mixed_mech = top_mixed[0]['mechanism']
        mixed_name = top_mixed[0]['name']
        
        if mixed_mech == 'SPREAD':
            if top_negative:
                neg_name = top_negative[0]['name']
                difference_explanation.append(
                    f"**{mixed_name}**은 정제마진(스프레드)에 따라 양면적 영향을 받는 반면, "
                    f"**{neg_name}**은 {var_name} 상승 시 일관되게 원가 부담이 증가합니다."
                )
            elif top_positive:
                pos_name = top_positive[0]['name']
                difference_explanation.append(
                    f"**{mixed_name}**은 정제마진(스프레드)에 따라 양면적 영향을 받는 반면, "
                    f"**{pos_name}**은 {var_name} 상승 시 일관되게 판가 수혜를 받습니다."
                )
    
    # 3) Positive vs Mixed 비교
    if top_positive and top_mixed:
        pos_mech = top_positive[0]['mechanism']
        mixed_mech = top_mixed[0]['mechanism']
        
        if pos_mech != mixed_mech:
            pos_name = top_positive[0]['name']
            mixed_name = top_mixed[0]['name']
            difference_explanation.append(
                f"**{pos_name}**은 {var_name} 상승 시 일관되게 긍정적 영향을 받는 반면, "
                f"**{mixed_name}**은 스프레드 구조로 인해 양면적 영향을 받습니다."
            )
    
    # 4) Negative vs Mixed 비교
    if top_negative and top_mixed:
        neg_mech = top_negative[0]['mechanism']
        mixed_mech = top_mixed[0]['mechanism']
        
        if neg_mech != mixed_mech:
            neg_name = top_negative[0]['name']
            mixed_name = top_mixed[0]['name']
            difference_explanation.append(
                f"**{neg_name}**은 {var_name} 상승 시 일관되게 부정적 영향을 받는 반면, "
                f"**{mixed_name}**은 스프레드 구조로 인해 양면적 영향을 받습니다."
            )
    
    return {
        'variable': {
            'code': variable,
            'name_kr': var_name,
        },
        'positive_impact': top_positive,
        'negative_impact': top_negative,
        'mixed_impact': top_mixed,
        'comparison_summary': comparison_summary,
        'difference_explanation': difference_explanation,
        'market_cap_insights': market_cap_insights,  # V1.5.5: 시가총액 기반 해석 (설명용 메타)
        'total_analyzed': len(companies),
    }


# =============================================================================
# ⭐ V1.5.3 업그레이드된 JSON 생성기
# =============================================================================

def generate_scenario_json_v153(
    scenario_type: str,
    direction: str,
    affected_companies: List[Dict],
    include_comparison: bool = True
) -> Dict:
    """
    V1.5.3 시나리오 분석 결과 JSON (Evidence + Exposure + Comparison)
    """
    var_name = get_variable_korean_name(scenario_type)
    direction_kr = '상승' if direction == 'UP' else '하락'
    
    results = []
    for company in affected_companies:
        mechanism = company.get('mechanism', 'DEMAND')
        polarity = company.get('polarity', 'MIXED')
        weight = company.get('weight', 0.5)
        text_match_weight = company.get('text_match_weight', 0)
        biz_summary = company.get('biz_summary', '')
        
        # 설명 생성
        reasoning = generate_mechanism_explanation(
            mechanism=mechanism,
            polarity=polarity,
            variable=scenario_type,
            company=company['name'],
            template_type='full'
        )
        
        # Confidence (모델 확신도)
        confidence = 'HIGH' if weight >= 0.8 else ('MEDIUM' if weight >= 0.5 else 'LOW')
        reasoning_with_confidence = apply_confidence_modifier(reasoning, confidence)
        
        # Exposure (기업 노출도) - V1.5.3 신규
        exposure = calculate_exposure_level(
            mechanism, weight, text_match_weight, biz_summary
        )
        
        # Evidence Snippet - V1.5.3 신규
        evidence_snippets = get_evidence_snippets(
            scenario_type, mechanism, polarity, biz_summary
        )
        
        # Evidence Source
        if text_match_weight > 0:
            evidence = get_evidence_source(
                'DART_BIZ_SUMMARY',
                f"매칭률: {text_match_weight:.0%}"
            )
        else:
            evidence = get_evidence_source('SECTOR_MAPPING')
        
        results.append({
            'ticker': company['ticker'],
            'name': company['name'],
            'market_cap': company.get('market_cap'),  # V1.5.5: 시가총액 (설명용 메타)
            'sector': company.get('sector_l1', ''),
            'value_chain': company.get('value_chain', ''),
            'impact': {
                'direction': polarity,
                'mechanism': mechanism,
                'mechanism_kr': {
                    'PRODUCT_PRICE': '판가 채널',
                    'INPUT_COST': '원가 채널',
                    'SPREAD': '스프레드 채널',
                    'DEMAND': '수요 채널',
                    'MACRO_SENSITIVITY': '거시 민감도',
                }.get(mechanism, mechanism),
                'weight': weight,
                'confidence': confidence,  # 모델 확신도
                'exposure': exposure,      # 기업 노출도 (V1.5.3)
            },
            'reasoning': reasoning_with_confidence,
            'evidence': {
                'source': evidence,
                'snippets': evidence_snippets,  # 증거 문장 (V1.5.3)
            },
        })
    
    output = {
        'scenario': {
            'variable': scenario_type,
            'variable_kr': var_name,
            'direction': direction,
            'direction_kr': direction_kr,
            'title': f"{var_name} {direction_kr} 시나리오",
        },
        'affected_companies': results,
        'generated_at': __import__('datetime').datetime.utcnow().isoformat(),
        'kg_version': 'v1.5.3',
    }
    
    # 비교 출력 추가 (V1.5.3)
    if include_comparison:
        output['comparison'] = generate_comparison_output(
            scenario_type, affected_companies, top_n=3
        )
    
    return output


if __name__ == '__main__':
    print_validation_report()

