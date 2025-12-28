# -*- coding: utf-8 -*-
"""
KRX 업종 기반 섹터 사전 필터 (v2)

⭐ 조건부 강화 필터 방식:
1. "명확한" KRX 업종만 강하게 적용 (confidence 높음)
2. "광범위한" KRX 업종은 참고만 (confidence 낮음)
3. 기업명 패턴과 조합하여 정확도 향상
"""
import logging
import re
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)

# ============================================================================
# Tier 1: 명확한 KRX 업종 (높은 신뢰도 - 0.8)
# 이 업종은 거의 확실하게 해당 섹터로 분류됨
# ============================================================================
KRX_TIER1_EXACT = {
    # SEC_FASHION (섬유/의류) - 매우 명확
    '봉제의복 제조업': ('SEC_FASHION', 'FASHION_OEM'),
    '직물직조 및 직물제품 제조업': ('SEC_FASHION', 'TEXTILE'),
    '방적 및 가공사 제조업': ('SEC_FASHION', 'TEXTILE'),
    '섬유제품 염색, 정리 및 마무리 가공업': ('SEC_FASHION', 'TEXTILE'),
    
    # SEC_AUTO (자동차) - 매우 명확
    '자동차 제조업': ('SEC_AUTO', 'OEM'),
    '자동차 신품 부품 제조업': ('SEC_AUTO', 'PARTS'),
    
    # SEC_SHIP (조선) - 매우 명확
    '선박 및 보트 건조업': ('SEC_SHIP', 'SHIPBUILDING'),
    
    # SEC_CONST (건설) - 매우 명확
    '건물 건설업': ('SEC_CONST', 'CONSTRUCTION'),
    '토목 건설업': ('SEC_CONST', 'CIVIL'),
    
    # SEC_BANK (은행) - 매우 명확
    '은행업': ('SEC_BANK', 'BANK'),
    
    # SEC_INS (보험) - 매우 명확
    '생명 보험업': ('SEC_INS', 'LIFE'),
    '손해 보험업': ('SEC_INS', 'NON_LIFE'),
    
    # SEC_TRAVEL (여행) - 매우 명확
    '항공 여객 운송업': ('SEC_TRAVEL', 'AIRLINE'),
    '호텔업': ('SEC_TRAVEL', 'HOTEL_RESORT'),
    
    # SEC_GAME (게임) - 매우 명확
    '게임 소프트웨어 개발 및 공급업': ('SEC_GAME', 'GAME'),
}

# ============================================================================
# Tier 2: 중간 신뢰도 KRX 업종 (0.5)
# 대체로 맞지만 예외가 있을 수 있음
# ============================================================================
KRX_TIER2_MODERATE = {
    # SEC_BIO (제약) - 바이오벤처는 다를 수 있음
    '의약품 제조업': ('SEC_BIO', 'PHARMA'),
    '기초 의약 물질 및 생물학적 제제 제조업': ('SEC_BIO', 'BIOPHARMA'),
    
    # SEC_MEDDEV (의료기기)
    '의료용 기기 제조업': ('SEC_MEDDEV', 'MEDDEV'),
    
    # SEC_STEEL (철강)
    '1차 철강 제조업': ('SEC_STEEL', 'STEEL'),
    
    # SEC_FOOD (식품)
    '도축, 육류 가공 및 저장 처리업': ('SEC_FOOD', 'FOOD'),
    '수산물 가공 및 저장 처리업': ('SEC_FOOD', 'FOOD'),
    '낙농제품 및 식용빙과류 제조업': ('SEC_FOOD', 'FOOD'),
    '알코올음료 제조업': ('SEC_FOOD', 'BEVERAGE'),
    '비알코올음료 및 얼음 제조업': ('SEC_FOOD', 'BEVERAGE'),
    
    # SEC_TELECOM (통신)
    '전기 통신업': ('SEC_TELECOM', 'TELECOM'),
    
    # SEC_UTIL (유틸리티)
    '전기업': ('SEC_UTIL', 'POWER'),
    '연료용 가스 제조 및 배관공급업': ('SEC_UTIL', 'GAS'),
    
    # SEC_ENT (엔터)
    '영화, 비디오물, 방송프로그램 제작업': ('SEC_ENT', 'CONTENT'),
    '방송업': ('SEC_ENT', 'MEDIA'),
    
    # SEC_FASHION (소매)
    '섬유, 의복, 신발 및 가죽제품 소매업': ('SEC_FASHION', 'FASHION_BRAND'),
    
    # SEC_RETAIL (유통)
    '종합 소매업': ('SEC_RETAIL', 'DEPARTMENT'),
}

# ============================================================================
# Tier 3: 낮은 신뢰도 KRX 업종 (0.2) - 참고만
# 너무 광범위하거나 다양한 기업이 속함
# ============================================================================
KRX_TIER3_WEAK = {
    # 너무 광범위함
    '기타 화학제품 제조업': None,  # 화장품, 페인트, 접착제 등 다양
    '기타 금융업': None,  # 지주, SPAC, 캐피탈 등 다양
    '전자부품 제조업': ('SEC_ELECTRONICS', 'COMPONENTS'),  # 반도체/디스플레이/PCB 혼재
    '소프트웨어 개발 및 공급업': ('SEC_IT', 'SOFTWARE'),
    '기타 식품 제조업': ('SEC_FOOD', 'FOOD'),
}

# ============================================================================
# 기업명 패턴 기반 추가 필터
# ============================================================================
COMPANY_NAME_PATTERNS = {
    # 지주회사 패턴 (명확)
    '지주': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING', 0.7),
    '홀딩스': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING', 0.7),
    '홀딩': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING', 0.6),
    'Holdings': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING', 0.6),
    
    # SPAC 패턴 (섹터 분류 제외)
    '스팩': ('SPAC', None, 0.9),
    'SPAC': ('SPAC', None, 0.9),
    '기업인수목적': ('SPAC', None, 0.9),
}

# ============================================================================
# 사업지주 탐지 (회사명에 패턴 없지만 실제 지주회사)
# 조건: KRX "기타 금융업" + 키워드에 "지주회사" 또는 배당 관련
# ============================================================================
KRX_HOLDING_INDICATORS = ['기타 금융업', '회사 본부 및 경영 컨설팅 서비스업']
HOLDING_KEYWORD_INDICATORS = ['#지주회사', '지주회사', '배당금수익', '배당수익', '임대수익', '로열티']

# ============================================================================
# 섹터별 Block List (명확한 금지 조합)
# 해당 KRX 업종이면 해당 섹터로 분류 금지
# ============================================================================
SECTOR_BLOCK_LIST = {
    'SEC_COSMETIC': [
        '봉제의복 제조업',           # → SEC_FASHION
        '직물직조 및 직물제품 제조업',  # → SEC_FASHION
        '방적 및 가공사 제조업',       # → SEC_FASHION
        '의약품 제조업',              # → SEC_BIO
    ],
    'SEC_FASHION': [
        '기타 화학제품 제조업',        # → SEC_COSMETIC 또는 SEC_CHEM
        '의약품 제조업',              # → SEC_BIO
    ],
    'SEC_BIO': [
        '봉제의복 제조업',            # → SEC_FASHION
    ],
}


def filter_sector_by_krx(
    industry_raw: Optional[str],
    company_name: Optional[str] = None,
    keywords: Optional[list] = None
) -> Tuple[Optional[str], Optional[str], float]:
    """
    KRX 업종 기반 섹터 사전 필터링 (v2)
    
    Args:
        industry_raw: KRX 업종명 (stocks.industry_raw)
        company_name: 기업명 (선택, 지주회사/SPAC 판별용)
        keywords: 기업 키워드 리스트 (선택, 사업지주 판별용)
    
    Returns:
        (major_sector, sub_sector, confidence)
        - Tier 1 매칭: confidence 0.8
        - Tier 2 매칭: confidence 0.5
        - Tier 3 매칭: confidence 0.2
        - 기업명 패턴: confidence 0.6~0.9
        - 사업지주 탐지: confidence 0.65
        - 매핑 실패: (None, None, 0.0)
    """
    # 1. 기업명 패턴 우선 체크 (지주회사, SPAC)
    if company_name:
        for pattern, (sector, sub, conf) in COMPANY_NAME_PATTERNS.items():
            if pattern in company_name:
                logger.info(f"[KRX 필터] 기업명 '{company_name}' → {sector} (패턴: {pattern}, conf: {conf})")
                return sector, sub, conf
    
    # 2. 사업지주 탐지 (회사명 패턴 없지만 KRX + 키워드로 판별)
    # P1-3: CJ, 두산 같은 기업 탐지
    if industry_raw and keywords:
        industry_stripped = industry_raw.strip()
        if industry_stripped in KRX_HOLDING_INDICATORS:
            # KRX가 지주 관련 업종인 경우, 키워드에서 확인
            for kw in keywords:
                if isinstance(kw, str) and any(ind in kw for ind in HOLDING_KEYWORD_INDICATORS):
                    logger.info(f"[KRX 필터] 사업지주 탐지 '{company_name}' → SEC_HOLDING "
                               f"(KRX: {industry_stripped}, 키워드: {kw})")
                    return 'SEC_HOLDING', 'INDUSTRIAL_HOLDING', 0.65
    
    if not industry_raw:
        return None, None, 0.0
    
    industry_raw = industry_raw.strip()
    
    # 2. Tier 1: 명확한 매칭 (confidence 0.8)
    if industry_raw in KRX_TIER1_EXACT:
        result = KRX_TIER1_EXACT[industry_raw]
        if result:
            major, sub = result
            logger.info(f"[KRX 필터] '{industry_raw}' → {major}/{sub} (Tier1, conf: 0.8)")
            return major, sub, 0.8
    
    # 3. Tier 2: 중간 매칭 (confidence 0.5)
    if industry_raw in KRX_TIER2_MODERATE:
        result = KRX_TIER2_MODERATE[industry_raw]
        if result:
            major, sub = result
            logger.info(f"[KRX 필터] '{industry_raw}' → {major}/{sub} (Tier2, conf: 0.5)")
            return major, sub, 0.5
    
    # 4. Tier 3: 약한 매칭 (confidence 0.2)
    if industry_raw in KRX_TIER3_WEAK:
        result = KRX_TIER3_WEAK[industry_raw]
        if result:
            major, sub = result
            logger.info(f"[KRX 필터] '{industry_raw}' → {major}/{sub} (Tier3, conf: 0.2)")
            return major, sub, 0.2
    
    logger.debug(f"[KRX 필터] '{industry_raw}' → 매핑 없음")
    return None, None, 0.0


def get_krx_weight_for_ensemble(krx_confidence: float) -> float:
    """
    KRX confidence에 따른 ENSEMBLE 가중치 반환
    
    - Tier 1 (conf 0.8): 가중치 0.3 (강하게 반영)
    - Tier 2 (conf 0.5): 가중치 0.2 (보통)
    - Tier 3 (conf 0.2): 가중치 0.1 (약하게 반영)
    - 기업명 패턴: 가중치 0.25
    """
    if krx_confidence >= 0.8:
        return 0.3  # Tier 1: 강하게
    elif krx_confidence >= 0.5:
        return 0.2  # Tier 2: 보통
    elif krx_confidence >= 0.2:
        return 0.1  # Tier 3: 약하게
    else:
        return 0.0


def should_override_ensemble(
    krx_sector: Optional[str],
    krx_confidence: float,
    ensemble_sector: Optional[str],
    ensemble_confidence: str
) -> bool:
    """
    KRX 결과가 ENSEMBLE 결과를 덮어써야 하는지 판단
    
    Returns:
        True: KRX 결과 사용
        False: ENSEMBLE 결과 사용
    """
    if not krx_sector:
        return False
    
    # SPAC은 항상 우선
    if krx_sector == 'SPAC':
        return True
    
    # Tier 1 (conf >= 0.8) + ENSEMBLE LOW → KRX 우선
    if krx_confidence >= 0.8 and ensemble_confidence == "LOW":
        return True
    
    # Tier 1 + ENSEMBLE이 완전히 다른 섹터 → 충돌 로그 (덮어쓰지는 않음)
    if krx_confidence >= 0.8 and krx_sector != ensemble_sector:
        logger.warning(f"[KRX vs ENSEMBLE 충돌] KRX: {krx_sector} (conf: {krx_confidence}) vs ENSEMBLE: {ensemble_sector} (conf: {ensemble_confidence})")
    
    return False


def apply_krx_filter_to_candidates(
    candidates: list,
    industry_raw: Optional[str],
    company_name: Optional[str] = None,
    keywords: Optional[list] = None
) -> list:
    """
    ENSEMBLE 후보 리스트에 KRX 가중치 적용
    
    1. KRX 섹터와 일치하는 후보의 점수에 가산점 부여
    2. Block list에 해당하는 후보는 점수 감점
    """
    krx_sector, krx_sub, krx_conf = filter_sector_by_krx(industry_raw, company_name, keywords)
    
    if not krx_sector or krx_sector == 'SPAC':
        return candidates
    
    krx_weight = get_krx_weight_for_ensemble(krx_conf)
    industry_stripped = industry_raw.strip() if industry_raw else None
    
    for candidate in candidates:
        candidate_sector = candidate.get('sector')
        
        # 1. KRX 섹터와 일치하면 가산점
        if candidate_sector == krx_sector:
            original_score = candidate.get('score', 0.0)
            boosted_score = original_score + krx_weight
            candidate['score'] = min(boosted_score, 1.0)
            candidate['krx_boosted'] = True
            candidate['krx_confidence'] = krx_conf
            logger.debug(f"[KRX 부스트] {krx_sector}: {original_score:.3f} → {candidate['score']:.3f}")
        
        # 2. Block list 체크 - 해당 KRX면 특정 섹터 감점
        if industry_stripped and candidate_sector in SECTOR_BLOCK_LIST:
            blocked_krx = SECTOR_BLOCK_LIST[candidate_sector]
            if industry_stripped in blocked_krx:
                original_score = candidate.get('score', 0.0)
                penalty = 0.3  # 감점
                candidate['score'] = max(original_score - penalty, 0.0)
                candidate['krx_blocked'] = True
                logger.info(f"[KRX 블록] {candidate_sector} 감점 "
                           f"(KRX: {industry_stripped}): {original_score:.3f} → {candidate['score']:.3f}")
    
    # 점수 재정렬
    candidates.sort(key=lambda x: x.get('score', 0.0), reverse=True)
    
    return candidates


def is_sector_blocked_by_krx(
    sector: str,
    industry_raw: Optional[str]
) -> bool:
    """
    해당 섹터가 KRX 업종에 의해 블록되는지 확인
    
    Returns:
        True: 해당 섹터로 분류하면 안 됨
        False: 분류 가능
    """
    if not industry_raw or sector not in SECTOR_BLOCK_LIST:
        return False
    
    industry_stripped = industry_raw.strip()
    blocked_krx = SECTOR_BLOCK_LIST[sector]
    
    return industry_stripped in blocked_krx


def classify_holding_type(
    company_name: Optional[str],
    revenue_by_segment: Optional[Dict[str, Any]] = None,
    keywords: Optional[list] = None
) -> str:
    """
    지주회사 유형 분류 (3분류 체계)
    
    Returns:
        'FINANCIAL_HOLDING': 금융지주 (은행/보험/증권 자회사)
        'PURE_HOLDING': 순수지주 (배당/임대/로열티 비중 ≥50%)
        'BUSINESS_HOLDING': 사업지주 (자체 사업부문 비중 ≥50%)
    """
    # 1. 금융지주 판정 (회사명 또는 키워드 기반)
    financial_keywords = ['금융지주', 'KB금융', '신한지주', '하나금융지주', '우리금융지주', 
                         '메리츠금융지주', '은행지주', '보험지주', '금융그룹']
    
    if company_name:
        for fk in financial_keywords:
            if fk in company_name:
                return 'FINANCIAL_HOLDING'
    
    if keywords:
        for kw in keywords:
            if isinstance(kw, str) and any(fk in kw for fk in financial_keywords):
                return 'FINANCIAL_HOLDING'
    
    # 2. 매출 비중 기반 분류 (순수지주 vs 사업지주)
    if revenue_by_segment and isinstance(revenue_by_segment, dict):
        holding_keywords = ['배당', '임대', '로열티', '브랜드', '상표권', '지주', '투자부문']
        business_keywords = ['사업부문', '제조', '서비스', '유통', '판매', '영업', '생산']
        
        holding_pct = 0.0
        business_pct = 0.0
        
        for segment, pct in revenue_by_segment.items():
            if not isinstance(pct, (int, float)):
                continue
            
            segment_lower = segment.lower()
            if any(kw in segment for kw in holding_keywords):
                holding_pct += pct
            elif any(kw in segment for kw in business_keywords):
                business_pct += pct
            else:
                # 기타 부문은 사업으로 간주
                business_pct += pct
        
        # 순수지주: 배당/임대/로열티 비중 ≥50%
        if holding_pct >= 50:
            return 'PURE_HOLDING'
        
        # 사업지주: 사업부문 비중 ≥50%
        if business_pct >= 50:
            return 'BUSINESS_HOLDING'
    
    # 기본값: 사업지주
    return 'BUSINESS_HOLDING'


def detect_holding_company(
    company_name: Optional[str],
    industry_raw: Optional[str],
    keywords: Optional[list] = None,
    products: Optional[list] = None,
    revenue_by_segment: Optional[Dict[str, Any]] = None,
    company_detail: Optional[Any] = None
) -> Tuple[bool, float, str, str]:
    """
    지주회사 종합 판정 (P0: 매출 비중 기반 최종 판정)
    
    조건 결합:
    1. 회사명 패턴 (지주/홀딩스) → 단독 가능
    2. KRX (기타 금융업) + 키워드 (지주회사/배당) → 결합 시만
    3. 제품 (배당금수익/임대수익) → 결합 시만
    4. ⭐ 매출 비중 (배당/임대/로열티 합계 >= 50%) → 확정
    
    Returns:
        (is_holding, confidence, reason, holding_type)
        holding_type: 'FINANCIAL_HOLDING' | 'PURE_HOLDING' | 'BUSINESS_HOLDING'
    """
    signals = []
    
    # Signal 0: 매출 비중 기반 판정 (가장 강력, P0)
    revenue_score = 0.0
    holding_revenue_pct = 0.0
    if revenue_by_segment and isinstance(revenue_by_segment, dict):
        holding_keywords = ['배당', '임대', '로열티', '브랜드', '상표권', '지주', '투자']
        for segment, pct in revenue_by_segment.items():
            if isinstance(pct, (int, float)) and any(kw in segment for kw in holding_keywords):
                holding_revenue_pct += pct
        
        if holding_revenue_pct >= 50:
            revenue_score = 0.5
            signals.append(f"매출비중({holding_revenue_pct:.1f}%)")
        elif holding_revenue_pct >= 30:
            revenue_score = 0.3
            signals.append(f"매출비중({holding_revenue_pct:.1f}%)")
    
    # Signal 1: 회사명 패턴 (가장 강력)
    name_pattern_score = 0.0
    if company_name:
        for pattern in ['지주', '홀딩스', '홀딩', 'Holdings']:
            if pattern in company_name:
                name_pattern_score = 0.6
                signals.append(f"회사명패턴({pattern})")
                break
    
    # Signal 2: KRX 업종
    krx_score = 0.0
    if industry_raw:
        industry = industry_raw.strip()
        if industry in KRX_HOLDING_INDICATORS:
            krx_score = 0.3
            signals.append(f"KRX({industry[:15]})")
    
    # Signal 3: 키워드
    keyword_score = 0.0
    if keywords:
        for kw in keywords:
            if isinstance(kw, str) and any(ind in kw for ind in ['지주회사', '#지주회사']):
                keyword_score = 0.3
                signals.append(f"키워드({kw})")
                break
    
    # Signal 4: 제품/서비스 (배당, 임대)
    product_score = 0.0
    if products:
        holding_products = ['배당금수익', '배당수익', '임대수익', '로열티', '브랜드사용료', '상표권']
        for prod in products:
            if isinstance(prod, str) and any(hp in prod for hp in holding_products):
                product_score = 0.2
                signals.append(f"제품({prod[:10]})")
                break
    
    # 🆕 Signal 5: 연결 재무제표 구조 (biz_summary 기반) - P0 개선
    consolidated_structure_score = 0.0
    subsidiary_count = 0
    control_keyword_hits = 0
    consolidated_evidence = {}
    
    if company_detail and hasattr(company_detail, 'biz_summary') and company_detail.biz_summary:
        biz_summary = str(company_detail.biz_summary)
        
        # 가드레일 #1: 자회사 패턴 다양도 (2개 이상)
        subsidiary_patterns = [
            r'[가-힣A-Za-z0-9\s]+㈜',  # SK온㈜
            r'\[[가-힣A-Za-z0-9\s]+㈜\]',  # [SK온㈜]
            r'\(주\)[가-힣A-Za-z0-9\s]+',  # (주)SK온
        ]
        subsidiary_matches = set()
        for pattern in subsidiary_patterns:
            matches = re.findall(pattern, biz_summary)
            subsidiary_matches.update(matches)
        subsidiary_count = len(subsidiary_matches)
        
        # 가드레일 #2: 지배구조 키워드 동반
        control_keywords = [
            '지배', '종속', '자회사', '계열사', '연결대상', '지분율', 
            '최대주주', '계열회사', '연결조정', '종속회사', '관리', '경영'
        ]
        control_keyword_hits = sum(1 for kw in control_keywords if kw in biz_summary)
        
        # 연결 키워드
        consolidated_keywords = ['연결 재무제표', '연결매출', '자회사', '계열사', '종속회사']
        has_consolidated_keyword = any(kw in biz_summary for kw in consolidated_keywords)
        
        # 가드레일 통과 조건: 자회사 2개 이상 + 지배구조 키워드 1개 이상
        if subsidiary_count >= 2 and control_keyword_hits >= 1:
            consolidated_structure_score = 0.4
            signals.append(f"연결재무제표구조(자회사{subsidiary_count}개,지배구조키워드{control_keyword_hits}개)")
            consolidated_evidence = {
                'subsidiary_count': subsidiary_count,
                'control_keyword_hits': control_keyword_hits,
                'has_consolidated_keyword': has_consolidated_keyword
            }
        elif subsidiary_count >= 1 and has_consolidated_keyword and control_keyword_hits >= 1:
            # 완화 조건: 자회사 1개 + 연결 키워드 + 지배구조 키워드
            consolidated_structure_score = 0.3
            signals.append(f"연결재무제표구조(자회사{subsidiary_count}개,지배구조키워드{control_keyword_hits}개)")
            consolidated_evidence = {
                'subsidiary_count': subsidiary_count,
                'control_keyword_hits': control_keyword_hits,
                'has_consolidated_keyword': has_consolidated_keyword
            }
    
    # 판정 로직
    total_score = revenue_score + name_pattern_score + krx_score + keyword_score + product_score + consolidated_structure_score
    
    # signal_count 계산 (consolidated_structure_score 포함) - P0 개선
    signal_count = (
        (1 if krx_score > 0 else 0) + 
        (1 if keyword_score > 0 else 0) + 
        (1 if product_score > 0 else 0) + 
        (1 if revenue_score > 0 else 0) +
        (1 if consolidated_structure_score > 0 else 0)  # 🆕 추가
    )
    
    # ⭐ P0: 매출 비중 >= 50%면 확정 (HOLDCO/HOLDING_PURE 가능)
    if revenue_score >= 0.5:
        holding_type = classify_holding_type(company_name, revenue_by_segment, keywords)
        return True, min(total_score, 1.0), "+".join(signals), holding_type
    
    # 회사명 패턴 있으면 단독으로 판정 가능 (HOLDCO/HOLDING_PURE 가능)
    if name_pattern_score >= 0.5:
        holding_type = classify_holding_type(company_name, revenue_by_segment, keywords)
        return True, min(total_score, 1.0), "+".join(signals), holding_type
    
    # 🆕 P0 개선: consolidated_structure_score만 있으면 BIZ_HOLDCO로만 승격 (단독 HOLDCO 금지)
    if consolidated_structure_score > 0 and signal_count == 1:
        # consolidated_structure_score만 있으면 BUSINESS_HOLDING으로만 제한
        return True, min(total_score, 0.6), "+".join(signals), 'BUSINESS_HOLDING'
    
    # 회사명 패턴 없으면 최소 2개 signal 필요 (HOLDCO/HOLDING_PURE 가능)
    if signal_count >= 2 and total_score >= 0.4:
        holding_type = classify_holding_type(company_name, revenue_by_segment, keywords)
        return True, min(total_score, 1.0), "+".join(signals), holding_type
    
    return False, 0.0, "", ""

