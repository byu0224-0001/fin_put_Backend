"""
문장 기반 드라이버 시그널 추출기 (MVP 최종 버전)

KF-DeBERTa-multitask를 사용하여 문장 단위로 P/Q/C 드라이버 시그널 추출
- 문장 기반 P/Q/C 자동 태깅
- 방향성 추출
- 문장 필터링
- 동적 Threshold
- 타입별/전체 Top-K 제한
"""
import logging
import warnings
import numpy as np
from typing import Dict, List, Optional, Any, Tuple

# Pecab RuntimeWarning 무시 (overflow는 경고일 뿐 실제 오류 아님)
warnings.filterwarnings('ignore', category=RuntimeWarning, module='pecab')
from app.utils.text_chunking import split_into_sentences
# KF-DeBERTa 모델은 제거됨 (Solar Embedding으로 대체)
# from app.services.embedding_model_direct import get_direct_embedding_model

def get_direct_embedding_model():
    """KF-DeBERTa 모델은 제거됨 - None 반환"""
    logger.warning("KF-DeBERTa 모델은 제거되었습니다. Solar Embedding을 사용하세요.")
    return None
from app.models.sector_reference import (
    SECTOR_L2_DEFINITIONS,
    ECONVAR_MASTER
)

# Backward compatibility alias
SUB_SECTOR_DEFINITIONS = SECTOR_L2_DEFINITIONS

logger = logging.getLogger(__name__)

# 드라이버 임베딩 캐시 (모듈 레벨)
_driver_embedding_cache: Dict[str, Tuple[np.ndarray, Dict]] = {}

# =============================================================================
# P/Q/C 키워드 정의 (문장 타입 분류용)
# =============================================================================

P_Q_C_KEYWORDS = {
    'P': [
        '가격', '단가', 'ASP', '스프레드', '프리미엄', '원/', '달러/', 
        '상승', '하락', '인상', '인하', '가격변동', '가격상승', '가격하락',
        '고정거래가격', '거래가격', '판매가격', '매출단가', '판매단가'
    ],
    'Q': [
        '수요', '발주', '출하', '판매량', '가동률', '수출', '수입', 
        '증가', '감소', '확대', '축소', '수주', '발주량', '출하량',
        '생산량', '수요증가', '수요감소', '발주증가', '출하증가', '판매량증가'
    ],
    'C': [
        '원가', '비용', '수율', '효율', '마진', '원가율', '개선', '악화',
        'CAPEX', '투자', '원가상승', '원가하락', '마진개선', '마진악화',
        '수율개선', '수율악화', '비용절감', '비용증가', '영업이익률'
    ]
}

# 타입별/전체 Top-K 제한 상수
MAX_DRIVERS_PER_TYPE = 3  # P/Q/C 타입별 최대 3개 드라이버
MAX_TOTAL_EVIDENCE = 9    # 전체 evidence 문장 최대 9개

# =============================================================================
# 헬퍼 함수들
# =============================================================================

def classify_sentence_type(sentence: str) -> Optional[str]:
    """
    문장 자체의 P/Q/C 타입 판단 (정규식 기반)
    
    Args:
        sentence: 분석할 문장
    
    Returns:
        'P', 'Q', 'C', 또는 None (불명확한 경우)
    """
    sentence_lower = sentence.lower()
    
    # 각 타입별 키워드 매칭 점수
    p_score = sum(1 for kw in P_Q_C_KEYWORDS['P'] if kw in sentence_lower)
    q_score = sum(1 for kw in P_Q_C_KEYWORDS['Q'] if kw in sentence_lower)
    c_score = sum(1 for kw in P_Q_C_KEYWORDS['C'] if kw in sentence_lower)
    
    # 가장 높은 점수의 타입 반환 (동점이면 None)
    scores = {'P': p_score, 'Q': q_score, 'C': c_score}
    max_score = max(scores.values())
    
    if max_score == 0:
        return None
    
    # 동점 처리: 여러 타입이 같으면 None (불명확)
    max_types = [t for t, s in scores.items() if s == max_score]
    if len(max_types) > 1:
        return None
    
    return max_types[0]


def extract_direction(sentence: str) -> Optional[str]:
    """
    문장에서 방향성(상승/하락) 추출
    
    Args:
        sentence: 분석할 문장
    
    Returns:
        '증가', '감소', 또는 None (불명확한 경우)
    """
    sentence_lower = sentence.lower()
    
    increase_keywords = ['증가', '상승', '확대', '증대', '인상', '개선', '향상', '증가세', '상승세', '확장']
    decrease_keywords = ['감소', '하락', '축소', '인하', '악화', '감소세', '하락세', '둔화', '축소']
    
    has_increase = any(kw in sentence_lower for kw in increase_keywords)
    has_decrease = any(kw in sentence_lower for kw in decrease_keywords)
    
    if has_increase and not has_decrease:
        return '증가'
    elif has_decrease and not has_increase:
        return '감소'
    else:
        return None  # 불명확하거나 둘 다 포함


def filter_candidate_sentences(
    sentences: List[str],
    sentence_sources: List[Tuple[str, str]]
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    공시스러운 문장만 필터링
    
    필터 기준:
    - 길이: 20~200자 사이
    - 숫자 포함 (선택적, 완화)
    - P/Q/C 키워드 포함
    
    Args:
        sentences: 원본 문장 리스트
        sentence_sources: 문장 출처 리스트
    
    Returns:
        (필터링된 문장 리스트, 필터링된 출처 리스트)
    """
    filtered_sentences = []
    filtered_sources = []
    all_keywords = set()
    for kw_list in P_Q_C_KEYWORDS.values():
        all_keywords.update(kw_list)
    
    for sentence, source in zip(sentences, sentence_sources):
        # 길이 체크
        if not (20 <= len(sentence) <= 200):
            continue
        
        # 숫자 포함 여부 (완화: 선택적)
        has_number = any(ch.isdigit() for ch in sentence)
        
        # P/Q/C 키워드 포함 여부
        has_keyword = any(kw in sentence for kw in all_keywords)
        
        # 숫자 또는 키워드 중 하나라도 있으면 통과 (완화된 기준)
        if has_number or has_keyword:
            filtered_sentences.append(sentence)
            filtered_sources.append(source)
    
    return filtered_sentences, filtered_sources


def get_fallback_drivers_by_l1(major_sector: str) -> List[str]:
    """
    L1 섹터 기반 보수적 드라이버 풀 (Fallback용)
    
    L2 기반 드라이버 추출 실패 시 사용
    """
    L1_FALLBACK_DRIVERS = {
        'SEC_SEMI': ['DRAM_ASP', 'NAND_ASP', 'HBM_DEMAND', 'EXCHANGE_RATE_USD_KRW', 'SEMICONDUCTOR_CAPEX'],
        'SEC_AUTO': ['AUTO_SALES_KR', 'EV_SALES', 'EXCHANGE_RATE_USD_KRW', 'STEEL_PRICE'],
        'SEC_BATTERY': ['LITHIUM_PRICE', 'COBALT_PRICE', 'EV_SALES', 'ESS_DEMAND'],
        'SEC_IT': ['IT_SPENDING', 'CLOUD_GROWTH', 'SOFTWARE_DEMAND'],
        'SEC_FINANCE': ['INTEREST_RATE', 'LOAN_DEMAND', 'CARD_SPENDING'],
        'SEC_RETAIL': ['CONSUMER_SPENDING', 'RETAIL_SALES', 'E_COMMERCE_GROWTH'],
        'SEC_CHEM': ['NAPHTHA_PRICE', 'OIL_PRICE', 'PETROCHEMICAL_SPREAD'],
        'SEC_STEEL': ['STEEL_PRICE', 'IRON_ORE_PRICE', 'CONSTRUCTION_ACTIVITY'],
        'SEC_CONST': ['CONSTRUCTION_ACTIVITY', 'HOUSING_START', 'INTEREST_RATE'],
        'SEC_SHIP': ['BDI_INDEX', 'NEWBUILD_ORDER', 'SHIPPING_RATE'],
        'SEC_BIO': ['PHARMA_RND', 'FDA_APPROVAL', 'HEALTHCARE_SPENDING'],
        'SEC_GAME': ['GAME_USER_TRAFFIC', 'NEW_GAME_LAUNCH', 'MOBILE_APP_SALES'],
        'SEC_MEDIA': ['AD_SPENDING', 'CONTENT_EXPORT', 'BOX_OFFICE'],
        'SEC_TRAVEL': ['OUTBOUND_TOURISTS', 'AIR_PASSENGER_TRAFFIC', 'HOTEL_OCCUPANCY'],
        'SEC_FOOD': ['FOOD_PRICE_INDEX', 'GRAIN_PRICE', 'EXCHANGE_RATE_USD_KRW'],
        'SEC_FASHION': ['CLOTHING_SALES', 'CONSUMER_SENTIMENT', 'COTTON_PRICE'],
        'SEC_COSMETIC': ['COSMETIC_EXPORT', 'CHINA_CONSUMPTION', 'DUTY_FREE_SALES'],
        'SEC_MACH': ['MACHINERY_ORDER', 'CAPEX_INVESTMENT', 'EXCHANGE_RATE_USD_KRW'],
        'SEC_DEFENSE': ['DEFENSE_BUDGET', 'ARMS_EXPORT', 'GEOPOLITICAL_RISK'],
        'SEC_UTIL': ['ELECTRICITY_DEMAND', 'SMP_PRICE', 'LNG_PRICE'],
        'SEC_TELECOM': ['ARPU_MOBILE', '5G_PENETRATION', 'MARKETING_COST'],
    }
    
    drivers = L1_FALLBACK_DRIVERS.get(major_sector, [])
    if drivers:
        logger.info(f"[L1 Fallback] {major_sector} → {len(drivers)}개 드라이버 제공 (보수적 풀)")
    return drivers


def get_candidate_drivers(
    major_sector: str,
    sub_sector: Optional[str]
) -> List[str]:
    """
    섹터/서브섹터 컨텍스트에 맞는 드라이버만 수집
    
    절대 "모든 섹터의 모든 드라이버"와 비교하지 않도록 보장
    
    Args:
        major_sector: Major Sector 코드
        sub_sector: Sub-sector 코드
    
    Returns:
        드라이버 코드 리스트
    """
    standard_drivers = []
    
    # 1) SECTOR_L2_DEFINITIONS에서 해당 섹터의 드라이버만 가져오기
    if major_sector in SUB_SECTOR_DEFINITIONS:
        if sub_sector and sub_sector in SUB_SECTOR_DEFINITIONS[major_sector]:
            sub_def = SUB_SECTOR_DEFINITIONS[major_sector][sub_sector]
            # ✅ 수정: recommended_drivers + common_drivers 사용 (drivers 필드 없음)
            recommended = sub_def.get('recommended_drivers', [])
            common = sub_def.get('common_drivers', [])
            standard_drivers = list(set(recommended + common))
            logger.debug(f"표준 드라이버 수집(L2): {sub_sector} → {len(standard_drivers)}개")
        else:
            # sub_sector가 None이면 major_sector의 모든 sub_sector 드라이버 수집
            logger.info(f"Sub-sector 정의 없음 ({major_sector}/{sub_sector}), major_sector의 모든 드라이버 수집")
            all_drivers = set()
            for sub_def in SUB_SECTOR_DEFINITIONS[major_sector].values():
                all_drivers.update(sub_def.get('recommended_drivers', []))
                all_drivers.update(sub_def.get('common_drivers', []))
            standard_drivers = list(all_drivers)
            logger.debug(f"Major sector 드라이버 수집(L2 종합): {major_sector} → {len(standard_drivers)}개")
    
    # 2) ✅ L1 기반 Fallback (L2에서 드라이버를 못 찾은 경우)
    if not standard_drivers:
        logger.warning(f"L2 드라이버 없음, L1 Fallback 시도: {major_sector}")
        standard_drivers = get_fallback_drivers_by_l1(major_sector)
    
    # 3) ECONVAR_MASTER에서 직접 찾기 (최종 Fallback, 최소한의 키워드 매칭)
    if not standard_drivers:
        logger.warning(f"L1 Fallback도 실패: {major_sector}/{sub_sector} → ECONVAR_MASTER 키워드 매칭 시도")
        # 섹터별 키워드 매칭 (최소한의 fallback)
        sector_keywords_map = {
            'SEC_CARD': ['카드', '결제', '신용', '체크'],
            'SEC_RETAIL': ['유통', '소매', '마트', '쇼핑'],
            'SEC_IT': ['IT', '소프트웨어', '클라우드', 'SaaS'],
            'SEC_SEMI': ['반도체', '메모리', 'DRAM', 'NAND', 'HBM'],
            'SEC_AUTO': ['자동차', '전기차', 'EV', '완성차'],
            'SEC_BATTERY': ['배터리', '2차전지', '양극재', '음극재'],
            'SEC_CHEM': ['화학', '석유화학', '정유', '나프타'],
            'SEC_STEEL': ['철강', '강판', '강재'],
        }
        
        if major_sector in sector_keywords_map:
            keywords = sector_keywords_map[major_sector]
            for driver_code, econvar_info in ECONVAR_MASTER.items():
                description = econvar_info.get('description', '') + ' ' + econvar_info.get('name_ko', '')
                if any(kw in description for kw in keywords):
                    standard_drivers.append(driver_code)
        
        if standard_drivers:
            logger.info(f"ECONVAR_MASTER에서 {len(standard_drivers)}개 드라이버 발견 (키워드 매칭)")
    
    if not standard_drivers:
        logger.error(f"[Step 4A] 드라이버 후보 수집 완전 실패: {major_sector}/{sub_sector}")
    else:
        logger.info(f"[Step 4A] 드라이버 후보: {len(standard_drivers)}개")
    
    return standard_drivers


def dedupe_similar_sentences(
    sentences: List[Dict[str, Any]],
    similarity_threshold: float = 0.95
) -> List[Dict[str, Any]]:
    """
    유사한 문장 중복 제거 (임베딩 기반)
    
    Args:
        sentences: 문장 딕셔너리 리스트 [{"text": "...", "similarity": 0.85, ...}, ...]
        similarity_threshold: 중복으로 판단할 유사도 임계값 (기본 0.95)
    
    Returns:
        중복 제거된 문장 리스트
    """
    if len(sentences) <= 1:
        return sentences
    
    try:
        # 문장 텍스트만 추출
        sentence_texts = [s["text"] for s in sentences]
        
        # 임베딩 생성
        model = get_direct_embedding_model()
        if model is None:
            logger.warning("임베딩 모델 로드 실패, 중복 제거 건너뜀")
            return sentences
        embeddings = model.encode(sentence_texts, convert_to_numpy=True)
        
        # 유사도 행렬 계산
        similarities = np.dot(embeddings, embeddings.T)
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        similarities = similarities / (norms * norms.T)
        
        # 중복 제거 (상위 문장 우선 유지)
        kept_indices = []
        for i in range(len(sentences)):
            is_duplicate = False
            for j in kept_indices:
                if similarities[i, j] >= similarity_threshold:
                    is_duplicate = True
                    break
            if not is_duplicate:
                kept_indices.append(i)
        
        return [sentences[i] for i in kept_indices]
    except Exception as e:
        logger.warning(f"문장 중복 제거 실패: {e}, 원본 반환")
        return sentences


def cap_signals_per_type(
    signals_by_type: Dict[str, Dict[str, Any]]
) -> Dict[str, List[Dict]]:
    """
    타입별/전체 Top-K 제한 적용 (evidence 중복 제거 포함)
    
    규칙:
    - 각 타입별 최대 3개 드라이버
    - 전체 합산 최대 9개 evidence 문장
    - evidence 문장 중복 제거 (유사도 0.95 이상)
    
    Args:
        signals_by_type: 타입별 드라이버 시그널 딕셔너리
    
    Returns:
        제한이 적용된 결과 딕셔너리
    """
    result = {
        "price_signals": [],
        "quantity_signals": [],
        "cost_signals": []
    }
    
    total_evidence_count = 0
    
    for signal_type in ["price_signals", "quantity_signals", "cost_signals"]:
        signals = signals_by_type[signal_type]
        
        # Score 기준 정렬
        sorted_signals = sorted(
            signals.values(),
            key=lambda x: x["score"],
            reverse=True
        )
        
        # 타입별 최대 3개까지 채우되, 전체 9개 제한 고려
        for signal in sorted_signals[:MAX_DRIVERS_PER_TYPE]:
            if total_evidence_count >= MAX_TOTAL_EVIDENCE:
                break
            
            # 각 드라이버의 상위 문장 선택 (남은 여유분 고려)
            remaining_slots = MAX_TOTAL_EVIDENCE - total_evidence_count
            candidate_sentences = sorted(
                signal["sentences"],
                key=lambda x: x["similarity"],
                reverse=True
            )[:min(5, remaining_slots + 2)]  # 중복 제거를 위해 여유분 확보
            
            # 🔥 신규: evidence 문장 중복 제거
            deduped_sentences = dedupe_similar_sentences(candidate_sentences)
            top_sentences = deduped_sentences[:min(3, remaining_slots)]
            
            if top_sentences:  # 문장이 있을 때만 추가
                # 방향성 추출 (첫 번째 문장 기준)
                direction = extract_direction(top_sentences[0]["text"]) if top_sentences else None
                
                result[signal_type].append({
                    "var": signal["var"],
                    "code": signal["code"],
                    "type": signal["type"],
                    "score": signal["score"],
                    "direction": direction,  # 🔥 신규: 방향성 추가
                    "evidence": [s["text"] for s in top_sentences],
                    "sentence_types": [classify_sentence_type(s["text"]) for s in top_sentences]  # 디버깅용
                })
                total_evidence_count += len(top_sentences)
    
    return result


def extract_driver_signals_from_sentences(
    company_detail: Any,  # CompanyDetail 객체
    major_sector: str,
    sub_sector: Optional[str] = None,
    sector_l2: Optional[str] = None  # ⭐ L2 정보 추가
) -> Dict[str, List[Dict]]:
    """
    문장 단위 임베딩 기반 P/Q/C 드라이버 시그널 추출 (MVP 최종 버전)
    
    프로세스:
    1. 텍스트 → 문장 단위 split
    2. 문장 필터링 (공시스러운 문장만)
    3. 각 문장을 KF-DeBERTa로 임베딩
    4. 섹터별 드라이버 후보 수집 (컨텍스트 한정)
    5. 드라이버 설명 임베딩
    6. 유사도 계산 및 동적 Threshold 적용
    7. 문장 타입과 드라이버 타입 정렬
    8. 타입별/전체 Top-K 제한
    
    Args:
        company_detail: CompanyDetail 객체
        major_sector: Major Sector 코드
        sub_sector: Sub-sector 코드
    
    Returns:
        {
            "price_signals": [
                {
                    "var": "DRAM ASP",
                    "code": "DRAM_ASP",
                    "type": "P",
                    "score": 0.85,
                    "direction": "증가",
                    "evidence": ["문장1", "문장2"]
                }
            ],
            "quantity_signals": [...],
            "cost_signals": [...]
        }
    """
    # ⭐ 로그 강화: Step 4A 시작
    logger.info(f"[Step 4A] 드라이버 시그널 추출 시작: {major_sector}/{sub_sector}" + (f" (L2: {sector_l2})" if sector_l2 else ""))
    
    # 1. 텍스트 수집 및 문장 단위 분리
    all_sentences = []
    sentence_sources = []  # 문장 출처 추적
    
    # biz_summary 문장 분리
    if company_detail.biz_summary:
        sentences = split_into_sentences(company_detail.biz_summary)
        all_sentences.extend(sentences)
        sentence_sources.extend([("biz_summary", s) for s in sentences])
    
    # products 설명 문장 분리
    if company_detail.products:
        for product in company_detail.products[:10]:
            product_text = str(product)
            sentences = split_into_sentences(product_text)
            all_sentences.extend(sentences)
            sentence_sources.extend([("products", s) for s in sentences])
    
    # keywords 설명 문장 분리
    if company_detail.keywords:
        for keyword in company_detail.keywords[:10]:
            keyword_text = str(keyword)
            sentences = split_into_sentences(keyword_text)
            all_sentences.extend(sentences)
            sentence_sources.extend([("keywords", s) for s in sentences])
    
    # raw_materials 설명 문장 분리
    if company_detail.raw_materials:
        for rm in company_detail.raw_materials[:10]:
            rm_text = str(rm)
            sentences = split_into_sentences(rm_text)
            all_sentences.extend(sentences)
            sentence_sources.extend([("raw_materials", s) for s in sentences])
    
    if not all_sentences:
        logger.warning("문장 추출 실패: 텍스트 없음")
        return {
            "price_signals": [],
            "quantity_signals": [],
            "cost_signals": []
        }
    
    logger.info(f"✅ [Step 4A] 문장 단위 분리 완료: {len(all_sentences)}개 문장")
    
    # 2. 문장 필터링 (공시스러운 문장만)
    filtered_sentences, filtered_sources = filter_candidate_sentences(
        all_sentences, sentence_sources
    )
    logger.info(f"[Step 4A] 필터링 후: {len(filtered_sentences)}개 (전체 {len(all_sentences)}개 중)")
    
    if not filtered_sentences:
        logger.warning("[Step 4A] 필터링 후 문장 없음")
        return {
            "price_signals": [],
            "quantity_signals": [],
            "cost_signals": []
        }
    
    logger.info(f"✅ [Step 4A] 문장 필터링 완료: {len(all_sentences)}개 → {len(filtered_sentences)}개")
    
    # 3. 섹터별 드라이버 후보 수집 (컨텍스트 한정)
    standard_drivers = get_candidate_drivers(major_sector, sub_sector)
    
    if not standard_drivers:
        logger.warning(f"드라이버 후보 없음: {major_sector}/{sub_sector}")
        return {
            "price_signals": [],
            "quantity_signals": [],
            "cost_signals": []
        }
    
    logger.info(f"✅ [Step 4A] 드라이버 후보 수집 완료: {len(standard_drivers)}개")
    
    # 4. 각 문장을 KF-DeBERTa로 임베딩 (필터링된 문장만)
    try:
        import time
        embedding_start = time.time()
        logger.info(f"🔄 [Step 4A] KF-DeBERTa 모델 로딩 중...")
        model = get_direct_embedding_model()
        if model is None:
            logger.warning("드라이버 임베딩 없이 유사도 계산을 건너뜁니다.")
            return {
                "price_signals": [],
                "quantity_signals": [],
                "cost_signals": []
            }
        model_load_time = time.time() - embedding_start
        if model_load_time > 1.0:  # 1초 이상 걸렸으면 로딩 시간 로그
            logger.info(f"✅ [Step 4A] KF-DeBERTa 모델 로딩 완료 ({model_load_time:.2f}초)")
        
        encode_start = time.time()
        logger.info(f"🔄 [Step 4A] 문장 임베딩 생성 중... ({len(filtered_sentences)}개 문장)")
        sentence_embeddings = model.encode(
            filtered_sentences,
            batch_size=16,
            convert_to_numpy=True
        )
        encode_time = time.time() - encode_start
        logger.info(f"✅ [Step 4A] 문장 임베딩 완료: {len(sentence_embeddings)}개 ({encode_time:.2f}초)")
    except Exception as e:
        logger.error(f"문장 임베딩 실패: {e}")
        return {
            "price_signals": [],
            "quantity_signals": [],
            "cost_signals": []
        }
    
    # 4. ECONVAR_MASTER의 드라이버 설명을 임베딩 (캐싱 + 배치 처리)
    driver_embeddings = {}
    driver_info = {}
    driver_texts_to_encode = []  # 캐시에 없는 드라이버만 배치로 인코딩
    driver_codes_to_encode = []
    
    for driver_code in standard_drivers:
        econvar_info = ECONVAR_MASTER.get(driver_code, {})
        if not econvar_info:
            continue
        
        # 캐시 확인
        if driver_code in _driver_embedding_cache:
            driver_emb, cached_info = _driver_embedding_cache[driver_code]
            driver_embeddings[driver_code] = driver_emb
            driver_info[driver_code] = cached_info
        else:
            # 드라이버 설명 텍스트 구성
            description = econvar_info.get('description', '')
            name_ko = econvar_info.get('name_ko', driver_code)
            driver_text = f"{name_ko} {description}"
            driver_texts_to_encode.append(driver_text)
            driver_codes_to_encode.append((driver_code, econvar_info))
    
    # 배치 임베딩 처리 (캐시에 없는 드라이버만)
    if driver_texts_to_encode:
        try:
            import time
            batch_start = time.time()
            batch_embeddings = model.encode(
                driver_texts_to_encode,
                batch_size=len(driver_texts_to_encode),  # 한 번에 처리
                convert_to_numpy=True
            )
            batch_time = time.time() - batch_start
            
            # 결과 저장 및 캐시 업데이트
            for (driver_code, econvar_info), driver_emb in zip(driver_codes_to_encode, batch_embeddings):
                driver_embeddings[driver_code] = driver_emb
                driver_info[driver_code] = econvar_info
                # 캐시에 저장
                _driver_embedding_cache[driver_code] = (driver_emb, econvar_info)
            
            if len(driver_texts_to_encode) > 1:
                logger.info(f"드라이버 임베딩 배치 처리 완료: {len(driver_texts_to_encode)}개 ({batch_time:.3f}초, 캐시: {len(driver_embeddings) - len(driver_texts_to_encode)}개)")
        except Exception as e:
            logger.warning(f"드라이버 임베딩 배치 처리 실패: {e}")
            # 개별 처리로 폴백
            for driver_code, econvar_info in driver_codes_to_encode:
                try:
                    description = econvar_info.get('description', '')
                    name_ko = econvar_info.get('name_ko', driver_code)
                    driver_text = f"{name_ko} {description}"
                    driver_emb = model.encode([driver_text], convert_to_numpy=True)[0]
                    driver_embeddings[driver_code] = driver_emb
                    driver_info[driver_code] = econvar_info
                    _driver_embedding_cache[driver_code] = (driver_emb, econvar_info)
                except Exception as e2:
                    logger.warning(f"드라이버 임베딩 실패 ({driver_code}): {e2}")
                    continue
    
    logger.info(f"드라이버 임베딩 완료: {len(driver_embeddings)}개 (캐시 히트: {len(driver_embeddings) - len(driver_texts_to_encode)}개)")
    
    # 5. 문장-드라이버 유사도 계산 및 분류 (벡터화 연산)
    driver_signals = {
        "price_signals": {},
        "quantity_signals": {},
        "cost_signals": {}
    }
    
    if not driver_embeddings:
        logger.warning("드라이버 임베딩이 없어 유사도 계산을 건너뜁니다.")
    else:
        import time
        similarity_start = time.time()
        
        # 벡터화 연산 준비
        sentence_embeddings_array = np.array(sentence_embeddings)  # (n, dim)
        driver_codes_list = list(driver_embeddings.keys())
        driver_embeddings_array = np.array([driver_embeddings[code] for code in driver_codes_list])  # (m, dim)
        
        # 배치 코사인 유사도 계산 (벡터화)
        # similarities[i, j] = sentence i와 driver j의 유사도
        similarities = np.dot(sentence_embeddings_array, driver_embeddings_array.T)  # (n, m)
        
        # 정규화 (코사인 유사도)
        norms_sentence = np.linalg.norm(sentence_embeddings_array, axis=1, keepdims=True)  # (n, 1)
        norms_driver = np.linalg.norm(driver_embeddings_array, axis=1)  # (m,)
        similarities = similarities / (norms_sentence * norms_driver)  # (n, m)
        
        # 각 문장의 최적 드라이버 찾기
        best_indices = np.argmax(similarities, axis=1)  # (n,) - 각 문장의 최적 드라이버 인덱스
        best_similarities = similarities[np.arange(len(similarities)), best_indices]  # (n,) - 최적 유사도 값
        
        similarity_time = time.time() - similarity_start
        if len(filtered_sentences) > 10:  # 문장이 많을 때만 로그 출력
            logger.info(f"벡터화 유사도 계산 완료: {len(filtered_sentences)}개 문장 × {len(driver_codes_list)}개 드라이버 ({similarity_time:.3f}초)")
        
        # 🔥 신규: 유사도 분포 로깅
        similarity_scores = best_similarities.tolist()
        logger.info(f"📊 [드라이버 시그널] 유사도 분포: "
                    f"min={min(similarity_scores):.3f}, "
                    f"max={max(similarity_scores):.3f}, "
                    f"mean={np.mean(similarity_scores):.3f}, "
                    f"median={np.median(similarity_scores):.3f}, "
                    f"std={np.std(similarity_scores):.3f}, "
                    f"상위20%={np.percentile(similarity_scores, 80):.3f}")
        
        # 🔥 신규: 동적 Threshold 적용 (percentile 80 → 70으로 완화)
        threshold = np.percentile(best_similarities, 70)  # 80 → 70으로 완화
        threshold = max(0.4, min(0.6, threshold))
        logger.info(f"📊 [드라이버 시그널] 동적 Threshold: {threshold:.3f} (percentile 70, 기존 고정값: 0.6)")
        
        threshold_mask = best_similarities > threshold
        
        # ⭐ Minimum Guarantee: 최소 3개 보장
        matched_count = np.sum(threshold_mask)
        if matched_count < 3 and len(best_similarities) >= 3:
            # 최소 3개 보장: 상위 3개 강제 선택
            logger.warning(f"⚠️ [Step 4A] Threshold 매칭 {matched_count}개 < 3개, 상위 3개 강제 선택")
            top3_indices = np.argsort(best_similarities)[-3:]
            threshold_mask = np.zeros_like(best_similarities, dtype=bool)
            threshold_mask[top3_indices] = True
        
        # 7. 드라이버 시그널 수집 (문장 타입과 드라이버 타입 정렬)
        for idx in np.where(threshold_mask)[0]:
            sentence = filtered_sentences[idx]
            source_type, _ = filtered_sources[idx]
            best_match_idx = best_indices[idx]
            best_match = driver_codes_list[best_match_idx]
            best_similarity = float(best_similarities[idx])
            info = driver_info[best_match]
            var_type = info.get('type', '')
            
            # 🔥 신규: 문장 자체의 P/Q/C 타입 판단
            sentence_type = classify_sentence_type(sentence)
            
            # 🔥 신규: 문장 타입과 드라이버 타입이 일치하는지 확인
            # 중요: sentence_type이 None이면 타입 검사를 생략 (정규식 기반 태깅의 recall 한계 보완)
            # sentence_type이 명확할 때만 타입 불일치 체크하여 정확도 향상
            if sentence_type is not None and sentence_type != var_type:
                # 타입 불일치: 유사도가 높아도 스킵 (정확도 향상)
                logger.debug(f"타입 불일치 스킵: 문장='{sentence[:50]}...' (문장타입={sentence_type}, 드라이버타입={var_type})")
                continue
            
            signal_key = f"{best_match}_{var_type}"
            
            if var_type == 'P':
                if signal_key not in driver_signals["price_signals"]:
                    driver_signals["price_signals"][signal_key] = {
                        "var": info.get('name_ko', best_match),
                        "code": best_match,
                        "type": "P",
                        "score": best_similarity,
                        "sentences": []
                    }
                driver_signals["price_signals"][signal_key]["sentences"].append({
                    "text": sentence,
                    "source": source_type,
                    "similarity": best_similarity
                })
                # 최고 점수 업데이트
                if best_similarity > driver_signals["price_signals"][signal_key]["score"]:
                    driver_signals["price_signals"][signal_key]["score"] = best_similarity
            
            elif var_type == 'Q':
                if signal_key not in driver_signals["quantity_signals"]:
                    driver_signals["quantity_signals"][signal_key] = {
                        "var": info.get('name_ko', best_match),
                        "code": best_match,
                        "type": "Q",
                        "score": best_similarity,
                        "sentences": []
                    }
                driver_signals["quantity_signals"][signal_key]["sentences"].append({
                    "text": sentence,
                    "source": source_type,
                    "similarity": best_similarity
                })
                if best_similarity > driver_signals["quantity_signals"][signal_key]["score"]:
                    driver_signals["quantity_signals"][signal_key]["score"] = best_similarity
            
            elif var_type == 'C':
                if signal_key not in driver_signals["cost_signals"]:
                    driver_signals["cost_signals"][signal_key] = {
                        "var": info.get('name_ko', best_match),
                        "code": best_match,
                        "type": "C",
                        "score": best_similarity,
                        "sentences": []
                    }
                driver_signals["cost_signals"][signal_key]["sentences"].append({
                    "text": sentence,
                    "source": source_type,
                    "similarity": best_similarity
                })
                if best_similarity > driver_signals["cost_signals"][signal_key]["score"]:
                    driver_signals["cost_signals"][signal_key]["score"] = best_similarity
    
    # 8. 타입별/전체 Top-K 제한 적용
    result = cap_signals_per_type(driver_signals)
    
    total_evidence = sum(len(s.get('evidence', [])) for s in result['price_signals'] + result['quantity_signals'] + result['cost_signals'])
    
    # 최종 결과 로깅
    if not result['price_signals'] and not result['quantity_signals'] and not result['cost_signals']:
        logger.warning(f"[Step 4A] 유사도 Threshold 미달 또는 매칭 실패 (드라이버 0개)")
    else:
        logger.info(f"✅ [Step 4A] 드라이버 시그널 추출 완료: "
                    f"P={len(result['price_signals'])}, "
                    f"Q={len(result['quantity_signals'])}, "
                    f"C={len(result['cost_signals'])}, "
                    f"전체 evidence={total_evidence}개")
    
    return result


