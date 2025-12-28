"""
밸류체인 분류 - Golden Set Centroid 기반 (Phase 5)

⭐ 핵심 변경: 텍스트 앵커 → 대표 기업 벡터 평균(Centroid) 앵커
- 텍스트 도메인 미스매치 문제 해결
- Top1 Score 향상 (0.44 → 0.6+ 목표)
"""
import logging
import numpy as np
from typing import List, Dict, Tuple, Optional
from functools import lru_cache
import os

logger = logging.getLogger(__name__)

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# 모델 캐시
_vc_embedding_model = None
DEFAULT_MODEL_NAME = "upskyy/kf-deberta-multitask"
_value_chain_reference_embeddings = {}
_centroid_embeddings = {}  # Centroid 캐시

# 호환성을 위한 변수
SENTENCE_TRANSFORMERS_AVAILABLE = True


# ═══════════════════════════════════════════════════════════════════════════════
# ⭐ GOLDEN SET: 각 밸류체인별 대표 기업 (Pure Player 원칙)
# ═══════════════════════════════════════════════════════════════════════════════
GOLDEN_SET = {
    'UPSTREAM': [
        # 원자재/소재: 광물 채굴, 1차 금속 가공, 순수 화학 소재
        '010130',  # 고려아연: 비철금속 제련
        '005490',  # POSCO홀딩스: 철강/리튬 소재
        '011780',  # 금호석유: 합성고무/수지
        '003670',  # 포스코퓨처엠: 2차전지 소재
        '009830',  # 한화솔루션: 태양광 소재/케미칼
        '096770',  # SK이노베이션: 정유/석유개발
        '010950',  # S-Oil: 순수 정유
        '006650',  # 대한유화: 순수 석유화학
    ],
    'MID_HARD': [
        # 제조/부품: 공장(Fab) 기반 대량 생산, B2B 부품
        '000660',  # SK하이닉스: 메모리 반도체 제조
        '373220',  # LG에너지솔루션: 배터리 셀 제조
        '006400',  # 삼성SDI: 배터리/디스플레이 제조
        '009150',  # 삼성전기: MLCC/모듈 부품
        '011070',  # LG이노텍: 카메라 모듈/기판
        '000270',  # 기아: 완성차 제조
        '034220',  # LG디스플레이: 디스플레이 패널
        '012330',  # 현대모비스: 자동차 부품
    ],
    'MID_SOFT': [
        # ⭐ Phase 7: 이질적 구성 (반도체 설계 25% + 순수 SW 75%)
        # 순수 팹리스/장비설계 (2개 - 축소)
        '058470',  # 리노공업: 반도체 검사장비 설계 (순수 기술)
        '217190',  # 제주반도체: 팹리스 (순수 설계)
        
        # 순수 SW/보안 (6개 - 확대: B2B SW 중심)
        '053800',  # 안랩: 보안 소프트웨어 (순수 SW)
        '012510',  # 더존비즈온: ERP 소프트웨어 (순수 SW)
        '030520',  # 한글과컴퓨터: 오피스 소프트웨어 (순수 SW)
        '208350',  # 지란지교시큐리티: 이메일 보안 SW
        '047560',  # 이스트소프트: 백신/유틸리티 SW
        '032850',  # 비트컴퓨터: ERP/SI 솔루션
    ],
    'DOWN_BIZ': [
        # 유통/상사/운송: 물건 이동, 판매 중개
        '028260',  # 삼성물산: 상사/건설
        '004170',  # 신세계: 백화점/유통
        '023530',  # 롯데쇼핑: 마트/백화점
        '086280',  # 현대글로비스: 물류/유통
        '139480',  # 이마트: 할인점 유통
        '005380',  # 현대차: 완성차 판매
        '066570',  # LG전자: 가전 판매
        '028670',  # 팬오션: 해운 물류
    ],
    'DOWN_SERVICE': [
        # 플랫폼/콘텐츠/B2C: 무형 서비스, 트래픽 기반
        '035420',  # NAVER: 검색/커머스 플랫폼
        '035720',  # 카카오: 메신저/플랫폼
        '251270',  # 넷마블: 게임 퍼블리싱
        '036570',  # 엔씨소프트: 게임 개발/서비스
        '352820',  # 하이브: 엔터테인먼트
        '035900',  # JYP Ent.: 엔터테인먼트
        '377300',  # 카카오페이: 핀테크
        '323410',  # 카카오뱅크: 인터넷 은행
    ]
}


# ═══════════════════════════════════════════════════════════════════════════════
# ⭐ 섹터 기반 Prior (가산점)
# ═══════════════════════════════════════════════════════════════════════════════
SECTOR_PRIOR = {
    # 반도체 섹터 → MID_HARD/MID_SOFT 가산
    'SEC_SEMI': {'MID_HARD': 0.03, 'MID_SOFT': 0.03},
    'SEC_SEMI_MANUFACTURING': {'MID_HARD': 0.05},
    'SEC_SEMI_FABLESS': {'MID_SOFT': 0.05},
    'SEC_SEMI_EQUIPMENT': {'MID_SOFT': 0.03, 'MID_HARD': 0.02},
    
    # 게임/엔터 섹터 → DOWN_SERVICE 가산
    'SEC_GAME': {'DOWN_SERVICE': 0.05},
    'SEC_ENT': {'DOWN_SERVICE': 0.05},
    'SEC_MEDIA': {'DOWN_SERVICE': 0.03},
    
    # 금융 섹터 → DOWN_SERVICE 가산 (플랫폼/핀테크 성격)
    'SEC_BANK': {'DOWN_SERVICE': 0.03},
    'SEC_FINANCE': {'DOWN_SERVICE': 0.03},
    'SEC_SECURITIES': {'DOWN_SERVICE': 0.02},
    
    # 유통/물류 → DOWN_BIZ 가산
    'SEC_RETAIL': {'DOWN_BIZ': 0.05},
    'SEC_LOGISTICS': {'DOWN_BIZ': 0.05},
    'SEC_DISTRIBUTION': {'DOWN_BIZ': 0.05},
    
    # 화학/소재 → UPSTREAM 가산
    'SEC_CHEMICAL': {'UPSTREAM': 0.05},
    'SEC_STEEL': {'UPSTREAM': 0.05},
    'SEC_MATERIAL': {'UPSTREAM': 0.05},
    
    # 자동차 → MID_HARD (제조) 또는 DOWN_BIZ (판매)
    'SEC_AUTO': {'MID_HARD': 0.03, 'DOWN_BIZ': 0.02},
    'SEC_AUTO_PARTS': {'MID_HARD': 0.05},
    
    # 바이오/제약 → MID_SOFT (R&D) 가산
    'SEC_BIO': {'MID_SOFT': 0.03},
    'SEC_PHARMA': {'MID_SOFT': 0.02, 'DOWN_BIZ': 0.02},
}


# ═══════════════════════════════════════════════════════════════════════════════
# 기존 텍스트 앵커 (Fallback용 - Centroid 실패 시)
# ═══════════════════════════════════════════════════════════════════════════════
VALUE_CHAIN_ANCHORS = {
    'UPSTREAM': {
        'name_ko': '업스트림 (상류 소재)',
        'description': '''Mining, refinery, raw material. Reserve, commodity price, CAPEX.
고려아연, 포스코, S-Oil, 금호석유, 한화솔루션. 광산, 정유, 석유화학, 철강, 리튬.''',
    },
    'MID_HARD': {
        'name_ko': '미드스트림 하드웨어 (중류 제조)',
        'description': '''Foundry, fab, OSAT, mass production. Yield, utilization, ASP.
SK하이닉스, LG에너지솔루션, 삼성SDI, 삼성전기. 반도체, 배터리, 디스플레이 제조.''',
    },
    'MID_SOFT': {
        'name_ko': '미드스트림 소프트웨어 (중류 설계/IP)',
        'description': '''Fabless, IT service, R&D, IP licensing. Patent, intangible assets.
삼성SDS, KT, 셀트리온, 리노공업. SI, 통신, 바이오, 반도체 장비 설계.''',
    },
    'DOWN_BIZ': {
        'name_ko': '다운스트림 비즈니스 (하류 유통/완제품)',
        'description': '''Retail, distribution, trading, logistics. GMV, inventory turnover.
삼성물산, 신세계, 롯데쇼핑, 현대글로비스. 백화점, 마트, 물류, 완성차 판매.''',
    },
    'DOWN_SERVICE': {
        'name_ko': '다운스트림 서비스 (하류 플랫폼/콘텐츠)',
        'description': '''Platform, game, fintech, streaming. MAU, DAU, ARPU, subscription.
네이버, 카카오, 넷마블, 엔씨소프트, 하이브. 검색, 게임, 핀테크, 엔터테인먼트.''',
    }
}

VALUE_CHAIN_REFERENCES = VALUE_CHAIN_ANCHORS


def _get_preferred_device() -> str:
    """GPU 사용 가능 시 CUDA, 아니면 CPU"""
    if TORCH_AVAILABLE:
        try:
            if torch.cuda.is_available():
                return "cuda"
        except Exception:
            pass
    return "cpu"


def get_embedding_model(model_name: str = None):
    """Solar Embedding 모델 래퍼 (호환성 유지)"""
    global _vc_embedding_model
    
    if _vc_embedding_model is None:
        _vc_embedding_model = "solar_embedding"
        logger.info("Solar Embedding (API-based)")
    
    return _vc_embedding_model


def clear_anchor_cache():
    """Anchor/Centroid 캐시 초기화"""
    global _value_chain_reference_embeddings, _centroid_embeddings
    _value_chain_reference_embeddings = {}
    _centroid_embeddings = {}
    logger.info("[CACHE CLEARED] All anchor/centroid caches cleared")


# ═══════════════════════════════════════════════════════════════════════════════
# ⭐ Centroid 앵커 계산 (핵심 함수)
# ═══════════════════════════════════════════════════════════════════════════════
def get_centroid_anchors(db_session, force_regenerate: bool = False) -> Dict[str, np.ndarray]:
    """
    Golden Set 기업들의 임베딩 평균(Centroid)을 앵커로 계산
    
    Args:
        db_session: SQLAlchemy DB 세션
        force_regenerate: True면 캐시 무시
    
    Returns:
        {밸류체인코드: centroid_vector} 딕셔너리
    """
    global _centroid_embeddings
    
    cache_key = 'CENTROID'
    
    if not force_regenerate and cache_key in _centroid_embeddings:
        logger.debug(f"[CACHE HIT] Centroid reuse")
        return _centroid_embeddings[cache_key]
    
    logger.info("[CENTROID] Calculating centroid anchors from Golden Set...")
    
    from sqlalchemy import text
    import ast
    
    centroids = {}
    stats = {}
    
    for vc_code, tickers in GOLDEN_SET.items():
        embeddings = []
        found_tickers = []
        
        for ticker in tickers:
            try:
                result = db_session.execute(text("""
                    SELECT embedding_vector
                    FROM company_embeddings
                    WHERE ticker = :ticker
                """), {'ticker': ticker})
                
                row = result.fetchone()
                if row and row[0]:
                    embedding_vector = row[0]
                    
                    # pgvector 타입 변환
                    if isinstance(embedding_vector, str):
                        vec_list = ast.literal_eval(embedding_vector)
                        embedding = np.array(vec_list, dtype=np.float32)
                    elif hasattr(embedding_vector, 'tolist'):
                        embedding = np.array(embedding_vector.tolist(), dtype=np.float32)
                    elif isinstance(embedding_vector, (list, tuple)):
                        embedding = np.array(embedding_vector, dtype=np.float32)
                    else:
                        continue
                    
                    embeddings.append(embedding)
                    found_tickers.append(ticker)
            except Exception as e:
                logger.debug(f"[{ticker}] Embedding load failed: {e}")
                continue
        
        if embeddings:
            # Centroid = 평균 벡터
            centroid = np.mean(embeddings, axis=0)
            # L2 정규화
            centroid = centroid / np.linalg.norm(centroid)
            centroids[vc_code] = centroid
            stats[vc_code] = len(embeddings)
            logger.debug(f"[{vc_code}] Centroid from {len(embeddings)}/{len(tickers)} companies")
        else:
            logger.warning(f"[{vc_code}] No embeddings found for Golden Set!")
    
    _centroid_embeddings[cache_key] = centroids
    logger.info(f"[OK] Centroid anchors: {stats}")
    
    return centroids


def get_value_chain_reference_embeddings(
    sector_code: str = None, 
    model=None, 
    force_regenerate: bool = False,
    db_session=None,
    use_centroid: bool = True
) -> Dict[str, np.ndarray]:
    """
    밸류체인 앵커 임베딩 반환
    
    ⭐ Phase 5: Centroid 우선 사용, 실패 시 텍스트 앵커 Fallback
    
    Args:
        sector_code: 섹터 코드 (미사용, 호환성)
        model: 임베딩 모델 (미사용, 호환성)
        force_regenerate: 캐시 무시
        db_session: DB 세션 (Centroid 계산용)
        use_centroid: True면 Centroid 사용, False면 텍스트 앵커
    
    Returns:
        {밸류체인코드: embedding_vector} 딕셔너리
    """
    global _value_chain_reference_embeddings
    
    # Centroid 모드
    if use_centroid and db_session is not None:
        centroids = get_centroid_anchors(db_session, force_regenerate)
        if centroids and len(centroids) >= 5:
            return centroids
        logger.warning("[FALLBACK] Centroid failed, using text anchors")
    
    # Fallback: 텍스트 앵커
    cache_key = sector_code or 'DEFAULT'
    
    if force_regenerate and cache_key in _value_chain_reference_embeddings:
        del _value_chain_reference_embeddings[cache_key]
    
    if cache_key in _value_chain_reference_embeddings:
        return _value_chain_reference_embeddings[cache_key]
    
    logger.info(f"[TEXT ANCHOR] Generating text-based anchors...")
    from app.services.solar_embedding_model import encode_solar_embedding
    
    embeddings = {}
    for vc_code, ref in VALUE_CHAIN_REFERENCES.items():
        reference_text = _build_value_chain_reference_text(ref)
        embedding = encode_solar_embedding(reference_text)
        embeddings[vc_code] = embedding
    
    _value_chain_reference_embeddings[cache_key] = embeddings
    logger.info(f"[OK] Text anchors cached: {len(embeddings)}")
    
    return embeddings


def _build_value_chain_reference_text(ref: dict) -> str:
    """밸류체인 참조 정보를 Anchor 문장으로 변환"""
    if 'description' in ref and ref['description']:
        anchor = ref['description']
        if 'name_ko' in ref and ref['name_ko']:
            anchor = f"{ref['name_ko']}: {anchor}"
        return anchor
    
    if 'name_ko' in ref:
        return ref['name_ko']
    
    return ''


def compute_cosine_similarity(emb1: np.ndarray, emb2: np.ndarray) -> float:
    """코사인 유사도 계산"""
    dot_product = np.dot(emb1, emb2)
    norm1 = np.linalg.norm(emb1)
    norm2 = np.linalg.norm(emb2)
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return float(dot_product / (norm1 * norm2))


# ═══════════════════════════════════════════════════════════════════════════════
# ⭐ Softmax Entropy Confidence 계산
# ═══════════════════════════════════════════════════════════════════════════════
def compute_softmax_confidence(scores: List[float], temperature: float = 0.1) -> float:
    """
    Softmax Entropy 기반 Confidence 계산
    
    낮은 엔트로피 = 높은 확신 (한 쪽으로 쏠림)
    높은 엔트로피 = 낮은 확신 (균등 분포)
    
    Args:
        scores: 각 밸류체인의 유사도 점수 리스트
        temperature: Softmax 온도 (낮을수록 날카로움)
    
    Returns:
        0~1 사이의 confidence (1이 가장 확신)
    """
    if not scores or len(scores) < 2:
        return 1.0
    
    scores = np.array(scores)
    
    # Softmax with temperature
    exp_scores = np.exp((scores - np.max(scores)) / temperature)
    probs = exp_scores / np.sum(exp_scores)
    
    # Shannon Entropy
    epsilon = 1e-10
    entropy = -np.sum(probs * np.log(probs + epsilon))
    
    # 최대 엔트로피 (균등 분포)
    max_entropy = np.log(len(scores))
    
    # Confidence = 1 - (entropy / max_entropy)
    confidence = 1.0 - (entropy / max_entropy)
    
    return float(max(0.0, min(1.0, confidence)))


def apply_sector_prior(
    similarities: List[Dict[str, any]],
    sector_l1: Optional[str] = None,
    sector_l2: Optional[str] = None
) -> List[Dict[str, any]]:
    """
    섹터 기반 Prior 적용 (가산점)
    
    Args:
        similarities: [{'value_chain': str, 'score': float}, ...] 리스트
        sector_l1: 섹터 L1 코드
        sector_l2: 섹터 L2 코드
    
    Returns:
        Prior가 적용된 similarities 리스트
    """
    # 섹터 코드 확인 (L2 우선, L1 fallback)
    sector_code = sector_l2 or sector_l1
    
    if not sector_code or sector_code not in SECTOR_PRIOR:
        return similarities
    
    priors = SECTOR_PRIOR[sector_code]
    
    for sim in similarities:
        vc = sim['value_chain']
        if vc in priors:
            original = sim['score']
            sim['score'] = original + priors[vc]
            sim['prior_applied'] = priors[vc]
            logger.debug(f"[Prior] {sector_code} → {vc}: +{priors[vc]:.2f}")
    
    return similarities


def classify_value_chain_embedding(
    company_text: str,
    sector_code: str = None,
    top_k: int = 3,
    min_threshold: float = 0.25,
    model_name: Optional[str] = None
) -> List[Dict[str, float]]:
    """
    임베딩 모델 기반 밸류체인 후보 생성 (호환성 유지)
    """
    if isinstance(company_text, list):
        company_text = ' '.join(str(item) for item in company_text if item)
    
    if not company_text or len(str(company_text).strip()) < 10:
        return []
    
    try:
        model = get_embedding_model(model_name)
        vc_embeddings = get_value_chain_reference_embeddings(sector_code, model)
        
        from app.services.solar_embedding_model import encode_solar_embedding
        company_embedding = encode_solar_embedding(company_text)
        
        priority_vc_codes = ['UPSTREAM', 'MID_HARD', 'MID_SOFT', 'DOWN_BIZ', 'DOWN_SERVICE']
        
        similarities = []
        for vc_code in priority_vc_codes:
            if vc_code in vc_embeddings:
                similarity = compute_cosine_similarity(company_embedding, vc_embeddings[vc_code])
                if similarity >= min_threshold:
                    similarities.append({
                        'value_chain': vc_code,
                        'score': similarity
                    })
        
        similarities.sort(key=lambda x: x['score'], reverse=True)
        results = similarities[:top_k]
        
        if len(results) >= 2:
            scores = [r['score'] for r in results]
            confidence = compute_softmax_confidence(scores)
            for result in results:
                result['confidence'] = confidence
                result['is_hybrid'] = confidence < 0.5
        elif len(results) == 1:
            results[0]['confidence'] = 1.0
            results[0]['is_hybrid'] = False
        
        return results
        
    except Exception as e:
        logger.error(f"Value chain classification failed: {e}")
        return []


def get_embedding_for_text(text: str, model=None) -> Optional[np.ndarray]:
    """텍스트의 임베딩 벡터 반환"""
    if not text or len(text.strip()) < 2:
        return None
    
    try:
        from app.services.solar_embedding_model import encode_solar_embedding
        embedding = encode_solar_embedding(text)
        return embedding
    except Exception as e:
        logger.error(f"Failed to generate embedding: {e}")
        return None


def batch_generate_embeddings(
    texts: List[str],
    batch_size: int = 32,
    model=None
) -> List[np.ndarray]:
    """배치 임베딩 생성"""
    if not texts:
        return []
    
    try:
        from app.services.solar_embedding_model import encode_solar_embedding
        embeddings = encode_solar_embedding(texts, batch_size=batch_size)
        if isinstance(embeddings, np.ndarray) and embeddings.ndim == 1:
            return [embeddings]
        return list(embeddings)
    except Exception as e:
        logger.error(f"Failed to generate batch embeddings: {e}")
        return []


def get_value_chain_embedding_segments(sector_code: str) -> Dict[str, dict]:
    """특정 섹터의 밸류체인 세그먼트 정보 반환"""
    return VALUE_CHAIN_REFERENCES
