"""
의미 기반 핵심 문장 추출

섹터 분류 정확도를 최대화하기 위한 문장 선택 알고리즘
- Keyword-based 필터링
- Embedding 기반 중요도 계산 (KF-DeBERTa)
- Score 조합 → 상위 N개 문장 선택
"""
import logging
from typing import List, Tuple, Dict, Optional
import numpy as np

logger = logging.getLogger(__name__)

# kss 라이브러리
try:
    import kss
    HAS_KSS = True
except ImportError:
    HAS_KSS = False
    logger.warning("kss 라이브러리가 설치되지 않았습니다.")

# 임베딩 모델 (KF-DeBERTa 재사용)
try:
    from app.services.sector_classifier_embedding import (
        get_embedding_model,
        get_sector_reference_embeddings,
        compute_cosine_similarity
    )
    EMBEDDING_AVAILABLE = True
except ImportError:
    EMBEDDING_AVAILABLE = False
    logger.warning("임베딩 모델 not available.")

# 섹터 Reference
from app.models.sector_reference import get_all_sector_references

# 동적 가중치 계산
try:
    from app.utils.dynamic_weight_calculator import determine_dynamic_weights_softmax
    DYNAMIC_WEIGHT_AVAILABLE = True
except ImportError:
    DYNAMIC_WEIGHT_AVAILABLE = False
    logger.warning("Dynamic weight calculator not available. Will use fixed weights.")

# 구조 Confidence 계산
try:
    from app.utils.structure_confidence import calculate_structure_confidence
    STRUCTURE_CONFIDENCE_AVAILABLE = True
except ImportError:
    STRUCTURE_CONFIDENCE_AVAILABLE = False
    logger.warning("Structure confidence calculator not available.")


# ============================================================================
# ① Keyword-based 필터링
# ============================================================================

# 섹터 분류와 직접 연결되는 일반 키워드
SECTOR_RELEVANT_KEYWORDS = [
    "주력", "주요 제품", "영위", "매출", "비중", "사업 부문", 
    "제조", "생산", "공급", "핵심", "시장", "경쟁력", "수요",
    "공정", "장비", "소재", "제품", "서비스", "매출원", 
    "주요 사업", "사업 영역", "매출 구성", "수주", "주요 고객",
    "사업 모델", "핵심 기술", "주요 시장", "매출 비중"
]

# 금융 용어
FINANCIAL_KEYWORDS = [
    "대출", "예금", "저축", "이자", "수수료",
    "투자", "자산운용", "포트폴리오", "펀드",
    "보험료", "보상", "재보험", "연금",
    "카드", "결제", "신용", "캐피탈", "리스",
    "금융상품", "예금상품", "대출상품"
]

# 산업 용어
INDUSTRIAL_KEYWORDS = [
    "수주", "발주", "납품", "공급망", "물류",
    "공정", "생산라인", "설비", "자동화",
    "R&D", "연구개발", "기술개발", "특허",
    "수출", "수입", "해외진출", "글로벌",
    "착공", "준공", "인허가", "착수"
]

# 확장된 일반 키워드
EXPANDED_GENERAL_KEYWORDS = SECTOR_RELEVANT_KEYWORDS + FINANCIAL_KEYWORDS + INDUSTRIAL_KEYWORDS

# 섹터별 특화 키워드 (28개 섹터, 정확도 위주로 핵심 키워드만)
# sector_classifier.py의 SECTOR_KEYWORDS에서 추출
SECTOR_SPECIFIC_KEYWORDS = {
    # [Tech & Growth] - 5개
    'SEC_SEMI': [
        "DRAM", "NAND", "HBM", "웨이퍼", "파운드리", "공정", "패키징", "메모리 반도체", "시스템 반도체"
    ],
    'SEC_BATTERY': [
        "2차전지", "양극재", "음극재", "전해액", "분리막", "셀", "리튬이온", "LFP", "NCM"
    ],
    'SEC_IT': [
        "소프트웨어", "SaaS", "클라우드", "플랫폼", "보안", "SI", "솔루션", "API"
    ],
    'SEC_GAME': [
        "모바일게임", "PC게임", "콘솔게임", "게임개발", "게임퍼블리싱", "e스포츠"
    ],
    'SEC_ELECTRONICS': [
        "디스플레이", "OLED", "LCD", "패널", "가전", "TV", "냉장고"
    ],
    
    # [Mobility] - 2개
    'SEC_AUTO': [
        "전기차", "EV", "완성차", "자동차부품", "전장", "모터", "인버터"
    ],
    'SEC_TIRE': [
        "타이어", "고무", "합성고무"
    ],
    
    # [Industry & Cyclical] - 6개
    'SEC_SHIP': [
        "조선", "선박", "조선소", "컨테이너선", "LNG선", "유조선", "해운"
    ],
    'SEC_DEFENSE': [
        "방산", "무기", "국방", "항공기", "미사일", "KFX", "KF-21"
    ],
    'SEC_MACH': [
        "공작기계", "산업기계", "전력기기", "건설기계", "중장비", "발전기"
    ],
    'SEC_CONST': [
        "건설", "건축", "토목", "플랜트", "EPC", "인프라", "공사"
    ],
    'SEC_STEEL': [
        "철강", "강판", "강재", "제철", "압연"
    ],
    'SEC_CHEM': [
        "석유화학", "정유", "나프타", "폴리머", "수지", "LNG"
    ],
    
    # [Consumer & K-Culture] - 6개
    'SEC_ENT': [
        "K-POP", "K팝", "아이돌", "앨범", "영화", "드라마", "OTT", "콘텐츠"
    ],
    'SEC_COSMETIC': [
        "화장품", "뷰티", "스킨케어", "메이크업", "OEM", "ODM"
    ],
    'SEC_TRAVEL': [
        "항공", "항공사", "호텔", "리조트", "면세", "카지노", "관광"
    ],
    'SEC_FOOD': [
        "식품", "음료", "가공식품", "냉동식품", "유제품", "담배"
    ],
    'SEC_RETAIL': [
        "유통", "소매", "마트", "편의점", "이커머스", "온라인쇼핑"
    ],
    'SEC_CONSUMER': [
        "가구", "인테리어", "렌탈", "가전렌탈"
    ],
    
    # [Healthcare] - 2개
    'SEC_BIO': [
        "제약", "신약", "바이오의약", "백신", "CMO", "CDMO", "임상"
    ],
    'SEC_MEDDEV': [
        "의료기기", "임플란트", "진단기기", "수술기기", "의료장비"
    ],
    
    # [Finance] - 5개
    'SEC_BANK': [
        "은행", "대출", "예금", "저축", "금융상품"
    ],
    'SEC_SEC': [
        "증권", "투자", "자산운용", "증권사", "브로커"
    ],
    'SEC_INS': [
        "보험", "생명보험", "손해보험", "재보험", "보험료"
    ],
    'SEC_CARD': [
        "카드", "신용카드", "결제", "캐피탈", "리스"
    ],
    'SEC_HOLDING': [
        "지주", "금융지주", "홀딩스"
    ],
    
    # [Utility] - 2개
    'SEC_UTIL': [
        "전기", "전력", "발전", "송전", "배전", "원자력", "신재생에너지", "태양광", "풍력"
    ],
    'SEC_TELECOM': [
        "통신", "이동통신", "5G", "6G", "통신망", "통신사"
    ],
}


def score_by_keywords(
    sentence: str,
    sector_hints: Optional[List[str]] = None
) -> float:
    """
    문장의 키워드 기반 점수 계산 (개선 버전)
    
    섹터 분류와 직접 관련된 키워드가 포함된 문장에 높은 점수 부여
    - 일반 키워드 점수
    - 섹터별 특화 키워드 점수 (가중치 높음)
    
    Args:
        sentence: 문장 텍스트
        sector_hints: 예상 섹터 리스트 (Rule-based 결과 등, 향후 활용)
    
    Returns:
        0.0 ~ 1.0 사이의 점수
    """
    if not sentence:
        return 0.0
    
    sentence_lower = sentence.lower()
    
    # 1. 일반 키워드 점수 (확장된 키워드 포함)
    general_matched = sum(
        1 for keyword in EXPANDED_GENERAL_KEYWORDS 
        if keyword in sentence_lower
    )
    general_score = min(general_matched / len(EXPANDED_GENERAL_KEYWORDS) * 2.0, 1.0)
    
    # 2. 섹터별 특화 키워드 점수 (가중치 높음)
    sector_score = 0.0
    if sector_hints:
        for sector in sector_hints:
            if sector in SECTOR_SPECIFIC_KEYWORDS:
                sector_keywords = SECTOR_SPECIFIC_KEYWORDS[sector]
                matched = sum(1 for kw in sector_keywords if kw in sentence_lower)
                if matched > 0:
                    # 섹터 특화 키워드는 3배 가중치
                    sector_score = max(
                        sector_score, 
                        matched / len(sector_keywords) * 3.0
                    )
    else:
        # sector_hints가 없으면 모든 섹터 키워드 확인
        for sector, sector_keywords in SECTOR_SPECIFIC_KEYWORDS.items():
            matched = sum(1 for kw in sector_keywords if kw in sentence_lower)
            if matched > 0:
                sector_score = max(
                    sector_score,
                    matched / len(sector_keywords) * 3.0
                )
    
    # 3. 최종 점수 (섹터 특화 키워드에 높은 가중치)
    # 일반 키워드 40% + 섹터 특화 키워드 60%
    total_score = min(0.4 * general_score + 0.6 * sector_score, 1.0)
    
    return total_score


# ============================================================================
# ② Embedding 기반 중요도 계산
# ============================================================================

def score_by_embedding(
    sentence: str, 
    sector_anchors: Dict[str, np.ndarray],
    model
) -> float:
    """
    문장과 섹터 anchor 간의 embedding 유사도 계산
    
    모든 섹터 anchor와 비교하여 가장 높은 유사도를 반환
    
    Args:
        sentence: 문장 텍스트
        sector_anchors: {sector_code: embedding_vector} 딕셔너리
        model: SentenceTransformer 모델 (KF-DeBERTa)
    
    Returns:
        0.0 ~ 1.0 사이의 최대 유사도 점수
    """
    if not sentence or not sector_anchors:
        return 0.0
    
    try:
        # 문장 임베딩 생성
        sent_emb = model.encode([sentence], normalize_embeddings=True)[0]
        
        # 모든 섹터 anchor와 유사도 계산
        similarities = []
        for sector_code, anchor_emb in sector_anchors.items():
            sim = compute_cosine_similarity(sent_emb, anchor_emb)
            similarities.append(sim)
        
        # 최대 유사도 반환 (가장 관련성 높은 섹터와의 유사도)
        max_sim = max(similarities) if similarities else 0.0
        
        return float(max_sim)
    
    except Exception as e:
        logger.warning(f"Embedding scoring failed: {e}")
        return 0.0


def score_by_embedding_batch(
    sentences: List[str],
    sector_anchors: Dict[str, np.ndarray],
    model,
    batch_size: int = 32
) -> List[float]:
    """
    문장 임베딩 배치 처리 (성능 최적화)
    
    여러 문장을 한 번에 임베딩하여 처리 속도 향상
    
    Args:
        sentences: 문장 리스트
        sector_anchors: {sector_code: embedding_vector} 딕셔너리
        model: SentenceTransformer 모델 (KF-DeBERTa)
        batch_size: 배치 크기
    
    Returns:
        각 문장의 최대 유사도 점수 리스트
    """
    if not sentences or not sector_anchors:
        return [0.0] * len(sentences)
    
    try:
        # 배치 단위로 임베딩 생성
        all_embeddings = []
        for i in range(0, len(sentences), batch_size):
            batch = sentences[i:i+batch_size]
            batch_embeddings = model.encode(
                batch,
                normalize_embeddings=True,
                batch_size=batch_size,
                show_progress_bar=False
            )
            all_embeddings.extend(batch_embeddings)
        
        # 모든 섹터 anchor 임베딩을 배열로 변환
        anchor_embeddings = np.array(list(sector_anchors.values()))
        
        # 각 문장 임베딩과 모든 anchor의 유사도 계산 (벡터화)
        scores = []
        for sent_emb in all_embeddings:
            # 한 번에 모든 anchor와 유사도 계산
            similarities = np.dot(anchor_embeddings, sent_emb)
            max_sim = float(np.max(similarities))
            scores.append(max_sim)
        
        return scores
    
    except Exception as e:
        logger.warning(f"Batch embedding scoring failed: {e}")
        return [0.0] * len(sentences)


# ============================================================================
# ③ Score 조합 → 핵심 문장 선택
# ============================================================================

def extract_key_sentences_for_sector(
    text: str,
    max_chars: int = 500,
    min_chars: int = 300,
    top_n: int = 5,
    keyword_weight: Optional[float] = None,  # None이면 동적 계산
    embedding_weight: Optional[float] = None,  # None이면 동적 계산
    use_embedding: bool = True,
    # 동적 가중치 파라미터
    rule_confidence: Optional[float] = None,
    embedding_confidence: Optional[float] = None,
    structure_confidence: Optional[float] = None,
    use_dynamic_weights: bool = True,
    tau: float = 0.5
) -> str:
    """
    섹터 분류를 위한 핵심 문장 추출
    
    3단계 프로세스:
    1. Keyword-based 필터링 (빠른 1단계)
    2. Embedding 기반 중요도 계산 (정확한 2단계)
    3. Score 조합 → 상위 N개 문장 선택 (3단계)
    
    Args:
        text: 원본 biz_summary 텍스트
        max_chars: 최대 문자 수 (기본값: 500)
        min_chars: 최소 문자 수 (기본값: 300)
        top_n: 선택할 최대 문장 개수 (기본값: 5)
        keyword_weight: 키워드 점수 가중치 (None이면 동적 계산, 기본값: 0.4)
        embedding_weight: 임베딩 점수 가중치 (None이면 동적 계산, 기본값: 0.6)
        use_embedding: 임베딩 사용 여부 (기본값: True)
        rule_confidence: Rule-based confidence (동적 가중치 사용 시 필요)
        embedding_confidence: Embedding confidence (동적 가중치 사용 시 필요)
        structure_confidence: Text structure confidence (동적 가중치 사용 시 필요)
        use_dynamic_weights: 동적 가중치 사용 여부 (기본값: True)
        tau: Temperature 파라미터 (동적 가중치 사용 시, 기본값: 0.5)
    
    Returns:
        핵심 문장들로 구성된 텍스트 (300~500자)
    """
    if not text:
        return ""
    
    # 이미 짧으면 그대로 반환
    if len(text) <= max_chars:
        return text
    
    # 문장 분리
    sentences = _split_sentences(text)
    
    if not sentences:
        # 문장 분리 실패 시 Fallback
        return text[:max_chars]
    
    # 각 문장에 대한 점수 계산
    scored_sentences = []
    
    # Embedding 준비 (사용하는 경우)
    sector_anchors = None
    model = None
    if use_embedding and EMBEDDING_AVAILABLE:
        try:
            model = get_embedding_model()
            sector_anchors = get_sector_reference_embeddings(model)
            logger.info(f"임베딩 모델 로드 완료. {len(sector_anchors)}개 섹터 anchor 준비됨")
        except Exception as e:
            logger.warning(f"Failed to load embedding model: {e}")
            use_embedding = False
    
    logger.info(f"의미 기반 문장 추출 시작: {len(sentences)}개 문장 분석")
    
    # 동적 가중치 계산 (사용하는 경우)
    final_keyword_weight = keyword_weight
    final_embedding_weight = embedding_weight
    
    if use_dynamic_weights and DYNAMIC_WEIGHT_AVAILABLE:
        # structure_confidence가 없으면 자동 계산
        final_structure_confidence = structure_confidence
        if final_structure_confidence is None and STRUCTURE_CONFIDENCE_AVAILABLE:
            try:
                # biz_summary에서 structure_confidence 계산
                final_structure_confidence = calculate_structure_confidence(
                    biz_summary=text,
                    products=None,  # 향후 확장 가능
                    keywords=None   # 향후 확장 가능
                )
                logger.debug(f"Structure confidence 자동 계산: {final_structure_confidence:.2f}")
            except Exception as e:
                logger.warning(f"Structure confidence 계산 실패: {e}")
                final_structure_confidence = 0.7  # 기본값
        
        if (rule_confidence is not None and 
            embedding_confidence is not None and 
            final_structure_confidence is not None):
            try:
                dynamic_weights = determine_dynamic_weights_softmax(
                    rule_confidence=rule_confidence,
                    embedding_confidence=embedding_confidence,
                    structure_confidence=final_structure_confidence,
                    tau=tau
                )
                final_keyword_weight = dynamic_weights['keyword_weight']
                final_embedding_weight = dynamic_weights['embedding_weight']
                logger.debug(
                    f"동적 가중치 적용: Keyword={final_keyword_weight:.2f}, "
                    f"Embedding={final_embedding_weight:.2f}, "
                    f"GPT={dynamic_weights.get('gpt_weight', 0.0):.2f}"
                )
            except Exception as e:
                logger.warning(f"동적 가중치 계산 실패, 기본값 사용: {e}")
                final_keyword_weight = keyword_weight or 0.4
                final_embedding_weight = embedding_weight or 0.6
        else:
            # Confidence 값이 없으면 기본값 사용
            final_keyword_weight = keyword_weight or 0.4
            final_embedding_weight = embedding_weight or 0.6
    else:
        # 동적 가중치 미사용 또는 사용 불가
        final_keyword_weight = keyword_weight or 0.4
        final_embedding_weight = embedding_weight or 0.6
    
    # 1단계: 키워드 점수 계산 (모든 문장)
    keyword_scores = []
    for sentence in sentences:
        keyword_score = score_by_keywords(sentence, sector_hints=None)
        keyword_scores.append(keyword_score)
    
    # 2단계: Embedding 점수 계산 (배치 처리)
    embedding_scores = []
    if use_embedding and sector_anchors and model:
        try:
            embedding_scores = score_by_embedding_batch(
                sentences,
                sector_anchors,
                model,
                batch_size=32
            )
            logger.info(f"배치 임베딩 처리 완료: {len(embedding_scores)}개 문장")
        except Exception as e:
            logger.warning(f"배치 임베딩 처리 실패, 개별 처리로 Fallback: {e}")
            # Fallback: 개별 처리
            for sentence in sentences:
                embedding_score = score_by_embedding(sentence, sector_anchors, model)
                embedding_scores.append(embedding_score)
    elif not use_embedding:
        # Embedding을 사용하지 않는 경우, 키워드 점수만 사용
        embedding_scores = keyword_scores.copy()
    else:
        embedding_scores = [0.0] * len(sentences)
    
    # 3단계: Score 조합 (동적 가중치 사용)
    for idx, sentence in enumerate(sentences):
        total_score = (
            final_keyword_weight * keyword_scores[idx] + 
            final_embedding_weight * embedding_scores[idx]
        )
        
        scored_sentences.append({
            'sentence': sentence,
            'keyword_score': keyword_scores[idx],
            'embedding_score': embedding_scores[idx],
            'total_score': total_score,
            'length': len(sentence)
        })
    
    # 점수순 정렬 (내림차순)
    scored_sentences.sort(key=lambda x: x['total_score'], reverse=True)
    
    # 상위 N개 문장 선택 (300~500자 범위 내)
    selected_sentences = []
    total_length = 0
    
    for item in scored_sentences:
        sentence = item['sentence']
        sentence_length = item['length']
        
        # 공백 추가 시 길이
        separator_length = 1 if selected_sentences else 0
        new_length = total_length + separator_length + sentence_length
        
        # 최대 길이 제한
        if new_length > max_chars:
            break
        
        # 최소 길이 확인 (최소한 min_chars는 채워야 함)
        if total_length < min_chars or new_length <= max_chars:
            selected_sentences.append(sentence)
            total_length = new_length
        
        # 최대 문장 개수 제한
        if len(selected_sentences) >= top_n:
            break
    
    # 최소 길이 미달 시 상위 문장 추가
    if total_length < min_chars and len(selected_sentences) < len(scored_sentences):
        for item in scored_sentences:
            if item['sentence'] in selected_sentences:
                continue
            
            sentence = item['sentence']
            sentence_length = item['length']
            separator_length = 1
            new_length = total_length + separator_length + sentence_length
            
            if new_length <= max_chars:
                selected_sentences.append(sentence)
                total_length = new_length
            
            if total_length >= min_chars:
                break
    
    # 결과 조합
    result = ' '.join(selected_sentences)
    
    # 길이 재확인 및 조정
    if len(result) > max_chars:
        result = result[:max_chars].rsplit(' ', 1)[0]  # 마지막 단어 제거
    
    logger.info(f"의미 기반 문장 추출 완료: {len(selected_sentences)}개 문장 선택, {len(result)}자")
    
    return result


def _split_sentences(text: str) -> List[str]:
    """
    텍스트를 문장 단위로 분리
    
    kss 라이브러리를 우선 사용하고, 없으면 기본 분할 사용
    """
    if not text:
        return []
    
    if HAS_KSS:
        try:
            return kss.split_sentences(text)
        except Exception as e:
            logger.warning(f"kss 분할 실패: {e}")
            return _split_sentences_fallback(text)
    else:
        return _split_sentences_fallback(text)


def _split_sentences_fallback(text: str) -> List[str]:
    """
    Fallback: kss가 없을 때 기본 문장 분할
    """
    import re
    
    if not text:
        return []
    
    # 문단 단위로 먼저 분리
    paragraphs = text.split('\n\n')
    all_sentences = []
    
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        
        # 문장 끝 패턴
        pattern = r'(?<!\d)(?<!\.)[.!?。]\s+(?=[가-힣A-Z])'
        sentences = re.split(pattern, para)
        
        for sent in sentences:
            sent = sent.strip()
            if sent and len(sent) > 1:
                all_sentences.append(sent)
    
    return all_sentences

