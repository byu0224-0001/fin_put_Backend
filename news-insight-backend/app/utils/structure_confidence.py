"""
Text Structure Confidence 계산 모듈

biz_summary 품질을 정량적으로 평가하여 structure_confidence 계산
- 문장 수 중심 평가 (길이 편차 보정)
- 키워드 다양성 평가
- 사업 설명 키워드 포함 여부
"""
import logging
from typing import Optional, List
import re

logger = logging.getLogger(__name__)

# kss 라이브러리
try:
    import kss
    HAS_KSS = True
except ImportError:
    HAS_KSS = False
    logger.warning("kss 라이브러리가 설치되지 않았습니다.")


def calculate_structure_confidence(
    biz_summary: str,
    products: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None
) -> float:
    """
    Text Structure Confidence 계산
    
    biz_summary 품질을 정량적으로 평가
    - 문장 수 중심 (길이 편차 보정)
    - 키워드 다양성
    - 사업 설명 키워드 포함 여부
    
    Args:
        biz_summary: 사업 개요 텍스트
        products: 제품 리스트 (선택)
        keywords: 키워드 리스트 (선택)
    
    Returns:
        0.0 ~ 1.0 사이의 confidence
    
    예시:
        >>> conf = calculate_structure_confidence(
        ...     biz_summary="당사는 반도체를 제조하고 있습니다. 주요 제품은 DRAM과 NAND입니다.",
        ...     products=["DRAM", "NAND"],
        ...     keywords=["반도체", "제조"]
        ... )
        >>> print(conf)
        0.65
    """
    if not biz_summary:
        return 0.0
    
    scores = []
    
    # 1. 문장 수 평가 (길이 편차 보정)
    # DART 요약문은 기업별 편차가 심하므로 길이보다 문장 수가 더 신뢰성 높음
    num_sentences = _count_sentences(biz_summary)
    # 3문장 이상이면 만점, 1문장 이하면 낮음
    sentence_score = min(num_sentences / 3.0, 1.0) if num_sentences > 0 else 0.0
    scores.append(("sentence_count", sentence_score * 0.35))  # 35% 가중치
    
    # 2. 텍스트 길이 평가 (보조 지표)
    # 너무 짧으면(100자 미만) 낮음, 적당하면(300자 이상) 만점
    length_score = min(len(biz_summary) / 300.0, 1.0) if len(biz_summary) > 0 else 0.0
    # 길이는 보조 지표이므로 낮은 가중치
    scores.append(("length", length_score * 0.15))  # 15% 가중치
    
    # 3. 키워드 다양성 평가
    if keywords:
        unique_keywords = len(set([str(k).lower().strip() for k in keywords if k]))
        # 5개 이상이면 만점, 1개면 낮음
        keyword_diversity = min(unique_keywords / 5.0, 1.0)
        scores.append(("keyword_diversity", keyword_diversity * 0.25))  # 25% 가중치
    else:
        scores.append(("keyword_diversity", 0.0))
    
    # 4. 사업 설명 키워드 포함 여부 (가장 중요)
    business_keywords = [
        '주력', '제품', '사업', '매출', '비중', '시장', '고객',
        '영위', '생산', '제조', '판매', '공급', '서비스', '시장',
        '경쟁력', '핵심', '주요', '사업 영역', '매출 구성'
    ]
    business_matches = sum(1 for kw in business_keywords if kw in biz_summary)
    # 5개 이상 포함되면 만점
    business_score = min(business_matches / 5.0, 1.0)
    scores.append(("business_keywords", business_score * 0.25))  # 25% 가중치
    
    # 최종 점수 계산
    total_score = sum(score for _, score in scores)
    structure_confidence = min(total_score, 1.0)
    
    logger.debug(
        f"Structure confidence 계산: "
        f"문장수={num_sentences}, 길이={len(biz_summary)}, "
        f"키워드다양성={unique_keywords if keywords else 0}, "
        f"사업키워드={business_matches} → {structure_confidence:.2f}"
    )
    
    return round(structure_confidence, 4)


def _count_sentences(text: str) -> int:
    """
    텍스트의 문장 수 계산
    
    kss를 우선 사용하고, 없으면 기본 분할 사용
    """
    if not text:
        return 0
    
    if HAS_KSS:
        try:
            sentences = kss.split_sentences(text)
            # 의미 있는 문장만 카운트 (너무 짧은 문장 제외)
            meaningful_sentences = [s for s in sentences if len(s.strip()) > 10]
            return len(meaningful_sentences)
        except Exception as e:
            logger.warning(f"kss 문장 분할 실패: {e}")
            return _count_sentences_fallback(text)
    else:
        return _count_sentences_fallback(text)


def _count_sentences_fallback(text: str) -> int:
    """
    Fallback: kss가 없을 때 기본 문장 분할
    """
    if not text:
        return 0
    
    # 문장 끝 패턴
    # 한국어: . ! ? 。 (단, 숫자 뒤의 .는 제외)
    pattern = r'(?<!\d)(?<!\.)[.!?。]\s+(?=[가-힣A-Z])|(?<!\d)(?<!\.)[.!?。]$'
    sentences = re.split(pattern, text)
    
    # 의미 있는 문장만 카운트
    meaningful_sentences = [s for s in sentences if s and len(s.strip()) > 10]
    
    return len(meaningful_sentences)

