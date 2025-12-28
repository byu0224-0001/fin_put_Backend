"""
동적 가중치 계산 모듈

Softmax 기반 가중치 결정으로 Threshold Effect 제거 및 정확도 향상

주요 개선 사항:
1. 가중치 합 1.0 보장 (floating error 방지)
2. GPT Weight 상한 적용 (0.25로 제한, 비용 폭증 방지)
3. 수치 안정성 강화 (round, 정규화)
"""
import logging
from typing import Dict, Optional
import numpy as np

logger = logging.getLogger(__name__)

# GPT Weight 상한 (비용 폭증 방지)
GPT_WEIGHT_UPPER_BOUND = 0.25


def determine_dynamic_weights_softmax(
    rule_confidence: float,
    embedding_confidence: float,
    structure_confidence: float,
    tau: float = 0.5
) -> Dict[str, float]:
    """
    Softmax 기반 동적 가중치 결정
    
    Threshold 기반 분기 대신 Softmax를 사용하여 부드러운 가중치 조정
    
    Args:
        rule_confidence: Rule-based confidence (0.0 ~ 1.0)
        embedding_confidence: Embedding confidence (0.0 ~ 1.0)
        structure_confidence: Text structure confidence (0.0 ~ 1.0)
        tau: Temperature 파라미터 (낮을수록 민감, 높을수록 부드러움)
            - 0.3: 매우 민감 (confidence 차이 크게 반영)
            - 0.5: 기본 (권장)
            - 1.0: 부드러움 (confidence 차이 작게 반영)
    
    Returns:
        {
            "keyword_weight": 0.0~1.0,
            "embedding_weight": 0.0~1.0,
            "gpt_weight": 0.0~1.0
        }
        (합계 = 1.0)
    
    예시:
        >>> weights = determine_dynamic_weights_softmax(
        ...     rule_confidence=0.85,
        ...     embedding_confidence=0.80,
        ...     structure_confidence=0.90,
        ...     tau=0.5
        ... )
        >>> # Rule이 높으니 keyword_weight가 높게 나옴
        >>> print(weights)
        {'keyword_weight': 0.52, 'embedding_weight': 0.42, 'gpt_weight': 0.06}
    """
    # 입력 검증
    rule_confidence = max(0.0, min(1.0, rule_confidence))
    embedding_confidence = max(0.0, min(1.0, embedding_confidence))
    structure_confidence = max(0.0, min(1.0, structure_confidence))
    tau = max(0.1, tau)  # 0 이하 방지
    
    # GPT 필요도 = 구조 품질이 낮을수록 높음
    gpt_need = 1.0 - structure_confidence
    
    # 3개 confidence를 배열로
    confidences = np.array([
        rule_confidence,
        embedding_confidence,
        gpt_need
    ])
    
    # Softmax 계산 (수치 안정성을 위해 max 빼기)
    scaled = confidences / tau
    exp_scaled = np.exp(scaled - np.max(scaled))
    weights = exp_scaled / np.sum(exp_scaled)
    
    # GPT Weight 상한 적용 (비용 폭증 방지)
    gpt_weight_raw = float(weights[2])
    gpt_weight_capped = min(gpt_weight_raw, 0.25)  # 최대 25%로 제한
    
    # GPT weight가 제한되면 나머지를 Rule과 Embedding에 재분배
    if gpt_weight_capped < gpt_weight_raw:
        reduction = gpt_weight_raw - gpt_weight_capped
        # Rule과 Embedding에 비례하여 재분배
        remaining_sum = weights[0] + weights[1]
        if remaining_sum > 0:
            rule_weight = float(weights[0] + reduction * (weights[0] / remaining_sum))
            embedding_weight = float(weights[1] + reduction * (weights[1] / remaining_sum))
        else:
            rule_weight = float(weights[0] + reduction * 0.5)
            embedding_weight = float(weights[1] + reduction * 0.5)
    else:
        rule_weight = float(weights[0])
        embedding_weight = float(weights[1])
    
    result = {
        "keyword_weight": round(rule_weight, 4),
        "embedding_weight": round(embedding_weight, 4),
        "gpt_weight": round(gpt_weight_capped, 4)
    }
    
    # 가중치 합 1.0 보장 (floating error 방지)
    total = result["keyword_weight"] + result["embedding_weight"] + result["gpt_weight"]
    if abs(total - 1.0) > 1e-6:  # floating error 허용 범위
        # 정규화
        result["keyword_weight"] = round(result["keyword_weight"] / total, 4)
        result["embedding_weight"] = round(result["embedding_weight"] / total, 4)
        result["gpt_weight"] = round(result["gpt_weight"] / total, 4)
    
    logger.debug(
        f"동적 가중치 계산: "
        f"Rule={rule_confidence:.2f}, Embed={embedding_confidence:.2f}, "
        f"Struct={structure_confidence:.2f} → "
        f"Keyword={result['keyword_weight']:.2f}, "
        f"Embedding={result['embedding_weight']:.2f}, "
        f"GPT={result['gpt_weight']:.2f}"
    )
    
    return result


def determine_dynamic_weights_hybrid(
    rule_confidence: float,
    embedding_confidence: float,
    structure_confidence: float,
    tau: float = 0.5
) -> Dict[str, float]:
    """
    하이브리드 방식: 매우 높은 confidence는 고정, 나머지는 Softmax
    
    장점:
    - 매우 명확한 경우는 빠르게 처리
    - 애매한 경우는 Softmax로 부드럽게 처리
    
    Args:
        rule_confidence: Rule-based confidence (0.0 ~ 1.0)
        embedding_confidence: Embedding confidence (0.0 ~ 1.0)
        structure_confidence: Text structure confidence (0.0 ~ 1.0)
        tau: Temperature 파라미터
    
    Returns:
        {
            "keyword_weight": 0.0~1.0,
            "embedding_weight": 0.0~1.0,
            "gpt_weight": 0.0~1.0
        }
    """
    # Case 1: Rule confidence가 압도적으로 높음 (0.9 이상)
    if rule_confidence > 0.9:
        logger.debug(f"Case 1: Rule confidence 매우 높음 ({rule_confidence:.2f}) → 고정 가중치")
        return {
            "keyword_weight": round(0.85, 4),
            "embedding_weight": round(0.10, 4),
            "gpt_weight": round(0.05, 4)
        }
    
    # Case 2: Embedding confidence가 압도적으로 높고 Rule이 낮음
    elif embedding_confidence > 0.85 and rule_confidence < 0.5:
        logger.debug(
            f"Case 2: Embedding confidence 매우 높음 ({embedding_confidence:.2f}) "
            f"& Rule 낮음 ({rule_confidence:.2f}) → 고정 가중치"
        )
        return {
            "keyword_weight": round(0.25, 4),
            "embedding_weight": round(0.65, 4),
            "gpt_weight": round(0.10, 4)  # GPT weight 상한 이하
        }
    
    # Case 3: 나머지는 Softmax로 부드럽게 결정
    else:
        logger.debug("Case 3: Softmax 기반 가중치 결정")
        return determine_dynamic_weights_softmax(
            rule_confidence,
            embedding_confidence,
            structure_confidence,
            tau
        )

