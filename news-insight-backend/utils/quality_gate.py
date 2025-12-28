"""
Quality Gate (Phase 2.0 P0)

목적: Extractor/Enrichment 품질 게이트

1. 추출 품질 게이트 (Extractor output validation)
2. Enrichment 게이트 (Edge append 조건)
3. Alignment 판정 규칙

⚠️ 책임 경계 (중요):
- is_valid: '추출 품질'만 판단 (JSON 파싱, 필수 필드, 길이, 의미 등)
- sentiment: 'alignment 판단'에만 영향 (is_valid와 독립적)
- sentiment가 애매해도 is_valid=False 처리하지 않음
- sentiment 때문에 enrichment 자체가 막히지 않게 유지
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
import re
import json
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 금지 문구 (컴플라이언스)
COMPLIANCE_BLACKLIST = [
    "목표주가",
    "매수",
    "매도",
    "비중확대",
    "비중축소",
    "투자의견",
    "목표가",
    "적정주가",
    "목표주가 상향",
    "목표주가 하향"
]

# 최소 길이 제한
MIN_ANALYST_LOGIC_LENGTH = 20  # 최소 20자
MIN_KEY_SENTENCE_LENGTH = 10   # 최소 10자


def validate_extractor_output(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    추출 품질 게이트 (Extractor output validation)
    
    Args:
        extracted_data: Insight Extractor 출력
    
    Returns:
        {
            "is_valid": bool,
            "quality_score": float,  # 0.0 ~ 1.0
            "issues": List[str],
            "compliance_flag": bool,
            "filtered_data": Dict  # 필터링된 데이터
        }
    """
    issues = []
    compliance_flag = False
    quality_score = 1.0
    
    # 1. JSON 파싱 실패 체크
    if not isinstance(extracted_data, dict):
        return {
            "is_valid": False,
            "quality_score": 0.0,
            "issues": ["JSON 파싱 실패"],
            "compliance_flag": True,
            "filtered_data": None
        }
    
    # 2. 필수 필드 체크
    required_fields = ["analyst_logic", "key_sentence"]
    for field in required_fields:
        if field not in extracted_data or not extracted_data[field]:
            issues.append(f"필수 필드 누락: {field}")
            quality_score -= 0.3
    
    # 3. analyst_logic 길이 체크
    analyst_logic = extracted_data.get("analyst_logic", "")
    if isinstance(analyst_logic, str):
        if len(analyst_logic.strip()) < MIN_ANALYST_LOGIC_LENGTH:
            issues.append(f"analyst_logic이 너무 짧음 ({len(analyst_logic)}자 < {MIN_ANALYST_LOGIC_LENGTH}자)")
            quality_score -= 0.2
        
        # 의미 없는 문장 체크
        if analyst_logic.strip() in ["없음", "N/A", "None", ""]:
            issues.append("analyst_logic이 의미 없음")
            quality_score -= 0.3
    
    # 4. key_sentence 길이 체크
    key_sentence = extracted_data.get("key_sentence", "")
    if isinstance(key_sentence, str):
        if len(key_sentence.strip()) < MIN_KEY_SENTENCE_LENGTH:
            issues.append(f"key_sentence이 너무 짧음 ({len(key_sentence)}자 < {MIN_KEY_SENTENCE_LENGTH}자)")
            quality_score -= 0.1
    
    # 5. 금지 문구 체크
    full_text = f"{analyst_logic} {key_sentence}".lower()
    for blacklist_term in COMPLIANCE_BLACKLIST:
        if blacklist_term in full_text:
            compliance_flag = True
            issues.append(f"금지 문구 포함: {blacklist_term}")
            quality_score -= 0.5
            break
    
    # 6. driver_candidates 유효성 체크
    driver_candidates = extracted_data.get("driver_candidates", [])
    if not isinstance(driver_candidates, list):
        issues.append("driver_candidates가 리스트가 아님")
        quality_score -= 0.2
    elif len(driver_candidates) == 0:
        issues.append("driver_candidates가 비어있음")
        quality_score -= 0.1
    
    # 품질 점수 정규화
    quality_score = max(0.0, min(1.0, quality_score))
    
    # 필터링된 데이터 (compliance_flag가 True면 노출 방식 변경)
    filtered_data = extracted_data.copy()
    if compliance_flag:
        # 금지 문구는 제거하지 않고, 플래그만 설정
        filtered_data["compliance_flag"] = True
        
        # key_sentence를 완전 패러프레이즈 강제 (사용자 노출용)
        # 원문은 broker_reports 내부 저장만, evidence_layer에는 짧은 키구절/요약형만 저장
        if "key_sentence" in filtered_data and filtered_data["key_sentence"]:
            # 짧은 요약형으로 변환 (원문 그대로 저장하지 않음)
            original_sentence = filtered_data["key_sentence"]
            # 간단한 요약 (실제로는 LLM으로 패러프레이즈 필요)
            filtered_data["key_sentence"] = original_sentence[:100] + "..." if len(original_sentence) > 100 else original_sentence
            filtered_data["key_sentence_original"] = original_sentence  # 원문은 별도 저장
    
    # compliance_flag가 True여도 차단하지 않고, 노출 방식만 변경
    is_valid = quality_score >= 0.5
    
    return {
        "is_valid": is_valid,
        "quality_score": quality_score,
        "issues": issues,
        "compliance_flag": compliance_flag,
        "filtered_data": filtered_data
    }


def check_enrichment_conditions(
    ticker: Optional[str],
    driver_code: Optional[str],
    edge_exists: bool
) -> Dict[str, Any]:
    """
    Enrichment 게이트 (Edge append 조건)
    
    Args:
        ticker: 매칭된 ticker
        driver_code: 매칭된 driver_code
        edge_exists: 기존 edge 존재 여부
    
    Returns:
        {
            "can_append": bool,
            "reason": str,
            "fallback_action": str  # "SKIP", "INDUSTRY_INSIGHT", "UNMATCHED_LOG"
        }
    """
    if not ticker:
        return {
            "can_append": False,
            "reason": "TICKER_NOT_FOUND",
            "fallback_action": "UNMATCHED_LOG"
        }
    
    if not driver_code:
        return {
            "can_append": False,
            "reason": "DRIVER_CODE_NOT_FOUND",
            "fallback_action": "INDUSTRY_INSIGHT"  # 산업 리포트일 수 있음
        }
    
    if not edge_exists:
        return {
            "can_append": False,
            "reason": "EDGE_NOT_EXISTS",
            "fallback_action": "SKIP"  # Hidden Edge 후보로 저장 가능 (Phase 2.1)
        }
    
    return {
        "can_append": True,
        "reason": "ALL_CONDITIONS_MET",
        "fallback_action": None
    }


def determine_alignment(
    kg_polarity: str,
    report_sentiment: str,
    report_conditions: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Alignment 판정 규칙
    
    Args:
        kg_polarity: KG의 polarity (POSITIVE, NEGATIVE, MIXED)
        report_sentiment: 리포트의 sentiment (POSITIVE, NEGATIVE, MIXED, CONDITIONAL)
        report_conditions: 리포트의 conditions (positive/negative 조건)
    
    Returns:
        {
            "alignment": "ALIGNED" | "PARTIAL" | "CONFLICT" | "UNKNOWN",
            "conflict_note": str,
            "confidence": float
        }
    """
    # 정규화
    kg_pol = kg_polarity.upper() if kg_polarity else "UNKNOWN"
    report_sent = report_sentiment.upper() if report_sentiment else "UNKNOWN"
    
    # 규칙표 기반 판정
    if kg_pol == "MIXED":
        if report_sent in ["MIXED", "CONDITIONAL"]:
            return {
                "alignment": "ALIGNED",
                "conflict_note": "",
                "confidence": 0.9
            }
        elif report_sent in ["POSITIVE", "NEGATIVE"]:
            return {
                "alignment": "PARTIAL",
                "conflict_note": f"KG는 MIXED인데 리포트는 {report_sent} 단정",
                "confidence": 0.7,
                "sentiment_method": sentiment_method,
                "report_sentiment": report_sent
            }
    
    elif kg_pol == "POSITIVE":
        if report_sent == "POSITIVE":
            return {
                "alignment": "ALIGNED",
                "conflict_note": "",
                "confidence": 0.9
            }
        elif report_sent == "NEGATIVE":
            return {
                "alignment": "CONFLICT",
                "conflict_note": "KG는 POSITIVE인데 리포트는 NEGATIVE",
                "confidence": 0.8,
                "sentiment_method": sentiment_method,
                "report_sentiment": report_sent
            }
        elif report_sent in ["MIXED", "CONDITIONAL"]:
            # 리포트가 조건부/단기 긍정을 언급
            if report_conditions and report_conditions.get("positive"):
                return {
                    "alignment": "PARTIAL",
                    "conflict_note": "KG는 POSITIVE인데 리포트는 조건부 긍정",
                    "confidence": 0.7,
                    "sentiment_method": sentiment_method,
                    "report_sentiment": report_sent
                }
            else:
                return {
                    "alignment": "PARTIAL",
                    "conflict_note": "KG는 POSITIVE인데 리포트는 MIXED",
                    "confidence": 0.6,
                    "sentiment_method": sentiment_method,
                    "report_sentiment": report_sent
                }
    
    elif kg_pol == "NEGATIVE":
        if report_sent == "NEGATIVE":
            return {
                "alignment": "ALIGNED",
                "conflict_note": "",
                "confidence": 0.9
            }
        elif report_sent == "POSITIVE":
            return {
                "alignment": "CONFLICT",
                "conflict_note": "KG는 NEGATIVE인데 리포트는 POSITIVE",
                "confidence": 0.8,
                "sentiment_method": sentiment_method,
                "report_sentiment": report_sent
            }
        elif report_sent in ["MIXED", "CONDITIONAL"]:
            # 리포트가 조건부/단기 부정을 언급
            if report_conditions and report_conditions.get("negative"):
                return {
                    "alignment": "PARTIAL",
                    "conflict_note": "KG는 NEGATIVE인데 리포트는 조건부 부정",
                    "confidence": 0.7,
                    "sentiment_method": sentiment_method,
                    "report_sentiment": report_sent
                }
            else:
                return {
                    "alignment": "PARTIAL",
                    "conflict_note": "KG는 NEGATIVE인데 리포트는 MIXED",
                    "confidence": 0.6,
                    "sentiment_method": sentiment_method,
                    "report_sentiment": report_sent
                }
    
    # 알 수 없는 경우
    return {
        "alignment": "UNKNOWN",
        "conflict_note": f"KG polarity={kg_pol}, 리포트 sentiment={report_sent} (method={sentiment_method})",
        "confidence": 0.5,
        "sentiment_method": sentiment_method  # 재현성 추적용
    }


def extract_temporal_hint(text: str) -> str:
    """
    Temporal hint 추출 (규칙 기반)
    
    Args:
        text: 리포트 텍스트
    
    Returns:
        "NEAR_TERM" | "MID_TERM" | "STRUCTURAL"
    """
    text_lower = text.lower()
    
    # 단기 키워드
    near_term_keywords = [
        "단기", "당분간", "올해", "올 하반기", "다음 분기",
        "향후 3개월", "향후 6개월", "단기적으로"
    ]
    
    # 중기 키워드
    mid_term_keywords = [
        "중기", "중장기", "향후 1~2년", "내년", "내후년",
        "향후 2~3년", "중기적으로"
    ]
    
    # 구조적 키워드
    structural_keywords = [
        "구조적", "장기", "근본적", "본질적", "구조적으로",
        "장기적으로", "근본적으로"
    ]
    
    # 키워드 매칭 (우선순위: 구조적 > 중기 > 단기)
    for keyword in structural_keywords:
        if keyword in text_lower:
            return "STRUCTURAL"
    
    for keyword in mid_term_keywords:
        if keyword in text_lower:
            return "MID_TERM"
    
    for keyword in near_term_keywords:
        if keyword in text_lower:
            return "NEAR_TERM"
    
    # 기본값: 중기
    return "MID_TERM"


if __name__ == "__main__":
    # 테스트 코드
    test_extracted = {
        "analyst_logic": "유가 변동보다는 정제마진 회복 여부가 주가의 핵심 트리거입니다.",
        "conditions": {
            "positive": "중국 경기 회복으로 석유제품 수요 증가 시",
            "negative": "OPEC 증산으로 공급 과잉 발생 시"
        },
        "key_sentence": "단기 유가 급등보다 정제마진 스프레드 개선이 중요",
        "driver_candidates": [{"text_mention": "정제마진", "confidence": 0.7}]
    }
    
    validation = validate_extractor_output(test_extracted)
    print(f"검증 결과: {validation}")
    
    alignment = determine_alignment("MIXED", "MIXED", test_extracted.get("conditions"))
    print(f"Alignment: {alignment}")
    
    temporal = extract_temporal_hint(test_extracted["analyst_logic"])
    print(f"Temporal hint: {temporal}")

