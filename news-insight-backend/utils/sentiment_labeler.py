"""
Sentiment Labeler (Phase 2.0 P0+)

목적: 리포트 sentiment를 룰 우선 + LLM 보조로 재현성 확보

정책:
- 룰 기반 라벨링 1차 (조건부/양면/부정/긍정)
- 애매할 때만 LLM이 보조

⚠️ 책임 경계 (중요):
- Sentiment Labeler는 'sentiment 추출'만 담당
- Quality Gate의 is_valid 판정과는 독립적
- sentiment가 UNKNOWN이어도 추출 품질(is_valid)에는 영향 없음
- sentiment는 alignment 판정에만 사용됨
"""
import sys
from pathlib import Path
from typing import Dict, Optional, Any
import re
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 긍정 키워드
POSITIVE_KEYWORDS = [
    "증가", "상승", "개선", "회복", "확대", "성장", "호재", "긍정적",
    "향상", "확장", "증대", "강화", "우호적", "유리", "호조"
]

# 부정 키워드
NEGATIVE_KEYWORDS = [
    "감소", "하락", "악화", "위축", "축소", "부담", "부정적", "악재",
    "저하", "약화", "불리", "우려", "리스크", "부담", "압박"
]

# 조건부 키워드
CONDITIONAL_KEYWORDS = [
    "조건부", "상황에 따라", "경우에 따라", "만약", "만일",
    "~시", "~하면", "~할 경우", "~한다면"
]

# 양면 키워드
MIXED_KEYWORDS = [
    "양면", "양가", "복합", "혼재", "동시에", "반면",
    "하지만", "그러나", "다만", "단", "한편"
]


def label_sentiment_with_rules(text: str) -> Optional[str]:
    """
    룰 기반 sentiment 라벨링
    
    Args:
        text: 리포트 텍스트
    
    Returns:
        "POSITIVE" | "NEGATIVE" | "MIXED" | "CONDITIONAL" | None
    """
    text_lower = text.lower()
    
    # 양면 키워드 체크 (우선순위 1)
    mixed_count = sum(1 for keyword in MIXED_KEYWORDS if keyword in text_lower)
    if mixed_count >= 2:  # 양면 키워드 2개 이상
        return "MIXED"
    
    # 조건부 키워드 체크 (우선순위 2)
    conditional_count = sum(1 for keyword in CONDITIONAL_KEYWORDS if keyword in text_lower)
    if conditional_count >= 1:
        return "CONDITIONAL"
    
    # 긍정/부정 키워드 카운트
    positive_count = sum(1 for keyword in POSITIVE_KEYWORDS if keyword in text_lower)
    negative_count = sum(1 for keyword in NEGATIVE_KEYWORDS if keyword in text_lower)
    
    # 양면 판정 (긍정/부정 키워드 모두 존재)
    if positive_count > 0 and negative_count > 0:
        return "MIXED"
    
    # 긍정 판정
    if positive_count > negative_count and positive_count >= 2:
        return "POSITIVE"
    
    # 부정 판정
    if negative_count > positive_count and negative_count >= 2:
        return "NEGATIVE"
    
    # 애매한 경우
    return None


def label_sentiment_with_llm(text: str, max_length: int = 500) -> Optional[str]:
    """
    LLM 보조 sentiment 라벨링 (최후수단)
    
    Args:
        text: 리포트 텍스트
        max_length: 최대 길이
    
    Returns:
        "POSITIVE" | "NEGATIVE" | "MIXED" | "CONDITIONAL" | None
    """
    try:
        from openai import OpenAI
        import os
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.warning("OPENAI_API_KEY가 없어 LLM 라벨링을 건너뜁니다.")
            return None
        
        client = OpenAI(api_key=api_key)
        
        prompt = f"""다음 텍스트의 sentiment를 판정하세요.

텍스트: {text[:max_length]}

다음 중 하나만 선택하여 출력하세요:
- POSITIVE (긍정적)
- NEGATIVE (부정적)
- MIXED (양면적)
- CONDITIONAL (조건부)

응답은 선택한 단어만 출력하세요 (설명 없이).
"""
        
        response = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[
                {"role": "system", "content": "Sentiment 판정 전문가. 단어만 출력하고 설명은 하지 않습니다."},
                {"role": "user", "content": prompt}
            ],
            max_completion_tokens=10
        )
        
        sentiment = response.choices[0].message.content.strip().upper()
        
        if sentiment in ["POSITIVE", "NEGATIVE", "MIXED", "CONDITIONAL"]:
            return sentiment
        else:
            logger.warning(f"LLM이 예상하지 못한 sentiment 반환: {sentiment}")
            return None
    
    except Exception as e:
        logger.error(f"LLM sentiment 라벨링 실패: {e}")
        return None


def label_sentiment(
    text: str,
    use_llm: bool = False
) -> Dict[str, Any]:
    """
    Sentiment 라벨링 (룰 우선 + LLM 보조)
    
    Args:
        text: 리포트 텍스트
        use_llm: LLM 사용 여부 (룰로 판정 불가 시)
    
    Returns:
        {
            "sentiment": "POSITIVE" | "NEGATIVE" | "MIXED" | "CONDITIONAL",
            "method": "RULE" | "LLM",
            "confidence": float
        }
    """
    # 1차: 룰 기반 라벨링
    rule_sentiment = label_sentiment_with_rules(text)
    
    if rule_sentiment:
        return {
            "sentiment": rule_sentiment,
            "method": "RULE",
            "confidence": 0.85
        }
    
    # 2차: LLM 보조 (애매할 때만)
    if use_llm:
        llm_sentiment = label_sentiment_with_llm(text)
        if llm_sentiment:
            return {
                "sentiment": llm_sentiment,
                "method": "LLM",
                "confidence": 0.7
            }
    
    # 판정 불가
    return {
        "sentiment": "UNKNOWN",
        "method": "NONE",
        "confidence": 0.0
    }


if __name__ == "__main__":
    # 테스트 코드
    test_texts = [
        "유가 상승으로 인한 원가 부담이 증가하지만, 제품 가격 상승으로 마진이 개선될 수 있습니다.",
        "정제마진 회복이 핵심 변수이며, 중국 경기 회복 시 긍정적 영향을 받을 것으로 예상됩니다.",
        "OPEC 증산으로 공급 과잉이 발생할 경우 부정적 영향을 받을 수 있습니다."
    ]
    
    for text in test_texts:
        result = label_sentiment(text, use_llm=False)
        print(f"\n텍스트: {text[:50]}...")
        print(f"Sentiment: {result['sentiment']} (method: {result['method']}, confidence: {result['confidence']:.2f})")

