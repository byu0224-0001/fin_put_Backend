"""
Insight Extractor (Phase 2.0 P0 - v2)

목적: 리포트에서 3가지 핵심 인사이트 추출
1. analyst_logic: 분석가의 논리/트리거 (1~2문장, 섹터 관점 주장)
2. conditions: 긍정/부정 조건
3. key_sentence: 핵심 문장 (짧게)

v2 변경사항:
- 메타데이터(날짜, 이메일, 전화번호 등) 전처리 제거
- 프롬프트 강화: 섹터 관점 주장 형식 강제
- 메타데이터 오염 감지 (meta_leak_detected)

주의: 원문 그대로가 아니라 요약/파라프레이즈 (컴플라이언스)
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import re

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.services.llm_handler import LLMHandler
from langchain_core.messages import HumanMessage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 메타데이터 패턴 (제거 대상)
METADATA_PATTERNS = [
    r'▶\s*Analyst[^\n]*',  # ▶Analyst 라인
    r'Analyst[가-힣\s]+[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # Analyst 이름 + 이메일
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # 이메일
    r'02-\d{3,4}-\d{4}',  # 전화번호 (02-XXXX-XXXX)
    r'\d{2,3}-\d{3,4}-\d{4}',  # 전화번호 일반
    r'RA\s*[가-힣]+',  # RA 이름
    r'Compliance\s*Notice[^\n]*(?:\n[^\n]*){0,5}',  # Compliance Notice 블록
    r'본\s*자료는[^\n]*(?:\n[^\n]*){0,3}',  # 면책조항 시작
    r'투자\s*판단의\s*참고자료[^\n]*',  # 면책조항
    r'I\d{4}\.\d{2}\.\d{2}',  # 리포트 코드 (I2025.12.24)
]

# 메타데이터 오염 감지 패턴 (출력 검증용)
LEAK_PATTERNS = [
    r'\d{4}\.\d{2}\.\d{2}',  # 날짜 (2025.12.24)
    r'\d{2}\.\d{2}\.\d{2}',  # 날짜 (25.12.24)
    r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # 이메일 도메인
    r'Analyst',  # Analyst 키워드
    r'Weekly\s*I',  # Weekly 리포트 코드
    r'Compliance',  # Compliance
]


def clean_metadata(text: str) -> str:
    """
    리포트 텍스트에서 메타데이터 제거
    
    제거 대상:
    - 애널리스트 정보 (이름, 이메일, 전화번호)
    - 면책조항 (Compliance Notice)
    - 리포트 코드
    """
    if not text:
        return text
    
    cleaned = text
    for pattern in METADATA_PATTERNS:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # 연속 공백/줄바꿈 정리
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    cleaned = re.sub(r' {2,}', ' ', cleaned)
    
    return cleaned.strip()


def detect_metadata_leak(text: str) -> bool:
    """
    출력 텍스트에 메타데이터가 오염되었는지 확인
    
    Returns:
        True: 메타데이터 오염 감지
        False: 정상
    """
    if not text:
        return False
    
    leak_count = 0
    for pattern in LEAK_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            leak_count += 1
    
    # 2개 이상 패턴 매칭 시 오염으로 판단
    return leak_count >= 2


def detect_hallucination(original_text: str, analyst_logic: str) -> dict:
    """
    ⭐ LLM 환각 감지: 출력이 원본 텍스트에 근거하는지 확인
    
    방법: analyst_logic의 핵심 명사/숫자가 원본에 존재하는지 검증
    
    Args:
        original_text: 원본 리포트 텍스트
        analyst_logic: LLM이 생성한 analyst_logic
    
    Returns:
        {
            "is_hallucination": bool,
            "missing_terms": list,  # 원본에 없는 단어들
            "confidence": float     # 0.0 ~ 1.0 (높을수록 환각 의심)
        }
    """
    if not analyst_logic or not original_text:
        return {"is_hallucination": False, "missing_terms": [], "confidence": 0.0}
    
    original_lower = original_text.lower()
    
    # 1. 핵심 명사 추출 (한글 2글자 이상)
    korean_nouns = re.findall(r'[가-힣]{2,}', analyst_logic)
    
    # 2. 숫자 패턴 추출 (퍼센트, 금액 등)
    numbers = re.findall(r'\d+(?:\.\d+)?%?', analyst_logic)
    
    # 3. 영문 단어 추출 (3글자 이상, 약어 포함)
    english_words = re.findall(r'[A-Za-z]{3,}', analyst_logic)
    
    # 검증: 추출된 단어가 원본에 있는지
    missing_terms = []
    
    # 한글 명사 검증 (조사 제거 후 확인)
    for noun in korean_nouns:
        if len(noun) < 2:
            continue
        # 조사 제거
        noun_stem = noun.rstrip('은는이가을를에서로의와과')
        if len(noun_stem) < 2:
            continue
        # 원본에서 검색
        if noun_stem not in original_lower and noun not in original_lower:
            # 일반적인 표현은 제외 (전망, 예상, 증가, 하락 등)
            common_words = {'전망', '예상', '증가', '하락', '상승', '개선', '악화', 
                          '긍정', '부정', '수익', '실적', '성장', '감소', '확대',
                          '이유', '영향', '요인', '가능', '필요', '중요', '핵심'}
            if noun_stem not in common_words:
                missing_terms.append(noun)
    
    # 숫자 검증 (원본에 없는 숫자는 환각 의심)
    for num in numbers:
        if num not in original_text:
            missing_terms.append(num)
    
    # 영문 단어 검증 (일반 약어 제외)
    common_english = {'the', 'and', 'for', 'with', 'from', 'yoy', 'qoq', 'etc'}
    for word in english_words:
        word_lower = word.lower()
        if word_lower not in common_english and word_lower not in original_lower:
            missing_terms.append(word)
    
    # 환각 판단: 3개 이상 의심 단어가 있으면 환각
    missing_count = len(missing_terms)
    total_terms = len(korean_nouns) + len(numbers) + len(english_words)
    
    if total_terms == 0:
        confidence = 0.0
    else:
        confidence = min(missing_count / max(total_terms, 1), 1.0)
    
    is_hallucination = missing_count >= 3 or confidence >= 0.3
    
    if is_hallucination:
        logger.warning(f"환각 의심 감지: {missing_terms[:5]} (confidence: {confidence:.2f})")
    
    return {
        "is_hallucination": is_hallucination,
        "missing_terms": missing_terms[:10],  # 최대 10개만
        "confidence": confidence
    }


def extract_insights(
    report_text: str,
    report_title: str = "",
    driver_code: Optional[str] = None,
    llm_handler: Optional[LLMHandler] = None
) -> Dict[str, Any]:
    """
    리포트에서 3가지 핵심 인사이트 추출
    
    Args:
        report_text: 리포트 본문 텍스트
        report_title: 리포트 제목 (선택)
        driver_code: 관련 드라이버 코드 (선택, 컨텍스트 제공)
        llm_handler: LLM 핸들러 (None이면 새로 생성)
    
    Returns:
        {
            "analyst_logic": "분석가의 논리/트리거",
            "conditions": {
                "positive": "긍정 조건",
                "negative": "부정 조건"
            },
            "key_sentence": "핵심 문장",
            "extraction_confidence": "HIGH" | "MED" | "LOW",
            "temporal_hint": "NEAR_TERM" | "MID_TERM" | "STRUCTURAL"
        }
    """
    if not report_text or len(report_text.strip()) < 100:
        logger.warning("리포트 텍스트가 너무 짧습니다.")
        return {
            "analyst_logic": "",
            "conditions": {"positive": "", "negative": ""},
            "key_sentence": "",
            "extraction_confidence": "LOW",
            "temporal_hint": "UNKNOWN",
            "meta_leak_detected": False
        }
    
    # ✅ P0-1: 메타데이터 전처리 (이메일, 전화번호, 면책조항 제거)
    cleaned_text = clean_metadata(report_text)
    
    # 정제 후에도 너무 짧으면 실패
    if len(cleaned_text.strip()) < 100:
        logger.warning("메타데이터 제거 후 본문이 너무 짧습니다.")
        return {
            "analyst_logic": "",
            "conditions": {"positive": "", "negative": ""},
            "key_sentence": "",
            "extraction_confidence": "LOW",
            "temporal_hint": "UNKNOWN",
            "meta_leak_detected": True
        }
    
    # LLM 핸들러 초기화
    if llm_handler is None:
        llm_handler = LLMHandler()
    
    # ✅ P0-2: 프롬프트 강화 - 섹터 관점 주장 형식 강제
    driver_context = ""
    if driver_code:
        driver_context = f"\n관련 경제 변수: {driver_code}\n"
    
    prompt = f"""
너는 10년 차 펀드매니저야. 증권사 리포트에서 **투자 판단에 직접 사용되는 핵심 주장**만 추출해.

⛔ 환각 방지 (가장 중요):
1. 아래 본문에 있는 내용만 사용해라. 본문에 없는 내용을 절대 생성하지 마라.
2. 본문에 없는 숫자, 기업명, 산업명을 만들어내지 마라.
3. 본문에 있는 표현을 재조합/요약하여 사용해라.
4. 본문에서 직접 확인할 수 없는 추측은 하지 마라.

⚠️ 메타데이터 금지:
- 날짜, 저자, 증권사명, 이메일 등 언급 금지
- "리포트에서는...", "분석가가..." 같은 인용 표현 금지
- 원문 제목이나 리포트명 반복 금지

✅ 필수 형식 (섹터 관점 주장):
analyst_logic은 반드시 다음 구조로 작성:
"[산업/섹터]가 [변화(상승/하락/전환)] 전망. 이유: [드라이버/근거]"

⚠️ 핵심 제약: analyst_logic에 사용되는 산업명, 섹터명, 기업명, 숫자는 
반드시 아래 본문에서 직접 확인할 수 있어야 함.

예시:
- "반도체 업종이 AI 수요 급증으로 실적 개선 전망. HBM 가격 상승과 데이터센터 투자 확대가 핵심 동력."
- "정유 섹터 수익성 악화 예상. 국제유가 하락과 정제마진 축소가 부정적 요인."

[리포트 정보]
제목: {report_title}
{driver_context}

[본문 - 이 텍스트에서만 정보를 추출하라]:
{cleaned_text[:6000]}

[추출 항목]
1. analyst_logic (80~150자 권장):
   - 위 형식으로 산업/섹터 관점 핵심 주장 작성
   - ⚠️ 본문에 있는 단어만 사용 (새로운 단어 생성 금지)
   
2. conditions:
   - positive: 긍정 시나리오 조건 (본문에서 추출, 없으면 빈 문자열)
   - negative: 부정 시나리오 조건 (본문에서 추출, 없으면 빈 문자열)
   
3. key_sentence (40~60자):
   - 본문에서 가장 중요한 문장을 요약 (본문 표현 기반)
   
4. temporal_hint:
   - NEAR_TERM: 1~3개월
   - MID_TERM: 3~12개월
   - STRUCTURAL: 장기/구조적

[반환 형식]
JSON만 반환 (Markdown 없이):
{{
    "analyst_logic": "...",
    "conditions": {{
        "positive": "...",
        "negative": "..."
    }},
    "key_sentence": "...",
    "temporal_hint": "NEAR_TERM" | "MID_TERM" | "STRUCTURAL"
}}
"""
    
    try:
        logger.info("LLM으로 인사이트 추출 중...")
        response = llm_handler.llm.invoke([HumanMessage(content=prompt)])
        response_text = response.content.strip()
        
        # 토큰 사용량 추출 (응답 메타데이터에서)
        usage_info = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }
        
        if hasattr(response, 'response_metadata') and response.response_metadata:
            usage = response.response_metadata.get('token_usage', {})
            if usage:
                usage_info["prompt_tokens"] = usage.get("prompt_tokens", 0)
                usage_info["completion_tokens"] = usage.get("completion_tokens", 0)
                usage_info["total_tokens"] = usage.get("total_tokens", 0)
        
        # 토큰 사용량 추출 (응답 메타데이터에서)
        usage_info = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }
        
        if hasattr(response, 'response_metadata') and response.response_metadata:
            usage = response.response_metadata.get('token_usage', {})
            if usage:
                usage_info["prompt_tokens"] = usage.get("prompt_tokens", 0)
                usage_info["completion_tokens"] = usage.get("completion_tokens", 0)
                usage_info["total_tokens"] = usage.get("total_tokens", 0)
        
        # JSON 파싱
        import json
        import json_repair
        
        # JSON 수리 시도
        try:
            result = json.loads(response_text)
        except json.JSONDecodeError:
            # json_repair로 수리 시도
            repaired = json_repair.repair_json(response_text)
            result = json.loads(repaired)
        
        # ✅ P0-3: 메타데이터 오염 감지
        analyst_logic = result.get("analyst_logic", "")
        meta_leak = detect_metadata_leak(analyst_logic)
        result["meta_leak_detected"] = meta_leak
        
        if meta_leak:
            logger.warning(f"메타데이터 오염 감지: {analyst_logic[:100]}...")
        
        # ✅ P0-4: 환각 감지 (원본 텍스트에 없는 내용 생성 여부)
        hallucination_result = detect_hallucination(cleaned_text, analyst_logic)
        result["hallucination_detected"] = hallucination_result["is_hallucination"]
        result["hallucination_confidence"] = hallucination_result["confidence"]
        result["hallucination_missing_terms"] = hallucination_result["missing_terms"]
        
        if hallucination_result["is_hallucination"]:
            logger.warning(f"환각 감지: missing={hallucination_result['missing_terms'][:5]}")
        
        # 신뢰도 판정
        confidence = "HIGH"
        if not analyst_logic or len(analyst_logic) < 20:
            confidence = "LOW"
        elif meta_leak:
            confidence = "LOW"  # 메타데이터 오염 시 신뢰도 하향
        elif hallucination_result["is_hallucination"]:
            confidence = "LOW"  # 환각 감지 시 신뢰도 하향
        elif hallucination_result["confidence"] > 0.2:
            confidence = "MED"  # 환각 의심 시 신뢰도 중간
        elif not result.get("key_sentence") or len(result.get("key_sentence", "")) < 10:
            confidence = "MED"
        
        result["extraction_confidence"] = confidence
        
        # temporal_hint 기본값
        if "temporal_hint" not in result:
            result["temporal_hint"] = "MID_TERM"
        
        logger.info(f"인사이트 추출 완료 (confidence: {confidence}, leak: {meta_leak})")
        
        # 토큰 사용량 포함
        result["_usage"] = usage_info
        
        return result
        
    except Exception as e:
        logger.error(f"인사이트 추출 실패: {e}", exc_info=True)
        return {
            "analyst_logic": "",
            "conditions": {"positive": "", "negative": ""},
            "key_sentence": "",
            "extraction_confidence": "LOW",
            "temporal_hint": "UNKNOWN",
            "meta_leak_detected": False
        }

