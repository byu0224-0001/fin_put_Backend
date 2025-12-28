"""
Driver Normalizer (Phase 2.0 P0+)

목적: 리포트 표현(정제마진/크랙스프레드/운임 등)을 KG driver_code로 정규화

3단계 매핑:
1. 동의어 사전 기반 매핑 (economic_variables.synonyms)
2. 임베딩 기반 후보 추천 (선택)
3. LLM 최후수단 (후보만 제시, 확정 금지)
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import re
import logging
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.models.economic_variable import EconomicVariable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 수동 동의어 사전 (economic_variables.synonyms에 없는 경우 보완)
MANUAL_SYNONYM_MAP = {
    "정제마진": ["REFINING_MARGIN", "OIL_PRICE"],
    "크랙스프레드": ["REFINING_MARGIN", "OIL_PRICE"],
    "운임": ["FREIGHT_RATE", "SHIPPING_RATE"],
    "해운운임": ["FREIGHT_RATE", "SHIPPING_RATE"],
    "컨테이너운임": ["FREIGHT_RATE", "CONTAINER_RATE"],
    "D램 가격": ["DRAM_ASP"],
    "낸드 가격": ["NAND_ASP"],
    "메모리 가격": ["DRAM_ASP", "NAND_ASP"],
    "반도체 가격": ["DRAM_ASP", "NAND_ASP", "SEMICONDUCTOR_PRICE"],
    "원유 가격": ["OIL_PRICE", "OIL_PRICE_WTI"],
    "유가": ["OIL_PRICE", "OIL_PRICE_WTI"],
    "금리": ["INTEREST_RATE", "RATE_FED"],
    "기준금리": ["INTEREST_RATE", "RATE_FED"],
    "환율": ["EXCHANGE_RATE", "USD_KRW"],
    "원달러": ["USD_KRW"],
    "원자재 가격": ["COMMODITY_PRICE"],
    "철강 가격": ["STEEL_PRICE"],
    "석탄 가격": ["COAL_PRICE"],
    "가스 가격": ["GAS_PRICE", "LNG_PRICE"]
}


def normalize_driver_with_synonyms(
    report_text: str,
    db: Optional[Session] = None
) -> List[Dict[str, Any]]:
    """
    1단계: 동의어 사전 기반 매핑
    
    Args:
        report_text: 리포트 텍스트 (키워드 또는 문장)
        db: DB 세션 (None이면 새로 생성)
    
    Returns:
        [{"driver_code": "...", "confidence": 0.9, "method": "SYNONYM", "matched_term": "..."}, ...]
    """
    if db is None:
        db = SessionLocal()
        should_close = True
    else:
        should_close = False
    
    try:
        # economic_variables 테이블에서 모든 변수 조회
        all_vars = db.query(EconomicVariable).all()
        
        candidates = []
        report_text_lower = report_text.lower()
        
        # 1. 수동 동의어 사전 체크
        for term, driver_codes in MANUAL_SYNONYM_MAP.items():
            if term in report_text or term.lower() in report_text_lower:
                for driver_code in driver_codes:
                    candidates.append({
                        "driver_code": driver_code,
                        "confidence": 0.85,  # 수동 사전은 높은 신뢰도
                        "method": "MANUAL_SYNONYM",
                        "matched_term": term
                    })
        
        # 2. economic_variables.synonyms 체크
        for var in all_vars:
            synonyms = var.synonyms or []
            name_ko = var.name_ko or ""
            
            # code, name_ko, synonyms 모두 체크
            search_terms = [var.code, name_ko] + synonyms
            
            for term in search_terms:
                if not term:
                    continue
                
                term_lower = term.lower()
                
                # 정확히 일치
                if term in report_text or term_lower in report_text_lower:
                    candidates.append({
                        "driver_code": var.code,
                        "confidence": 0.9,
                        "method": "SYNONYM_EXACT",
                        "matched_term": term
                    })
                # 부분 일치 (단어 단위)
                elif any(word in report_text_lower for word in term_lower.split() if len(word) > 2):
                    candidates.append({
                        "driver_code": var.code,
                        "confidence": 0.7,
                        "method": "SYNONYM_PARTIAL",
                        "matched_term": term
                    })
        
        # 중복 제거 및 confidence 기준 정렬
        seen = set()
        unique_candidates = []
        for candidate in candidates:
            key = candidate["driver_code"]
            if key not in seen:
                seen.add(key)
                unique_candidates.append(candidate)
            else:
                # 이미 있으면 confidence 높은 것으로 업데이트
                existing = next(c for c in unique_candidates if c["driver_code"] == key)
                if candidate["confidence"] > existing["confidence"]:
                    existing.update(candidate)
        
        # confidence 내림차순 정렬
        unique_candidates.sort(key=lambda x: x["confidence"], reverse=True)
        
        return unique_candidates[:5]  # Top-5 반환
    
    finally:
        if should_close:
            db.close()


def normalize_driver_with_llm(
    report_text: str,
    candidates_from_synonym: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    3단계: LLM 최후수단 (후보만 제시, 확정 금지)
    
    Args:
        report_text: 리포트 텍스트
        candidates_from_synonym: 1단계 결과 (있으면 힌트로 사용)
    
    Returns:
        [{"driver_code": "...", "confidence": 0.6, "method": "LLM", "reason": "..."}, ...]
    """
    try:
        from openai import OpenAI
        from dotenv import load_dotenv
        import os
        
        load_dotenv(project_root / '.env')
        api_key = os.getenv("OPENAI_API_KEY")
        
        if not api_key:
            logger.warning("OPENAI_API_KEY가 없어 LLM 매핑을 건너뜁니다.")
            return []
        
        client = OpenAI(api_key=api_key)
        
        # 힌트 구성
        hint_text = ""
        if candidates_from_synonym:
            hint_codes = [c["driver_code"] for c in candidates_from_synonym[:3]]
            hint_text = f"\n\n참고: 동의어 매칭 결과 후보: {', '.join(hint_codes)}"
        
        prompt = f"""리포트 텍스트에서 언급된 경제 변수를 표준 코드로 매핑하세요.

텍스트: {report_text[:500]}{hint_text}

다음 JSON 형식으로 응답하세요 (최대 3개):
{{
  "candidates": [
    {{
      "driver_code": "OIL_PRICE",
      "confidence": 0.7,
      "reason": "유가 관련 언급"
    }}
  ]
}}

주의:
- driver_code는 economic_variables 테이블의 code 필드 값이어야 합니다.
- 확정하지 말고 후보만 제시하세요.
- confidence는 0.5~0.8 사이로 설정하세요.
- 없으면 빈 배열을 반환하세요.
"""
        response = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[
                {"role": "system", "content": "경제 변수 표준 코드 매핑 전문가. 후보만 제시하고 확정하지 않습니다."},
                {"role": "user", "content": prompt}
            ],
            max_completion_tokens=200
        )
        
        import json
        result_text = response.choices[0].message.content.strip()
        
        # JSON 추출
        json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
        if json_match:
            result_json = json.loads(json_match.group())
            candidates = result_json.get("candidates", [])
            
            # method 추가
            for candidate in candidates:
                candidate["method"] = "LLM"
            
            return candidates
        else:
            logger.warning(f"LLM 응답에서 JSON을 찾을 수 없습니다: {result_text[:100]}")
            return []
    
    except Exception as e:
        logger.error(f"LLM 매핑 실패: {e}")
        return []


def normalize_driver(
    report_text: str,
    use_llm: bool = False,
    confidence_threshold: float = 0.7,
    db: Optional[Session] = None,
    report_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Driver 정규화 (3단계)
    
    Args:
        report_text: 리포트 텍스트 또는 키워드
        use_llm: LLM 사용 여부 (confidence 낮을 때만)
        confidence_threshold: 최소 confidence (이상이면 LLM 스킵)
        db: DB 세션
        report_metadata: 리포트 메타데이터 (후보 등록용)
    
    Returns:
        {
            "driver_code": "OIL_PRICE",  # Top-1 (confidence 가장 높은 것)
            "confidence": 0.9,
            "method": "SYNONYM_EXACT",
            "matched_term": "유가",
            "all_candidates": [...]  # Top-5
        }
    """
    # 1단계: 동의어 사전 매핑
    synonym_candidates = normalize_driver_with_synonyms(report_text, db)
    
    if not synonym_candidates:
        # 후보가 없으면 LLM 시도
        if use_llm:
            llm_candidates = normalize_driver_with_llm(report_text, None)
            if llm_candidates:
                synonym_candidates = llm_candidates
    
    # Top-1 선택
    if synonym_candidates:
        top_candidate = synonym_candidates[0]
        
        # confidence가 낮으면 LLM 보완
        if use_llm and top_candidate["confidence"] < confidence_threshold:
            llm_candidates = normalize_driver_with_llm(report_text, synonym_candidates)
            if llm_candidates:
                # LLM 결과와 비교하여 더 높은 confidence 선택
                llm_top = llm_candidates[0]
                if llm_top["confidence"] > top_candidate["confidence"]:
                    top_candidate = llm_top
                    synonym_candidates = llm_candidates + synonym_candidates
        
        return {
            "driver_code": top_candidate["driver_code"],
            "confidence": top_candidate["confidence"],
            "method": top_candidate["method"],
            "matched_term": top_candidate.get("matched_term", ""),
            "all_candidates": synonym_candidates[:5]
        }
    else:
        # ⭐ 모르는 단어가 나오면 후보로 등록
        # 리포트 텍스트에서 경제 변수로 보이는 단어 추출
        unknown_terms = _extract_unknown_economic_terms(report_text)
        
        # 후보 등록 (비동기 또는 별도 프로세스로 처리 가능)
        # ⭐ unknown_terms는 이제 (terms_list, metrics) 튜플
        if unknown_terms and isinstance(unknown_terms, tuple):
            terms_list, extraction_metrics = unknown_terms
            if terms_list:
                _register_driver_candidates(terms_list, report_text, report_metadata, extraction_metrics)
        elif unknown_terms:
            # 레거시 호환성 (리스트인 경우)
            _register_driver_candidates(unknown_terms, report_text, report_metadata)
        
        return {
            "driver_code": None,
            "confidence": 0.0,
            "method": "NONE",
            "matched_term": "",
            "all_candidates": [],
            "unknown_terms": unknown_terms  # ⭐ 후보 등록된 단어들
        }


def _extract_unknown_economic_terms(report_text: str) -> Tuple[List[str], Dict[str, Any]]:
    """
    리포트 텍스트에서 경제 변수로 보이는 모르는 단어 추출
    
    ⭐ 개선: 패턴 확장 및 신조어/최신 정책/약어 포함
    ⭐ P0-추가 3: known/unknown 분해 계측
    
    Args:
        report_text: 리포트 텍스트 (원문 chunk 권장)
    
    Returns:
        (추출된 unknown 단어 리스트, 메트릭 딕셔너리)
    """
    # 경제 변수 패턴 키워드 (확장)
    economic_keywords = [
        # 기존
        "마진", "프레드", "운임", "가격", "금리", "환율", "수요", "공급",
        "비용", "원가", "ASP", "CAPEX", "OPEX", "EBITDA", "ROE", "ROA",
        # 추가: 신조어/최신 정책/약어
        "D램", "NAND", "HBM", "AI", "ESG", "탄소", "규제", "정책",
        "법안", "협상", "관세", "수출", "수입", "물가", "인플레이션",
        "디플레이션", "경기", "경제", "성장", "침체", "회복", "부양",
        "긴축", "완화", "통화", "정책", "금융", "시장", "주식", "채권"
    ]
    
    # 문장 단위로 분리
    sentences = re.split(r'[.!?]\s+', report_text)
    unknown_terms = []
    
    for sentence in sentences:
        # 경제 변수 키워드가 포함된 문장에서 숫자/단위와 함께 나오는 단어 추출
        for keyword in economic_keywords:
            if keyword in sentence:
                # 키워드 주변 단어 추출 (예: "정제마진", "크랙스프레드", "D램 가격")
                words = re.findall(r'\b\w*' + keyword + r'\w*\b', sentence)
                unknown_terms.extend(words)
        
        # 추가: 신조어/약어 패턴 (키워드 없이도 추출)
        # 예: "AI 정책", "ESG 규제", "D램 가격" 등
        tech_policy_patterns = [
            r'\b[A-Z]{2,}\s+(?:정책|규제|법안|협상|가격|마진|프레드)\b',  # AI 정책, ESG 규제
            r'\b(?:D램|NAND|HBM)\s+(?:가격|수요|공급|마진)\b',  # D램 가격
            r'\b(?:AI|ESG|탄소)\s+(?:정책|규제|법안|협상)\b',  # AI 정책
        ]
        for pattern in tech_policy_patterns:
            matches = re.findall(pattern, sentence)
            unknown_terms.extend(matches)
        
        # ⭐ P0-E: 복합 경제 변수 패턴 추가
        complex_patterns = [
            # 금리 관련 복합 변수
            r'\d+년물\s*(?:국채|채권)\s*(?:금리|수익률|스프레드)',
            r'(?:국채|채권)\s*(?:금리|수익률)\s*(?:스프레드|차이)',
            r'(?:기준금리|정책금리)\s*(?:인상|인하|동결)',
            
            # 환율 관련 복합 변수
            r'(?:달러|원화|엔화|위안화)\s*(?:환율|강세|약세)',
            r'(?:USD|KRW|JPY|CNY)\s*(?:환율|강세|약세)',
            
            # 물가 관련 복합 변수
            r'(?:CPI|PPI|물가)\s*(?:상승률|하락률|전년동기)',
            r'(?:인플레이션|디플레이션)\s*(?:압력|우려|전망)',
            
            # 에너지 관련 복합 변수
            r'(?:원유|유가|석유)\s*(?:가격|상승|하락)',
            r'(?:WTI|브렌트)\s*(?:가격|상승|하락)',
            r'(?:정제마진|크랙스프레드)\s*(?:회복|개선|악화)',
            
            # 반도체 관련 복합 변수
            r'(?:D램|NAND|HBM)\s*(?:가격|ASP|수요|공급)',
            r'(?:반도체|메모리)\s*(?:가격|ASP|수요|공급)',
            
            # 정책/규제 관련 복합 변수
            r'(?:정책|규제|법안)\s*(?:변화|개정|강화|완화)',
            r'(?:SEC|CFTC|FED)\s*(?:정책|규제|법안)',
        ]
        
        for pattern in complex_patterns:
            matches = re.findall(pattern, sentence, re.IGNORECASE)
            unknown_terms.extend(matches)
    
    # 중복 제거 및 정리
    unknown_terms = list(set([term.strip() for term in unknown_terms if len(term.strip()) > 2]))
    
    # ⭐ P0.5-1: 후보 품질 필터 (길이/형태 필터)
    # ⭐ P0-G: 후보 추출 정확도 가드 (블랙리스트/동의어 정규화)
    filtered_terms = []
    
    # 블랙리스트 토큰: 변수 아닌 문장용 단어
    blacklist_tokens = [
        "전망", "영향", "증가", "감소", "변화", "추세", "동향", "전략", "정책", "규제",
        "우려", "개선", "악화", "가능성", "압력", "전망", "예상", "기대", "결론"
    ]
    
    # 단위/기호 정규화 + stop pattern: "전년동기" 같은 서술만 있는 후보는 제외
    stop_patterns = [
        r'^전년동기$',  # "전년동기" 단독
        r'^YoY$', r'^QoQ$',  # 단위만
        r'^%$', r'^bp$',  # 기호만
        r'^(?:증가|감소|변화|추세|동향)$',  # 서술어만
    ]
    
    for term in unknown_terms:
        # 길이 필터: 2~30자 범위
        if len(term) < 2 or len(term) > 30:
            continue
        
        # 숫자만/기호만 제외
        if term.isdigit() or all(not c.isalnum() for c in term):
            continue
        
        # ⭐ P0-G: 블랙리스트 토큰 체크 (단독으로 후보가 되지 않게)
        term_normalized = term.strip()
        if term_normalized in blacklist_tokens:
            continue
        
        # ⭐ P0-G: stop pattern 체크 (서술만 있는 후보 제외)
        skip_term = False
        for pattern in stop_patterns:
            if re.match(pattern, term_normalized, re.IGNORECASE):
                skip_term = True
                break
        if skip_term:
            continue
        
        # 한글/영문/숫자 조합만 허용 (기호는 최소화)
        if len([c for c in term if c.isalnum()]) < len(term) * 0.7:  # 70% 이상이 알파벳/숫자여야 함
            continue
        
        filtered_terms.append(term)
    
    # ⭐ P0.5-1: 중복 군집 지표 계산
    normalized_terms = [_normalize_candidate_term(term) for term in filtered_terms]
    unique_normalized = len(set(normalized_terms))
    total_terms = len(filtered_terms)
    
    if total_terms > 0:
        duplicate_ratio = 1.0 - (unique_normalized / total_terms)
        if duplicate_ratio > 0.7:  # 70% 이상이 중복이면 경고
            logger.warning(f"후보 중복 군집 경고: {total_terms}개 중 {unique_normalized}개만 고유 (중복률: {duplicate_ratio:.2%})")
    
    return filtered_terms[:10]  # 최대 10개로 확장


def _normalize_candidate_term(term: str) -> str:
    """
    후보 텍스트 정규화 (동일어/정규화 레이어)
    
    ⭐ P0-4: 동일어/정규화 레이어 적용
    
    Args:
        term: 원본 후보 텍스트
    
    Returns:
        정규화된 텍스트
    """
    if not term:
        return ""
    
    # 1. 대소문자/공백/하이픈 정리
    normalized = term.strip()
    normalized = re.sub(r'\s+', ' ', normalized)  # 연속 공백 → 1칸
    normalized = re.sub(r'[-_]+', '-', normalized)  # 연속 하이픈 → 1개
    
    # 2. 한글/영문 표기 변형 (간단 룰)
    # 예: "D램" → "DRAM", "D-RAM" → "DRAM"
    normalized = re.sub(r'[Dd][-]?[Rr][Aa][Mm]', 'DRAM', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'[Dd]램', 'DRAM', normalized)
    normalized = re.sub(r'[Nn][Aa][Nn][Dd]', 'NAND', normalized)
    normalized = re.sub(r'[Hh][Bb][Mm]', 'HBM', normalized)
    
    # 3. 숫자/단위 표준화
    normalized = re.sub(r'(\d+)\s*%', r'\1%', normalized)  # "10 %" → "10%"
    normalized = re.sub(r'(\d+)\s*bp', r'\1bp', normalized, flags=re.IGNORECASE)  # "10 bp" → "10bp"
    
    return normalized.strip()


def _register_driver_candidates(unknown_terms: List[str], report_text: str, 
                                 report_metadata: Optional[Dict[str, Any]] = None,
                                 extraction_metrics: Optional[Dict[str, Any]] = None):
    """
    모르는 단어를 드라이버 후보로 등록
    
    ⭐ 개선: 후보 등록 폭주 방지 강화
    - 리포트당 상한 체크 (최대 10개)
    - 쿨다운 체크 (24시간 내 동일 term 재등록 방지)
    - ⭐ P0-4: 동일어/정규화 레이어 적용
    
    Args:
        unknown_terms: 추출된 모르는 단어 리스트
        report_text: 리포트 텍스트
        report_metadata: 리포트 메타데이터 (선택)
    """
    try:
        from app.db import SessionLocal
        from app.models.driver_candidate import DriverCandidate
        from sqlalchemy import and_
        from datetime import timedelta
        
        db = SessionLocal()
        
        try:
            report_id = report_metadata.get("report_id") if report_metadata else None
            
            # ⭐ 1. 리포트당 상한 체크 (최대 10개)
            if report_id:
                existing_count = db.query(DriverCandidate).filter(
                    DriverCandidate.source_report_id == report_id
                ).count()
                
                if existing_count >= 10:
                    logger.warning(f"리포트당 후보 상한 도달: {report_id} (현재: {existing_count}개)")
                    return
            
            registered_count = 0
            skipped_count = 0
            skipped_cooldown_count = 0
            skipped_unique_conflict_count = 0  # ⭐ P0-추가 4: unique 충돌 카운트
            
            for term in unknown_terms:
                # ⭐ P0-4: 정규화 적용
                normalized_term = _normalize_candidate_term(term)
                if not normalized_term or len(normalized_term) < 2:
                    continue
                
                # ⭐ 2. 쿨다운 체크 (정규화된 term 기준)
                recent_candidate = db.query(DriverCandidate).filter(
                    and_(
                        DriverCandidate.candidate_text == normalized_term,
                        DriverCandidate.created_at >= datetime.utcnow() - timedelta(hours=24)
                    )
                ).first()
                
                if recent_candidate:
                    logger.debug(f"쿨다운 중인 후보 스킵: {normalized_term} (최근 등록: {recent_candidate.created_at})")
                    skipped_count += 1
                    skipped_cooldown_count += 1
                    continue
                
                # ⭐ 3. 리포트당 상한 재확인 (루프 내에서)
                if report_id and registered_count >= 10:
                    logger.warning(f"리포트당 후보 상한 도달 (루프 중단): {report_id}")
                    break
                
                # 이미 등록된 후보인지 확인 (정규화된 term 기준)
                existing = db.query(DriverCandidate).filter(
                    and_(
                        DriverCandidate.candidate_text == normalized_term,
                        DriverCandidate.status == 'PENDING'
                    )
                ).first()
                
                if existing:
                    # ⭐ P0-추가 4: unique 충돌 (정규화로 동일 term 처리)
                    skipped_unique_conflict_count += 1
                    # 발견 횟수 증가
                    existing.occurrence_count += 1
                    existing.last_seen_at = datetime.utcnow()
                    if report_metadata:
                        existing.source_report_id = report_metadata.get("report_id", existing.source_report_id)
                        existing.source_report_title = report_metadata.get("title", existing.source_report_title)
                    registered_count += 1
                else:
                    # 새 후보 등록 (정규화된 term 사용)
                    candidate = DriverCandidate(
                        candidate_text=normalized_term,  # ⭐ 정규화된 term 사용
                        suggested_driver_code=None,  # 나중에 LLM으로 제안 가능
                        confidence=0.0,
                        method="UNKNOWN_TERM_EXTRACTION",
                        status="PENDING",
                        source_report_id=report_id,
                        source_report_title=report_metadata.get("title") if report_metadata else None,
                        source_report_text=report_text[:1000] if report_text else None,  # 최대 1000자
                        context_sentence=_extract_context_sentence(normalized_term, report_text),
                        occurrence_count=1
                    )
                    db.add(candidate)
                    registered_count += 1
            
            db.commit()
            logger.info(
                f"드라이버 후보 등록 완료: {registered_count}개 등록, "
                f"스킵: {skipped_count}개 (쿨다운: {skipped_cooldown_count}, "
                f"unique 충돌: {skipped_unique_conflict_count})"
            )
            
            # ⭐ 메트릭 반환 (호출부에서 사용)
            return {
                "registered_count": registered_count,
                "skipped_count": skipped_count,
                "skipped_cooldown_count": skipped_cooldown_count,
                "skipped_unique_conflict_count": skipped_unique_conflict_count,
                "extraction_metrics": extraction_metrics or {}
            }
        except Exception as e:
            db.rollback()
            logger.error(f"드라이버 후보 등록 실패: {e}")
        finally:
            db.close()
    except ImportError:
        logger.warning("DriverCandidate 모델을 사용할 수 없습니다. 후보 등록을 건너뜁니다.")
    except Exception as e:
        logger.error(f"드라이버 후보 등록 중 오류: {e}")


def _extract_context_sentence(term: str, report_text: str, max_length: int = 200) -> str:
    """
    단어가 발견된 문장 추출
    
    Args:
        term: 찾을 단어
        report_text: 리포트 텍스트
        max_length: 최대 길이
    
    Returns:
        문장 텍스트
    """
    sentences = re.split(r'[.!?]\s+', report_text)
    for sentence in sentences:
        if term in sentence:
            return sentence.strip()[:max_length]
    return ""


if __name__ == "__main__":
    # 테스트 코드
    test_texts = [
        "정제마진 회복이 핵심",
        "크랙스프레드 개선 전망",
        "D램 가격 상승",
        "유가 급등",
        "금리 인상 우려"
    ]
    
    for text in test_texts:
        result = normalize_driver(text, use_llm=False)
        print(f"\n입력: {text}")
        print(f"결과: {result['driver_code']} (confidence: {result['confidence']:.2f}, method: {result['method']})")
        if result['all_candidates']:
            print(f"후보: {[c['driver_code'] for c in result['all_candidates'][:3]]}")

