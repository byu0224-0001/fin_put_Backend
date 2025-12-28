"""
Gemini Handler Service

Google Gemini 2.5 Pro API를 사용한 한국 금융 도메인 특화 인과 구조 분석
로컬 LLM (EXAONE/WON) 대체
"""
import logging
import json
import os
import time
from typing import Dict, Optional, List, Any
import json_repair
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

logger = logging.getLogger(__name__)

# 싱글톤 인스턴스
_gemini_handler_instance: Optional['GeminiHandler'] = None


class GeminiHandler:
    """
    Gemini 2.5 Pro API를 사용한 인과 구조 분석 핸들러
    
    한국 금융 도메인 특화 인과 구조 분석
    - 긴 컨텍스트 지원 (토큰 제한 거의 없음)
    - 요약/문장 분리 불필요
    - 전체 텍스트 직접 처리
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None  # None이면 환경 변수에서 가져옴 (기본: gemini-2.5-pro)
    ):
        """
        Args:
            api_key: Gemini API Key (None이면 환경 변수에서 가져옴)
            model_name: Gemini 모델 이름 (None이면 환경 변수에서 가져옴, 기본: gemini-2.5-pro)
        """
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY가 설정되지 않았습니다. 환경 변수 또는 인자로 제공해주세요.")
        
        # 모델명 처리 (models/ 접두사 제거 또는 추가)
        raw_model_name = model_name or os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
        # models/ 접두사가 없으면 추가
        if not raw_model_name.startswith("models/"):
            self.model_name = f"models/{raw_model_name}"
        else:
            self.model_name = raw_model_name
        
        # Gemini API 초기화
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(self.model_name)
        
        logger.info(f"✅ Gemini Handler 초기화 완료 (모델: {self.model_name})")
    
    def generate_causal_structure(
        self,
        company_detail: Any,  # CompanyDetail 객체
        major_sector: str,
        sub_sector: Optional[str],
        driver_signals: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Gemini로 인과 구조 생성
        
        Args:
            company_detail: CompanyDetail 객체
            major_sector: Major Sector 코드
            sub_sector: Sub-sector 코드
            driver_signals: Step 4A에서 추출한 드라이버 시그널 (선택적)
        
        Returns:
            {
                "upstream_impacts": [...],
                "downstream_impacts": [...],
                "key_drivers": [...],
                "granular_tags": [...],
                "cycle_reasoning": {...},
                "risk_factors": [...],
                "opportunity_factors": [...]
            }
        """
        # 프롬프트 구성
        prompt = self._build_reasoning_prompt(
            company_detail=company_detail,
            major_sector=major_sector,
            sub_sector=sub_sector,
            driver_signals=driver_signals
        )
        
        # Gemini 호출 (재시도 로직 포함)
        try:
            logger.info(f"📊 [Gemini] 프롬프트 길이: {len(prompt)}자")
            
            generation_start = time.time()
            
            # Gemini API 호출 (재시도 로직)
            max_retries = 3
            retry_delay = 60  # 초
            response = None
            
            for attempt in range(max_retries):
                try:
                    # Gemini API 호출
                    # Safety Settings 비활성화 (금융 분석 false positive 방지)
                    # JS ON Mode 강제 (response_mime_type) - 지원 여부 확인 필요
                    response = self.model.generate_content(
                        prompt,
                        generation_config={
                            "temperature": 0.0,
                            "max_output_tokens": 8192,  # 충분한 출력 토큰
                            # "response_mime_type": "application/json",  # JSON 모드 강제 (Gemini 2.5 Flash 지원 여부 확인 필요)
                        },
                        safety_settings={
                            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                        }
                    )
                    
                    generation_time = time.time() - generation_start
                    
                    if not response.text:
                        logger.error("❌ [Gemini] 응답이 비어있습니다.")
                        return self._get_empty_causal_structure()
                    
                    logger.info(f"✅ [Gemini] 텍스트 생성 완료 ({generation_time:.2f}초, {len(response.text)}자)")
                    break  # 성공 시 루프 종료
                    
                except Exception as api_error:
                    error_str = str(api_error)
                    
                    # 할당량 초과 오류 확인
                    if "Quota exceeded" in error_str or "quota" in error_str.lower():
                        if attempt < max_retries - 1:
                            # 재시도 지연 시간 추출 (가능한 경우)
                            if "retry_delay" in error_str:
                                import re
                                delay_match = re.search(r'seconds: (\d+)', error_str)
                                if delay_match:
                                    retry_delay = int(delay_match.group(1)) + 5  # 여유분 추가
                            
                            logger.warning(f"⚠️ [Gemini] 할당량 초과, {retry_delay}초 후 재시도 ({attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # 지수 백오프
                            continue
                        else:
                            logger.error(f"❌ [Gemini] 할당량 초과로 최대 재시도 횟수 초과")
                            raise
                    else:
                        # 할당량 오류가 아니면 즉시 재시도하지 않고 예외 전파
                        raise
            
            if response is None:
                logger.error("❌ [Gemini] API 호출 실패 (응답 없음)")
                return self._get_empty_causal_structure()
            
            generation_time = time.time() - generation_start
            
            # JSON 파싱
            parse_start = time.time()
            
            # 응답 텍스트 로깅 (디버깅용, 처음 500자만)
            response_preview = response.text[:500] if response.text else "None"
            logger.debug(f"📝 [Gemini] 응답 미리보기 (처음 500자):\n{response_preview}")
            
            causal_structure = self._parse_response(response.text)
            parse_time = time.time() - parse_start
            
            # 통계 로깅
            upstream_count = len(causal_structure.get('upstream_impacts', []))
            downstream_count = len(causal_structure.get('downstream_impacts', []))
            drivers_count = len(causal_structure.get('key_drivers', []))
            
            logger.info(f"✅ [Gemini] JSON 파싱 완료 ({parse_time:.2f}초)")
            logger.info(f"📊 [Gemini] 파싱 결과: 업스트림={upstream_count}, 다운스트림={downstream_count}, 드라이버={drivers_count}")
            
            # 빈 구조 경고
            if drivers_count == 0 and upstream_count == 0 and downstream_count == 0:
                logger.warning(f"⚠️ [Gemini] 인과 구조가 비어있습니다. 응답 확인 필요.")
                logger.debug(f"📝 [Gemini] 전체 응답:\n{response.text[:1000]}")
            
            return causal_structure
            
        except Exception as e:
            logger.error(f"[Gemini] 인과 구조 생성 실패: {e}", exc_info=True)
            return self._get_empty_causal_structure()
    
    def _build_reasoning_prompt(
        self,
        company_detail: Any,
        major_sector: str,
        sub_sector: Optional[str],
        driver_signals: Optional[Dict] = None
    ) -> str:
        """
        Gemini용 인과 구조 분석 프롬프트 구성 (MVP Production Ready)
        
        핵심 원칙:
        - LLM은 "판단자"가 아니라 "정제된 Fact를 연결해 설명하는 엔진"
        - Step 4A에서 이미 P/Q/C, 방향성, evidence, driver code 매칭 완료
        - MVP에서는 보수적 접근 (신규 driver는 CANDIDATE_로만)
        """
        # 텍스트 수집
        text_parts = []
        if company_detail.biz_summary:
            text_parts.append(f"## 사업 개요\n{company_detail.biz_summary}")
        if company_detail.products:
            products_text = "\n".join([str(p) for p in company_detail.products[:10]])
            if products_text:
                text_parts.append(f"## 주요 제품/서비스\n{products_text}")
        if company_detail.keywords:
            keywords_text = "\n".join([str(k) for k in company_detail.keywords[:10]])
            if keywords_text:
                text_parts.append(f"## 주요 키워드\n{keywords_text}")
        if company_detail.raw_materials:
            materials_text = "\n".join([str(rm) for rm in company_detail.raw_materials[:10]])
            if materials_text:
                text_parts.append(f"## 원자재\n{materials_text}")
        
        full_text = "\n\n".join(text_parts)
        
        # 드라이버 시그널 정보 구성 (Step 4A 결과 - 검증된 사실)
        driver_info = ""
        if driver_signals:
            price_signals = driver_signals.get('price_signals', [])
            quantity_signals = driver_signals.get('quantity_signals', [])
            cost_signals = driver_signals.get('cost_signals', [])
            
            if price_signals or quantity_signals or cost_signals:
                driver_info = "\n\n## 추출된 드라이버 시그널 (검증된 사실)\n"
                driver_info += "⚠️ 중요: 이 드라이버 시그널은 Step 4A에서 이미 검증된 사실(Fact)입니다.\n"
                driver_info += "- code, type, direction, evidence를 절대 변경하지 마세요.\n"
                driver_info += "- 이 드라이버들을 기반으로 인과관계를 설명하는 것이 당신의 역할입니다.\n"
                
                if price_signals:
                    driver_info += "\n### 가격(P) 드라이버:\n"
                    for signal in price_signals[:12]:
                        var = signal.get('var', '')
                        code = signal.get('code', '')
                        direction = signal.get('direction', '')
                        evidence = signal.get('evidence', [])
                        evidence_text = "\n  - ".join(evidence)
                        driver_info += f"- {var} ({code}): {direction}\n  - {evidence_text}\n"
                
                if quantity_signals:
                    driver_info += "\n### 수량(Q) 드라이버:\n"
                    for signal in quantity_signals[:12]:
                        var = signal.get('var', '')
                        code = signal.get('code', '')
                        direction = signal.get('direction', '')
                        evidence = signal.get('evidence', [])
                        evidence_text = "\n  - ".join(evidence)
                        driver_info += f"- {var} ({code}): {direction}\n  - {evidence_text}\n"
                
                if cost_signals:
                    driver_info += "\n### 원가(C) 드라이버:\n"
                    for signal in cost_signals[:12]:
                        var = signal.get('var', '')
                        code = signal.get('code', '')
                        direction = signal.get('direction', '')
                        evidence = signal.get('evidence', [])
                        evidence_text = "\n  - ".join(evidence)
                        driver_info += f"- {var} ({code}): {direction}\n  - {evidence_text}\n"
        
        # Hard Rules (출력 Contract - 최소한만)
        hard_rules = """
[Hard Rules - 출력 형식 규칙]

1. Sector 코드:
   - 반드시 SEC_XXX 형식만 사용 (예: SEC_SEMI, SEC_BATTERY, SEC_AUTO)
   - 시스템에서 제공된 sector allowlist 중에서만 선택
   - 괄호, 한글, 설명 추가 금지
   - 소문자나 하이픈 사용 금지
   - 새로운 섹터를 만들지 마세요

2. Driver code:
   - 제공된 driver_signals.code를 그대로 사용 (절대 변경 금지)
   - driver_signals에 없는 새로운 driver가 필요할 경우:
     * code는 반드시 "CANDIDATE_" 접두사로 시작 (예: CANDIDATE_AI_CHIP_DEMAND)
     * confidence 필수 (0.0~1.0)
     * confidence < 0.5일 경우 risk_factors에만 언급
     * MVP 단계에서는 정식 드라이버(ECONVAR_MASTER)만 사용 권장

3. schema_version:
   - 반드시 "v1.0" 포함

4. null 사용 금지:
   - 모든 필드는 배열([]) 또는 객체({}) 반환
   - 빈 값도 빈 배열/객체로 반환

5. 필수 필드:
   - key_drivers: 최소 1개 이상 필수
   - upstream_impacts, downstream_impacts, risk_factors는 없으면 빈 배열([]) 반환 가능

6. 배열 순서:
   - key_drivers: 중요도 높은 순서 (가장 핵심 드라이버 먼저)
   - upstream_impacts / downstream_impacts: confidence 내림차순

7. easy_explanation 길이 제한:
   - 3~5문장 이내로 제한
   - 불필요한 배경 설명, 역사 설명 금지
   - UI 카드에 바로 사용할 수 있는 간결한 설명
"""
        
        # Soft Guidance (업스트림/다운스트림 예시)
        soft_guidance = self._build_soft_guidance(major_sector)
        
        # Few-shot (최소한만)
        few_shot = self._build_few_shot_example(major_sector)
        
        # Safety Rule
        safety_rule = """
[Safety Rule - 투자 권유 금지]

⚠️ 절대 금지 사항:
- 매수/매도/목표가/수익률 예측 등 투자 행동 유도 표현 금지
- "투자하세요", "매수 추천", "목표가 XX원" 등 표현 금지
- sentiment_label은 "펀더멘털 드라이버 톤"만 판단 (긍정적/중립/부정적)
  → 투자 판단이 아닌 현상 톤으로만 제한
"""
        
        # Task (LLM 역할)
        task = """
[Task - 당신의 역할]

제공된 driver_signals는 이미 검증된 사실(Fact)입니다.
당신의 역할은:

1. 인과 연결: 드라이버들이 기업에 미치는 인과관계를 설명
2. 설명: 각 드라이버의 의미와 영향력을 전문가 관점에서 설명
3. 요약: 전체 인과 구조를 한 문장으로 요약 (summary_sentence)
4. 쉬운 설명: 전체 인과 구조를 초보자도 이해할 수 있게 풀어서 설명 (easy_explanation, 3~5문장)
5. 리스크/기회: 위험 요인과 기회 요인 정리
6. sentiment_label: key_drivers의 방향성과 risk_factors/opportunity_factors의 상대적 비중을 종합해 판단
7. granular_tags: 회사 설명이나 드라이버에서 제품/기술/채널 특성이 드러난다면 자연스럽게 키워드로 언급 (선택적, 시스템이 후처리로 정확한 L3 태그 부여)
"""
        
        # JSON 스키마 (easy_explanation을 top-level로 이동)
        json_schema = """
{
  "schema_version": "v1.0",
  "summary_sentence": "이 기업의 핵심 인과 구조를 한 문장으로 요약",
  "easy_explanation": "전체 인과 구조를 초보자도 이해할 수 있게 풀어서 설명 (3~5문장, 여러 드라이버를 엮은 하나의 서술)",
  "upstream_impacts": [
    {
      "sector": "SEC_XXX",
      "description": "업스트림 영향 설명",
      "impact_type": "positive/negative/neutral",
      "confidence": 0.0-1.0
    }
  ],
  "downstream_impacts": [
    {
      "sector": "SEC_XXX",
      "description": "다운스트림 영향 설명",
      "impact_type": "positive/negative/neutral",
      "confidence": 0.0-1.0
    }
  ],
  "key_drivers": [
    {
      "var": "드라이버 이름",
      "code": "드라이버 코드 (driver_signals.code 그대로 사용 또는 CANDIDATE_XXX)",
      "type": "P/Q/C (driver_signals.type 그대로 사용)",
      "direction": "증가/감소 (driver_signals.direction 그대로 사용)",
      "description": "드라이버 설명 (인과관계 중심, 전문가 관점)",
      "evidence": ["증거 문장1", "증거 문장2"]
    }
  ],
  "granular_tags": ["태그1", "태그2"]  // 선택적: 회사 설명이나 드라이버에서 제품/기술/채널 특성이 드러난다면 자연스럽게 키워드로 언급 (시스템이 후처리로 정확한 L3 태그 부여)
  "cycle_reasoning": {
    "cycle_type": "expansion/recession/recovery/unknown",
    "reasoning": "사이클 판단 근거"
  },
  "risk_factors": ["리스크1", "리스크2"],
  "opportunity_factors": ["기회1", "기회2"],
  "sentiment_label": "긍정적/중립/부정적"
}
"""
        
        prompt = f"""당신은 한국 금융 도메인 전문가입니다. 기업의 인과 구조를 정확하게 분석합니다.

## 분석 대상
- 섹터: {major_sector}
- 서브섹터: {sub_sector or 'N/A'}

## 기업 정보
{full_text}
{driver_info}

{hard_rules}

{soft_guidance}

{few_shot}

{safety_rule}

{task}

## 출력 형식
다음 JSON 형식으로 인과 구조를 분석해주세요. 반드시 유효한 JSON만 반환하세요 (마크다운 코드 블록, 설명, 주석 없이 순수 JSON만).

{json_schema}

## 최종 확인 사항
1. schema_version: "v1.0" 포함 확인
2. summary_sentence: 한 문장으로 핵심 요약
3. easy_explanation: 전체 인과 구조를 초보자도 이해할 수 있게 풀어서 설명 (3~5문장, 하나의 서술)
4. key_drivers: 최소 1개 이상 필수
   - 각 드라이버에 driver_tags가 부여될 수 있습니다 (시스템이 자동 부여)
   - driver_tags가 없는 드라이버는 해석 시 주의가 필요합니다
   - driver_tags가 없으면 "구조적 영향 판단 보류" 또는 "방향성 해석 불확실" 표현 사용
5. driver_signals의 code/type/direction/evidence 절대 변경 금지
6. 섹터 코드: SEC_XXX 형식만 사용, allowlist에서만 선택
7. 신규 driver: CANDIDATE_ 접두사 필수
8. null 사용 금지, 빈 배열([]) 사용
9. 투자 권유 표현 절대 금지
10. sentiment_label: key_drivers 방향성 + risk/opportunity 상대적 비중 종합 판단

## Driver Tags 기반 해석 가이드 (Effective Direction)

⚠️ 중요: Driver의 direction(증가/감소)만으로 영향을 판단하지 마세요.
Driver Tags를 반드시 참고하여 "이 기업에 대한 실제 영향"을 판단하세요.

### 예시
- 환율 상승 + IMPORT_DEPENDENT → 악재 (원가 상승)
- 환율 상승 + EXPORT_DRIVEN → 호재 (매출 증가)
- 환율 상승 + Driver Tags 없음 → "영향 불확실" 표현

### 규칙
1. Driver Tags가 있으면: Tags 기반으로 영향 방향 결정
2. Driver Tags가 없으면: 강한 인과 주장 금지, "가능성" 표현 사용
3. 상반 Tags(IMPORT_DEPENDENT vs EXPORT_DRIVEN)가 모두 있으면: 복합 영향 설명

## Driver Tags Confidence 기반 표현 가이드
- confidence >= 0.8: "주요 원인", "강한 영향", "확실히" 등 확신 표현 가능
- confidence 0.6~0.8: "영향 가능성", "일정 부분 영향" 등 중립 표현
- confidence < 0.6: "영향 제한적", "가능성 있음", "불확실" 등 약화 표현
- driver_tags 없음: "구조적 영향 판단 보류", "방향성 해석 불확실" 등 보수적 표현
"""
        
        return prompt
    
    def _build_soft_guidance(self, major_sector: str) -> str:
        """업스트림/다운스트림 Soft Guidance (예시만 제공)"""
        # 섹터별 예시 관계 (참고용)
        sector_examples = {
            "SEC_SEMI": {
                "upstream": ["SEC_CHEM (화학소재)", "SEC_MACH (반도체 장비)"],
                "downstream": ["SEC_IT (AI 서버)", "SEC_CONSUMER (스마트폰)"]
            },
            "SEC_BATTERY": {
                "upstream": ["SEC_MINING (리튬 채굴)", "SEC_CHEM (양극재)"],
                "downstream": ["SEC_AUTO (전기차)", "SEC_UTIL (ESS)"]
            },
            "SEC_AUTO": {
                "upstream": ["SEC_BATTERY (배터리)", "SEC_STEEL (강판)"],
                "downstream": ["SEC_RETAIL (자동차 판매)", "SEC_IT (자율주행)"]
            }
        }
        
        example = sector_examples.get(major_sector, sector_examples.get("SEC_SEMI"))
        
        guidance = f"""
[업스트림/다운스트림 분석 가이드]

예시 관계 (참고용, 반드시 이 중에서만 고를 필요 없음):
- {major_sector} 업스트림 예시: {', '.join(example['upstream'])}
- {major_sector} 다운스트림 예시: {', '.join(example['downstream'])}

⚠️ 중요: 
- 이 기업에 실제로 relevant한 관계만 선택하세요
- 예시는 참고용이며, 다른 섹터도 가능합니다
- 시스템에서 제공된 sector allowlist 중에서만 선택하세요
"""
        
        return guidance
    
    def _build_few_shot_example(self, major_sector: str) -> str:
        """Few-shot 예시 (핵심 섹터 2-3개, 각 항목 1개씩)"""
        examples = {
            "SEC_SEMI": {
                "input": {
                    "sector": "SEC_SEMI",
                    "sub_sector": "MEMORY",
                    "driver": {"code": "DRAM_ASP", "type": "P", "direction": "증가"}
                },
                "output": {
                    "schema_version": "v1.0",
                    "summary_sentence": "DRAM 가격 상승으로 인해 메모리 반도체 기업의 매출과 수익성이 개선되는 구조",
                    "easy_explanation": "이 회사는 메모리 반도체를 만드는 기업입니다. 메모리 가격이 오르면 매출이 늘어나고, 그 결과 수익성이 좋아지는 구조입니다. 특히 AI 서버 수요가 늘어나면서 고용량 메모리(HBM)에 대한 수요가 증가하고 있어 긍정적인 영향을 받고 있습니다.",
                    "upstream_impacts": [
                        {
                            "sector": "SEC_CHEM",
                            "description": "화학소재(실리콘 웨이퍼, 포토레지스트) 공급자",
                            "impact_type": "positive",
                            "confidence": 0.8
                        }
                    ],
                    "downstream_impacts": [
                        {
                            "sector": "SEC_IT",
                            "description": "AI 서버 제조사 (HBM 수요 증가)",
                            "impact_type": "positive",
                            "confidence": 0.9
                        }
                    ],
                    "key_drivers": [
                        {
                            "var": "DRAM ASP",
                            "code": "DRAM_ASP",
                            "type": "P",
                            "direction": "증가",
                            "description": "메모리 가격 상승이 매출에 직접적인 긍정적 영향",
                            "evidence": ["DRAM 가격이 상승하고 있다"]
                        }
                    ],
                    "sentiment_label": "긍정적"
                }
            },
            "SEC_BATTERY": {
                "input": {
                    "sector": "SEC_BATTERY",
                    "sub_sector": "CELL",
                    "driver": {"code": "LITHIUM_PRICE", "type": "C", "direction": "증가"}
                },
                "output": {
                    "schema_version": "v1.0",
                    "summary_sentence": "리튬 가격 상승으로 원가 부담이 증가하지만, 전기차 수요 확대로 수량 증가 효과가 상쇄하는 구조",
                    "easy_explanation": "이 회사는 배터리를 만드는 기업입니다. 리튬 가격이 오르면 배터리 만드는 비용이 늘어나지만, 전기차가 많이 팔리면서 배터리 수요가 크게 늘어나고 있어 전체적으로는 긍정적인 영향을 받고 있습니다.",
                    "key_drivers": [
                        {
                            "var": "리튬 가격",
                            "code": "LITHIUM_PRICE",
                            "type": "C",
                            "direction": "증가",
                            "description": "리튬 가격 상승이 원가에 부정적 영향",
                            "evidence": ["리튬 가격이 상승 추세"]
                        }
                    ],
                    "sentiment_label": "중립"
                }
            }
        }
        
        # 현재 섹터에 맞는 예시 선택
        example = examples.get(major_sector)
        if not example:
            # 기본 예시 (SEC_SEMI)
            example = examples.get("SEC_SEMI")
        
        import json
        few_shot = f"""
[예시 (Few-shot Learning)]

입력:
- 섹터: {example['input']['sector']}
- 드라이버: {example['input']['driver']['code']} ({example['input']['driver']['type']}, {example['input']['driver']['direction']})

출력 (정확한 형식):
{json.dumps(example['output'], ensure_ascii=False, indent=2)}

⚠️ 중요: 위 예시와 동일한 JSON 구조를 정확히 따르세요.
특히 easy_explanation은 여러 드라이버를 엮은 하나의 서술로 작성하세요 (3~5문장).
"""
        
        return few_shot
    
    def _parse_response(self, response: str) -> Dict[str, Any]:
        """Gemini 응답 파싱 (json_repair 포함 + 검증 강화)"""
        try:
            # JSON 코드 블록 제거
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            elif response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            # JSON 파싱 시도
            try:
                causal_structure = json.loads(response)
            except json.JSONDecodeError:
                # json_repair로 수리 시도
                try:
                    repaired = json_repair.repair_json(response)
                    causal_structure = json.loads(repaired)
                    logger.info("✅ [Gemini] json_repair로 JSON 수리 성공")
                except Exception as repair_error:
                    logger.warning(f"[Gemini] json_repair 실패, 직접 파싱 시도: {repair_error}")
                    # 직접 파싱 시도 (부분적)
                    causal_structure = self._parse_partial_json(response)
            
            # 필수 필드 검증 및 기본값 설정
            if not isinstance(causal_structure, dict):
                raise ValueError("응답이 딕셔너리가 아닙니다.")
            
            # Cycle reasoning 기본값 보장
            cycle_reasoning = causal_structure.get("cycle_reasoning", {})
            if not isinstance(cycle_reasoning, dict) or not cycle_reasoning:
                cycle_reasoning = {
                    "cycle_type": "unknown",
                    "reasoning": ""
                }
            
            # 기본 구조 보장 (새 필드 포함)
            result = {
                "schema_version": causal_structure.get("schema_version", "v1.0"),
                "summary_sentence": causal_structure.get("summary_sentence", ""),
                "easy_explanation": causal_structure.get("easy_explanation", ""),
                "upstream_impacts": causal_structure.get("upstream_impacts", []),
                "downstream_impacts": causal_structure.get("downstream_impacts", []),
                "key_drivers": causal_structure.get("key_drivers", []),
                "granular_tags": causal_structure.get("granular_tags", []),
                "cycle_reasoning": cycle_reasoning,
                "risk_factors": causal_structure.get("risk_factors", []),
                "opportunity_factors": causal_structure.get("opportunity_factors", []),
                "sentiment_label": causal_structure.get("sentiment_label", "중립")
            }
            
            # 검증: key_drivers 최소 1개
            if len(result["key_drivers"]) == 0:
                logger.warning("⚠️ [Gemini] key_drivers가 비어있습니다. 최소 1개 이상 필요합니다.")
            
            # 검증: easy_explanation 길이 (3~5문장 권장)
            easy_explanation = result.get("easy_explanation", "")
            if easy_explanation:
                sentences = easy_explanation.split('。')  # 한국어 문장 구분자
                if len(sentences) < 2:
                    sentences = easy_explanation.split('.')
                if len(sentences) > 6:
                    logger.warning(f"⚠️ [Gemini] easy_explanation이 너무 깁니다 ({len(sentences)}문장). 3~5문장 권장.")
            
            # 검증: Sector allowlist (위반 시 drop)
            from app.models.sector_reference import get_allowed_sectors_for_validation
            allowed_sectors = get_allowed_sectors_for_validation()
            
            # upstream_impacts 검증 및 필터링
            valid_upstream = []
            for impact in result["upstream_impacts"]:
                sector = impact.get("sector")
                if sector and sector in allowed_sectors:
                    valid_upstream.append(impact)
                elif sector:
                    logger.warning(f"⚠️ [Gemini] 허용되지 않은 섹터 코드 제거: {sector} (upstream_impacts)")
            result["upstream_impacts"] = valid_upstream
            
            # downstream_impacts 검증 및 필터링
            valid_downstream = []
            for impact in result["downstream_impacts"]:
                sector = impact.get("sector")
                if sector and sector in allowed_sectors:
                    valid_downstream.append(impact)
                elif sector:
                    logger.warning(f"⚠️ [Gemini] 허용되지 않은 섹터 코드 제거: {sector} (downstream_impacts)")
            result["downstream_impacts"] = valid_downstream
            
            # 검증: CANDIDATE_ Driver → Backend Flag 처리
            for driver in result["key_drivers"]:
                code = driver.get("code", "")
                if code.startswith("CANDIDATE_"):
                    driver["status"] = "candidate"
                    driver["use_for_kpi"] = False
                    logger.info(f"ℹ️ [Gemini] CANDIDATE 드라이버 발견: {code} (KPI/KG 제외)")
            
            logger.info("✅ [Gemini] 응답 파싱 성공")
            return result
            
        except Exception as e:
            logger.error(f"[Gemini] 응답 파싱 실패: {e}")
            logger.debug(f"원본 응답:\n{response[:500]}")
            return self._get_empty_causal_structure()
    
    def _parse_partial_json(self, response: str) -> Dict[str, Any]:
        """부분적 JSON 파싱 (최후의 수단)"""
        # 기본 구조 반환
        return self._get_empty_causal_structure()
    
    def _get_empty_causal_structure(self) -> Dict[str, Any]:
        """빈 인과 구조 반환 (새 필드 포함)"""
        return {
            "schema_version": "v1.0",
            "summary_sentence": "",
            "easy_explanation": "",
            "upstream_impacts": [],
            "downstream_impacts": [],
            "key_drivers": [],
            "granular_tags": [],
            "cycle_reasoning": {
                "cycle_type": "unknown",
                "reasoning": ""
            },
            "risk_factors": [],
            "opportunity_factors": [],
            "sentiment_label": "중립"
        }


def get_gemini_handler(
    api_key: Optional[str] = None,
    model_name: Optional[str] = None
) -> GeminiHandler:
    """
    Gemini Handler 싱글톤 인스턴스 반환
    
    Args:
        api_key: Gemini API Key (None이면 환경 변수에서 가져옴)
        model_name: Gemini 모델 이름
    
    Returns:
        GeminiHandler 인스턴스
    """
    global _gemini_handler_instance
    
    if _gemini_handler_instance is None:
        _gemini_handler_instance = GeminiHandler(api_key=api_key, model_name=model_name)
        logger.info("✅ Gemini Handler 싱글톤 생성 완료")
    elif _gemini_handler_instance.model_name != model_name:
        logger.info(f"모델 변경: {_gemini_handler_instance.model_name} → {model_name}")
        _gemini_handler_instance = GeminiHandler(api_key=api_key, model_name=model_name)
    
    return _gemini_handler_instance

