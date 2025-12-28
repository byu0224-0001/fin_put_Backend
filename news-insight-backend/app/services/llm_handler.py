"""
LLM Handler Service

LLM 호출 관리, JSON 검증 및 수리, Fallback 모델 지원
"""
import json
import logging
import os
import re
from typing import Dict, Optional, Any, Tuple
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
import json_repair
from app.services.retry_handler import retry_llm_api, llm_rate_limiter

logger = logging.getLogger(__name__)


class LLMHandler:
    """LLM 호출 및 응답 처리 핸들러"""
    
    def __init__(
        self,
        analysis_model: str = "gpt-5-mini",
        summary_model: str = "gpt-5-nano",
        api_key: Optional[str] = None,
        temperature: float = 0.0
    ):
        """
        Args:
            analysis_model: 분석용 LLM 모델명
            summary_model: 요약용 LLM 모델명
            api_key: OpenAI API Key (callable이면 문자열로 변환)
            temperature: Temperature 설정 (사용자 입력 그대로 전달)
        """
        self.analysis_model = analysis_model
        self.summary_model = summary_model
        
        # API 키를 문자열로 강제 변환 (async callable 오류 방지)
        key = api_key if api_key is not None else os.getenv("OPENAI_API_KEY", "")
        try:
            if callable(key):
                key = key()
        except Exception as key_error:
            logger.warning(f"API key callable 호출 실패: {key_error}")
            key = os.getenv("OPENAI_API_KEY", "")
        key = str(key) if key is not None else ""
        self.api_key = key
        
        # 분석용 LLM
        self.llm = ChatOpenAI(
            model=analysis_model,
            openai_api_key=self.api_key,
            temperature=temperature
        )
        
        # 요약용 LLM
        self.summary_llm = ChatOpenAI(
            model=summary_model,
            openai_api_key=self.api_key,
            temperature=temperature
        )
    
    @retry_llm_api
    @llm_rate_limiter
    def summarize(
        self,
        text: str,
        max_length: Optional[int] = None,
        target_length: Optional[int] = None
    ) -> str:
        """
        긴 텍스트를 요약 (gpt-5-nano 사용)
        
        Args:
            text: 요약할 텍스트
            max_length: 최대 길이 (자동 계산 시 None)
            target_length: 목표 길이
        
        Returns:
            요약된 텍스트
        """
        if not text or not text.strip():
            return ""
        
        # 동적 길이 조정
        if max_length is None:
            text_length = len(text)
            if text_length > 50000:
                max_length = 15000
            elif text_length > 30000:
                max_length = 12000
            else:
                max_length = 10000
        
        if target_length is None:
            target_length = max_length
        
        logger.info(f"텍스트 요약 중... (원본: {len(text)}자 → 목표: {target_length}자)")
        
        prompt = f"""
        다음 사업보고서 내용을 {target_length}자 이내로 핵심만 요약해줘.
        
        [포함할 핵심 정보]
        - 사업 내용 및 주요 제품/서비스
        - 주요 고객사/매출처
        - 핵심 원재료
        - 비용 구조
        - 경영 전략 및 전망
        
        [제외할 내용]
        - 면책 조항
        - 예측 정보 주의사항
        - 법규상 규제 사항
        - 반복되는 표나 숫자 나열
        
        원문:
        {text[:50000]}  # 최대 50,000자까지만
        """
        
        messages = [
            SystemMessage(content="You are a financial analyst. Summarize the business report concisely, focusing on key business information."),
            HumanMessage(content=prompt)
        ]
        
        try:
            response = self.summary_llm.invoke(messages)
            summarized = response.content.strip()
            logger.info(f"요약 완료: {len(summarized)}자")
            return summarized
        except Exception as e:
            logger.error(f"요약 실패: {e}")
            # Fallback: 원문 반환 (길이 제한)
            return text[:max_length]
    
    def is_financial_company(
        self,
        company_name: Optional[str] = None,
        ticker: Optional[str] = None,
        business_summary: Optional[str] = None,
        keywords: Optional[list] = None
    ) -> bool:
        """
        금융사 여부 판단 (financial_company_detector 사용)
        
        🆕 P1-4: 금융사 감지 일원화 - financial_company_detector 사용
        
        Args:
            ticker: 종목코드
            business_summary: 사업 요약
            company_name: 회사명
            keywords: 키워드 리스트
        
        Returns:
            금융사 여부 (True/False)
        """
        from app.services.financial_company_detector import detect_financial_company
        
        is_financial, _, _ = detect_financial_company(
            ticker=ticker,
            company_name=company_name,
            business_summary=business_summary,
            keywords=keywords
        )
        
        return is_financial
    
    def detect_single_segment(self, text: str) -> Tuple[bool, Optional[str]]:
        """
        P0-2: 단일 보고부문 감지
        
        Returns:
            (is_single_segment, reason)
        """
        single_segment_keywords = [
            '하나의 보고 부문', '단일 보고부문', '하나의 보고부문',
            '단일 부문', '하나의 부문', '단일보고부문',
            '연결실체는 하나의 보고 부문', '하나의 보고 부문으로 구성'
        ]
        
        text_lower = text.lower()
        for keyword in single_segment_keywords:
            if keyword in text_lower:
                return True, f"REPORT_STATES_SINGLE_SEGMENT:{keyword}"
        
        return False, None
    
    def handle_single_segment(
        self,
        text: str,
        company_name: Optional[str],
        ticker: Optional[str]
    ) -> Optional[Dict[str, float]]:
        """
        P0-2: 단일 부문 처리
        
        Returns:
            {"단일부문": 100.0} 또는 조건 충족 시 {"완성차(자동차 제조)": 100.0}
        """
        is_single, reason = self.detect_single_segment(text)
        if not is_single:
            return None
        
        # 조건: 회사명+텍스트에 '자동차 제조/완성차' 명시
        auto_keywords = ['완성차', '자동차 제조', '승용', 'RV', '자동차 판매', '자동차']
        text_lower = text.lower()
        company_lower = (company_name or "").lower()
        
        has_auto_context = any(kw in text_lower or kw in company_lower for kw in auto_keywords)
        
        if has_auto_context and (company_name and ('기아' in company_name or ticker == '000270')):
            logger.info(f"[{ticker or 'N/A'}] 단일 부문 감지 + 자동차 컨텍스트 → 완성차 라벨링")
            return {"완성차(자동차 제조)": 100.0}
        
        logger.info(f"[{ticker or 'N/A'}] 단일 부문 감지 → 단일부문 라벨링")
        return {"단일부문": 100.0}
    
    def extract_text_percentages(self, text: str) -> Optional[Dict[str, float]]:
        """
        P0-3: 텍스트에서 "OO사업 67%" 패턴 추출 (오탐 방지 강화)
        
        Returns:
            {"석유사업": 67.0, "화학사업": 14.0, ...} 또는 None
        """
        # 🆕 P0-B: 컨텍스트 키워드 (매출 비중 관련 키워드만 허용)
        context_keywords = ['매출', '비중', '차지', '구성', '사업', '부문', '수익', '영업', '매출액', '매출비중']
        
        # 패턴 1: "OO사업/부문 67%" 또는 "OO 67%"
        pattern1 = r'([가-힣A-Za-z0-9/&\-\s]+?)\s*(사업|부문)?\s*(이|가)?\s*(\d{1,2}(?:\.\d+)?)\s*%'
        # 패턴 2: "67%는 OO사업"
        pattern2 = r'(\d{1,2}(?:\.\d+)?)\s*%[는은이가]?\s*([가-힣A-Za-z0-9/&\-\s]+?)(?:사업|부문)?'
        
        results = {}
        
        # 패턴 1 적용
        for match in re.finditer(pattern1, text):
            groups = match.groups()
            if len(groups) >= 4:
                segment = groups[0].strip()
                pct_str = groups[3]
                match_start = match.start()
                match_end = match.end()
                
                # 🆕 P0-B: 주변 20자 이내에 컨텍스트 키워드 확인
                context_start = max(0, match_start - 20)
                context_end = min(len(text), match_end + 20)
                context_text = text[context_start:context_end].lower()
                
                has_context = any(kw in context_text for kw in context_keywords)
                if not has_context:
                    continue  # 컨텍스트 없으면 스킵 (오탐 방지)
                
                try:
                    pct = float(pct_str)
                    # 세그먼트명 정리
                    segment = re.sub(r'\s*(사업|부문)\s*$', '', segment)
                    if segment and 0 < pct <= 100:
                        # 중복 제거 (같은 세그먼트면 더 큰 값 사용)
                        if segment not in results or results[segment] < pct:
                            results[segment] = pct
                except (ValueError, TypeError):
                    continue
        
        # 패턴 2 적용
        for match in re.finditer(pattern2, text):
            groups = match.groups()
            if len(groups) >= 2:
                pct_str = groups[0]
                segment = groups[1].strip()
                match_start = match.start()
                match_end = match.end()
                
                # 🆕 P0-B: 주변 20자 이내에 컨텍스트 키워드 확인
                context_start = max(0, match_start - 20)
                context_end = min(len(text), match_end + 20)
                context_text = text[context_start:context_end].lower()
                
                has_context = any(kw in context_text for kw in context_keywords)
                if not has_context:
                    continue  # 컨텍스트 없으면 스킵 (오탐 방지)
                
                try:
                    pct = float(pct_str)
                    # 세그먼트명 정리
                    segment = re.sub(r'\s*(사업|부문)\s*$', '', segment)
                    if segment and 0 < pct <= 100:
                        if segment not in results or results[segment] < pct:
                            results[segment] = pct
                except (ValueError, TypeError):
                    continue
        
        if len(results) >= 2:
            total = sum(results.values())
            # 합계가 70-130% 범위면 유효
            if 70.0 <= total <= 130.0:
                logger.info(f"텍스트 기반 % 추출 성공: {len(results)}개 세그먼트, 총 {total:.1f}%")
                return results
            else:
                logger.debug(f"텍스트 기반 % 추출: 합계 범위 초과 ({total:.1f}%, 범위: 70-130%)")
        else:
            logger.debug(f"텍스트 기반 % 추출: 세그먼트 수 부족 ({len(results)}개, 최소 2개 필요)")
        
        return None
    
    @retry_llm_api
    @llm_rate_limiter
    def extract_structured_data(
        self,
        text: str,
        ticker: Optional[str] = None,
        company_name: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        텍스트에서 구조화된 데이터 추출 (gpt-5-mini 사용)
        
        Args:
            text: 분석할 텍스트
            schema: JSON 스키마 (선택사항)
        
        Returns:
            추출된 데이터 (Dict) 또는 None
        """
        if not text or not text.strip():
            return None
        
        # 금융사 여부 판단
        is_financial = self.is_financial_company(
            ticker=ticker,
            business_summary=text[:500] if text else None,  # 처음 500자만 확인
            company_name=company_name
        )
        
        if is_financial:
            logger.info("금융사로 감지됨 - 금융사 전용 프롬프트 사용")
            prompt = self._get_financial_prompt()
        else:
            logger.info("일반 기업으로 감지됨 - 제조업 프롬프트 사용")
            prompt = self._get_manufacturing_prompt()
        
        logger.info("AI 분석 및 JSON 구조화 중...")
        
        messages = [
            SystemMessage(content="You are a precise financial analyst. Extract only factual information from the provided text. Do not hallucinate or make assumptions. Output JSON only."),
            HumanMessage(content=prompt + "\n\n[사업보고서 내용]\n" + text[:50000])  # 최대 50,000자
        ]
        
        try:
            response = self.llm.invoke(messages)
            content = response.content.replace("```json", "").replace("```", "").strip()
            
            # JSON 수리 시도
            try:
                parsed_data = json.loads(content)
            except json.JSONDecodeError:
                logger.warning("JSON 파싱 실패, json_repair로 수리 시도 중...")
                try:
                    repaired_json = json_repair.repair_json(content)
                    parsed_data = json.loads(repaired_json)
                    logger.info("JSON 수리 성공")
                except Exception as repair_error:
                    logger.error(f"JSON 수리 실패: {repair_error}")
                    logger.debug(f"원본 응답: {content[:500]}")
                    return None
            
            # ✅ 타입 검증 강화: parsed_data가 딕셔너리가 아니면 오류
            if not isinstance(parsed_data, dict):
                logger.error(f"파싱된 데이터가 딕셔너리가 아닙니다. 타입: {type(parsed_data)}, 값: {str(parsed_data)[:200]}")
                return None
            
            # 하위 호환성: supply_chain이 없으면 raw_materials에서 생성 시도
            if 'supply_chain' not in parsed_data or not parsed_data.get('supply_chain'):
                if 'raw_materials' in parsed_data and parsed_data['raw_materials']:
                    # raw_materials를 supply_chain 형식으로 변환 (공급사 정보 없음)
                    parsed_data['supply_chain'] = [
                        {"item": item, "supplier": "정보없음"} 
                        for item in parsed_data['raw_materials']
                    ]
            
            # risk_factors 필드 제거 (토큰 절약)
            if 'risk_factors' in parsed_data:
                del parsed_data['risk_factors']
            
            # ✅ Pydantic 모델을 사용한 데이터 검증 및 타입 변환
            try:
                from app.models.llm_output import LLMOutputModel
                
                # Pydantic 모델로 검증 및 타입 변환
                validated_data = LLMOutputModel(**parsed_data)
                logger.info("Pydantic 검증 완료 - 숫자 필드 변환 및 리스트 필드 검증 완료")
                
                # 딕셔너리로 변환하여 반환
                parsed_data = validated_data.to_dict()
                
            except Exception as validation_error:
                logger.warning(f"Pydantic 검증 실패, 기본 검증으로 대체: {validation_error}")
                # Pydantic 검증 실패 시 기존 로직 사용
                is_financial = 'financial_value_chain' in parsed_data and parsed_data.get('financial_value_chain')
                
                if is_financial:
                    parsed_data['supply_chain'] = []
                
                # 필수 필드 기본값 설정
                required_fields = {
                    'business_summary': "정보없음",
                    'major_products': [],
                    'major_clients': "정보없음",
                    'supply_chain': [],
                    'capax_investment': "정보없음",
                    'cost_structure': "정보없음",
                    'keywords': []
                }
                
                for field, default_value in required_fields.items():
                    if field not in parsed_data:
                        logger.warning(f"필수 필드 누락: {field}, 기본값으로 채움")
                        parsed_data[field] = default_value
            
            # ✅ 최종 반환 전 타입 재확인 (안전장치)
            if not isinstance(parsed_data, dict):
                logger.error(f"최종 검증 실패: parsed_data가 딕셔너리가 아닙니다. 타입: {type(parsed_data)}")
                return None
            
            logger.info("구조화된 데이터 추출 완료")
            
            # 🆕 P0-2/P0-3: revenue_by_segment가 없으면 Fallback 로직 시도
            if parsed_data and not parsed_data.get('revenue_by_segment'):
                logger.warning(f"[{ticker or 'N/A'}] LLM에서 revenue_by_segment 추출 실패, Fallback 로직 시도")
                
                # Fallback 1: 단일 부문 감지
                single_segment_data = self.handle_single_segment(text, company_name, ticker)
                if single_segment_data:
                    logger.info(f"[{ticker or 'N/A'}] 단일 부문 처리 성공: {single_segment_data}")
                    parsed_data['revenue_by_segment'] = single_segment_data
                    return parsed_data
                else:
                    logger.debug(f"[{ticker or 'N/A'}] 단일 부문 감지 실패")
                
                # Fallback 2: 텍스트 기반 % 추출
                text_percentages = self.extract_text_percentages(text)
                if text_percentages:
                    logger.info(f"[{ticker or 'N/A'}] 텍스트 기반 % 추출 성공: {text_percentages}")
                    parsed_data['revenue_by_segment'] = text_percentages
                    return parsed_data
                else:
                    logger.debug(f"[{ticker or 'N/A'}] 텍스트 기반 % 추출 실패 (패턴 미매칭 또는 컨텍스트 키워드 부족)")
                
                # Fallback 3: 금융사 프롬프트 사용했는데 revenue_by_segment가 없으면 제조업 프롬프트로 재시도
                if is_financial:
                    logger.warning(f"[{ticker or 'N/A'}] 금융사 프롬프트에서 revenue_by_segment 추출 실패, 제조업 프롬프트로 재시도")
                    prompt = self._get_manufacturing_prompt()
                    messages = [
                        SystemMessage(content="You are a precise financial analyst. Extract only factual information from the provided text. Do not hallucinate or make assumptions. Output JSON only."),
                        HumanMessage(content=prompt + "\n\n[사업보고서 내용]\n" + text[:50000])
                    ]
                    try:
                        response = self.llm.invoke(messages)
                        content = response.content.replace("```json", "").replace("```", "").strip()
                        
                        try:
                            retry_parsed_data = json.loads(content)
                        except json.JSONDecodeError:
                            try:
                                repaired_json = json_repair.repair_json(content)
                                retry_parsed_data = json.loads(repaired_json)
                            except Exception:
                                retry_parsed_data = None
                        
                        if retry_parsed_data and isinstance(retry_parsed_data, dict) and retry_parsed_data.get('revenue_by_segment'):
                            logger.info(f"[{ticker or 'N/A'}] 제조업 프롬프트 재시도 성공: revenue_by_segment 추출됨")
                            parsed_data['revenue_by_segment'] = retry_parsed_data.get('revenue_by_segment')
                    except Exception as retry_error:
                        logger.warning(f"[{ticker or 'N/A'}] 제조업 프롬프트 재시도 실패: {retry_error}")
            
            return parsed_data  # ✅ 반드시 딕셔너리만 반환
            
        except Exception as e:
            error_str = str(e).lower()
            error_msg = str(e)
            
            # OpenAI API quota 오류 감지
            if any(keyword in error_str for keyword in [
                "insufficient_quota", "quota", "rate limit", 
                "exceeded your current quota", "billing"
            ]):
                logger.error(f"⚠️ OpenAI API Quota 초과: {error_msg}")
                # quota 오류는 특별히 표시하기 위해 예외를 다시 발생
                raise ValueError(f"QUOTA_ERROR: {error_msg}")
            
            logger.error(f"LLM 응답 처리 실패: {e}")
            import traceback
            logger.debug(f"오류 상세: {traceback.format_exc()}")
            return None  # ✅ 예외 발생 시 None 반환 (문자열 절대 반환 안 함)
    
    def _get_manufacturing_prompt(self) -> str:
        """제조업/일반 기업용 프롬프트"""
        return """
        너는 10년 차 펀드매니저야. 제공된 기업의 [사업보고서] 내용을 분석해서 
        투자 판단에 필요한 핵심 정보를 아래 JSON 포맷으로 정확하게 추출해.
        
        [🔥 최우선 확인: "사업의 개요" 섹션]
        - "Ⅱ. 사업의 내용 > 1. 사업의 개요"에서 회사 소개와 매출 비중 테이블이 있음
        - 이 섹션에서 회사가 어떤 사업을 하는지 (지주회사, 제조업, 서비스업 등)를 먼저 파악할 것
        - 사업의 개요 내 매출액 테이블이 있으면 revenue_by_segment 추출에 활용할 것
        
        [중요 지침]
        - 제공된 텍스트에서만 정보를 추출할 것 (추측하지 말것)
        - 명확히 언급되지 않은 정보는 "정보없음"으로 표시
        - 구체적인 숫자, 이름, 사실만 포함할 것
        
        [추출 항목]
        1. business_summary: "사업의 개요"에서 회사가 어떤 사업을 영위하는지 3줄 요약.
           - 지주회사인 경우 "지주회사로서 자회사 지분 보유 및 배당금 수익, 임대수익, 로열티 수익 등을 영위" 형태로 명시
           - 제조업인 경우 주요 제품과 사업 영역을 명확히 기술
        2. major_products: 주요 제품 및 서비스 리스트 (구체적 브랜드나 모델명 포함). 보고서에 명시된 것만.
        3. major_clients: 주요 매출처/고객사 실명 (예: Apple, 현대차). 보고서에 명시된 것만. 없으면 "정보없음".
        4. supply_chain: 원재료-공급사 쌍 리스트. 표의 '매입처' 컬럼에서 실명 추출.
           포맷: [{"item": "원재료명", "supplier": "공급사1, 공급사2"}]
           '기타', '국내법인' 등 실명이 아니면 제외.
        5. capax_investment: 설비투자(CAPEX)나 신규 시설 투자 계획 언급 요약. 보고서에 명시된 것만.
        6. cost_structure: 비용 구조에서 가장 큰 비중을 차지하는 것 (예: 원재료비, 인건비). 보고서에 명시된 것만.
        7. keywords: 기업을 설명하는 핵심 해시태그 5~7개.
           - 반드시 포함: 산업 분야 (예: #지주회사, #화장품, #반도체, #의류)
           - 사업 특성 (예: #OEM, #ODM, #수출중심, #배당수익)
           - 지주회사인 경우: #지주회사, #배당수익, #임대수익, #로열티 등
        8. revenue_by_segment: 사업부문별 매출 비중 (%) - 🔥 핵심 추출 항목!
           - "사업의 개요" 내 매출액 테이블에서 우선 추출
           - "매출 및 수주상황" 또는 "주요 제품 매출" 표에서도 추출
           - 부문명은 보고서에 명시된 그대로 사용 (예: "건설부문", "상사부문", "바이오부문")
           - 비중(%)이 명시되어 있으면 그대로 사용, 없으면 매출액 기준으로 비중 계산
           - 포맷: {"부문명": 비중(숫자), ...}
           - 지주회사인 경우: {"배당금수익": 비중, "임대수익": 비중, "로열티수익": 비중, ...}
           
           [복합 표(계층 구조) 처리]
           - 표가 "사업부문 > 품목 > 구체적 용도"처럼 계층 구조로 되어 있을 경우:
             * 가장 상위 개념인 "사업부문"을 기준으로 매출을 합산하세요
             * 하위 품목(예: 열연, 냉연)은 무시하고 상위 부문(예: 철강부문)의 합계를 사용하세요
           - "내부거래제거", "연결조정", "단순합계" 행은 사업부문이 아니므로 추출에서 제외하세요
           
           - 부문 내 "내부거래 제거" 항목은 제외
           - 🆕 P0-3: 표가 아니라 문장으로 "OO사업 67%, OO부문 14%"처럼 적혀 있으면 그것도 revenue_by_segment에 넣어라 (추측 금지, 명시된 퍼센트만)
           - 🆕 P0-2: 보고서에 "하나의 보고 부문" 또는 "단일 보고부문"이라고 명시되어 있으면 {"단일부문": 100.0} 반환
           - 🚨 반드시 추출하려고 시도할 것! 없으면 빈 객체 {} 반환
        
        [반환 형식]
        오직 JSON 형식만 반환할 것. (Markdown code block 없이)
        JSON 형식:
        {
            "business_summary": "...",
            "major_products": [...],
            "major_clients": "...",
            "supply_chain": [{"item": "원재료명", "supplier": "공급사명"}],
            "capax_investment": "...",
            "cost_structure": "...",
            "keywords": [...],
            "revenue_by_segment": {"부문명": 비중, ...}
        }
        """
    
    def _get_financial_prompt(self) -> str:
        """금융사 전용 프롬프트"""
        return """
        너는 10년 차 펀드매니저야. 제공된 [금융사 사업보고서] 내용을 분석해서 
        투자 판단에 필요한 핵심 정보를 아래 JSON 포맷으로 정확하게 추출해.
        
        [🔥 최우선 확인: "사업의 개요" 및 "영업의 현황" 섹션]
        - "Ⅱ. 사업의 내용 > 1. 사업의 개요"에서 회사가 어떤 금융사업을 하는지 확인
        - "2. 영업의 현황" 섹션에서 "영업의 종류", "영업의 개황", "부문정보", "세그먼트 정보" 테이블 확인
        - 금융지주회사인 경우: "영업의 종류" 테이블에서 사업부문별 구분 추출 (은행부문, 금융투자부문, 보험부문 등)
        - 일반 금융사(은행/보험/증권)인 경우: "영업의 현황" 섹션의 영업종류별 수익 구조 추출
        
        [중요 지침]
        - 제공된 텍스트에서만 정보를 추출할 것 (추측하지 말것)
        - 명확히 언급되지 않은 정보는 "정보없음" 또는 null로 표시
        - 구체적인 숫자, 비율, 사실만 포함할 것
        - 금융사는 supply_chain이 없으므로 빈 배열 []로 반환
        
        [추출 항목 - 금융사 전용]
        1. business_summary: "사업의 개요"에서 동사가 영위하는 금융사업 내용을 3줄 요약.
           - 금융지주회사인 경우: "금융지주회사로서 자회사(은행, 증권, 카드, 보험 등) 지분 보유 및 배당금 수익을 영위" 형태로 명시
        
        2. major_products: 주요 금융상품 및 서비스 리스트 (예: 기업대출, 가계대출, 카드, 보험상품, 자산운용 등)
        
        3. major_clients: 주요 거래 상대방 (예: "기업고객, 개인고객, 금융기관" 또는 "정보없음")
        
        4. supply_chain: 금융사는 공급망이 없으므로 빈 배열 [] 반환
        
        5. financial_value_chain: 금융사 밸류체인 구조화 (핵심!)
           - funding_structure: 자금 조달 구조
             * sources: 자금 조달원 리스트 (예: ["예금", "채권발행", "해외차입", "CP", "RP"], 보고서에 명시된 것만)
             * cost_of_funding: 이자비용률 (보고서에 명시된 경우만 숫자, 없으면 null)
             * rate_sensitivity: 금리 민감도 ("HIGH", "MEDIUM", "LOW" 또는 "정보없음")
             * duration_structure: ALM 구조 (예: "변동금리 위주", "고정금리 위주", 없으면 null)
           - asset_structure: 자산 구성
             * loans: 대출 구성 (보고서에 명시된 경우만)
               - corporate: 기업대출 비중 (%)
               - retail: 가계대출 비중 (%)
               - mortgage: 주택담보대출 비중 (%)
             * securities: 유가증권 구성 (보고서에 명시된 경우만)
               - bonds: 채권 비중 (%)
               - stocks: 주식 비중 (%)
               - alternatives: 대체투자 비중 (%)
             * industry_exposure: 산업군 노출 리스트 (예: ["건설", "PF", "조선", "중소기업"], 보고서에 명시된 것만)
           - revenue_structure: 수익 구조 (보고서에 명시된 경우만)
             * interest_income_ratio: 이자수익 비중 (%)
             * fee_income_ratio: 수수료수익 비중 (%)
             * trading_income_ratio: 트레이딩수익 비중 (%)
           - capital_adequacy: 자본적정성 지표 (BIS 비율 - 핵심!)
             * bis_total_ratio: BIS 총자본비율 (%) - [총자본/위험가중자산]x100
             * bis_tier1_ratio: BIS 기본자본비율 (%) - [기본자본/위험가중자산]x100
             * bis_cet1_ratio: BIS 보통주자본비율 (%) - [보통주자본/위험가중자산]x100
             * total_capital: 총자본 (억원)
             * risk_weighted_assets: 위험가중자산 (억원)
             * report_year: 보고서 기준연도 (예: "2024")
           - risk_exposure: 리스크 노출 (보고서에 명시된 경우만)
             * credit_risk: 신용리스크
               - npl_ratio: 부실채권 비율 (%)
               - provision_ratio: 충당금 비율 (%)
               - stage3_ratio: Stage3 비율 (%)
             * market_risk: 시장리스크
               - rate_risk: 금리 리스크 ("HIGH", "MEDIUM", "LOW" 또는 "정보없음")
               - fx_risk: 환율 리스크 ("HIGH", "MEDIUM", "LOW" 또는 "정보없음")
               - equity_risk: 주가 리스크 ("HIGH", "MEDIUM", "LOW" 또는 "정보없음")
             * liquidity_risk: 유동성리스크
               - lcr: 유동성커버리지비율 (보고서에 명시된 경우만)
               - loan_to_deposit_ratio: 예대율 (보고서에 명시된 경우만)
               - nsfr: 순안정자금비율 (보고서에 명시된 경우만)
             * sector_exposure: 특정 섹터 노출 리스트 (예: ["부동산", "PF", "건설"], 보고서에 명시된 것만)
           - major_counterparties: 주요 거래 상대방 (["기업고객", "개인고객", "금융기관"] 등)
        
        6. capax_investment: 설비투자(CAPEX)나 신규 시설 투자 계획 언급 요약. 보고서에 명시된 것만. 없으면 "정보없음".
        
        7. cost_structure: 비용 구조에서 가장 큰 비중을 차지하는 것 (예: 이자비용, 인건비, 운영비). 보고서에 명시된 것만.
        
        8. keywords: 기업을 설명하는 핵심 해시태그 5개. (보고서 내용 기반)
        
        9. revenue_by_segment: 사업부문별 매출/수익 비중 (%) - 🔥 핵심 추출 항목!
           [용어 확장] 금융사는 "매출액"이라는 단어를 쓰지 않습니다. 다음 용어들을 모두 "매출 데이터"로 간주하고 추출하세요:
           - "매출액", "매출비중" 외에 "영업수익", "순영업소득", "이자수익", "보험수익", "수입보험료", "당기순이익", "당기손익", "영업이익", "부문별 손익", "부문별 기여도"
           
           [금융지주회사 추출 방법]
           - "2. 영업의 현황 > 나. 영업의 종류" 테이블에서 사업부문별 구분 추출
           - 예: {"은행부문": 비중, "금융투자부문": 비중, "보험부문": 비중, "여신전문부문": 비중, "저축은행부문": 비중, "기타부문": 비중}
           - 각 부문의 계열사와 주요 사업 내용을 참고하여 부문명 정확히 추출
           - "당기손익 비중(%)", "부문별 기여도(%)" 같은 표가 있으면 우선 사용
           
           [일반 금융사(은행/보험/증권) 추출 방법]
           - "2. 영업의 현황" 섹션의 영업종류별 수익 구조 추출
           - 은행: {"은행계정": 비중, "신탁계정": 비중, ...}
           - 보험: {"생명보험": 비중, "손해보험": 비중, "장기보험": 비중, "일반보험": 비중, "자동차보험": 비중, ...}
             * 보험사는 "생명/손해보험" 구분 외에 "일반/장기/자동차" 보험 구분이나 "사망/생존" 급부 구분도 사업부문으로 인정
           - 증권: {"투자매매업": 비중, "투자중개업": 비중, "자산운용": 비중, ...}
           
           [비중 계산 명령] ⚠️ 매우 중요!
           - 표에 비중(%)이 명시되어 있으면 그대로 사용
           - 비중(%)이 명시되지 않은 경우:
             1. 각 부문의 금액(영업수익/당기손익/보험수익 등)을 합산하여 총액을 구하세요
             2. 각 부문의 금액을 총액으로 나누어 비중(%)을 계산하세요
             3. 소수점 둘째 자리까지 계산하여 revenue_by_segment에 포함하세요
             4. 예: 은행부문 100억, 증권부문 50억, 총액 150억 → {"은행부문": 66.67, "증권부문": 33.33}
           
           [복합 표(계층 구조) 처리]
           - 표가 "사업부문 > 품목 > 구체적 용도"처럼 계층 구조로 되어 있을 경우:
             * 가장 상위 개념인 "사업부문"을 기준으로 매출을 합산하세요
             * 하위 품목(예: 열연, 냉연)은 무시하고 상위 부문(예: 철강부문)의 합계를 사용하세요
           - "내부거래제거", "연결조정", "단순합계" 행은 사업부문이 아니므로 추출에서 제외하세요
           
           [부문명 규칙]
           - 부문명은 보고서에 명시된 그대로 사용 (예: "은행부문", "금융투자부문", "생명보험", "손해보험")
           - 포맷: {"부문명": 비중(숫자), ...}
           - 부문 내 "내부거래 제거" 항목은 제외
           - 🆕 P0-3: 표가 아니라 문장으로 "OO사업 67%, OO부문 14%"처럼 적혀 있으면 그것도 revenue_by_segment에 넣어라 (추측 금지, 명시된 퍼센트만)
           - 🆕 P0-2: 보고서에 "하나의 보고 부문" 또는 "단일 보고부문"이라고 명시되어 있으면 {"단일부문": 100.0} 반환
           - 🚨 반드시 추출하려고 시도할 것! 없으면 빈 객체 {} 반환
        
        [반환 형식]
        오직 JSON 형식만 반환할 것. (Markdown code block 없이)
        JSON 형식:
        {
            "business_summary": "...",
            "major_products": [...],
            "major_clients": "...",
            "supply_chain": [],
            "financial_value_chain": {
                "funding_structure": {
                    "sources": [...],
                    "cost_of_funding": null,
                    "rate_sensitivity": "...",
                    "duration_structure": null
                },
                "asset_structure": {
                    "loans": {"corporate": null, "retail": null, "mortgage": null},
                    "securities": {"bonds": null, "stocks": null, "alternatives": null},
                    "industry_exposure": [...]
                },
                "revenue_structure": {
                    "interest_income_ratio": null,
                    "fee_income_ratio": null,
                    "trading_income_ratio": null
                },
                "capital_adequacy": {
                    "bis_total_ratio": null,
                    "bis_tier1_ratio": null,
                    "bis_cet1_ratio": null,
                    "total_capital": null,
                    "risk_weighted_assets": null,
                    "report_year": "2024"
                },
                "risk_exposure": {
                    "credit_risk": {"npl_ratio": null, "provision_ratio": null, "stage3_ratio": null},
                    "market_risk": {"rate_risk": "...", "fx_risk": "...", "equity_risk": "..."},
                    "liquidity_risk": {"lcr": null, "loan_to_deposit_ratio": null, "nsfr": null},
                    "sector_exposure": [...]
                },
                "major_counterparties": [...]
            },
            "capax_investment": "...",
            "cost_structure": "...",
            "keywords": [...],
            "revenue_by_segment": {"부문명": 비중, ...}  // 🆕 추가
        }
        
        주의: 
        - 보고서에 명시되지 않은 정보는 null 또는 "정보없음"으로 표시
        - 숫자는 보고서에 정확히 명시된 경우만 포함
        - 추측하지 말 것
        """
    
    def validate_json(self, json_str: str) -> Optional[Dict[str, Any]]:
        """
        JSON 문자열 검증 및 수리
        
        Args:
            json_str: 검증할 JSON 문자열
        
        Returns:
            파싱된 Dict 또는 None
        """
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            try:
                repaired = json_repair.repair_json(json_str)
                return json.loads(repaired)
            except Exception as e:
                logger.error(f"JSON 검증 및 수리 실패: {e}")
                return None

