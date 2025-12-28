"""
Phase 3: GPT 최종 검증 (밸류체인 분류)

Top-2 후보 → 최종 1~3개 밸류체인 위치 결정
- 역할: 최종 밸류체인 위치 결정 및 근거 제시
- 모델: gpt-5-mini (비용 효율적)
"""
import logging
import json
from typing import List, Dict, Optional
import json_repair

logger = logging.getLogger(__name__)

from app.services.llm_handler import LLMHandler

# 밸류체인 위치 목록
ALL_VALUE_CHAINS = ['UPSTREAM', 'MIDSTREAM', 'DOWNSTREAM']


def _create_fallback_result(candidates: List[Dict[str, any]], max_positions: int = 3) -> List[Dict[str, any]]:
    """
    Fallback 결과 생성 (GPT 실패 시)
    
    Args:
        candidates: 후보 밸류체인 위치 리스트
        max_positions: 최대 위치 개수
    
    Returns:
        Fallback 결과 리스트
    """
    if not candidates:
        return []
    
    results = []
    total_score = sum(c.get('similarity', 0.0) for c in candidates[:max_positions])
    
    for i, candidate in enumerate(candidates[:max_positions]):
        score = candidate.get('similarity', 0.0)
        weight = score / total_score if total_score > 0 else 1.0 / len(candidates[:max_positions])
        
        results.append({
            'value_chain': candidate.get('value_chain', 'MIDSTREAM'),
            'weight': float(weight),
            'confidence': candidate.get('confidence', 'MEDIUM'),
            'method': 'ENSEMBLE',
            'reasoning': f"임베딩 유사도 기반 분류 (similarity={score:.2f})",
            'is_primary': (i == 0)
        })
    
    return results


def validate_value_chain_with_gpt(
    company_text: str,
    sector_code: str,
    company_name: Optional[str],
    candidates: List[Dict[str, any]],
    llm_handler: LLMHandler,
    max_positions: int = 3
) -> List[Dict[str, any]]:
    """
    GPT 기반 최종 밸류체인 위치 검증 (Top-2 → 최종 1~3개)
    
    역할: 최종 밸류체인 위치 결정 및 근거 제시
    특히 복합 기업(지주사, 종합상사)에서 여러 밸류체인 위치를 정확히 식별
    
    Args:
        company_text: 회사 설명 텍스트 (biz_summary)
        sector_code: 섹터 코드
        company_name: 회사명
        candidates: BGE-M3 Re-ranking 결과 (Top-2)
                   [{'value_chain': 'MIDSTREAM', 'similarity': 0.85, 'bge_score': 0.82}, ...]
        llm_handler: LLMHandler 인스턴스
        max_positions: 최대 선택 위치 개수 (기본값: 3)
    
    Returns:
        [
            {
                'value_chain': 'MIDSTREAM',
                'weight': 0.6,
                'confidence': 'HIGH',
                'method': 'ENSEMBLE',
                'reasoning': '...',
                'is_primary': True
            },
            ...
        ]
    """
    if not candidates or not company_text or not company_text.strip():
        logger.warning("Empty candidates or company text provided")
        return []
    
    try:
        # 후보 밸류체인 위치 추출
        candidate_positions = [c['value_chain'] for c in candidates]
        
        # GPT 프롬프트 구성
        prompt = f"""다음 기업의 사업 개요 텍스트를 분석하여 밸류체인 위치를 판단하세요.

[기업 정보]
회사명: {company_name or '알 수 없음'}
섹터: {sector_code}

[사업 개요]
{company_text[:3000]}  # 3000자로 제한

[후보 밸류체인 위치 (이미 다른 모델에서 추천됨)]
{', '.join(candidate_positions)}

[가능한 모든 밸류체인 위치]
- UPSTREAM: 원재료/부품/소재 조달 및 공급
- MIDSTREAM: 제조/생산/가공
- DOWNSTREAM: 판매/유통/서비스

[요구사항]
1. 위 사업 개요 텍스트를 분석하여 가장 적절한 밸류체인 위치 1~{max_positions}개를 선택하세요.
2. 후보 위치가 적절하면 후보 위치를 선택하되, 사업 개요와 맞지 않으면 다른 위치도 선택 가능합니다.
3. 각 위치 선택 근거를 biz_summary의 구체적인 문장으로 제시하세요.
4. 각 위치별 가중치(weight)를 0.0~1.0 사이로 부여하세요 (합계는 1.0이 되도록).
5. 가장 주요한 위치는 is_primary=true로 표시하세요.
6. 복합 기업(지주사, 종합상사)의 경우 여러 위치를 동시에 선택할 수 있습니다.

다음 JSON 형식으로만 응답하세요:
{{
    "value_chains": [
        {{
            "value_chain": "MIDSTREAM",
            "weight": 0.6,
            "reasoning": "사업 개요에서 '제조하고 있으며'라고 명시되어 있어 제조(MIDSTREAM) 위치가 가장 적절합니다.",
            "is_primary": true
        }},
        {{
            "value_chain": "DOWNSTREAM",
            "weight": 0.4,
            "reasoning": "'판매 및 유통' 사업도 언급되어 있어 판매(DOWNSTREAM) 위치도 포함됩니다.",
            "is_primary": false
        }}
    ]
}}
"""
        
        # LLM 호출
        logger.info(f"GPT 최종 밸류체인 검증 중... (후보: {candidate_positions})")
        
        try:
            response = llm_handler.llm.invoke([
                {
                    "role": "system",
                    "content": "You are a financial analyst specializing in Korean stock market value chain analysis. Analyze company business summaries and classify their value chain positions. Output JSON only."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ])
        except ValueError as e:
            if "Sync client is not available" in str(e):
                logger.warning("Sync client 오류 감지, Fallback 결과 사용")
                # Fallback 결과 반환
                return _create_fallback_result(candidates, max_positions)
            else:
                raise
        
        # 응답 파싱
        content = response.content if hasattr(response, 'content') else str(response)
        content = content.replace("```json", "").replace("```", "").strip()
        
        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            logger.warning("JSON 파싱 실패, json_repair 시도...")
            try:
                repaired = json_repair.repair_json(content)
                result = json.loads(repaired)
            except Exception as e:
                logger.error(f"JSON 수리 실패: {e}")
                # Fallback: 후보 위치 그대로 반환
                return _create_fallback_result(candidates, max_positions)
        
        # 결과 검증 및 변환
        value_chains = result.get('value_chains', [])
        
        if not value_chains:
            logger.warning("GPT가 밸류체인 위치를 선택하지 않음. 후보 위치 반환.")
            return _create_fallback_result(candidates, max_positions)
        
        # 최대 개수 제한
        value_chains = value_chains[:max_positions]
        
        # 가중치 정규화 (합계가 1.0이 되도록)
        total_weight = sum(vc.get('weight', 0.0) for vc in value_chains)
        if total_weight > 0:
            for vc in value_chains:
                vc['weight'] = vc.get('weight', 0.0) / total_weight
        else:
            # 가중치가 없으면 균등 분배
            equal_weight = 1.0 / len(value_chains)
            for vc in value_chains:
                vc['weight'] = equal_weight
        
        # 결과 변환
        validated_results = []
        for vc in value_chains:
            vc_position = vc.get('value_chain', '').upper()
            
            # 유효한 밸류체인 위치인지 확인
            if vc_position not in ALL_VALUE_CHAINS:
                logger.warning(f"Invalid value chain position: {vc_position}. Skipping.")
                continue
            
            validated_results.append({
                'value_chain': vc_position,
                'weight': float(vc.get('weight', 0.5)),
                'confidence': 'HIGH',  # GPT 검증 통과
                'method': 'ENSEMBLE',
                'reasoning': vc.get('reasoning', 'GPT 기반 검증'),
                'is_primary': vc.get('is_primary', False),
                'gpt_score': 1.0  # GPT 검증 통과
            })
        
        if not validated_results:
            logger.warning("검증된 결과 없음. Fallback 반환.")
            return _create_fallback_result(candidates, max_positions)
        
        logger.info(f"GPT 검증 완료: {len(validated_results)}개 위치 선택")
        
        return validated_results
        
    except Exception as e:
        logger.error(f"Error in GPT value chain validation: {e}", exc_info=True)
        # 에러 발생 시 Fallback 반환
        return _create_fallback_result(candidates, max_positions)

