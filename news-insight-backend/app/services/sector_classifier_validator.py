"""
Phase 3: GPT 인과 Reasoning 생성

Top-2 후보 → 최종 1~3개 섹터 결정 + 인과 구조 분석
- 역할: 섹터 선택 + 인과 구조 분석 + 투자 관점 시사점 생성
- 모델: gpt-5-mini (비용 효율적)
- 개선: 단순 검증에서 인과 추론 엔진 input 생성으로 역할 확장
"""
import logging
import json
from typing import List, Dict, Optional, Tuple
import json_repair

logger = logging.getLogger(__name__)

from app.services.llm_handler import LLMHandler
from app.models.company_detail import CompanyDetail

# 28개 섹터 목록
ALL_SECTORS = [
    'SEC_SEMI', 'SEC_BATTERY', 'SEC_IT', 'SEC_GAME', 'SEC_ELECTRONICS',
    'SEC_AUTO', 'SEC_TIRE',
    'SEC_SHIP', 'SEC_DEFENSE', 'SEC_MACH', 'SEC_CONST', 'SEC_STEEL', 'SEC_CHEM',
    'SEC_ENT', 'SEC_COSMETIC', 'SEC_TRAVEL', 'SEC_FOOD', 'SEC_RETAIL', 'SEC_CONSUMER',
    'SEC_BIO', 'SEC_MEDDEV',
    'SEC_BANK', 'SEC_SEC', 'SEC_INS', 'SEC_CARD', 'SEC_HOLDING',
    'SEC_UTIL', 'SEC_TELECOM'
]


def _format_list(data) -> str:
    """리스트를 문자열로 포맷팅"""
    if not data:
        return 'N/A'
    if isinstance(data, list):
        return ', '.join([str(item) for item in data[:10]])  # 최대 10개
    return str(data)


def _format_supply_chain(supply_chain) -> str:
    """공급망 정보를 문자열로 포맷팅"""
    if not supply_chain:
        return 'N/A'
    if isinstance(supply_chain, list):
        items = []
        for item in supply_chain[:5]:  # 최대 5개
            if isinstance(item, dict):
                item_name = item.get('item', '')
                supplier = item.get('supplier', '')
                if item_name:
                    items.append(f"{item_name} (공급사: {supplier})" if supplier else item_name)
            else:
                items.append(str(item))
        return ', '.join(items) if items else 'N/A'
    return str(supply_chain)


def validate_sectors_with_gpt(
    company_text: str,
    company_name: Optional[str],
    candidates: List[Dict[str, float]],
    llm_handler: LLMHandler,
    max_sectors: int = 3,
    company_detail: Optional[CompanyDetail] = None  # 추가: 인과 분석용
) -> List[Dict[str, any]]:
    """
    GPT 기반 최종 섹터 검증 + 인과 구조 분석 (Top-2 → 최종 1~3개)
    
    역할 변경:
    - 기존: 단순 섹터 선택 검증
    - 개선: 섹터 선택 + 인과 구조 분석 + 투자 관점 시사점 생성
    
    Args:
        company_text: 회사 설명 텍스트 (biz_summary)
        company_name: 회사명
        candidates: BGE-M3 Re-ranking 결과 (Top-2)
                   [{'sector': 'SEC_SEMI', 'score': 0.85, 'bge_score': 0.82}, ...]
        llm_handler: LLMHandler 인스턴스
        max_sectors: 최대 선택 섹터 개수 (기본값: 3)
        company_detail: CompanyDetail 객체 (인과 분석용, 선택적)
    
    Returns:
        [
            {
                'sector': 'SEC_SEMI',
                'score': 0.9,
                'weight': 0.6,
                'reasoning': '사업 개요에서 "DRAM, NAND 플래시" 언급...',
                'is_primary': True,
                'causal_structure': {
                    'upstream_impacts': [...],
                    'downstream_impacts': [...],
                    'key_drivers': [...],
                    'risk_factors': [...],
                    'opportunity_factors': [...]
                },
                'investment_insights': '투자 관점 시사점...'
            },
            ...
        ]
    """
    if not candidates or not company_text or not company_text.strip():
        logger.warning("Empty candidates or company text provided")
        return []
    
    try:
        # 후보 섹터 코드 추출
        candidate_sectors = [c['sector'] for c in candidates]
        
        # 인과 분석용 정보 준비
        products_text = _format_list(company_detail.products) if company_detail else 'N/A'
        clients_text = _format_list(company_detail.clients) if company_detail else 'N/A'
        supply_chain_text = _format_supply_chain(company_detail.supply_chain) if company_detail else 'N/A'
        raw_materials_text = _format_list(company_detail.raw_materials) if company_detail else 'N/A'
        keywords_text = _format_list(company_detail.keywords) if company_detail and hasattr(company_detail, 'keywords') else 'N/A'
        
        # GPT 프롬프트 구성 (인과 구조 분석 중심)
        prompt = f"""다음 기업의 사업 구조를 분석하여 섹터를 분류하고, **인과 관계 관점에서** 분석하세요.

[기업 정보]
- 회사명: {company_name or '알 수 없음'}
- 주요 제품: {products_text}
- 주요 고객사: {clients_text}
- 공급망: {supply_chain_text}
- 원재료: {raw_materials_text}
- 키워드: {keywords_text}

[사업 개요]
{company_text[:3000]}

[후보 섹터 (임베딩 모델과 BGE-M3가 추천한 후보)]
{', '.join(candidate_sectors)}

[요구사항]
1. **섹터 검증** (후보 중 1~{max_sectors}개 선택, 가중치 부여)
   - ⚠️ 중요: 당신의 역할은 "새로운 분류를 생성"하는 것이 아니라, "제공된 후보 섹터를 검증"하는 것입니다.
   - 반드시 위 후보 섹터 중에서만 선택하세요.
   - 각 섹터 선택 근거를 biz_summary의 구체적인 문장으로 제시하세요.
   - 각 섹터별 가중치(weight)를 0.0~1.0 사이로 부여하세요 (합계는 1.0이 되도록).
   - 가장 주요한 섹터는 is_primary=true로 표시하세요.
   - ⚠️ 절대 Sub-sector를 새로 만들거나 변경하지 마세요. Sub-sector는 이미 다른 모델이 결정했습니다.

2. **인과 구조 분석** (각 섹터별):
   - upstream_impacts: 이 섹터가 영향을 받는 상류 섹터/기업 (예: "웨이퍼 공급사", "장비 제조사")
   - downstream_impacts: 이 섹터가 영향을 주는 하류 섹터/기업 (예: "AI 서버 제조사", "스마트폰 제조사")
   - key_drivers: 핵심 드라이버 변수 (P/Q/C: 가격/수량/비용)
     * 예: {{"var": "DRAM ASP", "type": "P", "description": "메모리 가격이 매출에 직접 영향"}}
   - granular_tags: 이 기업의 세부 특성을 나타내는 태그 리스트 (선택적, JSON 형식)
     * 예: ["HBM", "EUV", "AI 서버"] (반도체의 경우)
     * 태그는 간결하게 2-5개로 제한
     * causal_structure 안에 "granular_tags" 필드로 포함
   - risk_factors: 전형적인 리스크 요인 (예: ["재고 과다", "가격 하락 사이클"])
   - opportunity_factors: 기회 요인 (예: ["AI 데이터센터 확대", "HBM 수요 증가"])

3. **투자 관점 시사점** (2-3줄):
   - 이 섹터 분류가 투자 의사결정에 주는 의미
   - 주목해야 할 핵심 변수나 이벤트
   - 예: "반도체 섹터 중에서도 AI 서버용 메모리에 집중되어 있어, AI 데이터센터 투자 확대 시 직접적 수혜 가능. 다만 메모리 가격 사이클에 민감하므로 재고 레벨 모니터링 필요."

**중요:** 
- 후보 섹터 목록에 있는 섹터만 선택 가능합니다.
- ⚠️ 당신은 "Classifier"가 아니라 "Validator"입니다.
- Sub-sector는 이미 결정되었으므로, 절대 새로 생성하거나 변경하지 마세요.
- 인과 구조 분석은 실제 사업 구조(제품, 고객사, 공급망)를 기반으로 작성하세요.
- 투자 시사점은 구체적이고 실용적으로 작성하세요.

다음 JSON 형식으로만 응답하세요:
{{
    "sectors": [
        {{
            "sector": "SEC_SEMI",
            "weight": 0.6,
            "reasoning": "사업 개요에서 'DRAM과 NAND 플래시 메모리를 제조'라고 명시되어 있어 반도체 섹터가 가장 적절합니다.",
            "is_primary": true,
            "causal_structure": {{
                "upstream_impacts": ["웨이퍼 공급사", "반도체 장비 제조사"],
                "downstream_impacts": ["AI 서버 제조사", "스마트폰 제조사"],
                "key_drivers": [
                    {{"var": "DRAM ASP", "type": "P", "description": "메모리 가격이 매출에 직접 영향"}},
                    {{"var": "AI 서버 CAPEX", "type": "Q", "description": "AI 투자 확대로 수요 증가"}}
                ],
                "granular_tags": ["HBM", "AI 서버"],
                "risk_factors": ["재고 과다", "가격 하락 사이클", "반도체 설비 투자 과다"],
                "opportunity_factors": ["AI 데이터센터 확대", "HBM 수요 증가"]
            }},
            "investment_insights": "반도체 섹터 중에서도 AI 서버용 메모리에 집중되어 있어, AI 데이터센터 투자 확대 시 직접적 수혜 가능. 다만 메모리 가격 사이클에 민감하므로 재고 레벨 모니터링 필요."
        }}
    ]
}}
"""
        
        # LLM 호출
        logger.info(f"GPT 최종 검증 중... (후보: {candidate_sectors})")
        
        response = llm_handler.llm.invoke([
            {
                "role": "system",
                "content": """You are a financial analyst specializing in Korean stock market sector validation and causal structure analysis. 
Your role is to:
1. VALIDATE (not classify) provided sector candidates based on business structure
2. Analyze causal relationships (upstream/downstream impacts)
3. Identify key drivers (P/Q/C: Price/Quantity/Cost)
4. Provide investment insights connecting sector classification to investment decisions
5. Generate granular tags for detailed sector characteristics

⚠️ CRITICAL: You are a VALIDATOR, not a CLASSIFIER. Do NOT create or modify sub-sectors. 
Sub-sectors have already been determined by ensemble models. Your job is to verify and explain, not to reclassify.

Output JSON only."""
            },
            {
                "role": "user",
                "content": prompt
            }
        ])
        
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
                # Fallback: 후보 섹터 그대로 반환
                return _create_fallback_result(candidates, max_sectors)
        
        # 결과 검증 및 변환
        sectors = result.get('sectors', [])
        
        if not sectors:
            logger.warning("GPT가 섹터를 선택하지 않음. 후보 섹터 반환.")
            return _create_fallback_result(candidates, max_sectors)
        
        # 최대 개수 제한
        sectors = sectors[:max_sectors]
        
        # 가중치 정규화 (합계 1.0이 되도록)
        total_weight = sum(s.get('weight', 0.0) for s in sectors)
        if total_weight > 0:
            for s in sectors:
                s['weight'] = s.get('weight', 0.0) / total_weight
        else:
            # 가중치가 없으면 균등 분배
            equal_weight = 1.0 / len(sectors)
            for s in sectors:
                s['weight'] = equal_weight
        
        # 최종 결과 구성 및 후보 섹터 검증
        validated_sectors = []
        candidate_sector_codes = set([c['sector'] for c in candidates])
        invalid_sectors = []
        
        for sector_info in sectors:
            sector_code = sector_info.get('sector')
            if not sector_code or sector_code not in ALL_SECTORS:
                logger.warning(f"Invalid sector code: {sector_code}")
                continue
            
            # ⭐ 후보 섹터 검증: GPT가 후보 섹터가 아닌 것을 선택했는지 확인
            if sector_code not in candidate_sector_codes:
                logger.warning(f"⚠️ GPT가 후보 섹터가 아닌 섹터를 선택했습니다: {sector_code} (후보: {candidate_sector_codes})")
                invalid_sectors.append(sector_code)
                continue
            
            # 원본 후보에서 점수 가져오기
            original_candidate = next((c for c in candidates if c['sector'] == sector_code), None)
            
            # 인과 구조 분석 결과 추출
            causal_structure = sector_info.get('causal_structure', {})
            investment_insights = sector_info.get('investment_insights', '')
            
            validated_sectors.append({
                'sector': sector_code,
                'score': float(sector_info.get('weight', 0.5)) * 100,  # 0-100 스케일
                'weight': float(sector_info.get('weight', 0.5)),
                'reasoning': sector_info.get('reasoning', ''),
                'is_primary': bool(sector_info.get('is_primary', False)),
                'gpt_score': float(sector_info.get('weight', 0.5)),
                # 원본 후보 점수도 유지
                'embedding_score': original_candidate.get('score', 0.0) if original_candidate else 0.0,
                'bge_score': original_candidate.get('bge_score', 0.0) if original_candidate else 0.0,
                # ⭐ 인과 구조 분석 결과 (레벨 2)
                'causal_structure': causal_structure if causal_structure else None,
                'investment_insights': investment_insights if investment_insights else None
            })
        
        # GPT가 후보 섹터가 아닌 것을 선택한 경우 Fallback
        if invalid_sectors:
            logger.warning(f"GPT가 후보 섹터가 아닌 섹터를 선택했습니다: {invalid_sectors}. Fallback 결과 사용.")
            return _create_fallback_result(candidates, max_sectors)
        
        # Primary 섹터가 없으면 첫 번째를 Primary로 설정
        if not any(s['is_primary'] for s in validated_sectors):
            if validated_sectors:
                validated_sectors[0]['is_primary'] = True
        
        logger.info(f"GPT 최종 검증 완료: {len(validated_sectors)}개 섹터 선택")
        
        return validated_sectors
        
    except Exception as e:
        logger.error(f"Error in GPT sector validation: {e}", exc_info=True)
        # 에러 발생 시 후보 섹터 그대로 반환
        return _create_fallback_result(candidates, max_sectors)


def _create_fallback_result(candidates: List[Dict[str, float]], max_sectors: int) -> List[Dict[str, any]]:
    """
    GPT 검증 실패 시 Fallback 결과 생성
    
    Args:
        candidates: BGE-M3 Re-ranking 결과
        max_sectors: 최대 섹터 개수
    
    Returns:
        Fallback 결과 리스트
    """
    if not candidates:
        return []
    
    fallback_sectors = []
    total_score = sum(c.get('bge_score', c.get('score', 0.0)) for c in candidates[:max_sectors])
    
    for i, candidate in enumerate(candidates[:max_sectors]):
        sector_code = candidate['sector']
        score = candidate.get('bge_score', candidate.get('score', 0.0))
        weight = score / total_score if total_score > 0 else 1.0 / len(candidates[:max_sectors])
        
        fallback_sectors.append({
            'sector': sector_code,
            'score': float(score) * 100,
            'weight': float(weight),
            'reasoning': f'GPT 검증 실패로 인한 Fallback (BGE-M3 점수: {score:.2f})',
            'is_primary': (i == 0),  # 첫 번째를 Primary로
            'gpt_score': 0.0,  # GPT 점수 없음
            'embedding_score': candidate.get('score', 0.0),
            'bge_score': candidate.get('bge_score', 0.0)
        })
    
    return fallback_sectors

