"""
4단계 멀티 모델 앙상블 섹터 분류 파이프라인

Step 1: Rule-based (가중치 40%) - Confidence HIGH면 즉시 반환
Step 2: 임베딩 모델 (가중치 30%) - Top-5 후보 생성
Step 3: BGE-M3 (가중치 20%) - Top-5 → Top-2 Re-ranking
Step 4: gpt-5-mini (가중치 10%) - Top-2 → 최종 1~3개 검증

예상 정확도: 90-95%
"""
import os
import logging
from typing import Optional, Dict, Any, List, Tuple
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# 기존 Rule-based 분류기
from app.services.sector_classifier import (
    classify_sector_rule_based,
    SECTOR_KEYWORDS
)

# Phase 1: 임베딩 모델 기반 후보 생성기
EMBEDDING_AVAILABLE = False
try:
    from app.services.sector_classifier_embedding import generate_sector_candidates, SENTENCE_TRANSFORMERS_AVAILABLE
    if SENTENCE_TRANSFORMERS_AVAILABLE:
        EMBEDDING_AVAILABLE = True
        logger.info("임베딩 모델 모듈 import 성공 (lazy loading - 실제 사용 시점에 로드)")
    else:
        EMBEDDING_AVAILABLE = False
        logger.warning("sentence-transformers not installed. Embedding model not available.")
except ImportError as e:
    EMBEDDING_AVAILABLE = False
    logger.warning(f"임베딩 모델 import 실패: {e}. Will skip candidate generation.")

# Phase 2: BGE-M3 Re-ranking
try:
    from app.services.sector_classifier_reranker import rerank_sector_candidates
    BGE_AVAILABLE = True
except ImportError:
    BGE_AVAILABLE = False
    logger.warning("BGE-M3 not available. Will skip reranking.")

# Phase 3: GPT 최종 검증
try:
    from app.services.sector_classifier_validator import validate_sectors_with_gpt
    GPT_AVAILABLE = True
except ImportError:
    GPT_AVAILABLE = False
    logger.warning("GPT validator not available. Will skip final validation.")

from app.models.company_detail import CompanyDetail
from app.models.sector_reference import LEGACY_SECTOR_MAPPING
from app.models.edge import Edge
from app.models.investor_sector import InvestorSector
from app.services.llm_handler import LLMHandler
from app.utils.text_chunking import truncate_to_sentences
from app.utils.company_complexity_detector import is_complex_company

# 밸류체인 분류 (하이브리드 방식)
try:
    from app.services.value_chain_classifier import classify_value_chain_hybrid
    VALUE_CHAIN_CLASSIFIER_AVAILABLE = True
except ImportError:
    VALUE_CHAIN_CLASSIFIER_AVAILABLE = False
    logger.warning("Value chain classifier not available. Will use rule-based only.")

# 의미 기반 핵심 문장 추출 (Phase 1)
try:
    from app.utils.semantic_sentence_extractor import extract_key_sentences_for_sector
    SEMANTIC_EXTRACTION_AVAILABLE = True
except ImportError:
    SEMANTIC_EXTRACTION_AVAILABLE = False
    logger.warning("Semantic sentence extraction not available. Will use fallback.")


# 전략 B: Anchor Boosting을 위한 White-list Anchor 정의
# 고객사(major_clients)에 이 기업들이 포함되면 해당 섹터에 가산점 부여
# 단, 후보군(Candidates)에 해당 섹터가 이미 존재할 때만 부여 (교집합 전략)
STRICT_ANCHORS = {
    # [반도체] 삼성전자, 하이닉스 납품 = 99% 반도체 소부장
    '삼성전자': 'SEC_SEMI', 'SK하이닉스': 'SEC_SEMI', 
    
    # [2차전지] 셀/소재 업체 납품 = 배터리 밸류체인
    'LG에너지솔루션': 'SEC_BATTERY', '삼성SDI': 'SEC_BATTERY', 'SK온': 'SEC_BATTERY', 
    '에코프로비엠': 'SEC_BATTERY', '포스코퓨처엠': 'SEC_BATTERY',
    
    # [자동차] 현대기아차 납품 = 자동차 부품
    '현대자동차': 'SEC_AUTO', '기아': 'SEC_AUTO', '현대모비스': 'SEC_AUTO',
    
    # [바이오] 삼바/셀트리온 납품 = 바이오 소부장/원료
    '삼성바이오로직스': 'SEC_BIO', '셀트리온': 'SEC_BIO',
    
    # [조선] 조선 3사 납품 = 조선 기자재
    'HD현대중공업': 'SEC_SHIP', '한화오션': 'SEC_SHIP', '삼성중공업': 'SEC_SHIP',
    
    # [방산] 체계종합업체 납품 = 방산 부품
    '한화에어로스페이스': 'SEC_DEFENSE', 'LIG넥스원': 'SEC_DEFENSE', '한국항공우주': 'SEC_DEFENSE'
}

# 범용 섹터 (Block List) - Boosting 완전 금지
INDUSTRY_AGNOSTIC_SECTORS = {
    'SEC_IT', 'SEC_IT_SERVICE', 'SEC_SI', 'SEC_SW', 'SEC_PLATFORM',
    'SEC_CONST', 'SEC_ENG', 'SEC_MEDIA', 'SEC_AD',
    'SEC_LOGISTICS', 'SEC_DISTRIBUTION', 'SEC_FINANCE', 'SEC_HOLDING'
}

# Boosting Budget (총량 상한제)
MAX_TOTAL_BOOST = 0.05  # 최대 +0.05
TOP2_GAP_THRESHOLD = 0.03  # Top-2 점수 차이 임계값

# Base Boosting 값
BASE_ANCHOR_BOOST = 0.03
BASE_KG_BOOST = 0.02

# Edge 타입별 가중치 (안전한 Edge만 사용)
EDGE_WEIGHTS = {
    'SUPPLIES_TO': 1.0,  # 공급 관계 (안전)
    'CORE_DEPENDENCY': 0.8,  # 핵심 의존 (안전)
    'CUSTOMER_OF': 0.0,  # 고객 관계 (위험)
    'PROJECT_WITH': 0.0,  # 프로젝트 관계 (위험)
    'RELATED_TO': 0.0,  # 일반 관련 (위험)
    'BUSINESS_RELATION': 0.0,  # 비즈니스 관계 (위험)
}


# Role 정의 (최종 설계안)
ROLE_DEFINITIONS = {
    'SI_IT_SERVICE': {
        'positive': ['si', '시스템통합', 'sm', '유지보수', '구축', '컨설팅', 'it서비스', 'it 서비스'],
        'negative': ['공장', '양산', '생산라인', '설비'],
        'is_agnostic': True
    },
    'SECURITY': {
        'positive': ['edr', 'xdr', 'siem', 'waf', 'iam', 'sso', '보안관제', '보안솔루션'],
        'negative': ['경비', 'cctv', '물리보안'],
        'is_agnostic': True
    },
    'DATA_INFRA': {
        'positive': ['etl', 'data lake', 'warehouse', 'bi', 'mlops', '데이터인프라'],
        'negative': ['공장', '양산'],
        'is_agnostic': True
    },
    'CONSTRUCTION': {
        'positive': ['건설', '시공', '토목', '플랜트', 'epc'],
        'negative': ['양산', '제품'],
        'is_agnostic': True
    }
}


def classify_company_role(
    company_detail: CompanyDetail,
    company_name: Optional[str] = None
) -> Tuple[Optional[Dict[str, Any]], float]:
    """
    기업 타입(company_role) 분류
    
    설계 원칙:
    - Boosting을 차단하는 신호이지 섹터를 정하는 신호가 아님
    - Positive/Negative 가중치 방식
    - Confidence는 격차 기반 계산
    
    Returns:
        (role_info, confidence): role_info는 {'role': str, 'is_agnostic': bool} 형태, confidence는 0.0 ~ 1.0
    """
    text = (company_detail.biz_summary or "").lower()
    products_text = ' '.join([str(p).lower() for p in (company_detail.products or [])])
    keywords_text = ' '.join([str(k).lower() for k in (company_detail.keywords or [])])
    combined_text = f"{text} {products_text} {keywords_text}"
    
    role_scores = {}
    
    # 각 Role별 점수 계산 (positive * 2 - negative * 3)
    for role_key, role_def in ROLE_DEFINITIONS.items():
        positive_hits = sum(1 for kw in role_def['positive'] if kw in combined_text)
        negative_hits = sum(1 for kw in role_def['negative'] if kw in combined_text)
        score = (positive_hits * 2) - (negative_hits * 3)
        role_scores[role_key] = {
            'score': score,
            'is_agnostic': role_def['is_agnostic']
        }
    
    # 최고 점수 Role 찾기
    if not role_scores or max(r['score'] for r in role_scores.values()) <= 0:
        return (None, 0.0)
    
    # Top-2 점수로 confidence 계산 (격차 기반)
    sorted_roles = sorted(role_scores.items(), key=lambda x: x[1]['score'], reverse=True)
    top1_score = sorted_roles[0][1]['score']
    top2_score = sorted_roles[1][1]['score'] if len(sorted_roles) > 1 else 0.0
    
    # Confidence = (top1 - top2) / (top1 + 1e-6)
    confidence = (top1_score - top2_score) / (top1_score + 1e-6)
    confidence = min(max(confidence, 0.0), 1.0)
    
    detected_role_key = sorted_roles[0][0]
    role_info = {
        'role': detected_role_key,
        'is_agnostic': role_scores[detected_role_key]['is_agnostic']
    }
    
    return (role_info, confidence)


def apply_anchor_boosting(
    candidates: List[Dict], 
    company_detail: CompanyDetail,
    company_name: Optional[str] = None,
    top2_gap: Optional[float] = None,
    remaining_budget: float = MAX_TOTAL_BOOST
) -> Tuple[List[Dict], Dict[str, Any], float]:
    """
    전략 B: Anchor Boosting (보정, 판단 아님)
    
    설계 원칙:
    - Boosting은 판단이 아니라 보정이다
    - 동점 깨기만 허용 (Top-2 gap < threshold)
    - 범용 섹터는 완전 금지
    - company_role 기반 감쇠(Multiplier) 적용
    - Budget 사전 체크 후 적용
    
    Args:
        candidates: 섹터 후보 리스트 (점수순 정렬됨)
        company_detail: CompanyDetail 객체
        company_name: 회사명
        top2_gap: Top-2 점수 차이 (None이면 계산)
        remaining_budget: 남은 Boosting Budget
    
    Returns:
        (보정된 후보 리스트, boosting_log, 실제 적용된 boost)
    """
    boosting_log = {
        "anchor_applied": False,
        "kg_applied": False,
        "reason": "",
        "multiplier": 1.0,
        "final_boost": 0.0
    }
    
    # Multiplier 초기화
    multiplier = 1.0
    
    # Gate 1: L1 섹터 Gate (가장 강력 - 완전 금지)
    if not candidates or len(candidates) < 2:
        return candidates, boosting_log, 0.0
    
    top_sector = candidates[0].get('sector')
    if top_sector in INDUSTRY_AGNOSTIC_SECTORS:
        boosting_log["reason"] = f"범용 섹터 ({top_sector}) → Boosting 완전 금지"
        boosting_log["multiplier"] = 0.0
        return candidates, boosting_log, 0.0
    
    # Gate 2: Role Gate (감쇠)
    role_info, role_confidence = classify_company_role(company_detail, company_name)
    if role_info and role_info.get('is_agnostic') and role_confidence >= 0.7:
        multiplier = min(multiplier, 0.2)  # 80% 감쇠
        boosting_log["reason"] = f"Role={role_info['role']} (confidence={role_confidence:.2f}) → 80% 감쇠"
    
    # Gate 3: Tie-breaker Gate (동점 아닐 때 차단)
    if top2_gap is None:
        if len(candidates) >= 2:
            top2_gap = candidates[0]['score'] - candidates[1]['score']
        else:
            top2_gap = 1.0
    
    if top2_gap >= TOP2_GAP_THRESHOLD:
        boosting_log["reason"] = f"Top-2 gap ({top2_gap:.3f}) >= threshold ({TOP2_GAP_THRESHOLD})"
        boosting_log["multiplier"] = 0.0
        return candidates, boosting_log, 0.0
    
    # Gate 4: 고객사 없음
    if not company_detail.clients:
        return candidates, boosting_log, 0.0
    
    # 1. 고객사 텍스트에서 Anchor 찾기
    found_anchor_sectors = set()
    clients_text = str(company_detail.clients)
    
    # 어떤 Anchor가 발견되었는지 추적 (로깅용)
    found_anchors_map = {}  # {sector: [anchor_names]}
    
    for anchor_name, anchor_sector in STRICT_ANCHORS.items():
        if anchor_name in clients_text:
            found_anchor_sectors.add(anchor_sector)
            if anchor_sector not in found_anchors_map:
                found_anchors_map[anchor_sector] = []
            found_anchors_map[anchor_sector].append(anchor_name)
    
    if not found_anchor_sectors:
        return candidates, boosting_log, 0.0
        
    # 2. Boost 계산
    calculated_boost = BASE_ANCHOR_BOOST * multiplier
    
    # Budget 사전 체크
    allowed_boost = min(calculated_boost, remaining_budget)
    
    if allowed_boost <= 0:
        boosting_log["reason"] = f"Budget 부족 (remaining: {remaining_budget:.3f})"
        return candidates, boosting_log, 0.0
    
    # 교집합(Intersection)에만 가산점 부여
    boosted = False
    actual_boost = 0.0
    
    for candidate in candidates:
        sector = candidate['sector']
        if sector in found_anchor_sectors:
            # 범용 섹터는 제외
            if sector in INDUSTRY_AGNOSTIC_SECTORS:
                continue
                
            old_score = candidate['score']
            boost = allowed_boost  # Budget 제한된 boost 사용
            candidate['score'] += boost
            candidate['score'] = min(1.0, candidate['score'])
            actual_boost = max(actual_boost, boost)
            
            # 근거 추가
            anchors = found_anchors_map.get(sector, [])
            anchor_str = ", ".join(anchors[:3])
            
            current_reasoning = candidate.get('reasoning', '')
            if current_reasoning:
                candidate['reasoning'] = f"{current_reasoning} [Anchor: {anchor_str}]"
            else:
                candidate['reasoning'] = f"주요 고객사({anchor_str}) 기반 보정"
            
            logger.info(
                f"🚀 Anchor Boosting: {sector} "
                f"(Score: {old_score:.2f} → {candidate['score']:.2f}, "
                f"Boost: +{boost:.3f}, Multiplier: {multiplier:.2f})"
            )
            boosted = True
    
    # 점수순 재정렬
    if boosted:
        candidates.sort(key=lambda x: x['score'], reverse=True)
        boosting_log.update({
            "anchor_applied": True,
            "multiplier": multiplier,
            "final_boost": actual_boost,
            "reason": f"Top-2 gap ({top2_gap:.3f}) < threshold, Anchor 발견"
        })
        
    return candidates, boosting_log, actual_boost


def apply_kg_edge_boosting(
    candidates: List[Dict], 
    ticker: str, 
    db: Session,
    company_detail: CompanyDetail,
    company_name: Optional[str] = None,
    top2_gap: Optional[float] = None,
    remaining_budget: float = MAX_TOTAL_BOOST,
    multiplier: float = 1.0
) -> Tuple[List[Dict], Dict[str, Any], float]:
    """
    KG Edge 기반 미세 보정 (보정, 판단 아님)
    
    설계 원칙:
    - 안전한 Edge 타입만 사용 (SUPPLIES_TO, CORE_DEPENDENCY)
    - 위험한 Edge 타입 제외 (CUSTOMER_OF, PROJECT_WITH 등)
    - Edge 타입별 가중치 차등 적용
    - 동점 깨기만 허용
    - Budget 사전 체크 후 적용
    
    Args:
        candidates: 섹터 후보 리스트
        ticker: 종목코드
        db: DB 세션
        company_detail: CompanyDetail 객체
        company_name: 회사명
        top2_gap: Top-2 점수 차이
        remaining_budget: 남은 Boosting Budget
        multiplier: Role Gate에서 계산된 multiplier
    
    Returns:
        (보정된 후보 리스트, boosting_log, 실제 적용된 boost)
    """
    boosting_log = {
        "anchor_applied": False,
        "kg_applied": False,
        "reason": "",
        "multiplier": multiplier,
        "final_boost": 0.0
    }
    
    # Gate 1: L1 섹터 Gate (가장 강력 - 완전 금지)
    if not candidates or len(candidates) < 2:
        return candidates, boosting_log, 0.0
    
    top_sector = candidates[0].get('sector')
    if top_sector in INDUSTRY_AGNOSTIC_SECTORS:
        boosting_log["reason"] = f"범용 섹터 ({top_sector}) → KG Boosting 완전 금지"
        boosting_log["multiplier"] = 0.0
        return candidates, boosting_log, 0.0
    
    # Gate 2: Role Gate (감쇠) - multiplier는 이미 계산됨
    if multiplier <= 0:
        boosting_log["reason"] = f"Role Gate로 인한 감쇠 (multiplier: {multiplier:.2f})"
        return candidates, boosting_log, 0.0
    
    # Gate 3: Tie-breaker Gate (동점 아닐 때 차단)
    if top2_gap is None:
        if len(candidates) >= 2:
            top2_gap = candidates[0]['score'] - candidates[1]['score']
        else:
            top2_gap = 1.0
    
    if top2_gap >= TOP2_GAP_THRESHOLD:
        boosting_log["reason"] = f"Top-2 gap ({top2_gap:.3f}) >= threshold ({TOP2_GAP_THRESHOLD})"
        boosting_log["multiplier"] = 0.0
        return candidates, boosting_log, 0.0
    
    # Gate 4: Boosting Budget 체크
    if remaining_budget <= 0:
        boosting_log["reason"] = f"Budget 부족 (remaining: {remaining_budget:.3f})"
        return candidates, boosting_log, 0.0
    
    try:
        # 안전한 Edge 타입만 조회 (위험한 타입 제외)
        safe_edge_types = [et for et, weight in EDGE_WEIGHTS.items() if weight > 0]
        
        if not safe_edge_types:
            return candidates, boosting_log, 0.0
        
        # 안전한 Edge 조회
        edges = db.query(Edge).filter(
            Edge.source_id == ticker,
            Edge.relation_type.in_(safe_edge_types)
        ).limit(20).all()
        
        if not edges:
            logger.debug(f"[{ticker}] 안전한 KG Edge 없음, 보정 스킵")
            return candidates, boosting_log, 0.0
        
        # 연결된 기업들의 섹터 확인 (Edge 타입별 가중치 적용)
        connected_sectors = {}
        
        for edge in edges:
            edge_type = edge.relation_type
            edge_weight = EDGE_WEIGHTS.get(edge_type, 0.0)
            
            if edge_weight <= 0:
                continue  # 위험한 Edge 타입은 스킵
            
            target_ticker = edge.target_id
            target_sectors = db.query(InvestorSector).filter(
                InvestorSector.ticker == target_ticker,
                InvestorSector.is_primary == True
            ).all()
            
            for sector in target_sectors:
                if sector.major_sector not in connected_sectors:
                    connected_sectors[sector.major_sector] = 0.0
                # Edge 타입별 가중치 적용
                connected_sectors[sector.major_sector] += edge.weight * edge_weight
        
        if not connected_sectors:
            return candidates, boosting_log, 0.0
        
        # Boost 계산
        calculated_boost = BASE_KG_BOOST * multiplier
        
        # Budget 사전 체크
        allowed_boost = min(calculated_boost, remaining_budget)
        
        if allowed_boost <= 0:
            boosting_log["reason"] = f"Budget 부족 (remaining: {remaining_budget:.3f})"
            return candidates, boosting_log, 0.0
        
        # 후보 섹터와 연결 섹터의 교집합에 미세 가산점
        boosted = False
        actual_boost = 0.0
        
        for candidate in candidates:
            sector = candidate['sector']
            if sector in connected_sectors:
                # 범용 섹터는 제외
                if sector in INDUSTRY_AGNOSTIC_SECTORS:
                    continue
                
                # 연결 강도에 비례하여 보정
                connection_strength = min(connected_sectors[sector] / 5.0, 1.0)
                boost = connection_strength * allowed_boost  # Budget 제한된 boost 사용
                
                if boost <= 0:
                    continue
                
                old_score = candidate['score']
                candidate['score'] += boost
                candidate['score'] = min(1.0, candidate['score'])
                actual_boost = max(actual_boost, boost)
                
                # 근거 추가
                current_reasoning = candidate.get('reasoning', '')
                kg_reasoning = f"[KG: {connected_sectors[sector]:.1f}]"
                candidate['reasoning'] = (
                    f"{current_reasoning} {kg_reasoning}"
                    if current_reasoning else f"KG Edge 보정 (+{boost:.3f})"
                ).strip()
                
                logger.info(
                    f"[{ticker}] KG Edge Boosting: {sector} "
                    f"(Score: {old_score:.2f} → {candidate['score']:.2f}, "
                    f"Boost: +{boost:.3f}, Multiplier: {multiplier:.2f})"
                )
                boosted = True
        
        # 점수순 재정렬
        if boosted:
            candidates.sort(key=lambda x: x['score'], reverse=True)
            boosting_log.update({
                "kg_applied": True,
                "multiplier": multiplier,
                "final_boost": actual_boost,
                "reason": f"Top-2 gap ({top2_gap:.3f}) < threshold, KG Edge 발견"
            })
        
        return candidates, boosting_log, actual_boost
        
    except Exception as e:
        logger.warning(f"[{ticker}] KG Edge Boosting 실패: {e}")
        return candidates, boosting_log, 0.0


def get_dynamic_weights(
    rule_conf: str,
    is_complex: bool,
    candidate_count: int,
    bge_used: bool,  # 호환성을 위한 파라미터 (항상 False)
    gpt_used: bool
) -> Dict[str, float]:
    """
    Rule/Embedding/GPT 가중치를 상황에 따라 동적으로 조정
    
    ⭐ BGE-M3 제거: Solar Embedding으로 통합
    - 기존: Rule 40%, KF-DeBERTa 30%, BGE-M3 20%, GPT 10%
    - 변경 후: Rule 40%, Solar Embedding 50%, GPT 10%
    """
    weights = {
        'rule': 0.4,
        'embedding': 0.5,  # ⬆ Solar Embedding (KF-DeBERTa + BGE-M3 통합)
        'bge': 0.0,  # 제거됨
        'gpt': 0.1 if gpt_used else 0.0
    }

    def redistribute(amount: float, targets: List[str]):
        if amount <= 0:
            return
        share = amount / len(targets)
        for target in targets:
            weights[target] += share

    # GPT 비활성화 시 가중치 재분배
    if not gpt_used:
        redistribute(weights.pop('gpt') if 'gpt' in weights else 0.0, ['rule', 'embedding'])
        weights['gpt'] = 0.0

    # Rule confidence 기반 조정
    if rule_conf == 'HIGH':
        weights['rule'] += 0.15
        weights['embedding'] -= 0.10
        weights['gpt'] -= 0.05
    elif rule_conf == 'LOW':
        weights['rule'] -= 0.1
        weights['embedding'] += 0.08
        weights['gpt'] += 0.02

    # 복합기업/후보 수가 많으면 임베딩 비중 강화
    if is_complex or candidate_count > 3:
        weights['embedding'] += 0.05
        weights['rule'] -= 0.05

    # 음수 방지
    for key in weights:
        weights[key] = max(weights[key], 0.0)

    total = sum(weights.values())
    if total == 0:
        return {'rule': 0.5, 'embedding': 0.5, 'bge': 0.0, 'gpt': 0.0}
    for key in weights:
        weights[key] = weights[key] / total
    return weights


def _prepare_company_text_for_embedding(
    company_detail: CompanyDetail,
    company_name: Optional[str] = None
) -> str:
    """
    Step 2 (Solar Embedding) 전용: 전체 텍스트 (4,000 토큰 지원)
    
    ⭐ 512 토큰 제한 제거 - 전체 정보 포함
    Solar Embedding은 4,000 토큰을 지원하므로 전체 텍스트 사용
    
    Args:
        company_detail: CompanyDetail 객체
        company_name: 회사명
    
    Returns:
        전체 텍스트 (압축 없음)
    """
    text_parts = []
    
    if company_name:
        text_parts.append(f"회사명: {company_name}")
    
    # biz_summary 전체 사용 (압축 불필요)
    if company_detail.biz_summary:
        text_parts.append(f"사업 개요: {company_detail.biz_summary}")
    
    # products/keywords 전체 포함 (정렬하여 hash 일관성 보장)
    if company_detail.products:
        products_sorted = sorted([str(p) for p in company_detail.products])
        products_text = ', '.join(products_sorted)
        text_parts.append(f"주요 제품: {products_text}")
    
    if company_detail.keywords:
        keywords_sorted = sorted([str(k) for k in company_detail.keywords])
        keywords_text = ', '.join(keywords_sorted)
        text_parts.append(f"키워드: {keywords_text}")
    
    # 추가 정보 포함 (정렬하여 hash 일관성 보장)
    if company_detail.clients:
        clients_sorted = sorted([str(c) for c in company_detail.clients])
        clients_text = ', '.join(clients_sorted)
        text_parts.append(f"주요 고객사: {clients_text}")
    
    if company_detail.supply_chain:
        # supply_chain은 딕셔너리 리스트이므로 정렬 후 문자열 변환
        supply_items = [
            f"{item.get('item', '')} (공급사: {item.get('supplier', '')})"
            for item in company_detail.supply_chain
        ]
        supply_sorted = sorted(supply_items)
        supply_text = ', '.join(supply_sorted)
        text_parts.append(f"공급망: {supply_text}")
    
    company_text = '\n\n'.join(text_parts)
    
    logger.debug(f"Step 2 최종 텍스트 길이: {len(company_text)}자 (압축 없음, Solar Embedding)")
    return company_text


def _prepare_company_text_for_bge(
    company_detail: CompanyDetail,
    company_name: Optional[str] = None
) -> str:
    """
    Step 3 (BGE-M3) 전용: 전체 텍스트 (8192 토큰 지원)
    
    BGE-M3는 장문을 처리할 수 있으므로 전체 정보 포함
    
    Args:
        company_detail: CompanyDetail 객체
        company_name: 회사명
    
    Returns:
        전체 텍스트
    """
    text_parts = []
    
    if company_name:
        text_parts.append(f"회사명: {company_name}")
    
    # biz_summary 전체 사용 (이미 LLM 요약본)
    if company_detail.biz_summary:
        text_parts.append(f"사업 개요: {company_detail.biz_summary}")
    
    # products/keywords 전체 포함
    if company_detail.products:
        products_text = ', '.join([str(p) for p in company_detail.products[:20]])
        text_parts.append(f"주요 제품: {products_text}")
    
    if company_detail.keywords:
        keywords_text = ', '.join([str(k) for k in company_detail.keywords[:20]])
        text_parts.append(f"키워드: {keywords_text}")
    
    # 추가 정보 (BGE-M3는 장문 지원)
    if company_detail.clients:
        clients_text = ', '.join([str(c) for c in company_detail.clients[:10]])
        text_parts.append(f"주요 고객사: {clients_text}")
    
    if company_detail.supply_chain:
        supply_text = ', '.join([
            f"{item.get('item', '')} (공급사: {item.get('supplier', '')})"
            for item in company_detail.supply_chain[:10]
        ])
        text_parts.append(f"공급망: {supply_text}")
    
    return '\n\n'.join(text_parts)


def _prepare_company_text_for_gpt(
    company_detail: CompanyDetail,
    company_name: Optional[str] = None
) -> str:
    """
    Step 4 (GPT) 전용: 최적화된 정보 (인과 분석용, 2000자)
    
    ⭐ 비용 절감: 핵심 정보만 선별 (30개 → 10개)
    ⭐ 품질 유지: 인과 분석에 필요한 정보는 모두 포함
    
    Args:
        company_detail: CompanyDetail 객체
        company_name: 회사명
    
    Returns:
        최적화된 텍스트 (2000자 이내)
    """
    text_parts = []
    
    if company_name:
        text_parts.append(f"회사명: {company_name}")
    
    # biz_summary: "사업의 개요" 우선 포함 (2000자 제한 고려)
    if company_detail.biz_summary:
        # GPT는 2000자 제한이 있으므로, biz_summary는 800자 정도로 제한
        # 나머지 공간은 products, keywords 등에 할당
        summary = _extract_biz_summary_with_priority(
            company_detail.biz_summary,
            max_chars=800,
            use_semantic_extraction=True
        )
        text_parts.append(f"사업 개요: {summary}")
    
    # ⭐ 최적화: 핵심 정보만 선별 (30개 → 10개)
    # products: 10개 (상위 제품만)
    if company_detail.products:
        products_text = ', '.join([str(p) for p in company_detail.products[:10]])
        text_parts.append(f"주요 제품: {products_text}")
    
    # keywords: 10개 (중복 제거 후 상위)
    if company_detail.keywords:
        # 중복 제거 및 상위 10개만
        unique_keywords = list(dict.fromkeys([str(k) for k in company_detail.keywords]))[:10]
        keywords_text = ', '.join(unique_keywords)
        text_parts.append(f"키워드: {keywords_text}")
    
    # clients: 10개 (주요 고객사만)
    if company_detail.clients:
        clients_text = ', '.join([str(c) for c in company_detail.clients[:10]])
        text_parts.append(f"주요 고객사: {clients_text}")
    
    # supply_chain: 10개 (핵심 공급망만)
    if company_detail.supply_chain:
        supply_text = ', '.join([
            f"{item.get('item', '')} (공급사: {item.get('supplier', '')})"
            for item in company_detail.supply_chain[:10]
        ])
        text_parts.append(f"공급망: {supply_text}")
    
    # raw_materials: 10개 (주요 원재료만)
    if company_detail.raw_materials:
        raw_materials_text = ', '.join([str(rm) for rm in company_detail.raw_materials[:10]])
        text_parts.append(f"원재료: {raw_materials_text}")
    
    company_text = '\n\n'.join(text_parts)
    
    # ⭐ 비용 절감: 2000자로 제한 (기존 3000자에서 감소)
    if len(company_text) > 2000:
        # 의미 기반으로 핵심 부분만 선택
        try:
            company_text = extract_key_sentences_for_sector(
                company_text,
                max_chars=2000,
                min_chars=1500
            )
        except Exception as e:
            logger.warning(f"GPT 텍스트 압축 실패: {e}")
            company_text = company_text[:1997] + "..."
    
    return company_text


def _extract_biz_summary_with_priority(
    biz_summary: str,
    max_chars: int = 500,
    use_semantic_extraction: bool = True
) -> str:
    """
    "사업의 개요" 관련 문단을 우선 포함하여 biz_summary 압축
    
    우선순위:
    1. "사업의 개요" 관련 키워드가 포함된 문단 (최우선)
    2. 의미 기반 핵심 문장 추출 (사용 가능한 경우)
    3. 일반 문단/문장 단위 압축
    
    Args:
        biz_summary: 원본 사업 요약 텍스트
        max_chars: 최대 문자 수
        use_semantic_extraction: 의미 기반 추출 사용 여부
    
    Returns:
        압축된 사업 요약 텍스트
    """
    if not biz_summary:
        return ""
    
    # 이미 max_chars 이하면 그대로 반환
    if len(biz_summary) <= max_chars:
        return biz_summary
    
    # 1. "사업의 개요" 관련 키워드가 포함된 문단 우선 추출
    priority_keywords = [
        '사업의 개요', '지주회사', '영위', '주요 사업', '사업 내용',
        '배당금수익', '임대수익', '로열티', '계열사', '자회사',
        '매출 비중', '사업부문', '부문별 매출'
    ]
    
    # 문단 분리
    import re
    paragraphs = re.split(r'\n\s*\n+', biz_summary)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    
    if not paragraphs:
        # 문단이 없으면 기존 로직 사용
        return _fallback_biz_summary_extraction(biz_summary, max_chars, use_semantic_extraction)
    
    # 우선순위 문단과 일반 문단 분리
    priority_paragraphs = []
    other_paragraphs = []
    
    for para in paragraphs:
        para_lower = para.lower()
        if any(kw in para_lower for kw in priority_keywords):
            priority_paragraphs.append(para)
        else:
            other_paragraphs.append(para)
    
    # 우선 문단 먼저 포함
    selected = []
    remaining_chars = max_chars
    
    # 최대 2개 우선 문단 포함
    for para in priority_paragraphs[:2]:
        if len(para) <= remaining_chars:
            selected.append(para)
            remaining_chars -= len(para) + 2  # \n\n 포함
        elif remaining_chars > 100:
            # 문단 일부만 포함 (문장 단위로 자르기)
            truncated = truncate_to_sentences(para, max_chars=remaining_chars, prefer_paragraphs=False)
            if truncated:
                selected.append(truncated)
                remaining_chars = 0
            break
    
    # 남은 공간에 일반 문단 추가
    if remaining_chars > 100:
        for para in other_paragraphs:
            if len(para) <= remaining_chars:
                selected.append(para)
                remaining_chars -= len(para) + 2
            elif remaining_chars > 50:
                # 문단 일부만 포함
                truncated = truncate_to_sentences(para, max_chars=remaining_chars, prefer_paragraphs=False)
                if truncated:
                    selected.append(truncated)
                    break
            else:
                break
    
    # 결과 조합
    if selected:
        result = '\n\n'.join(selected)
        if len(result) > max_chars:
            result = result[:max_chars].rstrip() + "..."
        return result
    
    # 우선 문단이 없거나 선택 실패 시 Fallback
    return _fallback_biz_summary_extraction(biz_summary, max_chars, use_semantic_extraction)


def _fallback_biz_summary_extraction(
    biz_summary: str,
    max_chars: int = 500,
    use_semantic_extraction: bool = True
) -> str:
    """
    Fallback: 의미 기반 추출 또는 일반 압축
    """
    if use_semantic_extraction and SEMANTIC_EXTRACTION_AVAILABLE:
        try:
            summary = extract_key_sentences_for_sector(
                biz_summary,
                max_chars=max_chars,
                min_chars=300,
                top_n=5,
                keyword_weight=0.4,
                embedding_weight=0.6,
                use_embedding=True
            )
            original_length = len(biz_summary)
            compression_ratio = (len(summary) / original_length * 100) if original_length > 0 else 0
            logger.debug(f"의미 기반 문장 추출 완료: {len(summary)}자 (압축률: {compression_ratio:.1f}%)")
            return summary
        except Exception as e:
            logger.warning(f"의미 기반 문장 추출 실패, Fallback 사용: {e}")
    
    # 최종 Fallback: 문단/문장 단위 자르기
    return truncate_to_sentences(
        biz_summary,
        max_chars=max_chars,
        prefer_paragraphs=True
    )


def _prepare_company_text(
    company_detail: CompanyDetail,
    company_name: Optional[str] = None,
    use_semantic_extraction: bool = True
) -> str:
    """
    회사 텍스트 준비 (biz_summary + products + keywords)
    
    ⚠️ Deprecated: 단계별 함수 사용 권장
    - Step 2: _prepare_company_text_for_embedding()
    - Step 3: _prepare_company_text_for_bge()
    - Step 4: _prepare_company_text_for_gpt()
    
    하위 호환성을 위해 유지 (기존 코드 호환)
    """
    text_parts = []
    
    if company_name:
        text_parts.append(f"회사명: {company_name}")
    
    if company_detail.biz_summary:
        # 🆕 "사업의 개요" 우선 포함 로직
        summary = _extract_biz_summary_with_priority(
            company_detail.biz_summary,
            use_semantic_extraction=use_semantic_extraction
        )
        text_parts.append(f"사업 개요: {summary}")
    
    if company_detail.products:
        products_text = ', '.join([str(p) for p in company_detail.products[:20]])
        text_parts.append(f"주요 제품: {products_text}")
    
    if company_detail.keywords:
        keywords_text = ', '.join([str(k) for k in company_detail.keywords[:20]])
        text_parts.append(f"키워드: {keywords_text}")
    
    return '\n\n'.join(text_parts)


def classify_sector_ensemble(
    db: Session,
    ticker: str,
    llm_handler: Optional[LLMHandler] = None,
    use_gpt: bool = True,
    use_embedding: bool = True,
    use_reranking: bool = True,
    max_sectors: int = 3,
    use_semantic_extraction: bool = True  # Phase 1 최적화: 의미 기반 핵심 문장 추출
) -> Optional[List[Dict[str, Any]]]:
    """
    4단계 멀티 모델 앙상블 섹터 분류
    
    Args:
        db: DB 세션
        ticker: 종목코드
        llm_handler: LLMHandler 객체 (GPT 사용 시)
        use_gpt: GPT 최종 검증 사용 여부
        use_embedding: 임베딩 모델 기반 후보 생성 사용 여부
        use_reranking: BGE-M3 Re-ranking 사용 여부
        max_sectors: 최대 섹터 개수 (기본값: 3)
        use_semantic_extraction: Phase 1 최적화 사용 여부 (기본값: True)
                                 의미 기반 핵심 문장 추출로 biz_summary 압축
    
    Returns:
        [
            {
                'major_sector': 'SEC_SEMI',
                'sub_sector': 'MEMORY',
                'value_chain': 'MIDSTREAM',
                'weight': 0.6,
                'is_primary': True,
                'confidence': 'HIGH',
                'method': 'ENSEMBLE',
                'rule_score': 0.95,
                'embedding_score': 0.85,
                'bge_score': 0.82,
                'gpt_score': 0.9,
                'ensemble_score': 0.88,
                'reasoning': 'GPT가 제공한 근거...'
            },
            ...
        ]
    """
    # CompanyDetail 조회
    from app.utils.stock_query import get_stock_by_ticker_safe
    company_detail = db.query(CompanyDetail).filter(
        CompanyDetail.ticker == ticker
    ).first()
    
    if not company_detail:
        logger.warning(f"[{ticker}] CompanyDetail 데이터 없음")
        return None
    
    stock = get_stock_by_ticker_safe(db, ticker)
    company_name = stock.stock_name if stock else None
    
    # ========================================================================
    # Step 0: KRX 업종 Prior (신규 추가) ⭐
    # ========================================================================
    logger.info(f"[{ticker}] Step 0: KRX 업종 Prior 시작")
    krx_major = None
    krx_sub = None
    krx_confidence = "LOW"
    try:
        from app.services.hierarchical_sector_classifier import classify_sector_step1_krx
        krx_major, krx_sub, krx_confidence = classify_sector_step1_krx(db, ticker)
        if krx_major:
            logger.info(f"[{ticker}] KRX 업종 Prior: {krx_major}/{krx_sub} (confidence: {krx_confidence})")
        else:
            logger.debug(f"[{ticker}] KRX 업종 정보 없음")
    except Exception as e:
        logger.warning(f"[{ticker}] KRX 업종 Prior 조회 실패: {e}")
    
    # ========================================================================
    # Step 1: Rule-based Classification (가중치 40%)
    # ========================================================================
    logger.info(f"[{ticker}] Step 1: Rule-based 분류 시작")
    
    rule_major, rule_sub, rule_vc, rule_conf, _ = classify_sector_rule_based(
        company_detail, company_name
    )
    
    rule_score_map = {'HIGH': 1.0, 'MEDIUM': 0.7, 'LOW': 0.4}
    rule_score = rule_score_map.get(rule_conf, 0.4)
    
    # 기존 섹터 코드를 새 섹터 코드로 매핑 (Rule-based 결과도)
    if rule_major and rule_major in LEGACY_SECTOR_MAPPING:
        mapped_major = LEGACY_SECTOR_MAPPING[rule_major]
        logger.info(f"[{ticker}] Rule-based 섹터 매핑: {rule_major} → {mapped_major}")
        rule_major = mapped_major
    
    # Step 0 결과와 Step 1 결과 비교 (KRX-Rule 일치 여부)
    if krx_major and rule_major:
        if krx_major == rule_major:
            # 일치 → Confidence 보너스
            rule_score += 0.1
            rule_score = min(1.0, rule_score)
            logger.info(f"[{ticker}] KRX-Rule 일치 → Confidence 보너스 (+0.1, 최종: {rule_score:.2f})")
        else:
            # 불일치 → KRX 섹터를 후보에 추가할 예정 (Step 2에서 처리)
            logger.info(f"[{ticker}] KRX-Rule 불일치 (KRX: {krx_major}, Rule: {rule_major})")
    
    # 밸류체인 분류 (하이브리드 방식)
    value_chain_results = None
    if VALUE_CHAIN_CLASSIFIER_AVAILABLE and rule_major:
        try:
            value_chain_results = classify_value_chain_hybrid(
                company_detail,
                rule_major,  # 섹터 코드 사용
                company_name,
                use_ensemble=True
            )
            if value_chain_results:
                # Primary value chain 사용
                rule_vc = value_chain_results[0].get('value_chain', rule_vc)
                logger.debug(f"[{ticker}] 하이브리드 밸류체인 분류: {rule_vc}")
        except Exception as e:
            logger.warning(f"[{ticker}] 밸류체인 분류 실패, 기존 방식 사용: {e}")
    
    # Confidence HIGH면 즉시 반환 (가중치 40% 충분)
    if rule_conf == "HIGH" and rule_score >= 0.9:
        logger.info(f"[{ticker}] Rule-based HIGH confidence → 즉시 반환")
        return [{
            'major_sector': rule_major,
            'sub_sector': rule_sub,
            'value_chain': rule_vc,
            'weight': 1.0,
            'is_primary': True,
            'confidence': rule_conf,
            'method': 'RULE_BASED',
            'rule_score': rule_score,
            'embedding_score': None,
            'bge_score': None,
            'gpt_score': None,
            'ensemble_score': rule_score,
            'reasoning': f'Rule-based 키워드 매칭으로 HIGH confidence ({rule_score:.2f})'
        }]
    
    # ========================================================================
    # Step 2: 임베딩 모델 기반 후보 생성기 (가중치 30%)
    # ========================================================================
    if not use_embedding or not EMBEDDING_AVAILABLE:
        logger.info(f"[{ticker}] 임베딩 모델 스킵 (use_embedding={use_embedding}, available={EMBEDDING_AVAILABLE})")
        candidates = []
    else:
        logger.info(f"[{ticker}] Step 2: 임베딩 모델 후보 생성 시작")
        
        try:
            # ⭐ Step 2 전용: 최적화된 400자 텍스트 사용
            company_text = _prepare_company_text_for_embedding(
                company_detail, 
                company_name
            )
            candidates = generate_sector_candidates(
                company_text,
                top_k=5,
                min_threshold=0.3,  # 0.4 → 0.3으로 낮춤 (더 많은 후보 생성)
                model_name=None,
                db=db,
                ticker=ticker,
                force_regenerate=False
            )
            
            if candidates:
                logger.info(f"[{ticker}] 임베딩 모델 후보 생성 완료: {len(candidates)}개 (텍스트 길이: {len(company_text)}자)")
            else:
                logger.warning(f"[{ticker}] 임베딩 모델 후보 생성 실패 또는 후보 없음 (텍스트 길이: {len(company_text) if company_text else 0}자)")
        except Exception as e:
            logger.error(f"[{ticker}] 임베딩 모델 후보 생성 중 오류: {e}", exc_info=True)
            candidates = []
            
    # Step 2.5: Anchor Boosting (전략 B) - Budget 상태 관리 방식
    # 동점 깨기만 허용, 범용 섹터 완전 금지, Role Gate 감쇠 적용
    top2_gap = None
    if len(candidates) >= 2:
        top2_gap = candidates[0]['score'] - candidates[1]['score']
    
    # Budget 상태 초기화
    remaining_budget = MAX_TOTAL_BOOST
    
    # Anchor Boosting 단계
    candidates, anchor_boosting_log, anchor_boost = apply_anchor_boosting(
        candidates, 
        company_detail, 
        company_name,
        top2_gap=top2_gap,
        remaining_budget=remaining_budget
    )
    
    # Budget 업데이트
    remaining_budget -= anchor_boost
    
    # Role Gate에서 계산된 multiplier 추출 (KG Boosting에서 재사용)
    multiplier = anchor_boosting_log.get('multiplier', 1.0)
    
    # Step 2.6: KG Edge Boosting (신규) - Budget 상태 관리 방식
    # 안전한 Edge 타입만 사용, Edge 타입별 가중치 차등, Boosting Budget 준수
    candidates, kg_boosting_log, kg_boost = apply_kg_edge_boosting(
        candidates, 
        ticker, 
        db,
        company_detail,
        company_name,
        top2_gap=top2_gap,
        remaining_budget=remaining_budget,
        multiplier=multiplier
    )
    
    # Budget 업데이트
    remaining_budget -= kg_boost
    
    # 총 적용된 boost 계산
    total_boost = anchor_boost + kg_boost
    
    # Boosting 로그 통합 (최종 결과에 포함)
    boosting_info = {
        "anchor_applied": anchor_boosting_log.get('anchor_applied', False),
        "kg_applied": kg_boosting_log.get('kg_applied', False),
        "reason": anchor_boosting_log.get('reason', '') or kg_boosting_log.get('reason', ''),
        "multiplier": multiplier,
        "final_boost": total_boost
    }
    
    # Rule-based 결과가 있으면 후보에 추가
    if rule_major and rule_score >= 0.4:
        # 기존 섹터 코드를 새 섹터 코드로 매핑
        mapped_sector = LEGACY_SECTOR_MAPPING.get(rule_major, rule_major)
        if mapped_sector != rule_major:
            logger.info(f"[{ticker}] Rule-based 섹터 매핑: {rule_major} → {mapped_sector}")
            rule_major = mapped_sector
        
        # 이미 후보에 있는지 확인
        existing = next((c for c in candidates if c['sector'] == rule_major), None)
        if not existing:
            candidates.append({
                'sector': rule_major,
                'score': rule_score
            })
        else:
            # Rule-based 점수와 평균
            existing['score'] = (existing['score'] + rule_score) / 2
    
    # Step 0 결과: KRX-Rule 불일치 시 KRX 섹터를 후보에 추가
    if krx_major and rule_major and krx_major != rule_major:
        # KRX 섹터가 후보에 없는 경우만 추가
        existing_krx = next((c for c in candidates if c['sector'] == krx_major), None)
        if not existing_krx:
            krx_score = 0.3  # 낮은 점수로 후보에만 추가
            candidates.append({
                'sector': krx_major,
                'score': krx_score,
                'reasoning': f'KRX 업종 기반 ({krx_confidence})'
            })
            logger.info(f"[{ticker}] KRX 섹터를 후보에 추가: {krx_major} (score: {krx_score})")
    
    if not candidates:
        logger.warning(f"[{ticker}] 후보 섹터 없음. Rule-based 결과 반환")
        if rule_major:
            return [{
                'major_sector': rule_major,
                'sub_sector': rule_sub,
                'value_chain': rule_vc,
                'weight': 1.0,
                'is_primary': True,
                'confidence': rule_conf,
                'method': 'RULE_BASED',
                'rule_score': rule_score,
                'ensemble_score': rule_score,
                'reasoning': 'Rule-based 분류 (후보 생성 실패)'
            }]
        return None
    
    # ========================================================================
    # Step 3: BGE-M3 Re-ranking 제거 (Solar Embedding으로 통합)
    # ========================================================================
    # ⭐ BGE-M3 제거: Solar Embedding이 충분히 정확하므로 Re-ranking 불필요
    # Step 2 결과에서 Top-2 직접 선택
    logger.info(f"[{ticker}] Step 3: BGE-M3 Re-ranking 제거 (Solar Embedding 사용)")
    reranked_candidates = candidates[:2]  # Top-2 직접 선택
    
    if not reranked_candidates:
        logger.warning(f"[{ticker}] Re-ranking 결과 없음. Rule-based 또는 원본 후보 사용")
        # Re-ranking 실패 시 원본 후보 또는 Rule-based 결과 사용
        if candidates:
            # 원본 후보가 있으면 사용
            reranked_candidates = candidates[:2]  # top_k 변수 대신 2 사용
        elif rule_major:
            # Rule-based 결과만 사용
            return [{
                'major_sector': rule_major,
                'sub_sector': rule_sub,
                'value_chain': rule_vc,
                'weight': 1.0,
                'is_primary': True,
                'confidence': rule_conf,
                'method': 'RULE_BASED',
                'rule_score': rule_score,
                'ensemble_score': rule_score,
                'reasoning': 'Rule-based 분류 (Re-ranking 실패)'
            }]
        else:
            logger.error(f"[{ticker}] 모든 분류 방법 실패")
            return None
    
    # ========================================================================
    # Step 3.5: Sub-sector 분류 (신규 추가) ⭐
    # ========================================================================
    # Major Top-2 후보별로 각각 Sub-sector 결정
    logger.info(f"[{ticker}] Step 3.5: Sub-sector 분류 시작")
    
    def classify_sub_sector(
        major_sector: str,
        company_detail: CompanyDetail,
        company_name: Optional[str] = None
    ) -> Optional[str]:
        """
        Major Sector 기반 Sub-sector 분류
        
        Args:
            major_sector: Major Sector 코드
            company_detail: CompanyDetail 객체
            company_name: 회사명
        
        Returns:
            Sub-sector 코드 또는 None
        """
        from app.models.sector_reference import SUB_SECTOR_DEFINITIONS
        
        if major_sector not in SUB_SECTOR_DEFINITIONS:
            return None
        
        sub_sectors = SUB_SECTOR_DEFINITIONS[major_sector]
        sub_sector_scores = {}
        
        # 텍스트 준비
        text_parts = []
        if company_detail.biz_summary:
            text_parts.append(company_detail.biz_summary.lower())
        if company_detail.products:
            products_text = ' '.join([str(p) for p in company_detail.products]).lower()
            text_parts.append(products_text)
        if company_detail.keywords:
            keywords_text = ' '.join([str(k) for k in company_detail.keywords]).lower()
            text_parts.append(keywords_text)
        
        combined_text = ' '.join(text_parts)
        
        # 각 Sub-sector별 키워드 매칭 점수 계산
        for sub_code, sub_def in sub_sectors.items():
            score = 0
            keywords = sub_def.get('keywords', [])
            for keyword in keywords:
                if keyword.lower() in combined_text:
                    score += 1
            
            if score > 0:
                sub_sector_scores[sub_code] = score
        
        # 최고 점수 Sub-sector 반환
        if sub_sector_scores:
            best_sub = max(sub_sector_scores.items(), key=lambda x: x[1])
            if best_sub[1] > 0:
                logger.debug(f"Sub-sector 분류: {major_sector} → {best_sub[0]} (점수: {best_sub[1]})")
                return best_sub[0]
        
        return None
    
    # Top-2 후보별로 Sub-sector 분류 (Step 3.5 최종 확정)
    # ⭐ 중요: 이 결과가 최종 Sub-sector이며, GPT는 변경할 수 없음
    sub_sector_final_map = {}  # {major_sector: sub_sector} 매핑 저장
    for candidate in reranked_candidates[:2]:
        major_sector = candidate.get('sector')
        if major_sector:
            sub_sector = classify_sub_sector(major_sector, company_detail, company_name)
            if sub_sector:
                candidate['sub_sector'] = sub_sector
                sub_sector_final_map[major_sector] = sub_sector  # 최종 확정값 저장
                logger.info(f"[{ticker}] Step 3.5 최종 확정: {major_sector} → Sub-sector: {sub_sector}")
    
    # ========================================================================
    # Step 5: Confidence 기반 Fallback 정교화 (개선) ⭐
    # ========================================================================
    # 3단계 분리:
    # - ≥ 0.90: 즉시 확정 (GPT 스킵)
    # - 0.70 ~ 0.90: Sub-sector만 수행 (GPT 스킵, BGE-M3 제거됨)
    # - < 0.70: GPT Deep Validation
    should_use_gpt = use_gpt and GPT_AVAILABLE and llm_handler
    should_use_bge_final = False  # BGE-M3 제거됨 (Solar Embedding으로 통합)
    
    if should_use_gpt:
        if rule_score >= 0.90:
            # 즉시 확정 (GPT 스킵)
            logger.info(f"[{ticker}] Rule confidence 매우 높음 ({rule_score:.2f}) → 즉시 확정 (GPT 스킵)")
            should_use_gpt = False
            should_use_bge_final = False
        elif rule_score >= 0.70:
            # Sub-sector만 수행 (GPT 스킵, BGE-M3 제거됨)
            logger.info(f"[{ticker}] Rule confidence 중간 ({rule_score:.2f}) → Sub-sector만 수행 (GPT 스킵)")
            should_use_gpt = False
            should_use_bge_final = False
        else:
            # GPT Deep Validation
            logger.info(f"[{ticker}] Rule confidence 낮음 ({rule_score:.2f}) → GPT Deep Validation")
            should_use_gpt = True
            should_use_bge_final = False  # BGE-M3 제거됨
    
    # ========================================================================
    # Step 4: GPT 최종 검증 (가중치 10%, 조건부 사용)
    # ========================================================================
    
    # 복잡도 계산
    try:
        is_complex = is_complex_company(company_detail, company_name)
    except Exception:
        is_complex = False
    
    dynamic_weights = get_dynamic_weights(
        rule_conf=rule_conf,
        is_complex=is_complex,
        candidate_count=len(candidates),
        bge_used=should_use_bge_final,
        gpt_used=should_use_gpt
    )

    if not should_use_gpt:
        logger.info(f"[{ticker}] GPT 최종 검증 스킵")
        # GPT 없이 BGE-M3 결과로 최종 결정 (Step 3.5 Sub-sector 사용)
        final_sectors = _create_result_from_candidates(
            reranked_candidates,
            rule_major,
            rule_sub,
            rule_vc,
            rule_score,
            max_sectors,
            dynamic_weights
        )
        
        # Step 3.5 Sub-sector를 최종 결과에 반영
        for fs in final_sectors:
            sector_code = fs.get('major_sector')
            if sector_code and sector_code in sub_sector_final_map:
                fs['sub_sector'] = sub_sector_final_map[sector_code]
                logger.debug(f"[{ticker}] Step 3.5 Sub-sector 반영: {sector_code} → {fs['sub_sector']}")
    else:
        logger.info(f"[{ticker}] Step 4: GPT 최종 검증 시작")
        
        # ⭐ Step 4 전용: 전체 정보 포함 텍스트 사용 (3000자, 인과 분석용)
        company_text = _prepare_company_text_for_gpt(
            company_detail, 
            company_name
        )
        validated_sectors = validate_sectors_with_gpt(
            company_text,
            company_name,
            reranked_candidates,
            llm_handler,
            max_sectors=max_sectors,
            company_detail=company_detail  # 인과 분석용 추가
        )
        
        if validated_sectors:
            # GPT 결과를 최종 형식으로 변환
            final_sectors = []
            for vs in validated_sectors:
                sector_code = vs.get('sector')
                if not sector_code:
                    logger.warning(f"[{ticker}] GPT 검증 결과에 섹터 코드 없음: {vs}")
                    continue
                
                # ⭐ Sub-sector 결정: Step 3.5 결과만 사용 (GPT는 변경 불가)
                # GPT는 검증 및 인과 분석만 수행하며, Sub-sector를 생성하거나 변경하지 않음
                sub_sector = (
                    sub_sector_final_map.get(sector_code) or  # Step 3.5 최종 확정값 (최우선)
                    next((c.get('sub_sector') for c in reranked_candidates if c.get('sector') == sector_code), None) or  # Fallback: Step 3.5 결과
                    (rule_sub if rule_major == sector_code else None)  # Fallback: Rule-based
                )
                
                # GPT가 Sub-sector를 제안했는지 확인 (로깅용, 사용하지 않음)
                gpt_suggested_sub = vs.get('sub_sector') or vs.get('sub_sector_suggestion')
                if gpt_suggested_sub and gpt_suggested_sub != sub_sector:
                    logger.debug(f"[{ticker}] GPT가 Sub-sector 제안: {gpt_suggested_sub} (무시됨, Step 3.5 결과 사용: {sub_sector})")
                
                # Value Chain 결정 (Rule-based 또는 기본값)
                value_chain = rule_vc if rule_major == sector_code else None
                
                # Confidence 결정
                ensemble_score = (
                    vs.get('embedding_score', 0.0) * dynamic_weights.get('embedding', 0.0) +
                    vs.get('bge_score', 0.0) * dynamic_weights.get('bge', 0.0) +
                    vs.get('gpt_score', 0.0) * dynamic_weights.get('gpt', 0.0) +
                    (rule_score if rule_major == sector_code else 0.0) * dynamic_weights.get('rule', 0.0)
                )
                
                if ensemble_score >= 0.8:
                    confidence = "HIGH"
                elif ensemble_score >= 0.6:
                    confidence = "MEDIUM"
                else:
                    confidence = "LOW"
                
                final_sectors.append({
                    'major_sector': sector_code,
                    'sub_sector': sub_sector,
                    'value_chain': value_chain,
                    'weight': vs.get('weight', 0.5),
                    'is_primary': vs.get('is_primary', False),
                    'confidence': confidence,
                    'method': 'ENSEMBLE',
                    'rule_score': rule_score if rule_major == sector_code else None,
                    'embedding_score': vs.get('embedding_score', 0.0),
                    'bge_score': vs.get('bge_score', 0.0),
                    'gpt_score': vs.get('gpt_score', 0.0),
                    'ensemble_score': ensemble_score,
                    'reasoning': vs.get('reasoning', ''),
                    # ⭐ 인과 구조 분석 결과 (레벨 2)
                    'causal_structure': vs.get('causal_structure'),
                    'investment_insights': vs.get('investment_insights')
                })
            
            if final_sectors:
                logger.info(f"[{ticker}] GPT 최종 검증 완료: {len(final_sectors)}개 섹터")
                
                # ========================================================================
                # Step 4.5: Granular Tags 필터링 + Exposure Drivers 추출 ⭐
                # ========================================================================
                logger.info(f"[{ticker}] Step 4.5: Granular Tags 필터링 및 Exposure Drivers 추출 시작")
                
                # 각 섹터별로 Granular Tags 필터링 및 Exposure Drivers 추출 (Step 3.5 Sub-sector 기준)
                for fs in final_sectors:
                    sector_code = fs.get('major_sector')
                    sub_sector_code = fs.get('sub_sector')  # ⭐ Step 3.5에서 확정한 Sub-sector
                    causal_structure = fs.get('causal_structure')
                    
                    if sector_code:
                        # ⭐ Granular Tags 필터링
                        if causal_structure and 'granular_tags' in causal_structure:
                            gpt_granular_tags = causal_structure.get('granular_tags', [])
                            filtered_tags = filter_granular_tags_by_sub_sector(
                                gpt_granular_tags,
                                sector_code,
                                sub_sector_code,
                                ticker=ticker  # 로깅용
                            )
                            # 필터링된 태그로 업데이트
                            causal_structure['granular_tags'] = filtered_tags
                            fs['causal_structure'] = causal_structure
                            
                            if len(filtered_tags) < len(gpt_granular_tags):
                                logger.info(f"[{ticker}] Granular 태그 필터링: {len(gpt_granular_tags)}개 → {len(filtered_tags)}개 (Sub-sector: {sub_sector_code})")
                        
                        # ⭐ Exposure Drivers 분리 추출
                        exposure_drivers, supporting_drivers = extract_exposure_drivers(
                            sector_code,
                            sub_sector_code,  # Step 3.5 확정값 전달
                            causal_structure,
                            company_detail
                        )
                        
                        if exposure_drivers:
                            fs['exposure_drivers'] = exposure_drivers
                            logger.debug(f"[{ticker}] {sector_code}/{sub_sector_code} → Exposure Drivers: {len(exposure_drivers)}개 (표준 드라이버)")
                        
                        if supporting_drivers:
                            fs['supporting_drivers'] = supporting_drivers
                            logger.debug(f"[{ticker}] {sector_code} → Supporting Drivers: {len(supporting_drivers)}개 (GPT 설명용)")
            else:
                logger.warning(f"[{ticker}] GPT 검증 결과 변환 실패. BGE-M3 결과 사용")
                final_sectors = _create_result_from_candidates(
                    reranked_candidates,
                    rule_major,
                    rule_sub,
                    rule_vc,
                    rule_score,
                    max_sectors,
                    dynamic_weights
                )
                
                # Step 3.5 Sub-sector를 최종 결과에 반영
                for fs in final_sectors:
                    sector_code = fs.get('major_sector')
                    if sector_code and sector_code in sub_sector_final_map:
                        fs['sub_sector'] = sub_sector_final_map[sector_code]
                        logger.debug(f"[{ticker}] Step 3.5 Sub-sector 반영 (변환 실패 Fallback): {sector_code} → {fs['sub_sector']}")
        else:
            # GPT 검증 실패 시 Fallback (Step 3.5 Sub-sector 사용)
            logger.warning(f"[{ticker}] GPT 검증 실패. BGE-M3 결과 사용")
            final_sectors = _create_result_from_candidates(
                reranked_candidates,
                rule_major,
                rule_sub,
                rule_vc,
                rule_score,
                max_sectors,
                dynamic_weights
            )
            
            # Step 3.5 Sub-sector를 최종 결과에 반영
            for fs in final_sectors:
                sector_code = fs.get('major_sector')
                if sector_code and sector_code in sub_sector_final_map:
                    fs['sub_sector'] = sub_sector_final_map[sector_code]
                    logger.debug(f"[{ticker}] Step 3.5 Sub-sector 반영 (Fallback): {sector_code} → {fs['sub_sector']}")
    
    # 최종 결과 검증 및 NULL 섹터 구출
    if not final_sectors:
        logger.error(f"[{ticker}] 최종 섹터 결과가 비어있음")
        
        # Fallback 1: Rule-based 결과 사용
        if rule_major:
            logger.warning(f"[{ticker}] Rule-based 결과를 최종 결과로 사용")
            fallback_result = [{
                'major_sector': rule_major,
                'sub_sector': rule_sub,
                'value_chain': rule_vc,
                'sector_l1': rule_major,
                'sector_l2': rule_sub,
                'weight': 1.0,
                'is_primary': True,
                'confidence': rule_conf,
                'method': 'RULE_BASED',
                'fallback_used': 'TRUE',  # ⭐ Fallback 사용 여부 (VARCHAR에 문자열 저장)
                'fallback_type': 'RULE',  # ⭐ Fallback 타입
                'rule_score': rule_score,
                'ensemble_score': rule_score,
                'reasoning': 'Rule-based 분류 (Fallback)',
                'boosting_log': boosting_info  # Boosting 로그 포함
            }]
            return fallback_result
        
        # Fallback 2: Candidates에서 Top-1 사용 (Ensemble Score 0.3 이상)
        if candidates and len(candidates) > 0:
            top_candidate = candidates[0]
            top_score = top_candidate.get('score', 0.0)
            
            if top_score >= 0.3:  # 최소 임계값
                logger.warning(f"[{ticker}] Candidates Top-1을 최종 결과로 사용 (score: {top_score:.3f}, VERY_LOW confidence)")
                fallback_result = [{
                    'major_sector': top_candidate.get('sector'),
                    'sub_sector': None,
                    'value_chain': None,
                    'sector_l1': top_candidate.get('sector'),
                    'sector_l2': None,
                    'weight': 1.0,
                    'is_primary': True,
                    'confidence': 'VERY_LOW',  # ⭐ 새로운 confidence 레벨
                    'method': 'ENSEMBLE_FALLBACK',
                    'fallback_used': 'TRUE',  # ⭐ Fallback 사용 여부 (VARCHAR에 문자열 저장)
                    'fallback_type': 'TOP1',  # ⭐ Fallback 타입
                    'rule_score': None,
                    'embedding_score': top_score,
                    'ensemble_score': top_score,
                    'reasoning': f'Ensemble Fallback: Top-1 candidate (score: {top_score:.3f})',
                    'boosting_log': boosting_info
                }]
                return fallback_result
        
        # Fallback 3: KRX 섹터 사용
        if krx_major:
            logger.warning(f"[{ticker}] KRX 섹터를 최종 결과로 사용 (VERY_LOW confidence)")
            fallback_result = [{
                'major_sector': krx_major,
                'sub_sector': krx_sub,
                'value_chain': None,
                'sector_l1': krx_major,
                'sector_l2': krx_sub,
                'weight': 1.0,
                'is_primary': True,
                'confidence': 'VERY_LOW',
                'method': 'KRX_FALLBACK',
                'fallback_used': 'TRUE',  # ⭐ Fallback 사용 여부 (VARCHAR에 문자열 저장)
                'fallback_type': 'KRX',  # ⭐ Fallback 타입
                'rule_score': None,
                'ensemble_score': 0.3,  # 기본값
                'reasoning': f'KRX 섹터 Fallback: {krx_major}/{krx_sub}',
                'boosting_log': boosting_info
            }]
            return fallback_result
        
        # 최종 Fallback: 일반적인 섹터 할당 (UNKNOWN)
        logger.error(f"[{ticker}] 모든 Fallback 실패, UNKNOWN 섹터 할당")
        return [{
            'major_sector': 'SEC_UNKNOWN',
            'sub_sector': None,
            'value_chain': None,
            'sector_l1': 'SEC_UNKNOWN',
            'sector_l2': None,
            'weight': 1.0,
            'is_primary': True,
            'confidence': 'VERY_LOW',
            'method': 'FALLBACK_UNKNOWN',
            'fallback_used': 'TRUE',  # ⭐ Fallback 사용 여부 (VARCHAR에 문자열 저장)
            'fallback_type': 'UNKNOWN',  # ⭐ Fallback 타입
            'rule_score': None,
            'ensemble_score': 0.0,
            'reasoning': '모든 분류 방법 실패, UNKNOWN 섹터 할당',
            'boosting_log': boosting_info
        }]
    
    # ⭐ NEW: final_sectors가 있지만 NULL 섹터인 경우 처리
    if final_sectors and len(final_sectors) > 0:
        primary_result = next((r for r in final_sectors if r.get('is_primary')), final_sectors[0])
        
        # ⭐ 핵심: sector_l1이 NULL이면 강제 Fallback
        if not primary_result.get('sector_l1') and not primary_result.get('major_sector'):
            logger.warning(f"[{ticker}] ⚠️ Primary 섹터가 NULL, 강제 Fallback 실행")
            
            # 확정적 규칙 적용
            primary_result['sector_l1'] = 'SEC_UNKNOWN'
            primary_result['major_sector'] = 'SEC_UNKNOWN'
            primary_result['fallback_used'] = 'TRUE'  # ⭐ VARCHAR에 문자열 저장
            primary_result['fallback_type'] = 'UNKNOWN'  # ⭐ 타입 분리
            primary_result['confidence'] = 'VERY_LOW'
            primary_result['method'] = 'FALLBACK_UNKNOWN'
            primary_result['ensemble_score'] = 0.0
            primary_result['reasoning'] = 'NULL 섹터 감지, UNKNOWN 할당'
            
            logger.info(f"[{ticker}] ✅ NULL 섹터 → SEC_UNKNOWN 할당 완료")
    
    # Boosting 로그를 첫 번째 섹터에 추가 (메타데이터)
    if final_sectors and boosting_info:
        final_sectors[0]['boosting_log'] = boosting_info
        logger.info(
            f"[{ticker}] Boosting 로그: "
            f"anchor_applied={boosting_info.get('anchor_applied')}, "
            f"kg_applied={boosting_info.get('kg_applied')}, "
            f"multiplier={boosting_info.get('multiplier'):.2f}, "
            f"final_boost={boosting_info.get('final_boost'):.3f}, "
            f"reason={boosting_info.get('reason')}"
        )
    
    return final_sectors
    
    # Boosting 로그를 첫 번째 섹터에 추가 (메타데이터)
    if final_sectors and boosting_info:
        final_sectors[0]['boosting_log'] = boosting_info
        logger.info(
            f"[{ticker}] Boosting 로그: "
            f"anchor_applied={boosting_info.get('anchor_applied')}, "
            f"kg_applied={boosting_info.get('kg_applied')}, "
            f"multiplier={boosting_info.get('multiplier'):.2f}, "
            f"final_boost={boosting_info.get('final_boost'):.3f}, "
            f"reason={boosting_info.get('reason')}"
        )
    
    return final_sectors


def filter_granular_tags_by_sub_sector(
    granular_tags: List[str],
    major_sector: str,
    sub_sector: Optional[str],
    ticker: Optional[str] = None  # 로깅용 (선택적)
) -> List[str]:
    """
    GPT가 생성한 granular_tags를 Sub-sector 기준으로 필터링
    
    ⭐ 중요: Sub-sector와 일치하지 않는 태그는 제거하여 논리 충돌 방지
    
    Args:
        granular_tags: GPT가 생성한 태그 리스트
        major_sector: Major Sector 코드
        sub_sector: Step 3.5에서 확정한 Sub-sector 코드
        ticker: 종목코드 (로깅용, 선택적)
    
    Returns:
        필터링된 태그 리스트 (최대 5개)
    """
    from app.models.sector_reference import SUB_SECTOR_DEFINITIONS
    
    if not granular_tags or not sub_sector:
        return []
    
    # Sub-sector의 keywords를 기반으로 허용 태그 생성
    allowed_keywords = []
    if major_sector in SUB_SECTOR_DEFINITIONS:
        if sub_sector in SUB_SECTOR_DEFINITIONS[major_sector]:
            sub_def = SUB_SECTOR_DEFINITIONS[major_sector][sub_sector]
            # keywords를 태그로 사용 (대소문자 무시)
            allowed_keywords = [k.upper() for k in sub_def.get('keywords', [])]
    
    # GPT 태그를 허용 태그와 매칭 (부분 매칭)
    filtered_tags = []
    for tag in granular_tags:
        tag_upper = str(tag).upper()
        # 허용 태그와 부분 매칭 확인
        matched = False
        for allowed in allowed_keywords:
            if allowed in tag_upper or tag_upper in allowed:
                filtered_tags.append(tag)
                matched = True
                break
        
        # 매칭되지 않으면 로깅 (디버깅용)
        if not matched and ticker:
            logger.debug(f"[{ticker}] Granular 태그 필터링됨: '{tag}' (Sub-sector: {sub_sector}와 불일치)")
    
    return filtered_tags[:5]  # 최대 5개로 제한


def extract_exposure_drivers(
    sector_code: str,
    sub_sector_code: Optional[str],  # ⭐ Step 3.5에서 확정한 Sub-sector
    causal_structure: Optional[Dict],
    company_detail: CompanyDetail
) -> Tuple[List[Dict], List[Dict]]:
    """
    섹터의 표준 드라이버와 GPT 결과를 분리하여 반환
    
    ⭐ 중요: 표준 드라이버와 GPT 설명용 드라이버를 분리
    - exposure_drivers: 표준 드라이버만 (정확한 경제 변수)
    - supporting_drivers: GPT 설명용 (일반적인 설명)
    
    Args:
        sector_code: Major Sector 코드
        sub_sector_code: Step 3.5에서 확정한 Sub-sector 코드
        causal_structure: GPT가 생성한 causal_structure (key_drivers 포함, 설명용)
        company_detail: CompanyDetail 객체
    
    Returns:
        (exposure_drivers: 표준 드라이버, supporting_drivers: GPT 설명용)
    """
    from app.models.sector_reference import (
        SUB_SECTOR_DEFINITIONS,
        ECONVAR_MASTER
    )
    
    exposure_drivers = []  # ⭐ 표준 드라이버만
    supporting_drivers = []  # ⭐ GPT 설명용
    
    # 1. ⭐ Step 3.5에서 확정한 Sub-sector의 표준 드라이버만 수집
    standard_drivers = []
    if sector_code in SUB_SECTOR_DEFINITIONS:
        if sub_sector_code and sub_sector_code in SUB_SECTOR_DEFINITIONS[sector_code]:
            # Step 3.5 확정 Sub-sector의 드라이버만 사용
            sub_def = SUB_SECTOR_DEFINITIONS[sector_code][sub_sector_code]
            drivers = sub_def.get('drivers', [])
            for driver_code in drivers:
                if driver_code not in standard_drivers:
                    standard_drivers.append(driver_code)
            
            # ⭐ 표준 드라이버를 exposure_drivers에 추가
            for driver_code in standard_drivers:
                econvar_info = ECONVAR_MASTER.get(driver_code, {})
                exposure_drivers.append({
                    'var': econvar_info.get('display_name', driver_code),
                    'code': driver_code,
                    'type': econvar_info.get('type', ''),
                    'description': econvar_info.get('description', '')
                })
            
            logger.debug(f"Step 3.5 Sub-sector 기준 표준 드라이버: {sub_sector_code} → {len(exposure_drivers)}개")
        else:
            # Sub-sector가 없으면 Major Sector의 모든 Sub-sector 드라이버 수집
            for sub_code, sub_def in SUB_SECTOR_DEFINITIONS[sector_code].items():
                drivers = sub_def.get('drivers', [])
                for driver_code in drivers:
                    if driver_code not in standard_drivers:
                        standard_drivers.append(driver_code)
            
            # 표준 드라이버 추가
            for driver_code in standard_drivers:
                econvar_info = ECONVAR_MASTER.get(driver_code, {})
                exposure_drivers.append({
                    'var': econvar_info.get('display_name', driver_code),
                    'code': driver_code,
                    'type': econvar_info.get('type', ''),
                    'description': econvar_info.get('description', '')
                })
    
    # 2. GPT의 key_drivers 가져오기 (설명용으로만)
    gpt_drivers = []
    if causal_structure:
        gpt_drivers = causal_structure.get('key_drivers', [])
    
    # 3. GPT 드라이버를 표준 드라이버와 매칭
    for gpt_driver in gpt_drivers:
        var_name = gpt_driver.get('var', '')
        var_type = gpt_driver.get('type', '')
        description = gpt_driver.get('description', '')
        
        # 표준 드라이버 코드 찾기 (부분 매칭)
        matched_code = None
        for std_code in standard_drivers:
            std_code_normalized = std_code.replace('_', ' ').upper()
            var_name_normalized = var_name.upper()
            if std_code_normalized in var_name_normalized or \
               var_name_normalized in std_code_normalized:
                matched_code = std_code
                break
        
        if matched_code:
            # ⭐ 표준 드라이버와 매칭되면 exposure_drivers의 description만 업데이트
            for ed in exposure_drivers:
                if ed['code'] == matched_code:
                    # GPT description이 더 구체적이면 업데이트
                    if description and len(description) > len(ed.get('description', '')):
                        ed['description'] = description
                    break
        else:
            # ⭐ 표준 드라이버와 매칭되지 않으면 supporting_drivers에 추가
            supporting_drivers.append({
                'var': var_name,
                'type': var_type,
                'description': description
            })
    
    return exposure_drivers, supporting_drivers


def _create_result_from_candidates(
    candidates: List[Dict[str, float]],
    rule_major: Optional[str],
    rule_sub: Optional[str],
    rule_vc: Optional[str],
    rule_score: float,
    max_sectors: int,
    weights: Dict[str, float]
) -> List[Dict[str, Any]]:
    """
    후보 결과로부터 최종 결과 생성 (GPT 없이)
    
    Args:
        candidates: BGE-M3 Re-ranking 결과
        rule_major: Rule-based 섹터
        rule_sub: Rule-based Sub-sector
        rule_vc: Rule-based Value Chain
        rule_score: Rule-based 점수
        max_sectors: 최대 섹터 개수
    
    Returns:
        최종 섹터 리스트
    """
    if not candidates:
        return []
    
    results = []
    total_score = sum(c.get('bge_score', c.get('score', 0.0)) for c in candidates[:max_sectors])
    
    for i, candidate in enumerate(candidates[:max_sectors]):
        sector_code = candidate['sector']
        bge_score = candidate.get('bge_score', candidate.get('score', 0.0))
        embedding_score = candidate.get('score', 0.0)
        
        weight = bge_score / total_score if total_score > 0 else 1.0 / len(candidates[:max_sectors])
        
        # Ensemble 점수 계산
        ensemble_score = (
            embedding_score * weights.get('embedding', 0.0) +
            bge_score * weights.get('bge', 0.0) +
            (rule_score if rule_major == sector_code else 0.0) * weights.get('rule', 0.0)
        )
        
        if ensemble_score >= 0.8:
            confidence = "HIGH"
        elif ensemble_score >= 0.6:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
        
        results.append({
            'major_sector': sector_code,
            'sub_sector': rule_sub if rule_major == sector_code else None,
            'value_chain': rule_vc if rule_major == sector_code else None,
            'weight': float(weight),
            'is_primary': (i == 0),
            'confidence': confidence,
            'method': 'ENSEMBLE',
            'rule_score': rule_score if rule_major == sector_code else None,
            'embedding_score': embedding_score,
            'bge_score': bge_score,
            'gpt_score': None,
            'ensemble_score': ensemble_score,
            'reasoning': f'GPT 검증 없이 BGE-M3 + 임베딩 모델 결과 사용'
        })
    
    return results

