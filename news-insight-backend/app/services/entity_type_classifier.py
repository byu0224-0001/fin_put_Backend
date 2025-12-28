# -*- coding: utf-8 -*-
"""
Entity Type 분류 서비스 (GPT 피드백: Soft entity_type)

지주회사, SPAC, REIT 등 기업 형태를 분류하여 classification_meta에 저장
"""
import logging
from typing import Optional, Dict, Any, Tuple
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.krx_sector_filter import detect_holding_company, classify_holding_type

logger = logging.getLogger(__name__)

# Entity Type 정의
ENTITY_TYPES = {
    'OPERATING': '일반 사업회사',
    'HOLDING_PURE': '순수 지주회사',
    'HOLDING_BUSINESS': '사업 지주회사',
    'HOLDING_FINANCIAL': '금융 지주회사',
    'SPAC': 'SPAC (특수목적인수회사)',
    'REIT': 'REIT (부동산투자회사)',
    'SUBSIDIARY': '계열사'  # CHAEBOL_CORE 제거: 계열사/사업회사로 통합
}


def classify_entity_type(
    stock: Stock,
    company_detail: Optional[CompanyDetail] = None
) -> Tuple[Optional[str], float, Dict[str, Any]]:
    """
    Entity Type 분류 (GPT 피드백: Soft entity_type)
    
    Args:
        stock: Stock 객체
        company_detail: CompanyDetail 객체 (선택)
    
    Returns:
        (entity_type, confidence, classification_meta)
        entity_type: 'OPERATING', 'HOLDING_PURE', 'HOLDING_BUSINESS', etc.
        confidence: 0.0~1.0
        classification_meta: {
            'entity_type': 'HOLDING_BUSINESS',
            'holding_type': 'BUSINESS_HOLDING',
            'holding_confidence': 0.8,
            'revenue_by_segment': {...},
            'evidence': [...]
        }
    """
    if not stock:
        return None, 0.0, {}
    
    company_name = stock.stock_name
    industry_raw = stock.industry_raw
    keywords = company_detail.keywords if company_detail else None
    products = company_detail.products if company_detail else None
    revenue_by_segment = company_detail.revenue_by_segment if company_detail else None
    
    classification_meta = {
        'entity_type': None,
        'holding_type': None,
        'holding_confidence': 0.0,
        'evidence': []
    }
    
    # 1. SPAC 판정
    if 'SPAC' in company_name or '스팩' in company_name:
        classification_meta['entity_type'] = 'SPAC'
        classification_meta['evidence'].append(f"회사명 패턴: {company_name}")
        return 'SPAC', 0.9, classification_meta
    
    # 2. REIT 판정
    if 'REIT' in company_name or '리츠' in company_name:
        classification_meta['entity_type'] = 'REIT'
        classification_meta['evidence'].append(f"회사명 패턴: {company_name}")
        return 'REIT', 0.9, classification_meta
    
    # 🆕 P0 최후 수단: SK이노베이션 특별 처리 (biz_summary에 자회사 정보 없음)
    # R1 개선: override_hit boolean 추가하여 로직과 구분
    ticker = stock.ticker if hasattr(stock, 'ticker') else None
    override_hit = False
    if ticker == '096770' or (company_name and 'SK이노베이션' in company_name):
        override_hit = True
        classification_meta['entity_type'] = 'HOLDING_BUSINESS'
        classification_meta['holding_type'] = 'BUSINESS_HOLDING'
        classification_meta['holding_confidence'] = 0.9
        classification_meta['evidence'].append('SK이노베이션 특별 처리: 중간지주회사 (석유/화학 직접 사업 + 다수 자회사 보유)')
        override_reason = 'biz_summary에 자회사 정보 없음, 실제로는 중간지주회사'
        # 🆕 A) override_hit 정의/저장 일관성: override_reason이 있으면 항상 override_hit=True
        classification_meta['override'] = {  # 한 덩어리로 고정
            'hit': True,
            'reason': override_reason,
            'source': 'RULE_OVERRIDE'
        }
        classification_meta['entity_type_evidence'] = {
            'signals': ['특별처리(SK이노베이션)'],
            'holding_confidence': 0.9,
            'holding_type': 'BUSINESS_HOLDING',
            'override_reason': override_reason,  # 하위 호환성
            'override_hit': True  # 하위 호환성
        }
        classification_meta['override_hit'] = True  # 하위 호환성
        classification_meta['override_reason'] = override_reason  # 하위 호환성
        return 'HOLDING_BUSINESS', 0.9, classification_meta
    
    # 3. 지주회사 판정
    is_holding, holding_conf, reason, holding_type = detect_holding_company(
        company_name, industry_raw, keywords, products, revenue_by_segment, company_detail
    )
    
    if is_holding:
        classification_meta['holding_type'] = holding_type
        classification_meta['holding_confidence'] = holding_conf
        classification_meta['evidence'].append(f"지주회사 판정: {reason}")
        
        # 🆕 P0 개선: entity_type_evidence 강화
        classification_meta['entity_type_evidence'] = {
            'signals': reason.split('+') if reason else [],
            'holding_confidence': holding_conf,
            'holding_type': holding_type
        }
        
        # 매출 비중 기반 세분화
        if revenue_by_segment and isinstance(revenue_by_segment, dict):
            holding_revenue_pct = 0.0
            business_revenue_pct = 0.0
            
            holding_keywords = ['배당', '임대', '로열티', '브랜드', '상표권', '지주', '투자']
            for segment, pct in revenue_by_segment.items():
                if isinstance(pct, (int, float)) and pct > 0:
                    if any(kw in str(segment) for kw in holding_keywords):
                        holding_revenue_pct += pct
                    else:
                        business_revenue_pct += pct
            
            total_revenue = holding_revenue_pct + business_revenue_pct
            if total_revenue > 0:
                holding_ratio = holding_revenue_pct / total_revenue
                
                if holding_ratio >= 0.7:
                    entity_type = 'HOLDING_PURE'
                    classification_meta['entity_type'] = entity_type
                    classification_meta['evidence'].append(f"순수 지주: 배당/임대 비중 {holding_ratio:.1%}")
                    return entity_type, holding_conf, classification_meta
                elif holding_ratio >= 0.3:
                    entity_type = 'HOLDING_BUSINESS'
                    classification_meta['entity_type'] = entity_type
                    classification_meta['evidence'].append(f"사업 지주: 배당/임대 {holding_ratio:.1%}, 사업 {1-holding_ratio:.1%}")
                    return entity_type, holding_conf, classification_meta
        
        # 매출 데이터 없으면 holding_type 기반 판정
        if holding_type == 'FINANCIAL_HOLDING':
            entity_type = 'HOLDING_FINANCIAL'
        elif holding_type == 'PURE_HOLDING':
            entity_type = 'HOLDING_PURE'
        else:
            entity_type = 'HOLDING_BUSINESS'
        
        classification_meta['entity_type'] = entity_type
        return entity_type, holding_conf, classification_meta
    
    # 4. 기본값: 일반 사업회사
    classification_meta['entity_type'] = 'OPERATING'
    classification_meta['entity_type_evidence'] = {
        'signals': [],
        'reason': '일반 사업회사 (지주회사 아님)'
    }
    return 'OPERATING', 1.0, classification_meta


def update_classification_meta(
    existing_meta: Optional[Dict[str, Any]],
    entity_type: Optional[str],
    entity_confidence: float,
    classification_meta: Dict[str, Any]
) -> Dict[str, Any]:
    """
    classification_meta 업데이트 (기존 메타데이터와 병합)
    
    Args:
        existing_meta: 기존 classification_meta (JSONB)
        entity_type: 분류된 entity_type
        entity_confidence: 신뢰도
        classification_meta: 새로운 메타데이터
    
    Returns:
        업데이트된 classification_meta
    """
    if not existing_meta:
        existing_meta = {}
    
    # Entity Type 정보 업데이트
    existing_meta['entity_type'] = entity_type
    existing_meta['entity_confidence'] = entity_confidence
    existing_meta['holding_type'] = classification_meta.get('holding_type')
    existing_meta['holding_confidence'] = classification_meta.get('holding_confidence', 0.0)
    existing_meta['evidence'] = classification_meta.get('evidence', [])
    
    # 타임스탬프
    from datetime import datetime
    existing_meta['entity_type_updated_at'] = datetime.utcnow().isoformat()
    
    return existing_meta

