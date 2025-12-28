# -*- coding: utf-8 -*-
"""
Scenario API - KG V1.5.3

매크로 시나리오 분석 API
- 유가 시나리오: GET /api/v1/scenario/oil_price
- 금리 시나리오: GET /api/v1/scenario/interest_rate
- 환율 시나리오: GET /api/v1/scenario/exchange_rate
- 기업 인사이트: GET /api/v1/scenario/company/{ticker}
- 비교 분석: GET /api/v1/scenario/compare/{variable}  (V1.5.3)
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from app.db import get_db
from app.services.kg_explanation_layer import (
    generate_scenario_json,
    generate_oil_scenario_json,
    generate_interest_rate_scenario_json,
    generate_company_insight_json,
    generate_mechanism_explanation,
    generate_2hop_story,
    get_variable_korean_name,
    generate_comparison_output,
    generate_scenario_json_v153,
    calculate_exposure_level,
    get_evidence_snippets,
)
import logging
import json

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/scenario", tags=["Scenario Analysis"])


# =============================================================================
# Response Models
# =============================================================================

class ImpactDetail(BaseModel):
    direction: str
    mechanism: str
    mechanism_kr: Optional[str] = None
    weight: float
    confidence: str


class EvidenceSource(BaseModel):
    type: str
    description: str
    detail: Optional[str] = None


class AffectedCompany(BaseModel):
    ticker: str
    name: str
    sector: str
    value_chain: str
    impact: ImpactDetail
    reasoning: str
    evidence_source: EvidenceSource
    mixed_analysis: Optional[Dict[str, str]] = None
    growth_stock_note: Optional[str] = None


class ScenarioInfo(BaseModel):
    variable: str
    variable_kr: str
    direction: str
    direction_kr: str
    title: str


class ScenarioResponse(BaseModel):
    scenario: ScenarioInfo
    affected_companies: List[AffectedCompany]
    generated_at: str
    kg_version: str


# =============================================================================
# Helper Functions
# =============================================================================

def get_affected_companies_by_driver(
    db: Session,
    driver_code: str,
    limit: int = 20,
    min_weight: float = 0.5
) -> List[Dict]:
    """특정 드라이버에 영향받는 기업 조회"""
    result = db.execute(text('''
        SELECT 
            e.source_id as ticker,
            s.stock_name as name,
            s.market_cap,
            i.sector_l1,
            i.value_chain,
            e.weight,
            e.properties
        FROM edges e
        JOIN stocks s ON e.source_id = s.ticker
        JOIN investor_sector i ON e.source_id = i.ticker AND i.is_primary = true
        WHERE e.target_id = :driver
        AND e.relation_type = 'DRIVEN_BY'
        AND e.weight >= :min_weight
        ORDER BY e.weight DESC
        LIMIT :limit
    '''), {
        'driver': driver_code,
        'min_weight': min_weight,
        'limit': limit
    })
    
    companies = []
    for row in result:
        props = row[6] if isinstance(row[6], dict) else (json.loads(row[6]) if row[6] else {})
        companies.append({
            'ticker': row[0],
            'name': row[1],
            'market_cap': int(row[2]) if row[2] else None,  # 시가총액 (원 단위)
            'sector_l1': row[3],
            'value_chain': row[4],
            'weight': float(row[5]) if row[5] else 0.5,
            'mechanism': props.get('mechanism', 'DEMAND'),
            'polarity': props.get('polarity', 'MIXED'),
            'text_match_weight': props.get('text_match_weight', 0),
        })
    
    return companies


def get_company_drivers(
    db: Session,
    ticker: str,
    limit: int = 10
) -> List[Dict]:
    """특정 기업의 드라이버 조회"""
    result = db.execute(text('''
        SELECT 
            e.target_id as driver_code,
            e.weight,
            e.properties
        FROM edges e
        WHERE e.source_id = :ticker
        AND e.relation_type = 'DRIVEN_BY'
        ORDER BY e.weight DESC
        LIMIT :limit
    '''), {
        'ticker': ticker,
        'limit': limit
    })
    
    drivers = []
    for row in result:
        props = row[2] if isinstance(row[2], dict) else (json.loads(row[2]) if row[2] else {})
        drivers.append({
            'driver_code': row[0],
            'weight': float(row[1]) if row[1] else 0.5,
            'mechanism': props.get('mechanism', 'DEMAND'),
            'polarity': props.get('polarity', 'MIXED'),
            'text_match_weight': props.get('text_match_weight', 0),
        })
    
    return drivers


def get_company_info(db: Session, ticker: str) -> Optional[Dict]:
    """기업 정보 조회"""
    result = db.execute(text('''
        SELECT 
            s.ticker,
            s.stock_name,
            i.sector_l1,
            i.value_chain
        FROM stocks s
        JOIN investor_sector i ON s.ticker = i.ticker AND i.is_primary = true
        WHERE s.ticker = :ticker
    '''), {'ticker': ticker})
    
    row = result.fetchone()
    if row:
        return {
            'ticker': row[0],
            'name': row[1],
            'sector_l1': row[2],
            'value_chain': row[3],
        }
    return None


# =============================================================================
# API Endpoints
# =============================================================================

@router.get("/oil_price", response_model=ScenarioResponse)
def get_oil_price_scenario(
    direction: str = Query("UP", description="UP or DOWN"),
    limit: int = Query(20, description="Number of companies"),
    min_weight: float = Query(0.5, description="Minimum weight threshold"),
    db: Session = Depends(get_db)
):
    """
    유가 변동 시나리오 분석
    
    - **direction**: UP (상승) or DOWN (하락)
    - **limit**: 반환할 기업 수
    - **min_weight**: 최소 영향도 임계값
    
    Returns:
        영향받는 기업 목록과 설명
    """
    try:
        companies = get_affected_companies_by_driver(db, 'OIL_PRICE', limit, min_weight)
        
        if not companies:
            raise HTTPException(status_code=404, detail="영향받는 기업을 찾을 수 없습니다.")
        
        result = generate_oil_scenario_json(direction.upper(), companies)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"유가 시나리오 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=f"분석 실패: {str(e)}")


@router.get("/interest_rate", response_model=ScenarioResponse)
def get_interest_rate_scenario(
    direction: str = Query("UP", description="UP or DOWN"),
    limit: int = Query(20, description="Number of companies"),
    min_weight: float = Query(0.5, description="Minimum weight threshold"),
    db: Session = Depends(get_db)
):
    """
    금리 변동 시나리오 분석
    
    - **direction**: UP (상승) or DOWN (하락)
    - **limit**: 반환할 기업 수
    - **min_weight**: 최소 영향도 임계값
    
    Returns:
        영향받는 기업 목록과 설명
    """
    try:
        companies = get_affected_companies_by_driver(db, 'INTEREST_RATE', limit, min_weight)
        
        if not companies:
            raise HTTPException(status_code=404, detail="영향받는 기업을 찾을 수 없습니다.")
        
        result = generate_interest_rate_scenario_json(direction.upper(), companies)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"금리 시나리오 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=f"분석 실패: {str(e)}")


@router.get("/exchange_rate")
def get_exchange_rate_scenario(
    direction: str = Query("UP", description="UP or DOWN"),
    limit: int = Query(20, description="Number of companies"),
    min_weight: float = Query(0.5, description="Minimum weight threshold"),
    db: Session = Depends(get_db)
):
    """
    환율 변동 시나리오 분석
    
    - **direction**: UP (상승, 원화 약세) or DOWN (하락, 원화 강세)
    - **limit**: 반환할 기업 수
    - **min_weight**: 최소 영향도 임계값
    
    Returns:
        영향받는 기업 목록과 설명
    """
    try:
        companies = get_affected_companies_by_driver(db, 'EXCHANGE_RATE_USD_KRW', limit, min_weight)
        
        if not companies:
            raise HTTPException(status_code=404, detail="영향받는 기업을 찾을 수 없습니다.")
        
        result = generate_scenario_json('EXCHANGE_RATE_USD_KRW', direction.upper(), companies)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"환율 시나리오 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=f"분석 실패: {str(e)}")


@router.get("/company/{ticker}")
def get_company_insight(
    ticker: str,
    limit: int = Query(10, description="Number of drivers"),
    db: Session = Depends(get_db)
):
    """
    특정 기업의 드라이버 분석
    
    - **ticker**: 종목코드 (예: 005930)
    - **limit**: 반환할 드라이버 수
    
    Returns:
        기업에 영향을 주는 드라이버 목록과 설명
    """
    try:
        company_info = get_company_info(db, ticker)
        
        if not company_info:
            raise HTTPException(status_code=404, detail=f"기업을 찾을 수 없습니다: {ticker}")
        
        drivers = get_company_drivers(db, ticker, limit)
        
        if not drivers:
            raise HTTPException(status_code=404, detail="드라이버 정보를 찾을 수 없습니다.")
        
        result = generate_company_insight_json(
            ticker=company_info['ticker'],
            name=company_info['name'],
            sector_l1=company_info['sector_l1'],
            value_chain=company_info['value_chain'],
            drivers=drivers
        )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"기업 인사이트 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=f"분석 실패: {str(e)}")


@router.get("/2hop/{var1}/{var2}/{ticker}")
def get_2hop_analysis(
    var1: str,
    var2: str,
    ticker: str,
    db: Session = Depends(get_db)
):
    """
    2-hop 경로 분석
    
    예: /2hop/OIL_PRICE/INTEREST_RATE/035420
    (유가 → 금리 → 네이버 경로 분석)
    
    - **var1**: 시작 변수 (예: OIL_PRICE)
    - **var2**: 중간 변수 (예: INTEREST_RATE)
    - **ticker**: 종목코드
    
    Returns:
        2-hop 경로 설명
    """
    try:
        # 회사 정보 조회
        company_info = get_company_info(db, ticker)
        if not company_info:
            raise HTTPException(status_code=404, detail=f"기업을 찾을 수 없습니다: {ticker}")
        
        # Macro Link 조회 (var1 → var2)
        result = db.execute(text('''
            SELECT properties->>'relation' as relation
            FROM edges
            WHERE source_id = :var1 AND target_id = :var2 AND relation_type = 'MACRO_LINK'
        '''), {'var1': var1, 'var2': var2})
        
        macro_row = result.fetchone()
        macro_relation = macro_row[0] if macro_row else 'CONTEXT_LINK'
        
        # Company Driver 조회 (var2 → company)
        result = db.execute(text('''
            SELECT properties->>'mechanism' as mech, properties->>'polarity' as pol
            FROM edges
            WHERE source_id = :ticker AND target_id = :var2 AND relation_type = 'DRIVEN_BY'
        '''), {'ticker': ticker, 'var2': var2})
        
        driver_row = result.fetchone()
        if not driver_row:
            raise HTTPException(status_code=404, detail=f"기업과 {var2} 간의 연결을 찾을 수 없습니다.")
        
        mechanism = driver_row[0] or 'DEMAND'
        polarity = driver_row[1] or 'MIXED'
        
        # 2-hop 스토리 생성
        story = generate_2hop_story(
            var1=var1,
            var1_to_var2_relation=macro_relation,
            var2=var2,
            var2_to_company_mechanism=mechanism,
            var2_to_company_polarity=polarity,
            company=company_info['name'],
            scenario_name=f"{get_variable_korean_name(var1)} → {get_variable_korean_name(var2)} → {company_info['name']}"
        )
        
        return {
            'path': {
                'var1': {'code': var1, 'name_kr': get_variable_korean_name(var1)},
                'macro_relation': macro_relation,
                'var2': {'code': var2, 'name_kr': get_variable_korean_name(var2)},
                'company_relation': {'mechanism': mechanism, 'polarity': polarity},
                'company': company_info,
            },
            'story': story,
            'kg_version': 'v1.5.2',
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"2-hop 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=f"분석 실패: {str(e)}")


@router.get("/variables")
def list_available_variables(db: Session = Depends(get_db)):
    """
    분석 가능한 경제 변수 목록
    
    Returns:
        변수 코드와 한국어 이름 목록
    """
    result = db.execute(text('''
        SELECT DISTINCT target_id
        FROM edges
        WHERE relation_type = 'DRIVEN_BY'
        ORDER BY target_id
    '''))
    
    variables = []
    for row in result:
        var_code = row[0]
        variables.append({
            'code': var_code,
            'name_kr': get_variable_korean_name(var_code),
        })
    
    return {
        'variables': variables,
        'total': len(variables),
    }


# =============================================================================
# V1.5.3 - 비교 분석 API (Comparison)
# =============================================================================

@router.get("/compare/{variable}")
def get_comparison_analysis(
    variable: str,
    direction: str = Query("UP", description="UP or DOWN"),
    top_n: int = Query(3, description="Top N for each category"),
    limit: int = Query(50, description="Maximum companies to analyze"),
    min_weight: float = Query(0.3, description="Minimum weight threshold"),
    db: Session = Depends(get_db)
):
    """
    [V1.5.3] 변수별 기업 비교 분석
    
    같은 변수에 대해 Top 3 수혜주 / Top 3 피해주 / 양면성 기업 비교
    
    - **variable**: 경제 변수 (예: OIL_PRICE, INTEREST_RATE)
    - **direction**: UP (상승) or DOWN (하락)
    - **top_n**: 각 카테고리별 상위 N개
    - **limit**: 분석할 최대 기업 수
    
    Returns:
        수혜주/피해주/양면성 비교 및 차이점 설명
    """
    try:
        companies = get_affected_companies_by_driver(db, variable, limit, min_weight)
        
        if not companies:
            # Empty State 처리 (V1.5.3)
            return {
                'variable': {
                    'code': variable,
                    'name_kr': get_variable_korean_name(variable),
                },
                'status': 'no_data',
                'message': f"'{get_variable_korean_name(variable)}' 변수에 연결된 기업을 찾을 수 없습니다.",
                'suggestion': "해당 변수가 KG에 등록되어 있는지 확인해주세요. GET /api/v1/scenario/variables로 사용 가능한 변수 목록을 확인할 수 있습니다.",
                'positive_impact': [],
                'negative_impact': [],
                'mixed_impact': [],
                'comparison_summary': '',
                'kg_version': 'v1.5.3',
            }
        
        comparison = generate_comparison_output(variable, companies, top_n)
        
        # 방향에 따른 해석 추가
        direction_kr = '상승' if direction.upper() == 'UP' else '하락'
        var_name = get_variable_korean_name(variable)
        
        return {
            **comparison,
            'scenario_context': {
                'direction': direction.upper(),
                'direction_kr': direction_kr,
                'interpretation': f"{var_name} {direction_kr} 시나리오에서의 영향 분석",
            },
            'kg_version': 'v1.5.3',
        }
        
    except Exception as e:
        logger.error(f"비교 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=f"분석 실패: {str(e)}")


@router.get("/v153/scenario/{variable}")
def get_scenario_v153(
    variable: str,
    direction: str = Query("UP", description="UP or DOWN"),
    limit: int = Query(20, description="Number of companies"),
    min_weight: float = Query(0.5, description="Minimum weight threshold"),
    include_comparison: bool = Query(True, description="Include comparison output"),
    db: Session = Depends(get_db)
):
    """
    [V1.5.3] 업그레이드된 시나리오 분석
    
    Evidence Snippet + Exposure Level + Comparison 포함
    
    - **variable**: 경제 변수 (예: OIL_PRICE, INTEREST_RATE)
    - **direction**: UP (상승) or DOWN (하락)
    - **include_comparison**: 비교 분석 포함 여부
    
    Returns:
        강화된 시나리오 분석 (증거 문장, 노출도, 비교)
    """
    try:
        companies = get_affected_companies_by_driver_v153(db, variable, limit, min_weight)
        
        if not companies:
            # Empty State 처리
            return {
                'scenario': {
                    'variable': variable,
                    'variable_kr': get_variable_korean_name(variable),
                    'direction': direction.upper(),
                    'direction_kr': '상승' if direction.upper() == 'UP' else '하락',
                    'title': f"{get_variable_korean_name(variable)} 시나리오",
                },
                'status': 'no_data',
                'message': "분석할 데이터가 부족합니다.",
                'suggestion': "해당 변수에 연결된 기업이 없거나, 최소 가중치 조건을 충족하는 기업이 없습니다. min_weight를 낮춰서 다시 시도해보세요.",
                'affected_companies': [],
                'kg_version': 'v1.5.3',
            }
        
        result = generate_scenario_json_v153(
            variable,
            direction.upper(),
            companies,
            include_comparison
        )
        return result
        
    except Exception as e:
        logger.error(f"V1.5.3 시나리오 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=f"분석 실패: {str(e)}")


def get_affected_companies_by_driver_v153(
    db: Session,
    driver_code: str,
    limit: int = 20,
    min_weight: float = 0.5
) -> List[Dict]:
    """V1.5.3 - biz_summary 포함된 기업 조회"""
    result = db.execute(text('''
        SELECT 
            e.source_id as ticker,
            s.stock_name as name,
            i.sector_l1,
            i.value_chain,
            e.weight,
            e.properties,
            cd.biz_summary
        FROM edges e
        JOIN stocks s ON e.source_id = s.ticker
        JOIN investor_sector i ON e.source_id = i.ticker AND i.is_primary = true
        LEFT JOIN company_details cd ON e.source_id = cd.ticker
        WHERE e.target_id = :driver
        AND e.relation_type = 'DRIVEN_BY'
        AND e.weight >= :min_weight
        ORDER BY e.weight DESC
        LIMIT :limit
    '''), {
        'driver': driver_code,
        'min_weight': min_weight,
        'limit': limit
    })
    
    companies = []
    for row in result:
        props = row[5] if isinstance(row[5], dict) else (json.loads(row[5]) if row[5] else {})
        companies.append({
            'ticker': row[0],
            'name': row[1],
            'sector_l1': row[2],
            'value_chain': row[3],
            'weight': float(row[4]) if row[4] else 0.5,
            'mechanism': props.get('mechanism', 'DEMAND'),
            'polarity': props.get('polarity', 'MIXED'),
            'text_match_weight': props.get('text_match_weight', 0),
            'biz_summary': row[6] or '',  # V1.5.3: Evidence Snippet용
        })
    
    return companies


# =============================================================================
# V1.5.3 - Empty State 처리가 적용된 기업 조회
# =============================================================================

@router.get("/company/{ticker}/v153")
def get_company_insight_v153(
    ticker: str,
    limit: int = Query(10, description="Number of drivers"),
    db: Session = Depends(get_db)
):
    """
    [V1.5.3] 기업 인사이트 (Exposure + Evidence 포함)
    
    - **ticker**: 종목코드 (예: 005930)
    - **limit**: 반환할 드라이버 수
    
    Returns:
        기업의 드라이버 분석 (노출도, 증거 문장 포함)
    """
    try:
        company_info = get_company_info(db, ticker)
        
        # Empty State: 기업이 없는 경우
        if not company_info:
            return {
                'status': 'not_found',
                'ticker': ticker,
                'message': f"종목코드 '{ticker}'에 해당하는 기업을 찾을 수 없습니다.",
                'suggestion': "종목코드가 올바른지 확인해주세요. (예: 삼성전자 = 005930)",
                'company': None,
                'drivers': [],
                'kg_version': 'v1.5.3',
            }
        
        drivers = get_company_drivers(db, ticker, limit)
        
        # Empty State: 드라이버가 없는 경우
        if not drivers:
            return {
                'status': 'no_drivers',
                'company': company_info,
                'message': f"'{company_info['name']}'에 대한 드라이버 정보가 아직 구축되지 않았습니다.",
                'suggestion': "해당 기업은 KG에 등록되어 있으나, 아직 경제 변수와의 연결이 설정되지 않았습니다.",
                'drivers': [],
                'kg_version': 'v1.5.3',
            }
        
        # biz_summary 조회
        result = db.execute(text('''
            SELECT biz_summary FROM company_details WHERE ticker = :ticker
        '''), {'ticker': ticker})
        biz_row = result.fetchone()
        biz_summary = biz_row[0] if biz_row else ''
        
        # 드라이버별 Exposure + Evidence 계산
        enriched_drivers = []
        for driver in drivers:
            exposure = calculate_exposure_level(
                driver['mechanism'],
                driver['weight'],
                driver.get('text_match_weight', 0),
                biz_summary
            )
            evidence_snippets = get_evidence_snippets(
                driver['driver_code'],
                driver['mechanism'],
                driver['polarity'],
                biz_summary
            )
            
            enriched_drivers.append({
                **driver,
                'exposure': exposure,
                'evidence_snippets': evidence_snippets,
            })
        
        return {
            'status': 'success',
            'company': company_info,
            'drivers': enriched_drivers,
            'total_drivers': len(enriched_drivers),
            'kg_version': 'v1.5.3',
        }
        
    except Exception as e:
        logger.error(f"V1.5.3 기업 인사이트 분석 실패: {e}")
        raise HTTPException(status_code=500, detail=f"분석 실패: {str(e)}")

