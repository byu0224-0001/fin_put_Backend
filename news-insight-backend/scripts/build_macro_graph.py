# -*- coding: utf-8 -*-
"""
Macro Graph Builder - KG V1.5

경제 변수 간의 관계(MACRO_LINK)를 생성합니다.

설계 원칙:
1. 강한 인과(CAUSE) 표현 금지 → PRESSURES, CONTEXT_LINK만 사용
2. 경제학 이론 기반의 설명 가능한 연결만 허용
3. 2-hop 추론을 위한 중간 다리 역할
"""

import sys
import os
import codecs
import json
from datetime import datetime

sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from app.db import get_db
from sqlalchemy import text


# =============================================================================
# Macro Graph Seed Definition (20~30개)
# =============================================================================

# relation 유형:
# - PRESSURES: A가 상승하면 B에 상승 압력 (인플레→금리 인상 압력)
# - NEGATIVE_PRESSURE: A가 상승하면 B에 하락 압력 (금리→성장주 가치 하락)
# - CONTEXT_LINK: A와 B가 같은 맥락에서 움직임 (경기↔소비)
# - COMPONENT_OF: A가 B의 구성 요소 (DRAM가격 → 반도체 수익)

MACRO_GRAPH_SEED = [
    # =========================================================================
    # 유가 경로 (Oil Price Chain)
    # =========================================================================
    {
        'source': 'OIL_PRICE',
        'target': 'INFLATION_CPI',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '유가 상승 → 물가 상승 압력',
    },
    {
        'source': 'OIL_PRICE',
        'target': 'NAPHTHA_PRICE',
        'relation': 'CONTEXT_LINK',
        'strength': 'HIGH',
        'description': '유가와 나프타가격은 동행',
    },
    {
        'source': 'OIL_PRICE',
        'target': 'TRANSPORT_COST',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '유가 상승 → 운송비 상승 압력',
    },
    
    # =========================================================================
    # 인플레이션-금리 경로
    # =========================================================================
    {
        'source': 'INFLATION_CPI',
        'target': 'INTEREST_RATE',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '인플레이션 → 금리 인상 압력',
    },
    {
        'source': 'INTEREST_RATE',
        'target': 'GROWTH_STOCK_VALUATION',
        'relation': 'NEGATIVE_PRESSURE',
        'strength': 'HIGH',
        'description': '금리 인상 → 성장주 밸류에이션 하락 압력',
    },
    {
        'source': 'INTEREST_RATE',
        'target': 'CONSTRUCTION_DEMAND',
        'relation': 'NEGATIVE_PRESSURE',
        'strength': 'MED',
        'description': '금리 인상 → 건설/부동산 수요 하락 압력',
    },
    {
        'source': 'INTEREST_RATE',
        'target': 'CONSUMER_SPENDING',
        'relation': 'NEGATIVE_PRESSURE',
        'strength': 'MED',
        'description': '금리 인상 → 소비 위축 압력',
    },
    {
        'source': 'INTEREST_RATE',
        'target': 'LOAN_DEMAND',
        'relation': 'NEGATIVE_PRESSURE',
        'strength': 'HIGH',
        'description': '금리 인상 → 대출 수요 감소',
    },
    
    # =========================================================================
    # 환율 경로
    # =========================================================================
    {
        'source': 'EXCHANGE_RATE_USD_KRW',
        'target': 'EXPORT_COMPETITIVENESS',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '원화 약세 → 수출 경쟁력 상승',
    },
    {
        'source': 'EXCHANGE_RATE_USD_KRW',
        'target': 'IMPORT_COST',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '원화 약세 → 수입 비용 상승',
    },
    {
        'source': 'US_INTEREST_RATE',
        'target': 'EXCHANGE_RATE_USD_KRW',
        'relation': 'PRESSURES',
        'strength': 'MED',
        'description': '미국 금리 인상 → 원화 약세 압력',
    },
    
    # =========================================================================
    # 경기/수요 경로
    # =========================================================================
    {
        'source': 'US_GDP',
        'target': 'KOREA_EXPORT',
        'relation': 'CONTEXT_LINK',
        'strength': 'HIGH',
        'description': '미국 경기 → 한국 수출 연동',
    },
    {
        'source': 'CHINA_GDP',
        'target': 'KOREA_EXPORT',
        'relation': 'CONTEXT_LINK',
        'strength': 'HIGH',
        'description': '중국 경기 → 한국 수출 연동',
    },
    {
        'source': 'ECONOMIC_GROWTH',
        'target': 'CONSUMER_SPENDING',
        'relation': 'CONTEXT_LINK',
        'strength': 'HIGH',
        'description': '경기 회복 → 소비 증가 연동',
    },
    {
        'source': 'CONSUMER_CONFIDENCE',
        'target': 'CONSUMER_SPENDING',
        'relation': 'PRESSURES',
        'strength': 'MED',
        'description': '소비자 심리 → 소비 영향',
    },
    
    # =========================================================================
    # 반도체 경로
    # =========================================================================
    {
        'source': 'GLOBAL_CAPEX',
        'target': 'SEMICONDUCTOR_DEMAND',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '글로벌 IT투자 → 반도체 수요',
    },
    {
        'source': 'SEMICONDUCTOR_DEMAND',
        'target': 'DRAM_ASP',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '반도체 수요 증가 → DRAM 가격 상승 압력',
    },
    {
        'source': 'SEMICONDUCTOR_DEMAND',
        'target': 'NAND_ASP',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '반도체 수요 증가 → NAND 가격 상승 압력',
    },
    {
        'source': 'AI_INVESTMENT',
        'target': 'HBM_DEMAND',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': 'AI 투자 확대 → HBM 수요 급증',
    },
    {
        'source': 'AI_INVESTMENT',
        'target': 'AI_SERVER_CAPEX',
        'relation': 'CONTEXT_LINK',
        'strength': 'HIGH',
        'description': 'AI 투자 → AI 서버 투자 연동',
    },
    
    # =========================================================================
    # 전기차/배터리 경로
    # =========================================================================
    {
        'source': 'EV_SALES',
        'target': 'BATTERY_DEMAND',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '전기차 판매 → 배터리 수요',
    },
    {
        'source': 'BATTERY_DEMAND',
        'target': 'LITHIUM_PRICE',
        'relation': 'PRESSURES',
        'strength': 'MED',
        'description': '배터리 수요 → 리튬 가격 압력',
    },
    
    # =========================================================================
    # 건설/부동산 경로
    # =========================================================================
    {
        'source': 'HOUSING_STARTS',
        'target': 'STEEL_DEMAND',
        'relation': 'PRESSURES',
        'strength': 'MED',
        'description': '주택 착공 → 철강 수요',
    },
    {
        'source': 'CONSTRUCTION_ORDERS',
        'target': 'CEMENT_DEMAND',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '건설 수주 → 시멘트 수요',
    },
    
    # =========================================================================
    # 소비/유통 경로
    # =========================================================================
    {
        'source': 'CONSUMER_SPENDING',
        'target': 'RETAIL_SALES',
        'relation': 'CONTEXT_LINK',
        'strength': 'HIGH',
        'description': '소비 지출 → 소매 판매 연동',
    },
    {
        'source': 'DIGITAL_ADOPTION',
        'target': 'E_COMMERCE_TRANS_VOL',
        'relation': 'PRESSURES',
        'strength': 'HIGH',
        'description': '디지털화 → 이커머스 성장',
    },
]

MACRO_GRAPH_VERSION = "v1.5"


def build_macro_graph(db, dry_run: bool = False):
    """
    Macro Graph (변수↔변수) 엣지 생성
    """
    print('=' * 70)
    print('Building Macro Graph (V1.5)')
    print('=' * 70)
    
    # 1. 기존 MACRO_LINK 삭제
    if not dry_run:
        db.execute(text("DELETE FROM edges WHERE relation_type = 'MACRO_LINK'"))
        db.commit()
        print('Deleted existing MACRO_LINK edges.')
    
    # 2. 엣지 생성
    edges_created = 0
    
    for link in MACRO_GRAPH_SEED:
        edge_id = f"{link['source']}_{link['target']}_MACRO_LINK"
        
        edge_data = {
            'id': edge_id,
            'source_type': 'ECONVAR',
            'source_id': link['source'],
            'target_type': 'ECONVAR',
            'target_id': link['target'],
            'relation_type': 'MACRO_LINK',
            'weight': 1.0 if link['strength'] == 'HIGH' else (0.7 if link['strength'] == 'MED' else 0.4),
            'properties': json.dumps({
                'relation': link['relation'],
                'strength': link['strength'],
                'description': link['description'],
                'evidence_type': 'ECON_THEORY',
                'rule_version': MACRO_GRAPH_VERSION,
                'created_at': datetime.utcnow().isoformat(),
            }),
        }
        
        if not dry_run:
            db.execute(text('''
                INSERT INTO edges (id, source_type, source_id, target_type, target_id, relation_type, weight, properties)
                VALUES (:id, :source_type, :source_id, :target_type, :target_id, :relation_type, :weight, :properties)
                ON CONFLICT (id) DO UPDATE SET weight = EXCLUDED.weight, properties = EXCLUDED.properties
            '''), edge_data)
        
        edges_created += 1
        print(f'  {link["source"]} --[{link["relation"]}]--> {link["target"]}')
    
    if not dry_run:
        db.commit()
    
    print()
    print(f'Total MACRO_LINK edges created: {edges_created}')
    
    return edges_created


def verify_macro_graph(db):
    """Macro Graph 검증"""
    print()
    print('=' * 70)
    print('Macro Graph Verification')
    print('=' * 70)
    
    # 1. 총 개수
    result = db.execute(text('''
        SELECT COUNT(*) FROM edges WHERE relation_type = 'MACRO_LINK'
    '''))
    total = result.fetchone()[0]
    print(f'Total MACRO_LINK edges: {total}')
    
    # 2. Relation 분포
    result = db.execute(text('''
        SELECT 
            properties->>'relation' as relation,
            COUNT(*) as cnt
        FROM edges
        WHERE relation_type = 'MACRO_LINK'
        GROUP BY relation
        ORDER BY cnt DESC
    '''))
    print('\nRelation Distribution:')
    for row in result:
        print(f'  {row[0]}: {row[1]}')
    
    # 3. 주요 변수의 연결 확인
    key_variables = ['OIL_PRICE', 'INTEREST_RATE', 'EXCHANGE_RATE_USD_KRW', 'SEMICONDUCTOR_DEMAND']
    
    print('\nKey Variable Connections:')
    for var in key_variables:
        # Outgoing
        result = db.execute(text('''
            SELECT target_id, properties->>'relation' as rel
            FROM edges
            WHERE source_id = :var AND relation_type = 'MACRO_LINK'
        '''), {'var': var})
        outgoing = list(result)
        
        # Incoming
        result = db.execute(text('''
            SELECT source_id, properties->>'relation' as rel
            FROM edges
            WHERE target_id = :var AND relation_type = 'MACRO_LINK'
        '''), {'var': var})
        incoming = list(result)
        
        print(f'\n  {var}:')
        print(f'    Outgoing ({len(outgoing)}): {[(t, r) for t, r in outgoing]}')
        print(f'    Incoming ({len(incoming)}): {[(s, r) for s, r in incoming]}')


def test_2hop_reasoning(db):
    """2-hop 추론 테스트"""
    print()
    print('=' * 70)
    print('2-Hop Reasoning Test')
    print('=' * 70)
    
    # =========================================================================
    # Test Case 1: OIL_PRICE → INTEREST_RATE → 성장주
    # =========================================================================
    print('\n--- Test Case 1: OIL_PRICE → INTEREST_RATE → Growth Stocks ---')
    
    # Hop 1: OIL_PRICE → INFLATION → INTEREST_RATE
    result = db.execute(text('''
        WITH hop1 AS (
            SELECT target_id as mid_var, properties->>'relation' as rel1
            FROM edges
            WHERE source_id = 'OIL_PRICE' AND relation_type = 'MACRO_LINK'
        ),
        hop2 AS (
            SELECT h1.mid_var, h1.rel1, e.target_id as end_var, e.properties->>'relation' as rel2
            FROM hop1 h1
            JOIN edges e ON h1.mid_var = e.source_id AND e.relation_type = 'MACRO_LINK'
            WHERE e.target_id = 'INTEREST_RATE'
        )
        SELECT * FROM hop2
    '''))
    
    paths = list(result)
    if paths:
        print('  Path found:')
        for mid, rel1, end, rel2 in paths:
            print(f'    OIL_PRICE --[{rel1}]--> {mid} --[{rel2}]--> {end}')
    else:
        # Direct path
        result = db.execute(text('''
            SELECT target_id, properties->>'relation' as rel
            FROM edges
            WHERE source_id = 'OIL_PRICE' AND relation_type = 'MACRO_LINK'
        '''))
        direct = list(result)
        print(f'  Direct from OIL_PRICE: {direct}')
    
    # INTEREST_RATE → IT 기업 (성장주)
    result = db.execute(text('''
        SELECT e.source_id, s.stock_name, 
               e.properties->>'mechanism' as mech,
               e.properties->>'polarity' as pol
        FROM edges e
        JOIN stocks s ON e.source_id = s.ticker
        JOIN investor_sector i ON e.source_id = i.ticker AND i.is_primary = true
        WHERE e.target_id = 'INTEREST_RATE' 
        AND e.relation_type = 'DRIVEN_BY'
        AND i.sector_l1 IN ('SEC_IT', 'SEC_BIO')
        LIMIT 5
    '''))
    
    growth_stocks = list(result)
    print(f'  Growth stocks affected by INTEREST_RATE ({len(growth_stocks)}):')
    for ticker, name, mech, pol in growth_stocks:
        print(f'    {ticker} ({name}): {mech} / {pol}')
    
    # =========================================================================
    # Test Case 2: OIL_PRICE → S-Oil (SPREAD vs INPUT_COST)
    # =========================================================================
    print('\n--- Test Case 2: OIL_PRICE → S-Oil (Multi-Path) ---')
    
    result = db.execute(text('''
        SELECT e.properties->>'mechanism' as mech,
               e.properties->>'polarity' as pol,
               e.weight
        FROM edges e
        WHERE e.source_id = '010950' -- S-Oil
        AND e.target_id = 'OIL_PRICE'
        AND e.relation_type = 'DRIVEN_BY'
    '''))
    
    soils = list(result)
    if soils:
        for mech, pol, weight in soils:
            print(f'  S-Oil + OIL_PRICE: mechanism={mech}, polarity={pol}, weight={weight}')
    else:
        print('  No direct edge found. Checking via sector...')
    
    # 화학 섹터의 OIL_PRICE 연결 확인
    result = db.execute(text('''
        SELECT e.source_id, s.stock_name,
               e.properties->>'mechanism' as mech,
               e.properties->>'polarity' as pol
        FROM edges e
        JOIN stocks s ON e.source_id = s.ticker
        JOIN investor_sector i ON e.source_id = i.ticker AND i.is_primary = true
        WHERE e.target_id = 'OIL_PRICE' 
        AND e.relation_type = 'DRIVEN_BY'
        AND i.sector_l1 = 'SEC_CHEM'
        LIMIT 5
    '''))
    
    chem_companies = list(result)
    print(f'  Chemical companies with OIL_PRICE ({len(chem_companies)}):')
    for ticker, name, mech, pol in chem_companies:
        print(f'    {ticker} ({name}): {mech} / {pol}')
    
    # =========================================================================
    # Test Case 3: INTEREST_RATE → NAVER, 카카오 등 IT 기업
    # =========================================================================
    print('\n--- Test Case 3: INTEREST_RATE → IT Platform Companies ---')
    
    platform_tickers = ['035420', '035720', '259960', '263750']  # 네이버, 카카오, 크래프톤, 펄어비스
    
    for ticker in platform_tickers:
        result = db.execute(text('''
            SELECT s.stock_name,
                   e.properties->>'mechanism' as mech,
                   e.properties->>'polarity' as pol
            FROM edges e
            JOIN stocks s ON e.source_id = s.ticker
            WHERE e.source_id = :ticker
            AND e.target_id = 'INTEREST_RATE'
            AND e.relation_type = 'DRIVEN_BY'
        '''), {'ticker': ticker})
        
        row = result.fetchone()
        if row:
            name, mech, pol = row
            print(f'  {ticker} ({name}): INTEREST_RATE → {mech} / {pol}')
        else:
            result2 = db.execute(text('SELECT stock_name FROM stocks WHERE ticker = :t'), {'t': ticker})
            name_row = result2.fetchone()
            name = name_row[0] if name_row else ticker
            print(f'  {ticker} ({name}): No INTEREST_RATE edge')
    
    print()
    print('=' * 70)
    print('2-Hop Reasoning Test Complete')
    print('=' * 70)


def main():
    print('=' * 70)
    print('Macro Graph Builder (V1.5)')
    print('=' * 70)
    print()
    
    db = next(get_db())
    
    # 1. Macro Graph 생성
    edges_created = build_macro_graph(db, dry_run=False)
    
    # 2. 검증
    verify_macro_graph(db)
    
    # 3. 2-hop 추론 테스트
    test_2hop_reasoning(db)
    
    print()
    print('=' * 70)
    print('Macro Graph Building Complete!')
    print('=' * 70)
    print(f'Total MACRO_LINK edges: {edges_created}')


if __name__ == '__main__':
    main()

