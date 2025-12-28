# -*- coding: utf-8 -*-
"""
KG 최종 검증 스크립트

A. Semantic Group 경계 모호성 검사
B. 방향성(Direction) 정합성 검증
C. VALUE_CHAIN ↔ DRIVEN_BY 충돌 체크
D. 그룹핑 정밀도 확인 (메모리 그룹 → 비메모리)
E. Temporal Relevance 메타 설계
F. IR 시나리오 JSON Export
"""

import sys
import os
import codecs
import json
from collections import defaultdict, Counter
from datetime import datetime

sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from app.db import get_db
from sqlalchemy import text


# =============================================================================
# Driver Semantic Groups (from driver_semantic_groups.py)
# =============================================================================

DRIVER_SEMANTIC_GROUPS = {
    'MEMORY_PRICE': {
        'display_name': '메모리 가격',
        'members': ['DRAM_ASP', 'NAND_ASP'],
    },
    'AI_INFRA_DEMAND': {
        'display_name': 'AI 인프라 수요',
        'members': ['HBM_DEMAND', 'AI_SERVER_CAPEX', 'GPU_DEMAND'],
    },
    'SEMICONDUCTOR_CYCLE': {
        'display_name': '반도체 사이클',
        'members': ['SEMICONDUCTOR_CAPEX', 'SEMICONDUCTOR_DEMAND', 'WAFER_DEMAND'],
    },
    'FX_RATE': {
        'display_name': '환율',
        'members': ['EXCHANGE_RATE_USD_KRW', 'EXCHANGE_RATE_CNY_KRW', 'EXCHANGE_RATE_JPY_KRW'],
    },
    'COMMODITY_ENERGY': {
        'display_name': '원자재/에너지',
        'members': ['OIL_PRICE', 'NAPHTHA_PRICE', 'COMMODITY_PRICE', 'GAS_PRICE', 'COAL_PRICE'],
    },
    'CONSUMER_RETAIL': {
        'display_name': '소비/유통',
        'members': ['CONSUMER_SPENDING', 'RETAIL_SALES', 'E_COMMERCE_TRANS_VOL', 'CONSUMER_CONFIDENCE'],
    },
    'INTEREST_CREDIT': {
        'display_name': '금리/금융',
        'members': ['INTEREST_RATE', 'LOAN_DEMAND', 'CONSUMER_CREDIT', 'DEPOSIT_GROWTH'],
    },
    'EV_BATTERY': {
        'display_name': '전기차/배터리',
        'members': ['EV_SALES', 'BATTERY_DEMAND', 'LITHIUM_PRICE', 'COBALT_PRICE'],
    },
    'IT_SPENDING': {
        'display_name': 'IT 투자',
        'members': ['ENTERPRISE_IT_SPENDING', 'CLOUD_ADOPTION', 'RD_SPENDING', 'SOFTWARE_DEMAND'],
    },
    'STEEL_METAL': {
        'display_name': '철강/금속',
        'members': ['STEEL_PRICE', 'IRON_ORE_PRICE', 'ALUMINUM_PRICE', 'COPPER_PRICE'],
    },
    'CONSTRUCTION_RE': {
        'display_name': '건설/부동산',
        'members': ['CONSTRUCTION_ORDERS', 'HOUSING_STARTS', 'REAL_ESTATE_PRICE', 'CEMENT_PRICE', 'HOUSING_MARKET'],
    },
    'HEALTHCARE': {
        'display_name': '헬스케어',
        'members': ['HEALTHCARE_SPENDING', 'PHARMA_RD_SPENDING', 'FDA_APPROVAL', 'AGING_POPULATION'],
    },
    'TRAVEL_AIRLINE': {
        'display_name': '여행/항공',
        'members': ['TOURIST_ARRIVALS', 'AIRLINE_PASSENGERS', 'HOTEL_OCCUPANCY'],
    },
    'AUTO_INDUSTRY': {
        'display_name': '자동차',
        'members': ['AUTO_SALES', 'AUTO_PRODUCTION', 'VEHICLE_INVENTORY'],
    },
}

# 역방향 매핑
DRIVER_TO_GROUP = {}
for group_id, group_info in DRIVER_SEMANTIC_GROUPS.items():
    for member in group_info['members']:
        DRIVER_TO_GROUP[member] = group_id


# =============================================================================
# Direction Rules (방향성 규칙)
# =============================================================================

# 드라이버별 섹터/기업 영향 방향 정의
# POSITIVE: 드라이버 상승 → 기업 실적/주가 상승
# NEGATIVE: 드라이버 상승 → 기업 실적/주가 하락
# MIXED: 복합적 영향

DRIVER_DIRECTION_RULES = {
    # 유가
    'OIL_PRICE': {
        'default': 'MIXED',
        'sector_rules': {
            'SEC_OIL': 'POSITIVE',      # 정유/에너지 → 유가 상승 = 매출 증가
            'SEC_CHEM': 'NEGATIVE',     # 화학(원가) → 유가 상승 = 비용 증가
            'SEC_TRAVEL': 'NEGATIVE',   # 항공/여행 → 유가 상승 = 연료비 증가
            'SEC_SHIP': 'NEGATIVE',     # 해운 → 유가 상승 = 연료비 증가
            'SEC_AUTO': 'NEGATIVE',     # 자동차 → 유가 상승 = 수요 감소
        },
    },
    # 금리
    'INTEREST_RATE': {
        'default': 'NEGATIVE',
        'sector_rules': {
            'SEC_BANK': 'POSITIVE',     # 은행 → 금리 상승 = NIM 확대
            'SEC_INSURANCE': 'POSITIVE', # 보험 → 금리 상승 = 투자수익 증가
            'SEC_SECURITIES': 'MIXED',  # 증권 → 혼합
            'SEC_IT': 'NEGATIVE',       # IT/성장주 → 금리 상승 = 밸류에이션 하락
            'SEC_BIO': 'NEGATIVE',      # 바이오 → 금리 상승 = 밸류에이션 하락
            'SEC_CONST': 'NEGATIVE',    # 건설 → 금리 상승 = 주택 수요 감소
        },
    },
    # 환율
    'EXCHANGE_RATE_USD_KRW': {
        'default': 'MIXED',
        'sector_rules': {
            'SEC_AUTO': 'POSITIVE',     # 자동차(수출) → 원화 약세 = 가격 경쟁력 상승
            'SEC_SEMI': 'POSITIVE',     # 반도체(수출) → 원화 약세 = 가격 경쟁력 상승
            'SEC_SHIP': 'POSITIVE',     # 조선(수출) → 원화 약세 = 가격 경쟁력 상승
            'SEC_OIL': 'NEGATIVE',      # 정유(수입) → 원화 약세 = 원유 수입비용 증가
            'SEC_TRAVEL': 'NEGATIVE',   # 항공/여행 → 원화 약세 = 해외여행 수요 감소
        },
    },
    # DRAM 가격
    'DRAM_ASP': {
        'default': 'POSITIVE',
        'sector_rules': {
            'SEC_SEMI': 'POSITIVE',     # 반도체 → DRAM 가격 상승 = 매출 증가
        },
    },
    # NAND 가격
    'NAND_ASP': {
        'default': 'POSITIVE',
        'sector_rules': {
            'SEC_SEMI': 'POSITIVE',     # 반도체 → NAND 가격 상승 = 매출 증가
        },
    },
    # 전기차 판매
    'EV_SALES': {
        'default': 'POSITIVE',
        'sector_rules': {
            'SEC_AUTO': 'POSITIVE',     # 자동차 → EV 판매 증가 = 매출 증가
            'SEC_BATTERY': 'POSITIVE',  # 배터리 → EV 판매 증가 = 배터리 수요 증가
        },
    },
}


# =============================================================================
# Temporal Relevance 설계
# =============================================================================

TEMPORAL_RELEVANCE_MAPPING = {
    # STRUCTURAL (구조적, 항상 중요)
    'EXCHANGE_RATE_USD_KRW': 'STRUCTURAL',
    'INTEREST_RATE': 'STRUCTURAL',
    'GDP_GROWTH': 'STRUCTURAL',
    'INFLATION': 'STRUCTURAL',
    
    # CYCLICAL (사이클, 시장 상황에 따라)
    'DRAM_ASP': 'CYCLICAL',
    'NAND_ASP': 'CYCLICAL',
    'SEMICONDUCTOR_CAPEX': 'CYCLICAL',
    'SEMICONDUCTOR_DEMAND': 'CYCLICAL',
    'AUTO_SALES': 'CYCLICAL',
    'HOUSING_STARTS': 'CYCLICAL',
    'CONSTRUCTION_ORDERS': 'CYCLICAL',
    
    # EVENT_DRIVEN (이벤트 기반, 특정 상황에서만)
    'OIL_PRICE': 'EVENT_DRIVEN',
    'FDA_APPROVAL': 'EVENT_DRIVEN',
    'TOURIST_ARRIVALS': 'EVENT_DRIVEN',
    'HBM_DEMAND': 'EVENT_DRIVEN',  # AI 붐 이벤트
    'AI_SERVER_CAPEX': 'EVENT_DRIVEN',
}


def check_semantic_group_diversity(db):
    """
    A. Semantic Group 경계 모호성 검사
    Top-3 Semantic Group이 서로 다른 관점을 주는지 확인
    """
    print('=' * 70)
    print('A. Semantic Group Diversity Check (Top-3)')
    print('=' * 70)
    
    # 기업별 Top-5 드라이버
    result = db.execute(text('''
        WITH ranked AS (
            SELECT 
                source_id,
                target_id,
                weight,
                ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY weight DESC) as rn
            FROM edges
            WHERE relation_type = 'DRIVEN_BY'
        )
        SELECT source_id, target_id, weight
        FROM ranked
        WHERE rn <= 5
        ORDER BY source_id, rn
    '''))
    
    company_drivers = defaultdict(list)
    for source_id, target_id, weight in result:
        company_drivers[source_id].append((target_id, weight))
    
    # 다양성 분석
    diversity_stats = {
        '1_group': 0,
        '2_groups': 0,
        '3+_groups': 0,
    }
    
    low_diversity_examples = []
    
    for ticker, drivers in company_drivers.items():
        # Top-3 드라이버의 그룹
        top3_groups = set()
        for driver, _ in drivers[:3]:
            group = DRIVER_TO_GROUP.get(driver, 'OTHER')
            top3_groups.add(group)
        
        unique_groups = len(top3_groups)
        
        if unique_groups == 1:
            diversity_stats['1_group'] += 1
            low_diversity_examples.append((ticker, drivers[:3]))
        elif unique_groups == 2:
            diversity_stats['2_groups'] += 1
        else:
            diversity_stats['3+_groups'] += 1
    
    total = len(company_drivers)
    
    print(f'\nTotal companies: {total}')
    print(f'\nTop-3 Semantic Group Diversity:')
    print(f'  1 group only: {diversity_stats["1_group"]} ({diversity_stats["1_group"]/total*100:.1f}%)')
    print(f'  2 groups: {diversity_stats["2_groups"]} ({diversity_stats["2_groups"]/total*100:.1f}%)')
    print(f'  3+ groups: {diversity_stats["3+_groups"]} ({diversity_stats["3+_groups"]/total*100:.1f}%)')
    
    diverse_rate = (diversity_stats['2_groups'] + diversity_stats['3+_groups']) / total * 100
    print(f'\n  [METRIC] 2+ groups rate: {diverse_rate:.1f}%')
    
    if diverse_rate < 70:
        print(f'  [WARNING] Diversity rate is below 70% threshold')
    else:
        print(f'  [OK] Diversity rate is acceptable')
    
    # 낮은 다양성 예시
    print('\n--- Low Diversity Examples (1 group only) ---')
    for ticker, drivers in low_diversity_examples[:5]:
        result = db.execute(text('SELECT stock_name FROM stocks WHERE ticker = :t'), {'t': ticker})
        name = result.fetchone()
        name = name[0] if name else ticker
        
        group = DRIVER_TO_GROUP.get(drivers[0][0], 'OTHER')
        group_name = DRIVER_SEMANTIC_GROUPS.get(group, {}).get('display_name', group)
        
        print(f'  {ticker} ({name}):')
        print(f'    Drivers: {[d[0] for d in drivers]}')
        print(f'    All belong to: {group_name}')
    
    return diverse_rate


def check_direction_sanity(db):
    """
    B. 방향성(Direction) 정합성 검증
    """
    print('\n' + '=' * 70)
    print('B. Direction Sanity Check')
    print('=' * 70)
    
    test_cases = [
        # (ticker, driver, expected_direction, reason)
        ('003490', 'OIL_PRICE', 'NEGATIVE', '항공사 - 유가 상승 시 연료비 증가'),
        ('035420', 'INTEREST_RATE', 'NEGATIVE', 'IT/성장주 - 금리 상승 시 밸류에이션 하락'),
        ('005380', 'EXCHANGE_RATE_USD_KRW', 'POSITIVE', '자동차 수출 - 원화 약세 시 경쟁력 상승'),
        ('005930', 'DRAM_ASP', 'POSITIVE', '반도체 - DRAM 가격 상승 시 매출 증가'),
        ('000660', 'HBM_DEMAND', 'POSITIVE', '반도체 - HBM 수요 증가 시 매출 증가'),
        ('051910', 'OIL_PRICE', 'NEGATIVE', '화학 - 유가 상승 시 원가 부담'),
    ]
    
    print('\n--- Direction Validation ---')
    issues = []
    
    for ticker, driver, expected, reason in test_cases:
        # 해당 기업의 섹터 확인
        result = db.execute(text('''
            SELECT stock_name, sector_l1 FROM stocks s
            JOIN investor_sector i ON s.ticker = i.ticker
            WHERE s.ticker = :ticker AND i.is_primary = true
        '''), {'ticker': ticker})
        row = result.fetchone()
        
        if not row:
            print(f'  [SKIP] {ticker}: Not found')
            continue
        
        name, sector = row
        
        # 방향성 규칙 확인
        driver_rules = DRIVER_DIRECTION_RULES.get(driver, {})
        sector_rules = driver_rules.get('sector_rules', {})
        actual_direction = sector_rules.get(sector, driver_rules.get('default', 'MIXED'))
        
        status = 'OK' if actual_direction == expected else 'MISMATCH'
        
        print(f'\n  {ticker} ({name}):')
        print(f'    Driver: {driver}')
        print(f'    Sector: {sector}')
        print(f'    Expected: {expected}')
        print(f'    Actual Rule: {actual_direction}')
        print(f'    Reason: {reason}')
        print(f'    Status: [{status}]')
        
        if status == 'MISMATCH':
            issues.append((ticker, name, driver, expected, actual_direction))
    
    # 대한항공 특별 체크
    print('\n--- Special Check: Korean Air (003490) ---')
    result = db.execute(text('''
        SELECT target_id, weight 
        FROM edges 
        WHERE source_id = '003490' AND relation_type = 'DRIVEN_BY'
        ORDER BY weight DESC
        LIMIT 5
    '''))
    drivers = list(result)
    print(f'  Top-5 drivers: {[(d[0], d[1]) for d in drivers]}')
    
    # OIL_PRICE가 있는지 확인
    has_oil = any(d[0] == 'OIL_PRICE' for d in drivers)
    if has_oil:
        print(f'  [CHECK] OIL_PRICE is in driver list')
        print(f'  [NOTE] Direction should be NEGATIVE (cost increase)')
    else:
        print(f'  [INFO] OIL_PRICE not in top drivers')
    
    # 요약
    print('\n--- Direction Check Summary ---')
    if issues:
        print(f'  [WARNING] Found {len(issues)} direction mismatches')
        for t, n, d, exp, act in issues:
            print(f'    - {n}: {d} expected {exp}, rule says {act}')
    else:
        print('  [OK] All direction rules are consistent')
    
    return issues


def check_value_chain_driver_conflict(db):
    """
    C. VALUE_CHAIN ↔ DRIVEN_BY 충돌 체크
    DOWN_SERVICE 기업인데 Top-3가 전부 원자재/제조 변수인 경우
    """
    print('\n' + '=' * 70)
    print('C. VALUE_CHAIN vs DRIVEN_BY Conflict Check')
    print('=' * 70)
    
    # 제조/원자재 관련 그룹
    manufacturing_groups = {'COMMODITY_ENERGY', 'STEEL_METAL', 'SEMICONDUCTOR_CYCLE', 'MEMORY_PRICE'}
    
    # DOWN_SERVICE 기업 조회
    result = db.execute(text('''
        SELECT ticker, sector_l1 FROM investor_sector 
        WHERE value_chain = 'DOWN_SERVICE' AND is_primary = true
    '''))
    service_companies = list(result)
    
    print(f'\nDOWN_SERVICE companies: {len(service_companies)}')
    
    conflicts = []
    
    for ticker, sector in service_companies:
        # Top-3 드라이버 조회
        result = db.execute(text('''
            SELECT target_id FROM edges
            WHERE source_id = :ticker AND relation_type = 'DRIVEN_BY'
            ORDER BY weight DESC
            LIMIT 3
        '''), {'ticker': ticker})
        
        top3 = [r[0] for r in result]
        if not top3:
            continue
        
        # Top-3의 그룹 확인
        groups = [DRIVER_TO_GROUP.get(d, 'OTHER') for d in top3]
        manufacturing_count = sum(1 for g in groups if g in manufacturing_groups)
        
        # 모든 Top-3가 제조/원자재 그룹이면 충돌
        if manufacturing_count >= 2:  # 3개 중 2개 이상
            result = db.execute(text('SELECT stock_name FROM stocks WHERE ticker = :t'), {'t': ticker})
            name = result.fetchone()
            name = name[0] if name else ticker
            conflicts.append((ticker, name, sector, top3, groups))
    
    print(f'\nConflicts found: {len(conflicts)}')
    
    print('\n--- Conflict Examples (Service company with manufacturing drivers) ---')
    for ticker, name, sector, top3, groups in conflicts[:10]:
        print(f'\n  {ticker} ({name}):')
        print(f'    Sector: {sector}')
        print(f'    Value Chain: DOWN_SERVICE')
        print(f'    Top-3 Drivers: {top3}')
        print(f'    Groups: {groups}')
        print(f'    [EXPLAIN] This company is service-focused but has cost sensitivity to raw materials.')
    
    # 권장 사항
    print('\n--- Recommendation ---')
    if len(conflicts) > 20:
        print(f'[ACTION] {len(conflicts)} companies need auto-annotation:')
        print('  "This company is service-focused but has supply chain cost sensitivity."')
    else:
        print(f'[OK] Only {len(conflicts)} conflicts - acceptable for complex conglomerates')
    
    return conflicts


def check_grouping_precision(db):
    """
    D. 그룹핑 정밀도 확인
    메모리 가격 그룹이 비메모리 기업에 연결되었는지 확인
    """
    print('\n' + '=' * 70)
    print('D. Grouping Precision Check (Memory Group)')
    print('=' * 70)
    
    # 비메모리 반도체 기업 (파운드리, 팹리스, 장비)
    non_memory_companies = [
        ('000990', 'DB하이텍', '비메모리 파운드리'),
        ('058470', '리노공업', '반도체 검사장비'),
        ('042700', '한미반도체', '반도체 장비'),
        ('039030', '이오테크닉스', '레이저 장비'),
        ('217190', '제주반도체', '팹리스'),
    ]
    
    memory_drivers = ['DRAM_ASP', 'NAND_ASP']
    
    print('\n--- Non-Memory Semiconductor Companies ---')
    issues = []
    
    for ticker, name, description in non_memory_companies:
        # 해당 기업의 메모리 관련 드라이버 확인
        result = db.execute(text('''
            SELECT target_id, weight 
            FROM edges 
            WHERE source_id = :ticker 
            AND relation_type = 'DRIVEN_BY'
            AND target_id IN ('DRAM_ASP', 'NAND_ASP')
        '''), {'ticker': ticker})
        
        memory_edges = list(result)
        
        print(f'\n  {ticker} ({name} - {description}):')
        
        if memory_edges:
            print(f'    [ISSUE] Has memory price drivers: {[(m[0], m[1]) for m in memory_edges]}')
            issues.append((ticker, name, memory_edges))
        else:
            print(f'    [OK] No memory price drivers')
    
    # 메모리 기업 대조 (삼성전자, SK하이닉스)
    print('\n--- Memory Companies (for comparison) ---')
    memory_companies = [
        ('005930', '삼성전자'),
        ('000660', 'SK하이닉스'),
    ]
    
    for ticker, name in memory_companies:
        result = db.execute(text('''
            SELECT target_id, weight 
            FROM edges 
            WHERE source_id = :ticker 
            AND relation_type = 'DRIVEN_BY'
            AND target_id IN ('DRAM_ASP', 'NAND_ASP')
        '''), {'ticker': ticker})
        
        memory_edges = list(result)
        print(f'  {ticker} ({name}): {[(m[0], m[1]) for m in memory_edges]}')
    
    # 요약
    print('\n--- Grouping Precision Summary ---')
    if issues:
        print(f'[WARNING] {len(issues)} non-memory companies have memory drivers')
        print('Consider adding sub-sector filtering for MEMORY_PRICE group')
    else:
        print('[OK] Memory group is correctly targeted')
    
    return issues


def generate_ir_scenario_json(db):
    """
    F. IR 시나리오 JSON Export
    유가 급등 시나리오
    """
    print('\n' + '=' * 70)
    print('F. IR Scenario JSON Export (Oil Price Surge)')
    print('=' * 70)
    
    # OIL_PRICE 드라이버가 연결된 기업 조회
    result = db.execute(text('''
        SELECT 
            e.source_id as ticker,
            s.stock_name as company_name,
            i.sector_l1,
            i.sector_l2,
            i.value_chain,
            e.weight
        FROM edges e
        JOIN stocks s ON e.source_id = s.ticker
        JOIN investor_sector i ON e.source_id = i.ticker AND i.is_primary = true
        WHERE e.target_id = 'OIL_PRICE' AND e.relation_type = 'DRIVEN_BY'
        ORDER BY e.weight DESC
        LIMIT 20
    '''))
    
    affected_companies = []
    
    for row in result:
        ticker = row.ticker
        
        # 해당 기업의 Top-3 드라이버
        top3_result = db.execute(text('''
            SELECT target_id, weight
            FROM edges
            WHERE source_id = :ticker AND relation_type = 'DRIVEN_BY'
            ORDER BY weight DESC
            LIMIT 3
        '''), {'ticker': ticker})
        
        top3_drivers = []
        for d_row in top3_result:
            group = DRIVER_TO_GROUP.get(d_row[0], None)
            group_name = DRIVER_SEMANTIC_GROUPS.get(group, {}).get('display_name', d_row[0]) if group else d_row[0]
            top3_drivers.append({
                'code': d_row[0],
                'weight': float(d_row[1]),
                'group': group,
                'display_name': group_name,
            })
        
        # 방향성 결정
        driver_rules = DRIVER_DIRECTION_RULES.get('OIL_PRICE', {})
        sector_rules = driver_rules.get('sector_rules', {})
        direction = sector_rules.get(row.sector_l1, driver_rules.get('default', 'MIXED'))
        
        # 이유 텍스트 생성
        reasoning = generate_reasoning(row.sector_l1, row.value_chain, direction, 'OIL_PRICE')
        
        affected_companies.append({
            'ticker': ticker,
            'company_name': row.company_name,
            'sector': row.sector_l1,
            'value_chain': row.value_chain,
            'impact_score': float(row.weight),
            'direction': direction,
            'top3_drivers': top3_drivers,
            'reasoning': reasoning,
        })
    
    scenario_json = {
        'scenario_id': 'OIL_PRICE_SURGE',
        'scenario_name': '유가 급등 시나리오',
        'description': '국제 유가가 급등할 경우 영향을 받는 기업 분석',
        'generated_at': datetime.now().isoformat(),
        'kg_version': 'v1.0',
        'rule_version': 'v1.1',
        'affected_companies': affected_companies,
        'summary': {
            'total_companies': len(affected_companies),
            'positive_impact': len([c for c in affected_companies if c['direction'] == 'POSITIVE']),
            'negative_impact': len([c for c in affected_companies if c['direction'] == 'NEGATIVE']),
            'mixed_impact': len([c for c in affected_companies if c['direction'] == 'MIXED']),
        }
    }
    
    # JSON 출력
    print('\n--- IR Scenario JSON ---')
    print(json.dumps(scenario_json, ensure_ascii=False, indent=2))
    
    # 파일 저장
    output_path = os.path.join(os.path.dirname(__file__), 'ir_scenario_oil_price.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(scenario_json, f, ensure_ascii=False, indent=2)
    
    print(f'\n[SAVED] {output_path}')
    
    return scenario_json


def generate_reasoning(sector, value_chain, direction, driver):
    """
    LLM 스타일의 이유 텍스트 생성 (템플릿 기반)
    """
    templates = {
        ('SEC_TRAVEL', 'NEGATIVE'): '항공/여행업은 유가 상승 시 연료비 부담이 증가하여 수익성이 악화됩니다.',
        ('SEC_SHIP', 'NEGATIVE'): '해운업은 유가 상승 시 연료비 부담이 증가하여 마진이 감소합니다.',
        ('SEC_OIL', 'POSITIVE'): '정유/에너지 기업은 유가 상승 시 제품 판매가격이 상승하여 매출이 증가합니다.',
        ('SEC_CHEM', 'NEGATIVE'): '화학업은 유가 상승 시 나프타 등 원료 가격이 상승하여 원가 부담이 증가합니다.',
        ('SEC_AUTO', 'NEGATIVE'): '자동차 산업은 유가 상승 시 소비자의 차량 구매 심리가 위축될 수 있습니다.',
    }
    
    key = (sector, direction)
    if key in templates:
        return templates[key]
    
    # 기본 템플릿
    if direction == 'POSITIVE':
        return f'{sector} 섹터는 {driver} 상승 시 긍정적 영향을 받을 수 있습니다.'
    elif direction == 'NEGATIVE':
        return f'{sector} 섹터는 {driver} 상승 시 비용 증가 또는 수요 감소의 영향을 받을 수 있습니다.'
    else:
        return f'{sector} 섹터는 {driver} 변화에 복합적인 영향을 받습니다.'


def create_temporal_relevance_mapping():
    """
    E. Temporal Relevance 메타 설계 출력
    """
    print('\n' + '=' * 70)
    print('E. Temporal Relevance Metadata Design')
    print('=' * 70)
    
    print('\n--- Temporal Relevance Categories ---')
    print('  STRUCTURAL: Always important (e.g., Interest Rate, FX)')
    print('  CYCLICAL: Depends on market cycle (e.g., DRAM price)')
    print('  EVENT_DRIVEN: Only during specific events (e.g., Oil shock)')
    
    print('\n--- Proposed Mapping ---')
    for driver, temporal in sorted(TEMPORAL_RELEVANCE_MAPPING.items()):
        print(f'  {driver}: {temporal}')
    
    print('\n--- Schema Extension ---')
    print('''
-- Add to economic_variables table (V1.2)
ALTER TABLE economic_variables 
ADD COLUMN temporal_relevance VARCHAR(20) 
CHECK (temporal_relevance IN ('STRUCTURAL', 'CYCLICAL', 'EVENT_DRIVEN'));

-- Or add to edge properties
-- edges.properties['temporal_relevance'] = 'CYCLICAL'
''')
    
    return TEMPORAL_RELEVANCE_MAPPING


def main():
    print('=' * 70)
    print('KG FINAL VALIDATION')
    print('=' * 70)
    print()
    
    db = next(get_db())
    
    # A. Semantic Group 다양성
    diversity_rate = check_semantic_group_diversity(db)
    
    # B. 방향성 검증
    direction_issues = check_direction_sanity(db)
    
    # C. VALUE_CHAIN 충돌
    vc_conflicts = check_value_chain_driver_conflict(db)
    
    # D. 그룹핑 정밀도
    grouping_issues = check_grouping_precision(db)
    
    # E. Temporal Relevance
    temporal_mapping = create_temporal_relevance_mapping()
    
    # F. IR 시나리오 JSON
    scenario_json = generate_ir_scenario_json(db)
    
    # Final Summary
    print('\n' + '=' * 70)
    print('FINAL VALIDATION SUMMARY')
    print('=' * 70)
    
    print('\n[A] Semantic Group Diversity:')
    print(f'    2+ groups rate: {diversity_rate:.1f}%')
    print(f'    Status: {"OK" if diversity_rate >= 70 else "NEEDS ATTENTION"}')
    
    print('\n[B] Direction Sanity:')
    print(f'    Issues found: {len(direction_issues)}')
    print(f'    Status: {"OK" if len(direction_issues) == 0 else "REVIEW NEEDED"}')
    
    print('\n[C] VALUE_CHAIN Conflicts:')
    print(f'    Conflicts: {len(vc_conflicts)}')
    print(f'    Status: {"OK" if len(vc_conflicts) <= 20 else "ADD AUTO-ANNOTATION"}')
    
    print('\n[D] Grouping Precision:')
    print(f'    Issues: {len(grouping_issues)}')
    print(f'    Status: {"OK" if len(grouping_issues) == 0 else "REVIEW NEEDED"}')
    
    print('\n[E] Temporal Relevance:')
    print(f'    Mapping defined: {len(temporal_mapping)} drivers')
    print(f'    Status: DESIGN READY (implement in V1.2)')
    
    print('\n[F] IR Scenario JSON:')
    print(f'    Companies affected: {scenario_json["summary"]["total_companies"]}')
    print(f'    Status: EXPORTED')


if __name__ == '__main__':
    main()

