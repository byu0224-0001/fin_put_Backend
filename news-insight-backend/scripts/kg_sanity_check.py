# -*- coding: utf-8 -*-
"""
KG Sanity Check Script
IR 직전 검증을 위한 스크립트

P0-A Checks:
1. 대표 기업 로직 검증 (삼성전자/대한항공)
2. 매크로 시나리오 Top-K (OIL_PRICE → Top 10)
3. DRIVEN_BY 의미 밀도 체크

P0-B Checks:
4. DRIVEN_BY 재현성 (rule_version)
5. SUPPLIES_TO vs POTENTIAL 위계
"""

import sys
import os
import codecs
import json
from collections import defaultdict

# 인코딩 설정
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from app.db import get_db
from sqlalchemy import text


def check_representative_companies(db):
    """
    P0-A-1: 대표 기업 로직 검증
    삼성전자, 대한항공의 DRIVEN_BY 연결이 상식적인지 확인
    """
    print('=' * 70)
    print('P0-A-1: Representative Company Logic Check')
    print('=' * 70)
    
    # 대표 기업 목록
    target_companies = [
        ('005930', '삼성전자', 'SEC_SEMI', 'MID_HARD'),  # 반도체/제조
        ('003490', '대한항공', 'SEC_TRAVEL', 'DOWN_SERVICE'),  # 항공/서비스
        ('000660', 'SK하이닉스', 'SEC_SEMI', 'MID_HARD'),  # 반도체/제조
        ('035420', '네이버', 'SEC_IT', 'DOWN_SERVICE'),  # IT/플랫폼
        ('005380', '현대자동차', 'SEC_AUTO', 'MID_HARD'),  # 자동차/제조
        ('096770', 'SK이노베이션', 'SEC_CHEM', 'UPSTREAM'),  # 정유/원자재
    ]
    
    anomalies = []
    
    for ticker, name, expected_sector, expected_vc in target_companies:
        print(f'\n--- {ticker} ({name}) ---')
        print(f'Expected: {expected_sector} / {expected_vc}')
        
        # 실제 분류 확인
        result = db.execute(text('''
            SELECT sector_l1, value_chain, ROUND(value_chain_confidence::numeric, 3)
            FROM investor_sector
            WHERE ticker = :ticker AND is_primary = true
        '''), {'ticker': ticker})
        row = result.fetchone()
        
        if row:
            actual_sector, actual_vc, conf = row
            print(f'Actual: {actual_sector} / {actual_vc} (conf={conf})')
            
            # 섹터/VC 불일치 체크
            if actual_sector != expected_sector:
                anomalies.append(f'{name}: sector mismatch ({expected_sector} vs {actual_sector})')
            if actual_vc != expected_vc:
                anomalies.append(f'{name}: value_chain mismatch ({expected_vc} vs {actual_vc})')
        else:
            print('  [WARNING] Company not found in investor_sector')
            anomalies.append(f'{name}: not found')
            continue
        
        # DRIVEN_BY 연결 확인
        result = db.execute(text('''
            SELECT target_id, weight, properties
            FROM edges
            WHERE source_id = :ticker AND relation_type = 'DRIVEN_BY'
            ORDER BY weight DESC
            LIMIT 10
        '''), {'ticker': ticker})
        
        print(f'\nTop 10 DRIVEN_BY edges:')
        drivers = []
        for row in result:
            driver, weight, props = row
            props_dict = props if isinstance(props, dict) else {}
            text_match = props_dict.get('text_match_weight', 0)
            source = props_dict.get('source', 'unknown')
            print(f'  {driver}: weight={weight}, text_match={text_match}, source={source}')
            drivers.append(driver)
        
        # 상식 체크: 특정 드라이버가 있어야 하는지
        expected_drivers = {
            '삼성전자': ['DRAM_ASP', 'NAND_ASP', 'HBM_DEMAND', 'SEMICONDUCTOR_CAPEX'],
            '대한항공': ['OIL_PRICE', 'TOURIST_ARRIVALS', 'AIRLINE_PASSENGERS'],
            'SK하이닉스': ['DRAM_ASP', 'NAND_ASP', 'HBM_DEMAND'],
            '네이버': ['ENTERPRISE_IT_SPENDING', 'CLOUD_ADOPTION', 'DIGITAL_ADOPTION'],
            '현대자동차': ['AUTO_SALES', 'EV_SALES', 'STEEL_PRICE'],
            'SK이노베이션': ['OIL_PRICE', 'NAPHTHA_PRICE'],
        }
        
        if name in expected_drivers:
            missing = [d for d in expected_drivers[name] if d not in drivers]
            if missing:
                print(f'  [WARNING] Missing expected drivers: {missing}')
                anomalies.append(f'{name}: missing drivers {missing}')
    
    print('\n' + '=' * 70)
    print('P0-A-1 Summary:')
    if anomalies:
        print(f'  [WARNING] Found {len(anomalies)} anomalies:')
        for a in anomalies:
            print(f'    - {a}')
    else:
        print('  [OK] All representative companies passed sanity check')
    
    return anomalies


def check_macro_scenario_topk(db):
    """
    P0-A-2: 매크로 시나리오 Top-K
    OIL_PRICE, EXCHANGE_RATE 등 주요 변수 → 영향 기업 Top 10
    """
    print('\n' + '=' * 70)
    print('P0-A-2: Macro Scenario Top-K Check')
    print('=' * 70)
    
    scenarios = [
        ('OIL_PRICE', ['SEC_CHEM', 'SEC_SHIP', 'SEC_TRAVEL', 'SEC_UTIL']),
        ('EXCHANGE_RATE_USD_KRW', ['SEC_AUTO', 'SEC_SEMI', 'SEC_ELECTRONICS']),
        ('INTEREST_RATE', ['SEC_FINANCE', 'SEC_CONST', 'SEC_REIT']),
        ('EV_SALES', ['SEC_AUTO', 'SEC_BATTERY']),
        ('DRAM_ASP', ['SEC_SEMI']),
    ]
    
    anomalies = []
    
    for driver, expected_sectors in scenarios:
        print(f'\n--- {driver} Impact Analysis ---')
        print(f'Expected sectors: {expected_sectors}')
        
        # Top 10 영향 기업
        result = db.execute(text('''
            SELECT e.source_id, s.stock_name, i.sector_l1, i.value_chain, e.weight
            FROM edges e
            JOIN stocks s ON e.source_id = s.ticker
            JOIN investor_sector i ON e.source_id = i.ticker AND i.is_primary = true
            WHERE e.target_id = :driver AND e.relation_type = 'DRIVEN_BY'
            ORDER BY e.weight DESC
            LIMIT 10
        '''), {'driver': driver})
        
        rows = list(result)
        
        if not rows:
            print(f'  [WARNING] No companies found for {driver}')
            anomalies.append(f'{driver}: no edges found')
            continue
        
        print(f'\nTop 10 Companies:')
        sector_counts = defaultdict(int)
        for ticker, name, sector, vc, weight in rows:
            print(f'  {ticker} ({name}): {sector}/{vc}, weight={weight}')
            sector_counts[sector] += 1
        
        # 섹터 분포 체크
        print(f'\nSector Distribution in Top 10:')
        for sector, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
            status = 'OK' if sector in expected_sectors else 'UNEXPECTED'
            print(f'  {sector}: {count} [{status}]')
            
            if sector not in expected_sectors and count >= 3:
                anomalies.append(f'{driver}: unexpected sector {sector} dominates ({count}/10)')
    
    print('\n' + '=' * 70)
    print('P0-A-2 Summary:')
    if anomalies:
        print(f'  [WARNING] Found {len(anomalies)} anomalies:')
        for a in anomalies:
            print(f'    - {a}')
    else:
        print('  [OK] All macro scenarios passed sanity check')
    
    return anomalies


def check_driven_by_density(db):
    """
    P0-A-3: DRIVEN_BY 의미 밀도 체크
    기업당 DRIVEN_BY 개수 분포 분석
    """
    print('\n' + '=' * 70)
    print('P0-A-3: DRIVEN_BY Density Check')
    print('=' * 70)
    
    # 기업당 DRIVEN_BY 개수
    result = db.execute(text('''
        SELECT source_id, COUNT(*) as edge_count
        FROM edges
        WHERE relation_type = 'DRIVEN_BY'
        GROUP BY source_id
        ORDER BY edge_count DESC
    '''))
    
    counts = [row[1] for row in result]
    
    if not counts:
        print('  [ERROR] No DRIVEN_BY edges found')
        return ['No DRIVEN_BY edges']
    
    # 통계
    import statistics
    avg = statistics.mean(counts)
    median = statistics.median(counts)
    p90 = sorted(counts)[int(len(counts) * 0.9)]
    p95 = sorted(counts)[int(len(counts) * 0.95)]
    max_count = max(counts)
    min_count = min(counts)
    
    print(f'Total companies with DRIVEN_BY: {len(counts)}')
    print(f'Min edges per company: {min_count}')
    print(f'Max edges per company: {max_count}')
    print(f'Average: {avg:.1f}')
    print(f'Median: {median}')
    print(f'P90: {p90}')
    print(f'P95: {p95}')
    
    # 분포
    print('\nEdge Count Distribution:')
    buckets = {
        '1-3': 0,
        '4-6': 0,
        '7-10': 0,
        '11-15': 0,
        '16+': 0,
    }
    for c in counts:
        if c <= 3:
            buckets['1-3'] += 1
        elif c <= 6:
            buckets['4-6'] += 1
        elif c <= 10:
            buckets['7-10'] += 1
        elif c <= 15:
            buckets['11-15'] += 1
        else:
            buckets['16+'] += 1
    
    for bucket, cnt in buckets.items():
        pct = cnt / len(counts) * 100
        print(f'  {bucket} edges: {cnt} ({pct:.1f}%)')
    
    # Top-K 정책 제안
    print('\n--- Top-K Policy Recommendation ---')
    print(f'P90 ({p90} edges) suggests: Show Top-{min(p90, 5)} drivers per company')
    print(f'Recommended: Top-3 for UI, Top-5 for detailed view')
    
    # 과다 연결 기업 확인
    print('\n--- Companies with 15+ DRIVEN_BY edges ---')
    result = db.execute(text('''
        SELECT e.source_id, s.stock_name, i.sector_l1, COUNT(*) as edge_count
        FROM edges e
        JOIN stocks s ON e.source_id = s.ticker
        JOIN investor_sector i ON e.source_id = i.ticker AND i.is_primary = true
        WHERE e.relation_type = 'DRIVEN_BY'
        GROUP BY e.source_id, s.stock_name, i.sector_l1
        HAVING COUNT(*) >= 15
        ORDER BY edge_count DESC
        LIMIT 10
    '''))
    
    high_density = list(result)
    if high_density:
        for ticker, name, sector, count in high_density:
            print(f'  {ticker} ({name}): {sector}, {count} edges')
    else:
        print('  None (good!)')
    
    anomalies = []
    if max_count > 20:
        anomalies.append(f'Max edges ({max_count}) too high - consider capping')
    if avg > 10:
        anomalies.append(f'Average edges ({avg:.1f}) high - may need filtering')
    
    print('\n' + '=' * 70)
    print('P0-A-3 Summary:')
    if anomalies:
        print(f'  [WARNING] {len(anomalies)} concerns:')
        for a in anomalies:
            print(f'    - {a}')
    else:
        print('  [OK] DRIVEN_BY density is reasonable')
    
    return anomalies


def check_driven_by_reproducibility(db):
    """
    P0-B-1: DRIVEN_BY 재현성 체크
    rule_version, mapping_version 기록 여부
    """
    print('\n' + '=' * 70)
    print('P0-B-1: DRIVEN_BY Reproducibility Check')
    print('=' * 70)
    
    # properties에 source 정보가 있는지 확인
    result = db.execute(text('''
        SELECT 
            properties->>'source' as source,
            COUNT(*) as cnt
        FROM edges
        WHERE relation_type = 'DRIVEN_BY'
        GROUP BY properties->>'source'
    '''))
    
    print('DRIVEN_BY sources:')
    for row in result:
        print(f'  {row[0]}: {row[1]}')
    
    # 샘플 엣지의 properties 확인
    print('\nSample edge properties:')
    result = db.execute(text('''
        SELECT source_id, target_id, properties
        FROM edges
        WHERE relation_type = 'DRIVEN_BY'
        ORDER BY RANDOM()
        LIMIT 3
    '''))
    
    for row in result:
        props = row[2] if isinstance(row[2], dict) else {}
        print(f'  {row[0]} -> {row[1]}:')
        print(f'    {json.dumps(props, ensure_ascii=False, indent=6)}')
    
    # 재현성 정책 제안
    print('\n--- Reproducibility Policy ---')
    print('Current: source (sector_mapping / value_chain_boost)')
    print('Recommended additions:')
    print('  - rule_version: "v1.0"')
    print('  - created_at: timestamp')
    print('  - mapping_hash: hash of sector-driver mapping')
    
    return []


def check_supply_chain_hierarchy(db):
    """
    P0-B-2: SUPPLIES_TO vs POTENTIAL_SUPPLIES_TO 위계 확인
    """
    print('\n' + '=' * 70)
    print('P0-B-2: Supply Chain Hierarchy Check')
    print('=' * 70)
    
    # 각 유형별 개수
    result = db.execute(text('''
        SELECT relation_type, COUNT(*) as cnt
        FROM edges
        WHERE relation_type IN ('SUPPLIES_TO', 'POTENTIAL_SUPPLIES_TO', 'SELLS_TO')
        GROUP BY relation_type
    '''))
    
    print('Supply Chain Edge Counts:')
    counts = {}
    for row in result:
        print(f'  {row[0]}: {row[1]}')
        counts[row[0]] = row[1]
    
    supplies_to = counts.get('SUPPLIES_TO', 0)
    potential = counts.get('POTENTIAL_SUPPLIES_TO', 0)
    
    # 비율 분석
    if supplies_to > 0 and potential > 0:
        ratio = potential / supplies_to
        print(f'\nPOTENTIAL / CONFIRMED ratio: {ratio:.1f}x')
        
        if ratio > 5:
            print('  [WARNING] POTENTIAL edges may be too many')
    
    # SUPPLIES_TO 샘플
    print('\n--- SUPPLIES_TO (Confirmed) Samples ---')
    result = db.execute(text('''
        SELECT e.source_id, s1.stock_name as source_name, 
               e.target_id, s2.stock_name as target_name
        FROM edges e
        LEFT JOIN stocks s1 ON e.source_id = s1.ticker
        LEFT JOIN stocks s2 ON e.target_id = s2.ticker
        WHERE e.relation_type = 'SUPPLIES_TO'
        ORDER BY RANDOM()
        LIMIT 5
    '''))
    
    for row in result:
        print(f'  {row[0]} ({row[1] or "?"}) --> {row[2]} ({row[3] or "?"})')
    
    # POTENTIAL_SUPPLIES_TO 샘플
    print('\n--- POTENTIAL_SUPPLIES_TO Samples ---')
    result = db.execute(text('''
        SELECT e.source_id, s1.stock_name as source_name, 
               e.target_id, s2.stock_name as target_name
        FROM edges e
        LEFT JOIN stocks s1 ON e.source_id = s1.ticker
        LEFT JOIN stocks s2 ON e.target_id = s2.ticker
        WHERE e.relation_type = 'POTENTIAL_SUPPLIES_TO'
        ORDER BY RANDOM()
        LIMIT 5
    '''))
    
    for row in result:
        print(f'  {row[0]} ({row[1] or "?"}) --> {row[2]} ({row[3] or "?"})')
    
    # 정책 제안
    print('\n--- Hierarchy Policy Recommendation ---')
    print('SUPPLIES_TO: Show in all views (confirmed)')
    print('POTENTIAL_SUPPLIES_TO: Hide in basic view, show in "exploration" mode')
    print('SELLS_TO: Customer relationship, show separately')
    
    anomalies = []
    if potential > supplies_to * 10:
        anomalies.append(f'POTENTIAL ({potential}) >> CONFIRMED ({supplies_to}) - may confuse users')
    
    return anomalies


def main():
    print('=' * 70)
    print('KG SANITY CHECK - IR Preparation')
    print('=' * 70)
    print()
    
    db = next(get_db())
    
    all_anomalies = []
    
    # P0-A Checks
    all_anomalies.extend(check_representative_companies(db))
    all_anomalies.extend(check_macro_scenario_topk(db))
    all_anomalies.extend(check_driven_by_density(db))
    
    # P0-B Checks
    all_anomalies.extend(check_driven_by_reproducibility(db))
    all_anomalies.extend(check_supply_chain_hierarchy(db))
    
    # Final Summary
    print('\n' + '=' * 70)
    print('FINAL SANITY CHECK SUMMARY')
    print('=' * 70)
    
    if all_anomalies:
        print(f'\n[ATTENTION] Total anomalies found: {len(all_anomalies)}')
        for i, a in enumerate(all_anomalies, 1):
            print(f'  {i}. {a}')
        print('\nRecommendation: Review and fix before IR')
    else:
        print('\n[PASS] All sanity checks passed!')
        print('KG is ready for IR demonstration')
    
    print('\n' + '=' * 70)
    print('IR Display Policy Recommendations:')
    print('=' * 70)
    print('1. DRIVEN_BY: Show Top-3 per company (UI), Top-5 (detail)')
    print('2. SUPPLIES_TO: Show confirmed only, hide POTENTIAL')
    print('3. Weight threshold: >= 0.5 for primary display')
    print('4. Add rule_version to all edges for reproducibility')


if __name__ == '__main__':
    main()

