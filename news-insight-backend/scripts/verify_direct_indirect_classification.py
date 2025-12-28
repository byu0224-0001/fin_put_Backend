# -*- coding: utf-8 -*-
"""
P0-A: Direct/Indirect 분기 오탐 샘플링 검증

Phase 1 Freeze 직전 QA용 스크립트
- OIL_PRICE에서 DIRECT/INDIRECT Top 50 샘플링
- INTEREST_RATE에서 DIRECT/INDIRECT Top 50 샘플링
- 사람이 빠르게 훑어볼 수 있는 리포트 생성
"""

import sys
import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.db import engine
from app.services.kg_explanation_layer import (
    classify_impact_nature,
    get_variable_korean_name,
)

def sample_direct_indirect_classification(
    variable: str,
    limit: int = 50,
    db=None
):
    """
    Direct/Indirect 분기 샘플링
    
    Returns:
        {
            'direct': [...],  # DIRECT로 분류된 기업
            'indirect': [...],  # INDIRECT로 분류된 기업
        }
    """
    if db is None:
        db = engine.connect()
    
    # 해당 변수에 연결된 기업 조회
    result = db.execute(text('''
        SELECT DISTINCT
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
        WHERE e.target_id = :variable
        AND e.relation_type = 'DRIVEN_BY'
        AND e.weight >= 0.3
        ORDER BY e.weight DESC
        LIMIT :limit
    '''), {
        'variable': variable,
        'limit': limit * 2  # 여유있게 가져온 후 분류
    })
    
    direct_companies = []
    indirect_companies = []
    
    for row in result:
        ticker = row[0]
        name = row[1]
        sector_l1 = row[2]
        value_chain = row[3]
        weight = float(row[4]) if row[4] else 0.5
        props = row[5] if isinstance(row[5], dict) else {}
        biz_summary = row[6] or ''
        
        mechanism = props.get('mechanism', 'DEMAND')
        
        # Direct/Indirect 판정
        impact_info = classify_impact_nature(
            variable=variable,
            sector_l1=sector_l1,
            mechanism=mechanism,
            evidence_snippets=[],
            biz_summary=biz_summary
        )
        
        company_data = {
            'ticker': ticker,
            'name': name,
            'sector_l1': sector_l1,
            'value_chain': value_chain,
            'weight': weight,
            'mechanism': mechanism,
            'nature': impact_info['nature'],
            'reason': impact_info['reason'],
            'channel': impact_info.get('channel'),
        }
        
        if impact_info['nature'] == 'DIRECT':
            direct_companies.append(company_data)
        else:
            indirect_companies.append(company_data)
    
    # Top 50만 선택
    direct_companies = sorted(direct_companies, key=lambda x: x['weight'], reverse=True)[:limit]
    indirect_companies = sorted(indirect_companies, key=lambda x: x['weight'], reverse=True)[:limit]
    
    return {
        'variable': variable,
        'variable_kr': get_variable_korean_name(variable),
        'direct': direct_companies,
        'indirect': indirect_companies,
    }


def print_qa_report(variable: str, limit: int = 50):
    """QA 리포트 출력"""
    print('=' * 80)
    print(f'[P0-A] Direct/Indirect 분기 오탐 샘플링 검증')
    print(f'변수: {get_variable_korean_name(variable)} ({variable})')
    print('=' * 80)
    
    results = sample_direct_indirect_classification(variable, limit)
    
    print(f'\n[DIRECT 분류] (Top {len(results["direct"])})')
    print('-' * 80)
    print(f'{"순위":<5} {"종목코드":<8} {"기업명":<20} {"섹터":<15} {"가중치":<8} {"판정 근거":<30}')
    print('-' * 80)
    
    for i, c in enumerate(results['direct'][:20], 1):  # 상위 20개만 출력
        print(f'{i:<5} {c["ticker"]:<8} {c["name"]:<20} {c["sector_l1"]:<15} {c["weight"]:<8.2f} {c["reason"][:30]:<30}')
    
    if len(results['direct']) > 20:
        print(f'... 외 {len(results["direct"]) - 20}개')
    
    print(f'\n[INDIRECT 분류] (Top {len(results["indirect"])})')
    print('-' * 80)
    print(f'{"순위":<5} {"종목코드":<8} {"기업명":<20} {"섹터":<15} {"가중치":<8} {"채널":<15} {"판정 근거":<30}')
    print('-' * 80)
    
    for i, c in enumerate(results['indirect'][:20], 1):  # 상위 20개만 출력
        channel = c.get('channel', 'N/A')
        print(f'{i:<5} {c["ticker"]:<8} {c["name"]:<20} {c["sector_l1"]:<15} {c["weight"]:<8.2f} {channel:<15} {c["reason"][:30]:<30}')
    
    if len(results['indirect']) > 20:
        print(f'... 외 {len(results["indirect"]) - 20}개')
    
    # 섹터별 분포
    print(f'\n[섹터별 분포]')
    print('-' * 80)
    
    from collections import Counter
    direct_sectors = Counter([c['sector_l1'] for c in results['direct']])
    indirect_sectors = Counter([c['sector_l1'] for c in results['indirect']])
    
    print('DIRECT 섹터:')
    for sector, count in direct_sectors.most_common(10):
        print(f'  {sector}: {count}개')
    
    print('\nINDIRECT 섹터:')
    for sector, count in indirect_sectors.most_common(10):
        print(f'  {sector}: {count}개')
    
    # 의심 케이스 (섹터가 같은데 분류가 다른 경우)
    print(f'\n[의심 케이스] (같은 섹터인데 분류가 다른 기업)')
    print('-' * 80)
    
    sector_nature = {}
    for c in results['direct'] + results['indirect']:
        sector = c['sector_l1']
        if sector not in sector_nature:
            sector_nature[sector] = []
        sector_nature[sector].append(c['nature'])
    
    suspicious = []
    for sector, natures in sector_nature.items():
        if len(set(natures)) > 1:  # 섹터 내에서 DIRECT/INDIRECT 혼재
            suspicious.append(sector)
    
    if suspicious:
        print(f'의심 섹터: {", ".join(suspicious)}')
        print('→ 이 섹터들은 예외 리스트/디버그 로깅 대상으로 검토 필요')
    else:
        print('의심 케이스 없음')
    
    return results


def save_qa_result_to_log(variable: str, results: Dict, output_dir: str = 'logs'):
    """
    V1.5.4: QA 결과를 JSON 파일로 저장 (회귀 테스트용)
    """
    import json
    from datetime import datetime
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{output_dir}/qa_direct_indirect_{variable}_{timestamp}.json"
    
    log_data = {
        'variable': variable,
        'variable_kr': results.get('variable_kr', ''),
        'timestamp': timestamp,
        'direct_count': len(results.get('direct', [])),
        'indirect_count': len(results.get('indirect', [])),
        'direct_companies': [
            {
                'ticker': c['ticker'],
                'name': c['name'],
                'sector': c['sector_l1'],
                'weight': c['weight'],
                'reason': c['reason'],
            }
            for c in results.get('direct', [])[:20]  # 상위 20개만
        ],
        'indirect_companies': [
            {
                'ticker': c['ticker'],
                'name': c['name'],
                'sector': c['sector_l1'],
                'weight': c['weight'],
                'channel': c.get('channel'),
                'reason': c['reason'],
            }
            for c in results.get('indirect', [])[:20]  # 상위 20개만
        ],
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
    
    print(f'\n[QA 로그 저장] {filename}')
    return filename


if __name__ == '__main__':
    import os
    
    # OIL_PRICE 검증
    oil_results = sample_direct_indirect_classification('OIL_PRICE', limit=50)
    print_qa_report('OIL_PRICE', limit=50)
    save_qa_result_to_log('OIL_PRICE', oil_results)
    
    print('\n' + '=' * 80)
    print('\n')
    
    # INTEREST_RATE 검증
    interest_results = sample_direct_indirect_classification('INTEREST_RATE', limit=50)
    print_qa_report('INTEREST_RATE', limit=50)
    save_qa_result_to_log('INTEREST_RATE', interest_results)
    
    print('\n' + '=' * 80)
    print('P0-A 검증 완료!')
    print('=' * 80)
    print('\n[QA 체크리스트]')
    print('1. DIRECT 분류에서 "이상한" 기업이 있는가? (예: 반도체가 DIRECT)')
    print('2. INDIRECT 분류에서 "직접이어야 할" 기업이 있는가? (예: 정유사가 INDIRECT)')
    print('3. 의심 섹터의 예외 처리가 필요한가?')
    print('4. 판정 근거가 합리적인가?')
    print('\n[QA 로그]')
    print('  - logs/qa_direct_indirect_*.json 파일에 저장됨')
    print('  - Phase 2에서 회귀 테스트용으로 사용')
    print('=' * 80)

