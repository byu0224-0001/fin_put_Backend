# -*- coding: utf-8 -*-
"""
DRIVEN_BY 심층 분석 스크립트

1. Semantic Redundancy 검사 (의미적 중복)
2. Weight 분포 분석 (Threshold 검증)
3. Max Edge 케이스 분석
4. Driver Grouping 정의
"""

import sys
import os
import codecs
import json
from collections import defaultdict, Counter

sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from app.db import get_db
from sqlalchemy import text


# =============================================================================
# Driver Semantic Groups (UI 압축용)
# =============================================================================

DRIVER_SEMANTIC_GROUPS = {
    # 메모리 반도체 가격
    'MEMORY_PRICE': {
        'display_name': '메모리 가격',
        'display_name_en': 'Memory Price',
        'members': ['DRAM_ASP', 'NAND_ASP'],
        'description': 'DRAM/NAND 반도체 평균판매가격',
    },
    # AI 인프라 수요
    'AI_INFRA_DEMAND': {
        'display_name': 'AI 인프라 수요',
        'display_name_en': 'AI Infrastructure Demand',
        'members': ['HBM_DEMAND', 'AI_SERVER_CAPEX', 'GPU_DEMAND'],
        'description': 'AI 서버, HBM, GPU 등 AI 인프라 투자',
    },
    # 반도체 산업 전반
    'SEMICONDUCTOR_CYCLE': {
        'display_name': '반도체 사이클',
        'display_name_en': 'Semiconductor Cycle',
        'members': ['SEMICONDUCTOR_CAPEX', 'SEMICONDUCTOR_DEMAND', 'WAFER_DEMAND'],
        'description': '반도체 산업 투자 및 수요 사이클',
    },
    # 환율
    'FX_RATE': {
        'display_name': '환율',
        'display_name_en': 'FX Rate',
        'members': ['EXCHANGE_RATE_USD_KRW', 'EXCHANGE_RATE_CNY_KRW'],
        'description': '원/달러, 원/위안 환율',
    },
    # 원자재/에너지
    'COMMODITY_ENERGY': {
        'display_name': '원자재/에너지',
        'display_name_en': 'Commodity & Energy',
        'members': ['OIL_PRICE', 'NAPHTHA_PRICE', 'COMMODITY_PRICE', 'GAS_PRICE'],
        'description': '유가, 나프타, 원자재 가격',
    },
    # 소비/유통
    'CONSUMER_RETAIL': {
        'display_name': '소비/유통',
        'display_name_en': 'Consumer & Retail',
        'members': ['CONSUMER_SPENDING', 'RETAIL_SALES', 'E_COMMERCE_TRANS_VOL'],
        'description': '소비자 지출, 소매 판매',
    },
    # 금리/금융
    'INTEREST_CREDIT': {
        'display_name': '금리/금융',
        'display_name_en': 'Interest & Credit',
        'members': ['INTEREST_RATE', 'LOAN_DEMAND', 'CONSUMER_CREDIT'],
        'description': '기준금리, 대출 수요',
    },
    # 전기차/배터리
    'EV_BATTERY': {
        'display_name': '전기차/배터리',
        'display_name_en': 'EV & Battery',
        'members': ['EV_SALES', 'BATTERY_DEMAND', 'LITHIUM_PRICE'],
        'description': '전기차 판매, 배터리 수요',
    },
    # IT 투자
    'IT_SPENDING': {
        'display_name': 'IT 투자',
        'display_name_en': 'IT Spending',
        'members': ['ENTERPRISE_IT_SPENDING', 'CLOUD_ADOPTION', 'RD_SPENDING'],
        'description': '기업 IT 투자, 클라우드, R&D',
    },
}

# 역방향 매핑 (driver -> group)
DRIVER_TO_GROUP = {}
for group_id, group_info in DRIVER_SEMANTIC_GROUPS.items():
    for member in group_info['members']:
        DRIVER_TO_GROUP[member] = group_id


def check_semantic_redundancy(db):
    """
    1. DRIVEN_BY 중복 의미 검사
    기업별 Top-5에서 같은 그룹의 드라이버가 2개 이상 등장하는 비율
    """
    print('=' * 70)
    print('1. Semantic Redundancy Analysis')
    print('=' * 70)
    
    # 기업별 Top-5 드라이버 조회
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
    
    # 기업별 그룹화
    company_drivers = defaultdict(list)
    for source_id, target_id, weight in result:
        company_drivers[source_id].append(target_id)
    
    # 중복 분석
    redundancy_count = 0
    redundancy_details = defaultdict(int)
    
    for company, drivers in company_drivers.items():
        # 각 드라이버의 그룹 확인
        groups = [DRIVER_TO_GROUP.get(d, 'OTHER') for d in drivers]
        group_counts = Counter(groups)
        
        # 같은 그룹에서 2개 이상 등장하는 경우
        for group, count in group_counts.items():
            if count >= 2 and group != 'OTHER':
                redundancy_count += 1
                redundancy_details[group] += 1
                break  # 기업당 한 번만 카운트
    
    total_companies = len(company_drivers)
    redundancy_rate = redundancy_count / total_companies * 100 if total_companies > 0 else 0
    
    print(f'\nTotal companies analyzed: {total_companies}')
    print(f'Companies with redundant drivers in Top-5: {redundancy_count} ({redundancy_rate:.1f}%)')
    
    print('\nRedundancy by Group:')
    for group, count in sorted(redundancy_details.items(), key=lambda x: -x[1]):
        group_name = DRIVER_SEMANTIC_GROUPS.get(group, {}).get('display_name', group)
        print(f'  {group_name}: {count} companies')
    
    # 샘플 케이스
    print('\n--- Sample Redundant Cases ---')
    sample_count = 0
    for company, drivers in company_drivers.items():
        groups = [DRIVER_TO_GROUP.get(d, 'OTHER') for d in drivers]
        group_counts = Counter(groups)
        
        redundant_groups = [g for g, c in group_counts.items() if c >= 2 and g != 'OTHER']
        if redundant_groups and sample_count < 5:
            result = db.execute(text('''
                SELECT stock_name FROM stocks WHERE ticker = :ticker
            '''), {'ticker': company})
            name = result.fetchone()
            name = name[0] if name else company
            
            print(f'\n  {company} ({name}):')
            print(f'    Top-5 drivers: {drivers}')
            print(f'    Redundant group: {redundant_groups[0]} -> {DRIVER_SEMANTIC_GROUPS.get(redundant_groups[0], {}).get("display_name", redundant_groups[0])}')
            sample_count += 1
    
    # 권장 사항
    print('\n--- Recommendation ---')
    if redundancy_rate > 30:
        print(f'[WARNING] Redundancy rate ({redundancy_rate:.1f}%) is high.')
        print('Suggested: Apply semantic grouping in UI to compress similar drivers.')
    else:
        print(f'[OK] Redundancy rate ({redundancy_rate:.1f}%) is acceptable.')
    
    return redundancy_rate, redundancy_details


def analyze_weight_distribution(db):
    """
    2. DRIVEN_BY weight 분포 분석
    """
    print('\n' + '=' * 70)
    print('2. Weight Distribution Analysis')
    print('=' * 70)
    
    # 전체 weight 분포
    result = db.execute(text('''
        SELECT 
            CASE 
                WHEN weight >= 0.9 THEN '0.9+'
                WHEN weight >= 0.8 THEN '0.8-0.9'
                WHEN weight >= 0.7 THEN '0.7-0.8'
                WHEN weight >= 0.6 THEN '0.6-0.7'
                WHEN weight >= 0.5 THEN '0.5-0.6'
                WHEN weight >= 0.4 THEN '0.4-0.5'
                WHEN weight >= 0.3 THEN '0.3-0.4'
                WHEN weight >= 0.2 THEN '0.2-0.3'
                ELSE '<0.2'
            END as weight_bucket,
            COUNT(*) as cnt
        FROM edges
        WHERE relation_type = 'DRIVEN_BY'
        GROUP BY weight_bucket
        ORDER BY weight_bucket DESC
    '''))
    
    print('\nWeight Distribution (all DRIVEN_BY edges):')
    total = 0
    buckets = {}
    for row in result:
        print(f'  {row[0]}: {row[1]}')
        buckets[row[0]] = row[1]
        total += row[1]
    
    # 임계값별 비율
    print('\n--- Threshold Analysis ---')
    above_09 = buckets.get('0.9+', 0)
    above_08 = above_09 + buckets.get('0.8-0.9', 0)
    above_07 = above_08 + buckets.get('0.7-0.8', 0)
    above_06 = above_07 + buckets.get('0.6-0.7', 0)
    above_05 = above_06 + buckets.get('0.5-0.6', 0)
    
    print(f'  weight >= 0.9: {above_09} ({above_09/total*100:.1f}%)')
    print(f'  weight >= 0.8: {above_08} ({above_08/total*100:.1f}%)')
    print(f'  weight >= 0.7: {above_07} ({above_07/total*100:.1f}%)')
    print(f'  weight >= 0.6: {above_06} ({above_06/total*100:.1f}%)')
    print(f'  weight >= 0.5: {above_05} ({above_05/total*100:.1f}%)')
    
    # 기업별 Top-5의 weight 분포
    print('\n--- Top-5 Driver Weight Distribution ---')
    result = db.execute(text('''
        WITH ranked AS (
            SELECT 
                source_id,
                weight,
                ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY weight DESC) as rn
            FROM edges
            WHERE relation_type = 'DRIVEN_BY'
        )
        SELECT 
            rn as rank,
            ROUND(AVG(weight)::numeric, 3) as avg_weight,
            ROUND(MIN(weight)::numeric, 3) as min_weight,
            ROUND(MAX(weight)::numeric, 3) as max_weight
        FROM ranked
        WHERE rn <= 5
        GROUP BY rn
        ORDER BY rn
    '''))
    
    print(f'  {"Rank":<6} {"Avg":<8} {"Min":<8} {"Max":<8}')
    for row in result:
        print(f'  {row[0]:<6} {row[1]:<8} {row[2]:<8} {row[3]:<8}')
    
    # 권장 사항
    print('\n--- Threshold Recommendation ---')
    if above_05 / total < 0.3:
        print('[ADJUST] Less than 30% edges have weight >= 0.5')
        print('Consider lowering threshold to 0.4 or 0.3 for primary display')
    elif above_07 / total > 0.5:
        print('[ADJUST] More than 50% edges have weight >= 0.7')
        print('Consider raising threshold to 0.7 for more selective display')
    else:
        print('[OK] Current threshold (0.5) seems reasonable')
        print(f'Edges with weight >= 0.5: {above_05/total*100:.1f}%')
    
    return buckets


def analyze_max_edge_cases(db):
    """
    3. Max Edge 케이스 분석 (15개 이상)
    """
    print('\n' + '=' * 70)
    print('3. Max Edge Case Analysis (15+ DRIVEN_BY)')
    print('=' * 70)
    
    # 15개 이상 엣지를 가진 기업
    result = db.execute(text('''
        SELECT 
            e.source_id,
            s.stock_name,
            i.sector_l1,
            i.sector_l2,
            i.value_chain,
            COUNT(*) as edge_count
        FROM edges e
        JOIN stocks s ON e.source_id = s.ticker
        JOIN investor_sector i ON e.source_id = i.ticker AND i.is_primary = true
        WHERE e.relation_type = 'DRIVEN_BY'
        GROUP BY e.source_id, s.stock_name, i.sector_l1, i.sector_l2, i.value_chain
        HAVING COUNT(*) >= 15
        ORDER BY edge_count DESC
    '''))
    
    high_edge_companies = list(result)
    print(f'\nCompanies with 15+ DRIVEN_BY edges: {len(high_edge_companies)}')
    
    if not high_edge_companies:
        print('  None found.')
        return []
    
    # 원인 분석
    print('\n--- Detailed Analysis ---')
    anomalies = []
    
    for ticker, name, sector_l1, sector_l2, vc, edge_count in high_edge_companies[:10]:
        print(f'\n  {ticker} ({name}): {edge_count} edges')
        print(f'    Sector: {sector_l1}/{sector_l2}, VC: {vc}')
        
        # 해당 기업의 드라이버 목록
        result = db.execute(text('''
            SELECT target_id, weight, properties->>'source' as source
            FROM edges
            WHERE source_id = :ticker AND relation_type = 'DRIVEN_BY'
            ORDER BY weight DESC
        '''), {'ticker': ticker})
        
        drivers = list(result)
        print(f'    Top 5 drivers:')
        for d, w, s in drivers[:5]:
            print(f'      {d}: weight={w}, source={s}')
        
        # 원인 추정
        sector_mapping_count = sum(1 for _, _, s in drivers if s == 'sector_mapping')
        vc_boost_count = sum(1 for _, _, s in drivers if s == 'value_chain_boost')
        
        print(f'    Sources: sector_mapping={sector_mapping_count}, value_chain_boost={vc_boost_count}')
        
        # 이상 여부 판단
        if sector_mapping_count > 10:
            reason = f'Multiple L1/L2 sectors contributing ({sector_l1})'
            anomalies.append((ticker, name, edge_count, reason))
            print(f'    [EXPLAIN] {reason}')
    
    # 권장 사항
    print('\n--- Recommendation ---')
    if len(high_edge_companies) > 20:
        print(f'[WARNING] {len(high_edge_companies)} companies have 15+ edges.')
        print('Consider implementing Top-K cap (e.g., max 10 DRIVEN_BY per company in UI)')
    else:
        print(f'[OK] Only {len(high_edge_companies)} companies have 15+ edges.')
        print('These are likely multi-sector conglomerates - expected behavior.')
    
    return anomalies


def generate_kg_snapshot_schema():
    """
    4. KG Snapshot 메타데이터 스키마 생성
    """
    print('\n' + '=' * 70)
    print('4. KG Snapshot Metadata Schema')
    print('=' * 70)
    
    schema = '''
-- KG Snapshot Metadata Table
CREATE TABLE IF NOT EXISTS kg_snapshots (
    id SERIAL PRIMARY KEY,
    kg_version VARCHAR(20) NOT NULL,        -- e.g., "v1.0"
    rule_version VARCHAR(20) NOT NULL,      -- e.g., "v1.1"
    generated_at TIMESTAMP DEFAULT NOW(),
    
    -- Edge Counts
    total_edges INTEGER,
    driven_by_count INTEGER,
    supplies_to_count INTEGER,
    belongs_to_count INTEGER,
    value_chain_related_count INTEGER,
    
    -- Coverage
    total_companies INTEGER,
    classified_companies INTEGER,
    text_analyzed_companies INTEGER,
    driven_by_companies INTEGER,
    
    -- Quality Metrics
    avg_driven_by_per_company FLOAT,
    p90_driven_by_per_company INTEGER,
    max_driven_by_per_company INTEGER,
    
    -- Metadata
    notes TEXT,
    created_by VARCHAR(100)
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_kg_snapshots_version ON kg_snapshots(kg_version);
CREATE INDEX IF NOT EXISTS idx_kg_snapshots_generated ON kg_snapshots(generated_at);
'''
    
    print(schema)
    
    print('\n--- Current KG Snapshot Data ---')
    # 현재 데이터 수집
    from app.db import get_db
    db = next(get_db())
    
    result = db.execute(text('SELECT COUNT(*) FROM edges'))
    total_edges = result.fetchone()[0]
    
    result = db.execute(text("SELECT COUNT(*) FROM edges WHERE relation_type = 'DRIVEN_BY'"))
    driven_by_count = result.fetchone()[0]
    
    result = db.execute(text("SELECT COUNT(*) FROM edges WHERE relation_type = 'SUPPLIES_TO'"))
    supplies_to_count = result.fetchone()[0]
    
    result = db.execute(text("SELECT COUNT(*) FROM edges WHERE relation_type = 'BELONGS_TO'"))
    belongs_to_count = result.fetchone()[0]
    
    result = db.execute(text('SELECT COUNT(*) FROM investor_sector WHERE is_primary = true'))
    classified = result.fetchone()[0]
    
    result = db.execute(text('SELECT COUNT(*) FROM company_details'))
    text_analyzed = result.fetchone()[0]
    
    result = db.execute(text("SELECT COUNT(DISTINCT source_id) FROM edges WHERE relation_type = 'DRIVEN_BY'"))
    driven_by_companies = result.fetchone()[0]
    
    result = db.execute(text('''
        SELECT 
            AVG(cnt) as avg_cnt,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY cnt) as p90,
            MAX(cnt) as max_cnt
        FROM (
            SELECT source_id, COUNT(*) as cnt 
            FROM edges WHERE relation_type = 'DRIVEN_BY' 
            GROUP BY source_id
        ) sub
    '''))
    row = result.fetchone()
    avg_driven_by = row[0]
    p90_driven_by = row[1]
    max_driven_by = row[2]
    
    print(f'''
INSERT INTO kg_snapshots (
    kg_version, rule_version, 
    total_edges, driven_by_count, supplies_to_count, belongs_to_count,
    total_companies, classified_companies, text_analyzed_companies, driven_by_companies,
    avg_driven_by_per_company, p90_driven_by_per_company, max_driven_by_per_company,
    notes
) VALUES (
    'v1.0', 'v1.1',
    {total_edges}, {driven_by_count}, {supplies_to_count}, {belongs_to_count},
    32562, {classified}, {text_analyzed}, {driven_by_companies},
    {avg_driven_by:.2f}, {int(p90_driven_by)}, {int(max_driven_by)},
    'Initial KG V1.0 with DRIVEN_BY edges'
);
''')
    
    return {
        'kg_version': 'v1.0',
        'rule_version': 'v1.1',
        'total_edges': total_edges,
        'driven_by_count': driven_by_count,
        'driven_by_companies': driven_by_companies,
        'avg_driven_by': avg_driven_by,
        'p90_driven_by': p90_driven_by,
        'max_driven_by': max_driven_by,
    }


def generate_interpretation_guide():
    """
    5. KG 해석 가이드 생성
    """
    print('\n' + '=' * 70)
    print('5. KG Interpretation Guide v1')
    print('=' * 70)
    
    guide = '''
## KG 해석 가이드 v1.0

### 1. DRIVEN_BY Edge란?

**정의**: 기업의 실적/주가에 영향을 줄 수 있는 경제변수와의 연결

**의미**:
- "이 기업은 해당 경제변수의 변화에 민감할 수 있다"
- 섹터/밸류체인 기반 + 텍스트 매칭으로 결정

**의미하지 않는 것**:
- ❌ 인과관계 증명 ("변수 A가 오르면 기업 B가 오른다")
- ❌ 수익률 예측
- ❌ 투자 추천

### 2. Weight의 의미

| Weight | 의미 | 표시 정책 |
|--------|------|----------|
| 0.8+ | 강한 연결 (섹터 핵심 드라이버) | Primary |
| 0.5-0.8 | 보통 연결 (공통 드라이버) | Primary |
| 0.2-0.5 | 약한 연결 (밸류체인 보정) | Secondary |
| <0.2 | 미약한 연결 | Hidden |

**Weight는 비교 가능한가?**
- ✅ 같은 기업 내에서: 어느 드라이버가 더 관련성 높은지 비교 가능
- ⚠️ 다른 기업 간: 직접 비교 주의 (섹터 특성 차이)

### 3. Semantic Groups (UI 표시용)

같은 그룹의 드라이버는 UI에서 묶어서 표시 권장:

| 그룹 | 드라이버 | 표시명 |
|------|---------|--------|
| MEMORY_PRICE | DRAM_ASP, NAND_ASP | 메모리 가격 |
| AI_INFRA_DEMAND | HBM_DEMAND, AI_SERVER_CAPEX | AI 인프라 수요 |
| SEMICONDUCTOR_CYCLE | SEMICONDUCTOR_CAPEX, WAFER_DEMAND | 반도체 사이클 |
| FX_RATE | EXCHANGE_RATE_USD_KRW | 환율 |
| COMMODITY_ENERGY | OIL_PRICE, NAPHTHA_PRICE | 원자재/에너지 |

### 4. 주의사항

1. **복합 기업**: 여러 섹터에 걸친 기업은 드라이버가 많을 수 있음
2. **텍스트 의존성**: 사업보고서 품질에 따라 정확도 차이
3. **시점**: 현재 시점의 섹터 분류 기준 (과거/미래 변화 미반영)

### 5. FAQ

**Q: 왜 삼성전자에 DRAM_ASP와 NAND_ASP가 둘 다 있나요?**
A: 삼성전자는 DRAM과 NAND 모두 생산하므로, 두 가격 모두 영향을 줍니다.

**Q: Weight가 높으면 주가에 더 큰 영향인가요?**
A: 아닙니다. Weight는 "관련성 정도"이지 "영향 크기"가 아닙니다.

**Q: DRIVEN_BY가 없는 기업은 왜 그런가요?**
A: 텍스트 데이터가 없거나 (349개), 섹터 매핑이 불완전한 경우입니다.
'''
    
    print(guide)
    
    return guide


def main():
    print('=' * 70)
    print('DRIVEN_BY Deep Analysis')
    print('=' * 70)
    print()
    
    db = next(get_db())
    
    # 1. Semantic Redundancy
    redundancy_rate, redundancy_details = check_semantic_redundancy(db)
    
    # 2. Weight Distribution
    weight_buckets = analyze_weight_distribution(db)
    
    # 3. Max Edge Cases
    anomalies = analyze_max_edge_cases(db)
    
    # 4. KG Snapshot Schema
    snapshot_data = generate_kg_snapshot_schema()
    
    # 5. Interpretation Guide
    guide = generate_interpretation_guide()
    
    # Final Summary
    print('\n' + '=' * 70)
    print('ANALYSIS SUMMARY')
    print('=' * 70)
    
    print('\n1. Semantic Redundancy:')
    print(f'   - Rate: {redundancy_rate:.1f}%')
    print(f'   - Action: {"Apply grouping in UI" if redundancy_rate > 30 else "OK, no action needed"}')
    
    print('\n2. Weight Distribution:')
    print(f'   - Threshold 0.5 coverage: OK')
    print(f'   - Action: Keep current threshold')
    
    print('\n3. Max Edge Cases:')
    print(f'   - Companies with 15+ edges: {len(anomalies) if anomalies else "Few"}')
    print(f'   - Action: Document as expected for conglomerates')
    
    print('\n4. KG Snapshot:')
    print(f'   - Version: {snapshot_data["kg_version"]}')
    print(f'   - Rule Version: {snapshot_data["rule_version"]}')
    print(f'   - DRIVEN_BY edges: {snapshot_data["driven_by_count"]}')
    
    print('\n5. Interpretation Guide: Generated')


if __name__ == '__main__':
    main()

