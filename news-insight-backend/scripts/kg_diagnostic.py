# -*- coding: utf-8 -*-
"""
KG 구축 현황 진단 스크립트
- supply_chain 실명 비율
- biz_summary 길이 분포 및 결측률
- 섹터-밸류체인 이상치 탐지
- 2,949 vs 2,600 갭 분석
"""

import sys
import os
import codecs
import json
import re

# 인코딩 설정
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

# 프로젝트 루트 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from app.db import get_db
from sqlalchemy import text


def check_supply_chain_quality(db):
    """P0-1: supply_chain 실명 비율 확인"""
    print('=' * 70)
    print('P0-1: supply_chain Data Quality Check')
    print('=' * 70)
    
    # 1. 전체 통계
    result = db.execute(text('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN supply_chain IS NOT NULL 
                     AND supply_chain::text != '[]' 
                     AND supply_chain::text != 'null' 
                     AND supply_chain::text != '{}' 
                THEN 1 ELSE 0 END) as has_data
        FROM company_details
    '''))
    row = result.fetchone()
    total, has_data = row[0], row[1]
    print(f'Total company_details: {total}')
    print(f'Has supply_chain data: {has_data}')
    print(f'Coverage: {has_data/total*100:.1f}%' if total > 0 else 'N/A')
    
    # 2. supply_chain 내용 분석 (실명 vs 익명)
    print()
    print('-' * 70)
    print('supply_chain Sample Analysis (Named vs Anonymous)')
    print('-' * 70)
    
    result = db.execute(text('''
        SELECT cd.ticker, s.stock_name, cd.supply_chain::text
        FROM company_details cd
        JOIN stocks s ON cd.ticker = s.ticker
        WHERE cd.supply_chain IS NOT NULL 
          AND cd.supply_chain::text != '[]' 
          AND cd.supply_chain::text != 'null'
          AND cd.supply_chain::text != '{}'
          AND LENGTH(cd.supply_chain::text) > 10
        ORDER BY LENGTH(cd.supply_chain::text) DESC
        LIMIT 15
    '''))
    
    named_count = 0
    anonymous_count = 0
    
    # 실명 패턴 (한국 기업명)
    named_patterns = [
        r'삼성', r'현대', r'SK', r'LG', r'포스코', r'한화', r'롯데', r'두산',
        r'기아', r'셀트리온', r'카카오', r'네이버', r'CJ', r'GS', r'신세계',
        r'\w+전자', r'\w+화학', r'\w+건설', r'\w+제약', r'\w+바이오',
    ]
    # 익명 패턴
    anonymous_patterns = [r'[A-Z]사', r'[가-하]사', r'업체\d+', r'협력사\d+']
    
    for row in result:
        ticker, name, sc_text = row[0], row[1], row[2]
        
        # 실명 여부 판단
        is_named = any(re.search(p, sc_text) for p in named_patterns)
        is_anonymous = any(re.search(p, sc_text) for p in anonymous_patterns)
        
        if is_named and not is_anonymous:
            named_count += 1
            status = 'NAMED'
        elif is_anonymous:
            anonymous_count += 1
            status = 'ANONYMOUS'
        else:
            status = 'MIXED/UNCLEAR'
        
        sc_preview = sc_text[:150] + '...' if len(sc_text) > 150 else sc_text
        print(f'[{status}] {ticker} ({name}):')
        print(f'  {sc_preview}')
        print()
    
    print('-' * 70)
    print(f'Sample Summary: Named={named_count}, Anonymous={anonymous_count}')
    print('-' * 70)
    
    return has_data, total


def check_biz_summary_quality(db):
    """P0-2: biz_summary 길이 분포 및 결측률"""
    print()
    print('=' * 70)
    print('P0-2: biz_summary Quality Check')
    print('=' * 70)
    
    # 1. 길이 분포
    result = db.execute(text('''
        WITH summary_stats AS (
            SELECT 
                CASE 
                    WHEN biz_summary IS NULL THEN 'NULL'
                    WHEN LENGTH(biz_summary) = 0 THEN 'EMPTY'
                    WHEN LENGTH(biz_summary) < 50 THEN '<50 chars'
                    WHEN LENGTH(biz_summary) < 200 THEN '50-200 chars'
                    WHEN LENGTH(biz_summary) < 500 THEN '200-500 chars'
                    WHEN LENGTH(biz_summary) < 1000 THEN '500-1000 chars'
                    ELSE '1000+ chars'
                END as length_bucket,
                CASE 
                    WHEN biz_summary IS NULL THEN 1
                    WHEN LENGTH(biz_summary) = 0 THEN 2
                    WHEN LENGTH(biz_summary) < 50 THEN 3
                    WHEN LENGTH(biz_summary) < 200 THEN 4
                    WHEN LENGTH(biz_summary) < 500 THEN 5
                    WHEN LENGTH(biz_summary) < 1000 THEN 6
                    ELSE 7
                END as sort_order
            FROM company_details
        )
        SELECT length_bucket, COUNT(*) as cnt
        FROM summary_stats
        GROUP BY length_bucket, sort_order
        ORDER BY sort_order
    '''))
    
    print('biz_summary Length Distribution:')
    total = 0
    low_quality = 0
    for row in result:
        bucket, cnt = row[0], row[1]
        total += cnt
        if bucket in ['NULL', 'EMPTY', '<50 chars']:
            low_quality += cnt
        print(f'  {bucket}: {cnt}')
    
    print(f'  -- Total: {total}, Low Quality (NULL/EMPTY/<50): {low_quality} ({low_quality/total*100:.1f}%)')
    
    # 2. products/keywords 결측률
    print()
    print('products/keywords Missing Rate:')
    result = db.execute(text('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN products IS NULL OR products::text = '[]' THEN 1 ELSE 0 END) as no_products,
            SUM(CASE WHEN keywords IS NULL OR keywords::text = '[]' THEN 1 ELSE 0 END) as no_keywords
        FROM company_details
    '''))
    row = result.fetchone()
    total, no_products, no_keywords = row[0], row[1], row[2]
    print(f'  No products: {no_products}/{total} ({no_products/total*100:.1f}%)')
    print(f'  No keywords: {no_keywords}/{total} ({no_keywords/total*100:.1f}%)')
    
    return low_quality, total


def check_sector_vc_anomalies(db):
    """P0-3: 섹터-밸류체인 이상치 탐지"""
    print()
    print('=' * 70)
    print('P0-3: Sector-ValueChain Anomaly Detection')
    print('=' * 70)
    
    # 1. SEC_BIO/SEC_PHARM인데 MID_HARD/DOWN_BIZ로 분류된 경우
    print('Case 1: BIO/PHARM sector but classified as MID_HARD/DOWN_BIZ')
    result = db.execute(text('''
        SELECT i.ticker, s.stock_name, i.sector_l1, i.value_chain, 
               ROUND(i.value_chain_confidence::numeric, 3) as conf
        FROM investor_sector i
        JOIN stocks s ON i.ticker = s.ticker
        WHERE i.is_primary = true
          AND i.sector_l1 IN ('SEC_BIO', 'SEC_PHARM')
          AND i.value_chain IN ('MID_HARD', 'DOWN_BIZ')
        LIMIT 10
    '''))
    rows = list(result)
    if rows:
        for row in rows:
            print(f'  {row[0]} ({row[1]}): {row[2]} -> {row[3]} (conf={row[4]})')
    else:
        print('  No anomalies found.')
    
    # 2. SEC_BANK/SEC_INSURANCE인데 UPSTREAM/MID_*로 분류된 경우
    print()
    print('Case 2: BANK/INSURANCE sector but classified as UPSTREAM/MID_*')
    result = db.execute(text('''
        SELECT i.ticker, s.stock_name, i.sector_l1, i.value_chain,
               ROUND(i.value_chain_confidence::numeric, 3) as conf
        FROM investor_sector i
        JOIN stocks s ON i.ticker = s.ticker
        WHERE i.is_primary = true
          AND i.sector_l1 IN ('SEC_BANK', 'SEC_INSURANCE', 'SEC_SECURITIES', 'SEC_FINANCE')
          AND i.value_chain IN ('UPSTREAM', 'MID_HARD', 'MID_SOFT')
        LIMIT 10
    '''))
    rows = list(result)
    if rows:
        for row in rows:
            print(f'  {row[0]} ({row[1]}): {row[2]} -> {row[3]} (conf={row[4]})')
    else:
        print('  No anomalies found.')
    
    # 3. SEC_GAME/SEC_ENT인데 MID_HARD로 분류된 경우
    print()
    print('Case 3: GAME/ENTERTAINMENT sector but classified as MID_HARD')
    result = db.execute(text('''
        SELECT i.ticker, s.stock_name, i.sector_l1, i.value_chain,
               ROUND(i.value_chain_confidence::numeric, 3) as conf
        FROM investor_sector i
        JOIN stocks s ON i.ticker = s.ticker
        WHERE i.is_primary = true
          AND i.sector_l1 IN ('SEC_GAME', 'SEC_ENT', 'SEC_MEDIA')
          AND i.value_chain IN ('UPSTREAM', 'MID_HARD')
        LIMIT 10
    '''))
    rows = list(result)
    if rows:
        for row in rows:
            print(f'  {row[0]} ({row[1]}): {row[2]} -> {row[3]} (conf={row[4]})')
    else:
        print('  No anomalies found.')
    
    return len(rows)


def check_coverage_gap(db):
    """P0-4: 2,949 vs 2,600 갭 분석"""
    print()
    print('=' * 70)
    print('P0-4: Coverage Gap Analysis (stocks vs company_details/embeddings)')
    print('=' * 70)
    
    # 1. 각 테이블 카운트
    result = db.execute(text('SELECT COUNT(*) FROM stocks'))
    stocks_count = result.fetchone()[0]
    
    result = db.execute(text('SELECT COUNT(*) FROM investor_sector WHERE is_primary = true'))
    investor_sector_count = result.fetchone()[0]
    
    result = db.execute(text('SELECT COUNT(*) FROM company_details'))
    company_details_count = result.fetchone()[0]
    
    result = db.execute(text('SELECT COUNT(*) FROM company_embeddings'))
    embeddings_count = result.fetchone()[0]
    
    print(f'stocks: {stocks_count}')
    print(f'investor_sector (primary): {investor_sector_count}')
    print(f'company_details: {company_details_count}')
    print(f'company_embeddings: {embeddings_count}')
    
    gap = stocks_count - company_details_count
    print(f'\nGap (stocks - company_details): {gap}')
    print(f'Text-based Analysis Coverage: {company_details_count}/{stocks_count} ({company_details_count/stocks_count*100:.1f}%)')
    
    # 2. 갭에 해당하는 기업 샘플
    print()
    print('Sample of companies without company_details:')
    result = db.execute(text('''
        SELECT s.ticker, s.stock_name, i.sector_l1
        FROM stocks s
        LEFT JOIN company_details cd ON s.ticker = cd.ticker
        LEFT JOIN investor_sector i ON s.ticker = i.ticker AND i.is_primary = true
        WHERE cd.ticker IS NULL
        LIMIT 15
    '''))
    
    for row in result:
        print(f'  {row[0]} ({row[1]}): {row[2] or "No sector"}')
    
    return gap, stocks_count, company_details_count


def check_edges_status(db):
    """현재 edges 테이블 상태"""
    print()
    print('=' * 70)
    print('Current KG Edges Status')
    print('=' * 70)
    
    result = db.execute(text('SELECT COUNT(*) FROM edges'))
    total = result.fetchone()[0]
    print(f'Total edges: {total}')
    
    if total > 0:
        result = db.execute(text('''
            SELECT relation_type, COUNT(*) as cnt
            FROM edges 
            GROUP BY relation_type 
            ORDER BY cnt DESC
        '''))
        print('\nEdges by type:')
        for row in result:
            print(f'  {row[0]}: {row[1]}')
    
    # DRIVEN_BY 체크
    result = db.execute(text('''
        SELECT COUNT(*) FROM edges WHERE relation_type = 'DRIVEN_BY'
    '''))
    driven_by_count = result.fetchone()[0]
    print(f'\nDRIVEN_BY edges: {driven_by_count}')
    
    return driven_by_count


def main():
    print('=' * 70)
    print('KG Construction Diagnostic Report')
    print('=' * 70)
    print()
    
    db = next(get_db())
    
    # P0-1: supply_chain 품질
    sc_has_data, sc_total = check_supply_chain_quality(db)
    
    # P0-2: biz_summary 품질
    low_quality, text_total = check_biz_summary_quality(db)
    
    # P0-3: 섹터-VC 이상치
    anomaly_count = check_sector_vc_anomalies(db)
    
    # P0-4: 커버리지 갭
    gap, stocks_count, details_count = check_coverage_gap(db)
    
    # 현재 edges 상태
    driven_by_count = check_edges_status(db)
    
    # 최종 요약
    print()
    print('=' * 70)
    print('DIAGNOSTIC SUMMARY')
    print('=' * 70)
    print(f'1. supply_chain coverage: {sc_has_data}/{sc_total} ({sc_has_data/sc_total*100:.1f}%)')
    print(f'2. biz_summary low quality: {low_quality}/{text_total} ({low_quality/text_total*100:.1f}%)')
    print(f'3. Sector-VC anomalies found: {anomaly_count}')
    print(f'4. Coverage gap: {gap} companies without text data')
    print(f'5. DRIVEN_BY edges: {driven_by_count} ({"MISSING - NEEDS CREATION" if driven_by_count == 0 else "OK"})')
    print()
    print('Text-based Analysis Coverage: {:.1f}%'.format(details_count/stocks_count*100))
    print('=' * 70)


if __name__ == '__main__':
    main()

