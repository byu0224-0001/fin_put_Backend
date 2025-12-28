#!/usr/bin/env python3
"""MID_SOFT 분류 결과 검증 스크립트"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from app.db import get_db
from sqlalchemy import text

db = next(get_db())

# MID_SOFT 분류 기업 조회
print('='*70)
print('CHECK: MID_SOFT Classification Sample')
print('='*70)

result = db.execute(text('''
    SELECT 
        s.stock_name,
        isec.ticker,
        isec.sector_l2,
        isec.value_chain,
        isec.value_chain_detail,
        ROUND(CAST(isec.value_chain_confidence AS numeric), 4) as confidence
    FROM investor_sector isec
    JOIN stocks s ON isec.ticker = s.ticker
    WHERE isec.value_chain = 'MID_SOFT'
        AND isec.is_primary = true
    ORDER BY isec.value_chain_confidence DESC
    LIMIT 30
'''))

rows = result.fetchall()
print(f'MID_SOFT count: {len(rows)}')
print()
print(f'{"Ticker":10} {"Name":20} {"Sector":25} {"Detail":15} {"Confidence":10}')
print('-'*80)
for row in rows:
    name = (row[0][:18] if row[0] else 'N/A')
    detail = str(row[4] or '-')
    print(f'{row[1]:10} {name:20} {str(row[2] or "N/A"):25} {detail:15} {row[5]:.4f}')

print()
print('='*70)
print('CHECK: SEC_SEMI sector -> MID_SOFT')
print('='*70)

result2 = db.execute(text('''
    SELECT 
        s.stock_name,
        isec.ticker,
        isec.sector_l2,
        isec.value_chain,
        ROUND(CAST(isec.value_chain_confidence AS numeric), 4) as confidence
    FROM investor_sector isec
    JOIN stocks s ON isec.ticker = s.ticker
    WHERE isec.sector_l1 = 'SEC_SEMI'
        AND isec.is_primary = true
        AND isec.value_chain = 'MID_SOFT'
    ORDER BY isec.value_chain_confidence DESC
    LIMIT 20
'''))

rows2 = result2.fetchall()
print(f'SEC_SEMI -> MID_SOFT count: {len(rows2)}')
for row in rows2:
    name = (row[0][:18] if row[0] else 'N/A')
    print(f'{row[1]:10} {name:20} {str(row[2] or "N/A"):25} {row[4]:.4f}')

print()
print('='*70)
print('CHECK: Value Chain Distribution')
print('='*70)

result3 = db.execute(text('''
    SELECT 
        value_chain,
        COUNT(*) as cnt,
        ROUND(AVG(CAST(value_chain_confidence AS numeric)), 4) as avg_conf
    FROM investor_sector
    WHERE is_primary = true AND value_chain IS NOT NULL
    GROUP BY value_chain
    ORDER BY cnt DESC
'''))

rows3 = result3.fetchall()
total = sum(r[1] for r in rows3)
print(f'{"Value Chain":15} {"Count":>8} {"Ratio":>8} {"Avg Conf":>10}')
print('-'*45)
for row in rows3:
    ratio = row[1] / total * 100
    print(f'{row[0]:15} {row[1]:>8} {ratio:>7.1f}% {row[2]:>10.4f}')
print('-'*45)
print(f'{"TOTAL":15} {total:>8}')

print()
print('='*70)
print('CHECK: Known Fabless Companies')
print('='*70)

# 알려진 팹리스 기업들 확인
fabless_tickers = ['000660', '005930', '042700', '058470', '095340', '102710', '217190']

result4 = db.execute(text('''
    SELECT 
        s.stock_name,
        isec.ticker,
        isec.sector_l2,
        isec.value_chain,
        isec.value_chain_detail,
        ROUND(CAST(isec.value_chain_confidence AS numeric), 4) as confidence
    FROM investor_sector isec
    JOIN stocks s ON isec.ticker = s.ticker
    WHERE isec.ticker IN :tickers
        AND isec.is_primary = true
'''), {'tickers': tuple(fabless_tickers)})

rows4 = result4.fetchall()
print(f'{"Ticker":10} {"Name":20} {"ValueChain":15} {"Detail":15} {"Confidence":10}')
print('-'*70)
for row in rows4:
    name = (row[0][:18] if row[0] else 'N/A')
    detail = str(row[4] or '-')
    print(f'{row[1]:10} {name:20} {str(row[3] or "N/A"):15} {detail:15} {row[5]:.4f}')

