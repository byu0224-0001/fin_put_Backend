# -*- coding: utf-8 -*-
"""Check 2: market_cap 데이터 유무 확인"""

import sys
import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')

import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.db import engine

db = engine.connect()

# stocks 테이블 컬럼 확인
result = db.execute(text("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'stocks' 
    ORDER BY ordinal_position
"""))

print("=" * 80)
print("[Check 2] stocks 테이블 컬럼 확인")
print("=" * 80)
columns = []
for row in result:
    columns.append(row[0])
    print(f"  {row[0]}: {row[1]}")

print(f"\n총 컬럼 수: {len(columns)}")

# market_cap 또는 유사 컬럼 확인
market_cap_candidates = ['market_cap', 'marketcap', 'market_capitalization', '시가총액', 'market_value']
found = [col for col in columns if any(candidate.lower() in col.lower() for candidate in market_cap_candidates)]

if found:
    print(f"\n✅ 시가총액 관련 컬럼 발견: {', '.join(found)}")
    # 데이터 확인
    for col in found:
        result = db.execute(text(f"""
            SELECT COUNT(*) as total, COUNT({col}) as has_value
            FROM stocks
        """))
        row = result.fetchone()
        if row[0] > 0:
            ratio = (row[1] / row[0]) * 100
            print(f"  {col}: {row[1]}/{row[0]} ({ratio:.1f}%)")
else:
    print("\n❌ market_cap 컬럼이 없습니다.")
    print("\n[대안]")
    print("  1. Tie-Breaking에서 market_cap 제거 (현재 코드는 이미 ticker로 fallback)")
    print("  2. 나중에 market_cap 컬럼 추가 후 사용")
    print("  3. 매출액(sales) 컬럼으로 대체 가능 여부 확인")

# 매출액 컬럼 확인
sales_candidates = ['sales', 'revenue', '매출', 'revenue_total']
sales_found = [col for col in columns if any(candidate.lower() in col.lower() for candidate in sales_candidates)]

if sales_found:
    print(f"\n💡 매출액 관련 컬럼 발견: {', '.join(sales_found)}")
    for col in sales_found:
        result = db.execute(text(f"""
            SELECT COUNT(*) as total, COUNT({col}) as has_value
            FROM stocks
        """))
        row = result.fetchone()
        if row[0] > 0:
            ratio = (row[1] / row[0]) * 100
            print(f"  {col}: {row[1]}/{row[0]} ({ratio:.1f}%)")

db.close()

print("\n" + "=" * 80)
print("Check 2 완료!")
print("=" * 80)

