# -*- coding: utf-8 -*-
"""
SEC_COSMETIC에서 SEC_FASHION으로 재분류 필요한 기업 확인
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock

db = SessionLocal()

# SEC_COSMETIC으로 분류된 기업 중 실제 섬유/의류인 기업 수
fashion_krx = [
    '봉제의복 제조업', 
    '직물직조 및 직물제품 제조업', 
    '방적 및 가공사 제조업', 
    '섬유제품 염색',
    '섬유, 의복, 신발 및 가죽제품 소매업'
]

cosmetic_companies = db.query(
    InvestorSector.ticker,
    Stock.stock_name,
    Stock.industry_raw
).join(
    Stock, InvestorSector.ticker == Stock.ticker
).filter(
    InvestorSector.major_sector == 'SEC_COSMETIC',
    InvestorSector.is_primary == True
).all()

fashion_misclassified = []
for ticker, name, industry_raw in cosmetic_companies:
    industry_raw = industry_raw or ''
    if any(kw in industry_raw for kw in fashion_krx):
        fashion_misclassified.append((ticker, name, industry_raw))

print(f'=== SEC_COSMETIC 중 섬유/의류 KRX 업종 기업 ({len(fashion_misclassified)}개) ===')
print('→ SEC_FASHION으로 재분류 필요')
print()
for ticker, name, industry in fashion_misclassified:
    print(f'  {ticker} - {name} ({industry})')

db.close()

