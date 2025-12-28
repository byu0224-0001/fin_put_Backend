# -*- coding: utf-8 -*-
"""
FASHION_RETAIL 2개 기업 확인
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

db = SessionLocal()

companies = ['신세계', '신세계인터내셔날']

for name in companies:
    stock = db.query(Stock).filter(Stock.stock_name == name).first()
    if stock:
        sector = db.query(InvestorSector).filter(
            InvestorSector.ticker == stock.ticker,
            InvestorSector.is_primary == True
        ).first()
        detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == stock.ticker).first()
        
        print(f'=== {name} ({stock.ticker}) ===')
        print(f'KRX업종: {stock.industry_raw}')
        if sector:
            print(f'major_sector: {sector.major_sector}')
            print(f'sub_sector: {sector.sub_sector}')
        if detail:
            print(f'keywords: {detail.keywords[:5] if detail.keywords else None}')
            print(f'biz_summary: {detail.biz_summary[:200] if detail.biz_summary else None}...')
        print()

db.close()

