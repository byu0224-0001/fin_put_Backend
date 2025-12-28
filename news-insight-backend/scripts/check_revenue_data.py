# -*- coding: utf-8 -*-
"""
매출 비중 데이터 현황 확인 및 지주회사 매출 구조 분석
"""
import sys
from pathlib import Path
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

# 토스증권 지주사 중 주요 기업
HOLDING_COMPANIES = [
    "POSCO홀딩스", "SK", "CJ", "GS", "LG", "두산", "효성", "한진칼",
    "롯데지주", "영원무역홀딩스", "코오롱", "대웅", "한화", "LS"
]


def check_revenue_data():
    """매출 비중 데이터 현황 확인"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("[매출 비중 데이터(revenue_by_segment) 현황]")
        print("=" * 80)
        
        # 전체 CompanyDetail 확인
        total = db.query(CompanyDetail).count()
        
        # revenue_by_segment가 있는 기업
        with_revenue = db.query(CompanyDetail).filter(
            CompanyDetail.revenue_by_segment != None,
            CompanyDetail.revenue_by_segment != {}
        ).count()
        
        print(f"\n전체 CompanyDetail: {total}개")
        print(f"revenue_by_segment 있음: {with_revenue}개 ({with_revenue/total*100:.1f}%)")
        
        # 지주회사 매출 비중 확인
        print(f"\n[지주회사 매출 비중 데이터]")
        print("-" * 80)
        
        for company_name in HOLDING_COMPANIES:
            # Stock에서 검색
            stock = db.query(Stock).filter(
                Stock.stock_name == company_name
            ).first()
            
            if not stock:
                stock = db.query(Stock).filter(
                    Stock.stock_name.contains(company_name)
                ).first()
            
            if not stock:
                print(f"\n{company_name}: 미발견")
                continue
            
            # CompanyDetail 확인
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).first()
            
            if detail:
                revenue = detail.revenue_by_segment
                products = detail.products[:5] if detail.products else []
                
                print(f"\n{stock.stock_name} ({stock.ticker}):")
                print(f"  products: {products}")
                
                if revenue and isinstance(revenue, dict) and len(revenue) > 0:
                    print(f"  revenue_by_segment: ✅")
                    for seg, pct in revenue.items():
                        print(f"    - {seg}: {pct}%")
                else:
                    print(f"  revenue_by_segment: ❌ 없음")
            else:
                print(f"\n{company_name} ({stock.ticker}): CompanyDetail 없음")
        
        # 매출 비중 데이터 예시 (상위 5개)
        print(f"\n[매출 비중 데이터 예시 (상위 5개)]")
        print("-" * 80)
        
        samples = db.query(CompanyDetail).filter(
            CompanyDetail.revenue_by_segment != None,
            CompanyDetail.revenue_by_segment != {}
        ).limit(5).all()
        
        for detail in samples:
            stock = db.query(Stock).filter(Stock.ticker == detail.ticker).first()
            name = stock.stock_name if stock else detail.ticker
            print(f"\n{name}:")
            for seg, pct in detail.revenue_by_segment.items():
                print(f"  - {seg}: {pct}%")
        
    finally:
        db.close()


if __name__ == "__main__":
    check_revenue_data()

