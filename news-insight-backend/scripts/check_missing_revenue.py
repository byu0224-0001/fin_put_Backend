# -*- coding: utf-8 -*-
"""
매출 비중 데이터가 없는 기업 확인 및 리스트 생성
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

def main():
    db = SessionLocal()
    
    # 매출 비중이 없거나 빈 딕셔너리인 기업 확인
    all_details = db.query(CompanyDetail, Stock).join(
        Stock, CompanyDetail.ticker == Stock.ticker
    ).all()
    
    missing_revenue = []
    for detail, stock in all_details:
        # None이거나 빈 딕셔너리인 경우
        if detail.revenue_by_segment is None or detail.revenue_by_segment == {} or len(detail.revenue_by_segment) == 0:
            missing_revenue.append((detail, stock))
    
    print(f"=== 매출 비중 없는 기업: {len(missing_revenue)}개 ===")
    print()
    
    # 상위 30개 출력
    print("[상위 30개 기업]")
    for i, (detail, stock) in enumerate(missing_revenue[:30]):
        print(f"  {i+1}. {stock.stock_name} ({stock.ticker}) - {stock.market}")
    
    # ticker 리스트 저장
    tickers = [detail.ticker for detail, stock in missing_revenue]
    
    with open('reports/missing_revenue_tickers.txt', 'w', encoding='utf-8') as f:
        for ticker in tickers:
            f.write(ticker + '\n')
    
    print()
    print(f"총 ticker 수: {len(tickers)}")
    print(f"파일 저장: reports/missing_revenue_tickers.txt")
    
    db.close()
    return tickers

if __name__ == '__main__':
    main()

